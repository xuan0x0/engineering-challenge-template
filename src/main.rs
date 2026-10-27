//! Kalshi orderbook collector. Subscribes to every open non-MVE market across
//! sharded websockets, and writes snapshots + price level updates as SBE.

use std::io::IsTerminal;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use clap::Parser;
use tokio::signal::unix::SignalKind;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use engineering_challenge::collector::{self, Collector};
use engineering_challenge::kalshi::auth::Signer;
use engineering_challenge::kalshi::connection::{ShardConfig, run_shard};
use engineering_challenge::kalshi::rest::RestClient;
use engineering_challenge::kalshi::{DEFAULT_REST_URL, DEFAULT_WS_URL};
use engineering_challenge::universe::{self, ShardLimits};
use engineering_challenge::writer::{OutputTarget, RotatingWriter};

const EVENT_CHANNEL_CAPACITY: usize = 1 << 16;
/// After shutdown starts, how long the collector waits for shards to hand over
/// in-flight events before closing the file regardless.
const COLLECTOR_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Parser)]
#[command(about = "Collects Kalshi orderbook data into SBE files")]
struct Args {
    /// Kalshi API key ID.
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    api_key_id: String,

    /// Path to the RSA private key (PEM) for the API key.
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    private_key_path: PathBuf,

    #[arg(long, env = "KALSHI_WS_URL", default_value = DEFAULT_WS_URL)]
    ws_url: String,

    #[arg(long, env = "KALSHI_REST_URL", default_value = DEFAULT_REST_URL)]
    rest_url: String,

    /// Directory for hourly-rotated output files.
    #[arg(long, default_value = "data")]
    output_dir: PathBuf,

    /// Write SBE to stdout instead of rotating files.
    #[arg(long)]
    stdout: bool,

    /// Target markets per websocket. New sockets open once all are full.
    /// ~150k open markets / 1000 = ~150 sockets.
    #[arg(long, default_value_t = 1000, value_parser = clap::value_parser!(u64).range(1..))]
    markets_per_connection: u64,

    /// Hard cap on websockets. Kalshi allows 200 concurrent connections per
    /// user on the Basic tier; past this cap, markets overflow onto the
    /// least-loaded socket.
    #[arg(long, default_value_t = 180, value_parser = clap::value_parser!(u64).range(1..))]
    max_connections: u64,

    /// How often to refresh the list of open markets. A full listing takes
    /// about a minute to page through.
    #[arg(long, default_value_t = 120)]
    discovery_interval_secs: u64,

    /// Redundant websockets per set of markets. The published data comes from
    /// one of them; the others are hot standbys (see `market.rs`).
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=4))]
    replicas: u64,

    /// Fail over when a backup socket has delivered an update the source
    /// hasn't for this long.
    #[arg(long, default_value_t = 500)]
    failover_lag_ms: u64,

    /// Proactively reconnect each socket this often (0 disables). Replicas of
    /// a shard recycle at staggered times, so with `--replicas` >= 2 there is
    /// always a live source.
    #[arg(long, default_value_t = 1800)]
    recycle_interval_secs: u64,

    /// How often to write a full snapshot of every book.
    #[arg(long, default_value_t = 60)]
    snapshot_interval_secs: u64,

    /// Only subscribe to the first N markets (for testing).
    #[arg(long)]
    max_markets: Option<usize>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    // reqwest and tungstenite pull in different rustls crypto backends; pick one.
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| anyhow::anyhow!("failed to install rustls crypto provider"))?;

    let args = Args::parse();
    let signer = Arc::new(Signer::from_pem_file(
        &args.api_key_id,
        &args.private_key_path,
    )?);
    let rest = RestClient::new(&args.rest_url)?;
    let target = if args.stdout {
        OutputTarget::Stdout
    } else {
        OutputTarget::Directory {
            dir: args.output_dir.clone(),
            prefix: "kalshi_orderbook".to_owned(),
        }
    };
    let writer = RotatingWriter::open(target, Utc::now())?;

    let replicas = usize::try_from(args.replicas)?;
    let cancel = CancellationToken::new();
    let (events_tx, events_rx) = mpsc::channel(EVENT_CHANNEL_CAPACITY);

    let mut collector_task = tokio::spawn(collector::run(
        Collector::new(
            writer,
            replicas,
            Duration::from_millis(args.failover_lag_ms),
        ),
        events_rx,
        Duration::from_secs(args.snapshot_interval_secs),
        cancel.clone(),
        COLLECTOR_DRAIN_TIMEOUT,
    ));

    let max_markets = args.max_markets;
    let fetch = move || {
        let rest = rest.clone();
        async move {
            let tickers = rest.open_market_tickers().await?;
            Ok(match max_markets {
                Some(n) => tickers.into_iter().take(n).collect(),
                None => tickers,
            })
        }
    };
    let shard_template = ShardConfig {
        shard_id: 0,
        replica: 0,
        ws_url: args.ws_url.clone(),
        signer,
        events: events_tx,
        cancel: cancel.clone(),
        recycle_interval: (args.recycle_interval_secs > 0)
            .then(|| Duration::from_secs(args.recycle_interval_secs)),
        replica_count: replicas,
    };
    let spawn = {
        let template = shard_template.clone();
        move |shard_id, replica, commands| {
            let cfg = ShardConfig {
                shard_id,
                replica,
                ..template.clone()
            };
            tokio::spawn(run_shard(cfg, commands))
        }
    };
    // Only shards (and the spawner inside the universe task) hold event
    // senders, so the collector finishes once they have all shut down.
    drop(shard_template);
    let mut universe_task = tokio::spawn(universe::run(
        fetch,
        spawn,
        replicas,
        ShardLimits {
            max_per_shard: usize::try_from(args.markets_per_connection)?,
            // Each shard uses `replicas` connections out of the budget.
            max_shards: (usize::try_from(args.max_connections)? / replicas).max(1),
        },
        Duration::from_secs(args.discovery_interval_secs),
        cancel.clone(),
    ));

    let mut sigterm = tokio::signal::unix::signal(SignalKind::terminate())
        .context("failed to listen for SIGTERM")?;
    let trigger = tokio::select! {
        result = tokio::signal::ctrl_c() => {
            result.context("failed to listen for SIGINT")?;
            Trigger::Signal("SIGINT")
        }
        _ = sigterm.recv() => Trigger::Signal("SIGTERM"),
        result = &mut collector_task => Trigger::CollectorExited(result),
        _ = &mut universe_task => Trigger::UniverseExited,
    };
    let universe_done = matches!(trigger, Trigger::UniverseExited);
    match &trigger {
        Trigger::Signal(name) => tracing::info!("{name} received; shutting down"),
        Trigger::CollectorExited(_) => tracing::error!("collector exited unexpectedly"),
        Trigger::UniverseExited => tracing::error!("universe task exited unexpectedly"),
    }
    cancel.cancel();
    tokio::spawn(async {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::warn!("second SIGINT; exiting immediately");
            std::process::exit(130);
        }
    });

    // The collector closes the file by itself at most COLLECTOR_DRAIN_TIMEOUT
    // after cancellation, so awaiting it cannot hang.
    let collector_result = match trigger {
        Trigger::CollectorExited(result) => result,
        _ => collector_task.await,
    };
    if !universe_done
        && tokio::time::timeout(SHUTDOWN_TIMEOUT, universe_task)
            .await
            .is_err()
    {
        tracing::warn!("shards did not stop within {SHUTDOWN_TIMEOUT:?}");
    }
    collector_result
        .context("collector task panicked")?
        .context("collector failed")?;
    tracing::info!("shutdown complete");
    Ok(())
}

enum Trigger {
    Signal(&'static str),
    CollectorExited(Result<anyhow::Result<()>, tokio::task::JoinError>),
    UniverseExited,
}
