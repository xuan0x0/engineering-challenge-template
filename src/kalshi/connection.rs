//! One websocket shard: connects, keeps its markets subscribed, forwards book
//! events to the collector, and reconnects on any failure.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use rsa::rand_core::{OsRng, RngCore};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

use super::auth::Signer;
use super::messages::Incoming;
use super::subscription::{Fault, SubscriptionState};
use crate::events::BookEvent;
use crate::universe::ShardCmd;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const PING_INTERVAL: Duration = Duration::from_secs(10);
/// No frames at all (including pongs) for this long means the socket is dead.
const STALE_TIMEOUT: Duration = Duration::from_secs(30);
const CLOSE_SEND_TIMEOUT: Duration = Duration::from_secs(1);
const CLOSE_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);
const MIN_BACKOFF: Duration = Duration::from_millis(250);
const MAX_BACKOFF: Duration = Duration::from_secs(30);
/// A second resync within this window means resyncing isn't fixing the
/// problem; drop the connection and back off instead.
const MIN_RESYNC_INTERVAL: Duration = Duration::from_secs(10);
/// Delay between initial connects of consecutive shards, so ~150 sockets
/// don't all hit the exchange in the same instant.
const STARTUP_STAGGER: Duration = Duration::from_millis(100);

#[derive(Clone)]
pub struct ShardConfig {
    pub shard_id: usize,
    pub ws_url: String,
    pub signer: Arc<Signer>,
    pub events: mpsc::Sender<BookEvent>,
    pub cancel: CancellationToken,
}

enum SessionEnd {
    /// Cancelled, the command channel closed, or the collector is gone.
    Shutdown,
    Disconnected(String),
}

pub async fn run_shard(cfg: ShardConfig, mut commands: mpsc::Receiver<ShardCmd>) {
    let shard = cfg.shard_id;
    let mut state = SubscriptionState::default();
    let mut backoff = MIN_BACKOFF;

    let stagger = STARTUP_STAGGER * u32::try_from(shard).unwrap_or(u32::MAX);
    tokio::select! {
        _ = cfg.cancel.cancelled() => return,
        _ = tokio::time::sleep(stagger) => {}
    }

    loop {
        let connected = tokio::select! {
            _ = cfg.cancel.cancelled() => break,
            result = connect(&cfg) => result,
        };
        let end = match connected {
            Ok(ws) => {
                tracing::info!(shard, "connected");
                state.reset_connection();
                session(&cfg, ws, &mut state, &mut commands, &mut backoff).await
            }
            Err(e) => SessionEnd::Disconnected(format!("connect failed: {e:#}")),
        };
        match end {
            SessionEnd::Shutdown => break,
            SessionEnd::Disconnected(reason) => {
                tracing::warn!(shard, %reason, retry_in = ?backoff, "disconnected");
            }
        }
        tokio::select! {
            _ = cfg.cancel.cancelled() => break,
            _ = tokio::time::sleep(with_jitter(backoff)) => {}
        }
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
    tracing::info!(shard, "shard stopped");
}

/// Adds up to 50% random jitter so shards that failed together don't retry in
/// lockstep.
fn with_jitter(base: Duration) -> Duration {
    let fraction = f64::from(OsRng.next_u32()) / f64::from(u32::MAX);
    base.mul_f64(1.0 + 0.5 * fraction)
}

async fn connect(cfg: &ShardConfig) -> anyhow::Result<WsStream> {
    let mut request = cfg.ws_url.as_str().into_client_request()?;
    let path = request.uri().path().to_owned();
    let auth = cfg
        .signer
        .headers(Utc::now().timestamp_millis(), "GET", &path);
    let headers = request.headers_mut();
    headers.insert("KALSHI-ACCESS-KEY", HeaderValue::from_str(&auth.key)?);
    headers.insert(
        "KALSHI-ACCESS-SIGNATURE",
        HeaderValue::from_str(&auth.signature)?,
    );
    headers.insert(
        "KALSHI-ACCESS-TIMESTAMP",
        HeaderValue::from_str(&auth.timestamp)?,
    );

    let (ws, _response) = tokio::time::timeout(
        CONNECT_TIMEOUT,
        tokio_tungstenite::connect_async_with_config(request, None, true),
    )
    .await
    .context("connect timed out")??;
    Ok(ws)
}

async fn session(
    cfg: &ShardConfig,
    ws: WsStream,
    state: &mut SubscriptionState,
    commands: &mut mpsc::Receiver<ShardCmd>,
    backoff: &mut Duration,
) -> SessionEnd {
    let shard = cfg.shard_id;
    let (mut sink, mut stream) = ws.split();
    let mut ping = tokio::time::interval(PING_INTERVAL);
    let mut last_frame = Instant::now();
    let mut last_resync: Option<Instant> = None;

    macro_rules! send_all {
        ($cmds:expr) => {
            for text in $cmds {
                if let Err(e) = sink.send(Message::Text(text.into())).await {
                    return SessionEnd::Disconnected(format!("send failed: {e}"));
                }
            }
        };
    }

    // Apply commands queued while we were disconnected before subscribing, so
    // the first subscribe doesn't include markets that have since closed.
    while let Ok(cmd) = commands.try_recv() {
        if !apply_command(cfg, state, cmd).await {
            return SessionEnd::Shutdown;
        }
    }
    send_all!(state.reconcile());

    loop {
        tokio::select! {
            biased;
            _ = cfg.cancel.cancelled() => {
                close(cfg, &mut sink, &mut stream, state).await;
                return SessionEnd::Shutdown;
            }
            frame = stream.next() => {
                // Stamp before any parsing so the timestamp reflects arrival.
                let received_at = Utc::now();
                let text = match frame {
                    None => return SessionEnd::Disconnected("stream ended".to_owned()),
                    Some(Err(e)) => return SessionEnd::Disconnected(format!("read failed: {e}")),
                    Some(Ok(Message::Close(frame))) => {
                        return SessionEnd::Disconnected(format!("closed by server: {frame:?}"));
                    }
                    Some(Ok(Message::Text(text))) => text,
                    Some(Ok(_)) => {
                        last_frame = Instant::now();
                        continue;
                    }
                };
                last_frame = Instant::now();
                let was_subscribed = state.is_subscribed();
                let mut outgoing = Vec::new();
                match handle_text(text.as_str(), received_at, state, &mut outgoing) {
                    Ok(Some(event)) => {
                        if cfg.events.send(event).await.is_err() {
                            return SessionEnd::Shutdown;
                        }
                    }
                    Ok(None) => {}
                    Err(Fault::Resync(reason)) => {
                        if last_resync.is_some_and(|t| t.elapsed() < MIN_RESYNC_INTERVAL) {
                            return SessionEnd::Disconnected(format!("repeated resyncs: {reason}"));
                        }
                        tracing::warn!(shard, %reason, "resubscribing");
                        last_resync = Some(Instant::now());
                        outgoing.extend(state.resync());
                    }
                    Err(Fault::Rejected(reason)) => {
                        return SessionEnd::Disconnected(format!("subscribe rejected: {reason}"));
                    }
                }
                if !was_subscribed && state.is_subscribed() {
                    tracing::info!(shard, markets = state.desired_len(), "subscribed");
                    *backoff = MIN_BACKOFF;
                }
                send_all!(outgoing);
            }
            cmd = commands.recv() => {
                let Some(cmd) = cmd else {
                    close(cfg, &mut sink, &mut stream, state).await;
                    return SessionEnd::Shutdown;
                };
                if !apply_command(cfg, state, cmd).await {
                    return SessionEnd::Shutdown;
                }
                send_all!(state.reconcile());
            }
            _ = ping.tick() => {
                if last_frame.elapsed() > STALE_TIMEOUT {
                    return SessionEnd::Disconnected("no frames received; socket stale".to_owned());
                }
                if let Err(e) = sink.send(Message::Ping(Default::default())).await {
                    return SessionEnd::Disconnected(format!("ping failed: {e}"));
                }
            }
        }
    }
}

/// Updates the desired market set. Returns false if the collector is gone.
async fn apply_command(cfg: &ShardConfig, state: &mut SubscriptionState, cmd: ShardCmd) -> bool {
    match cmd {
        ShardCmd::Add(tickers) => state.add(tickers),
        ShardCmd::Remove(tickers) => {
            for event in state.remove(tickers) {
                if cfg.events.send(event).await.is_err() {
                    return false;
                }
            }
        }
    }
    true
}

/// Sends Close (bounded, in case the socket is wedged) and drains what's left.
async fn close(
    cfg: &ShardConfig,
    sink: &mut SplitSink<WsStream, Message>,
    stream: &mut futures_util::stream::SplitStream<WsStream>,
    state: &mut SubscriptionState,
) {
    match tokio::time::timeout(CLOSE_SEND_TIMEOUT, sink.send(Message::Close(None))).await {
        Ok(Ok(())) => drain_after_close(cfg, stream, state).await,
        Ok(Err(e)) => tracing::warn!(shard = cfg.shard_id, error = %e, "close failed"),
        Err(_) => tracing::warn!(shard = cfg.shard_id, "close timed out"),
    }
}

fn handle_text(
    text: &str,
    received_at: chrono::DateTime<Utc>,
    state: &mut SubscriptionState,
    outgoing: &mut Vec<String>,
) -> Result<Option<BookEvent>, Fault> {
    let incoming: Incoming = serde_json::from_str(text).map_err(|e| {
        let preview: String = text.chars().take(200).collect();
        Fault::Resync(format!("unparseable message ({e}): {preview}"))
    })?;
    state.on_message(incoming, received_at, outgoing)
}

/// After sending Close, keep forwarding book events until the server
/// acknowledges the close (or a short timeout), so nothing in flight is lost.
async fn drain_after_close(
    cfg: &ShardConfig,
    stream: &mut futures_util::stream::SplitStream<WsStream>,
    state: &mut SubscriptionState,
) {
    let deadline = Instant::now() + CLOSE_DRAIN_TIMEOUT;
    while let Ok(Some(Ok(frame))) = tokio::time::timeout_at(deadline, stream.next()).await {
        let received_at = Utc::now();
        let Message::Text(text) = frame else {
            if matches!(frame, Message::Close(_)) {
                break;
            }
            continue;
        };
        if let Ok(Some(event)) = handle_text(text.as_str(), received_at, state, &mut Vec::new())
            && cfg.events.send(event).await.is_err()
        {
            break;
        }
    }
    tracing::info!(shard = cfg.shard_id, "websocket closed");
}
