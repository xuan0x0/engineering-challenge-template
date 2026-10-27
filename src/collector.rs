//! The collector owns every orderbook and the output writer. It is the only
//! task that writes output, so message order in the file matches the order
//! events were received across all connections.
//!
//! Each symbol is carried by several redundant connections; [`MarketState`]
//! decides which one is published (see [`crate::market`]).

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::events::{BookEvent, SourcedEvent};
use crate::market::{MarketState, Output};
use crate::writer::RotatingWriter;

/// How often lagging sources are checked for failover.
pub const LAG_CHECK_INTERVAL: Duration = Duration::from_millis(100);

#[derive(Debug, Default, Clone, Copy)]
struct Stats {
    snapshots: u64,
    deltas: u64,
    orphan_deltas: u64,
    negative_levels: u64,
    invalidations: u64,
    failovers: u64,
}

pub struct Collector {
    markets: HashMap<String, MarketState>,
    /// Symbols whose source is currently behind a backup.
    lagging: HashSet<String>,
    replica_count: usize,
    lag_threshold: chrono::Duration,
    writer: RotatingWriter,
    stats: Stats,
}

impl Collector {
    pub fn new(writer: RotatingWriter, replica_count: usize, lag_threshold: Duration) -> Self {
        Self {
            markets: HashMap::new(),
            lagging: HashSet::new(),
            replica_count,
            lag_threshold: chrono::Duration::from_std(lag_threshold)
                .unwrap_or(chrono::Duration::MAX),
            writer,
            stats: Stats::default(),
        }
    }

    pub fn has_market(&self, symbol: &str) -> bool {
        self.markets.contains_key(symbol)
    }

    pub fn handle(&mut self, sourced: SourcedEvent) -> anyhow::Result<()> {
        let replica = sourced.replica;
        match sourced.event {
            BookEvent::Snapshot {
                symbol,
                received_at,
                exchange_timestamp,
                sequence,
                bids,
                asks,
            } => {
                let market = self
                    .markets
                    .entry(symbol.clone())
                    .or_insert_with(|| MarketState::new(self.replica_count));
                let out = market.on_snapshot(
                    &symbol,
                    replica,
                    &bids,
                    &asks,
                    sequence,
                    received_at,
                    exchange_timestamp,
                );
                self.stats.snapshots += u64::from(out.is_some());
                self.finish(&symbol, out)?;
            }
            BookEvent::Delta {
                symbol,
                received_at,
                exchange_timestamp,
                sequence,
                side,
                price,
                quantity_delta,
            } => {
                let Some(market) = self.markets.get_mut(&symbol) else {
                    // Connections only forward deltas after a snapshot, so this
                    // indicates a bug upstream rather than an exchange issue.
                    tracing::warn!(%symbol, "delta for symbol without a snapshot; dropped");
                    self.stats.orphan_deltas += 1;
                    return Ok(());
                };
                let (out, info) = market.on_delta(
                    &symbol,
                    replica,
                    side,
                    price,
                    quantity_delta,
                    sequence,
                    received_at,
                    exchange_timestamp,
                );
                if info.went_negative {
                    tracing::warn!(%symbol, replica, %side, price, quantity_delta, "level went negative");
                    self.stats.negative_levels += 1;
                }
                self.stats.deltas += u64::from(info.published);
                self.finish(&symbol, out)?;
            }
            BookEvent::Invalidated { symbols } => {
                let now = Utc::now();
                for symbol in symbols {
                    let Some(market) = self.markets.get_mut(&symbol) else {
                        continue;
                    };
                    self.stats.invalidations += 1;
                    let out = market.on_invalidated(&symbol, replica, now);
                    self.stats.failovers += u64::from(out.is_some());
                    self.finish(&symbol, out)?;
                }
            }
            BookEvent::Removed { symbol } => {
                if self
                    .markets
                    .get_mut(&symbol)
                    .is_some_and(|m| m.on_removed(replica))
                {
                    self.markets.remove(&symbol);
                }
                self.lagging.remove(&symbol);
            }
        }
        Ok(())
    }

    /// Fails over every symbol whose source has lagged a backup for longer
    /// than the threshold.
    pub fn check_lagging(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        let candidates: Vec<String> = self.lagging.iter().cloned().collect();
        for symbol in candidates {
            let Some(market) = self.markets.get_mut(&symbol) else {
                self.lagging.remove(&symbol);
                continue;
            };
            let out = market.check_lag(&symbol, now, self.lag_threshold);
            self.stats.failovers += u64::from(out.is_some());
            self.finish(&symbol, out)?;
        }
        Ok(())
    }

    /// Writes a snapshot of every published book. `received_at` is `now` (when
    /// the snapshot was taken); `exchange_timestamp` is that of the book's
    /// latest exchange event.
    pub fn write_all_snapshots(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        for (symbol, market) in &self.markets {
            if let Some(snapshot) = market.snapshot(symbol, now) {
                self.writer.write(&snapshot)?;
            }
        }
        Ok(())
    }

    /// Rotates the output file if the hour changed. Every new file starts with
    /// a full set of snapshots so it can be replayed on its own.
    pub fn maybe_rotate(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        if self.writer.needs_rotation(now) {
            self.writer.rotate(now)?;
            self.write_all_snapshots(now)?;
        }
        Ok(())
    }

    pub fn close(self) -> anyhow::Result<()> {
        self.writer.close()
    }

    /// Writes an output (if any) and refreshes the symbol's lagging status.
    fn finish(&mut self, symbol: &str, out: Option<Output>) -> anyhow::Result<()> {
        match out {
            Some(Output::Snapshot(s)) => self.writer.write(&s)?,
            Some(Output::Update(u)) => self.writer.write(&u)?,
            None => {}
        }
        if self
            .markets
            .get(symbol)
            .is_some_and(MarketState::is_lagging)
        {
            if !self.lagging.contains(symbol) {
                self.lagging.insert(symbol.to_owned());
            }
        } else {
            self.lagging.remove(symbol);
        }
        Ok(())
    }

    fn log_and_reset_stats(&mut self) {
        let s = std::mem::take(&mut self.stats);
        tracing::info!(
            books = self.markets.len(),
            snapshots = s.snapshots,
            deltas = s.deltas,
            orphan_deltas = s.orphan_deltas,
            negative_levels = s.negative_levels,
            invalidations = s.invalidations,
            failovers = s.failovers,
            "collector stats since last periodic snapshot"
        );
    }
}

/// Runs until every event sender has been dropped, then flushes and closes
/// the output. Connections drop their senders on shutdown, so remaining
/// in-flight events are drained before the file is closed. If some sender is
/// still alive `drain_timeout` after `cancel` fires (e.g. a wedged socket), the
/// file is closed anyway so buffered data is never lost.
pub async fn run(
    mut collector: Collector,
    mut events: mpsc::Receiver<SourcedEvent>,
    snapshot_interval: Duration,
    cancel: CancellationToken,
    drain_timeout: Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(snapshot_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick fires immediately
    let mut lag_ticker = tokio::time::interval(LAG_CHECK_INTERVAL);
    lag_ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let drain_deadline = async {
        cancel.cancelled().await;
        tokio::time::sleep(drain_timeout).await;
    };
    tokio::pin!(drain_deadline);

    let result = loop {
        tokio::select! {
            () = &mut drain_deadline => {
                tracing::warn!("event senders still open after drain timeout; closing output");
                break Ok(());
            }
            maybe_event = events.recv() => {
                let Some(event) = maybe_event else { break Ok(()) };
                let now = Utc::now();
                if let Err(e) = collector.maybe_rotate(now).and_then(|()| collector.handle(event)) {
                    break Err(e);
                }
            }
            _ = lag_ticker.tick() => {
                if let Err(e) = collector.check_lagging(Utc::now()) {
                    break Err(e);
                }
            }
            _ = ticker.tick() => {
                let now = Utc::now();
                if let Err(e) = collector.maybe_rotate(now)
                    .and_then(|()| collector.write_all_snapshots(now))
                {
                    break Err(e);
                }
                collector.log_and_reset_stats();
            }
        }
    };

    // Always attempt to flush what we have, even after a write error.
    let closed = collector.close();
    result.and(closed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side;
    use crate::events::Level;
    use crate::util::dtos::challenge_orderbook::{
        AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
    };
    use crate::util::{AlignedSbeReader, OrderbookSchema};
    use crate::writer::OutputTarget;

    #[derive(Debug, PartialEq)]
    enum Out {
        Snap(SnapshotDomain),
        Upd(PriceLevelUpdateDomain),
    }

    fn ts(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).unwrap()
    }

    fn read_all(path: &std::path::Path) -> Vec<Out> {
        let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path).unwrap();
        let mut out = Vec::new();
        while let Some(msg) = reader.read_msg().unwrap() {
            out.push(match msg {
                AnyMessage::Snapshot(d) => Out::Snap(SnapshotDomain::try_from_decoder(d).unwrap()),
                AnyMessage::PriceLevelUpdate(d) => {
                    Out::Upd(PriceLevelUpdateDomain::try_from_decoder(d).unwrap())
                }
                _ => panic!("unknown message"),
            });
        }
        out
    }

    fn on(replica: usize, event: BookEvent) -> SourcedEvent {
        SourcedEvent { replica, event }
    }

    fn snapshot_event(symbol: &str, at: DateTime<Utc>) -> SourcedEvent {
        on(0, snapshot_on(symbol, at, 1))
    }

    fn delta_event(symbol: &str, at: DateTime<Utc>, delta: i64) -> SourcedEvent {
        on(0, delta_on(symbol, at, delta, 2))
    }

    fn snapshot_on(symbol: &str, at: DateTime<Utc>, sequence: u64) -> BookEvent {
        BookEvent::Snapshot {
            symbol: symbol.to_owned(),
            received_at: at,
            exchange_timestamp: at,
            sequence: Some(sequence),
            bids: vec![Level {
                price: 400_000,
                quantity: 10_000_000,
            }],
            asks: vec![Level {
                price: 600_000,
                quantity: 5_000_000,
            }],
        }
    }

    fn delta_on(symbol: &str, at: DateTime<Utc>, delta: i64, sequence: u64) -> BookEvent {
        BookEvent::Delta {
            symbol: symbol.to_owned(),
            received_at: at,
            exchange_timestamp: at,
            sequence: Some(sequence),
            side: Side::Buy,
            price: 400_000,
            quantity_delta: delta,
        }
    }

    fn new_collector(dir: &std::path::Path, at: DateTime<Utc>) -> Collector {
        let target = OutputTarget::Directory {
            dir: dir.to_owned(),
            prefix: "test".to_owned(),
        };
        Collector::new(
            RotatingWriter::open(target, at).unwrap(),
            2,
            Duration::from_millis(500),
        )
    }

    #[test]
    fn writes_snapshot_then_absolute_quantity_updates() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ts(1_700_000_000);
        let mut collector = new_collector(dir.path(), t0);
        let path = collector.writer.path().to_owned();

        collector.handle(snapshot_event("A", t0)).unwrap();
        collector.handle(delta_event("A", t0, -3_000_000)).unwrap();
        collector.handle(delta_event("A", t0, -7_000_000)).unwrap();
        collector.handle(delta_event("UNKNOWN", t0, 1)).unwrap();
        collector.close().unwrap();

        let out = read_all(&path);
        assert_eq!(out.len(), 3, "orphan delta must not be written");
        let Out::Snap(snap) = &out[0] else {
            panic!("expected snapshot")
        };
        assert_eq!(snap.symbol, "A");
        let quantities: Vec<i64> = out[1..]
            .iter()
            .map(|o| match o {
                Out::Upd(u) => u.quantity,
                Out::Snap(_) => panic!("expected update"),
            })
            .collect();
        assert_eq!(quantities, vec![7_000_000, 0]);
    }

    #[test]
    fn removed_symbol_is_excluded_from_periodic_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ts(1_700_000_000);
        let mut collector = new_collector(dir.path(), t0);
        let path = collector.writer.path().to_owned();

        collector.handle(snapshot_event("A", t0)).unwrap();
        collector.handle(snapshot_event("B", t0)).unwrap();
        let removed = |r| {
            on(
                r,
                BookEvent::Removed {
                    symbol: "A".to_owned(),
                },
            )
        };
        collector.handle(removed(0)).unwrap();
        assert!(
            collector.has_market("A"),
            "kept until every replica unsubscribes"
        );
        collector.handle(removed(1)).unwrap();
        assert!(!collector.has_market("A"));
        collector.write_all_snapshots(t0).unwrap();
        collector.close().unwrap();

        let out = read_all(&path);
        let periodic: Vec<&str> = out[2..]
            .iter()
            .map(|o| match o {
                Out::Snap(s) => s.symbol.as_str(),
                Out::Upd(_) => panic!("expected snapshot"),
            })
            .collect();
        assert_eq!(periodic, vec!["B"]);
    }

    #[test]
    fn rotation_starts_new_file_with_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ts(1_700_000_000);
        let mut collector = new_collector(dir.path(), t0);
        collector.handle(snapshot_event("A", t0)).unwrap();

        let t1 = t0 + chrono::Duration::hours(1);
        collector.maybe_rotate(t1).unwrap();
        let second = collector.writer.path().to_owned();
        collector.close().unwrap();

        let out = read_all(&second);
        assert_eq!(out.len(), 1);
        let Out::Snap(snap) = &out[0] else {
            panic!("expected snapshot")
        };
        assert_eq!((snap.symbol.as_str(), snap.received_at), ("A", t1));
    }

    #[test]
    fn failover_writes_backup_snapshot_then_backup_deltas() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ts(1_700_000_000);
        let mut collector = new_collector(dir.path(), t0);
        let path = collector.writer.path().to_owned();

        collector.handle(on(0, snapshot_on("A", t0, 1))).unwrap();
        collector.handle(on(1, snapshot_on("A", t0, 1))).unwrap();
        // Replica 1 sees an update that replica 0 never delivers.
        collector
            .handle(on(1, delta_on("A", t0, 1_000_000, 2)))
            .unwrap();
        collector
            .handle(on(
                0,
                BookEvent::Invalidated {
                    symbols: vec!["A".to_owned()],
                },
            ))
            .unwrap();
        collector
            .handle(on(1, delta_on("A", t0, 1_000_000, 3)))
            .unwrap();
        collector.close().unwrap();

        let out = read_all(&path);
        assert_eq!(
            out.len(),
            3,
            "initial snapshot, failover snapshot, one update"
        );
        let Out::Snap(failover) = &out[1] else {
            panic!("expected failover snapshot")
        };
        assert_eq!(failover.bids[0].quantity, 11_000_000);
        let Out::Upd(update) = &out[2] else {
            panic!("expected update")
        };
        assert_eq!(update.quantity, 12_000_000);
    }

    #[test]
    fn lagging_source_fails_over_on_check() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ts(1_700_000_000);
        let mut collector = new_collector(dir.path(), t0);
        let path = collector.writer.path().to_owned();

        collector.handle(on(0, snapshot_on("A", t0, 1))).unwrap();
        collector.handle(on(1, snapshot_on("A", t0, 1))).unwrap();
        collector
            .handle(on(1, delta_on("A", t0, 1_000_000, 2)))
            .unwrap();
        collector
            .check_lagging(t0 + chrono::Duration::milliseconds(100))
            .unwrap();
        collector
            .check_lagging(t0 + chrono::Duration::milliseconds(600))
            .unwrap();
        collector.close().unwrap();

        let out = read_all(&path);
        assert_eq!(out.len(), 2, "initial snapshot + failover snapshot");
        assert!(matches!(&out[1], Out::Snap(s) if s.bids[0].quantity == 11_000_000));
    }

    #[tokio::test]
    async fn run_closes_after_drain_timeout_even_if_a_sender_lingers() {
        let dir = tempfile::tempdir().unwrap();
        let collector = new_collector(dir.path(), Utc::now());
        let path = collector.writer.path().to_owned();
        let (tx, rx) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let handle = tokio::spawn(run(
            collector,
            rx,
            Duration::from_secs(3600),
            cancel.clone(),
            Duration::from_millis(50),
        ));
        tx.send(snapshot_event("A", Utc::now())).await.unwrap();
        cancel.cancel();
        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("collector must not wait forever")
            .unwrap()
            .unwrap();
        assert_eq!(read_all(&path).len(), 1);
        drop(tx);
    }

    #[test]
    fn periodic_snapshot_keeps_last_exchange_timestamp() {
        let dir = tempfile::tempdir().unwrap();
        let t0 = ts(1_700_000_000);
        let mut collector = new_collector(dir.path(), t0);
        let path = collector.writer.path().to_owned();
        collector.handle(snapshot_event("A", t0)).unwrap();
        let t1 = t0 + chrono::Duration::seconds(5);
        collector.handle(delta_event("A", t1, 1)).unwrap();
        let t2 = t0 + chrono::Duration::seconds(60);
        collector.write_all_snapshots(t2).unwrap();
        collector.close().unwrap();

        let Out::Snap(periodic) = read_all(&path).pop().unwrap() else {
            panic!("expected snapshot");
        };
        assert_eq!(
            (periodic.received_at, periodic.exchange_timestamp),
            (t2, t1)
        );
    }

    #[tokio::test]
    async fn run_drains_events_and_closes_when_senders_drop() {
        let dir = tempfile::tempdir().unwrap();
        let collector = new_collector(dir.path(), Utc::now());
        let path = collector.writer.path().to_owned();
        let (tx, rx) = mpsc::channel(16);
        let handle = tokio::spawn(run(
            collector,
            rx,
            Duration::from_secs(3600),
            CancellationToken::new(),
            Duration::from_secs(1),
        ));

        let now = Utc::now();
        tx.send(snapshot_event("A", now)).await.unwrap();
        tx.send(delta_event("A", now, 1_000_000)).await.unwrap();
        drop(tx);
        handle.await.unwrap().unwrap();

        assert_eq!(read_all(&path).len(), 2);
    }
}
