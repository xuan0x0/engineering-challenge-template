//! The collector owns every orderbook and the output writer. It is the only
//! task that writes output, so message order in the file matches the order
//! events were received across all connections.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::events::BookEvent;
use crate::orderbook::OrderBook;
use crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain;
use crate::writer::RotatingWriter;

#[derive(Debug, Default, Clone, Copy)]
struct Stats {
    snapshots: u64,
    deltas: u64,
    orphan_deltas: u64,
    negative_levels: u64,
}

struct TrackedBook {
    book: OrderBook,
    /// Exchange timestamp of the latest event applied to this book. Periodic
    /// snapshots carry it rather than inventing one.
    exchange_timestamp: DateTime<Utc>,
}

pub struct Collector {
    books: HashMap<String, TrackedBook>,
    writer: RotatingWriter,
    stats: Stats,
}

impl Collector {
    pub fn new(writer: RotatingWriter) -> Self {
        Self {
            books: HashMap::new(),
            writer,
            stats: Stats::default(),
        }
    }

    pub fn book(&self, symbol: &str) -> Option<&OrderBook> {
        self.books.get(symbol).map(|t| &t.book)
    }

    pub fn handle(&mut self, event: BookEvent) -> anyhow::Result<()> {
        match event {
            BookEvent::Snapshot {
                symbol,
                received_at,
                exchange_timestamp,
                bids,
                asks,
            } => {
                let book = OrderBook::from_levels(&bids, &asks);
                self.writer
                    .write(&book.to_snapshot(&symbol, received_at, exchange_timestamp))?;
                self.books.insert(
                    symbol,
                    TrackedBook {
                        book,
                        exchange_timestamp,
                    },
                );
                self.stats.snapshots += 1;
            }
            BookEvent::Delta {
                symbol,
                received_at,
                exchange_timestamp,
                side,
                price,
                quantity_delta,
            } => {
                let Some(tracked) = self.books.get_mut(&symbol) else {
                    // Connections only forward deltas after a snapshot, so this
                    // indicates a bug upstream rather than an exchange issue.
                    tracing::warn!(%symbol, "delta for symbol without a snapshot; dropped");
                    self.stats.orphan_deltas += 1;
                    return Ok(());
                };
                tracked.exchange_timestamp = exchange_timestamp;
                let outcome = tracked.book.apply_delta(side, price, quantity_delta);
                if outcome.went_negative {
                    tracing::warn!(%symbol, %side, price, quantity_delta, "level went negative");
                    self.stats.negative_levels += 1;
                }
                self.writer.write(&PriceLevelUpdateDomain {
                    backtest_timestamp: received_at,
                    exchange_timestamp,
                    price_level: price,
                    quantity: outcome.quantity,
                    side,
                    symbol,
                })?;
                self.stats.deltas += 1;
            }
            BookEvent::Removed { symbol } => {
                self.books.remove(&symbol);
            }
        }
        Ok(())
    }

    /// Writes a snapshot of every book. `received_at` is `now` (when the
    /// snapshot was taken); `exchange_timestamp` is that of the book's latest
    /// exchange event.
    pub fn write_all_snapshots(&mut self, now: DateTime<Utc>) -> anyhow::Result<()> {
        for (symbol, tracked) in &self.books {
            self.writer.write(&tracked.book.to_snapshot(
                symbol,
                now,
                tracked.exchange_timestamp,
            ))?;
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

    fn log_and_reset_stats(&mut self) {
        let s = std::mem::take(&mut self.stats);
        tracing::info!(
            books = self.books.len(),
            snapshots = s.snapshots,
            deltas = s.deltas,
            orphan_deltas = s.orphan_deltas,
            negative_levels = s.negative_levels,
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
    mut events: mpsc::Receiver<BookEvent>,
    snapshot_interval: Duration,
    cancel: CancellationToken,
    drain_timeout: Duration,
) -> anyhow::Result<()> {
    let mut ticker = tokio::time::interval(snapshot_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await; // the first tick fires immediately
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
    use crate::util::dtos::challenge_orderbook::{AnyMessage, SnapshotDomain};
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

    fn snapshot_event(symbol: &str, at: DateTime<Utc>) -> BookEvent {
        BookEvent::Snapshot {
            symbol: symbol.to_owned(),
            received_at: at,
            exchange_timestamp: at,
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

    fn delta_event(symbol: &str, at: DateTime<Utc>, delta: i64) -> BookEvent {
        BookEvent::Delta {
            symbol: symbol.to_owned(),
            received_at: at,
            exchange_timestamp: at,
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
        Collector::new(RotatingWriter::open(target, at).unwrap())
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
        collector
            .handle(BookEvent::Removed {
                symbol: "A".to_owned(),
            })
            .unwrap();
        assert!(collector.book("A").is_none());
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
