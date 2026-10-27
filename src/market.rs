//! Per-symbol state across redundant connections, and choice of the "source"
//! connection whose data is published.
//!
//! Every replica keeps its own book. Only the source's events are published.
//! The source is switched when it becomes untrustworthy (invalidated) or when
//! it falls behind: another replica has delivered a delta with a later exchange
//! sequence
//! and the source hasn't caught up within the lag threshold. Each switch
//! publishes a snapshot of the new source's book, since replicas may differ
//! by dropped or delayed messages.

use chrono::{DateTime, Utc};

use crate::Side;
use crate::events::{Level, ReplicaId};
use crate::orderbook::OrderBook;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

#[derive(Debug, Clone, PartialEq)]
pub enum Output {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
}

#[derive(Debug, Clone, Default)]
struct ReplicaView {
    /// `None` until a snapshot arrives, and again after invalidation.
    book: Option<OrderBook>,
    /// Latest sequence this replica's book reflects (snapshot or delta).
    sequence: Option<u64>,
    /// Latest sequence of a delta. Only deltas prove a replica is ahead: a
    /// snapshot's sequence covers "everything so far", so a fresh snapshot
    /// can carry a higher sequence than the source's last delta even when no
    /// change has happened since.
    delta_sequence: Option<u64>,
    exchange_timestamp: Option<DateTime<Utc>>,
    removed: bool,
}

#[derive(Debug, Clone)]
pub struct MarketState {
    replicas: Vec<ReplicaView>,
    source: ReplicaId,
    /// Some replica has unsubscribed: the market is closing, publish nothing.
    closing: bool,
    /// When a backup first got ahead of the source.
    lagging_since: Option<DateTime<Utc>>,
}

/// What happened when a delta was applied, for stats and logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeltaInfo {
    pub went_negative: bool,
    pub published: bool,
}

impl MarketState {
    pub fn new(replica_count: usize) -> Self {
        assert!(replica_count > 0, "need at least one replica");
        Self {
            replicas: vec![ReplicaView::default(); replica_count],
            source: 0,
            closing: false,
            lagging_since: None,
        }
    }

    pub fn source(&self) -> ReplicaId {
        self.source
    }

    pub fn is_lagging(&self) -> bool {
        self.lagging_since.is_some()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_snapshot(
        &mut self,
        symbol: &str,
        replica: ReplicaId,
        bids: &[Level],
        asks: &[Level],
        sequence: Option<u64>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> Option<Output> {
        let view = self.replicas.get_mut(replica)?;
        if view.removed {
            return None;
        }
        view.book = Some(OrderBook::from_levels(bids, asks));
        view.sequence = sequence;
        view.delta_sequence = None;
        view.exchange_timestamp = Some(exchange_timestamp);

        // A replica's fresh snapshot is published if it is the source, or if
        // the source currently has nothing trustworthy to publish.
        let claim = replica == self.source || !self.is_valid(self.source);
        let out = (claim && !self.closing).then(|| {
            self.source = replica;
            Output::Snapshot(self.source_snapshot(symbol, received_at))
        });
        self.update_lag(received_at);
        out
    }

    #[allow(clippy::too_many_arguments)]
    pub fn on_delta(
        &mut self,
        symbol: &str,
        replica: ReplicaId,
        side: Side,
        price: i64,
        quantity_delta: i64,
        sequence: Option<u64>,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> (Option<Output>, DeltaInfo) {
        let Some(view) = self.replicas.get_mut(replica) else {
            return (None, DeltaInfo::default());
        };
        let Some(book) = view.book.as_mut() else {
            return (None, DeltaInfo::default());
        };
        let outcome = book.apply_delta(side, price, quantity_delta);
        view.sequence = sequence.or(view.sequence);
        view.delta_sequence = sequence.or(view.delta_sequence);
        view.exchange_timestamp = Some(exchange_timestamp);

        let publish = replica == self.source && !self.closing;
        let out = publish.then(|| {
            Output::Update(PriceLevelUpdateDomain {
                backtest_timestamp: received_at,
                exchange_timestamp,
                price_level: price,
                quantity: outcome.quantity,
                side,
                symbol: symbol.to_owned(),
            })
        });
        self.update_lag(received_at);
        let info = DeltaInfo {
            went_negative: outcome.went_negative,
            published: out.is_some(),
        };
        (out, info)
    }

    /// The replica's book is no longer trustworthy. If it was the source,
    /// fails over to the most up-to-date valid replica.
    pub fn on_invalidated(
        &mut self,
        symbol: &str,
        replica: ReplicaId,
        now: DateTime<Utc>,
    ) -> Option<Output> {
        let view = self.replicas.get_mut(replica)?;
        view.book = None;
        view.sequence = None;
        view.delta_sequence = None;
        let out = if replica == self.source {
            self.switch_to_best(symbol, now)
        } else {
            None
        };
        self.update_lag(now);
        out
    }

    /// Returns true once every replica has unsubscribed, meaning the market
    /// state can be dropped.
    pub fn on_removed(&mut self, replica: ReplicaId) -> bool {
        if let Some(view) = self.replicas.get_mut(replica) {
            *view = ReplicaView {
                removed: true,
                ..ReplicaView::default()
            };
        }
        self.closing = true;
        self.lagging_since = None;
        self.replicas.iter().all(|v| v.removed)
    }

    /// Fails over if the source has been behind a backup for at least
    /// `threshold`.
    pub fn check_lag(
        &mut self,
        symbol: &str,
        now: DateTime<Utc>,
        threshold: chrono::Duration,
    ) -> Option<Output> {
        let since = self.lagging_since?;
        if now - since < threshold {
            return None;
        }
        let out = self.switch_to_best(symbol, now);
        self.update_lag(now);
        out
    }

    /// The published book, for periodic snapshots. `None` while the market is
    /// closing or no replica has a trustworthy book.
    pub fn snapshot(&self, symbol: &str, now: DateTime<Utc>) -> Option<SnapshotDomain> {
        if self.closing || !self.is_valid(self.source) {
            return None;
        }
        Some(self.source_snapshot(symbol, now))
    }

    fn is_valid(&self, replica: ReplicaId) -> bool {
        self.replicas.get(replica).is_some_and(|v| v.book.is_some())
    }

    fn source_snapshot(&self, symbol: &str, received_at: DateTime<Utc>) -> SnapshotDomain {
        let view = &self.replicas[self.source];
        let book = view.book.clone().unwrap_or_default();
        book.to_snapshot(
            symbol,
            received_at,
            view.exchange_timestamp.unwrap_or(received_at),
        )
    }

    /// Switches to the valid replica (other than the source) with the highest
    /// sequence, publishing its snapshot. Stays put if there is none.
    fn switch_to_best(&mut self, symbol: &str, now: DateTime<Utc>) -> Option<Output> {
        if self.closing {
            return None;
        }
        let best = self
            .replicas
            .iter()
            .enumerate()
            .filter(|&(i, v)| i != self.source && v.book.is_some())
            .max_by_key(|&(_, v)| v.sequence)
            .map(|(i, _)| i)?;
        tracing::info!(%symbol, from = self.source, to = best, "switching source replica");
        self.source = best;
        Some(Output::Snapshot(self.source_snapshot(symbol, now)))
    }

    fn update_lag(&mut self, now: DateTime<Utc>) {
        let source_seq = self.replicas[self.source].sequence;
        let backup_ahead = self.is_valid(self.source)
            && self.replicas.iter().enumerate().any(|(i, v)| {
                i != self.source && v.book.is_some() && v.delta_sequence > source_seq
            });
        if !backup_ahead || self.closing {
            self.lagging_since = None;
        } else if self.lagging_since.is_none() {
            self.lagging_since = Some(now);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SYM: &str = "M";

    fn ts(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(1_700_000_000_000 + ms).unwrap()
    }

    fn lvl(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    fn snap(
        m: &mut MarketState,
        replica: ReplicaId,
        qty: i64,
        seq: u64,
        at: i64,
    ) -> Option<Output> {
        m.on_snapshot(
            SYM,
            replica,
            &[lvl(400_000, qty)],
            &[],
            Some(seq),
            ts(at),
            ts(at),
        )
    }

    fn delta(m: &mut MarketState, replica: ReplicaId, d: i64, seq: u64, at: i64) -> Option<Output> {
        m.on_delta(
            SYM,
            replica,
            Side::Buy,
            400_000,
            d,
            Some(seq),
            ts(at),
            ts(at),
        )
        .0
    }

    fn snapshot_qty(out: Option<Output>) -> i64 {
        match out {
            Some(Output::Snapshot(s)) => s.bids.first().map_or(0, |l| l.quantity),
            other => panic!("expected snapshot, got {other:?}"),
        }
    }

    fn update_qty(out: Option<Output>) -> i64 {
        match out {
            Some(Output::Update(u)) => u.quantity,
            other => panic!("expected update, got {other:?}"),
        }
    }

    #[test]
    fn only_source_events_are_published() {
        let mut m = MarketState::new(2);
        assert_eq!(snapshot_qty(snap(&mut m, 0, 10, 1, 0)), 10);
        assert!(
            snap(&mut m, 1, 10, 1, 1).is_none(),
            "backup snapshot not published"
        );
        assert_eq!(update_qty(delta(&mut m, 0, 5, 2, 2)), 15);
        assert!(
            delta(&mut m, 1, 5, 2, 3).is_none(),
            "backup delta not published"
        );
        assert_eq!(m.source(), 0);
    }

    #[test]
    fn first_snapshot_from_any_replica_claims_source() {
        let mut m = MarketState::new(2);
        assert_eq!(snapshot_qty(snap(&mut m, 1, 7, 1, 0)), 7);
        assert_eq!(m.source(), 1);
        assert!(snap(&mut m, 0, 7, 1, 1).is_none());
    }

    #[test]
    fn invalidated_source_fails_over_with_backup_snapshot() {
        let mut m = MarketState::new(2);
        snap(&mut m, 0, 10, 1, 0);
        snap(&mut m, 1, 10, 1, 0);
        // The backup saw an update the source missed before disconnecting.
        delta(&mut m, 1, 3, 2, 1);
        assert_eq!(snapshot_qty(m.on_invalidated(SYM, 0, ts(2))), 13);
        assert_eq!(m.source(), 1);
        assert_eq!(update_qty(delta(&mut m, 1, 1, 3, 3)), 14);
        // The old source reconnects and resnapshots: it becomes a backup.
        assert!(snap(&mut m, 0, 14, 3, 4).is_none());
    }

    #[test]
    fn invalidating_a_backup_does_not_switch() {
        let mut m = MarketState::new(2);
        snap(&mut m, 0, 10, 1, 0);
        snap(&mut m, 1, 10, 1, 0);
        assert!(m.on_invalidated(SYM, 1, ts(1)).is_none());
        assert_eq!(m.source(), 0);
    }

    #[test]
    fn invalidated_with_no_alternative_stops_publishing_until_resnapshot() {
        let mut m = MarketState::new(1);
        snap(&mut m, 0, 10, 1, 0);
        assert!(m.on_invalidated(SYM, 0, ts(1)).is_none());
        assert!(
            m.snapshot(SYM, ts(2)).is_none(),
            "stale book must not be published"
        );
        assert!(delta(&mut m, 0, 1, 2, 3).is_none());
        assert_eq!(snapshot_qty(snap(&mut m, 0, 20, 5, 4)), 20);
    }

    #[test]
    fn lagging_source_is_replaced_after_threshold() {
        let threshold = chrono::Duration::milliseconds(500);
        let mut m = MarketState::new(2);
        snap(&mut m, 0, 10, 1, 0);
        snap(&mut m, 1, 10, 1, 0);

        delta(&mut m, 1, 2, 2, 100); // backup ahead
        assert!(m.is_lagging());
        assert!(
            m.check_lag(SYM, ts(400), threshold).is_none(),
            "within threshold"
        );

        delta(&mut m, 0, 2, 2, 450); // source catches up
        assert!(!m.is_lagging());
        assert!(m.check_lag(SYM, ts(700), threshold).is_none());

        delta(&mut m, 1, 1, 3, 800); // backup ahead again, source stalls
        assert_eq!(snapshot_qty(m.check_lag(SYM, ts(1_300), threshold)), 13);
        assert_eq!(m.source(), 1);
        assert!(!m.is_lagging());
    }

    #[test]
    fn fresh_backup_snapshot_with_higher_sequence_is_not_lag() {
        let mut m = MarketState::new(2);
        snap(&mut m, 0, 10, 100, 0);
        delta(&mut m, 0, 1, 105, 1);
        // Backup reconnects; its snapshot sequence is newer, but no change
        // has happened that the source missed.
        snap(&mut m, 1, 11, 200, 2);
        assert!(!m.is_lagging());
        // A delta newer than anything the source has seen is real lag.
        delta(&mut m, 1, 1, 201, 3);
        assert!(m.is_lagging());
    }

    #[test]
    fn closing_market_publishes_nothing_and_drops_when_all_removed() {
        let mut m = MarketState::new(2);
        snap(&mut m, 0, 10, 1, 0);
        snap(&mut m, 1, 10, 1, 0);
        assert!(!m.on_removed(0));
        assert!(delta(&mut m, 1, 1, 2, 1).is_none());
        assert!(m.snapshot(SYM, ts(2)).is_none());
        assert!(
            snap(&mut m, 0, 10, 3, 3).is_none(),
            "removed replica ignored"
        );
        assert!(m.on_removed(1));
    }

    #[test]
    fn periodic_snapshot_comes_from_source() {
        let mut m = MarketState::new(2);
        snap(&mut m, 0, 10, 1, 0);
        snap(&mut m, 1, 99, 1, 0);
        let s = m.snapshot(SYM, ts(60_000)).unwrap();
        assert_eq!(s.bids[0].quantity, 10);
        assert_eq!((s.received_at, s.exchange_timestamp), (ts(60_000), ts(0)));
    }
}
