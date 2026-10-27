//! In-memory orderbook for a single symbol.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::Side;
use crate::events::Level;
use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// Result of applying a relative delta to a level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeltaOutcome {
    /// The absolute quantity now resting at the level (0 = level removed).
    pub quantity: i64,
    /// True when the delta would have taken the level below zero, which means
    /// our book has diverged from the exchange's.
    pub went_negative: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl OrderBook {
    pub fn from_levels(bids: &[Level], asks: &[Level]) -> Self {
        let collect = |levels: &[Level]| {
            levels
                .iter()
                .filter(|l| l.quantity > 0)
                .map(|l| (l.price, l.quantity))
                .collect()
        };
        Self {
            bids: collect(bids),
            asks: collect(asks),
        }
    }

    pub fn apply_delta(&mut self, side: Side, price: i64, quantity_delta: i64) -> DeltaOutcome {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let current = levels.get(&price).copied().unwrap_or(0);
        let next = current + quantity_delta;
        if next > 0 {
            levels.insert(price, next);
        } else {
            levels.remove(&price);
        }
        DeltaOutcome {
            quantity: next.max(0),
            went_negative: next < 0,
        }
    }

    pub fn quantity_at(&self, side: Side, price: i64) -> i64 {
        let levels = match side {
            Side::Buy => &self.bids,
            Side::Sell => &self.asks,
        };
        levels.get(&price).copied().unwrap_or(0)
    }

    /// Bids best (highest) first, asks best (lowest) first.
    pub fn to_snapshot(
        &self,
        symbol: &str,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp,
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(price: i64, quantity: i64) -> Level {
        Level { price, quantity }
    }

    #[test]
    fn from_levels_drops_empty_levels() {
        let book = OrderBook::from_levels(&[lvl(400_000, 0), lvl(410_000, 5)], &[]);
        assert_eq!(book.quantity_at(Side::Buy, 400_000), 0);
        assert_eq!(book.quantity_at(Side::Buy, 410_000), 5);
    }

    #[test]
    fn delta_adds_modifies_and_removes_levels() {
        let mut book = OrderBook::default();
        let added = book.apply_delta(Side::Sell, 600_000, 10);
        assert_eq!(added.quantity, 10);
        assert_eq!(book.apply_delta(Side::Sell, 600_000, -4).quantity, 6);
        let removed = book.apply_delta(Side::Sell, 600_000, -6);
        assert_eq!(
            removed,
            DeltaOutcome {
                quantity: 0,
                went_negative: false
            }
        );
        assert_eq!(book, OrderBook::default());
    }

    #[test]
    fn delta_below_zero_is_flagged_and_clamped() {
        let mut book = OrderBook::from_levels(&[lvl(400_000, 3)], &[]);
        let outcome = book.apply_delta(Side::Buy, 400_000, -5);
        assert_eq!(
            outcome,
            DeltaOutcome {
                quantity: 0,
                went_negative: true
            }
        );
        assert_eq!(book.quantity_at(Side::Buy, 400_000), 0);
    }

    #[test]
    fn snapshot_orders_best_levels_first() {
        let book = OrderBook::from_levels(
            &[lvl(300_000, 1), lvl(450_000, 2), lvl(400_000, 3)],
            &[lvl(700_000, 4), lvl(550_000, 5)],
        );
        let ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
        let snap = book.to_snapshot("SYM", ts, ts);
        let bids: Vec<_> = snap.bids.iter().map(|e| e.price_level).collect();
        let asks: Vec<_> = snap.asks.iter().map(|e| e.price_level).collect();
        assert_eq!(bids, vec![450_000, 400_000, 300_000]);
        assert_eq!(asks, vec![550_000, 700_000]);
        assert_eq!(snap.symbol, "SYM");
    }
}
