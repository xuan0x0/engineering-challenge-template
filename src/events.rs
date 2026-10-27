//! Exchange-agnostic orderbook events, produced by exchange connections and
//! consumed by the [`collector`](crate::collector).

use chrono::{DateTime, Utc};

use crate::Side;

/// A price level in fixed point (see [`crate::fixed_point`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BookEvent {
    /// Full replacement of a symbol's book.
    Snapshot {
        symbol: String,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// Relative change to one price level.
    Delta {
        symbol: String,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        side: Side,
        price: i64,
        quantity_delta: i64,
    },
    /// The symbol is no longer subscribed; its book should be dropped.
    Removed { symbol: String },
}
