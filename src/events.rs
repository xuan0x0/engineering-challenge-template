//! Exchange-agnostic orderbook events, produced by exchange connections and
//! consumed by the [`collector`](crate::collector).

use chrono::{DateTime, Utc};

use crate::Side;

/// Index of a connection within a group of redundant connections that carry
/// the same symbols.
pub type ReplicaId = usize;

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
        /// Exchange ordering key, comparable across connections (Kalshi's
        /// `clock`). `None` if the exchange provides none.
        sequence: Option<u64>,
        bids: Vec<Level>,
        asks: Vec<Level>,
    },
    /// Relative change to one price level.
    Delta {
        symbol: String,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
        sequence: Option<u64>,
        side: Side,
        price: i64,
        quantity_delta: i64,
    },
    /// This connection's view of these symbols can no longer be trusted
    /// (disconnect, resync, or an uninterpretable message) until it sends a
    /// fresh snapshot.
    Invalidated { symbols: Vec<String> },
    /// The symbol is no longer subscribed; its book should be dropped.
    Removed { symbol: String },
}

/// A [`BookEvent`] tagged with the redundant connection it arrived on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourcedEvent {
    pub replica: ReplicaId,
    pub event: BookEvent,
}
