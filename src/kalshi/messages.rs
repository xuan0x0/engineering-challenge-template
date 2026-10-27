//! Kalshi websocket wire format, and conversion into exchange-agnostic
//! [`BookEvent`]s.
//!
//! Kalshi books are two bid ladders: YES bids and NO bids. A NO bid at price p
//! is equivalent to a YES ask at `1 - p`, so we publish the YES book:
//! YES levels become bids, NO levels become asks at the complementary price.

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::json;

use crate::Side;
use crate::events::{BookEvent, Level};
use crate::fixed_point::{SCALE, parse_fixed};

pub const ORDERBOOK_CHANNEL: &str = "orderbook_delta";

/// A decimal that Kalshi may send as either a JSON string or number.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(untagged)]
pub enum Decimal {
    Str(String),
    Int(i64),
}

impl Decimal {
    fn to_fixed(&self) -> anyhow::Result<i64> {
        match self {
            Self::Str(s) => parse_fixed(s),
            Self::Int(i) => i
                .checked_mul(SCALE)
                .ok_or_else(|| anyhow::anyhow!("integer {i} overflows fixed point")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Incoming {
    Subscribed {
        id: Option<u64>,
        msg: SubscribedMsg,
    },
    Ok {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
    },
    Unsubscribed {
        id: Option<u64>,
        sid: Option<u64>,
        seq: Option<u64>,
    },
    Error {
        id: Option<u64>,
        msg: ErrorMsg,
    },
    OrderbookSnapshot {
        sid: u64,
        seq: u64,
        msg: SnapshotMsg,
        sending_ts_ms: Option<i64>,
    },
    OrderbookDelta {
        sid: u64,
        seq: u64,
        msg: DeltaMsg,
        sending_ts_ms: Option<i64>,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SubscribedMsg {
    pub channel: String,
    pub sid: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ErrorMsg {
    pub code: i64,
    pub msg: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct SnapshotMsg {
    pub market_ticker: String,
    #[serde(default)]
    pub yes_dollars_fp: Vec<(Decimal, Decimal)>,
    #[serde(default)]
    pub no_dollars_fp: Vec<(Decimal, Decimal)>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DeltaMsg {
    pub market_ticker: String,
    pub price_dollars: Decimal,
    pub delta_fp: Decimal,
    pub side: KalshiSide,
    pub ts_ms: Option<i64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KalshiSide {
    Yes,
    No,
}

impl KalshiSide {
    /// Maps a Kalshi bid on this side to a level on the YES book.
    fn to_yes_book(self, price: i64) -> (Side, i64) {
        match self {
            Self::Yes => (Side::Buy, price),
            Self::No => (Side::Sell, SCALE - price),
        }
    }
}

fn millis_to_utc(ms: Option<i64>, fallback: DateTime<Utc>) -> DateTime<Utc> {
    ms.and_then(DateTime::from_timestamp_millis)
        .unwrap_or(fallback)
}

fn levels(raw: &[(Decimal, Decimal)], side: KalshiSide) -> anyhow::Result<Vec<Level>> {
    raw.iter()
        .map(|(price, quantity)| {
            let (_, price) = side.to_yes_book(price.to_fixed()?);
            Ok(Level {
                price,
                quantity: quantity.to_fixed()?,
            })
        })
        .collect()
}

impl SnapshotMsg {
    pub fn to_event(
        &self,
        received_at: DateTime<Utc>,
        sending_ts_ms: Option<i64>,
    ) -> anyhow::Result<BookEvent> {
        Ok(BookEvent::Snapshot {
            symbol: self.market_ticker.clone(),
            received_at,
            exchange_timestamp: millis_to_utc(sending_ts_ms, received_at),
            bids: levels(&self.yes_dollars_fp, KalshiSide::Yes)?,
            asks: levels(&self.no_dollars_fp, KalshiSide::No)?,
        })
    }
}

impl DeltaMsg {
    pub fn to_event(
        &self,
        received_at: DateTime<Utc>,
        sending_ts_ms: Option<i64>,
    ) -> anyhow::Result<BookEvent> {
        let (side, price) = self.side.to_yes_book(self.price_dollars.to_fixed()?);
        Ok(BookEvent::Delta {
            symbol: self.market_ticker.clone(),
            received_at,
            exchange_timestamp: millis_to_utc(self.ts_ms.or(sending_ts_ms), received_at),
            side,
            price,
            quantity_delta: self.delta_fp.to_fixed()?,
        })
    }
}

pub fn subscribe_cmd(id: u64, tickers: &[String]) -> String {
    json!({
        "id": id,
        "cmd": "subscribe",
        "params": { "channels": [ORDERBOOK_CHANNEL], "market_tickers": tickers },
    })
    .to_string()
}

pub fn update_subscription_cmd(
    id: u64,
    sid: u64,
    action: UpdateAction,
    tickers: &[String],
) -> String {
    json!({
        "id": id,
        "cmd": "update_subscription",
        "params": { "sids": [sid], "market_tickers": tickers, "action": action.as_str() },
    })
    .to_string()
}

pub fn unsubscribe_cmd(id: u64, sid: u64) -> String {
    json!({ "id": id, "cmd": "unsubscribe", "params": { "sids": [sid] } }).to_string()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateAction {
    AddMarkets,
    DeleteMarkets,
}

impl UpdateAction {
    fn as_str(self) -> &'static str {
        match self {
            Self::AddMarkets => "add_markets",
            Self::DeleteMarkets => "delete_markets",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap()
    }

    #[test]
    fn parses_snapshot_and_maps_no_side_to_asks() {
        let raw = r#"{"type":"orderbook_snapshot","sid":2,"seq":2,"msg":{
            "market_ticker":"FED-23DEC-T3.00","market_id":"x","clock":4182057,
            "yes_dollars_fp":[["0.0800","300.00"],["0.2200","333.00"]],
            "no_dollars_fp":[["0.5400","20.00"]]},
            "sending_ts_ms":1700000000500}"#;
        let Incoming::OrderbookSnapshot {
            sid,
            seq,
            msg,
            sending_ts_ms,
        } = serde_json::from_str(raw).unwrap()
        else {
            panic!("expected snapshot");
        };
        assert_eq!((sid, seq), (2, 2));

        let BookEvent::Snapshot {
            symbol,
            exchange_timestamp,
            bids,
            asks,
            ..
        } = msg.to_event(t0(), sending_ts_ms).unwrap()
        else {
            panic!("expected snapshot event");
        };
        assert_eq!(symbol, "FED-23DEC-T3.00");
        assert_eq!(exchange_timestamp.timestamp_millis(), 1_700_000_000_500);
        assert_eq!(
            bids,
            vec![
                Level {
                    price: 80_000,
                    quantity: 300_000_000
                },
                Level {
                    price: 220_000,
                    quantity: 333_000_000
                },
            ]
        );
        assert_eq!(
            asks,
            vec![Level {
                price: 460_000,
                quantity: 20_000_000
            }]
        );
    }

    #[test]
    fn snapshot_with_empty_sides_omitted() {
        let raw = r#"{"type":"orderbook_snapshot","sid":1,"seq":1,
            "msg":{"market_ticker":"EMPTY","market_id":"x","clock":1}}"#;
        let Incoming::OrderbookSnapshot { msg, .. } = serde_json::from_str(raw).unwrap() else {
            panic!("expected snapshot");
        };
        let BookEvent::Snapshot {
            bids,
            asks,
            exchange_timestamp,
            ..
        } = msg.to_event(t0(), None).unwrap()
        else {
            panic!("expected snapshot event");
        };
        assert!(bids.is_empty() && asks.is_empty());
        assert_eq!(exchange_timestamp, t0());
    }

    #[test]
    fn parses_delta_with_ts_ms() {
        let raw = r#"{"type":"orderbook_delta","sid":2,"seq":3,"msg":{
            "market_ticker":"FED-23DEC-T3.00","market_id":"x","clock":4182061,
            "price_dollars":"0.9600","delta_fp":"-54.00","side":"no",
            "ts_ms":1700000000250},"sending_ts_ms":1700000000300}"#;
        let Incoming::OrderbookDelta {
            msg, sending_ts_ms, ..
        } = serde_json::from_str(raw).unwrap()
        else {
            panic!("expected delta");
        };
        assert_eq!(
            msg.to_event(t0(), sending_ts_ms).unwrap(),
            BookEvent::Delta {
                symbol: "FED-23DEC-T3.00".to_owned(),
                received_at: t0(),
                exchange_timestamp: DateTime::from_timestamp_millis(1_700_000_000_250).unwrap(),
                side: Side::Sell,
                price: 40_000,
                quantity_delta: -54_000_000,
            }
        );
    }

    #[test]
    fn yes_delta_is_a_bid_at_same_price() {
        let raw = r#"{"type":"orderbook_delta","sid":2,"seq":3,"msg":{
            "market_ticker":"M","price_dollars":"0.4500","delta_fp":"10.00","side":"yes"}}"#;
        let Incoming::OrderbookDelta { msg, .. } = serde_json::from_str(raw).unwrap() else {
            panic!("expected delta");
        };
        let BookEvent::Delta {
            side,
            price,
            quantity_delta,
            ..
        } = msg.to_event(t0(), None).unwrap()
        else {
            panic!("expected delta event");
        };
        assert_eq!(
            (side, price, quantity_delta),
            (Side::Buy, 450_000, 10_000_000)
        );
    }

    #[test]
    fn parses_control_messages() {
        let sub: Incoming = serde_json::from_str(
            r#"{"type":"subscribed","id":1,"msg":{"channel":"orderbook_delta","sid":7}}"#,
        )
        .unwrap();
        assert_eq!(
            sub,
            Incoming::Subscribed {
                id: Some(1),
                msg: SubscribedMsg {
                    channel: "orderbook_delta".to_owned(),
                    sid: 7
                },
            }
        );
        let err: Incoming = serde_json::from_str(
            r#"{"type":"error","id":3,"msg":{"code":26,"msg":"Per-subscription market limit exceeded"}}"#,
        )
        .unwrap();
        assert!(matches!(
            err,
            Incoming::Error {
                msg: ErrorMsg { code: 26, .. },
                ..
            }
        ));
        let ok: Incoming = serde_json::from_str(
            r#"{"type":"ok","id":4,"sid":7,"seq":9,"msg":{"market_tickers":["A"]}}"#,
        )
        .unwrap();
        assert_eq!(
            ok,
            Incoming::Ok {
                id: Some(4),
                sid: Some(7),
                seq: Some(9)
            }
        );
        let unknown: Incoming = serde_json::from_str(r#"{"type":"ticker","msg":{}}"#).unwrap();
        assert_eq!(unknown, Incoming::Unknown);
    }

    #[test]
    fn malformed_price_is_an_error() {
        let msg = DeltaMsg {
            market_ticker: "M".to_owned(),
            price_dollars: Decimal::Str("bad".to_owned()),
            delta_fp: Decimal::Int(1),
            side: KalshiSide::Yes,
            ts_ms: None,
        };
        assert!(msg.to_event(t0(), None).is_err());
    }

    #[test]
    fn builds_commands() {
        let tickers = vec!["A".to_owned(), "B".to_owned()];
        let sub: serde_json::Value = serde_json::from_str(&subscribe_cmd(1, &tickers)).unwrap();
        assert_eq!(sub["cmd"], "subscribe");
        assert_eq!(sub["params"]["channels"][0], ORDERBOOK_CHANNEL);
        assert_eq!(sub["params"]["market_tickers"][1], "B");

        let upd: serde_json::Value = serde_json::from_str(&update_subscription_cmd(
            2,
            9,
            UpdateAction::DeleteMarkets,
            &tickers,
        ))
        .unwrap();
        assert_eq!(upd["params"]["sids"][0], 9);
        assert_eq!(upd["params"]["action"], "delete_markets");

        let unsub: serde_json::Value = serde_json::from_str(&unsubscribe_cmd(3, 9)).unwrap();
        assert_eq!(unsub["params"]["sids"][0], 9);
    }
}
