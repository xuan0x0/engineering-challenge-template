//! Pure subscription state for one Kalshi websocket. It decides which commands
//! to send and which incoming messages to forward, without doing any IO, so the
//! protocol logic can be unit tested.
//!
//! Each socket holds at most one `orderbook_delta` subscription (one `sid`).
//! Markets are added and removed with `update_subscription`. Sequence numbers
//! are per `sid`; any gap triggers a resync (unsubscribe + fresh subscribe),
//! which makes Kalshi resend snapshots for every market. A message we can
//! parse but not convert (e.g. an unexpected price format) quarantines just
//! that market: its book is dropped until the next fresh snapshot.

use std::collections::{BTreeSet, HashSet};

use chrono::{DateTime, Utc};

use super::messages::{
    Incoming, UpdateAction, subscribe_cmd, unsubscribe_cmd, update_subscription_cmd,
};
use crate::events::BookEvent;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Fault {
    /// We missed or could not interpret a message on the current subscription.
    Resync(String),
    /// The exchange rejected our subscription; reconnect after a backoff.
    Rejected(String),
}

#[derive(Debug, Default)]
pub struct SubscriptionState {
    /// Markets this shard should be subscribed to.
    desired: BTreeSet<String>,
    /// Markets included in commands already sent for the current subscription.
    sent: BTreeSet<String>,
    sid: Option<u64>,
    /// Command id of an in-flight subscribe.
    pending: Option<u64>,
    /// Subscribes abandoned by a resync before they were acked. If their ack
    /// arrives later, the resulting sid must be unsubscribed.
    stale_pending: HashSet<u64>,
    /// Command ids of in-flight `update_subscription` commands.
    inflight_updates: HashSet<u64>,
    last_seq: Option<u64>,
    /// Markets that have received a snapshot on the current subscription.
    /// Deltas are only forwarded after a snapshot.
    snapshotted: HashSet<String>,
    next_id: u64,
}

impl SubscriptionState {
    pub fn desired_len(&self) -> usize {
        self.desired.len()
    }

    pub fn is_subscribed(&self) -> bool {
        self.sid.is_some()
    }

    /// Forget all per-connection state; call after (re)connecting.
    pub fn reset_connection(&mut self) {
        self.sent.clear();
        self.sid = None;
        self.pending = None;
        self.stale_pending.clear();
        self.inflight_updates.clear();
        self.last_seq = None;
        self.snapshotted.clear();
    }

    /// Markets whose book this connection has published since its last
    /// snapshot. Call before a reset or resync to invalidate them downstream.
    pub fn take_snapshotted(&mut self) -> Vec<String> {
        self.snapshotted.drain().collect()
    }

    pub fn add(&mut self, tickers: Vec<String>) {
        self.desired.extend(tickers);
    }

    /// Stops tracking `tickers` and returns `Removed` events for the ones we
    /// had, so the collector drops their books.
    pub fn remove(&mut self, tickers: Vec<String>) -> Vec<BookEvent> {
        tickers
            .into_iter()
            .filter(|t| self.desired.remove(t))
            .map(|symbol| {
                self.snapshotted.remove(&symbol);
                BookEvent::Removed { symbol }
            })
            .collect()
    }

    /// Commands that bring the exchange-side subscription in line with
    /// `desired`.
    pub fn reconcile(&mut self) -> Vec<String> {
        if self.pending.is_some() {
            // Wait for the subscribe ack; we reconcile again when it arrives.
            return Vec::new();
        }
        let Some(sid) = self.sid else {
            if self.desired.is_empty() {
                return Vec::new();
            }
            let id = self.next_id();
            let tickers: Vec<String> = self.desired.iter().cloned().collect();
            self.pending = Some(id);
            self.sent = self.desired.clone();
            return vec![subscribe_cmd(id, &tickers)];
        };

        if self.desired.is_empty() {
            let id = self.next_id();
            self.sid = None;
            self.sent.clear();
            self.last_seq = None;
            return vec![unsubscribe_cmd(id, sid)];
        }

        let to_add: Vec<String> = self.desired.difference(&self.sent).cloned().collect();
        let to_delete: Vec<String> = self.sent.difference(&self.desired).cloned().collect();
        let mut out = Vec::new();
        if !to_delete.is_empty() {
            let id = self.next_id();
            self.inflight_updates.insert(id);
            out.push(update_subscription_cmd(
                id,
                sid,
                UpdateAction::DeleteMarkets,
                &to_delete,
            ));
        }
        if !to_add.is_empty() {
            let id = self.next_id();
            self.inflight_updates.insert(id);
            out.push(update_subscription_cmd(
                id,
                sid,
                UpdateAction::AddMarkets,
                &to_add,
            ));
        }
        self.sent = self.desired.clone();
        out
    }

    /// Drops the current subscription and starts a fresh one on the same
    /// connection.
    pub fn resync(&mut self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(sid) = self.sid {
            let id = self.next_id();
            out.push(unsubscribe_cmd(id, sid));
        }
        let mut stale = std::mem::take(&mut self.stale_pending);
        stale.extend(self.pending);
        self.reset_connection();
        self.stale_pending = stale;
        out.extend(self.reconcile());
        out
    }

    /// Processes one incoming message. Returns an event to forward, and pushes
    /// any commands that must be sent in response onto `outgoing`.
    pub fn on_message(
        &mut self,
        incoming: Incoming,
        received_at: DateTime<Utc>,
        outgoing: &mut Vec<String>,
    ) -> Result<Option<BookEvent>, Fault> {
        match incoming {
            Incoming::Subscribed { id, msg } => {
                if id.is_some() && id == self.pending {
                    self.pending = None;
                    self.sid = Some(msg.sid);
                    self.last_seq = None;
                    outgoing.extend(self.reconcile());
                } else if let Some(id) = id
                    && self.stale_pending.remove(&id)
                {
                    let unsub_id = self.next_id();
                    outgoing.push(unsubscribe_cmd(unsub_id, msg.sid));
                }
                Ok(None)
            }
            Incoming::Ok { id, sid, seq } => {
                if let Some(id) = id {
                    self.inflight_updates.remove(&id);
                }
                if let (Some(sid), Some(seq)) = (sid, seq)
                    && Some(sid) == self.sid
                {
                    self.check_seq(seq)?;
                }
                Ok(None)
            }
            Incoming::Unsubscribed { sid, seq, .. } => {
                if let (Some(sid), Some(seq)) = (sid, seq)
                    && Some(sid) == self.sid
                {
                    self.check_seq(seq)?;
                }
                Ok(None)
            }
            Incoming::Error { id, msg } => {
                let detail = format!("code {}: {}", msg.code, msg.msg);
                if id.is_some() && id == self.pending {
                    self.pending = None;
                    return Err(Fault::Rejected(detail));
                }
                if let Some(id) = id
                    && self.inflight_updates.remove(&id)
                {
                    // We no longer know which markets the exchange has; start over.
                    return Err(Fault::Resync(format!(
                        "update_subscription failed: {detail}"
                    )));
                }
                tracing::warn!(?id, %detail, "kalshi command error");
                Ok(None)
            }
            Incoming::OrderbookSnapshot {
                sid,
                seq,
                msg,
                sending_ts_ms,
            } => {
                if !self.accept(sid, seq, &msg.market_ticker)? {
                    return Ok(None);
                }
                match msg.to_event(received_at, sending_ts_ms) {
                    Ok(event) => {
                        self.snapshotted.insert(msg.market_ticker);
                        Ok(Some(event))
                    }
                    Err(e) => Ok(Some(self.quarantine(msg.market_ticker, &e))),
                }
            }
            Incoming::OrderbookDelta {
                sid,
                seq,
                msg,
                sending_ts_ms,
            } => {
                if !self.accept(sid, seq, &msg.market_ticker)?
                    || !self.snapshotted.contains(&msg.market_ticker)
                {
                    return Ok(None);
                }
                match msg.to_event(received_at, sending_ts_ms) {
                    Ok(event) => Ok(Some(event)),
                    Err(e) => Ok(Some(self.quarantine(msg.market_ticker, &e))),
                }
            }
            Incoming::Unknown => Ok(None),
        }
    }

    /// Stops forwarding deltas for a market whose message we could not
    /// interpret, and tells the collector its book from this connection is
    /// unreliable.
    /// The market recovers on its next snapshot (resync or reconnect).
    fn quarantine(&mut self, symbol: String, error: &anyhow::Error) -> BookEvent {
        tracing::error!(%symbol, error = %format!("{error:#}"), "uninterpretable message; market quarantined");
        self.snapshotted.remove(&symbol);
        BookEvent::Invalidated {
            symbols: vec![symbol],
        }
    }

    /// Validates sequencing for a book message and decides whether it is for a
    /// market we still want.
    fn accept(&mut self, sid: u64, seq: u64, ticker: &str) -> Result<bool, Fault> {
        if Some(sid) != self.sid {
            // Leftover from a subscription we already dropped.
            return Ok(false);
        }
        self.check_seq(seq)?;
        Ok(self.desired.contains(ticker))
    }

    fn check_seq(&mut self, seq: u64) -> Result<(), Fault> {
        if let Some(last) = self.last_seq
            && seq != last + 1
        {
            return Err(Fault::Resync(format!(
                "seq gap: expected {}, got {seq}",
                last + 1
            )));
        }
        self.last_seq = Some(seq);
        Ok(())
    }

    fn next_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kalshi::messages::{
        Decimal, DeltaMsg, ErrorMsg, KalshiSide, SnapshotMsg, SubscribedMsg,
    };

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp(1_700_000_000, 0).unwrap()
    }

    fn tickers(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    fn cmd(raw: &str) -> serde_json::Value {
        serde_json::from_str(raw).unwrap()
    }

    fn subscribed(id: u64, sid: u64) -> Incoming {
        Incoming::Subscribed {
            id: Some(id),
            msg: SubscribedMsg {
                channel: "orderbook_delta".to_owned(),
                sid,
            },
        }
    }

    fn snapshot(sid: u64, seq: u64, ticker: &str) -> Incoming {
        Incoming::OrderbookSnapshot {
            sid,
            seq,
            msg: SnapshotMsg {
                market_ticker: ticker.to_owned(),
                yes_dollars_fp: vec![(Decimal::Str("0.40".into()), Decimal::Str("1".into()))],
                no_dollars_fp: vec![],
                clock: Some(seq),
            },
            sending_ts_ms: None,
        }
    }

    fn delta(sid: u64, seq: u64, ticker: &str) -> Incoming {
        Incoming::OrderbookDelta {
            sid,
            seq,
            msg: DeltaMsg {
                market_ticker: ticker.to_owned(),
                price_dollars: Decimal::Str("0.40".into()),
                delta_fp: Decimal::Str("2".into()),
                side: KalshiSide::Yes,
                ts_ms: None,
                clock: Some(seq),
            },
            sending_ts_ms: None,
        }
    }

    /// Returns a state subscribed (sid 10) to `markets`.
    fn subscribed_state(markets: &[&str]) -> SubscriptionState {
        let mut s = SubscriptionState::default();
        s.add(tickers(markets));
        let out = s.reconcile();
        let id = cmd(&out[0])["id"].as_u64().unwrap();
        let mut follow_up = Vec::new();
        s.on_message(subscribed(id, 10), t0(), &mut follow_up)
            .unwrap();
        assert!(follow_up.is_empty());
        s
    }

    #[test]
    fn subscribes_once_and_batches_adds_until_ack() {
        let mut s = SubscriptionState::default();
        assert!(s.reconcile().is_empty(), "nothing to subscribe to yet");

        s.add(tickers(&["A", "B"]));
        let out = s.reconcile();
        assert_eq!(out.len(), 1);
        let sub = cmd(&out[0]);
        assert_eq!(sub["cmd"], "subscribe");
        assert_eq!(
            sub["params"]["market_tickers"],
            serde_json::json!(["A", "B"])
        );

        s.add(tickers(&["C"]));
        assert!(s.reconcile().is_empty(), "must wait for subscribe ack");

        let mut follow_up = Vec::new();
        let id = sub["id"].as_u64().unwrap();
        s.on_message(subscribed(id, 10), t0(), &mut follow_up)
            .unwrap();
        assert_eq!(follow_up.len(), 1);
        let add = cmd(&follow_up[0]);
        assert_eq!(add["params"]["action"], "add_markets");
        assert_eq!(add["params"]["sids"], serde_json::json!([10]));
        assert_eq!(add["params"]["market_tickers"], serde_json::json!(["C"]));
    }

    #[test]
    fn remove_sends_delete_and_emits_removed_events() {
        let mut s = subscribed_state(&["A", "B"]);
        let events = s.remove(tickers(&["A", "UNKNOWN"]));
        assert_eq!(
            events,
            vec![BookEvent::Removed {
                symbol: "A".to_owned()
            }]
        );
        let out = s.reconcile();
        assert_eq!(out.len(), 1);
        assert_eq!(cmd(&out[0])["params"]["action"], "delete_markets");
        assert_eq!(
            cmd(&out[0])["params"]["market_tickers"],
            serde_json::json!(["A"])
        );
    }

    #[test]
    fn removing_all_markets_unsubscribes() {
        let mut s = subscribed_state(&["A"]);
        s.remove(tickers(&["A"]));
        let out = s.reconcile();
        assert_eq!(cmd(&out[0])["cmd"], "unsubscribe");
        assert!(!s.is_subscribed());
    }

    #[test]
    fn forwards_snapshot_then_deltas_and_drops_unsnapshotted_deltas() {
        let mut s = subscribed_state(&["A", "B"]);
        let mut out = Vec::new();
        assert!(
            s.on_message(delta(10, 1, "A"), t0(), &mut out)
                .unwrap()
                .is_none()
        );
        let snap = s.on_message(snapshot(10, 2, "A"), t0(), &mut out).unwrap();
        assert!(matches!(snap, Some(BookEvent::Snapshot { .. })));
        let d = s.on_message(delta(10, 3, "A"), t0(), &mut out).unwrap();
        assert!(matches!(
            d,
            Some(BookEvent::Delta {
                quantity_delta: 2_000_000,
                ..
            })
        ));
    }

    #[test]
    fn ignores_messages_for_removed_markets_and_stale_sids() {
        let mut s = subscribed_state(&["A", "B"]);
        let mut out = Vec::new();
        s.on_message(snapshot(10, 1, "A"), t0(), &mut out).unwrap();
        s.remove(tickers(&["A"]));
        assert!(
            s.on_message(delta(10, 2, "A"), t0(), &mut out)
                .unwrap()
                .is_none()
        );
        assert!(
            s.on_message(snapshot(99, 1, "B"), t0(), &mut out)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn seq_gap_triggers_resync_with_fresh_subscription() {
        let mut s = subscribed_state(&["A"]);
        let mut out = Vec::new();
        s.on_message(snapshot(10, 1, "A"), t0(), &mut out).unwrap();
        let fault = s.on_message(delta(10, 3, "A"), t0(), &mut out).unwrap_err();
        assert!(matches!(fault, Fault::Resync(_)));

        let cmds: Vec<serde_json::Value> = s.resync().iter().map(|c| cmd(c)).collect();
        assert_eq!(cmds[0]["cmd"], "unsubscribe");
        assert_eq!(cmds[0]["params"]["sids"], serde_json::json!([10]));
        assert_eq!(cmds[1]["cmd"], "subscribe");
        assert!(!s.is_subscribed());
    }

    #[test]
    fn ok_messages_count_toward_sequence() {
        let mut s = subscribed_state(&["A"]);
        let mut out = Vec::new();
        s.on_message(snapshot(10, 1, "A"), t0(), &mut out).unwrap();
        s.on_message(
            Incoming::Ok {
                id: Some(5),
                sid: Some(10),
                seq: Some(2),
            },
            t0(),
            &mut out,
        )
        .unwrap();
        assert!(
            s.on_message(delta(10, 3, "A"), t0(), &mut out)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn bad_delta_quarantines_only_that_market() {
        let mut s = subscribed_state(&["A", "B"]);
        let mut out = Vec::new();
        s.on_message(snapshot(10, 1, "A"), t0(), &mut out).unwrap();
        s.on_message(snapshot(10, 2, "B"), t0(), &mut out).unwrap();
        let Incoming::OrderbookDelta {
            sid,
            seq,
            mut msg,
            sending_ts_ms,
        } = delta(10, 3, "A")
        else {
            unreachable!()
        };
        msg.price_dollars = Decimal::Str("0.1234567".into());
        let bad = Incoming::OrderbookDelta {
            sid,
            seq,
            msg,
            sending_ts_ms,
        };
        let event = s.on_message(bad, t0(), &mut out).unwrap();
        assert_eq!(
            event,
            Some(BookEvent::Invalidated {
                symbols: vec!["A".to_owned()]
            })
        );
        assert!(
            s.on_message(delta(10, 4, "A"), t0(), &mut out)
                .unwrap()
                .is_none()
        );
        assert!(
            s.on_message(delta(10, 5, "B"), t0(), &mut out)
                .unwrap()
                .is_some()
        );
        assert!(out.is_empty(), "no resync for a single bad message");
    }

    #[test]
    fn failed_update_subscription_triggers_resync() {
        let mut s = subscribed_state(&["A"]);
        s.add(tickers(&["B"]));
        let id = cmd(&s.reconcile()[0])["id"].as_u64();
        let err = Incoming::Error {
            id,
            msg: ErrorMsg {
                code: 1,
                msg: "boom".to_owned(),
            },
        };
        assert!(matches!(
            s.on_message(err, t0(), &mut Vec::new()),
            Err(Fault::Resync(_))
        ));
    }

    #[test]
    fn late_ack_for_abandoned_subscribe_is_unsubscribed() {
        let mut s = SubscriptionState::default();
        s.add(tickers(&["A"]));
        let first_id = cmd(&s.reconcile()[0])["id"].as_u64().unwrap();
        let resync = s.resync();
        let second_id = cmd(&resync[0])["id"].as_u64().unwrap();
        assert_ne!(first_id, second_id);

        let mut out = Vec::new();
        s.on_message(subscribed(first_id, 77), t0(), &mut out)
            .unwrap();
        assert_eq!(cmd(&out[0])["cmd"], "unsubscribe");
        assert_eq!(cmd(&out[0])["params"]["sids"], serde_json::json!([77]));
        assert!(!s.is_subscribed());

        s.on_message(subscribed(second_id, 78), t0(), &mut Vec::new())
            .unwrap();
        assert!(s.is_subscribed());
    }

    #[test]
    fn rejected_subscribe_is_reported() {
        let mut s = SubscriptionState::default();
        s.add(tickers(&["A"]));
        let id = cmd(&s.reconcile()[0])["id"].as_u64();
        let err = Incoming::Error {
            id,
            msg: ErrorMsg {
                code: 26,
                msg: "limit".to_owned(),
            },
        };
        let fault = s.on_message(err, t0(), &mut Vec::new()).unwrap_err();
        assert_eq!(fault, Fault::Rejected("code 26: limit".to_owned()));
    }
}
