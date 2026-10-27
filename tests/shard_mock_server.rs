//! Runs a real shard against a local mock of the Kalshi websocket, covering
//! the signed handshake, subscribe, event forwarding, seq-gap resync, and
//! clean close on cancellation.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rsa::RsaPrivateKey;
use rsa::rand_core::OsRng;
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_util::sync::CancellationToken;

use engineering_challenge::Side;
use engineering_challenge::events::BookEvent;
use engineering_challenge::kalshi::auth::Signer;
use engineering_challenge::kalshi::connection::{ShardConfig, run_shard};
use engineering_challenge::universe::ShardCmd;

const TIMEOUT: Duration = Duration::from_secs(5);

fn snapshot(sid: u64, seq: u64) -> String {
    json!({"type":"orderbook_snapshot","sid":sid,"seq":seq,"msg":{
        "market_ticker":"M","market_id":"x","clock":1,
        "yes_dollars_fp":[["0.40","10.00"]],"no_dollars_fp":[["0.55","5.00"]]}})
    .to_string()
}

fn delta(sid: u64, seq: u64) -> String {
    json!({"type":"orderbook_delta","sid":sid,"seq":seq,"msg":{
        "market_ticker":"M","market_id":"x","clock":2,
        "price_dollars":"0.40","delta_fp":"-4.00","side":"yes","ts_ms":1700000000000i64}})
    .to_string()
}

async fn next_json<S>(ws: &mut S) -> Value
where
    S: StreamExt<Item = Result<Message, tokio_tungstenite::tungstenite::Error>> + Unpin,
{
    loop {
        let msg = tokio::time::timeout(TIMEOUT, ws.next())
            .await
            .expect("timed out waiting for client message")
            .expect("client hung up")
            .unwrap();
        if let Message::Text(t) = msg {
            return serde_json::from_str(t.as_str()).unwrap();
        }
    }
}

#[tokio::test]
#[allow(clippy::result_large_err)] // the Err type is fixed by tungstenite's callback signature
async fn shard_subscribes_forwards_resyncs_and_closes() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let seen_headers: Arc<Mutex<Vec<String>>> = Arc::default();

    let server = {
        let seen_headers = seen_headers.clone();
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let callback = |req: &Request, resp: Response| {
                let mut seen = seen_headers.lock().unwrap();
                for name in [
                    "KALSHI-ACCESS-KEY",
                    "KALSHI-ACCESS-SIGNATURE",
                    "KALSHI-ACCESS-TIMESTAMP",
                ] {
                    if req.headers().contains_key(name) {
                        seen.push(name.to_owned());
                    }
                }
                Ok(resp)
            };
            let mut ws = tokio_tungstenite::accept_hdr_async(tcp, callback)
                .await
                .unwrap();

            let sub = next_json(&mut ws).await;
            assert_eq!(sub["cmd"], "subscribe");
            assert_eq!(sub["params"]["market_tickers"], json!(["M"]));
            let reply = json!({"type":"subscribed","id":sub["id"],"msg":{"channel":"orderbook_delta","sid":1}});
            ws.send(Message::Text(reply.to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text(snapshot(1, 1).into())).await.unwrap();
            ws.send(Message::Text(delta(1, 2).into())).await.unwrap();
            // Skip seq 3: the client must resubscribe.
            ws.send(Message::Text(delta(1, 4).into())).await.unwrap();

            let unsub = next_json(&mut ws).await;
            assert_eq!(unsub["cmd"], "unsubscribe");
            assert_eq!(unsub["params"]["sids"], json!([1]));
            let resub = next_json(&mut ws).await;
            assert_eq!(resub["cmd"], "subscribe");
            // Late message from the dropped subscription must be ignored.
            ws.send(Message::Text(delta(1, 5).into())).await.unwrap();
            let reply = json!({"type":"subscribed","id":resub["id"],"msg":{"channel":"orderbook_delta","sid":2}});
            ws.send(Message::Text(reply.to_string().into()))
                .await
                .unwrap();
            ws.send(Message::Text(snapshot(2, 1).into())).await.unwrap();

            // Expect a Close frame once the client is cancelled.
            loop {
                match tokio::time::timeout(TIMEOUT, ws.next()).await {
                    Ok(Some(Ok(Message::Close(_)))) => break true,
                    Ok(Some(Ok(_))) => continue,
                    _ => break false,
                }
            }
        })
    };

    let (events_tx, mut events_rx) = mpsc::channel(16);
    let (cmd_tx, cmd_rx) = mpsc::channel(4);
    let cancel = CancellationToken::new();
    let cfg = ShardConfig {
        shard_id: 0,
        ws_url: format!("ws://{addr}/trade-api/ws/v2"),
        signer: Arc::new(Signer::new(
            "test-key",
            RsaPrivateKey::new(&mut OsRng, 2048).unwrap(),
        )),
        events: events_tx,
        cancel: cancel.clone(),
    };
    let shard = tokio::spawn(run_shard(cfg, cmd_rx));
    cmd_tx
        .send(ShardCmd::Add(vec!["M".to_owned()]))
        .await
        .unwrap();

    let mut events = Vec::new();
    for _ in 0..3 {
        let event = tokio::time::timeout(TIMEOUT, events_rx.recv())
            .await
            .unwrap()
            .unwrap();
        events.push(event);
    }
    cancel.cancel();
    tokio::time::timeout(TIMEOUT, shard).await.unwrap().unwrap();
    assert!(server.await.unwrap(), "server should receive a Close frame");

    assert_eq!(
        seen_headers.lock().unwrap().len(),
        3,
        "all auth headers sent"
    );
    assert!(matches!(&events[0], BookEvent::Snapshot { bids, asks, .. }
        if bids[0].price == 400_000 && asks[0].price == 450_000));
    assert!(matches!(
        &events[1],
        BookEvent::Delta {
            side: Side::Buy,
            price: 400_000,
            quantity_delta: -4_000_000,
            ..
        }
    ));
    assert!(
        matches!(&events[2], BookEvent::Snapshot { .. }),
        "fresh snapshot after resync"
    );
    assert!(
        events_rx.recv().await.is_none(),
        "no stray events after shutdown"
    );
}
