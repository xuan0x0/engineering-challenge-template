# Read before getting started!!

First, read [src/main.rs](https://github.com/voluntas-io/engineering-challenge/blob/main/src/main.rs). Then, some background on this repo:

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away for you, and you can simply use the helpers, as demonstrated in `main.rs`.

### More background, in case you are curious!

FIX SBE is defined in XML, which I find extremely cumbersome to edit, especially
when updating schemas frequently. To solve this, I have also introduced some
helper code that takes our own custom YAML format, and turns it into FIX SBE
XML, which in turn is used to generate rust code to encode and decode the
messages. **You don't need to edit any of these files**. This explanation is
simply to help you understand the origins of the various files in this repo.

If you're curious to see the YAML schema, take a look at [challenge_orderbook.yaml](https://github.com/voluntas-io/engineering-challenge/blob/main/sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
We also use a custom timestamp field, so that the Rust struct can use
`chrono::DateTime` in the generated struct, and in SBE schema, it is a uint64
representing the timestamp as the number of microseconds since unix epoch.


---

# Kalshi orderbook collector

Subscribes to every open, non-multivariate Kalshi market, maintains an
orderbook per market, and writes `PriceLevelUpdate` deltas plus a full
`Snapshot` of every book each minute, using the SBE helpers above.

## Running

```bash
export KALSHI_API_KEY_ID=...                  # API key ID
export KALSHI_PRIVATE_KEY_PATH=/path/key.pem  # Ed25519 or RSA private key (PEM)
cargo run --release -- --output-dir data      # hourly files in ./data
cargo run --release -- --stdout > out.sbe     # or a single SBE stream on stdout
```

Logs go to stderr (`RUST_LOG=debug` for more). Useful flags:
`--max-markets N` (only subscribe to the first N, for testing),
`--markets-per-connection` (default 1000), `--max-connections` (default 180),
`--discovery-interval-secs` (default 120), `--snapshot-interval-secs` (default 60).
Press Ctrl-C once for a graceful shutdown (websockets closed, file flushed);
press it twice to exit immediately.

Inspect output with:

```bash
cargo run --release --bin dump -- data/kalshi_orderbook_<ts>.sbe [TICKER]          # every message
cargo run --release --bin dump -- --final data/kalshi_orderbook_<ts>.sbe [TICKER]  # final books
cargo run --release --bin dump -- --final --as-of 2026-10-27T02:12:00Z <file> [TICKER]
```

## Output semantics

- Prices and quantities are fixed point (1_000_000 = $1 / 1 contract).
- Kalshi books are YES bids and NO bids. We publish the **YES book**: YES bids
  are `Buy` levels; a NO bid at `p` is a `Sell` (ask) level at `1 - p`.
- `PriceLevelUpdate.quantity` is the **absolute** quantity after the change;
  `0` means the level was removed.
- `backtest_timestamp` / `received_at` is stamped the moment the websocket
  frame is read, before parsing. `exchange_timestamp` is Kalshi's `ts_ms` for
  deltas, or `sending_ts_ms` for snapshots, falling back to the receive time.
- Files rotate every UTC hour; each new file begins with a snapshot of every
  book so it can be replayed on its own. Kalshi-sent snapshots (on subscribe,
  reconnect, or resync) are written as they arrive.

## Design

```
            REST /markets (every 120s)
                     │ diff
                     ▼
   universe ──ShardCmd(Add/Remove)──▶ shard 0..N (one websocket each)
                                          │ BookEvent (mpsc)
                                          ▼
                              collector (owns books + writer) ──▶ SBE file
```

| Module | Role |
|---|---|
| `universe` | Exchange-agnostic discovery loop; load-balances symbols across shards (least-loaded, capped per shard and in total). |
| `kalshi::connection` | Websocket IO for a shard: signed handshake, ping/stale detection, reconnect with exponential backoff, clean Close on shutdown. |
| `kalshi::subscription` | Pure protocol state machine: one `orderbook_delta` subscription per socket, `update_subscription` add/delete, per-`sid` sequence checking, resync on gaps. |
| `kalshi::messages` | Wire types and conversion into exchange-agnostic `BookEvent`s. |
| `collector` | Single writer: applies events to books, writes deltas, 60s snapshots, hourly rotation. |
| `orderbook`, `writer`, `fixed_point` | Book data structure, rotating `AlignedSbeWriter`, exact decimal parsing. |

Correctness guards:
- Sequence gaps and failed `update_subscription` commands resubscribe for
  fresh snapshots. A second resync within 10s drops the socket and backs off
  instead of looping.
- A book message that parses but can't be converted quarantines only that
  market. Its book is dropped rather than left wrong, until the next snapshot.
- Deltas are only forwarded after a market's snapshot.
- Messages from dropped subscriptions are ignored. Late acks for abandoned
  subscribes are unsubscribed.
- Partial or empty REST listings never cause mass unsubscribes.
- Commands queued while disconnected are applied before resubscribing.
- Reconnects use jittered exponential backoff. Initial connects are staggered
  by 100ms per shard.
- On SIGINT/SIGTERM the collector drains in-flight events, but closes the file
  within 10s even if a socket is wedged.

Known limitations (planned for the extensions):
- One socket per market set, so a disconnect means a gap until the
  resnapshot. Extension 1 adds redundant sockets.
- A book that diverges (a level going negative) is logged but only repaired
  on the next resync or reconnect. Extension 2 adds per-symbol health checks.
- Data is flushed in 64 MiB chunks (required by `AlignedSbeReader`). A hard
  crash or a second Ctrl-C loses the unflushed tail.

Scale: there are ~150k open non-MVE markets. Kalshi's Basic tier allows 200
concurrent websockets per user, so the defaults use ~150 sockets of 1000
markets each.

## Live verification (2026-10-27)

- **Correctness:** 200 markets were collected while Kalshi's REST orderbook
  for each one was fetched concurrently. Books were rebuilt from the SBE file
  as of each fetch time (`dump --final --as-of`).
  - **192/192 matched exactly**, with 0 mismatches.
  - 8 more were excluded because an update landed within 1s of the fetch.
- **Full scale:** 148,818 markets on 149 sockets (1000 per socket).
  - Every socket subscribed, with zero disconnects, resyncs, or errors.
  - ~1.3–1.6M deltas/min.
  - ~370 MB RSS, ~55% of one core.
  - Market churn (+36 / −138) was handled live.
  - SIGINT closed all 149 websockets and flushed the file in 0.2s.
- **Output volume:** ~640 MB in 4.5 min (744k snapshots, 5.2M updates).
  That's ~8.5 GB/hour, dominated by deltas.

## Tests

```bash
cargo test                      # unit tests + mock-websocket integration test
cargo test -- --ignored         # also hits the live Kalshi REST API
```
