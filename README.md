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

| Flag | Default | Meaning |
|---|---|---|
| `--max-markets N` | all | Only subscribe to the first N markets (for testing). |
| `--replicas` | 2 | Redundant websockets per set of markets. |
| `--failover-lag-ms` | 500 | Fail over when a backup is ahead of the source for this long. |
| `--recycle-interval-secs` | 1800 | Proactively reconnect each socket (staggered across replicas); 0 disables. |
| `--max-connections` | 180 | Total websocket budget (Kalshi Basic allows 200 per user). |
| `--markets-per-connection` | 1000 | Target markets per socket (exceeded once the budget is reached). |
| `--discovery-interval-secs` | 120 | Market list refresh. |
| `--snapshot-interval-secs` | 60 | Full snapshot of every book. |

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
  book so it can be replayed on its own. Snapshots are also written when a
  market first appears, when the source exchange connection resnapshots, and
  whenever the published source switches to another redundant socket.

## Design

```
            REST /markets (every 120s)
                     │ diff
                     ▼
   universe ──ShardCmd(Add/Remove)──▶ shard 0..N ─┬─ replica 0 (websocket)
                                                  └─ replica 1 (websocket)
                                                        │ SourcedEvent (mpsc)
                                                        ▼
                     collector: MarketState per symbol (book per replica,
                                one published "source") ──▶ SBE file
```

| Module | Role |
|---|---|
| `universe` | Exchange-agnostic discovery loop; load-balances symbols across shards (least-loaded, capped per shard and in total). |
| `kalshi::connection` | Websocket IO for a shard: signed handshake, ping/stale detection, reconnect with exponential backoff, clean Close on shutdown. |
| `kalshi::subscription` | Pure protocol state machine: one `orderbook_delta` subscription per socket, `update_subscription` add/delete, per-`sid` sequence checking, resync on gaps. |
| `kalshi::messages` | Wire types and conversion into exchange-agnostic `BookEvent`s. |
| `market` | Pure per-symbol failover logic: a book per replica, publishes only the source, switches source (emitting a snapshot) on invalidation or lag. |
| `collector` | Single writer: routes events to `MarketState`, checks lag every 100ms, 60s snapshots, hourly rotation. |
| `orderbook`, `writer`, `fixed_point` | Book data structure, rotating `AlignedSbeWriter`, exact decimal parsing. |

### Redundant websockets (Extension 1)

Every shard's markets are subscribed on `--replicas` independent sockets.
The collector keeps each replica's book and publishes one replica (the
source). It switches source and writes a snapshot of the new source's book
when either of these happens:

- **Invalidation:** the source disconnects, resyncs after a sequence gap, or
  quarantines the market. Its book is untrustworthy, so it switches
  immediately.
- **Lag:** a backup has delivered a delta with a newer Kalshi `clock` than
  anything the source has seen, and the source hasn't caught up within
  `--failover-lag-ms`. This catches silently stalled or buffered sockets.
  Snapshot clocks only set the baseline, because a fresh snapshot's clock can
  exceed the last delta's even when nothing changed.

Sockets are also recycled every `--recycle-interval-secs` (part of
Extension 2). The replicas of a shard are offset by half a cycle, so one is
always live. The redundant set doubles inbound traffic, so with 180
connections the defaults use 90 shards of ~1650 markets.

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

Known limitations:
- A diverging book (a level going negative) is logged, and is only repaired
  by the next resnapshot or a failover. Per-symbol rate monitoring (the rest
  of Extension 2) is not implemented.
- If every replica of a shard is down at once, that shard has a gap until the
  first replica resnapshots. The gap is visible as a snapshot after missing
  time, never as a silently wrong book.
- Data is flushed in 64 MiB chunks (required by `AlignedSbeReader`). A hard
  crash or a second Ctrl-C loses the unflushed tail.

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
- **Failover under forced churn:** 200 markets × 2 replicas, with every socket
  recycled every 40s, while REST orderbooks were fetched concurrently.
  - 8 recycles caused ~700 source switches. Every switch landed in the same
    second as a recycle, with no flapping.
  - **195/195 reconstructed books matched exactly**, with 0 mismatches.
  - 5 more were excluded because an update landed within 1s of the fetch.
- **Full scale with redundancy:** 147,079 markets on 180 sockets (90 shards ×
  2 replicas, ~1650 markets per socket).
  - Every socket subscribed, with zero disconnects or errors.
  - ~1.47M deltas/min published.
  - ~500 MB RSS, ~88% of one core.
  - SIGINT closed all 180 sockets in 0.3s.

## Tests

```bash
cargo test                      # unit tests + mock-websocket integration test
cargo test -- --ignored         # also hits the live Kalshi REST API
```
