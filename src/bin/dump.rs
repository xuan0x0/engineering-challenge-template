//! Prints the messages in an SBE output file, one per line, for inspection.
//!
//! Usage:
//!   `cargo run --bin dump -- <file.sbe> [SYMBOL]`          every message
//!   `cargo run --bin dump -- --final <file.sbe> [SYMBOL]`  reconstructed final books
//!   `cargo run --bin dump -- --final --as-of <RFC3339> <file.sbe> [SYMBOL]`
//!       books as of a receive time (messages received after it are ignored)

use std::collections::BTreeMap;

use engineering_challenge::events::Level;
use engineering_challenge::orderbook::OrderBook;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, PriceLevelUpdateDomain, SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, OrderbookSchema};

const USAGE: &str = "usage: dump [--final [--as-of <RFC3339>]] <file.sbe> [SYMBOL]";

fn main() -> anyhow::Result<()> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let final_books = args.first().is_some_and(|a| a == "--final");
    if final_books {
        args.remove(0);
    }
    let as_of = if args.first().is_some_and(|a| a == "--as-of") {
        args.remove(0);
        let raw = args.first().ok_or_else(|| anyhow::anyhow!(USAGE))?;
        let ts = chrono::DateTime::parse_from_rfc3339(raw)?.with_timezone(&chrono::Utc);
        args.remove(0);
        Some(ts)
    } else {
        None
    };
    let after_cutoff = |t: chrono::DateTime<chrono::Utc>| as_of.is_some_and(|cut| t > cut);
    let path = args.first().ok_or_else(|| anyhow::anyhow!(USAGE))?.clone();
    let symbol_filter = args.get(1).cloned();
    let wanted = |s: &str| symbol_filter.as_deref().is_none_or(|f| f == s);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(&path)?;
    let mut books: BTreeMap<String, OrderBook> = BTreeMap::new();
    let (mut snapshots, mut updates) = (0u64, 0u64);
    while let Some(msg) = reader.read_msg()? {
        match msg {
            AnyMessage::Snapshot(decoded) => {
                let s = SnapshotDomain::try_from_decoder(decoded)?;
                snapshots += 1;
                if !wanted(&s.symbol) || after_cutoff(s.received_at) {
                    continue;
                }
                if final_books {
                    let bids: Vec<Level> = s
                        .bids
                        .iter()
                        .map(|l| Level {
                            price: l.price_level,
                            quantity: l.quantity,
                        })
                        .collect();
                    let asks: Vec<Level> = s
                        .asks
                        .iter()
                        .map(|l| Level {
                            price: l.price_level,
                            quantity: l.quantity,
                        })
                        .collect();
                    books.insert(s.symbol, OrderBook::from_levels(&bids, &asks));
                } else {
                    print_snapshot(&s);
                }
            }
            AnyMessage::PriceLevelUpdate(decoded) => {
                let u = PriceLevelUpdateDomain::try_from_decoder(decoded)?;
                updates += 1;
                if !wanted(&u.symbol) || after_cutoff(u.backtest_timestamp) {
                    continue;
                }
                if final_books {
                    let book = books.entry(u.symbol.clone()).or_default();
                    let current = book.quantity_at(u.side, u.price_level);
                    book.apply_delta(u.side, u.price_level, u.quantity - current);
                } else {
                    print_update(&u);
                }
            }
            _ => anyhow::bail!("unknown message type"),
        }
    }
    if final_books {
        for (symbol, book) in &books {
            print_book(symbol, book);
        }
    }
    eprintln!("total: {snapshots} snapshots, {updates} updates");
    Ok(())
}

fn print_snapshot(s: &SnapshotDomain) {
    let best_bid = s.bids.first().map(|l| (l.price_level, l.quantity));
    let best_ask = s.asks.first().map(|l| (l.price_level, l.quantity));
    println!(
        "SNAP {} recv={} exch={} bids={} asks={} best_bid={best_bid:?} best_ask={best_ask:?}",
        s.symbol,
        s.received_at,
        s.exchange_timestamp,
        s.bids.len(),
        s.asks.len(),
    );
}

fn print_update(u: &PriceLevelUpdateDomain) {
    println!(
        "UPD  {} recv={} exch={} {} price={} qty={}",
        u.symbol, u.backtest_timestamp, u.exchange_timestamp, u.side, u.price_level, u.quantity,
    );
}

/// One line per book: `BOOK <symbol> bids=price:qty,... asks=price:qty,...`,
/// best level first on each side.
fn print_book(symbol: &str, book: &OrderBook) {
    let snap = book.to_snapshot(
        symbol,
        chrono::DateTime::UNIX_EPOCH,
        chrono::DateTime::UNIX_EPOCH,
    );
    let fmt = |levels: Vec<(i64, i64)>| {
        levels
            .iter()
            .map(|(p, q)| format!("{p}:{q}"))
            .collect::<Vec<_>>()
            .join(",")
    };
    let bids = snap
        .bids
        .iter()
        .map(|l| (l.price_level, l.quantity))
        .collect();
    let asks = snap
        .asks
        .iter()
        .map(|l| (l.price_level, l.quantity))
        .collect();
    println!("BOOK {symbol} bids={} asks={}", fmt(bids), fmt(asks));
}
