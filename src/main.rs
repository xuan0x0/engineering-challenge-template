use std::time::{Duration, Instant};

use engineering_challenge::kalshi::rest::fetch_open_markets;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;

    let start = Instant::now();
    let markets = fetch_open_markets(&client).await?;
    eprintln!(
        "fetched {} open markets in {:.1?}",
        markets.len(),
        start.elapsed()
    );

    for m in markets.iter().take(5) {
        eprintln!("{} {} closes {}", m.ticker, m.status, m.close_time);
    }

    Ok(())
}
