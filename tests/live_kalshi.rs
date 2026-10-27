//! Hits the live Kalshi REST API. Run with `cargo test -- --ignored`.

use engineering_challenge::kalshi::DEFAULT_REST_URL;
use engineering_challenge::kalshi::rest::RestClient;

#[tokio::test]
#[ignore = "requires network access to Kalshi"]
async fn lists_open_non_mve_markets() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let client = RestClient::new(DEFAULT_REST_URL).unwrap();
    let tickers = client.open_market_tickers().await.unwrap();
    eprintln!("open non-MVE markets: {}", tickers.len());
    assert!(!tickers.is_empty());
    assert!(tickers.iter().all(|t| !t.is_empty()));
}
