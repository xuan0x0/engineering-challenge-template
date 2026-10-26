use reqwest::StatusCode;
use serde::Deserialize;
use std::collections::HashSet;
use std::time::{Duration, Instant};
use tokio::time::sleep;

const BASE_URL: &str = "https://api.elections.kalshi.com/trade-api/v2";
const MAX_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, Deserialize)]
pub struct Market {
    pub ticker: String,
    pub event_ticker: String,
    pub status: String,
    pub close_time: String,
    pub mve_collection_ticker: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MarketsResponse {
    markets: Vec<Market>,
    cursor: Option<String>,
}

async fn fetch_page(
    client: &reqwest::Client,
    cursor: Option<&str>,
) -> anyhow::Result<MarketsResponse> {
    let mut query = vec![
        ("status", "open"),
        ("mve_filter", "exclude"),
        ("limit", "1000"),
    ];

    if let Some(c) = cursor {
        query.push(("cursor", c));
    }

    let page = client
        .get(format!("{BASE_URL}/markets"))
        .query(&query)
        .send()
        .await?
        .error_for_status()?
        .json::<MarketsResponse>()
        .await?;

    Ok(page)
}

/// Rate limits (429), server errors (5xx) and transport failures such as
/// timeouts are worth retrying. Other HTTP errors (400, 404, ...) will fail
/// the same way every time.
fn is_retryable(error: &anyhow::Error) -> bool {
    match error.downcast_ref::<reqwest::Error>().and_then(reqwest::Error::status) {
        Some(status) => status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error(),
        None => true,
    }
}

pub async fn fetch_open_markets(client: &reqwest::Client) -> anyhow::Result<Vec<Market>> {
    let mut markets = Vec::new();
    let mut cursor: Option<String> = None;
    let mut page_number = 0;

    loop {
        page_number += 1;
        let start = Instant::now();
        let mut attempt: u32 = 0;

        let page = loop {
            match fetch_page(client, cursor.as_deref()).await {
                Ok(page) => break page,

                Err(error) => {
                    eprintln!(
                        "page {page_number} attempt {}/{MAX_ATTEMPTS} failed: {error:#}",
                        attempt + 1
                    );

                    if attempt + 1 == MAX_ATTEMPTS || !is_retryable(&error) {
                        return Err(error);
                    }

                    sleep(Duration::from_secs(1 << attempt)).await;
                    attempt += 1;
                }
            }
        };

        eprintln!(
            "page {page_number}: {} markets, elapsed {:.1?}",
            page.markets.len(),
            start.elapsed()
        );

        markets.extend(
            page.markets
                .into_iter()
                .filter(|m| m.mve_collection_ticker.as_deref().is_none_or(str::is_empty)),
        );

        match page.cursor {
            Some(c) if !c.is_empty() => cursor = Some(c),
            _ => break,
        }
    }

    // The market set changes while we paginate, so the same ticker can show
    // up on two pages. Keep the first occurrence.
    let mut seen = HashSet::new();
    let before = markets.len();
    markets.retain(|m| seen.insert(m.ticker.clone()));
    eprintln!("dropped {} duplicate markets", before - markets.len());

    Ok(markets)
}
