//! Market discovery via `GET /markets`.

use std::collections::BTreeSet;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;

const PAGE_LIMIT: &str = "1000";
const MAX_RETRIES: u32 = 5;

#[derive(Debug, Deserialize)]
struct MarketsPage {
    markets: Vec<Market>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Market {
    ticker: String,
}

#[derive(Clone)]
pub struct RestClient {
    http: reqwest::Client,
    base_url: String,
}

impl RestClient {
    pub fn new(base_url: impl Into<String>) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .build()?;
        Ok(Self {
            http,
            base_url: base_url.into(),
        })
    }

    /// Returns the tickers of every open, non-multivariate market. Fails as a
    /// whole if any page fails, so callers never see a partial listing (which
    /// would look like markets closing).
    pub async fn open_market_tickers(&self) -> anyhow::Result<BTreeSet<String>> {
        let url = format!("{}/markets", self.base_url);
        let mut tickers = BTreeSet::new();
        let mut cursor: Option<String> = None;
        loop {
            let page = self.fetch_page(&url, cursor.as_deref()).await?;
            tickers.extend(page.markets.into_iter().map(|m| m.ticker));
            match page.cursor.filter(|c| !c.is_empty()) {
                Some(next) => cursor = Some(next),
                None => break,
            }
        }
        Ok(tickers)
    }

    async fn fetch_page(&self, url: &str, cursor: Option<&str>) -> anyhow::Result<MarketsPage> {
        let mut query = vec![
            ("status", "open"),
            ("mve_filter", "exclude"),
            ("limit", PAGE_LIMIT),
        ];
        if let Some(c) = cursor {
            query.push(("cursor", c));
        }

        let mut backoff = Duration::from_millis(500);
        for attempt in 1..=MAX_RETRIES {
            let response = self.http.get(url).query(&query).send().await;
            match response {
                Ok(r) if r.status().is_success() => {
                    return r.json().await.context("failed to decode markets page");
                }
                Ok(r)
                    if r.status() == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || r.status().is_server_error() =>
                {
                    tracing::debug!(status = %r.status(), attempt, "retrying markets page");
                }
                Ok(r) => anyhow::bail!("markets request failed with status {}", r.status()),
                Err(e) => tracing::debug!(error = %e, attempt, "retrying markets page"),
            }
            tokio::time::sleep(backoff).await;
            backoff *= 2;
        }
        anyhow::bail!("markets request failed after {MAX_RETRIES} attempts")
    }
}
