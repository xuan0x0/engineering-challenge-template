//! Everything Kalshi-specific: authentication, market discovery, the websocket
//! wire format, and the per-shard connection task.

pub mod auth;
pub mod connection;
pub mod messages;
pub mod rest;
pub mod subscription;

pub const DEFAULT_WS_URL: &str = "wss://external-api-ws.kalshi.com/trade-api/ws/v2";
pub const DEFAULT_REST_URL: &str = "https://external-api.kalshi.com/trade-api/v2";
