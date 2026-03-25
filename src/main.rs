use std::collections::{HashMap, HashSet};
use std::env;
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use openssl::ssl::{SslConnector, SslMethod, SslVerifyMode};
use postgres_openssl::MakeTlsConnector;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::signal;
use tokio_postgres::config::Host;
use tracing::{error, info, warn};

const GAMMA_API_BASE: &str = "https://gamma-api.polymarket.com";
const MARKET_WSS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const DEFAULT_TABLE: &str = "polymarket_markets";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MarketLite {
    id: String,
    #[serde(default)]
    condition_id: Option<String>,
    #[serde(default)]
    slug: Option<String>,
    #[serde(default)]
    question: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
    #[serde(default)]
    neg_risk_market_id: Option<String>,
    #[serde(default)]
    neg_risk: bool,
    #[serde(default)]
    clob_token_ids: Vec<String>,
    #[serde(default)]
    outcomes: Vec<String>,
    #[serde(default)]
    order_price_min_tick_size: Option<f64>,
    #[serde(default)]
    order_min_size: Option<f64>,
    #[serde(default)]
    fees_enabled: bool,
    #[serde(default)]
    end_date_iso: Option<String>,
    #[serde(default)]
    end_date: Option<i64>,
    #[serde(default)]
    active: bool,
    #[serde(default)]
    closed: bool,
}

#[derive(Debug, Deserialize)]
struct NewMarketEvent {
    id: String,
    #[serde(rename = "assets_ids", alias = "asset_ids", default)]
    asset_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct Settings {
    pg_url: String,
    table: String,
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().ok();
    init_logging();

    let settings = match load_settings() {
        Ok(v) => v,
        Err(e) => {
            error!("{e}");
            std::process::exit(2);
        }
    };

    info!("starting polymarket_cate: table={}", settings.table);
    if let Err(e) = run_startup_full_sync_once(&settings).await {
        warn!("startup full-sync failed: {e}");
    }

    let ws_task = tokio::spawn(run_new_market_ws_loop(settings.clone()));

    tokio::select! {
        _ = signal::ctrl_c() => {
            info!("ctrl-c received, shutting down");
        }
        res = ws_task => {
            warn!("ws task exited: {:?}", res);
        }
    }
}

fn init_logging() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,hyper=warn,reqwest=warn,tungstenite=warn".into()),
        )
        .with_target(false)
        .try_init();
}

fn load_settings() -> Result<Settings, String> {
    let pg_url = env::var("MARKET_SYNC_PG_URL")
        .ok()
        .or_else(|| env::var("DATABASE_URL").ok())
        .or_else(|| env::var("PG_URL").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "missing PG URL: set MARKET_SYNC_PG_URL or DATABASE_URL".to_string())?;

    let table = env::var("MARKET_SYNC_TABLE")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| DEFAULT_TABLE.to_string());

    Ok(Settings { pg_url, table })
}

async fn run_new_market_ws_loop(settings: Settings) {
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        let res = run_new_market_ws_once(&settings).await;
        if let Err(e) = res {
            warn!("market ws loop error: {e}");
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(max_backoff);
    }
}

async fn run_new_market_ws_once(settings: &Settings) -> Result<(), String> {
    let (mut ws, _) = tokio_tungstenite::connect_async(MARKET_WSS_URL)
        .await
        .map_err(|e| format!("ws connect failed: {e}"))?;

    let subscription = serde_json::json!({
        "type": "market",
        "operation": "subscribe",
        "markets": [],
        "assets_ids": [],
        "initial_dump": false,
        "custom_feature_enabled": true
    });
    ws.send(tokio_tungstenite::tungstenite::Message::Text(
        subscription.to_string(),
    ))
    .await
    .map_err(|e| format!("ws subscribe failed: {e}"))?;

    info!("market ws subscribed for new_market events");

    while let Some(msg) = ws.next().await {
        match msg {
            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                if let Err(e) = handle_ws_text_message(settings, &text).await {
                    warn!("ws message handle failed: {e}");
                }
            }
            Ok(tokio_tungstenite::tungstenite::Message::Ping(payload)) => {
                ws.send(tokio_tungstenite::tungstenite::Message::Pong(payload))
                    .await
                    .map_err(|e| format!("send pong failed: {e}"))?;
            }
            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                return Err("ws closed by server".to_string());
            }
            Ok(_) => {}
            Err(e) => return Err(format!("ws read failed: {e}")),
        }
    }

    Err("ws stream ended".to_string())
}

async fn handle_ws_text_message(settings: &Settings, text: &str) -> Result<(), String> {
    let value: Value =
        serde_json::from_str(text).map_err(|e| format!("parse ws json failed: {e}"))?;
    match value {
        Value::Object(obj) => {
            maybe_handle_ws_event(settings, Value::Object(obj)).await?;
        }
        Value::Array(items) => {
            for v in items {
                maybe_handle_ws_event(settings, v).await?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn maybe_handle_ws_event(settings: &Settings, v: Value) -> Result<(), String> {
    let event_type = v
        .get("event_type")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if event_type != "new_market" {
        return Ok(());
    }

    let ev: NewMarketEvent =
        serde_json::from_value(v).map_err(|e| format!("decode new_market failed: {e}"))?;

    let market = if let Some(first_token) = ev.asset_ids.first() {
        fetch_market_by_token_id(first_token).await?
    } else {
        None
    };

    if let Some(market) = market {
        upsert_markets_to_pg(&settings.pg_url, &settings.table, &[market.clone()]).await?;
        info!(
            "new_market synced to pg: id={}, token_count={}",
            market.id,
            market.clob_token_ids.len()
        );
    } else {
        warn!(
            "new_market received but market details not found from gamma: id={}, assets={}",
            ev.id,
            ev.asset_ids.len()
        );
    }

    Ok(())
}

async fn fetch_full_market_catalog() -> Result<Vec<MarketLite>, String> {
    let client = Client::builder()
        .no_proxy()
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;

    let mut offset = 0;
    let limit = 500;
    let mut out = Vec::new();

    loop {
        let resp = client
            .get(format!("{GAMMA_API_BASE}/markets"))
            .query(&[
                ("limit", limit.to_string()),
                ("offset", offset.to_string()),
                ("order", "id".to_string()),
                ("ascending", "false".to_string()),
                ("closed", "false".to_string()),
                ("active", "true".to_string()),
                ("include_tag", "true".to_string()),
            ])
            .send()
            .await
            .map_err(|e| format!("market fetch failed: {e}"))?;

        if !resp.status().is_success() {
            return Err(format!("market fetch status: {}", resp.status()));
        }

        let batch: Vec<Value> = resp
            .json()
            .await
            .map_err(|e| format!("market parse failed: {e}"))?;
        if batch.is_empty() {
            break;
        }

        for item in batch.iter().filter_map(parse_market_lite) {
            if is_active_and_unexpired(&item) {
                out.push(item);
            }
        }

        if batch.len() < limit as usize {
            break;
        }
        offset += limit;
        if offset > 200_000 {
            break;
        }
    }

    Ok(out)
}

async fn fetch_market_by_token_id(token_id: &str) -> Result<Option<MarketLite>, String> {
    let client = Client::builder()
        .no_proxy()
        .build()
        .map_err(|e| format!("http client build failed: {e}"))?;

    for key in ["clobTokenIds", "clob_token_ids", "token_ids", "tokenIds"] {
        let resp = client
            .get(format!("{GAMMA_API_BASE}/markets"))
            .query(&[
                (key, token_id.to_string()),
                ("include_tag", "true".to_string()),
            ])
            .send()
            .await
            .map_err(|e| format!("market fetch failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("market fetch status: {}", resp.status()));
        }
        let batch: Vec<Value> = resp
            .json()
            .await
            .map_err(|e| format!("market parse failed: {e}"))?;
        for m in batch {
            if let Some(market) = parse_market_lite(&m) {
                if market.clob_token_ids.iter().any(|t| t == token_id)
                    && is_active_and_unexpired(&market)
                {
                    return Ok(Some(market));
                }
            }
        }
    }
    Ok(None)
}

fn parse_market_lite(v: &Value) -> Option<MarketLite> {
    let id = id_to_string(v.get("id")?)?;
    let clob_token_ids = parse_string_array(v.get("clobTokenIds"));
    let outcomes = parse_string_array(v.get("outcomes"));
    let order_price_min_tick_size = v
        .get("orderPriceMinTickSize")
        .and_then(Value::as_f64)
        .or_else(|| {
            v.get("orderPriceMinTickSize")
                .and_then(Value::as_str)
                .and_then(|s| s.parse::<f64>().ok())
        });
    let order_min_size = v.get("orderMinSize").and_then(Value::as_f64).or_else(|| {
        v.get("orderMinSize")
            .and_then(Value::as_str)
            .and_then(|s| s.parse::<f64>().ok())
    });
    let active = v.get("active").and_then(Value::as_bool).unwrap_or(true);
    let closed = v.get("closed").and_then(Value::as_bool).unwrap_or(false);
    let fees_enabled = v
        .get("feesEnabled")
        .or_else(|| v.get("fees_enabled"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let tags = parse_tags(v.get("tags"));
    let neg_risk_market_id = v
        .get("negRiskMarketId")
        .or_else(|| v.get("neg_risk_market_id"))
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let neg_risk_flag = v
        .get("negRisk")
        .or_else(|| v.get("neg_risk"))
        .and_then(Value::as_bool)
        .unwrap_or(false);

    Some(MarketLite {
        id,
        condition_id: v
            .get("conditionId")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        slug: v
            .get("slug")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        question: v
            .get("question")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        tags,
        neg_risk_market_id: neg_risk_market_id.clone(),
        neg_risk: neg_risk_flag || neg_risk_market_id.is_some(),
        clob_token_ids,
        outcomes,
        order_price_min_tick_size,
        order_min_size,
        fees_enabled,
        end_date_iso: v
            .get("endDateIso")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        end_date: v.get("endDate").and_then(Value::as_i64),
        active,
        closed,
    })
}

fn id_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn parse_string_array(v: Option<&Value>) -> Vec<String> {
    let Some(v) = v else {
        return Vec::new();
    };
    if let Some(arr) = v.as_array() {
        return arr
            .iter()
            .filter_map(|x| x.as_str().map(ToString::to_string))
            .collect();
    }
    if let Some(s) = v.as_str() {
        if let Ok(arr) = serde_json::from_str::<Vec<String>>(s) {
            return arr;
        }
    }
    Vec::new()
}

fn parse_tags(v: Option<&Value>) -> Vec<String> {
    let Some(v) = v else {
        return Vec::new();
    };
    if let Some(arr) = v.as_array() {
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            if let Some(s) = item.as_str() {
                let t = s.trim();
                if !t.is_empty() {
                    out.push(t.to_string());
                }
            } else if let Some(label) = item.get("label").and_then(Value::as_str) {
                let t = label.trim();
                if !t.is_empty() {
                    out.push(t.to_string());
                }
            } else if let Some(slug) = item.get("slug").and_then(Value::as_str) {
                let t = slug.trim();
                if !t.is_empty() {
                    out.push(t.to_string());
                }
            }
        }
        return out;
    }
    Vec::new()
}

fn is_active_and_unexpired(m: &MarketLite) -> bool {
    if !m.active || m.closed {
        return false;
    }
    let now = chrono::Utc::now();
    if let Some(end_iso) = m.end_date_iso.as_deref() {
        if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(end_iso) {
            if dt.with_timezone(&chrono::Utc) <= now {
                return false;
            }
        }
    }
    if let Some(end_ts) = m.end_date {
        if end_ts > 0 && end_ts <= now.timestamp() {
            return false;
        }
    }
    true
}

async fn full_sync_to_pg(
    pg_url: &str,
    table_raw: &str,
    incoming: &[MarketLite],
) -> Result<(usize, usize, usize), String> {
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    ensure_schema(pg_url, &table).await?;

    let existing_ids = load_existing_ids(pg_url, &table).await?;
    let incoming_ids: HashSet<String> = incoming.iter().map(|m| m.id.clone()).collect();
    let delete_ids: Vec<String> = existing_ids.difference(&incoming_ids).cloned().collect();

    let upserts = upsert_markets_to_pg(pg_url, &table, incoming).await?;
    let deleted = delete_markets_from_pg(pg_url, &table, &delete_ids).await?;
    Ok((upserts, deleted, incoming.len()))
}

async fn run_startup_full_sync_once(settings: &Settings) -> Result<(), String> {
    if has_full_sync_done(&settings.pg_url, &settings.table).await? {
        info!("startup full-sync skipped: already completed before");
        return Ok(());
    }

    info!("startup full-sync begin");
    let markets = fetch_full_market_catalog().await?;
    let (upserts, deleted, total) =
        full_sync_to_pg(&settings.pg_url, &settings.table, &markets).await?;
    mark_full_sync_done(&settings.pg_url, &settings.table).await?;
    info!(
        "startup full-sync done: total={}, upserts={}, deleted={}",
        total, upserts, deleted
    );
    Ok(())
}

async fn upsert_markets_to_pg(
    pg_url: &str,
    table_raw: &str,
    markets: &[MarketLite],
) -> Result<usize, String> {
    if markets.is_empty() {
        return Ok(0);
    }
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    let (client, _handle) = connect_pg(pg_url).await?;
    ensure_schema_with_client(&client, &table).await?;

    let upsert_sql = format!(
        "insert into {table} (
            id, condition_id, slug, question, tags, neg_risk_market_id, neg_risk,
            clob_token_ids, outcomes, order_price_min_tick_size, order_min_size, fees_enabled,
            end_date_iso, end_date, active, closed, synced_at
        )
        values (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11, $12,
            $13, $14, $15, $16, now()
        )
        on conflict (id) do update set
            condition_id = excluded.condition_id,
            slug = excluded.slug,
            question = excluded.question,
            tags = excluded.tags,
            neg_risk_market_id = excluded.neg_risk_market_id,
            neg_risk = excluded.neg_risk,
            clob_token_ids = excluded.clob_token_ids,
            outcomes = excluded.outcomes,
            order_price_min_tick_size = excluded.order_price_min_tick_size,
            order_min_size = excluded.order_min_size,
            fees_enabled = excluded.fees_enabled,
            end_date_iso = excluded.end_date_iso,
            end_date = excluded.end_date,
            active = excluded.active,
            closed = excluded.closed,
            synced_at = now()",
        table = table
    );

    for m in markets {
        let tags: Vec<String> = m
            .tags
            .iter()
            .map(|t| normalize_tag_for_pg(t))
            .filter(|t| !t.is_empty())
            .collect();
        client
            .execute(
                &upsert_sql,
                &[
                    &m.id,
                    &m.condition_id,
                    &m.slug,
                    &m.question,
                    &tags,
                    &m.neg_risk_market_id,
                    &m.neg_risk,
                    &m.clob_token_ids,
                    &m.outcomes,
                    &m.order_price_min_tick_size,
                    &m.order_min_size,
                    &m.fees_enabled,
                    &m.end_date_iso,
                    &m.end_date,
                    &m.active,
                    &m.closed,
                ],
            )
            .await
            .map_err(|e| format!("upsert market {} failed: {e}", m.id))?;
    }
    Ok(markets.len())
}

async fn delete_markets_from_pg(
    pg_url: &str,
    table_raw: &str,
    ids: &[String],
) -> Result<usize, String> {
    if ids.is_empty() {
        return Ok(0);
    }
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    let (client, _handle) = connect_pg(pg_url).await?;
    let sql = format!("delete from {table} where id = any($1)", table = table);
    let changed = client
        .execute(&sql, &[&ids])
        .await
        .map_err(|e| format!("delete removed markets failed: {e}"))?;
    Ok(changed as usize)
}

async fn load_existing_ids(pg_url: &str, table_raw: &str) -> Result<HashSet<String>, String> {
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    let (client, _handle) = connect_pg(pg_url).await?;
    ensure_schema_with_client(&client, &table).await?;
    let sql = format!("select id from {table}", table = table);
    let rows = client
        .query(&sql, &[])
        .await
        .map_err(|e| format!("load existing ids failed: {e}"))?;
    let mut out = HashSet::with_capacity(rows.len());
    for row in rows {
        let id: String = row.get(0);
        out.insert(id);
    }
    Ok(out)
}

async fn ensure_schema(pg_url: &str, table_raw: &str) -> Result<(), String> {
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    let (client, _handle) = connect_pg(pg_url).await?;
    ensure_schema_with_client(&client, &table).await
}

async fn ensure_schema_with_client(
    client: &tokio_postgres::Client,
    table: &str,
) -> Result<(), String> {
    let schema_sql = format!(
        "create table if not exists {table} (
            id text primary key,
            condition_id text,
            slug text,
            question text,
            tags text[] not null default '{{}}',
            neg_risk_market_id text,
            neg_risk boolean not null default false,
            clob_token_ids text[] not null default '{{}}',
            outcomes text[] not null default '{{}}',
            order_price_min_tick_size double precision,
            order_min_size double precision,
            fees_enabled boolean not null default false,
            end_date_iso text,
            end_date bigint,
            active boolean not null default true,
            closed boolean not null default false,
            synced_at timestamptz not null default now()
        );
        create table if not exists market_sync_state (
            table_name text primary key,
            full_sync_done boolean not null default false,
            full_sync_at timestamptz,
            updated_at timestamptz not null default now()
        );
        create index if not exists {table}_slug_idx on {table}(slug);
        create index if not exists {table}_active_idx on {table}(active, closed);",
        table = table
    );
    client
        .batch_execute(&schema_sql)
        .await
        .map_err(|e| format!("schema ensure failed: {e}"))?;
    Ok(())
}

async fn has_full_sync_done(pg_url: &str, table_raw: &str) -> Result<bool, String> {
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    let (client, _handle) = connect_pg(pg_url).await?;
    ensure_schema_with_client(&client, &table).await?;
    let row = client
        .query_opt(
            "select full_sync_done from market_sync_state where table_name = $1",
            &[&table],
        )
        .await
        .map_err(|e| format!("load market_sync_state failed: {e}"))?;
    Ok(row.map(|r| r.get::<usize, bool>(0)).unwrap_or(false))
}

async fn mark_full_sync_done(pg_url: &str, table_raw: &str) -> Result<(), String> {
    let table = sanitize_ident(table_raw).ok_or_else(|| format!("invalid table: {table_raw}"))?;
    let (client, _handle) = connect_pg(pg_url).await?;
    ensure_schema_with_client(&client, &table).await?;
    client
        .execute(
            "insert into market_sync_state(table_name, full_sync_done, full_sync_at, updated_at)
             values ($1, true, now(), now())
             on conflict(table_name) do update set
               full_sync_done = true,
               full_sync_at = now(),
               updated_at = now()",
            &[&table],
        )
        .await
        .map_err(|e| format!("update market_sync_state failed: {e}"))?;
    Ok(())
}

async fn connect_pg(
    pg_url: &str,
) -> Result<(tokio_postgres::Client, tokio::task::JoinHandle<()>), String> {
    let config: tokio_postgres::Config = pg_url
        .parse()
        .map_err(|e| format!("pg url parse failed: {e}"))?;

    let hosts = config.get_hosts();
    if hosts.is_empty() {
        return Err("pg host missing".to_string());
    }

    let mut builder =
        SslConnector::builder(SslMethod::tls()).map_err(|e| format!("ssl builder failed: {e}"))?;
    if let Ok(ca_file) = env::var("PG_SSL_ROOT_CERT") {
        let ca_file = ca_file.trim();
        if !ca_file.is_empty() {
            builder
                .set_ca_file(ca_file)
                .map_err(|e| format!("set ca file failed: {e}"))?;
        } else {
            builder
                .set_default_verify_paths()
                .map_err(|e| format!("set default verify paths failed: {e}"))?;
        }
    } else {
        builder
            .set_default_verify_paths()
            .map_err(|e| format!("set default verify paths failed: {e}"))?;
    }
    builder.set_verify(SslVerifyMode::PEER);

    match &hosts[0] {
        Host::Tcp(_) => {}
        _ => return Err("only TCP pg hosts are supported".to_string()),
    }
    let connector = MakeTlsConnector::new(builder.build());

    let (client, conn) = config
        .connect(connector)
        .await
        .map_err(|e| format!("pg connect failed: {e}"))?;

    let handle = tokio::spawn(async move {
        if let Err(e) = conn.await {
            warn!("pg connection error: {e}");
        }
    });
    Ok((client, handle))
}

fn sanitize_ident(ident: &str) -> Option<String> {
    let id = ident.trim();
    if id.is_empty() {
        return None;
    }
    if id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '.')
    {
        Some(id.to_string())
    } else {
        None
    }
}

fn normalize_tag_for_pg(tag: &str) -> String {
    let t = tag.trim();
    if t.is_empty() {
        return String::new();
    }
    let words = t
        .replace('_', " ")
        .replace('-', " ")
        .split_whitespace()
        .map(|w| {
            let mut chars = w.chars();
            match chars.next() {
                Some(first) => {
                    let mut out = String::new();
                    out.extend(first.to_uppercase());
                    out.push_str(&chars.as_str().to_lowercase());
                    out
                }
                None => String::new(),
            }
        })
        .collect::<Vec<_>>();
    words.join(" ")
}

#[allow(dead_code)]
fn _market_hash(m: &MarketLite) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    m.id.hash(&mut h);
    m.condition_id.hash(&mut h);
    m.slug.hash(&mut h);
    m.question.hash(&mut h);
    m.tags.hash(&mut h);
    m.neg_risk_market_id.hash(&mut h);
    m.neg_risk.hash(&mut h);
    m.clob_token_ids.hash(&mut h);
    m.outcomes.hash(&mut h);
    m.order_price_min_tick_size.map(f64::to_bits).hash(&mut h);
    m.order_min_size.map(f64::to_bits).hash(&mut h);
    m.fees_enabled.hash(&mut h);
    m.end_date_iso.hash(&mut h);
    m.end_date.hash(&mut h);
    m.active.hash(&mut h);
    m.closed.hash(&mut h);
    h.finish()
}

#[allow(dead_code)]
fn _markets_by_id(markets: &[MarketLite]) -> HashMap<String, MarketLite> {
    markets
        .iter()
        .cloned()
        .map(|m| (m.id.clone(), m))
        .collect::<HashMap<_, _>>()
}
