# polymarket_cate

独立的 Polymarket 市场同步服务，包含两条链路：

- 启动时执行一次全量市场同步到 PostgreSQL（若历史已做过则自动跳过）
- 订阅 Polymarket `market` websocket 的 `new_market` 事件，实时增量写入 PostgreSQL

## 环境变量

- `MARKET_SYNC_PG_URL` 或 `DATABASE_URL` 或 `PG_URL`：PostgreSQL 连接串（必填）
- `MARKET_SYNC_TABLE`：目标表名，默认 `polymarket_markets`
- `PG_SSL_VERIFY`：可选，是否校验证书（`true/false`，默认 `false`）
- `PG_SSL_ROOT_CERT`：可选，PostgreSQL TLS CA 文件路径（设置后会启用证书校验）
- `RUST_LOG`：日志级别，默认 `info`

## 本地运行

```bash
cd polymarket_cate
cp .env.example .env
cargo run
```

## PostgreSQL 表结构

服务启动后会自动创建目标表（若不存在），字段包括：

- `id` (pk), `condition_id`, `slug`, `question`
- `tags`, `neg_risk_market_id`, `neg_risk`
- `clob_token_ids`, `outcomes`
- `order_price_min_tick_size`, `order_min_size`, `fees_enabled`
- `end_date_iso`, `end_date`, `active`, `closed`
- `synced_at`

## Railway 部署

该目录包含 `railway.toml`，可直接作为 Railway service 根目录部署。

需要在 Railway Variables 中配置至少：

- `DATABASE_URL`
- `RUST_LOG=info`

可选：

- `MARKET_SYNC_TABLE`
- `PG_SSL_VERIFY`
- `PG_SSL_ROOT_CERT`
