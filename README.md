# Nebula

The core of a perpetual-futures exchange in TypeScript/Bun, live at [nebula.ashutoshsao.com](https://nebula.ashutoshsao.com/trade/BTC-PERP): an HTTP API at the edge, Redis Streams as the command bus, and a single in-memory matching engine that owns all trading state.

Covers matching, margin locks, position netting, liquidation, funding-rate settlement, and snapshot recovery.

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="./perps-v2.svg">
  <source media="(prefers-color-scheme: light)" srcset="./perps-v2-light.svg">
  <img alt="Architecture diagram" src="./perps-v2.svg">
</picture>

## Core Idea

The API never mutates exchange state directly. `POST /api/order` becomes a typed `create_order` command with a `correlationId`, written to Redis. The engine reads commands sequentially, runs matching/risk logic, and writes the result back. That gives the order book a single authority, with no races.

Engine state lives in memory:

- `ORDERBOOKS` — per-symbol bid/ask trees (`sorted-btree`)
- `ORDERS` / `POSITIONS` / `BALANCES` — per-user records
- `MARKETS` — symbol config (max leverage, min qty)
- `INDEX_PRICES` — mark prices driving liquidation checks

## What It Can Do

- Signup/signin with Argon2 + JWT
- Admin market creation, USD balance onramp
- Limit + market orders, matching against best liquidity
- Margin locking/refunding, position netting (increase/reduce/close/flip)
- Liquidation on index-price updates, with ADL fallback
- Funding-rate settlement at 00:00 / 08:00 / 16:00 UTC
- Live Binance mark-price feed driving index prices
- Simple market-maker + degen taker bots for local liquidity
- Engine snapshots persisted to Cloudflare R2, with stream-ID recovery on restart
- Integration tests across API + engine

## Performance

The matching engine handles **~200k orders/sec with p99 ~40µs** over 1M orders (in-process, Apple Silicon laptop, no Redis or network). Reproduce with:

```sh
cd apps/engine && bun run bench          # 1M orders by default
bun run bench 100000                     # or pass an order count
```

The benchmark ([apps/engine/bench/matching.bench.ts](./apps/engine/bench/matching.bench.ts)) seeds a book, then sends a deterministic mix of 80% limit orders (many crossing) and 20% market orders through `handleCommand`.

Writing it exposed a bug: when a buy ran out partway through a price level, the asks it had filled at that level were never removed from the book, producing zero-quantity fills and slowing every later buy. Fixing it (25218b0) took throughput from ~11k to ~320k orders/sec on 100k orders.

## Monorepo Map

```txt
apps/
  api/           Express API, auth, validation, Redis loopback
  engine/        Matching engine, risk logic, snapshots, command dispatcher
  price-feeder/  Binance mark-price feed -> index-price commands
  market-maker/  Local liquidity bots (quoting + taking)
  db-poller/     Persists engine events into Postgres/TimescaleDB
  wss/           Realtime market/user channels
  web/           Vite + React exchange UI
  tests/         Bun tests covering API + engine behavior

packages/
  db/            Prisma client + Postgres schema
  redis/         Redis client factory
  types/         Shared Zod schemas, command types, Redis keys
  ui/            Shared UI package
```

## Request Lifecycle

1. `apps/api/controller/exchange.controller.ts` validates the body (Zod).
2. `apps/api/service/loopBack.ts` writes a command to Redis with a `correlationId`.
3. `apps/engine/index.ts` reads the next command off the stream.
4. `apps/engine/src/controller/engine.controller.ts` dispatches it (e.g. `createOrder`).
5. The handler checks limits, locks margin, matches liquidity, updates fills/positions.
6. The engine writes the result to a response queue or the global event stream.
7. The API resolves the original HTTP request via the correlation ID.

## Tech Stack

Bun workspaces · TypeScript · Express 5 · Redis Streams · PostgreSQL/TimescaleDB · Prisma · Zod · Argon2 · JWT · sorted-btree · Vite

## Local Setup

```sh
bun install
```

You'll need Redis and Postgres/TimescaleDB running, plus env files for `packages/db`, `packages/redis`, `apps/engine`, `apps/api` (see `packages/redis/example.env`, `apps/web/.env.example`). Minimum vars:

```sh
PORT=4000
DATABASE_URL=postgresql://USER:PASSWORD@HOST:PORT/DB
REDIS_URL=redis://localhost:6379
JWT_SECRET=dev-jwt-secret
ADMIN_SECRET=dev-admin-secret
PORT_WSS=3030
```

Engine snapshots go to Cloudflare R2, so the engine also needs `R2_BUCKET`, `R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`.

Migrate, then run each service in its own terminal:

```sh
cd packages/db && bunx prisma migrate deploy && cd -

bun --filter @repo/engine dev
bun --filter @repo/api dev
bun --filter @repo/db-poller dev
bun --filter @repo/wss dev
bun --filter @repo/price-feeder dev
bun --filter @repo/market-maker dev   # optional: seeds local liquidity
bun --filter web dev                  # browser client
```

Run tests (needs `DATABASE_URL` and `REDIS_URL` pointed at running services):

```sh
bun --filter @repo/tests test
```

## Docker Setup

`docker compose` runs the full stack: Redis, TimescaleDB, migrations, and every app service.

```sh
docker compose --env-file .env.docker up --build
```

- Web: `http://localhost:5173`
- API: `http://localhost:4000`
- WSS: `ws://localhost:3030`
- Redis: `localhost:6379`, Postgres: `localhost:5432`

```sh
docker compose down       # stop
docker compose down -v    # stop and wipe Redis/Postgres data
```

## Production Deployment

Runs live on GKE: `nebula.ashutoshsao.com` (frontend, on Vercel), `api.nebula.ashutoshsao.com`, `ws.nebula.ashutoshsao.com` (backend services, in-cluster).

TimescaleDB and Redis are self-hosted in-cluster (StatefulSets + PVCs), not managed services — needed for TimescaleDB's hypertable/continuous-aggregate features, which aren't available on managed Postgres providers. `ingress-nginx` + `cert-manager` handle routing and TLS.

All k8s manifests, secrets (SOPS-encrypted), and the ingress/backup config live in a separate private `ops` repo, not here — that's the source of truth for anything infra-related.

## Example Flow

```sh
# sign up
curl -X POST http://localhost:4000/api/signup \
  -H 'content-type: application/json' \
  -d '{"username":"ada","password":"pass123","name":"Ada"}'

# create a market (admin)
curl -X POST http://localhost:4000/api/market \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer YOUR_JWT' \
  -H 'token: YOUR_ADMIN_SECRET' \
  -d '{"symbol":"BTC","imageUrl":"https://example.com/btc.png","maxLeverage":10,"minQty":1}'

# add balance
curl -X POST http://localhost:4000/api/onramp \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer YOUR_JWT' \
  -d '{"amount":100000}'

# limit order
curl -X POST http://localhost:4000/api/order \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer YOUR_JWT' \
  -d '{"orderType":"limit","side":"buy","price":100,"qty":10,"leverage":1,"symbol":"BTC"}'

# market order
curl -X POST http://localhost:4000/api/order \
  -H 'content-type: application/json' \
  -H 'authorization: Bearer YOUR_JWT' \
  -d '{"orderType":"market","side":"buy","qty":4,"leverage":1,"slippageBps":100,"symbol":"BTC"}'
```

## Design Notes

State stays in memory because the engine is the system of record for the hot path; Postgres holds slower-moving records (users, market metadata), Redis Streams provide ordering between HTTP and the engine.

The order book uses `BTree<number, RestingOrder[]>` so asks/bids traverse low-to-high/high-to-low naturally. Positions are netted per user+symbol: same-side fills grow size and re-average price, opposite-side fills reduce/close/flip. Liquidation price derives from average price and margin per quantity.

Snapshots serialize the engine's maps (including BTree contents) and upload them to R2, keeping the last 10. On restart, the engine loads the latest snapshot and resumes reading Redis from the saved stream ID.

For the reasoning behind these choices (single-writer engine, Redis Streams as the bus, integer cents, snapshot/replay recovery, bots-as-demo-data), see [DECISIONS.md](./DECISIONS.md).

## Current Rough Edges

This is a prototype, not a production exchange:

- Single-process, in-memory matching engine.
- Liquidation math, insurance funds, fees, realized PnL, bankruptcy prices, and ADL ranking are simplified.
- `apps/web` is a Vite + React scaffold, still under active development.

