# price-feeder

Publishes integer-cent index prices to the engine command stream and market
pub/sub channels. Run with `bun index.ts`; configure `REDIS_URL` (see `.env.example`).

Backpack is the primary source (`wss://ws.backpack.exchange`,
`markPrice.<BASE>_USDC_PERP`). Binance is a connected fallback
(`wss://fstream.binance.com/market/stream`, `<base>usdt@markPrice@1s`).
Both adapters use the **index price** (`i`), not the venue's mark price (`p`).
Backpack microsecond timestamps are converted to milliseconds.

Only one source publishes for each base asset: Binance ticks are suppressed
while Backpack has supplied a valid tick within 10 seconds. Startup reserves
10 seconds for Backpack to connect. Once Backpack is stale, fresh Binance ticks
can publish; the next newer Backpack tick restores the primary. Every source
change is logged. Old, duplicate, invalid, and excessively future-dated ticks
are rejected. The providers use different indexes (including USDC versus USDT
markets), so failover can change the reference price.

A socket that stops delivering fresh prices for any subscribed base for 15
seconds is terminated and reconnected with exponential backoff (1–30 seconds).
Subscriptions are rebuilt for every new socket. Markets added after startup
are subscribed on both feeds, and markets sharing a base receive the same tick.
No REST polling runs alongside the streams.

`GET :8080/health` returns 200 after every market has successfully published
prices to Redis, and 503 if any market has gone 30 seconds without publication.
Kubernetes startup, readiness, and liveness probes use this endpoint. Redis or
bootstrap failures exit the process so Kubernetes can restart it.

Validation: `bun test apps/price-feeder` from the repository root.
