// In-process matching benchmark: calls handleCommand directly (no Redis, no network).
// Usage: bun run bench [orders]   (default 1,000,000)
import { handleCommand } from "../src/controller/engine.controller";

const SYMBOL = "BENCH-USD";
const TRADERS = 200; // first half only buys, second half only sells
const N = Number(process.argv[2] ?? 1_000_000);

let msg = 0;
const cmd = (type: string, payload: unknown = {}) =>
  handleCommand({
    streamMsgId: `${Date.now()}-${++msg}`,
    correlationId: "bench",
    responseQueue: "bench",
    type,
    payload,
  } as any) as any;

const trader = (side: "buy" | "sell", i: number) =>
  side === "buy" ? `u${i % (TRADERS / 2)}` : `u${TRADERS / 2 + (i % (TRADERS / 2))}`;

cmd("create_market", { marketId: "bench", symbol: SYMBOL, maxLeverage: 10, minQty: 1 });
for (let u = 0; u < TRADERS; u++) cmd("add_balance", { userId: `u${u}`, amount: 1e12 });

// Seed a book: bids 90-99, asks 101-110.
for (let i = 0; i < 2000; i++) {
  const side = i % 2 ? "buy" : "sell";
  cmd("create_order", {
    userId: trader(side, i), symbol: SYMBOL, orderType: "limit", side,
    price: side === "buy" ? 90 + (i % 10) : 101 + (i % 10), qty: 1 + (i % 5), leverage: 1,
  });
}

// Deterministic mix: 80% limit orders priced 95-105 (many cross), 20% market orders.
let seed = 42;
const rnd = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff;

const lat = new Float64Array(N);
let fills = 0;
let errors = 0;
const t0 = performance.now();
for (let i = 0; i < N; i++) {
  const side = rnd() < 0.5 ? "buy" : "sell";
  const isMarket = rnd() < 0.2;
  const price = Math.round(95 + rnd() * 10);
  const qty = 1 + Math.floor(rnd() * 5);
  const s = performance.now();
  try {
    const r = cmd("create_order", isMarket
      ? { userId: trader(side, i), symbol: SYMBOL, orderType: "market", side, qty, leverage: 1, slippageBps: 10_000 }
      : { userId: trader(side, i), symbol: SYMBOL, orderType: "limit", side, price, qty, leverage: 1 });
    fills += r?.fills?.length ?? 0;
  } catch {
    errors++;
  }
  lat[i] = performance.now() - s;
}
const seconds = (performance.now() - t0) / 1000;

lat.sort();
const pct = (p: number) => `${(lat[Math.floor(p * (N - 1))]! * 1000).toFixed(1)}µs`;
console.log({
  orders: N,
  seconds: Number(seconds.toFixed(2)),
  ordersPerSec: Math.round(N / seconds),
  fills,
  errors,
  p50: pct(0.5),
  p99: pct(0.99),
  p999: pct(0.999),
});
