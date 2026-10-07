// End-to-end order benchmark: HTTP → API → Redis stream → engine → response → HTTP.
// Open model (constant arrival rate), so a slow system can't slow the load down.
//
// Run (k6 in Docker, on the bench compose network):
//   docker run --rm -i --network nebula-bench_default \
//     -e RATE=500 -e DURATION=30s -e ADMIN_SECRET=... \
//     grafana/k6 run - < bench/e2e/orders.k6.js
// Results: read `order_latency` (steady state after WARMUP); `http_req_duration` also counts setup/warm-up.
import http from "k6/http";
import { check } from "k6";
import { Counter, Trend } from "k6/metrics";

const BASE = __ENV.BASE_URL || "http://api:4000/api";
const RATE = Number(__ENV.RATE || 200);
const DURATION = __ENV.DURATION || "30s";
const WARMUP = __ENV.WARMUP || "10s"; // same rate, excluded from order_latency
const TRADERS = Number(__ENV.TRADERS || 200); // first half only buy, second half only sell

const rejected = new Counter("orders_rejected");
// Steady-state order latency: recorded only in the measured scenario, never during warm-up.
const orderLatency = new Trend("order_latency", true);

export const options = {
  setupTimeout: "5m",
  summaryTrendStats: ["avg", "min", "med", "p(90)", "p(99)", "p(99.9)", "max", "count"],
  // Warm-up runs first at the same rate (JIT, connection pools, VU spin-up), then the measured run.
  // Read order_latency for results; http_req_duration includes setup and warm-up.
  scenarios: {
    warmup: {
      executor: "constant-arrival-rate",
      exec: "warmup",
      rate: RATE,
      timeUnit: "1s",
      duration: WARMUP,
      preAllocatedVUs: Math.min(Math.max(RATE, 50), 2000),
      maxVUs: 4000,
    },
    orders: {
      executor: "constant-arrival-rate",
      exec: "measured",
      startTime: WARMUP,
      rate: RATE,
      timeUnit: "1s",
      duration: DURATION,
      preAllocatedVUs: Math.min(Math.max(RATE, 50), 2000),
      maxVUs: 4000,
    },
  },
};

const JSON_HEADERS = { "content-type": "application/json" };

function signup(username) {
  const res = http.post(
    `${BASE}/signup`,
    JSON.stringify({ username, password: "benchpass123", name: username }),
    { headers: JSON_HEADERS },
  );
  if (res.status !== 201) throw new Error(`signup ${username}: ${res.status} ${res.body}`);
  return res.json("token");
}

const auth = (token, extra = {}) => ({
  headers: { ...JSON_HEADERS, authorization: `Bearer ${token}`, ...extra },
});

export function setup() {
  const run = Date.now();
  // Chosen once here: init code runs per VU, so a clock-based name there would differ per VU.
  const SYMBOL = __ENV.SYMBOL || `BENCH${run % 100000}`;
  const admin = signup(`bench_admin_${run}`);
  const market = http.post(
    `${BASE}/market`,
    JSON.stringify({ symbol: SYMBOL, imageUrl: "https://example.com/x.png", maxLeverage: 10, minQty: 1 }),
    auth(admin, { token: __ENV.ADMIN_SECRET }),
  );
  if (market.status >= 300) throw new Error(`create market: ${market.status} ${market.body}`);

  const tokens = [];
  for (let i = 0; i < TRADERS; i++) {
    const token = signup(`bench_${run}_${i}`);
    const onramp = http.post(`${BASE}/onramp`, JSON.stringify({ amount: 1_000_000_000 }), auth(token));
    if (onramp.status !== 200) throw new Error(`onramp: ${onramp.status} ${onramp.body}`);
    tokens.push(token);
  }

  // Seed a book: bids 90-99, asks 101-110 (same shape as the in-process benchmark).
  for (let i = 0; i < 400; i++) {
    const side = i % 2 ? "buy" : "sell";
    const t = side === "buy" ? tokens[i % (TRADERS / 2)] : tokens[TRADERS / 2 + (i % (TRADERS / 2))];
    http.post(
      `${BASE}/order`,
      JSON.stringify({
        symbol: SYMBOL, orderType: "limit", side, leverage: 1, qty: 1 + (i % 5),
        price: side === "buy" ? 90 + (i % 10) : 101 + (i % 10),
      }),
      auth(t),
    );
  }
  return { tokens, SYMBOL };
}

function placeOrder({ tokens, SYMBOL }) {
  const side = Math.random() < 0.5 ? "buy" : "sell";
  const half = TRADERS / 2;
  const idx = Math.floor(Math.random() * half);
  const token = side === "buy" ? tokens[idx] : tokens[half + idx];
  const qty = 1 + Math.floor(Math.random() * 5);
  const order = Math.random() < 0.2
    ? { symbol: SYMBOL, orderType: "market", side, qty, leverage: 1, slippageBps: 10_000 }
    : { symbol: SYMBOL, orderType: "limit", side, qty, leverage: 1, price: Math.round(95 + Math.random() * 10) };

  const res = http.post(`${BASE}/order`, JSON.stringify(order), auth(token));
  const ok = check(res, { "order accepted": (r) => r.status >= 200 && r.status < 300 });
  if (!ok) rejected.add(1);
  return res;
}

export function warmup(data) {
  placeOrder(data);
}

export function measured(data) {
  const res = placeOrder(data);
  orderLatency.add(res.timings.duration);
}
