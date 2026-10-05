import { afterAll, beforeAll, beforeEach, describe, expect, it } from "bun:test";
import jwt from "jsonwebtoken";
import { prisma } from "@repo/db";
import type { EngineCommandType, EngineRequest, PositionClose } from "@repo/types";
import { handleCommand } from "../engine/src/controller/engine.controller";
import { updatePosition } from "../engine/src/helper/updatePosition";
import { BALANCES, FUNDING_RATE_ACCOUMILATOR, INDEX_PRICES, LAST_FUNDING, MARK_PRICE_EWMA, MARKET_UPDATE_IDS, MARKETS, ORDERBOOKS, ORDERS, POSITIONS } from "../engine/src/engine-store";
import { getMarketSymbolsForFeedSymbol, newMarket, resetPriceFeederState, subscribedMarkets } from "../price-feeder/src/helper/newMarket";

const WORKSPACE_ROOT = `${import.meta.dir}/../..`;
const PORT = Number(process.env.TEST_API_PORT ?? "4210");
const BASE_URL = `http://127.0.0.1:${PORT}/api`;
const processes: Array<ReturnType<typeof Bun.spawn>> = [];
const processLogs: string[] = [];

const requiredEnv = ["DATABASE_URL", "REDIS_URL"] as const;

let username = "";
let password = "";
let authToken = "";
let refreshToken = "";
let marketSymbol = "";
let apiRestingOrderId = "";

let streamMsgCounter = 0;

function nextStreamMsgId() {
  streamMsgCounter += 1;
  return `${Date.now()}-${streamMsgCounter}`;
}

function engineCommand(type: EngineCommandType, payload: unknown = {}) {
  return handleCommand({
    streamMsgId: nextStreamMsgId(),
    correlationId: crypto.randomUUID(),
    responseQueue: "test-response-queue",
    type,
    payload: payload as EngineRequest["payload"],
  }) as any;
}

function resetEngineStore() {
  BALANCES.clear();
  INDEX_PRICES.clear();
  MARKET_UPDATE_IDS.clear();
  MARKETS.clear();
  ORDERBOOKS.clear();
  ORDERS.clear();
  POSITIONS.clear();
  FUNDING_RATE_ACCOUMILATOR.clear();
  MARK_PRICE_EWMA.clear();
  LAST_FUNDING.clear();
}

async function loadEnvFile(path: string) {
  const file = Bun.file(path);
  if (!(await file.exists())) return;

  for (const line of (await file.text()).split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;

    const separator = trimmed.indexOf("=");
    if (separator === -1) continue;

    const key = trimmed.slice(0, separator).trim();
    const value = trimmed.slice(separator + 1).trim().replace(/^["']|["']$/g, "");
    process.env[key] ??= value;
  }
}

async function loadAppEnv() {
  await loadEnvFile(`${WORKSPACE_ROOT}/packages/db/.env`);
  await loadEnvFile(`${WORKSPACE_ROOT}/packages/redis/.env`);
  await loadEnvFile(`${WORKSPACE_ROOT}/apps/engine/.env`);
  await loadEnvFile(`${WORKSPACE_ROOT}/apps/api/.env`);
  await loadEnvFile(`${WORKSPACE_ROOT}/apps/tests/.env`);

  process.env.REDIS_URL = process.env.TEST_REDIS_URL ?? "redis://localhost:6379/15";
}

function appEnv() {
  return {
    ...process.env,
    PORT: String(PORT),
    JWT_SECRET: process.env.JWT_SECRET ?? "test-jwt-secret",
    ADMIN_SECRET: process.env.ADMIN_SECRET ?? "test-admin-secret",
  };
}

async function captureOutput(label: string, stream: ReadableStream<Uint8Array> | null) {
  if (!stream) return;

  const reader = stream.getReader();
  const decoder = new TextDecoder();
  try {
    while (true) {
      const { done, value } = await reader.read();
      if (done) return;
      processLogs.push(`[${label}] ${decoder.decode(value)}`);
    }
  } catch {
    // The streams close when the child processes are stopped in afterAll.
  }
}

function startProcess(label: string, args: string[]) {
  const process = Bun.spawn(args, {
    cwd: WORKSPACE_ROOT,
    env: appEnv(),
    stdout: "pipe",
    stderr: "pipe",
  });

  processes.push(process);
  void captureOutput(`${label}:stdout`, process.stdout);
  void captureOutput(`${label}:stderr`, process.stderr);
}

async function post(path: string, body: unknown, headers: Record<string, string> = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "POST",
    headers: {
      "content-type": "application/json",
      ...headers,
    },
    body: JSON.stringify(body),
  });

  const text = await response.text();
  let json: unknown = null;
  if (text) {
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(
        `POST ${path} returned ${response.status} with a non-JSON body:\n${text}\n${processLogs.join("")}`,
      );
    }
  }

  return {
    response,
    json: json as any,
  };
}

async function del(path: string, headers: Record<string, string> = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "DELETE",
    headers,
  });

  const text = await response.text();
  let json: unknown = null;
  if (text) {
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(
        `DELETE ${path} returned ${response.status} with a non-JSON body:\n${text}\n${processLogs.join("")}`,
      );
    }
  }

  return {
    response,
    json: json as any,
  };
}

async function get(path: string, headers: Record<string, string> = {}) {
  const response = await fetch(`${BASE_URL}${path}`, {
    method: "GET",
    headers,
  });

  const text = await response.text();
  let json: unknown = null;
  if (text) {
    try {
      json = JSON.parse(text);
    } catch {
      throw new Error(
        `GET ${path} returned ${response.status} with a non-JSON body:\n${text}\n${processLogs.join("")}`,
      );
    }
  }

  return {
    response,
    json: json as any,
  };
}

async function waitFor<T>(callback: () => Promise<T | undefined>, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;

  while (Date.now() < deadline) {
    try {
      const result = await callback();
      if (result !== undefined) return result;
    } catch (error) {
      lastError = error;
    }
    await Bun.sleep(100);
  }

  throw new Error(`Timed out waiting for condition: ${String(lastError)}\n${processLogs.join("")}`);
}

async function waitForApi() {
  const deadline = Date.now() + 15_000;
  let lastError: unknown;

  while (Date.now() < deadline) {
    const exited = processes.find((process) => process.exitCode !== null);
    if (exited) {
      throw new Error(`A test process exited with code ${exited.exitCode}\n${processLogs.join("")}`);
    }

    try {
      const { response } = await post("/signin", {});
      if (response.status === 400) return;
    } catch (error) {
      lastError = error;
    }

    await Bun.sleep(250);
  }

  throw new Error(`API did not become ready: ${String(lastError)}\n${processLogs.join("")}`);
}

beforeEach(() => {
  resetEngineStore();
});

describe("api integration", () => {
  beforeAll(async () => {
    await loadAppEnv();

    const missing = requiredEnv.filter((key) => !process.env[key]);
    if (missing.length > 0) {
      throw new Error(`Missing test environment variables: ${missing.join(", ")}`);
    }

    startProcess("engine", ["bun", "apps/engine/index.ts"]);
    startProcess("db-poller", ["bun", "apps/db-poller/index.ts"]);
    startProcess("api", ["bun", "apps/api/index.ts"]);
    await waitForApi();
  }, 20_000);

  afterAll(() => {
    for (const process of processes) process.kill();
  });

  describe("auth endpoints", () => {
    it("rejects signup without a username", async () => {
      const { response } = await post("/signup", { password: "password-123" });
      expect(response.status).toBe(400);
    });

    it("signs up a user", async () => {
      username = `test-${crypto.randomUUID()}@example.com`;
      password = "password-123";

      const { response, json } = await post("/signup", {
        name: "Test User",
        username,
        password,
      });

      expect(response.status).toBe(201);
      expect(json.token).toBeString();
      expect(json.refreshToken).toBeString();
    });

    it("rejects signin without a username", async () => {
      const { response } = await post("/signin", { password });
      expect(response.status).toBe(400);
    });

    it("rejects incorrect credentials", async () => {
      const { response } = await post("/signin", {
        username,
        password: "incorrect-password",
      });
      expect(response.status).toBe(401);
    });

    it("signs in with valid credentials", async () => {
      const { response, json } = await post("/signin", { username, password });

      expect(response.status).toBe(200);
      expect(json.token).toBeString();
      expect(json.refreshToken).toBeString();
      authToken = json.token;
      refreshToken = json.refreshToken;
    });

    it("rejects an expired access token with a TOKEN_EXPIRED code", async () => {
      const secret = process.env.JWT_SECRET ?? "test-jwt-secret";
      const expiredToken = jwt.sign({ userId: crypto.randomUUID() }, secret, { expiresIn: "-10s" });

      const { response, json } = await get("/balance", { authorization: `Bearer ${expiredToken}` });

      expect(response.status).toBe(401);
      expect(json.code).toBe("TOKEN_EXPIRED");
    });

    it("exchanges a refresh token for a new working access token", async () => {
      const { response, json } = await post("/refresh", { refreshToken });

      expect(response.status).toBe(200);
      expect(json.token).toBeString();

      const balance = await get("/balance", { authorization: `Bearer ${json.token}` });
      expect(balance.response.status).toBe(200);
    });

    it("rejects an unknown refresh token", async () => {
      const { response } = await post("/refresh", { refreshToken: "not-a-real-refresh-token" });
      expect(response.status).toBe(401);
    });

    it("rejects a refresh token whose account was deleted — the bug this whole flow exists to close", async () => {
      const throwawayUsername = `throwaway-${crypto.randomUUID()}@example.com`;
      const signup = await post("/signup", {
        name: "Throwaway",
        username: throwawayUsername,
        password: "password-123",
      });
      expect(signup.response.status).toBe(201);
      const throwawayRefreshToken = signup.json.refreshToken as string;

      await prisma.user.delete({ where: { username: throwawayUsername } });

      const { response } = await post("/refresh", { refreshToken: throwawayRefreshToken });
      expect(response.status).toBe(401);
    });

    it("logout revokes the refresh token", async () => {
      const loggedOut = await post("/logout", { refreshToken });
      expect(loggedOut.response.status).toBe(200);

      const { response } = await post("/refresh", { refreshToken });
      expect(response.status).toBe(401);

      // re-signin so the rest of the suite still has a live token/refreshToken pair
      const signin = await post("/signin", { username, password });
      authToken = signin.json.token;
      refreshToken = signin.json.refreshToken;
    });
  });

  describe("order endpoints", () => {
    it("creates a market", async () => {
      marketSymbol = `TEST-${crypto.randomUUID().slice(0, 8).toUpperCase()}-USD`;
      const { response, json } = await post(
        "/market",
        {
          symbol: marketSymbol,
          imageUrl: "https://example.com/market.png",
          maxLeverage: 10,
          minQty: 1,
        },
        {
          authorization: `Bearer ${authToken}`,
          token: process.env.ADMIN_SECRET ?? "test-admin-secret",
        },
      );

      expect(response.status).toBe(201);
      expect(json.marketId).toBeString();

      const { response: marketsResponse, json: marketsJson } = await get("/markets");
      expect(marketsResponse.status).toBe(200);
      expect(marketsJson.markets.some((market: any) => market.symbol === marketSymbol)).toBe(true);
    });

    it("adds balance", async () => {
      const { response, json } = await post(
        "/onramp",
        { amount: 100_000 },
        { authorization: `Bearer ${authToken}` },
      );

      expect(response.status).toBe(200);
      expect(json.response.available).toBeGreaterThanOrEqual(100_000);

      const { response: balanceResponse, json: balanceJson } = await get(
        "/balance",
        { authorization: `Bearer ${authToken}` },
      );
      expect(balanceResponse.status).toBe(200);
      expect(balanceJson.response.available).toBeGreaterThanOrEqual(100_000);
    });

    it("places an unmatched limit order", async () => {
      const { response, json } = await post(
        "/order",
        {
          orderType: "limit",
          side: "buy",
          price: 100,
          qty: 10,
          leverage: 1,
          symbol: marketSymbol,
        },
        { authorization: `Bearer ${authToken}` },
      );

      expect(response.status).toBe(200);
      expect(json.order.orderId).toBeString();
      apiRestingOrderId = json.order.orderId;
      expect(json.order.filledQty).toBe(0);
      expect(json.order.status).toBe("open");
      expect(json.depthDiff).toMatchObject({
        symbol: marketSymbol,
        firstUpdateId: 1,
        finalUpdateId: 1,
        prevUpdateId: 0,
        bids: [[100, 10]],
        asks: [],
      });

      const { response: depthResponse, json: depthJson } = await get(`/depth/${marketSymbol}`);
      expect(depthResponse.status).toBe(200);
      expect(depthJson.bids).toEqual([[100, 10]]);
      expect(depthJson.asks).toEqual([]);

      await waitFor(async () => {
        const { response: ordersResponse, json: ordersJson } = await get(
          "/orders",
          { authorization: `Bearer ${authToken}` },
        );
        expect(ordersResponse.status).toBe(200);

        return ordersJson.orders.some((order: any) => order.id === apiRestingOrderId)
          ? ordersJson
          : undefined;
      });
    });

    it("cancels a resting order through the API cancel route", async () => {
      const { response, json } = await del(
        `/order/${apiRestingOrderId}`,
        { authorization: `Bearer ${authToken}` },
      );

      expect(response.status).toBe(200);
      expect(json.order.orderId).toBe(apiRestingOrderId);
      expect(json.order.status).toBe("cancelled");
      expect(json.depthDiff).toMatchObject({
        symbol: marketSymbol,
        firstUpdateId: 2,
        finalUpdateId: 2,
        prevUpdateId: 1,
        bids: [[100, 0]],
        asks: [],
      });

      await waitFor(async () => {
        const { response: ordersResponse, json: ordersJson } = await get(
          "/orders",
          { authorization: `Bearer ${authToken}` },
        );
        expect(ordersResponse.status).toBe(200);

        return ordersJson.orders.find((order: any) =>
          order.id === apiRestingOrderId && order.status === "cancelled"
        );
      });
    });

    it("returns empty market-data arrays when no trades have printed", async () => {
      const { response: tradesResponse, json: tradesJson } = await get(`/trades/${marketSymbol}`);
      expect(tradesResponse.status).toBe(200);
      expect(tradesJson.trades).toEqual([]);

      const { response: klinesResponse, json: klinesJson } = await get(`/klines/${marketSymbol}`);
      expect(klinesResponse.status).toBe(200);
      expect(klinesJson.candles).toEqual([]);

      const { response: tickerResponse } = await get(`/ticker/${marketSymbol}`);
      expect(tickerResponse.status).toBe(404);
    });

    it("builds different OHLCV buckets from multiple fills per interval", async () => {
      const { timescale } = await import("@repo/db");
      const symbol = `KLINES-${crypto.randomUUID().slice(0, 8).toUpperCase()}-USD`;
      const base = Date.UTC(2026, 5, 30, 10, 0, 0);
      const rows = [
        [crypto.randomUUID(), new Date(base + 5_000), symbol, 100, 1, "buy"],
        [crypto.randomUUID(), new Date(base + 90_000), symbol, 110, 2, "buy"],
        [crypto.randomUUID(), new Date(base + 6 * 60_000), symbol, 90, 3, "sell"],
        [crypto.randomUUID(), new Date(base + 65 * 60_000), symbol, 120, 4, "buy"],
        [crypto.randomUUID(), new Date(base + 24 * 60 * 60_000), symbol, 130, 5, "sell"],
      ];

      for (const row of rows) {
        await timescale.query(
          `INSERT INTO fills_ts (fill_id, time, symbol, price, qty, side)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          row,
        );
      }

      // candles_1d buckets align to UTC midnight, which for `base` (10:00 UTC)
      // falls nearly 10 hours before it — from must reach back past that
      // midnight or the WHERE clause silently excludes the whole day's candle
      const from = base - 24 * 60 * 60_000;
      const to = base + 25 * 60 * 60_000;

      // continuous aggregates only materialize via their background refresh
      // policy, which only looks at a window recent relative to real NOW() (1
      // hour for candles_1m, up to 7 days for candles_1d) — this fixture uses a
      // fixed historical timestamp that eventually ages out of every one of
      // those windows, so refresh synchronously instead of relying on the
      // policy. Bounded (not NULL/NULL) so it stays cheap and doesn't
      // lock-contend with the real background policy on this shared dev
      // database, but padded well beyond `from`/`to` since Timescale requires
      // a refresh window to span at least one full bucket — the coarsest
      // being candles_1d's 1-day bucket, which the ~25h [from, to] span alone
      // isn't reliably wide enough to guarantee. Order matters: each aggregate
      // is built from the one before it (1m -> 5m -> 15m -> 1h -> 4h -> 1d).
      const refreshFrom = base - 3 * 24 * 60 * 60_000;
      const refreshTo = base + 3 * 24 * 60 * 60_000;
      for (const view of ["candles_1m", "candles_5m", "candles_15m", "candles_1h", "candles_4h", "candles_1d"]) {
        await timescale.query(
          `CALL refresh_continuous_aggregate($1::regclass, $2::timestamptz, $3::timestamptz)`,
          [view, new Date(refreshFrom), new Date(refreshTo)],
        );
      }
      const klinesPath = (interval: string) => `/klines/${symbol}?interval=${interval}&from=${from}&to=${to}`;

      const oneMinute = await get(klinesPath("1m"));
      expect(oneMinute.response.status).toBe(200);
      expect(oneMinute.json.candles).toHaveLength(5);
      expect(oneMinute.json.candles[0]).toMatchObject({
        open: "100",
        high: "100",
        low: "100",
        close: "100",
        volume: "1",
      });

      const fiveMinute = await get(klinesPath("5m"));
      expect(fiveMinute.response.status).toBe(200);
      expect(fiveMinute.json.candles).toHaveLength(4);
      expect(fiveMinute.json.candles[0]).toMatchObject({
        open: "100",
        high: "110",
        low: "100",
        close: "110",
        volume: "3",
      });

      const oneHour = await get(klinesPath("1h"));
      expect(oneHour.response.status).toBe(200);
      expect(oneHour.json.candles).toHaveLength(3);
      expect(oneHour.json.candles[0]).toMatchObject({
        open: "100",
        high: "110",
        low: "90",
        close: "90",
        volume: "6",
      });

      const oneDay = await get(klinesPath("1d"));
      expect(oneDay.response.status).toBe(200);
      expect(oneDay.json.candles).toHaveLength(2);
      expect(oneDay.json.candles[0]).toMatchObject({
        open: "100",
        high: "120",
        low: "90",
        close: "120",
        volume: "10",
      });
    });

    it("persists matched fills and exposes them as recent trades", async () => {
      const restingSell = await post(
        "/order",
        {
          orderType: "limit",
          side: "sell",
          price: 120,
          qty: 2,
          leverage: 1,
          symbol: marketSymbol,
        },
        { authorization: `Bearer ${authToken}` },
      );
      expect(restingSell.response.status).toBe(200);

      const marketBuy = await post(
        "/order",
        {
          orderType: "market",
          side: "buy",
          qty: 2,
          leverage: 1,
          slippageBps: 10_000,
          symbol: marketSymbol,
        },
        { authorization: `Bearer ${authToken}` },
      );
      expect(marketBuy.response.status).toBe(200);
      expect(marketBuy.json.fills).toHaveLength(1);

      const fillId = marketBuy.json.fills[0].fillId;

      await waitFor(async () => {
        const { response: fillsResponse, json: fillsJson } = await get(
          "/fills",
          { authorization: `Bearer ${authToken}` },
        );
        expect(fillsResponse.status).toBe(200);

        const fill = fillsJson.fills.find((fill: any) => fill.id === fillId);
        if (!fill) return undefined;

        expect(fill).toMatchObject({
          id: fillId,
          symbol: marketSymbol,
          makerSide: "sell",
          side: "sell",
        });
        return fill;
      });

      await waitFor(async () => {
        const { response: ordersResponse, json: ordersJson } = await get(
          "/orders",
          { authorization: `Bearer ${authToken}` },
        );
        expect(ordersResponse.status).toBe(200);

        return ordersJson.orders.find((order: any) =>
          order.id === restingSell.json.order.orderId &&
          order.status === "filled" &&
          order.filledQty === 2
        );
      });

      await waitFor(async () => {
        const { response: ordersResponse, json: ordersJson } = await get(
          "/orders",
          { authorization: `Bearer ${authToken}` },
        );
        expect(ordersResponse.status).toBe(200);
        expect(ordersJson.orders.some((order: any) =>
          order.id === restingSell.json.order.orderId &&
          (order.status === "open" || order.status === "partially_filled")
        )).toBe(false);

        return ordersJson.orders.find((order: any) =>
          order.id === restingSell.json.order.orderId &&
          order.status === "filled"
        );
      });

      await waitFor(async () => {
        const { response: tradesResponse, json: tradesJson } = await get(`/trades/${marketSymbol}`);
        expect(tradesResponse.status).toBe(200);

        return tradesJson.trades.find((trade: any) =>
          trade.symbol === marketSymbol &&
          Number(trade.price) === 120 &&
          Number(trade.qty) === 2 &&
          trade.side === "buy"
        );
      });
    });
  });
});

describe("engine commands", () => {
  it("adds and fetches balances through the command dispatcher", () => {
    expect(
      engineCommand("add_balance", {
        userId: "balance-user",
        amount: 1_000,
      }),
    ).toEqual({ available: 1_000, locked: 0 });
    expect(
      engineCommand("add_balance", {
        userId: "balance-user",
        amount: 250,
      }),
    ).toEqual({ available: 1_250, locked: 0 });
    const fetchedBalance = engineCommand("get_balance", {
      userId: "balance-user",
    });
    expect(fetchedBalance).toEqual({ available: 1_250, locked: 0 });
  });

  it("creates a market and returns sorted depth for resting limit orders", () => {
    engineCommand("create_market", {
      marketId: "depth-market",
      symbol: "DEPTH-USD",
      maxLeverage: 5,
      minQty: 1,
    });
    expect(engineCommand("get_markets")).toEqual(["DEPTH-USD"]);
    engineCommand("add_balance", { userId: "maker-a", amount: 10_000 });
    engineCommand("add_balance", { userId: "maker-b", amount: 10_000 });

    engineCommand("create_order", {
      userId: "maker-a",
      symbol: "DEPTH-USD",
      orderType: "limit",
      side: "sell",
      price: 110,
      qty: 3,
      leverage: 1,
    });
    engineCommand("create_order", {
      userId: "maker-a",
      symbol: "DEPTH-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 2,
      leverage: 1,
    });
    engineCommand("create_order", {
      userId: "maker-b",
      symbol: "DEPTH-USD",
      orderType: "limit",
      side: "buy",
      price: 90,
      qty: 4,
      leverage: 1,
    });
    engineCommand("create_order", {
      userId: "maker-b",
      symbol: "DEPTH-USD",
      orderType: "limit",
      side: "buy",
      price: 95,
      qty: 1,
      leverage: 1,
    });

    expect(engineCommand("get_depth", { symbol: "DEPTH-USD" })).toEqual({
      symbol: "DEPTH-USD",
      lastUpdateId: 4,
      asks: [
        [100, 2],
        [110, 3],
      ],
      bids: [
        [95, 1],
        [90, 4],
      ],
    });
  });

  it("matches a market buy against the best ask and updates both positions", () => {
    engineCommand("create_market", {
      marketId: "match-market",
      symbol: "MATCH-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "seller", amount: 20_000 });
    engineCommand("add_balance", { userId: "buyer", amount: 20_000 });

    const makerOrder = engineCommand("create_order", {
      userId: "seller",
      symbol: "MATCH-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 10,
      leverage: 1,
    });
    const takerOrder = engineCommand("create_order", {
      userId: "buyer",
      symbol: "MATCH-USD",
      orderType: "market",
      side: "buy",
      qty: 4,
      leverage: 1,
      slippageBps: 10_000,
    });

    expect(takerOrder.order.status).toBe("filled");
    expect(takerOrder.order.filledQty).toBe(4);
    expect(takerOrder.fills).toHaveLength(1);
    expect(takerOrder.fills[0]).toMatchObject({
      makerOrderId: makerOrder.order.orderId,
      makerUserId: "seller",
      takerUserId: "buyer",
      makerSide: "sell",
      qty: 4,
      price: 100,
      symbol: "MATCH-USD",
    });
    expect(takerOrder.fills[0].createdAt).toBeNumber();
    expect(takerOrder.makerOrders).toHaveLength(1);
    expect(takerOrder.makerOrders[0].orderId).toBe(makerOrder.order.orderId);
    expect(takerOrder.makerOrders[0].status).toBe("partially_filled");
    expect(takerOrder.depthDiff).toEqual({
      symbol: "MATCH-USD",
      firstUpdateId: 2,
      finalUpdateId: 2,
      prevUpdateId: 1,
      bids: [],
      asks: [[100, 6]],
    });
    expect(ORDERS.get(makerOrder.order.orderId)?.status).toBe("partially_filled");
    expect(ORDERS.get(makerOrder.order.orderId)?.filledQty).toBe(4);
    expect(POSITIONS.get("buyer")?.get("MATCH-USD")).toMatchObject({
      positionSide: "long",
      qty: 4,
      averagePrice: 100,
    });
    expect(POSITIONS.get("seller")?.get("MATCH-USD")).toMatchObject({
      positionSide: "short",
      qty: 4,
      averagePrice: 100,
    });
    expect(engineCommand("get_depth", { symbol: "MATCH-USD" }).asks).toEqual([[100, 6]]);
  });

  it("applies market slippage bps around the best price", () => {
    engineCommand("create_market", {
      marketId: "slippage-market",
      symbol: "SLIPPAGE-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "seller", amount: 20_000 });
    engineCommand("add_balance", { userId: "buyer", amount: 20_000 });

    engineCommand("create_order", {
      userId: "seller",
      symbol: "SLIPPAGE-USD",
      orderType: "limit",
      side: "sell",
      price: 6_000,
      qty: 2,
      leverage: 1,
    });
    const marketOrder = engineCommand("create_order", {
      userId: "buyer",
      symbol: "SLIPPAGE-USD",
      orderType: "market",
      side: "buy",
      qty: 2,
      leverage: 1,
      slippageBps: 100,
    });

    expect(marketOrder.order.price).toBe(6_060);
    expect(marketOrder.order.status).toBe("filled");
    expect(marketOrder.order.filledQty).toBe(2);
    expect(marketOrder.fills[0]).toMatchObject({
      price: 6_000,
      qty: 2,
      symbol: "SLIPPAGE-USD",
    });
  });

  it("rejects orders that violate market limits or have no market liquidity", () => {
    engineCommand("create_market", {
      marketId: "validation-market",
      symbol: "VALIDATION-USD",
      maxLeverage: 3,
      minQty: 5,
    });
    engineCommand("add_balance", { userId: "validator", amount: 10_000 });

    expect(() =>
      engineCommand("create_order", {
        userId: "validator",
        symbol: "VALIDATION-USD",
        orderType: "limit",
        side: "buy",
        price: 100,
        qty: 5,
        leverage: 4,
      }),
    ).toThrow("maximum leverage allowed for VALIDATION-USD is 3");

    expect(() =>
      engineCommand("create_order", {
        userId: "validator",
        symbol: "VALIDATION-USD",
        orderType: "limit",
        side: "buy",
        price: 100,
        qty: 4,
        leverage: 1,
      }),
    ).toThrow("min qty is 5");

    expect(() =>
      engineCommand("create_order", {
        userId: "validator",
        symbol: "VALIDATION-USD",
        orderType: "market",
        side: "buy",
        qty: 5,
        leverage: 1,
        slippageBps: 10_000,
      }),
    ).toThrow("no liquidity on asks");
  });

  it("partially fills a market order and refunds margin for unfilled quantity", () => {
    engineCommand("create_market", {
      marketId: "partial-market",
      symbol: "PARTIAL-MARKET-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "maker", amount: 10_000 });
    engineCommand("add_balance", { userId: "taker", amount: 1_000 });

    const makerOrder = engineCommand("create_order", {
      userId: "maker",
      symbol: "PARTIAL-MARKET-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 3,
      leverage: 1,
    });
    const takerOrder = engineCommand("create_order", {
      userId: "taker",
      symbol: "PARTIAL-MARKET-USD",
      orderType: "market",
      side: "buy",
      qty: 5,
      leverage: 1,
      slippageBps: 10_000,
    });

    expect(takerOrder.order.status).toBe("partially_filled");
    expect(takerOrder.order.filledQty).toBe(3);
    expect(takerOrder.fills).toHaveLength(1);
    expect(takerOrder.depthDiff.asks).toEqual([[100, 0]]);
    expect(ORDERS.get(makerOrder.order.orderId)?.status).toBe("filled");
    expect(engineCommand("get_depth", { symbol: "PARTIAL-MARKET-USD" }).asks).toEqual([]);
    expect(engineCommand("get_balance", { userId: "taker" })).toEqual({
      available: 700,
      locked: 300,
    });
  });

  it("keeps unfilled limit quantity resting and refunds price improvement", () => {
    engineCommand("create_market", {
      marketId: "price-improvement-market",
      symbol: "PRICE-IMPROVEMENT-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "seller", amount: 10_000 });
    engineCommand("add_balance", { userId: "buyer", amount: 1_000 });

    engineCommand("create_order", {
      userId: "seller",
      symbol: "PRICE-IMPROVEMENT-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 2,
      leverage: 1,
    });
    const buyerOrder = engineCommand("create_order", {
      userId: "buyer",
      symbol: "PRICE-IMPROVEMENT-USD",
      orderType: "limit",
      side: "buy",
      price: 110,
      qty: 5,
      leverage: 1,
    });

    expect(buyerOrder.order.status).toBe("partially_filled");
    expect(buyerOrder.order.filledQty).toBe(2);
    expect(buyerOrder.depthDiff).toEqual({
      symbol: "PRICE-IMPROVEMENT-USD",
      firstUpdateId: 2,
      finalUpdateId: 2,
      prevUpdateId: 1,
      bids: [[110, 3]],
      asks: [[100, 0]],
    });
    expect(engineCommand("get_depth", { symbol: "PRICE-IMPROVEMENT-USD" })).toEqual({
      symbol: "PRICE-IMPROVEMENT-USD",
      lastUpdateId: 2,
      asks: [],
      bids: [[110, 3]],
    });
    expect(engineCommand("get_balance", { userId: "buyer" })).toEqual({
      available: 470,
      locked: 530,
    });
  });

  it("cancels a resting order and refunds locked margin", () => {
    engineCommand("create_market", {
      marketId: "cancel-market",
      symbol: "CANCEL-USD",
      maxLeverage: 5,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "canceller", amount: 1_000 });

    const order = engineCommand("create_order", {
      userId: "canceller",
      symbol: "CANCEL-USD",
      orderType: "limit",
      side: "buy",
      price: 50,
      qty: 4,
      leverage: 1,
    });
    expect(engineCommand("get_balance", { userId: "canceller" })).toEqual({
      available: 800,
      locked: 200,
    });

    const cancelledOrder = engineCommand("cancel_order", {
      userId: "canceller",
      orderId: order.order.orderId,
    });

    expect(cancelledOrder.order.status).toBe("cancelled");
    expect(cancelledOrder.depthDiff).toEqual({
      symbol: "CANCEL-USD",
      firstUpdateId: 2,
      finalUpdateId: 2,
      prevUpdateId: 1,
      bids: [[50, 0]],
      asks: [],
    });
    expect(engineCommand("get_balance", { userId: "canceller" })).toEqual({
      available: 1_000,
      locked: 0,
    });
    expect(engineCommand("get_depth", { symbol: "CANCEL-USD" }).bids).toEqual([]);
  });

  it("returns the mark price on index updates and rejects unknown commands", () => {
    const response = engineCommand("update_index_price", {
      symbol: "INDEX-USD",
      price: 123,
    });

    // no orderbook for this symbol — mark price falls back to the index price
    expect(response).toEqual({
      symbol: "INDEX-USD",
      markPrice: 123,
      events: [],
      predictedFundingRate: 0,
      fundingSamples: 0,
    });
    expect(INDEX_PRICES.get("INDEX-USD")).toBe(123);
    expect(() =>
      handleCommand({
        streamMsgId: nextStreamMsgId(),
        correlationId: crypto.randomUUID(),
        responseQueue: "test-response-queue",
        type: "does_not_exist" as EngineCommandType,
        payload: {} as EngineRequest["payload"],
      }),
    ).toThrow("unknown command");
  });

  it("settles funding across every market using the accumulated premium average", () => {
    for (const symbol of ["FUNDING-A-USD", "FUNDING-B-USD"]) {
      engineCommand("create_market", {
        marketId: `${symbol}-market`,
        symbol,
        maxLeverage: 10,
        minQty: 1,
      });
      engineCommand("add_balance", { userId: `${symbol}-seller`, amount: 20_000 });
      engineCommand("add_balance", { userId: `${symbol}-buyer`, amount: 20_000 });

      engineCommand("create_order", {
        userId: `${symbol}-seller`,
        symbol,
        orderType: "limit",
        side: "sell",
        price: 100,
        qty: 10,
        leverage: 1,
      });
      engineCommand("create_order", {
        userId: `${symbol}-buyer`,
        symbol,
        orderType: "market",
        side: "buy",
        qty: 10,
        leverage: 1,
        slippageBps: 10_000,
      });

      // two index-price ticks so the accumulator has more than one sample to average
      engineCommand("update_index_price", { symbol, price: 99 });
      engineCommand("update_index_price", { symbol, price: 99 });
    }

    const buyerMarginBefore = POSITIONS.get("FUNDING-A-USD-buyer")?.get("FUNDING-A-USD")!.margin;
    const sellerMarginBefore = POSITIONS.get("FUNDING-A-USD-seller")?.get("FUNDING-A-USD")!.margin;

    const response = engineCommand("funding_rate", {});
    const settlements = response.settlements as Array<{ symbol: string; userId: string; rate: number; settledAt: number }>;

    // both markets settled — proves the multi-market loop isn't cut short
    expect(settlements.some((s) => s.symbol === "FUNDING-A-USD")).toBe(true);
    expect(settlements.some((s) => s.symbol === "FUNDING-B-USD")).toBe(true);
    expect(settlements.length).toBe(4); // buyer + seller for each of the 2 markets

    for (const settlement of settlements) {
      // averaged premium, not the extreme instantaneous value, and within the clamp
      expect(Math.abs(settlement.rate)).toBeLessThanOrEqual(0.0075);
      expect(settlement.settledAt).toBeNumber();
    }

    // long pays short (index below last traded price) — margin should move in opposite directions
    const buyerMarginAfter = POSITIONS.get("FUNDING-A-USD-buyer")?.get("FUNDING-A-USD")!.margin;
    const sellerMarginAfter = POSITIONS.get("FUNDING-A-USD-seller")?.get("FUNDING-A-USD")!.margin;
    expect(buyerMarginAfter).toBeLessThan(buyerMarginBefore!);
    expect(sellerMarginAfter).toBeGreaterThan(sellerMarginBefore!);

    // accumulator resets after settlement
    expect(FUNDING_RATE_ACCOUMILATOR.get("FUNDING-A-USD")).toEqual({ sumPremium: 0, samples: 0 });
    expect(LAST_FUNDING.get("FUNDING-A-USD")).toBe(
      settlements.find((s) => s.symbol === "FUNDING-A-USD")!.rate,
    );
  });

  it("falls back to the last settled rate when no index-price ticks occurred this window", () => {
    engineCommand("create_market", {
      marketId: "funding-fallback-market",
      symbol: "FUNDING-FALLBACK-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "fallback-seller", amount: 20_000 });
    engineCommand("add_balance", { userId: "fallback-buyer", amount: 20_000 });

    engineCommand("create_order", {
      userId: "fallback-seller",
      symbol: "FUNDING-FALLBACK-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 10,
      leverage: 1,
    });
    engineCommand("create_order", {
      userId: "fallback-buyer",
      symbol: "FUNDING-FALLBACK-USD",
      orderType: "market",
      side: "buy",
      qty: 10,
      leverage: 1,
      slippageBps: 10_000,
    });

    engineCommand("update_index_price", { symbol: "FUNDING-FALLBACK-USD", price: 99 });

    const first = engineCommand("funding_rate", {});
    const firstRate = first.settlements.find(
      (s: { symbol: string }) => s.symbol === "FUNDING-FALLBACK-USD",
    ).rate;

    // no new update_index_price ticks before the second settlement — accumulator is empty
    expect(FUNDING_RATE_ACCOUMILATOR.get("FUNDING-FALLBACK-USD")).toEqual({ sumPremium: 0, samples: 0 });

    const second = engineCommand("funding_rate", {});
    const secondRate = second.settlements.find(
      (s: { symbol: string }) => s.symbol === "FUNDING-FALLBACK-USD",
    ).rate;

    expect(secondRate).toBe(firstRate);
  });

  it("liquidates a position through update_index_price and returns a tagged synthetic order", () => {
    engineCommand("create_market", {
      marketId: "liq-market",
      symbol: "LIQ-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "liq-trader", amount: 10_000 });
    engineCommand("add_balance", { userId: "liq-maker", amount: 10_000 });
    engineCommand("add_balance", { userId: "liq-liquidity-buyer", amount: 10_000 });

    // resting ask so the trader can open a long position
    engineCommand("create_order", {
      userId: "liq-maker",
      symbol: "LIQ-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 10,
      leverage: 1,
    });

    // opens a 10x long — liquidationPrice = 100 - floor(margin/qty) = 100 - 10 = 90
    engineCommand("create_order", {
      userId: "liq-trader",
      symbol: "LIQ-USD",
      orderType: "market",
      side: "buy",
      qty: 10,
      leverage: 10,
      slippageBps: 10_000,
    });

    // resting bid so the forced liquidation sell has somewhere to match (not ADL)
    engineCommand("create_order", {
      userId: "liq-liquidity-buyer",
      symbol: "LIQ-USD",
      orderType: "limit",
      side: "buy",
      price: 89,
      qty: 10,
      leverage: 1,
    });

    const response = engineCommand("update_index_price", { symbol: "LIQ-USD", price: 89 });

    expect(response.symbol).toBe("LIQ-USD");
    expect(response.markPrice).toBeNumber();
    expect(Array.isArray(response.events)).toBe(true);
    const liquidation = (response.events as Array<{ order: { userId: string; side: string }; fills: unknown[]; reason?: string }>)
      .find((event) => event.order.userId === "liq-trader");

    expect(liquidation).toBeDefined();
    expect(liquidation!.reason).toBe("liquidation");
    expect(liquidation!.order.side).toBe("sell");
    expect(liquidation!.fills.length).toBeGreaterThan(0);
  });

  it("computes the EWMA mark price from the last traded price", () => {
    engineCommand("create_market", {
      marketId: "ewma-market",
      symbol: "EWMA-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "ewma-seller", amount: 20_000 });
    engineCommand("add_balance", { userId: "ewma-buyer", amount: 20_000 });

    engineCommand("create_order", {
      userId: "ewma-seller",
      symbol: "EWMA-USD",
      orderType: "limit",
      side: "sell",
      price: 100,
      qty: 5,
      leverage: 1,
    });
    engineCommand("create_order", {
      userId: "ewma-buyer",
      symbol: "EWMA-USD",
      orderType: "market",
      side: "buy",
      qty: 5,
      leverage: 1,
      slippageBps: 10_000,
    });

    // first tick: prevMark seeds from the index price -> 0.15*100 + 0.85*90 = 91.5
    const first = engineCommand("update_index_price", { symbol: "EWMA-USD", price: 90 });
    expect(first.markPrice).toBe(Math.round(0.15 * 100 + 0.85 * 90)); // 92
    expect(MARK_PRICE_EWMA.get("EWMA-USD")).toBeCloseTo(91.5);

    // second tick: converges toward last traded price -> 0.15*100 + 0.85*91.5 = 92.775
    const second = engineCommand("update_index_price", { symbol: "EWMA-USD", price: 90 });
    expect(second.markPrice).toBe(Math.round(0.15 * 100 + 0.85 * 91.5)); // 93
    expect(MARK_PRICE_EWMA.get("EWMA-USD")).toBeCloseTo(92.775);
  });

  it("keeps the mark price at the index price for markets with no trades", () => {
    engineCommand("create_market", {
      marketId: "untraded-market",
      symbol: "UNTRADED-USD",
      maxLeverage: 10,
      minQty: 1,
    });

    // lastTradedPrice is 0 — the EWMA must not decay toward it, and the
    // funding accumulator must not collect poisoned samples
    for (let i = 0; i < 3; i++) {
      const response = engineCommand("update_index_price", { symbol: "UNTRADED-USD", price: 100 });
      expect(response.markPrice).toBe(100);
    }
    expect(MARK_PRICE_EWMA.has("UNTRADED-USD")).toBe(false);
    expect(FUNDING_RATE_ACCOUMILATOR.has("UNTRADED-USD")).toBe(false);
  });

  it("removes a fully filled ask from the book when the buy exhausts exactly at it", () => {
    engineCommand("create_market", {
      marketId: "exhaust-market",
      symbol: "EXHAUST-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "exhaust-maker-a", amount: 10_000 });
    engineCommand("add_balance", { userId: "exhaust-maker-b", amount: 10_000 });
    engineCommand("add_balance", { userId: "exhaust-buyer", amount: 10_000 });

    engineCommand("create_order", {
      userId: "exhaust-maker-a", symbol: "EXHAUST-USD",
      orderType: "limit", side: "sell", price: 100, qty: 2, leverage: 1,
    });
    const secondAsk = engineCommand("create_order", {
      userId: "exhaust-maker-b", symbol: "EXHAUST-USD",
      orderType: "limit", side: "sell", price: 100, qty: 3, leverage: 1,
    });

    engineCommand("create_order", {
      userId: "exhaust-buyer", symbol: "EXHAUST-USD",
      orderType: "limit", side: "buy", price: 100, qty: 2, leverage: 1,
    });
    expect(engineCommand("get_depth", { symbol: "EXHAUST-USD" }).asks).toEqual([[100, 3]]);

    // a filled ask left in the book would show up here as a qty-0 fill
    const next = engineCommand("create_order", {
      userId: "exhaust-buyer", symbol: "EXHAUST-USD",
      orderType: "limit", side: "buy", price: 100, qty: 1, leverage: 1,
    });
    expect(next.fills).toHaveLength(1);
    expect(next.fills[0].qty).toBe(1);
    expect(next.fills[0].makerOrderId).toBe(secondAsk.order.orderId);
  });

  it("reduces a position built at two prices without throwing and settles PnL against the averaged entry", () => {
    engineCommand("create_market", {
      marketId: "avg-market",
      symbol: "AVG-USD",
      maxLeverage: 10,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "avg-trader", amount: 1_000_000 });
    engineCommand("add_balance", { userId: "avg-mm", amount: 1_000_000 });

    const order = (userId: string, side: "buy" | "sell", price: number, qty: number) =>
      engineCommand("create_order", {
        userId, symbol: "AVG-USD", orderType: "limit", side, price, qty, leverage: 1,
      });

    order("avg-mm", "sell", 100, 1);
    order("avg-trader", "buy", 100, 1);
    order("avg-mm", "sell", 103, 2);
    order("avg-trader", "buy", 103, 2);
    // (1*100 + 2*103) / 3 = 102
    expect(POSITIONS.get("avg-trader")?.get("AVG-USD")).toMatchObject({ qty: 3, averagePrice: 102 });

    order("avg-mm", "buy", 104, 1);
    const reduce = order("avg-trader", "sell", 104, 1);

    expect(reduce.order.status).toBe("filled");
    const takerClose = reduce.closedPositions.find((c: PositionClose) => c.userId === "avg-trader");
    expect(takerClose).toMatchObject({ closeType: "reduce", entryPrice: 102, exitPrice: 104, qty: 1, realizedPnl: 2 });
    expect(POSITIONS.get("avg-trader")?.get("AVG-USD")).toMatchObject({ qty: 2, averagePrice: 102 });
    // starting balance + 2 PnL; the remaining 2 @ 102 stay locked
    expect(engineCommand("get_balance", { userId: "avg-trader" })).toEqual({ available: 999_798, locked: 204 });
  });
});

describe("balance settlement on position close", () => {
  it("frees all locked margin (taker side) when a taker order fully closes its own position", () => {
    engineCommand("create_market", {
      marketId: "settle-taker-market",
      symbol: "SETTLE-TAKER-USD",
      maxLeverage: 20,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "settle-taker", amount: 1_000_000 });
    engineCommand("add_balance", { userId: "settle-taker-mm", amount: 1_000_000 });

    // resting liquidity on both sides so the taker can open then close
    engineCommand("create_order", {
      userId: "settle-taker-mm", symbol: "SETTLE-TAKER-USD",
      orderType: "limit", side: "sell", price: 100, qty: 10, leverage: 1,
    });
    engineCommand("create_order", {
      userId: "settle-taker-mm", symbol: "SETTLE-TAKER-USD",
      orderType: "limit", side: "buy", price: 90, qty: 10, leverage: 1,
    });

    const before = engineCommand("get_balance", { userId: "settle-taker" });
    const totalBefore = before.available + before.locked;

    engineCommand("create_order", {
      userId: "settle-taker", symbol: "SETTLE-TAKER-USD",
      orderType: "market", side: "buy", qty: 5, leverage: 10, slippageBps: 10_000,
    });
    const afterOpen = engineCommand("get_balance", { userId: "settle-taker" });
    expect(afterOpen.locked).toBeGreaterThan(0);

    engineCommand("create_order", {
      userId: "settle-taker", symbol: "SETTLE-TAKER-USD",
      orderType: "market", side: "sell", qty: 5, leverage: 10, slippageBps: 10_000,
    });
    const afterClose = engineCommand("get_balance", { userId: "settle-taker" });

    // fully flat — no position left, so nothing should still be locked
    expect(afterClose.locked).toBe(0);
    // price dropped 100 -> 90 on a long: realized PnL is exactly -(100-90)*5 = -50
    expect(afterClose.available + afterClose.locked).toBe(totalBefore - 50);
  });

  it("frees all locked margin (maker side) when a resting maker order fully closes its own position", () => {
    engineCommand("create_market", {
      marketId: "settle-maker-market",
      symbol: "SETTLE-MAKER-USD",
      maxLeverage: 20,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "settle-maker", amount: 1_000_000 });
    engineCommand("add_balance", { userId: "settle-maker-counterparty", amount: 1_000_000 });

    // settle-maker opens a long as a taker first
    engineCommand("create_order", {
      userId: "settle-maker-counterparty", symbol: "SETTLE-MAKER-USD",
      orderType: "limit", side: "sell", price: 100, qty: 5, leverage: 1,
    });
    engineCommand("create_order", {
      userId: "settle-maker", symbol: "SETTLE-MAKER-USD",
      orderType: "market", side: "buy", qty: 5, leverage: 10, slippageBps: 10_000,
    });

    const totalBeforeClose = (() => {
      const b = engineCommand("get_balance", { userId: "settle-maker" });
      return b.available + b.locked;
    })();

    // settle-maker now rests a sell to close, as the MAKER this time
    engineCommand("create_order", {
      userId: "settle-maker", symbol: "SETTLE-MAKER-USD",
      orderType: "limit", side: "sell", price: 110, qty: 5, leverage: 10,
    });
    // counterparty takes it, filling settle-maker's resting close order
    engineCommand("create_order", {
      userId: "settle-maker-counterparty", symbol: "SETTLE-MAKER-USD",
      orderType: "market", side: "buy", qty: 5, leverage: 1, slippageBps: 10_000,
    });

    const afterClose = engineCommand("get_balance", { userId: "settle-maker" });

    expect(afterClose.locked).toBe(0);
    // price rose 100 -> 110 on a long: realized PnL is exactly (110-100)*5 = 50
    expect(afterClose.available + afterClose.locked).toBe(totalBeforeClose + 50);
  });

  it("only releases the netted portion on a partial reduce, leaving the rest correctly locked", () => {
    engineCommand("create_market", {
      marketId: "settle-reduce-market",
      symbol: "SETTLE-REDUCE-USD",
      maxLeverage: 20,
      minQty: 1,
    });
    engineCommand("add_balance", { userId: "settle-reducer", amount: 1_000_000 });
    engineCommand("add_balance", { userId: "settle-reducer-mm", amount: 1_000_000 });

    // bid strictly below the ask so the two resting mm quotes don't cross each other
    engineCommand("create_order", {
      userId: "settle-reducer-mm", symbol: "SETTLE-REDUCE-USD",
      orderType: "limit", side: "sell", price: 100, qty: 10, leverage: 1,
    });
    engineCommand("create_order", {
      userId: "settle-reducer-mm", symbol: "SETTLE-REDUCE-USD",
      orderType: "limit", side: "buy", price: 99, qty: 10, leverage: 1,
    });

    const totalBefore = (() => {
      const b = engineCommand("get_balance", { userId: "settle-reducer" });
      return b.available + b.locked;
    })();

    engineCommand("create_order", {
      userId: "settle-reducer", symbol: "SETTLE-REDUCE-USD",
      orderType: "market", side: "buy", qty: 10, leverage: 10, slippageBps: 10_000,
    });
    // get_balance returns a live reference, so pull out the primitive before more trades mutate it
    const lockedAfterOpen = engineCommand("get_balance", { userId: "settle-reducer" }).locked as number;

    const reduceOrder = engineCommand("create_order", {
      userId: "settle-reducer", symbol: "SETTLE-REDUCE-USD",
      orderType: "market", side: "sell", qty: 4, leverage: 10, slippageBps: 10_000,
    });
    const afterReduce = engineCommand("get_balance", { userId: "settle-reducer" });
    const lockedAfterReduce = afterReduce.locked as number;
    const totalAfterReduce = (afterReduce.available as number) + lockedAfterReduce;
    const reducePnl = reduceOrder.closedPositions[0].realizedPnl as number;

    // locked should shrink by exactly 4/10 of what was locked at open, not stay inflated
    expect(lockedAfterReduce).toBe(Math.floor(lockedAfterOpen * 6 / 10));
    expect(totalAfterReduce).toBe(totalBefore + reducePnl);

    const closeOrder = engineCommand("create_order", {
      userId: "settle-reducer", symbol: "SETTLE-REDUCE-USD",
      orderType: "market", side: "sell", qty: 6, leverage: 10, slippageBps: 10_000,
    });
    const afterFullClose = engineCommand("get_balance", { userId: "settle-reducer" });
    const closePnl = closeOrder.closedPositions[0].realizedPnl as number;

    expect(afterFullClose.locked).toBe(0);
    expect(afterFullClose.available + afterFullClose.locked).toBe(totalBefore + reducePnl + closePnl);
  });
});

describe("updatePosition", () => {
  it("computes positive realized PnL for a long reduce when price rose", () => {
    const userId = "long-reduce";
    const symbol = "BTC-PERP";

    updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 10, fillPrice: 100, fillMargin: 100, leverage: 10,
      fillCreatedAt: 1000,
    });

    const close = updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 4, fillPrice: 120, fillMargin: 48, leverage: 10,
      fillCreatedAt: 2000,
    });

    expect(close).not.toBeNull();
    expect(close!.closeType).toBe("reduce");
    expect(close!.positionSide).toBe("long");
    expect(close!.realizedPnl).toBe(80); // (120 - 100) * 4
    expect(close!.marginReleased).toBe(40); // floor(100 * 4 / 10)
    expect(close!.openedAt).toBe(1000);
    expect(close!.closedAt).toBe(2000);

    const remaining = POSITIONS.get(userId)!.get(symbol)!;
    expect(remaining.qty).toBe(6);
    expect(remaining.margin).toBe(60);
  });

  it("computes positive realized PnL for a short reduce when price dropped", () => {
    const userId = "short-reduce";
    const symbol = "BTC-PERP";

    updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 10, fillPrice: 100, fillMargin: 100, leverage: 10,
      fillCreatedAt: 1000,
    });

    const close = updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 4, fillPrice: 80, fillMargin: 32, leverage: 10,
      fillCreatedAt: 2000,
    });

    expect(close!.positionSide).toBe("short");
    expect(close!.realizedPnl).toBe(80); // -(80 - 100) * 4
  });

  it("closes exactly and removes the position", () => {
    const userId = "exact-close";
    const symbol = "BTC-PERP";

    updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 5, fillPrice: 100, fillMargin: 50, leverage: 10,
      fillCreatedAt: 1000,
    });

    const close = updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 5, fillPrice: 110, fillMargin: 55, leverage: 10,
      fillCreatedAt: 3000,
    });

    expect(close!.closeType).toBe("close");
    expect(close!.realizedPnl).toBe(50);
    expect(close!.marginReleased).toBe(50);
    expect(POSITIONS.get(userId)!.get(symbol)).toBeUndefined();
  });

  it("flips: closes the old side at its old qty and opens a fresh position on the new side", () => {
    const userId = "flip";
    const symbol = "BTC-PERP";

    updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 10, fillPrice: 100, fillMargin: 100, leverage: 10,
      fillCreatedAt: 1000,
    });

    const close = updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 15, fillPrice: 90, fillMargin: 135, leverage: 10,
      fillCreatedAt: 4000,
    });

    expect(close!.closeType).toBe("flip");
    expect(close!.positionSide).toBe("short");
    expect(close!.qty).toBe(10); // old qty, not the 15 that filled
    expect(close!.realizedPnl).toBe(100); // -(90 - 100) * 10
    expect(close!.marginReleased).toBe(100);
    expect(close!.openedAt).toBe(1000);

    const flipped = POSITIONS.get(userId)!.get(symbol)!;
    expect(flipped.positionSide).toBe("long");
    expect(flipped.qty).toBe(5); // remaining 15 - 10
    expect(flipped.margin).toBe(45); // scaled to the 5 remaining qty, not the full 15-qty fillMargin
    expect(flipped.openedAt).toBe(4000);
  });

  it("preserves openedAt across same-side increases", () => {
    const userId = "increase";
    const symbol = "BTC-PERP";

    updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 5, fillPrice: 100, fillMargin: 50, leverage: 10,
      fillCreatedAt: 1000,
    });

    const result = updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 5, fillPrice: 110, fillMargin: 55, leverage: 10,
      fillCreatedAt: 5000,
    });

    expect(result).toBeNull();
    expect(POSITIONS.get(userId)!.get(symbol)!.openedAt).toBe(1000);
  });

  it("keeps margin an integer after a non-evenly-divisible partial reduce", () => {
    const userId = "float-leak";
    const symbol = "BTC-PERP";

    updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 3, fillPrice: 100, fillMargin: 100, leverage: 3,
      fillCreatedAt: 1000,
    });

    const close = updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 1, fillPrice: 100, fillMargin: 34, leverage: 3,
      fillCreatedAt: 2000,
    });

    expect(Number.isInteger(close!.marginReleased)).toBe(true);
    expect(close!.marginReleased).toBe(33); // floor(100 * 1 / 3)
    expect(Number.isInteger(POSITIONS.get(userId)!.get(symbol)!.margin)).toBe(true);
  });

  it("balance conservation: open -> partial reduce -> full close changes available+locked by exactly the realized PnL", () => {
    const userId = "conservation";
    const symbol = "BTC-PERP";

    // mimics createOrder.ts's order-placement margin lock — separate from updatePosition itself
    const balance = { available: 1000, locked: 0 };
    const openMargin = 100;
    balance.available -= openMargin;
    balance.locked += openMargin;

    updatePosition({
      userId, symbol, positionSide: "long",
      fillQty: 10, fillPrice: 100, fillMargin: openMargin, leverage: 10,
      fillCreatedAt: 1000,
    });

    function settle(close: PositionClose | null) {
      if (!close) return;
      balance.locked -= close.marginReleased;
      balance.available += close.marginReleased + close.realizedPnl;
    }

    const totalBefore = balance.available + balance.locked;

    const reduceClose = updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 4, fillPrice: 120, fillMargin: 48, leverage: 10,
      fillCreatedAt: 2000,
    });
    settle(reduceClose);

    const closeClose = updatePosition({
      userId, symbol, positionSide: "short",
      fillQty: 6, fillPrice: 90, fillMargin: 54, leverage: 10,
      fillCreatedAt: 3000,
    });
    settle(closeClose);

    const expectedPnl = (reduceClose?.realizedPnl ?? 0) + (closeClose?.realizedPnl ?? 0);
    const totalAfter = balance.available + balance.locked;

    expect(totalAfter - totalBefore).toBe(expectedPnl);
    expect(balance.locked).toBe(0);
  });
});

describe("price feeder", () => {
  beforeEach(() => {
    resetPriceFeederState();
  });

  it("subscribes to a Binance mark-price stream once per market", () => {
    const sentMessages: string[] = [];
    const ws = {
      send(message: string) {
        sentMessages.push(message);
      },
    };

    newMarket(ws as any, "BTC");
    newMarket(ws as any, "BTC");
    newMarket(ws as any, "SOL");

    expect(sentMessages.map((message) => JSON.parse(message))).toEqual([
      {
        method: "SUBSCRIBE",
        params: ["btcusdt@markPrice@1s"],
        id: 1,
      },
      {
        method: "SUBSCRIBE",
        params: ["solusdt@markPrice@1s"],
        id: 2,
      },
    ]);
    expect(subscribedMarkets).toEqual(new Set(["BTC", "SOL"]));
  });

  it("fans a shared feed symbol out to every market on it, instead of the last one winning", () => {
    // regression: two markets with the same base asset but different quote
    // segments (e.g. "PUMP-PERP" and "PUMP-USD") both resolve to Binance
    // feed symbol PUMPUSDT. The old implementation kept a single
    // feedSymbol -> marketSymbol mapping, so the second newMarket() call
    // silently overwrote the first — that market's index price went dead
    // forever with no error anywhere in the pipeline.
    const sentMessages: string[] = [];
    const ws = {
      send(message: string) {
        sentMessages.push(message);
      },
    };

    newMarket(ws as any, "PUMP-PERP");
    newMarket(ws as any, "PUMP-USD");

    // only one Binance SUBSCRIBE for the shared feed symbol — no duplicate stream
    expect(sentMessages).toHaveLength(1);
    expect(JSON.parse(sentMessages[0]!).params).toEqual(["pumpusdt@markPrice@1s"]);

    // but both markets are still tracked and would both receive the tick
    expect(getMarketSymbolsForFeedSymbol("PUMPUSDT")).toEqual(new Set(["PUMP-PERP", "PUMP-USD"]));
  });

  it("maps a base-quote market symbol to its base-asset Binance feed, ignoring the quote segment", () => {
    // regression: toBinanceSymbol used to only strip "-"/"_"/"/" and append USDT,
    // so "BTC-PERP" became the bogus feed symbol "BTCPERPUSDT" — Binance silently
    // ACKs a SUBSCRIBE to it but never pushes data, so index prices went dead
    // with no error anywhere in the pipeline.
    const sentMessages: string[] = [];
    const ws = {
      send(message: string) {
        sentMessages.push(message);
      },
    };

    newMarket(ws as any, "BTC-PERP");
    newMarket(ws as any, "ETH-USD");

    expect(sentMessages.map((message) => JSON.parse(message).params[0])).toEqual([
      "btcusdt@markPrice@1s",
      "ethusdt@markPrice@1s",
    ]);
  });
});
