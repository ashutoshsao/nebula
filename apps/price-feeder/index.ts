import WebSocket from "ws"
import { loopback } from "./src/loopBack";
import { getFeedSymbolForMarketSymbol, getMarketSymbolsForFeedSymbol, newMarket, subscribedMarkets } from "./src/helper/newMarket";
import { parseMarkPriceFrame } from "./src/helper/parseMarkPriceFrame";
import { getRedisClient } from "@repo/redis";
import { REDIS_KEYS } from "@repo/types";

const readRedis = getRedisClient();
const writeRedis = getRedisClient();
const enableRestFallback = process.env.PRICE_FEEDER_REST_FALLBACK === "true";
let restPollStarted = false;
let watchNewMarketsStarted = false;

const BASE_RECONNECT_DELAY_MS = 1_000;
const MAX_RECONNECT_DELAY_MS = 30_000;
let reconnectDelayMs = BASE_RECONNECT_DELAY_MS;

// mutable so a reconnect can swap in a fresh socket — watchNewMarkets() and
// message handlers below always read the current value via this binding
let ws: WebSocket;

function connect() {
  // markPrice is a "Market"-category stream under Binance's 2026-03 routed WS
  // architecture. The unrouted /stream endpoint still ACKs a SUBSCRIBE to it but
  // silently never pushes data (confirmed by hand against the live socket) — the
  // routed /market/stream endpoint is required to actually receive ticks.
  ws = new WebSocket("wss://fstream.binance.com/market/stream");

  ws.on("open", async () => {
    console.log("price-feeder: connected to Binance")
    reconnectDelayMs = BASE_RECONNECT_DELAY_MS
    if (!watchNewMarketsStarted) {
      watchNewMarketsStarted = true
      watchNewMarkets()  // start watching FIRST — don't miss any create_market events
    }
    const markets = await loopback() as string[]  // then get existing markets
    for (const symbol of markets) newMarket(ws, symbol)  // subscribe to all
    console.log(`price-feeder: subscribed to ${markets.length} market(s): ${markets.join(", ")}`)
    if (enableRestFallback) startPremiumIndexPoll()
  })

  // Binance connections drop periodically under normal operation (idle resets, network
  // blips, server-side restarts) — without reconnecting here the feeder goes silently
  // dead: the process keeps running, so nothing crashes or restarts it, but no more
  // prices ever get published. This previously caused a ~31 hour outage in production.
  ws.on("close", (code, reason) => {
    console.error(`price-feeder: Binance socket closed (${code}) ${reason.toString()} — reconnecting in ${reconnectDelayMs}ms`)
    setTimeout(connect, reconnectDelayMs)
    reconnectDelayMs = Math.min(reconnectDelayMs * 2, MAX_RECONNECT_DELAY_MS)
  })

  ws.on("error", (err) => {
    console.error("price-feeder: Binance socket error", err)
  })

  ws.on("message", async (data) => {
    const tick = parseMarkPriceFrame(data.toString())
    if (!tick) return

    const symbols = getMarketSymbolsForFeedSymbol(tick.feedSymbol)
    if (symbols.size === 0) return

    for (const symbol of symbols) {
      await publishIndexPrice(symbol, tick.price, tick.time)
    }
  })

  ws.on("ping", (data) => ws.pong(data))
}

connect()

async function watchNewMarkets() {
  const client = await readRedis
  let lastId = '$'

  while (true) {
    const streams = await client.xRead([
      { key: REDIS_KEYS.engineEvents, id: lastId }
    ], { BLOCK: 0, COUNT: 10 })

    if (!streams) continue

    for (const stream of streams) {
      for (const msg of stream.messages) {
        lastId = msg.id
        if (msg.message.type !== 'create_market') continue
        const data = JSON.parse(msg.message.data)
        newMarket(ws, data.symbol)
        if (enableRestFallback) startPremiumIndexPoll()
      }
    }
  }
}

async function publishIndexPrice(symbol: string, price: number, time: number) {
  const client = await writeRedis;
  await client.xAdd(REDIS_KEYS.engineCommands, '*', {
    type: 'update_index_price',
    correlationId: crypto.randomUUID(),
    responseQueue: '',
    payload: JSON.stringify({ symbol, price })
  })

  await client.publish(`market:${symbol}:index`, JSON.stringify({
    symbol,
    price,
    time
  }))
}

function startPremiumIndexPoll() {
  if (restPollStarted) return;
  restPollStarted = true;
  void pollPremiumIndex();
  setInterval(() => {
    void pollPremiumIndex();
  }, 3_000);
}

async function pollPremiumIndex() {
  for (const symbol of subscribedMarkets) {
    const feedSymbol = getFeedSymbolForMarketSymbol(symbol);
    try {
      const response = await fetch(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${feedSymbol}`);
      if (!response.ok) continue;
      const data = await response.json() as { markPrice?: string; time?: number };
      const markPrice = Number(data.markPrice);
      if (!Number.isFinite(markPrice) || markPrice <= 0) continue;
      await publishIndexPrice(symbol, Math.round(markPrice * 100), data.time ?? Date.now());
    } catch {
      // Keep the websocket feeder alive if the REST fallback misses a poll.
    }
  }
}
