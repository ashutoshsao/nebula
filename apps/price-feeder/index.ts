import { loopback } from "./src/loopBack";
import { getRedisClient } from "@repo/redis";
import { REDIS_KEYS } from "@repo/types";
import { baseAsset, FeedConnection, PriceRouter, type Tick } from "./src/feeds";

const readRedis = getRedisClient();
const writeRedis = getRedisClient();
const markets = new Set<string>();
const published = new Map<string, number>();
const started = Date.now();
const fatal = (error: unknown) => {
  console.error("price-feeder: fatal", error);
  process.exit(1);
};

const router = new PriceRouter((tick) => { void publish(tick).catch(fatal); });
const feeds = (["binance", "backpack"] as const).map(source =>
  new FeedConnection(source, tick => router.accept(source, tick)));

function addMarket(symbol: string) {
  markets.add(symbol);
  for (const feed of feeds) feed.addMarket(symbol);
}

async function publish(tick: Tick) {
  const redis = await writeRedis;
  for (const symbol of markets) {
    if (baseAsset(symbol) !== tick.base) continue;
    await redis.xAdd(REDIS_KEYS.engineCommands, "*", {
      type: "update_index_price",
      correlationId: crypto.randomUUID(),
      responseQueue: "",
      payload: JSON.stringify({ symbol, price: tick.price }),
    });
    await redis.publish(`market:${symbol}:index`, JSON.stringify({ symbol, price: tick.price, time: tick.time }));
    published.set(symbol, Date.now());
  }
}

async function watchNewMarkets() {
  const redis = await readRedis;
  // Pin a real cursor before bootstrap, so markets created during bootstrap
  // cannot fall between the engine snapshot and a blocking read using "$".
  const newest = await redis.xRevRange(REDIS_KEYS.engineEvents, "+", "-", { COUNT: 1 });
  let lastId = newest[0]?.id ?? "0-0";
  const existing = await loopback() as string[];
  for (const symbol of existing) addMarket(symbol);
  for (const feed of feeds) feed.start();
  console.log(`price-feeder: started ${existing.join(", ")} with Backpack primary / Binance fallback`);
  while (true) {
    const streams = await redis.xRead([{ key: REDIS_KEYS.engineEvents, id: lastId }], { BLOCK: 0, COUNT: 10 });
    if (!streams) continue;
    for (const stream of streams) {
      for (const msg of stream.messages) {
        lastId = msg.id;
        if (msg.message.type === "create_market") addMarket(JSON.parse(msg.message.data).symbol);
      }
    }
  }
}

// Check successful Redis publication, rather than merely an open socket.
Bun.serve({
  port: 8080,
  fetch() {
    const now = Date.now();
    const stale = [...markets].filter(symbol => now - (published.get(symbol) ?? started) > 30_000);
    const ready = markets.size > 0 && [...markets].every(symbol => published.has(symbol)) && stale.length === 0;
    return Response.json({ status: ready ? "ok" : "stale", stale }, { status: ready ? 200 : 503 });
  },
});
setInterval(() => { for (const feed of feeds) feed.check(); }, 1000);
for (const signal of ["SIGTERM", "SIGINT"] as const) {
  process.on(signal, () => { for (const feed of feeds) feed.stop(); process.exit(0); });
}
watchNewMarkets().catch(fatal);
