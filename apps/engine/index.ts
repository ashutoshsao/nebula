import { getRedisClient } from "@repo/redis";
import { CancelOrderResponse, CreateOrderResponse, EngineRequest, nextFundingTime, OrderRecord, REDIS_KEYS, RedisResponseType, UpdateIndexPriceResponse } from "@repo/types";
import { handleCommand } from "./src/controller/engine.controller";
import { loadSnapshot, saveSnapshot } from "./src/helper/snapshot";
import { ORDERS } from "./src/engine-store";
const readClient = getRedisClient()
const writeClient = getRedisClient()
type RedisClient = Awaited<ReturnType<typeof getRedisClient>>

function evictIfTerminal(order: OrderRecord | undefined) {
  if (!order) return;
  if (order.status === "filled" || order.status === "cancelled") {
    ORDERS.delete(order.orderId);
  }
}
const GLOBAL_EVENTS = new Set([
  "funding_rate",
  "create_order",
  "cancel_order",
  "create_market"
])

let lastSeenId: string
let lastSnapshotTime = Date.now()

//snapshot every 5 mins
const SNAPSHOT_INTERVAL = 5 * 60 * 1000
// Commands read per XREAD round trip.
const READ_BATCH = 500

async function startUp() {
  //loadSnapshot
  lastSeenId = await loadSnapshot();
  let readRedis = await readClient;
  let writeRedis = await writeClient;

  scheduleFundingRate(writeRedis);
  while (true) {
    const streams = await readRedis.xRead([{
      key: REDIS_KEYS.engineCommands,
      id: lastSeenId
    }], {
      BLOCK: 0,
      COUNT: READ_BATCH
    }) as RedisResponseType | null;
    if (!streams) continue;
    // Writes for the whole batch are sent without waiting on each one (the client pipelines them
    // in order on one connection) and awaited together before the next read or snapshot.
    const pending: Promise<unknown>[] = [];
    for (const stream of streams) {
      for (const msg of stream.messages) {
        lastSeenId = msg.id;
        const { correlationId, type, responseQueue, payload } = msg.message
        try {
          const request: EngineRequest = {
            streamMsgId: msg.id,
            correlationId,
            type: type as EngineRequest['type'],
            responseQueue,
            payload: JSON.parse(payload)
          }

          const response = handleCommand(request);

          if (!response) continue;

          if (type === "update_index_price") {
            const { symbol, markPrice, events, predictedFundingRate, fundingSamples } = response as UpdateIndexPriceResponse;

            // mark price is ephemeral — fire-and-forget pub/sub, never the durable event log
            const time = parseInt(`${msg.id.split("-")[0]}`);
            pending.push(writeRedis.publish(`market:${symbol}:markPrice`, JSON.stringify({
              symbol,
              price: markPrice,
              time
            })))

            // not awaited: runs every price tick, can't afford a redis round trip here
            writeRedis.set(REDIS_KEYS.predictedFunding(symbol), JSON.stringify({
              symbol,
              rate: predictedFundingRate,
              samples: fundingSamples,
              updatedAt: time,
            }), { EX: 300 }).catch((err) => console.log(`predicted funding write failed for ${symbol}: ${(err as Error).message}`))

            // the durable stream only sees ticks that carry liquidation/ADL events
            if (events.length > 0) {
              pending.push(writeRedis.xAdd(REDIS_KEYS.engineEvents, '*', {
                type,
                correlationId,
                ok: 'true',
                error: '',
                data: JSON.stringify(response),
              }, {
                TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 10_000 }
              }))

              for (const event of events) {
                evictIfTerminal(event.order);
                event.makerOrders.forEach(evictIfTerminal);
              }
            }
            continue;
          }

          if (GLOBAL_EVENTS.has(type)) {
            // create_order, cancel_order, create_market,
            pending.push(writeRedis.xAdd(REDIS_KEYS.engineEvents, '*', {
              type,
              correlationId,
              ok: 'true',
              error: '',
              data: JSON.stringify(response),
            }, {
              TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 10_000 }
            }))

            if (type === "create_order") {
              const { order, makerOrders } = response as CreateOrderResponse;
              evictIfTerminal(order);
              makerOrders.forEach(evictIfTerminal);
            } else if (type === "cancel_order") {
              const { order } = response as CancelOrderResponse;
              evictIfTerminal(order);
            }
          } else {
            pending.push(writeRedis.xAdd(responseQueue, '*', {
              type,
              correlationId,
              ok: 'true',
              error: '',
              data: JSON.stringify(response),
            }, {
              TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 100 }
            }))
            // caller's process may never come back to read this (crash/restart) —
            // let the queue self-destruct instead of leaking forever
            pending.push(writeRedis.expire(responseQueue, 300))
          }
        }
        catch (err) {
          pending.push(writeRedis.xAdd(responseQueue, '*', {
            type,
            correlationId,
            ok: 'false',
            error: (err as Error).message,
            data: '',
          }, {
            TRIM: { strategy: 'MAXLEN', strategyModifier: '~', threshold: 100 }
          }))
          pending.push(writeRedis.expire(responseQueue, 300))
        }
      }
    }
    await Promise.all(pending);
    if (Date.now() - lastSnapshotTime > SNAPSHOT_INTERVAL) {
      try {
        await saveSnapshot(lastSeenId)
        lastSnapshotTime = Date.now()
        // safe to drop anything older than what's now durably snapshotted
        await writeRedis.xTrim(REDIS_KEYS.engineCommands, 'MINID', lastSeenId)
      } catch (err) {
        // Wait a full interval before retrying. Without this the check stays true and every
        // subsequent command retries the R2 upload inside the matching loop.
        lastSnapshotTime = Date.now()
        console.log(`Snapshot save failed, will retry next interval: ${(err as Error).message}`)
      }
    }
  }
}

function scheduleFundingRate(writeRedis: RedisClient) {
  setTimeout(async function trigger() {
    await writeRedis.xAdd(REDIS_KEYS.engineCommands, '*', {
      type: "funding_rate",
      correlationId: crypto.randomUUID(),
      responseQueue: '',
      payload: JSON.stringify({})
    })
    scheduleFundingRate(writeRedis);
  }, nextFundingTime() - Date.now())
}

startUp().catch((err) => {
  console.error("Engine crashed:", err);
  process.exit(1);
});

