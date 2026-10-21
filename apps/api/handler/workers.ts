import { getRedisClient } from "@repo/redis";
import { REDIS_KEYS } from "@repo/types";
import { Request, Response } from "express";

// Health of the background workers that have no URL of their own (price-feeder, market-maker, db-poller, wss),
// read from the engine's event stream: each one either writes to it or consumes it, so its freshness shows
// whether the worker is doing its job, not just whether its process is up. A worker can be running and still
// dead: the price-feeder once kept running for ~31 hours after Binance dropped its socket.
let redisPromise: ReturnType<typeof getRedisClient> | undefined; // connected on first request (tests import this file)

export const SCAN = 1000; // newest events looked at: a few minutes of traffic
export const PRICE_MAX_AGE_MS = 60_000;
export const ORDER_MAX_AGE_MS = 120_000;
export const CONSUMER_MAX_BEHIND_MS = 60_000;
export const MAX_PENDING = 500; // delivered but not acknowledged: db-poller leaves failed writes pending

type Event = { id: string; message: Record<string, string> };
type Group = { name: string; pending: number; lastDeliveredId: string };
type Check = { ok: boolean; detail: string };

const idTime = (id: string) => Number(id.split("-")[0]);
const age = (now: number, id: string | undefined) => (id ? Math.round((now - idTime(id)) / 1000) : null);

function newest(events: Event[], type: string): string | undefined {
  return events.find((e) => e.message.type === type)?.id; // XREVRANGE: newest first
}

function fresh(now: number, id: string | undefined, maxAgeMs: number, what: string): Check {
  const seconds = age(now, id);
  if (seconds === null) return { ok: false, detail: `no ${what} in the last ${SCAN} events` };
  return { ok: now - idTime(id!) <= maxAgeMs, detail: `last ${what} ${seconds}s ago` };
}

function consumer(lastId: string, group: Group | undefined): Check {
  if (!group) return { ok: false, detail: "consumer group missing" };
  const behind = Math.round((idTime(lastId) - idTime(group.lastDeliveredId)) / 1000);
  return {
    ok: behind * 1000 <= CONSUMER_MAX_BEHIND_MS && group.pending <= MAX_PENDING,
    detail: `${behind}s behind the newest event, ${group.pending} pending`,
  };
}

// Pure, so it can be tested without Redis.
export function workerChecks(now: number, events: Event[], groups: Group[]): Record<string, Check> {
  const lastId = events[0]?.id ?? "0-0";
  const group = (name: string) => groups.find((g) => g.name === name);
  return {
    "price-feeder": fresh(now, newest(events, "update_index_price"), PRICE_MAX_AGE_MS, "index price"),
    "market-maker": fresh(now, newest(events, "create_order"), ORDER_MAX_AGE_MS, "order"),
    "db-poller": consumer(lastId, group("db-poller")),
    wss: consumer(lastId, group("wss")),
  };
}

export async function workers(_req: Request, res: Response) {
  try {
    const redis = await (redisPromise ??= getRedisClient());
    const [events, groups] = await Promise.all([
      redis.xRevRange(REDIS_KEYS.engineEvents, "+", "-", { COUNT: SCAN }),
      redis.xInfoGroups(REDIS_KEYS.engineEvents),
    ]);
    const checks = workerChecks(
      Date.now(),
      events as Event[],
      groups.map((g) => ({ name: String(g.name), pending: Number(g.pending), lastDeliveredId: String(g["last-delivered-id"]) })),
    );
    const ok = Object.values(checks).every((c) => c.ok);
    res.status(ok ? 200 : 503).json({ status: ok ? "ok" : "degraded", checks });
  } catch (error) {
    res.status(503).json({ status: "degraded", error: (error as Error).message });
  }
}
