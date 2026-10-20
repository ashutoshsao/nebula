import { prisma } from "@repo/db";
import { getRedisClient } from "@repo/redis";
import { Request, Response } from "express";
import { loopback } from "../service/loopBack";

// Deep health for the status page: proves an order could actually flow (database, Redis, and the engine
// answering a read-only command). Not meant for the k8s liveness probe: a database blip shouldn't restart pods.
const redisPromise = getRedisClient();
const TIMEOUT_MS = 3000;

function within<T>(work: Promise<T>, ms: number): Promise<T> {
  return Promise.race([
    work,
    new Promise<never>((_, reject) => setTimeout(() => reject(new Error(`timed out after ${ms}ms`)), ms)),
  ]);
}

type Check = { ok: boolean; ms?: number; error?: string };

async function check(name: string, run: () => Promise<unknown>): Promise<[string, Check]> {
  const started = performance.now();
  try {
    await within(run(), TIMEOUT_MS);
    return [name, { ok: true, ms: Math.round(performance.now() - started) }];
  } catch (error) {
    return [name, { ok: false, error: (error as Error).message }];
  }
}

export async function health(_req: Request, res: Response) {
  const checks: Record<string, Check> = Object.fromEntries(await Promise.all([
    check("database", () => prisma.$queryRaw`SELECT 1`),
    check("redis", async () => (await redisPromise).ping()),
    check("engine", () => loopback("get_depth", { symbol: "BTC-PERP" })),
  ]));
  const ok = Object.values(checks).every((c) => c.ok);
  const version = { version: process.env.VERSION ?? "dev", commit: process.env.GIT_SHA ?? "unknown" };
  res.status(ok ? 200 : 503).json({ status: ok ? "ok" : "degraded", ...version, checks });
}
