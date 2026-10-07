import { getRedisClient } from "@repo/redis";
import { EngineCommandType, EnginePayload, EngineResponse, REDIS_KEYS } from "@repo/types";

const writeClientPromise = getRedisClient();
const readClientPromise = getRedisClient();

type PendingRequest = {
  resolve: (value: unknown) => void
  reject: (reason: Error) => void
  timeout: ReturnType<typeof setTimeout>
}

const loopbackResponses = new Map<string, PendingRequest>();
const responseBe = crypto.randomUUID();
let lastGlobalId = '$';
let lastBackendId = '$';

export const loopback = async (type: EngineCommandType, payload: EnginePayload) => {
  const correlationId = crypto.randomUUID();

  return new Promise(async (resolve, reject) => {
    const redis = await writeClientPromise;

    const timeout = setTimeout(() => {
      loopbackResponses.delete(correlationId);
      reject(new Error("Engine response timed out"));
    }, 10_000);

    loopbackResponses.set(correlationId, { resolve, reject, timeout });

    try {
      await redis.xAdd(REDIS_KEYS.engineCommands, '*', {
        type,
        correlationId,
        responseQueue: REDIS_KEYS.responseQueue(responseBe),
        payload: JSON.stringify(payload),
      });
    } catch (err) {
      clearTimeout(timeout);
      loopbackResponses.delete(correlationId);
      console.error("Failed to send command to engine:", err);
      reject(new Error("Failed to send to engine"));
    }
  })
}

async function waitForResponse() {
  const redis = await readClientPromise;

  while (true) {
    const streams = await redis.xRead([
      { key: REDIS_KEYS.engineEvents, id: lastGlobalId },
      { key: REDIS_KEYS.responseQueue(responseBe), id: lastBackendId },
    ], { BLOCK: 0, COUNT: 500 }) // batch: one round trip per up-to-500 messages per stream

    if (!streams) continue;

    for (const stream of streams) {
      for (const msg of stream.messages) {
        if (stream.name === REDIS_KEYS.engineEvents) {
          lastGlobalId = msg.id;
        } else {
          lastBackendId = msg.id;
        }

        const raw = msg.message;
        // Most global events belong to other API instances: check ownership before parsing.
        const pending = loopbackResponses.get(raw.correlationId);
        if (!pending) continue;
        const engineResponse: EngineResponse = {
          type: raw.type as EngineCommandType,
          correlationId: raw.correlationId,
          ok: raw.ok === "true",
          data: raw.data ? JSON.parse(raw.data) : undefined,
          error: raw.error || undefined
        }

        clearTimeout(pending.timeout);
        engineResponse.ok
          ? pending.resolve(engineResponse.data)
          : pending.reject(new Error(engineResponse.error ?? "EngineError"));
        loopbackResponses.delete(engineResponse.correlationId);
      }
    }
  }
}

waitForResponse().catch((err) => {
  console.error("Loopback response listener crashed:", err);
  process.exit(1);
});
