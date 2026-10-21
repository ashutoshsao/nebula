import { describe, expect, test } from "bun:test";
import { workerChecks } from "./workers";

const NOW = 1_800_000_000_000;
const ev = (secondsAgo: number, type: string) => ({ id: `${NOW - secondsAgo * 1000}-0`, message: { type } });
const groups = (lastDeliveredId: string, pending = 0) => [
  { name: "db-poller", pending, lastDeliveredId },
  { name: "wss", pending: 0, lastDeliveredId },
];

describe("workerChecks", () => {
  const events = [ev(1, "create_order"), ev(2, "cancel_order"), ev(5, "update_index_price")];

  test("all fresh", () => {
    const checks = workerChecks(NOW, events, groups(events[0]!.id));
    expect(Object.values(checks).every((c) => c.ok)).toBe(true);
  });

  test("stale prices: the feeder is up but publishing nothing", () => {
    const checks = workerChecks(NOW, [ev(1, "create_order"), ev(90, "update_index_price")], groups(`${NOW - 1000}-0`));
    expect(checks["price-feeder"]!.ok).toBe(false);
    expect(checks["market-maker"]!.ok).toBe(true);
  });

  test("no orders in the scanned window", () => {
    const checks = workerChecks(NOW, [ev(1, "update_index_price")], groups(`${NOW - 1000}-0`));
    expect(checks["market-maker"]).toEqual({ ok: false, detail: "no order in the last 1000 events" });
  });

  test("db-poller behind or stuck on failed writes", () => {
    expect(workerChecks(NOW, events, groups(`${NOW - 300_000}-0`))["db-poller"]!.ok).toBe(false);
    expect(workerChecks(NOW, events, groups(events[0]!.id, 900))["db-poller"]!.ok).toBe(false);
  });

  test("missing consumer group", () => {
    const checks = workerChecks(NOW, events, []);
    expect(checks.wss).toEqual({ ok: false, detail: "consumer group missing" });
  });
});
