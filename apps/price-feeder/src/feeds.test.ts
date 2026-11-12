import { describe, expect, it } from "bun:test";
import { EventEmitter } from "node:events";
import WebSocket from "ws";
import { FeedConnection, parseTick, PriceRouter, type Source, type Tick } from "./feeds";
import { newMarket, resetPriceFeederState } from "./helper/newMarket";

const frame = (source: Source, time: number, index = "123.456") => JSON.stringify({ data: {
  e: source === "backpack" ? "markPrice" : "markPriceUpdate",
  s: source === "backpack" ? "BTC_USDC_PERP" : "BTCUSDT",
  i: index, p: "999", E: source === "backpack" ? time * 1000 : time,
} });

describe("feed parsing", () => {
  it("normalizes index cents and timestamps from both venues, ignoring mark price", () => {
    for (const source of ["binance", "backpack"] as const) {
      expect(parseTick(source, frame(source, 100_000))).toEqual({ base: "BTC", price: 12346, time: 100_000 });
    }
  });
  it("rejects malformed frames, control frames and invalid prices", () => {
    for (const raw of ["broken", '{"result":null}', "null", frame("backpack", 100, "NaN"), frame("backpack", 100, "-2")]) {
      expect(parseTick("backpack", raw)).toBeNull();
    }
  });
});

describe("Backpack primary routing", () => {
  it("waits for primary, fails over per market, recovers and suppresses competing sources", () => {
    let now = 100_000;
    const sent: Array<{ source: Source; tick: Tick }> = [];
    const router = new PriceRouter((tick, source) => sent.push({ source, tick }), () => now);
    const send = (source: Source, base = "BTC", time = now) => router.accept(source, { base, price: 12345, time });
    send("binance");
    expect(sent).toHaveLength(0);
    send("backpack");
    now += 1000;
    send("binance");
    expect(sent.map(x => x.source)).toEqual(["backpack"]);
    now += 10_000;
    send("binance");
    expect(sent.map(x => x.source)).toEqual(["backpack", "binance"]);
    now += 1;
    send("backpack");
    now += 1;
    send("binance");
    send("backpack", "BTC", now - 20_000); // delayed frame
    send("backpack", "BTC", now + 10_000); // future frame
    send("backpack", "BTC", now - 1); // duplicate timestamp
    expect(sent.map(x => x.source)).toEqual(["backpack", "binance", "backpack"]);
    send("binance", "ETH"); // BTC health must not prevent ETH fallback
    expect(sent.at(-1)?.tick.base).toBe("ETH");
  });
});

class FakeSocket extends EventEmitter {
  readyState = WebSocket.CONNECTING as number;
  sent: string[] = [];
  terminated = false;
  send(raw: string) { this.sent.push(raw); }
  terminate() { this.terminated = true; this.readyState = WebSocket.CLOSED; this.emit("close", 1006); }
  open() { this.readyState = WebSocket.OPEN; this.emit("open"); }
}

describe("connection recovery", () => {
  it("terminates a silent socket and resubscribes all markets on the replacement", async () => {
    let now = 100_000;
    const sockets: FakeSocket[] = [];
    const ticks: Tick[] = [];
    const feed = new FeedConnection("backpack", tick => ticks.push(tick), () => {
      const socket = new FakeSocket(); sockets.push(socket); return socket as unknown as WebSocket;
    }, () => now);
    try {
      feed.addMarket("BTC-PERP");
      feed.addMarket("BTC-USD");
      feed.start();
      sockets[0]!.open();
      expect(sockets[0]!.sent).toHaveLength(1);
      sockets[0]!.emit("message", frame("backpack", now));
      expect(ticks).toHaveLength(1);
      now += 16_000;
      sockets[0]!.emit("message", frame("backpack", now - 20_000));
      feed.check();
      expect(sockets[0]!.terminated).toBe(true);
      await Bun.sleep(1100);
      expect(sockets).toHaveLength(2);
      sockets[1]!.open();
      expect(sockets[1]!.sent).toEqual(sockets[0]!.sent);
      sockets[1]!.emit("message", frame("backpack", now));
      feed.check();
      expect(sockets[1]!.terminated).toBe(false);
      feed.addMarket("ETH-PERP");
      expect(JSON.parse(sockets[1]!.sent[1]!).params).toEqual(["markPrice.ETH_USDC_PERP"]);
    } finally { feed.stop(); }
  });
  it("keeps legacy subscription deduplication scoped to the socket", () => {
    resetPriceFeederState();
    const old = new FakeSocket(); const replacement = new FakeSocket();
    newMarket(old as unknown as WebSocket, "BTC-PERP");
    newMarket(old as unknown as WebSocket, "BTC-PERP");
    newMarket(replacement as unknown as WebSocket, "BTC-PERP");
    expect(old.sent).toHaveLength(1);
    expect(replacement.sent).toHaveLength(1);
  });
});
