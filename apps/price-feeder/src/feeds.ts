import WebSocket from "ws";

export type Source = "binance" | "backpack";
export type Tick = { base: string; price: number; time: number };
export const PRIMARY_FRESH_MS = 10_000;
export const STALE_MS = 15_000;

// Both streams carry index (i) and mark (p). Nebula consumes the index.
export function parseTick(source: Source, raw: string): Tick | null {
  try {
    const frame = JSON.parse(raw);
    const msg = frame.data ?? frame;
    if (msg.e !== (source === "binance" ? "markPriceUpdate" : "markPrice")) return null;
    const suffix = source === "binance" ? "USDT" : "_USDC_PERP";
    if (typeof msg.s !== "string" || !msg.s.endsWith(suffix)) return null;
    const price = Math.round(Number(msg.i) * 100);
    const time = Number(msg.E) / (source === "backpack" ? 1000 : 1);
    if (!Number.isSafeInteger(price) || price <= 0 || !Number.isFinite(time) || time <= 0) return null;
    return { base: msg.s.slice(0, -suffix.length), price, time };
  } catch {
    return null;
  }
}

export const baseAsset = (symbol: string) => symbol.split(/[-_/]/)[0]!.toUpperCase();
const streamName = (source: Source, base: string) => source === "binance"
  ? `${base.toLowerCase()}usdt@markPrice@1s` : `markPrice.${base}_USDC_PERP`;
const urls: Record<Source, string> = {
  binance: "wss://fstream.binance.com/market/stream",
  backpack: "wss://ws.backpack.exchange",
};

export class FeedConnection {
  private socket?: WebSocket;
  private reconnect?: ReturnType<typeof setTimeout>;
  private delay = 1000;
  private stopped = false;
  private lastTick = new Map<string, number>();
  private requested = new Map<string, number>();
  private bases = new Set<string>();

  constructor(
    readonly source: Source,
    private onTick: (tick: Tick) => void,
    private factory: (url: string) => WebSocket = (url) => new WebSocket(url, { handshakeTimeout: 10_000 }),
    private now = Date.now,
  ) {}

  addMarket(symbol: string) {
    const base = baseAsset(symbol);
    if (this.bases.has(base)) return;
    this.bases.add(base);
    if (this.socket?.readyState === WebSocket.OPEN) this.subscribe(base);
  }

  start() {
    if (this.stopped) return;
    const socket = this.factory(urls[this.source]);
    this.socket = socket;
    // Subscription state belongs to this connection, never the process.
    this.requested.clear();
    this.lastTick.clear();
    socket.on("open", () => {
      if (socket !== this.socket || this.stopped) return;
      console.log(`price-feeder: ${this.source} connected`);
      for (const base of this.bases) this.subscribe(base);
    });
    socket.on("message", (raw) => {
      if (socket !== this.socket || this.stopped) return;
      const tick = parseTick(this.source, raw.toString());
      const now = this.now();
      if (!tick || !this.bases.has(tick.base) || now - tick.time > PRIMARY_FRESH_MS || tick.time - now > 5000) return;
      this.lastTick.set(tick.base, now);
      this.delay = 1000; // A successful handshake alone does not prove recovery.
      this.onTick(tick);
    });
    socket.on("error", (err) => {
      console.error(`price-feeder: ${this.source} socket error: ${err.message}`);
      socket.terminate();
    });
    socket.on("close", (code) => {
      if (socket !== this.socket || this.stopped) return;
      this.socket = undefined;
      this.requested.clear();
      console.error(`price-feeder: ${this.source} closed (${code}); reconnecting in ${this.delay}ms`);
      this.reconnect = setTimeout(() => this.start(), this.delay);
      this.delay = Math.min(this.delay * 2, 30_000);
    });
  }

  private subscribe(base: string) {
    this.socket!.send(JSON.stringify({ method: "SUBSCRIBE", params: [streamName(this.source, base)], id: 1 }));
    this.requested.set(base, this.now());
  }

  check() {
    const now = this.now();
    for (const [base, requestedAt] of this.requested) {
      if (now - (this.lastTick.get(base) ?? requestedAt) > STALE_MS) {
        console.error(`price-feeder: ${this.source} ${base} stale; terminating socket`);
        this.socket?.terminate();
        break;
      }
    }
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.reconnect);
    this.socket?.terminate();
  }
}

// One writer per market. Keep the fallback warm, but use it only when the
// primary is stale. Reject old/out-of-order source timestamps on every path.
export class PriceRouter {
  private primary = new Map<string, number>();
  private latest = new Map<string, number>();
  private sources = new Map<string, Source>();
  private started: number;

  constructor(private publish: (tick: Tick, source: Source) => void, private now = Date.now) { this.started = now(); }

  accept(source: Source, tick: Tick) {
    const now = this.now();
    if (now - tick.time > PRIMARY_FRESH_MS || tick.time - now > 5000) return;
    if (source === "backpack") this.primary.set(tick.base, now);
    if (source === "binance" && now - (this.primary.get(tick.base) ?? this.started) < PRIMARY_FRESH_MS) return;
    if (tick.time <= (this.latest.get(tick.base) ?? 0)) return;
    this.latest.set(tick.base, tick.time);
    if (this.sources.get(tick.base) !== source) {
      console.log(`price-feeder: ${tick.base} using ${source} index`);
      this.sources.set(tick.base, source);
    }
    this.publish(tick, source);
  }
}
