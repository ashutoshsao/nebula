import { WebSocket } from "ws";

export const subscribedMarkets = new Set<string>();
// multiple market symbols can share a base asset (e.g. "BTC-PERP" and "BTC-USD"
// both feed off Binance's BTCUSDT) — fan out to every market on that feed symbol,
// don't just keep the last one
const feedSymbolToMarketSymbols = new Map<string, Set<string>>();
let socketSubscriptions = new WeakMap<WebSocket, Set<string>>();

let subscribeId = 1

// market symbols are "{BASE}-{QUOTE}" (e.g. "BTC-PERP", "ETH-USD") — only the base
// asset maps to a Binance feed; the quote segment ("PERP", "USD", ...) is ours, not Binance's
function toBinanceSymbol(symbol: string) {
  const base = symbol.split(/[-_/]/)[0]!.toUpperCase();
  return `${base}USDT`;
}

export function getFeedSymbolForMarketSymbol(symbol: string) {
  return toBinanceSymbol(symbol);
}

export function newMarket(ws: WebSocket, symbol: string) {
  const feedSymbol = getFeedSymbolForMarketSymbol(symbol);

  const marketsOnFeed = feedSymbolToMarketSymbols.get(feedSymbol) ?? new Set<string>();
  marketsOnFeed.add(symbol);
  feedSymbolToMarketSymbols.set(feedSymbol, marketsOnFeed);
  subscribedMarkets.add(symbol);

  const subscriptions = socketSubscriptions.get(ws) ?? new Set<string>();
  socketSubscriptions.set(ws, subscriptions);
  if (subscriptions.has(feedSymbol)) return;
  subscriptions.add(feedSymbol);

  ws.send(JSON.stringify({
    method: "SUBSCRIBE",
    params: [`${feedSymbol.toLowerCase()}@markPrice@1s`],
    id: subscribeId++
  }))
}

export function getMarketSymbolsForFeedSymbol(feedSymbol: string) {
  return feedSymbolToMarketSymbols.get(feedSymbol) ?? new Set<string>();
}

/** test-only: clears all module-level subscription state */
export function resetPriceFeederState() {
  subscribedMarkets.clear();
  socketSubscriptions = new WeakMap();
  feedSymbolToMarketSymbols.clear();
}
