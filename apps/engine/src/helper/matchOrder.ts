import { Fill, OrderRecord } from "@repo/types";
import { ORDERBOOKS } from "../engine-store";

export function matchOrder(limitPrice: number, order: OrderRecord, streamMsgId: string) {
  let orderbook = ORDERBOOKS.get(order.symbol);
  if (!orderbook) throw new Error(`Market ${order.symbol} doesn't exist`);

  const fills: Fill[] = [];
  let remainingQty = order.qty;
  let totalCost = 0;
  const touchedAskPrices: number[] = [];
  const touchedBidPrices: number[] = [];

  if (order.side === "buy") {
    orderbook.asks.forEachPair((askPrice, restingOrders) => {
      if (askPrice > limitPrice) return { break: 0 };
      if (remainingQty <= 0) return { break: 0 };

      touchedAskPrices.push(askPrice);
      for (const restingOrder of restingOrders) {
        if (remainingQty <= 0) break;
        const remainingFillQty = restingOrder.qty - restingOrder.filledQty;
        const fillQty = Math.min(remainingFillQty, remainingQty);

        fills.push({
          fillId: `${order.orderId}-fill-${restingOrder.orderId}`,
          symbol: order.symbol,
          qty: fillQty,
          price: askPrice,
          makerOrderId: restingOrder.orderId,
          takerOrderId: order.orderId,
          makerUserId: restingOrder.userId,
          takerUserId: order.userId,
          makerSide: "sell",
          createdAt: parseInt(`${streamMsgId.split('-')[0]}`)
        })
        //update lastTradedPrice
        orderbook.lastTradedPrice = askPrice;
        restingOrder.filledQty += fillQty;
        remainingQty -= fillQty;
        totalCost += fillQty * askPrice;

        //update restingOrder's status
        if (restingOrder.filledQty === restingOrder.qty) restingOrder.status = "filled";
        else if (restingOrder.filledQty === 0) restingOrder.status = "open";
        else restingOrder.status = "partially_filled";
      }
      const remainingRestingOrders = restingOrders.filter((order) => order.filledQty < order.qty);
      if (remainingRestingOrders.length === 0) {
        orderbook.asks.delete(askPrice);
      } else {
        orderbook.asks.set(askPrice, remainingRestingOrders);
      }
    })
  } else {
    //sell
    for (const [bidPrice, restingOrders] of orderbook.bids.entriesReversed()) {
      if (bidPrice < limitPrice) break;
      if (remainingQty <= 0) break;

      touchedBidPrices.push(bidPrice);
      for (const restingOrder of restingOrders) {
        if (remainingQty <= 0) break;
        const remainingFillQty = restingOrder.qty - restingOrder.filledQty;
        const fillQty = Math.min(remainingFillQty, remainingQty);

        fills.push({
          symbol: order.symbol,
          fillId: `${order.orderId}-fill-${restingOrder.orderId}`,
          qty: fillQty,
          price: bidPrice,
          makerOrderId: restingOrder.orderId,
          takerOrderId: order.orderId,
          makerUserId: restingOrder.userId,
          takerUserId: order.userId,
          makerSide: "buy",
          createdAt: parseInt(`${streamMsgId.split('-')[0]}`)
        })

        //update lastTradedPrice
        orderbook.lastTradedPrice = bidPrice;

        restingOrder.filledQty += fillQty;
        remainingQty -= fillQty;
        totalCost += fillQty * bidPrice;

        if (restingOrder.filledQty === restingOrder.qty) restingOrder.status = "filled";
        else if (restingOrder.filledQty === 0) restingOrder.status = "open";
        else restingOrder.status = "partially_filled";
      }
      const remainingRestingOrders = restingOrders.filter((order) => order.filledQty < order.qty);
      if (remainingRestingOrders.length === 0) {
        orderbook.bids.delete(bidPrice);
      } else {
        orderbook.bids.set(bidPrice, remainingRestingOrders);
      }
    }
  }
  return { fills, remainingQty, totalCost, touchedAskPrices, touchedBidPrices };
}
