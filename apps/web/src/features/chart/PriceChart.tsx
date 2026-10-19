import { useEffect, useRef } from "react";
import {
  CandlestickSeries,
  ColorType,
  HistogramSeries,
  LineStyle,
  TickMarkType,
  createChart,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type IPriceLine,
  type ISeriesApi,
  type Time,
  type UTCTimestamp,
  type WhitespaceData,
} from "lightweight-charts";
import type { Candle } from "../../api/types";
import { usd } from "../../lib/format";

const GREEN = "#00c896";
const RED = "#f6465d";
const GRID = "#1a1c20";
const AXIS_TEXT = "#5c6068";
const MARK_LINE_COLOR = "#3b82f6";
const INDEX_LINE_COLOR = "#a855f7";

// lightweight-charts paints to a canvas, so it can't use CSS variables directly: read the
// theme tokens from index.css (falling back to the dark values) and re-read them whenever
// <html data-theme> changes.
function themeColors() {
  const css = getComputedStyle(document.documentElement);
  return {
    grid: css.getPropertyValue("--color-border-soft").trim() || GRID,
    text: css.getPropertyValue("--color-text-dim").trim() || AXIS_TEXT,
  };
}

function toTime(bucket: string): UTCTimestamp {
  return Math.floor(new Date(bucket).getTime() / 1000) as UTCTimestamp;
}

// lightweight-charts treats `Time` as a UTC epoch internally and formats its axis/crosshair
// with UTC getters by default, regardless of the viewer's timezone. Every other timestamp in
// the app (fill history, trade blotter, position history — see lib/format.ts:formatTime)
// resolves to the browser's local timezone via Intl, so override both formatters here to match.
export function localTickMarkFormatter(time: Time, tickMarkType: TickMarkType): string {
  const date = new Date((time as number) * 1000);
  switch (tickMarkType) {
    case TickMarkType.Year:
      return date.toLocaleDateString([], { year: "numeric" });
    case TickMarkType.Month:
      return date.toLocaleDateString([], { month: "short", year: "2-digit" });
    case TickMarkType.DayOfMonth:
      return date.toLocaleDateString([], { month: "short", day: "numeric" });
    case TickMarkType.TimeWithSeconds:
      return date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit", second: "2-digit" });
    default:
      return date.toLocaleTimeString([], { hour12: false, hour: "2-digit", minute: "2-digit" });
  }
}

export function localTimeFormatter(time: Time): string {
  const date = new Date((time as number) * 1000);
  return date.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
}

function toCandlestickData(candles: Candle[]): CandlestickData[] {
  return candles.map((c) => ({
    time: toTime(c.bucket),
    open: usd(c.open),
    high: usd(c.high),
    low: usd(c.low),
    close: usd(c.close),
  }));
}

function toVolumeData(candles: Candle[]): HistogramData[] {
  return candles.map((c) => ({
    time: toTime(c.bucket),
    value: Number(c.volume),
    color: Number(c.close) >= Number(c.open) ? "rgba(0, 200, 150, 0.5)" : "rgba(246, 70, 93, 0.5)",
  }));
}

const INTERVAL_SECONDS: Record<string, number> = {
  "1m": 60,
  "5m": 300,
  "15m": 900,
  "1h": 3600,
  "4h": 14400,
  "1d": 86400,
};

// Sparse markets can have a handful of candles (or just one), which leaves the
// time scale with almost no ticks. Pad the series with whitespace buckets so the
// axis always spans a real time window and gaps between candles stay linear.
const MIN_VISIBLE_BARS = 60;

function padWithWhitespace(
  data: CandlestickData[],
  intervalSeconds: number,
): (CandlestickData | WhitespaceData)[] {
  if (data.length === 0) return data;

  const byTime = new Map<number, CandlestickData>(data.map((d) => [d.time as number, d]));
  const last = data[data.length - 1]!.time as number;
  const first = data[0]!.time as number;
  const start = Math.min(first, last - (MIN_VISIBLE_BARS - 1) * intervalSeconds);

  const out: (CandlestickData | WhitespaceData)[] = [];
  for (let t = start; t <= last; t += intervalSeconds) {
    out.push(byTime.get(t) ?? { time: t as UTCTimestamp });
  }
  // Keep any real candles that fall off the aligned grid.
  for (const d of data) {
    if ((((d.time as number) - start) % intervalSeconds) !== 0 || (d.time as number) < start) {
      out.push(d);
    }
  }
  return out.sort((a, b) => (a.time as number) - (b.time as number));
}

const RANGE_SECONDS: Record<string, number | null> = {
  All: null,
  "1y": 365 * 86400,
  "6m": 182 * 86400,
  "3m": 90 * 86400,
  "1m": 30 * 86400,
  "5d": 5 * 86400,
  "1d": 86400,
};

function bucketsMatch(a: Candle[], b: Candle[]) {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) if (a[i]!.bucket !== b[i]!.bucket) return false;
  return true;
}

// only called on initial load / explicit range change, never on a routine candle tick
function applyVisibleRange(chart: IChartApi | null, candles: Candle[], range: string) {
  if (!chart || candles.length === 0) return;

  const seconds = RANGE_SECONDS[range];
  const lastTime = toTime(candles[candles.length - 1]!.bucket);
  if (seconds == null) {
    chart.timeScale().fitContent();
    return;
  }
  // clamp to earliest candle we have, otherwise a long range on a young market looks empty
  const firstTime = toTime(candles[0]!.bucket);
  const from = Math.max(lastTime - seconds, firstTime);
  chart.timeScale().setVisibleRange({ from: from as UTCTimestamp, to: lastTime });
}

export function PriceChart({
  candles,
  showVolume,
  range,
  interval,
  priceSource = "Last",
  markPrice,
  indexPrice,
}: {
  candles: Candle[];
  showVolume: boolean;
  range: string;
  interval: string;
  priceSource?: "Last" | "Mark" | "Index";
  markPrice?: number;
  indexPrice?: number;
}) {
  const containerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candleSeriesRef = useRef<ISeriesApi<"Candlestick"> | null>(null);
  const volumeSeriesRef = useRef<ISeriesApi<"Histogram"> | null>(null);
  const candlesRef = useRef(candles);
  candlesRef.current = candles;
  const prevCandlesRef = useRef<Candle[]>([]);
  const prevShowVolumeRef = useRef(showVolume);
  const dataReadyRef = useRef(false);
  const priceLineRef = useRef<IPriceLine | null>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;

    const colors = themeColors();
    const chart = createChart(el, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: colors.text,
        fontFamily: "Inter, sans-serif",
        fontSize: 11,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      rightPriceScale: { borderColor: colors.grid },
      timeScale: {
        borderColor: colors.grid,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 5,
        tickMarkFormatter: localTickMarkFormatter,
      },
      localization: { timeFormatter: localTimeFormatter },
      crosshair: { mode: 0 },
      autoSize: true,
    });

    const candleSeries = chart.addSeries(CandlestickSeries, {
      upColor: GREEN,
      downColor: RED,
      borderVisible: false,
      wickUpColor: GREEN,
      wickDownColor: RED,
    });

    const volumeSeries = chart.addSeries(HistogramSeries, {
      priceFormat: { type: "volume" },
      priceScaleId: "volume",
    });
    volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.82, bottom: 0 } });
    candleSeries.priceScale().applyOptions({ scaleMargins: { top: 0.08, bottom: 0.24 } });

    chartRef.current = chart;
    candleSeriesRef.current = candleSeries;
    volumeSeriesRef.current = volumeSeries;

    const themeObserver = new MutationObserver(() => {
      const { grid, text } = themeColors();
      chart.applyOptions({
        layout: { textColor: text },
        grid: { vertLines: { color: grid }, horzLines: { color: grid } },
        rightPriceScale: { borderColor: grid },
        timeScale: { borderColor: grid },
      });
    });
    themeObserver.observe(document.documentElement, { attributes: true, attributeFilter: ["data-theme"] });

    return () => {
      themeObserver.disconnect();
      chart.remove();
      chartRef.current = null;
      candleSeriesRef.current = null;
      volumeSeriesRef.current = null;
      priceLineRef.current = null;
    };
  }, []);

  useEffect(() => {
    const prev = prevCandlesRef.current;
    const volumeToggled = prevShowVolumeRef.current !== showVolume;
    prevShowVolumeRef.current = showVolume;

    // repeatedly calling setData() (a full series replace) on every routine trade tick
    // makes lightweight-charts recompute its viewport from scratch each time, which is
    // what caused the whole visible range to visibly drift/shift on live updates. Use
    // the incremental update() API for the two routine cases — the last bucket's OHLCV
    // changed, or exactly one new bucket appended after unchanged history — and reserve
    // setData() for genuine structural changes (initial load, symbol/interval switch, or
    // a visibility-refetch that can inject different/earlier history).
    const sameBucketUpdated =
      !volumeToggled &&
      candles.length > 0 &&
      candles.length === prev.length &&
      bucketsMatch(candles.slice(0, -1), prev.slice(0, -1)) &&
      candles[candles.length - 1]!.bucket === prev[prev.length - 1]?.bucket;
    const newBucketAppended =
      !volumeToggled &&
      prev.length > 0 &&
      candles.length === prev.length + 1 &&
      bucketsMatch(candles.slice(0, -1), prev);

    if (sameBucketUpdated || newBucketAppended) {
      const bar = candles[candles.length - 1]!;
      candleSeriesRef.current?.update(toCandlestickData([bar])[0]!);
      if (showVolume) volumeSeriesRef.current?.update(toVolumeData([bar])[0]!);
    } else {
      const intervalSeconds = INTERVAL_SECONDS[interval] ?? 60;
      candleSeriesRef.current?.setData(padWithWhitespace(toCandlestickData(candles), intervalSeconds));
      volumeSeriesRef.current?.setData(showVolume ? toVolumeData(candles) : []);
    }
    prevCandlesRef.current = candles;

    if (candles.length === 0) {
      // symbol switched (or still loading) — re-fit once real data lands again
      dataReadyRef.current = false;
    } else if (!dataReadyRef.current) {
      dataReadyRef.current = true;
      applyVisibleRange(chartRef.current, candles, range);
    }
  }, [candles, showVolume, interval]);

  useEffect(() => {
    if (!dataReadyRef.current) return;
    applyVisibleRange(chartRef.current, candlesRef.current, range);
  }, [range, interval]);

  // no historical mark/index series available, so overlay a live reference price line instead
  useEffect(() => {
    const series = candleSeriesRef.current;
    if (!series) return;

    const cents = priceSource === "Mark" ? markPrice : priceSource === "Index" ? indexPrice : undefined;

    if (cents === undefined) {
      if (priceLineRef.current) {
        series.removePriceLine(priceLineRef.current);
        priceLineRef.current = null;
      }
      return;
    }

    const price = usd(cents);
    if (priceLineRef.current) {
      priceLineRef.current.applyOptions({ price, title: priceSource });
    } else {
      priceLineRef.current = series.createPriceLine({
        price,
        color: priceSource === "Mark" ? MARK_LINE_COLOR : INDEX_LINE_COLOR,
        lineWidth: 1,
        lineStyle: LineStyle.Dashed,
        axisLabelVisible: true,
        title: priceSource,
      });
    }
  }, [priceSource, markPrice, indexPrice]);

  return <div ref={containerRef} className="absolute inset-0" />;
}
