import type { Kline, StrategyDirection } from "./types.js";

export type TrendBias = "bullish" | "bearish" | "neutral";

export interface MacdOptions {
  fast: number;
  slow: number;
  signal: number;
  confirmBars?: number;
  minHistBps?: number;
  minSlopeBps?: number;
}

export interface MacdSnapshot {
  ready: boolean;
  trend: TrendBias;
  line: number | null;
  signal: number | null;
  histogram: number | null;
  histogramBps: number | null;
  histogramSlopeBps: number | null;
  priceSlopeBps: number | null;
  bars: number;
  reason: string;
}

const DEFAULT_CONFIRM_BARS = 3;
const DEFAULT_MIN_HIST_BPS = 0.02;
const DEFAULT_MIN_SLOPE_BPS = 0;

function finitePrice(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function getCloses(klines: readonly Kline[]): number[] {
  return klines
    .map((k) => finitePrice(k.close))
    .filter((value): value is number => value != null);
}

function emaSeries(values: readonly number[], period: number): Array<number | null> {
  const result: Array<number | null> = new Array(values.length).fill(null);
  if (period <= 0 || values.length < period) return result;

  const multiplier = 2 / (period + 1);
  let sum = 0;
  let ema: number | null = null;

  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    if (!Number.isFinite(value)) continue;

    if (ema == null) {
      sum += value;
      if (i === period - 1) {
        ema = sum / period;
        result[i] = ema;
      }
      continue;
    }

    ema = value * multiplier + ema * (1 - multiplier);
    result[i] = ema;
  }

  return result;
}

function signalSeries(macd: Array<number | null>, period: number): Array<number | null> {
  const result: Array<number | null> = new Array(macd.length).fill(null);
  if (period <= 0) return result;

  const multiplier = 2 / (period + 1);
  const seed: number[] = [];
  let ema: number | null = null;

  for (let i = 0; i < macd.length; i++) {
    const value = macd[i];
    if (value == null || !Number.isFinite(value)) continue;

    if (ema == null) {
      seed.push(value);
      if (seed.length === period) {
        ema = seed.reduce((sum, item) => sum + item, 0) / period;
        result[i] = ema;
      }
      continue;
    }

    ema = value * multiplier + ema * (1 - multiplier);
    result[i] = ema;
  }

  return result;
}

function lastReadyIndex(values: Array<number | null>): number {
  for (let i = values.length - 1; i >= 0; i--) {
    if (values[i] != null && Number.isFinite(values[i])) return i;
  }
  return -1;
}

function countFinite(values: Array<number | null>): number {
  return values.reduce((count, value) => count + (value != null && Number.isFinite(value) ? 1 : 0), 0);
}

export function buildMacdSnapshot(
  klines: readonly Kline[],
  options: MacdOptions = { fast: 12, slow: 26, signal: 9 },
): MacdSnapshot {
  const closes = getCloses(klines);
  const confirmBars = Math.max(2, Math.round(options.confirmBars ?? DEFAULT_CONFIRM_BARS));
  const minHistBps = Math.max(0, options.minHistBps ?? DEFAULT_MIN_HIST_BPS);
  const minSlopeBps = Math.max(0, options.minSlopeBps ?? DEFAULT_MIN_SLOPE_BPS);
  const barsNeeded = Math.max(options.fast, options.slow) + options.signal + confirmBars;

  if (closes.length < barsNeeded) {
    return {
      ready: false,
      trend: "neutral",
      line: null,
      signal: null,
      histogram: null,
      histogramBps: null,
      histogramSlopeBps: null,
      priceSlopeBps: null,
      bars: closes.length,
      reason: `样本不足 ${closes.length}/${barsNeeded}`,
    };
  }

  const fast = emaSeries(closes, options.fast);
  const slow = emaSeries(closes, options.slow);
  const macdLine = closes.map((_, i) => {
    const f = fast[i];
    const s = slow[i];
    return f == null || s == null ? null : f - s;
  });
  const signal = signalSeries(macdLine, options.signal);
  const histogram = macdLine.map((line, i) => {
    const sig = signal[i];
    return line == null || sig == null ? null : line - sig;
  });

  const idx = lastReadyIndex(histogram);
  if (idx < confirmBars - 1) {
    return {
      ready: false,
      trend: "neutral",
      line: null,
      signal: null,
      histogram: null,
      histogramBps: null,
      histogramSlopeBps: null,
      priceSlopeBps: null,
      bars: countFinite(histogram),
      reason: "MACD 尚未成形",
    };
  }

  const recent = histogram.slice(Math.max(0, idx - confirmBars + 1), idx + 1);
  if (recent.length < confirmBars || recent.some((value) => value == null)) {
    return {
      ready: false,
      trend: "neutral",
      line: null,
      signal: null,
      histogram: null,
      histogramBps: null,
      histogramSlopeBps: null,
      priceSlopeBps: null,
      bars: countFinite(histogram),
      reason: "确认柱不足",
    };
  }

  const price = closes[idx];
  const firstHist = recent[0] ?? 0;
  const lastHist = histogram[idx] ?? 0;
  const line = macdLine[idx] ?? null;
  const sig = signal[idx] ?? null;
  const histBps = (lastHist / price) * 10000;
  const histSlopeBps = ((lastHist - firstHist) / price) * 10000;
  const prevPrice = closes[Math.max(0, idx - confirmBars + 1)];
  const priceSlopeBps = ((price - prevPrice) / price) * 10000;

  let trend: TrendBias = "neutral";
  let reason = "MACD 中性";
  if (
    line != null &&
    sig != null &&
    line > sig &&
    histBps >= minHistBps &&
    histSlopeBps >= minSlopeBps &&
    priceSlopeBps >= 0
  ) {
    trend = "bullish";
    reason = `MACD 上行 hist=${histBps.toFixed(3)}bps slope=${histSlopeBps.toFixed(3)}bps`;
  } else if (
    line != null &&
    sig != null &&
    line < sig &&
    histBps <= -minHistBps &&
    histSlopeBps <= -minSlopeBps &&
    priceSlopeBps <= 0
  ) {
    trend = "bearish";
    reason = `MACD 下行 hist=${histBps.toFixed(3)}bps slope=${histSlopeBps.toFixed(3)}bps`;
  }

  return {
    ready: true,
    trend,
    line,
    signal: sig,
    histogram: lastHist,
    histogramBps: histBps,
    histogramSlopeBps: histSlopeBps,
    priceSlopeBps,
    bars: countFinite(histogram),
    reason,
  };
}

export function isDirectionAgainstTrend(
  direction: StrategyDirection,
  trend: TrendBias,
): boolean {
  return (direction === "down" && trend === "bullish") || (direction === "up" && trend === "bearish");
}
