/**
 * 策略6 · 动量扫尾 — 波动率自适应 + 最后10秒直接扫尾
 *
 * 设计目标：
 * - 用最近 1m K 线振幅估计当前波动率
 * - 波动越大，入场 diff 阈值越高、允许入场的时间窗口越短
 * - 最后 10 秒若 diff 超过“近似 99% 的 10 秒波动阈值”，直接吃单并持有到结算
 */

import type {
  IStrategy,
  StrategyKey,
  StrategyNumber,
  StrategyDirection,
  StrategyTickContext,
  EntrySignal,
  ExitSignal,
  StrategyDescription,
  Kline,
} from "./types.js";

const BASE_WINDOW_MAX_REMAINING = 60;
const BASE_ENTRY_DIFF = 50;
const BASE_ENTRY_PROB_CAP = 94;
const DIRECT_SWEEP_REMAINING = 10;
const VOL_LOOKBACK = 6;
const MAX_SPREAD_PCT = 0.035;
const MAX_FINAL_SPREAD_PCT = 0.05;
const MIN_TREND_CONFIRM_DIFF = 8;

interface DynamicParams {
  refPrice: number | null;
  volatilityBps: number;
  adaptiveWindowMax: number;
  adaptiveEntryDiff: number;
  adaptiveProbCap: number;
  finalSweepDiff: number;
  stopLossDiff: number;
}

interface S6State {
  volatilityBps: number;
  adaptiveWindowMax: number;
  adaptiveEntryDiff: number;
  adaptiveProbCap: number;
  finalSweepDiff: number;
  stopLossDiff: number;
  spreadPct: number | null;
  trend1mDiff: number;
  entryBlockedReason: string;
  holdToSettlement: boolean;
  pendingHoldToSettlement: boolean;
  entryMode: "idle" | "adaptive" | "settle";
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function median(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1
    ? sorted[mid]
    : (sorted[mid - 1] + sorted[mid]) / 2;
}

function createState(): S6State {
  return {
    volatilityBps: 12,
    adaptiveWindowMax: BASE_WINDOW_MAX_REMAINING,
    adaptiveEntryDiff: BASE_ENTRY_DIFF,
    adaptiveProbCap: BASE_ENTRY_PROB_CAP,
    finalSweepDiff: 55,
    stopLossDiff: 8,
    spreadPct: null,
    trend1mDiff: 0,
    entryBlockedReason: "",
    holdToSettlement: false,
    pendingHoldToSettlement: false,
    entryMode: "idle",
  };
}

function extractRecentClosed1m(klines: readonly Kline[]): Kline[] {
  return klines.filter((k) => k.closed).slice(-VOL_LOOKBACK);
}

function getRefPrice(klines: readonly Kline[]): number | null {
  const recent = klines[klines.length - 1];
  if (!recent) return null;
  const candidates = [recent.close, recent.open, recent.high, recent.low];
  for (const value of candidates) {
    if (Number.isFinite(value) && value > 0) return value;
  }
  return null;
}

function getRecentVolatilityBps(klines: readonly Kline[]): number {
  const recent = extractRecentClosed1m(klines);
  const ranges = recent
    .map((k) => {
      if (
        !Number.isFinite(k.open) ||
        !Number.isFinite(k.high) ||
        !Number.isFinite(k.low) ||
        k.open <= 0
      ) {
        return null;
      }
      return ((k.high - k.low) / k.open) * 10000;
    })
    .filter((v): v is number => v != null && Number.isFinite(v) && v > 0);
  const med = median(ranges);
  return clamp(med ?? 12, 8, 40);
}

function buildDynamicParams(ctx: StrategyTickContext): DynamicParams {
  const refPrice = getRefPrice(ctx.kline1m);
  const volatilityBps = getRecentVolatilityBps(ctx.kline1m);
  const price = refPrice ?? 100000;

  // 波动越大，允许进场的时间越短，避免尾盘乱跳时过早追单。
  const adaptiveWindowMax = Math.round(
    clamp(
      BASE_WINDOW_MAX_REMAINING - Math.max(0, volatilityBps - 12) * 1.5,
      24,
      60,
    ),
  );

  // 基础 diff=50，在高波动下按价格百分比增量抬高阈值。
  const adaptiveEntryDiff = Math.round(
    clamp(
      BASE_ENTRY_DIFF + price * Math.max(0, volatilityBps - 10) * 0.0001 * 0.22,
      50,
      140,
    ),
  );

  // 波动越大，对概率追高越保守。
  const adaptiveProbCap = Math.round(
    clamp(
      BASE_ENTRY_PROB_CAP - Math.floor(Math.max(0, volatilityBps - 12) / 6),
      88,
      BASE_ENTRY_PROB_CAP,
    ),
  );

  // 近似“未来10秒 99% 不会波动到”的阈值：
  // 用近 6 根 1m 中位振幅的 50% 换算到 USD，并设置 55 美元地板。
  const finalSweepDiff = Math.round(
    clamp(price * Math.max(volatilityBps, 11) * 0.0001 * 0.5, 55, 180),
  );

  const stopLossDiff = Math.round(clamp(adaptiveEntryDiff * 0.18, 8, 22));

  return {
    refPrice,
    volatilityBps,
    adaptiveWindowMax,
    adaptiveEntryDiff,
    adaptiveProbCap,
    finalSweepDiff,
    stopLossDiff,
  };
}

function getSpreadPct(ctx: StrategyTickContext): number | null {
  const { bestBid, bestAsk } = ctx;
  if (
    bestBid == null ||
    bestAsk == null ||
    bestBid <= 0 ||
    bestAsk <= 0 ||
    bestAsk < bestBid
  ) {
    return null;
  }
  const mid = (bestBid + bestAsk) / 2;
  if (!(mid > 0)) return null;
  return (bestAsk - bestBid) / mid;
}

function getShortTrendDiff(klines: readonly Kline[]): number {
  const recent = klines.filter((k) => k.closed).slice(-2);
  if (recent.length < 2) return 0;
  const [prev, last] = recent;
  if (!Number.isFinite(prev.close) || !Number.isFinite(last.close)) return 0;
  return last.close - prev.close;
}

export class S6MomentumSweep implements IStrategy {
  readonly key: StrategyKey = "s6";
  readonly number: StrategyNumber = 6;
  readonly name = "动量扫尾";

  private s: S6State = createState();

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略6 · 动量扫尾",
      lines: [
        { text: "📊 最近6根1m中位振幅 => 动态波动率，波动越大阈值越高" },
        { text: "⏱ 波动越大，允许入场的时间窗口越短（约 60s 收缩到 24s）" },
        {
          text: "📈 常规扫尾：diff 超过动态阈值，且概率未追高 + 1m短趋势同向时入场",
          marginTop: true,
        },
        {
          text: `📚 盘口过滤：常规点差>${Math.round(MAX_SPREAD_PCT * 100)}%不做，最后${DIRECT_SWEEP_REMAINING}s 直扫点差>${Math.round(MAX_FINAL_SPREAD_PCT * 100)}%也不做`,
        },
        {
          text: `⚡ 最后${DIRECT_SWEEP_REMAINING}s：diff 超过动态极端阈值且盘口可买时直接扫尾，随后持有到结算`,
          color: "#3fb950",
        },
        {
          text: "止盈：>40s ≥97% / >20s ≥98% / >10s ≥99% / <10s 持仓到结束",
          color: "#3fb950",
        },
        {
          text: "止损：常规扫尾仓在 diff 回撤到动态止损阈值时退出",
          color: "#f85149",
        },
      ],
    };
  }

  updateGuards(_ctx: StrategyTickContext): void {}

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    if (rem <= 0) return null;

    const params = buildDynamicParams(ctx);
    this.s.volatilityBps = params.volatilityBps;
    this.s.adaptiveWindowMax = params.adaptiveWindowMax;
    this.s.adaptiveEntryDiff = params.adaptiveEntryDiff;
    this.s.adaptiveProbCap = params.adaptiveProbCap;
    this.s.finalSweepDiff = params.finalSweepDiff;
    this.s.stopLossDiff = params.stopLossDiff;
    this.s.spreadPct = getSpreadPct(ctx);
    this.s.trend1mDiff = getShortTrendDiff(ctx.kline1m);
    this.s.entryBlockedReason = "";
    this.s.pendingHoldToSettlement = false;
    this.s.holdToSettlement = false;

    if (this.s.spreadPct == null) {
      this.s.entryBlockedReason = "盘口缺失";
      return null;
    }

    if (rem <= DIRECT_SWEEP_REMAINING) {
      if (this.s.spreadPct > MAX_FINAL_SPREAD_PCT) {
        this.s.entryBlockedReason = `末段点差过大 ${(this.s.spreadPct * 100).toFixed(1)}%`;
        return null;
      }
      if (diff >= params.finalSweepDiff) {
        this.s.pendingHoldToSettlement = true;
        this.s.entryMode = "settle";
        return { direction: "up" };
      }
      if (diff <= -params.finalSweepDiff) {
        this.s.pendingHoldToSettlement = true;
        this.s.entryMode = "settle";
        return { direction: "down" };
      }
      return null;
    }

    if (rem > params.adaptiveWindowMax) return null;
    if (this.s.spreadPct > MAX_SPREAD_PCT) {
      this.s.entryBlockedReason = `点差过大 ${(this.s.spreadPct * 100).toFixed(1)}%`;
      return null;
    }

    if (
      diff >= params.adaptiveEntryDiff &&
      upPct < params.adaptiveProbCap &&
      this.s.trend1mDiff >= MIN_TREND_CONFIRM_DIFF
    ) {
      this.s.entryMode = "adaptive";
      return { direction: "up" };
    }
    if (
      diff <= -params.adaptiveEntryDiff &&
      dnPct < params.adaptiveProbCap &&
      this.s.trend1mDiff <= -MIN_TREND_CONFIRM_DIFF
    ) {
      this.s.entryMode = "adaptive";
      return { direction: "down" };
    }
    if (Math.abs(diff) >= params.adaptiveEntryDiff) {
      this.s.entryBlockedReason = "短时趋势未确认";
    }
    return null;
  }

  onEntryFilled(
    _ctx: StrategyTickContext,
    _direction: StrategyDirection,
  ): void {
    this.s.holdToSettlement = this.s.pendingHoldToSettlement;
  }

  checkExit(
    ctx: StrategyTickContext,
    direction: StrategyDirection,
  ): ExitSignal {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;

    if (this.s.holdToSettlement) return null;

    const myPct = direction === "up" ? upPct : dnPct;
    if (rem >= 40 && myPct >= 97) {
      return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥97% rem=${rem}s` };
    }
    if (rem >= 20 && rem < 40 && myPct >= 98) {
      return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥98% rem=${rem}s` };
    }
    if (rem >= 10 && rem < 20 && myPct >= 99) {
      return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥99% rem=${rem}s` };
    }

    if (direction === "up" && diff <= this.s.stopLossDiff) {
      return {
        signal: "sl",
        reason: `动态止损 diff=${Math.round(diff)}≤${this.s.stopLossDiff}`,
      };
    }
    if (direction === "down" && diff >= -this.s.stopLossDiff) {
      return {
        signal: "sl",
        reason: `动态止损 diff=${Math.round(diff)}≥${-this.s.stopLossDiff}`,
      };
    }
    return null;
  }

  resetState(): void {
    this.s = createState();
  }

  getStatePayload(): Record<string, unknown> {
    return {
      volatilityBps: this.s.volatilityBps,
      adaptiveWindowMax: this.s.adaptiveWindowMax,
      adaptiveEntryDiff: this.s.adaptiveEntryDiff,
      adaptiveProbCap: this.s.adaptiveProbCap,
      finalSweepDiff: this.s.finalSweepDiff,
      stopLossDiff: this.s.stopLossDiff,
      spreadPct: this.s.spreadPct,
      trend1mDiff: this.s.trend1mDiff,
      entryBlockedReason: this.s.entryBlockedReason,
      holdToSettlement: this.s.holdToSettlement,
      entryMode: this.s.entryMode,
    };
  }
}
