/**
 * 策略7 · 动量预设 — 基于 S6 的三档预设自动切换
 *
 * 数据依据：
 * - 60 天 BTCUSDT 1m：常规扫尾 diff / 1m 趋势确认分层
 * - 7 天 BTCUSDT 1s：最后 10 秒直扫阈值分层
 *
 * 预设：
 * - aggressive: diff=50 / trend=10 / final10=40
 * - balanced:   diff=80 / trend=20 / final10=60
 * - conservative: diff=130 / trend=40 / final10=80
 *
 * 波动分层：
 * - vol <= p75(8.4bps)    => aggressive
 * - p75 < vol <= p90(12.18bps) => balanced
 * - vol > p90             => conservative
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

const BASE_ENTRY_PROB_CAP = 94;
const DIRECT_SWEEP_REMAINING = 10;
const VOL_LOOKBACK = 6;
const MAX_SPREAD_PCT = 0.035;
const MAX_FINAL_SPREAD_PCT = 0.05;

const VOL_P75_BPS = 8.4;
const VOL_P90_BPS = 12.18;

type PresetName = "aggressive" | "balanced" | "conservative";

interface PresetConfig {
  name: PresetName;
  label: string;
  baseWindowMax: number;
  baseEntryDiff: number;
  minTrendConfirmDiff: number;
  finalSweepFloor: number;
  probabilityCap: number;
}

const PRESETS: Record<PresetName, PresetConfig> = {
  aggressive: {
    name: "aggressive",
    label: "激进",
    baseWindowMax: 60,
    baseEntryDiff: 50,
    minTrendConfirmDiff: 10,
    finalSweepFloor: 40,
    probabilityCap: 94,
  },
  balanced: {
    name: "balanced",
    label: "均衡",
    baseWindowMax: 52,
    baseEntryDiff: 80,
    minTrendConfirmDiff: 20,
    finalSweepFloor: 60,
    probabilityCap: 92,
  },
  conservative: {
    name: "conservative",
    label: "保守",
    baseWindowMax: 40,
    baseEntryDiff: 130,
    minTrendConfirmDiff: 40,
    finalSweepFloor: 80,
    probabilityCap: 90,
  },
};

interface DynamicParams {
  preset: PresetConfig;
  volatilityBps: number;
  adaptiveWindowMax: number;
  adaptiveEntryDiff: number;
  adaptiveProbCap: number;
  finalSweepDiff: number;
  stopLossDiff: number;
}

interface S7State {
  preset: PresetName;
  presetLabel: string;
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

function createState(): S7State {
  return {
    preset: "balanced",
    presetLabel: PRESETS.balanced.label,
    volatilityBps: 12,
    adaptiveWindowMax: PRESETS.balanced.baseWindowMax,
    adaptiveEntryDiff: PRESETS.balanced.baseEntryDiff,
    adaptiveProbCap: PRESETS.balanced.probabilityCap,
    finalSweepDiff: PRESETS.balanced.finalSweepFloor,
    stopLossDiff: 14,
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
  return clamp(med ?? 12, 5, 40);
}

function pickPreset(volatilityBps: number): PresetConfig {
  if (volatilityBps <= VOL_P75_BPS) return PRESETS.aggressive;
  if (volatilityBps <= VOL_P90_BPS) return PRESETS.balanced;
  return PRESETS.conservative;
}

function buildDynamicParams(ctx: StrategyTickContext): DynamicParams {
  const volatilityBps = getRecentVolatilityBps(ctx.kline1m);
  const preset = pickPreset(volatilityBps);
  const refPrice = getRefPrice(ctx.kline1m);
  const price = refPrice ?? 100000;

  const adaptiveWindowMax = Math.round(
    clamp(
      preset.baseWindowMax - Math.max(0, volatilityBps - VOL_P75_BPS) * 1.2,
      20,
      preset.baseWindowMax,
    ),
  );

  const adaptiveEntryDiff = Math.round(
    clamp(
      preset.baseEntryDiff +
        price * Math.max(0, volatilityBps - 8) * 0.0001 * 0.18,
      preset.baseEntryDiff,
      preset.baseEntryDiff + 60,
    ),
  );

  const adaptiveProbCap = Math.round(
    clamp(
      preset.probabilityCap - Math.floor(Math.max(0, volatilityBps - 10) / 5),
      86,
      BASE_ENTRY_PROB_CAP,
    ),
  );

  const finalSweepDiff = Math.round(
    clamp(
      Math.max(
        preset.finalSweepFloor,
        price * Math.max(volatilityBps, 8) * 0.0001 * 0.45,
      ),
      preset.finalSweepFloor,
      220,
    ),
  );

  const stopLossDiff = Math.round(clamp(adaptiveEntryDiff * 0.18, 8, 24));

  return {
    preset,
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

export class S7PresetMomentumSweep implements IStrategy {
  readonly key: StrategyKey = "s7";
  readonly number: StrategyNumber = 7;
  readonly name = "动量预设";

  private s: S7State = createState();

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略7 · 动量预设",
      lines: [
        {
          text: "📊 基于 BTC 60天1m + 7天1s 统计，把扫尾分成 激进 / 均衡 / 保守 三档",
        },
        {
          text: `波动率分层：<=${VOL_P75_BPS}bps 用激进，<=${VOL_P90_BPS}bps 用均衡，>${VOL_P90_BPS}bps 用保守`,
        },
        { text: "📈 激进：diff50 / 趋势10 / 末10秒40", marginTop: true },
        { text: "📊 均衡：diff80 / 趋势20 / 末10秒60" },
        { text: "🛡 保守：diff130 / 趋势40 / 末10秒80" },
        {
          text: `盘口过滤：常规点差>${Math.round(MAX_SPREAD_PCT * 100)}%不做，末段直扫点差>${Math.round(MAX_FINAL_SPREAD_PCT * 100)}%不做`,
          color: "#888",
          marginTop: true,
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
    this.s.preset = params.preset.name;
    this.s.presetLabel = params.preset.label;
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

    if (rem > params.adaptiveWindowMax) {
      this.s.entryBlockedReason = `等待进入窗口 <=${params.adaptiveWindowMax}s`;
      return null;
    }
    if (this.s.spreadPct > MAX_SPREAD_PCT) {
      this.s.entryBlockedReason = `点差过大 ${(this.s.spreadPct * 100).toFixed(1)}%`;
      return null;
    }

    if (
      diff >= params.adaptiveEntryDiff &&
      upPct < params.adaptiveProbCap &&
      this.s.trend1mDiff >= params.preset.minTrendConfirmDiff
    ) {
      this.s.entryMode = "adaptive";
      return { direction: "up" };
    }
    if (
      diff <= -params.adaptiveEntryDiff &&
      dnPct < params.adaptiveProbCap &&
      this.s.trend1mDiff <= -params.preset.minTrendConfirmDiff
    ) {
      this.s.entryMode = "adaptive";
      return { direction: "down" };
    }
    if (Math.abs(diff) >= params.adaptiveEntryDiff) {
      this.s.entryBlockedReason = `短时趋势未确认(<${params.preset.minTrendConfirmDiff})`;
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
      preset: this.s.preset,
      presetLabel: this.s.presetLabel,
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
