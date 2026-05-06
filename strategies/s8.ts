/**
 * 策略8 · 稳健套利 — 合理概率折价 + 盘口成本过滤 + 连续确认
 *
 * 说明：
 * - 这是基于 fair-prob 的统计套利，不是无风险双边对冲套利。
 * - 入场只在“合理概率 - 预估买入成本”足够厚时触发。
 * - 为了降低噪声，额外要求盘口点差、短趋势、概率动量和信号持续时间同时通过。
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
import { getFairProb } from "./fair-prob.js";

const WINDOW_MAX_REMAINING = 210;
const WINDOW_MIN_REMAINING = 35;
const MIN_DIRECTIONAL_DIFF = 20;
const VOL_LOOKBACK = 8;

const BASE_MIN_EDGE_PCT = 9;
const MAX_MIN_EDGE_PCT = 18;
const MAX_SPREAD_PCT = 0.03;
const MIN_ENTRY_COST_PCT = 8;
const MAX_ENTRY_COST_PCT = 90;
const MIN_FAIR_PROB_PCT = 25;

const CONFIRM_TICKS = 2;
const CONFIRM_MS = 1600;
const MAX_SIGNAL_GAP_MS = 2800;
const POST_SIGNAL_COOLDOWN_MS = 15000;
const MAX_NEGATIVE_PROB_MOMENTUM = -3;

const MIN_HOLD_MS = 2500;
const EXIT_EDGE_PCT = 2.5;
const TAKE_PROFIT_SELL_GAIN_PCT = 5;
const MIN_PROFIT_LOCK_PCT = 1;
const STOP_LOSS_SELL_DROP_PCT = 5;
const TRAILING_PROB_RETRACE = 6;
const FAIR_BREAKDOWN_BUFFER_PCT = 3;
const EXIT_REVERSAL_DIFF = 5;
const MAX_HOLD_SECONDS = 45;
const FORCE_EXIT_REM = 12;

interface QuoteProxy {
  buyCostPct: number;
  sellValuePct: number;
}

interface DynamicParams {
  volatilityBps: number;
  trendDiff: number;
  trendReady: boolean;
  trendRequirement: number;
  spreadPct: number | null;
  minEdgePct: number;
}

interface Opportunity {
  direction: StrategyDirection;
  fairPct: number;
  buyCostPct: number;
  sellValuePct: number;
  edgePct: number;
  marketPct: number;
  probDeltaPct: number;
}

interface CandidateState extends Opportunity {
  firstSeenTs: number;
  lastSeenTs: number;
  ticks: number;
}

interface S8State {
  volatilityBps: number;
  trendDiff: number;
  trendReady: boolean;
  trendRequirement: number;
  spreadPct: number | null;
  minEdgePct: number;
  fairPct: number | null;
  costPct: number | null;
  sellValuePct: number | null;
  edgePct: number | null;
  marketPct: number | null;
  probDeltaPct: number;
  bestDirection: StrategyDirection | "none";
  candidateDirection: StrategyDirection | "none";
  candidateTicks: number;
  candidateConfirmMs: number;
  entryBlockedReason: string;
  cooldownUntil: number;
  entryTs: number;
  entryCostPct: number | null;
  entryEdgePct: number | null;
  peakMarketPct: number | null;
}

function createState(): S8State {
  return {
    volatilityBps: 12,
    trendDiff: 0,
    trendReady: false,
    trendRequirement: 14,
    spreadPct: null,
    minEdgePct: BASE_MIN_EDGE_PCT,
    fairPct: null,
    costPct: null,
    sellValuePct: null,
    edgePct: null,
    marketPct: null,
    probDeltaPct: 0,
    bestDirection: "none",
    candidateDirection: "none",
    candidateTicks: 0,
    candidateConfirmMs: 0,
    entryBlockedReason: "",
    cooldownUntil: 0,
    entryTs: 0,
    entryCostPct: null,
    entryEdgePct: null,
    peakMarketPct: null,
  };
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

function getRecentClosed(klines: readonly Kline[], count: number): Kline[] {
  return klines.filter((k) => k.closed).slice(-count);
}

function getRecentVolatilityBps(klines: readonly Kline[]): number {
  const ranges = getRecentClosed(klines, VOL_LOOKBACK)
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
  return clamp(median(ranges) ?? 12, 5, 45);
}

function getTrendDiff(klines: readonly Kline[]): { value: number; ready: boolean } {
  const recent = getRecentClosed(klines, 4);
  if (recent.length < 4) return { value: 0, ready: false };
  const first = recent[0];
  const last = recent[recent.length - 1];
  if (!Number.isFinite(first.close) || !Number.isFinite(last.close)) {
    return { value: 0, ready: false };
  }
  return { value: last.close - first.close, ready: true };
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

function getQuoteProxy(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
): QuoteProxy | null {
  const { bestBid, bestAsk } = ctx;
  if (bestBid == null || bestAsk == null || bestBid <= 0 || bestAsk <= 0) {
    return null;
  }
  if (direction === "up") {
    return {
      buyCostPct: clamp(bestAsk * 100, 0, 100),
      sellValuePct: clamp(bestBid * 100, 0, 100),
    };
  }
  return {
    buyCostPct: clamp((1 - bestBid) * 100, 0, 100),
    sellValuePct: clamp((1 - bestAsk) * 100, 0, 100),
  };
}

function getMarketPct(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
): number | null {
  return direction === "up" ? ctx.upPct : ctx.dnPct;
}

function getProbabilityDelta(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
): number {
  if (ctx.prevUpPct == null || ctx.upPct == null) return 0;
  const upDelta = ctx.upPct - ctx.prevUpPct;
  return direction === "up" ? upDelta : -upDelta;
}

function buildDynamicParams(ctx: StrategyTickContext): DynamicParams {
  const volatilityBps = getRecentVolatilityBps(ctx.kline1m);
  const trend = getTrendDiff(ctx.kline1m);
  const spreadPct = getSpreadPct(ctx);
  const spreadPenalty = spreadPct == null ? 3 : spreadPct * 100 * 0.85;
  const volatilityPenalty = Math.max(0, volatilityBps - 10) * 0.45;
  const minEdgePct = clamp(
    BASE_MIN_EDGE_PCT + spreadPenalty + volatilityPenalty,
    BASE_MIN_EDGE_PCT,
    MAX_MIN_EDGE_PCT,
  );
  const trendRequirement = Math.round(
    clamp(12 + Math.max(0, volatilityBps - 10) * 1.2, 12, 36),
  );

  return {
    volatilityBps,
    trendDiff: trend.value,
    trendReady: trend.ready,
    trendRequirement,
    spreadPct,
    minEdgePct,
  };
}

function createOpportunity(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  fairUp: number,
): Opportunity | null {
  const quote = getQuoteProxy(ctx, direction);
  const marketPct = getMarketPct(ctx, direction);
  if (!quote || marketPct == null) return null;
  const fairPct = direction === "up" ? fairUp : 100 - fairUp;
  return {
    direction,
    fairPct,
    buyCostPct: quote.buyCostPct,
    sellValuePct: quote.sellValuePct,
    edgePct: fairPct - quote.buyCostPct,
    marketPct,
    probDeltaPct: getProbabilityDelta(ctx, direction),
  };
}

function isDirectionConfirmed(
  ctx: StrategyTickContext,
  params: DynamicParams,
  direction: StrategyDirection,
): boolean {
  if (ctx.diff == null || !params.trendReady) return false;
  if (direction === "up") {
    return (
      ctx.diff >= MIN_DIRECTIONAL_DIFF &&
      params.trendDiff >= params.trendRequirement
    );
  }
  return (
    ctx.diff <= -MIN_DIRECTIONAL_DIFF &&
    params.trendDiff <= -params.trendRequirement
  );
}

export class S8SteadyArb implements IStrategy {
  readonly key: StrategyKey = "s8";
  readonly number: StrategyNumber = 8;
  readonly name = "稳健套利";

  private s: S8State = createState();
  private candidate: CandidateState | null = null;

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略8 · 稳健套利",
      lines: [
        {
          text: "用 fair-prob 合理概率减去预估买入成本，只做折价足够厚的一侧",
        },
        {
          text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~${WINDOW_MIN_REMAINING}s 检测，避开开局和最后结算噪声`,
        },
        {
          text: `入场边际：基础 ≥${BASE_MIN_EDGE_PCT}%；波动和点差越大，要求自动提高到最高 ${MAX_MIN_EDGE_PCT}%`,
        },
        {
          text: `过滤：点差≤${(MAX_SPREAD_PCT * 100).toFixed(1)}%、买入成本 ${MIN_ENTRY_COST_PCT}%~${MAX_ENTRY_COST_PCT}%、1m短趋势同向、概率不能快速反向`,
          marginTop: true,
        },
        {
          text: `连续确认：同向信号至少 ${CONFIRM_TICKS} tick 且持续 ${(CONFIRM_MS / 1000).toFixed(1)}s 才入场`,
          color: "#58a6ff",
        },
        {
          text: "止盈：边际收敛 / 卖价盈利 / 概率峰值回撤锁利",
          color: "#3fb950",
          marginTop: true,
        },
        {
          text: `止损：卖价回撤 ${STOP_LOSS_SELL_DROP_PCT}% / fair 失效 / diff 反穿 / ${MAX_HOLD_SECONDS}s 超时 / 最后${FORCE_EXIT_REM}s强平`,
          color: "#f85149",
        },
        {
          text: "这是统计套利，不是无风险双边套利；建议小金额回测后再调参",
          color: "#888",
          marginTop: true,
        },
      ],
    };
  }

  updateGuards(_ctx: StrategyTickContext): void {}

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    const { rem, upPct, dnPct, diff, now } = ctx;
    this.s.entryBlockedReason = "";
    this.s.bestDirection = "none";
    this.s.fairPct = null;
    this.s.costPct = null;
    this.s.sellValuePct = null;
    this.s.edgePct = null;
    this.s.marketPct = null;
    this.s.probDeltaPct = 0;

    if (upPct == null || dnPct == null || diff == null) {
      this.resetCandidate("等待概率/差价");
      return null;
    }

    const params = buildDynamicParams(ctx);
    this.s.volatilityBps = params.volatilityBps;
    this.s.trendDiff = params.trendDiff;
    this.s.trendReady = params.trendReady;
    this.s.trendRequirement = params.trendRequirement;
    this.s.spreadPct = params.spreadPct;
    this.s.minEdgePct = params.minEdgePct;

    if (now < this.s.cooldownUntil) {
      this.resetCandidate("信号冷却中");
      this.s.entryBlockedReason = `冷却中 ${Math.ceil((this.s.cooldownUntil - now) / 1000)}s`;
      return null;
    }

    if (rem > WINDOW_MAX_REMAINING || rem <= WINDOW_MIN_REMAINING) {
      this.resetCandidate("不在窗口");
      this.s.entryBlockedReason =
        rem > WINDOW_MAX_REMAINING
          ? `等待进入窗口 <=${WINDOW_MAX_REMAINING}s`
          : `临近结算 <${WINDOW_MIN_REMAINING}s`;
      return null;
    }

    if (params.spreadPct == null) {
      this.resetCandidate("盘口缺失");
      this.s.entryBlockedReason = "盘口缺失";
      return null;
    }
    if (params.spreadPct > MAX_SPREAD_PCT) {
      this.resetCandidate("点差过大");
      this.s.entryBlockedReason = `点差过大 ${(params.spreadPct * 100).toFixed(1)}%`;
      return null;
    }
    if (!params.trendReady) {
      this.resetCandidate("趋势样本不足");
      this.s.entryBlockedReason = "1m趋势样本不足";
      return null;
    }

    const fairUp = getFairProb(diff, rem);
    if (fairUp == null) {
      this.resetCandidate("合理概率缺失");
      this.s.entryBlockedReason = "合理概率缺失";
      return null;
    }

    const opportunities = (["up", "down"] as const)
      .map((direction) => createOpportunity(ctx, direction, fairUp))
      .filter((o): o is Opportunity => o != null)
      .filter((o) => {
        if (o.fairPct < MIN_FAIR_PROB_PCT) return false;
        if (
          o.buyCostPct < MIN_ENTRY_COST_PCT ||
          o.buyCostPct > MAX_ENTRY_COST_PCT
        ) {
          return false;
        }
        if (o.edgePct < params.minEdgePct) return false;
        if (o.probDeltaPct < MAX_NEGATIVE_PROB_MOMENTUM) return false;
        return isDirectionConfirmed(ctx, params, o.direction);
      })
      .sort((a, b) => b.edgePct - a.edgePct);

    const best = opportunities[0] ?? null;
    if (!best) {
      this.resetCandidate("没有满足过滤的折价");
      this.s.entryBlockedReason = this.buildBlockedReason(ctx, params, fairUp);
      return null;
    }

    this.recordOpportunity(best);
    const candidate = this.updateCandidate(best, now);
    if (candidate.ticks < CONFIRM_TICKS || now - candidate.firstSeenTs < CONFIRM_MS) {
      this.s.entryBlockedReason = `确认中 ${candidate.ticks}/${CONFIRM_TICKS} ${Math.round(
        (now - candidate.firstSeenTs) / 100,
      ) / 10}s`;
      return null;
    }

    this.s.cooldownUntil = now + POST_SIGNAL_COOLDOWN_MS;
    this.candidate = null;
    this.s.candidateDirection = "none";
    this.s.candidateTicks = 0;
    this.s.candidateConfirmMs = 0;
    return { direction: best.direction };
  }

  onEntryFilled(ctx: StrategyTickContext, direction: StrategyDirection): void {
    const quote = getQuoteProxy(ctx, direction);
    const marketPct = getMarketPct(ctx, direction);
    this.s.entryTs = ctx.now;
    this.s.entryCostPct = quote?.buyCostPct ?? marketPct ?? null;
    this.s.entryEdgePct = this.s.edgePct;
    this.s.peakMarketPct = marketPct ?? null;
  }

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { rem, upPct, dnPct, diff, now } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;

    const marketPct = direction === "up" ? upPct : dnPct;
    if (this.s.peakMarketPct == null || marketPct > this.s.peakMarketPct) {
      this.s.peakMarketPct = marketPct;
    }

    const quote = getQuoteProxy(ctx, direction);
    const sellValuePct = quote?.sellValuePct ?? marketPct;
    const entryCostPct = this.s.entryCostPct ?? marketPct;
    const heldMs = this.s.entryTs > 0 ? now - this.s.entryTs : 0;
    const heldSeconds = Math.round(heldMs / 1000);
    const fairUp = getFairProb(diff, rem);
    const fairPct =
      fairUp == null ? null : direction === "up" ? fairUp : 100 - fairUp;
    const currentBuyEdge =
      fairPct == null || quote == null ? null : fairPct - quote.buyCostPct;

    if (rem <= FORCE_EXIT_REM && rem > 0) {
      return {
        signal: sellValuePct >= entryCostPct ? "tp" : "sl",
        reason: `临近结算强平 rem=${rem}s 卖价${sellValuePct.toFixed(1)}% 入场${entryCostPct.toFixed(1)}%`,
      };
    }

    if (heldMs >= MIN_HOLD_MS) {
      if (sellValuePct >= entryCostPct + TAKE_PROFIT_SELL_GAIN_PCT) {
        return {
          signal: "tp",
          reason: `卖价盈利 ${sellValuePct.toFixed(1)}%≥${(entryCostPct + TAKE_PROFIT_SELL_GAIN_PCT).toFixed(1)}%`,
        };
      }

      if (
        currentBuyEdge != null &&
        currentBuyEdge <= EXIT_EDGE_PCT &&
        sellValuePct >= entryCostPct + MIN_PROFIT_LOCK_PCT
      ) {
        return {
          signal: "tp",
          reason: `套利边际收敛 edge=${currentBuyEdge.toFixed(1)}% 卖价${sellValuePct.toFixed(1)}%`,
        };
      }

      if (
        this.s.peakMarketPct != null &&
        this.s.peakMarketPct >= entryCostPct + TAKE_PROFIT_SELL_GAIN_PCT &&
        marketPct <= this.s.peakMarketPct - TRAILING_PROB_RETRACE &&
        sellValuePct >= entryCostPct + MIN_PROFIT_LOCK_PCT
      ) {
        return {
          signal: "tp",
          reason: `峰值回撤锁利 概率${marketPct}% 峰值${this.s.peakMarketPct}%`,
        };
      }

      if (sellValuePct <= entryCostPct - STOP_LOSS_SELL_DROP_PCT) {
        return {
          signal: "sl",
          reason: `卖价回撤 ${sellValuePct.toFixed(1)}%≤${(entryCostPct - STOP_LOSS_SELL_DROP_PCT).toFixed(1)}%`,
        };
      }

      if (fairPct != null && fairPct <= entryCostPct - FAIR_BREAKDOWN_BUFFER_PCT) {
        return {
          signal: "sl",
          reason: `fair失效 ${fairPct.toFixed(1)}%≤${(entryCostPct - FAIR_BREAKDOWN_BUFFER_PCT).toFixed(1)}%`,
        };
      }

      if (direction === "up" && diff <= -EXIT_REVERSAL_DIFF) {
        return {
          signal: "sl",
          reason: `diff反穿 ${Math.round(diff)}≤${-EXIT_REVERSAL_DIFF}`,
        };
      }
      if (direction === "down" && diff >= EXIT_REVERSAL_DIFF) {
        return {
          signal: "sl",
          reason: `diff反穿 ${Math.round(diff)}≥${EXIT_REVERSAL_DIFF}`,
        };
      }
    }

    if (heldMs >= MAX_HOLD_SECONDS * 1000) {
      return {
        signal: sellValuePct >= entryCostPct ? "tp" : "sl",
        reason: `超时平仓 ${heldSeconds}s 卖价${sellValuePct.toFixed(1)}% 入场${entryCostPct.toFixed(1)}%`,
      };
    }

    return null;
  }

  resetState(): void {
    this.s = createState();
    this.candidate = null;
  }

  getStatePayload(): Record<string, unknown> {
    return {
      volatilityBps: this.s.volatilityBps,
      trendDiff: this.s.trendDiff,
      trendReady: this.s.trendReady,
      trendRequirement: this.s.trendRequirement,
      spreadPct: this.s.spreadPct,
      minEdgePct: this.s.minEdgePct,
      fairPct: this.s.fairPct,
      costPct: this.s.costPct,
      sellValuePct: this.s.sellValuePct,
      edgePct: this.s.edgePct,
      marketPct: this.s.marketPct,
      probDeltaPct: this.s.probDeltaPct,
      bestDirection: this.s.bestDirection,
      candidateDirection: this.s.candidateDirection,
      candidateTicks: this.s.candidateTicks,
      candidateConfirmMs: this.s.candidateConfirmMs,
      entryBlockedReason: this.s.entryBlockedReason,
      cooldownUntil: this.s.cooldownUntil,
      entryCostPct: this.s.entryCostPct,
      entryEdgePct: this.s.entryEdgePct,
      peakMarketPct: this.s.peakMarketPct,
    };
  }

  private recordOpportunity(opportunity: Opportunity): void {
    this.s.bestDirection = opportunity.direction;
    this.s.fairPct = opportunity.fairPct;
    this.s.costPct = opportunity.buyCostPct;
    this.s.sellValuePct = opportunity.sellValuePct;
    this.s.edgePct = opportunity.edgePct;
    this.s.marketPct = opportunity.marketPct;
    this.s.probDeltaPct = opportunity.probDeltaPct;
  }

  private updateCandidate(opportunity: Opportunity, now: number): CandidateState {
    if (
      !this.candidate ||
      this.candidate.direction !== opportunity.direction ||
      now - this.candidate.lastSeenTs > MAX_SIGNAL_GAP_MS
    ) {
      this.candidate = {
        ...opportunity,
        firstSeenTs: now,
        lastSeenTs: now,
        ticks: 1,
      };
    } else {
      this.candidate = {
        ...this.candidate,
        ...opportunity,
        lastSeenTs: now,
        ticks: this.candidate.ticks + 1,
      };
    }

    this.s.candidateDirection = this.candidate.direction;
    this.s.candidateTicks = this.candidate.ticks;
    this.s.candidateConfirmMs = now - this.candidate.firstSeenTs;
    return this.candidate;
  }

  private resetCandidate(reason: string): void {
    this.candidate = null;
    this.s.candidateDirection = "none";
    this.s.candidateTicks = 0;
    this.s.candidateConfirmMs = 0;
    this.s.entryBlockedReason = reason;
  }

  private buildBlockedReason(
    ctx: StrategyTickContext,
    params: DynamicParams,
    fairUp: number,
  ): string {
    const up = createOpportunity(ctx, "up", fairUp);
    const down = createOpportunity(ctx, "down", fairUp);
    const best = [up, down]
      .filter((o): o is Opportunity => o != null)
      .sort((a, b) => b.edgePct - a.edgePct)[0];
    if (!best) return "盘口成本缺失";

    this.recordOpportunity(best);
    if (best.edgePct < params.minEdgePct) {
      return `边际不足 ${best.edgePct.toFixed(1)}%<${params.minEdgePct.toFixed(1)}%`;
    }
    if (
      best.buyCostPct < MIN_ENTRY_COST_PCT ||
      best.buyCostPct > MAX_ENTRY_COST_PCT
    ) {
      return `买入成本不在区间 ${best.buyCostPct.toFixed(1)}%`;
    }
    if (!isDirectionConfirmed(ctx, params, best.direction)) {
      return `趋势未确认 diff=${Math.round(ctx.diff ?? 0)} trend=${Math.round(params.trendDiff)}`;
    }
    if (best.probDeltaPct < MAX_NEGATIVE_PROB_MOMENTUM) {
      return `概率反向过快 ${best.probDeltaPct.toFixed(1)}%`;
    }
    if (best.fairPct < MIN_FAIR_PROB_PCT) {
      return `fair过低 ${best.fairPct.toFixed(1)}%`;
    }
    return "过滤未通过";
  }
}
