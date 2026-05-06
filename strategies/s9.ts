/**
 * 策略9 · 智能锁仓套利
 *
 * 在 S8 的“单边低估进场”基础上，持仓后持续比较：
 * - 直接卖出当前仓位的结果
 * - 买入反边凑完整套的锁仓结果
 * - 继续持有的期望结果
 *
 * 注意：这里的锁仓是执行层买入反方向，形成近似 complete set。
 * 具体成交数量受盘口、滑点、FOK 成交行为影响，仍需小金额先跑。
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

const WINDOW_MAX_REMAINING = 220;
const WINDOW_MIN_REMAINING = 32;
const MIN_DIRECTIONAL_DIFF = 20;
const VOL_LOOKBACK = 8;

const BASE_MIN_EDGE_PCT = 8.5;
const MAX_MIN_EDGE_PCT = 17;
const MAX_SPREAD_PCT = 0.032;
const MIN_ENTRY_COST_PCT = 8;
const MAX_ENTRY_COST_PCT = 88;
const MIN_FAIR_PROB_PCT = 24;

const CONFIRM_TICKS = 2;
const CONFIRM_MS = 1500;
const MAX_SIGNAL_GAP_MS = 2800;
const POST_SIGNAL_COOLDOWN_MS = 15000;
const MAX_NEGATIVE_PROB_MOMENTUM = -3;

const MIN_HOLD_MS = 2500;
const LOCK_COST_BUFFER_PCT = 0.8;
const LOCK_ADVANTAGE_PCT = 0.7;
const MIN_LOCK_PROFIT_PCT = 2.2;
const SOFT_LOSS_PCT = -3.2;
const HARD_STOP_PCT = -8;
const MAX_LOCK_LOSS_PCT = -6.2;
const LOCK_IMPROVEMENT_PCT = 1.4;
const DIRECT_TP_PCT = 6;
const EXIT_EDGE_PCT = 2.2;
const FAIR_BREAKDOWN_BUFFER_PCT = 3;
const EXIT_REVERSAL_DIFF = 6;
const MAX_HOLD_SECONDS = 55;
const FORCE_EXIT_REM = 10;

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

type S9Decision = "idle" | "scan" | "hold" | "sell" | "lockProfit" | "lockLoss" | "locked";

interface S9State {
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
  sellPnlPct: number | null;
  lockPnlPct: number | null;
  lockEquivalentSellPct: number | null;
  lockAdvantagePct: number | null;
  holdScorePct: number | null;
  oppositeCostPct: number | null;
  decision: S9Decision;
  locked: boolean;
  lockDirection: StrategyDirection | null;
  lockReason: string;
}

function createState(): S9State {
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
    sellPnlPct: null,
    lockPnlPct: null,
    lockEquivalentSellPct: null,
    lockAdvantagePct: null,
    holdScorePct: null,
    oppositeCostPct: null,
    decision: "idle",
    locked: false,
    lockDirection: null,
    lockReason: "",
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

function opposite(direction: StrategyDirection): StrategyDirection {
  return direction === "up" ? "down" : "up";
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
  const spreadPenalty = spreadPct == null ? 3 : spreadPct * 100 * 0.9;
  const volatilityPenalty = Math.max(0, volatilityBps - 10) * 0.42;
  const minEdgePct = clamp(
    BASE_MIN_EDGE_PCT + spreadPenalty + volatilityPenalty,
    BASE_MIN_EDGE_PCT,
    MAX_MIN_EDGE_PCT,
  );
  const trendRequirement = Math.round(
    clamp(12 + Math.max(0, volatilityBps - 10) * 1.15, 12, 36),
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

export class S9SmartLockArb implements IStrategy {
  readonly key: StrategyKey = "s9";
  readonly number: StrategyNumber = 9;
  readonly name = "智能锁仓套利";

  private s: S9State = createState();
  private candidate: CandidateState | null = null;

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略9 · 智能锁仓套利",
      lines: [
        { text: "先按 S8 类似逻辑寻找单边低估，确认后只买便宜的一侧" },
        { text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~${WINDOW_MIN_REMAINING}s 检测，边际随波动和点差动态提高` },
        { text: `入场：edge≥动态门槛 + 盘口≤${(MAX_SPREAD_PCT * 100).toFixed(1)}% + 1m趋势同向 + 连续确认`, marginTop: true },
        { text: "持仓后每 tick 比较：直接卖出 / 买反边锁仓 / 继续持有", color: "#58a6ff" },
        { text: `盈利锁仓：仅当锁仓收益≥${MIN_LOCK_PROFIT_PCT}%，且比直接卖出多≥${LOCK_ADVANTAGE_PCT}% 才买反边锁利`, color: "#3fb950", marginTop: true },
        { text: `亏损补救：若直接卖亏损，但锁损比卖出少亏≥${LOCK_IMPROVEMENT_PCT}%，且锁后亏损不超过${Math.abs(MAX_LOCK_LOSS_PCT)}%，买反边锁损`, color: "#f0a500" },
        { text: `硬退出：直接卖亏损≤${HARD_STOP_PCT}%、fair失效、diff反穿、超时或最后${FORCE_EXIT_REM}s`, color: "#f85149" },
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
    this.s.decision = "scan";

    if (upPct == null || dnPct == null || diff == null) {
      this.resetCandidate("等待概率/差价");
      return null;
    }

    const params = buildDynamicParams(ctx);
    this.recordParams(params);

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
    this.s.decision = "hold";
    this.s.locked = false;
    this.s.lockDirection = null;
    this.s.lockReason = "";
  }

  onLockFilled(_ctx: StrategyTickContext, direction: StrategyDirection): void {
    this.s.locked = true;
    this.s.lockDirection = direction;
    this.s.decision = "locked";
  }

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { rem, upPct, dnPct, diff, now } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    if (this.s.locked) return null;

    const quote = getQuoteProxy(ctx, direction);
    const oppositeQuote = getQuoteProxy(ctx, opposite(direction));
    const marketPct = getMarketPct(ctx, direction);
    const entryCostPct = this.s.entryCostPct ?? marketPct ?? null;
    if (!quote || !oppositeQuote || entryCostPct == null) return null;

    const heldMs = this.s.entryTs > 0 ? now - this.s.entryTs : 0;
    const heldSeconds = Math.round(heldMs / 1000);
    const fairUp = getFairProb(diff, rem);
    const fairPct =
      fairUp == null ? null : direction === "up" ? fairUp : 100 - fairUp;
    const spreadPct = getSpreadPct(ctx);
    const riskPenalty =
      (spreadPct == null ? 2 : spreadPct * 100 * 0.55) +
      Math.max(0, this.s.volatilityBps - 10) * 0.18 +
      (rem < 30 ? 1.2 : 0);
    const sellPnlPct = quote.sellValuePct - entryCostPct;
    const lockEquivalentSellPct =
      100 - oppositeQuote.buyCostPct - LOCK_COST_BUFFER_PCT;
    const lockPnlPct = lockEquivalentSellPct - entryCostPct;
    const lockAdvantagePct = lockEquivalentSellPct - quote.sellValuePct;
    const holdScorePct =
      fairPct == null ? null : fairPct - entryCostPct - riskPenalty;
    const currentBuyEdge =
      fairPct == null ? null : fairPct - quote.buyCostPct;

    this.s.sellValuePct = quote.sellValuePct;
    this.s.oppositeCostPct = oppositeQuote.buyCostPct;
    this.s.sellPnlPct = sellPnlPct;
    this.s.lockPnlPct = lockPnlPct;
    this.s.lockEquivalentSellPct = lockEquivalentSellPct;
    this.s.lockAdvantagePct = lockAdvantagePct;
    this.s.holdScorePct = holdScorePct;
    this.s.fairPct = fairPct;
    this.s.costPct = quote.buyCostPct;
    this.s.marketPct = marketPct;
    this.s.decision = "hold";

    if (rem <= FORCE_EXIT_REM && rem > 0) {
      this.s.decision = "sell";
      return {
        signal: sellPnlPct >= 0 ? "tp" : "sl",
        reason: `临近结算卖出 rem=${rem}s sellPnl=${sellPnlPct.toFixed(1)}% lockPnl=${lockPnlPct.toFixed(1)}%`,
      };
    }

    if (heldMs >= MIN_HOLD_MS) {
      if (
        lockPnlPct >= MIN_LOCK_PROFIT_PCT &&
        lockAdvantagePct >= LOCK_ADVANTAGE_PCT
      ) {
        this.s.decision = "lockProfit";
        this.s.lockReason = `锁利 lockPnl=${lockPnlPct.toFixed(1)}% 优于卖出${lockAdvantagePct.toFixed(1)}%`;
        return {
          signal: "lock",
          reason: this.s.lockReason,
        };
      }

      if (
        sellPnlPct <= SOFT_LOSS_PCT &&
        lockPnlPct >= MAX_LOCK_LOSS_PCT &&
        lockAdvantagePct >= LOCK_IMPROVEMENT_PCT
      ) {
        this.s.decision = "lockLoss";
        this.s.lockReason = `锁损优于卖出${lockAdvantagePct.toFixed(1)}% lockPnl=${lockPnlPct.toFixed(1)}% sellPnl=${sellPnlPct.toFixed(1)}%`;
        return {
          signal: "lock",
          reason: this.s.lockReason,
        };
      }

      if (sellPnlPct >= DIRECT_TP_PCT) {
        this.s.decision = "sell";
        return {
          signal: "tp",
          reason: `直接卖出止盈 sellPnl=${sellPnlPct.toFixed(1)}% lockPnl=${lockPnlPct.toFixed(1)}% lockAdv=${lockAdvantagePct.toFixed(1)}%`,
        };
      }

      if (
        currentBuyEdge != null &&
        currentBuyEdge <= EXIT_EDGE_PCT &&
        sellPnlPct >= 1
      ) {
        this.s.decision = "sell";
        return {
          signal: "tp",
          reason: `边际收敛卖出 edge=${currentBuyEdge.toFixed(1)}% sellPnl=${sellPnlPct.toFixed(1)}%`,
        };
      }

      if (sellPnlPct <= HARD_STOP_PCT && lockAdvantagePct < LOCK_IMPROVEMENT_PCT) {
        this.s.decision = "sell";
        return {
          signal: "sl",
          reason: `硬止损 sellPnl=${sellPnlPct.toFixed(1)}% lockPnl=${lockPnlPct.toFixed(1)}% lockAdv=${lockAdvantagePct.toFixed(1)}%`,
        };
      }

      if (fairPct != null && fairPct <= entryCostPct - FAIR_BREAKDOWN_BUFFER_PCT) {
        this.s.decision = "sell";
        return {
          signal: "sl",
          reason: `fair失效 ${fairPct.toFixed(1)}%≤${(entryCostPct - FAIR_BREAKDOWN_BUFFER_PCT).toFixed(1)}%`,
        };
      }

      if (direction === "up" && diff <= -EXIT_REVERSAL_DIFF) {
        this.s.decision = "sell";
        return {
          signal: "sl",
          reason: `diff反穿 ${Math.round(diff)}≤${-EXIT_REVERSAL_DIFF}`,
        };
      }
      if (direction === "down" && diff >= EXIT_REVERSAL_DIFF) {
        this.s.decision = "sell";
        return {
          signal: "sl",
          reason: `diff反穿 ${Math.round(diff)}≥${EXIT_REVERSAL_DIFF}`,
        };
      }
    }

    if (heldMs >= MAX_HOLD_SECONDS * 1000) {
      this.s.decision = "sell";
      return {
        signal: sellPnlPct >= 0 ? "tp" : "sl",
        reason: `超时卖出 ${heldSeconds}s sellPnl=${sellPnlPct.toFixed(1)}% lockPnl=${lockPnlPct.toFixed(1)}%`,
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
      sellPnlPct: this.s.sellPnlPct,
      lockPnlPct: this.s.lockPnlPct,
      lockEquivalentSellPct: this.s.lockEquivalentSellPct,
      lockAdvantagePct: this.s.lockAdvantagePct,
      holdScorePct: this.s.holdScorePct,
      oppositeCostPct: this.s.oppositeCostPct,
      decision: this.s.decision,
      locked: this.s.locked,
      lockDirection: this.s.lockDirection,
      lockReason: this.s.lockReason,
    };
  }

  private recordParams(params: DynamicParams): void {
    this.s.volatilityBps = params.volatilityBps;
    this.s.trendDiff = params.trendDiff;
    this.s.trendReady = params.trendReady;
    this.s.trendRequirement = params.trendRequirement;
    this.s.spreadPct = params.spreadPct;
    this.s.minEdgePct = params.minEdgePct;
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
