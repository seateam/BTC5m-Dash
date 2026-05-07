/**
 * S10 - Bonereaper-style dual-leg inventory strategy.
 *
 * Bonereaper-style paper baseline:
 * - The first leg is still a positive-EV underpriced side.
 * - The target state is a complete Up+Down set whenever the second leg can be
 *   bought cheaply enough to lock profit or cap tail risk.
 * - In paper mode complete sets stay visible by default so the dashboard can
 *   audit both legs until settlement; pre-settlement merge is optional.
 */

import type {
  EntrySignal,
  ExitSignal,
  IStrategy,
  Kline,
  MakerQuoteSignal,
  MakerStatusSnapshot,
  StrategyDescription,
  StrategyDirection,
  StrategyKey,
  StrategyNumber,
  StrategyTickContext,
} from "./types.js";
import { getFairProb } from "./fair-prob.js";

const WINDOW_MAX_REMAINING = 288;
const WINDOW_MIN_REMAINING = 11;
const CONFIRM_TICKS = 1;
const CONFIRM_MS = 0;
const MAX_SIGNAL_GAP_MS = 2200;

const MIN_COST_PCT = 2;
const MAX_COST_PCT = 98;
const DEFAULT_MIN_EDGE_PCT = 2.4;
const VOL_LOOKBACK = 8;
const MIN_NOTIONAL = 6;
const MAX_NOTIONAL = 28;
const SCALE_IN_MIN_GAP_MS = 10_000;
const SCALE_IN_MIN_REMAINING = 18;
const SCALE_IN_EDGE_ADDON_PCT = 0.35;
const MAX_WINDOW_NOTIONAL_ESTIMATE = 420;
const LOCK_MIN_HOLD_MS = 2500;
const LOCK_MIN_REMAINING = 9;
const LOCK_BASE_BUFFER_PCT = 0.75;
const LOCK_ADVANTAGE_FLOOR_PCT = -2.5;
const DEFENSIVE_LOCK_MAX_LOSS_PCT = -1.8;
const DEFENSIVE_LOCK_REM = 45;
const DEFENSIVE_FAIR_BREAKDOWN_PCT = 6;
const MAKER_MIN_REMAINING = 4;
const MAKER_MAX_REMAINING = 290;
const MAKER_TARGET_SET_EDGE_OPENING = 4.6;
const MAKER_TARGET_SET_EDGE_MID = 3.2;
const MAKER_TARGET_SET_EDGE_LATE = 2.0;
const MAKER_MIN_PRICE = 0.03;
const MAKER_MAX_PRICE = 0.97;
const MAKER_TERMINAL_MAX_PRICE = 0.99;
const MAKER_TTL_MS = 2200;
const MAKER_TERMINAL_TTL_MS = 850;
const MAKER_BASE_NOTIONAL = 18;
const MAKER_MAX_SHARES_PER_SIDE = 75;
const MAKER_SOFT_IMBALANCE_SHARES = 45;
const MAKER_HARD_IMBALANCE_SHARES = 110;
const MAKER_REBALANCE_SIZE_MULT = 1.35;
const MAKER_MIN_QUOTE_SHARES = 5;
const MAKER_MIN_PROJECTED_INV_EV_ROI_PCT = 0.25;
const MAKER_SOFT_PROJECTED_PAIR_LOSS_PCT = 4;
const MAKER_MAX_PROJECTED_PAIR_LOSS_PCT = 10;
const MAKER_MIN_TAIL_EV_FOR_PAIR_LOSS_PCT = 12;
const MAKER_MIN_TERMINAL_TAIL_EV_PCT = 0.5;
const MAKER_MAX_PROJECTED_TAIL_SHARES = 42;
const MAKER_MAX_TERMINAL_TAIL_SHARES = 45;
const MAKER_MAX_SEED_TAIL_SHARES = 16;
const MAKER_MAX_BALANCE_TAIL_SHARES = 34;
const MAKER_TERMINAL_START_REM = 13;
const MAKER_TERMINAL_CHASE_MIN_BID_PCT = 82;
const MAKER_TERMINAL_PANIC_MAX_SHARES = 5;
const MAKER_LOW_FAIR_TAIL_BLOCK_PCT = 35;
const MAKER_MAX_LOW_FAIR_TAIL_SHARES = 8;
const MAKER_MIN_INSURANCE_TAIL_SHARES = 6;
const MAKER_MAX_INSURANCE_SHARES = 28;
const MAKER_WEAK_SIDE_BLOCK_REM_SEC = 180;
const MAKER_WEAK_SIDE_BLOCK_GAP_PCT = 30;
const MAKER_LATE_WEAK_SIDE_BLOCK_REM_SEC = 120;
const MAKER_LATE_WEAK_SIDE_BLOCK_GAP_PCT = 12;
const MAKER_CONVICTION_WEAK_SIDE_BLOCK_REM_SEC = 45;
const MAKER_CONVICTION_WEAK_SIDE_BLOCK_GAP_PCT = 5;
const MAKER_WEAK_SIDE_LOCK_MIN_PAIR_PROFIT_PCT = 1.0;
const MAKER_MIN_HEDGE_PAIR_PROFIT_PCT = 0.35;
const MAKER_MIN_BALANCED_REPAIR_PAIR_PROFIT_PCT = 0.05;
const MAKER_BALANCED_REPAIR_MIN_TAIL_SHARES = 8;
const MAKER_BALANCED_REPAIR_INV_ROI_FLOOR_PCT = -1.5;

type S10Phase = "opening" | "inventory" | "conviction" | "terminal";
type S10MakerModule = "idle" | "seed" | "balance" | "conviction" | "terminal";

type S10Decision = "idle" | "scan" | "enter" | "hold" | "lockProfit" | "lockDefensive" | "locked";

interface QuoteProxy {
  buyCostPct: number;
  sellValuePct: number;
}

interface Opportunity {
  direction: StrategyDirection;
  phase: S10Phase;
  fairPct: number;
  costPct: number;
  sellValuePct: number;
  marketPct: number;
  rawEdgePct: number;
  bufferPct: number;
  edgePct: number;
  roiPct: number;
  directionalDiff: number;
  amount: number;
  score: number;
}

interface LockOpportunity {
  direction: StrategyDirection;
  oppositeDirection: StrategyDirection;
  firstLegAvgCostPct: number;
  firstLegSellValuePct: number;
  oppositeCostPct: number;
  lockTotalCostPct: number;
  lockGrossProfitPct: number;
  lockBufferPct: number;
  lockNetProfitPct: number;
  lockAdvantagePct: number;
  minLockProfitPct: number;
  fairPct: number | null;
  holdScorePct: number | null;
  targetSharesEstimate: number;
  ready: boolean;
  defensive: boolean;
  reason: string;
}

interface PhaseProfile {
  phase: S10Phase;
  minEdgePct: number;
  minRawEdgePct: number;
  minFairPct: number;
  minDirectionalDiff: number;
  maxSpreadPct: number;
  baseAmount: number;
}

interface S10State {
  decision: S10Decision;
  ready: boolean;
  status: string;
  reason: string;
  entryBlockedReason: string;
  fairPct: number | null;
  costPct: number | null;
  edgePct: number | null;
  roiPct: number | null;
  marketPct: number | null;
  sellValuePct: number | null;
  rawEdgePct: number | null;
  bufferPct: number | null;
  amount: number | null;
  minEdgePct: number;
  minRawEdgePct: number;
  minFairPct: number;
  minDirectionalDiff: number;
  maxSpreadPct: number;
  volatilityBps: number;
  spreadPct: number | null;
  phase: S10Phase | "none";
  bestDirection: StrategyDirection | "none";
  candidateDirection: StrategyDirection | "none";
  candidateTicks: number;
  candidateConfirmMs: number;
  entryTs: number;
  entryDirection: StrategyDirection | null;
  entryCostPct: number | null;
  firstLegAvgCostPct: number | null;
  firstLegSharesEstimate: number;
  firstLegCostUsd: number;
  oppositeCostPct: number | null;
  lockTotalCostPct: number | null;
  lockGrossProfitPct: number | null;
  lockNetProfitPct: number | null;
  lockAdvantagePct: number | null;
  lockBufferPct: number | null;
  lockMinProfitPct: number;
  lockTargetSharesEstimate: number;
  lockDirection: StrategyDirection | null;
  locked: boolean;
  makerMode: string;
  makerActiveOrders: number;
  makerUpOrders: number;
  makerDownOrders: number;
  makerUpBidPct: number | null;
  makerDownBidPct: number | null;
  makerTotalBidCostPct: number | null;
  makerTargetEdgePct: number | null;
  makerFilledCount: number;
  makerMergedCount: number;
  makerLastFill: string;
  makerLastReason: string;
  makerRiskReason: string;
  makerModule: S10MakerModule;
  makerTerminalDirection: StrategyDirection | null;
  makerTerminalConfidencePct: number | null;
  makerTerminalEdgePct: number | null;
  makerTerminalMaxPricePct: number | null;
  makerTerminalReason: string;
  makerProjectedEvRoiPct: number | null;
  makerProjectedPairPct: number | null;
  makerProjectedTailEvPct: number | null;
  makerProjectedTailShares: number;
  inventoryUpSize: number;
  inventoryDownSize: number;
  inventoryImbalance: number;
  pairedShares: number;
  pairedAvgCostPct: number | null;
  pairedProfitPct: number | null;
  tailDirection: StrategyDirection | null;
  tailShares: number;
  tailAvgCostPct: number | null;
  tailFairPct: number | null;
  tailEvPct: number | null;
  inventoryEvUsd: number | null;
  inventoryEvRoiPct: number | null;
  scaleInCount: number;
  lastScaleInAt: number;
  estimatedWindowNotional: number | null;

  // Compatibility with the existing S10 panel keys.
  targetShares: number;
  totalCost: number | null;
  totalCostPct: number | null;
  grossProfitPct: number | null;
  netProfitPct: number | null;
  upAvgAsk: number | null;
  downAvgAsk: number | null;
  firstLegDirection: StrategyDirection | null;
  firstLegCost: number | null;
  secondLegDirection: StrategyDirection | null;
  secondLegCost: number | null;
  lockReason: string;
}

function createState(): S10State {
  return {
    decision: "idle",
    ready: false,
    status: "idle",
    reason: "",
    entryBlockedReason: "",
    fairPct: null,
    costPct: null,
    edgePct: null,
    roiPct: null,
    marketPct: null,
    sellValuePct: null,
    rawEdgePct: null,
    bufferPct: null,
    amount: null,
    minEdgePct: DEFAULT_MIN_EDGE_PCT,
    minRawEdgePct: 0,
    minFairPct: 0,
    minDirectionalDiff: 0,
    maxSpreadPct: 0,
    volatilityBps: 12,
    spreadPct: null,
    phase: "none",
    bestDirection: "none",
    candidateDirection: "none",
    candidateTicks: 0,
    candidateConfirmMs: 0,
    entryTs: 0,
    entryDirection: null,
    entryCostPct: null,
    firstLegAvgCostPct: null,
    firstLegSharesEstimate: 0,
    firstLegCostUsd: 0,
    oppositeCostPct: null,
    lockTotalCostPct: null,
    lockGrossProfitPct: null,
    lockNetProfitPct: null,
    lockAdvantagePct: null,
    lockBufferPct: null,
    lockMinProfitPct: 1.2,
    lockTargetSharesEstimate: 0,
    lockDirection: null,
    locked: false,
    makerMode: "idle",
    makerActiveOrders: 0,
    makerUpOrders: 0,
    makerDownOrders: 0,
    makerUpBidPct: null,
    makerDownBidPct: null,
    makerTotalBidCostPct: null,
    makerTargetEdgePct: null,
    makerFilledCount: 0,
    makerMergedCount: 0,
    makerLastFill: "",
    makerLastReason: "",
    makerRiskReason: "",
    makerModule: "idle",
    makerTerminalDirection: null,
    makerTerminalConfidencePct: null,
    makerTerminalEdgePct: null,
    makerTerminalMaxPricePct: null,
    makerTerminalReason: "",
    makerProjectedEvRoiPct: null,
    makerProjectedPairPct: null,
    makerProjectedTailEvPct: null,
    makerProjectedTailShares: 0,
    inventoryUpSize: 0,
    inventoryDownSize: 0,
    inventoryImbalance: 0,
    pairedShares: 0,
    pairedAvgCostPct: null,
    pairedProfitPct: null,
    tailDirection: null,
    tailShares: 0,
    tailAvgCostPct: null,
    tailFairPct: null,
    tailEvPct: null,
    inventoryEvUsd: null,
    inventoryEvRoiPct: null,
    scaleInCount: 0,
    lastScaleInAt: 0,
    estimatedWindowNotional: null,
    targetShares: 0,
    totalCost: null,
    totalCostPct: null,
    grossProfitPct: null,
    netProfitPct: null,
    upAvgAsk: null,
    downAvgAsk: null,
    firstLegDirection: null,
    firstLegCost: null,
    secondLegDirection: null,
    secondLegCost: null,
    lockReason: "",
  };
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function opposite(direction: StrategyDirection): StrategyDirection {
  return direction === "up" ? "down" : "up";
}

function getPositionSize(ctx: StrategyTickContext, direction: StrategyDirection): number {
  return direction === "up" ? ctx.position.upSize : ctx.position.downSize;
}

function getPositionCostPct(ctx: StrategyTickContext, direction: StrategyDirection): number | null {
  const cost = direction === "up" ? ctx.position.upCostPct : ctx.position.downCostPct;
  return cost != null && Number.isFinite(cost) ? cost : null;
}

function median(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 1 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function getRecentClosed(klines: readonly Kline[], count: number): Kline[] {
  return klines.filter((k) => k.closed).slice(-count);
}

function getRecentVolatilityBps(klines: readonly Kline[]): number {
  const ranges = getRecentClosed(klines, VOL_LOOKBACK)
    .map((k) => {
      if (!Number.isFinite(k.open) || !Number.isFinite(k.high) || !Number.isFinite(k.low) || k.open <= 0) {
        return null;
      }
      return ((k.high - k.low) / k.open) * 10000;
    })
    .filter((v): v is number => v != null && Number.isFinite(v) && v > 0);
  return clamp(median(ranges) ?? 12, 5, 45);
}

function getSpreadPct(ctx: StrategyTickContext): number | null {
  const { bestBid, bestAsk } = ctx;
  if (bestBid == null || bestAsk == null || bestBid <= 0 || bestAsk <= 0 || bestAsk < bestBid) return null;
  const mid = (bestBid + bestAsk) / 2;
  return mid > 0 ? (bestAsk - bestBid) / mid : null;
}

function getQuoteProxy(ctx: StrategyTickContext, direction: StrategyDirection): QuoteProxy | null {
  const { bestBid, bestAsk } = ctx;
  if (
    bestBid == null ||
    bestAsk == null ||
    bestBid < 0 ||
    bestAsk <= 0 ||
    bestAsk < bestBid ||
    bestBid > 1 ||
    bestAsk > 1
  ) return null;
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

function getMarketPct(ctx: StrategyTickContext, direction: StrategyDirection): number | null {
  return direction === "up" ? ctx.upPct : ctx.dnPct;
}

function getPhaseProfile(rem: number, volatilityBps: number): PhaseProfile {
  const volAdd = clamp((volatilityBps - 12) * 0.08, 0, 1.8);
  if (rem >= 210) {
    return {
      phase: "opening",
      minEdgePct: 3.2 + volAdd,
      minRawEdgePct: 4.0 + volAdd,
      minFairPct: 55,
      minDirectionalDiff: 12,
      maxSpreadPct: 0.07,
      baseAmount: 8,
    };
  }
  if (rem >= 75) {
    return {
      phase: "inventory",
      minEdgePct: 2.0 + volAdd,
      minRawEdgePct: 2.8 + volAdd,
      minFairPct: 53,
      minDirectionalDiff: 8,
      maxSpreadPct: 0.085,
      baseAmount: 12,
    };
  }
  if (rem >= 24) {
    return {
      phase: "conviction",
      minEdgePct: 1.0 + volAdd * 0.5,
      minRawEdgePct: 1.5 + volAdd * 0.5,
      minFairPct: 57,
      minDirectionalDiff: 14,
      maxSpreadPct: 0.1,
      baseAmount: 16,
    };
  }
  return {
    phase: "terminal",
    minEdgePct: 0.6,
    minRawEdgePct: 1.0,
    minFairPct: 66,
    minDirectionalDiff: 25,
    maxSpreadPct: 0.12,
    baseAmount: 10,
  };
}

function getExecutionBufferPct(spreadPct: number | null, volatilityBps: number, rem: number): number {
  const spreadBuffer = spreadPct == null ? 1.1 : clamp(spreadPct * 100 * 0.35, 0.25, 3.5);
  const latencyBuffer = rem <= 24 ? 1.15 : rem <= 75 ? 0.8 : 0.55;
  const volBuffer = clamp((volatilityBps - 12) * 0.06, 0, 1.2);
  return clamp(spreadBuffer + latencyBuffer + volBuffer, 0.8, 4.8);
}

function getLockBufferPct(spreadPct: number | null, volatilityBps: number, rem: number): number {
  const spreadBuffer = spreadPct == null ? 0.8 : clamp(spreadPct * 100 * 0.22, 0.15, 1.8);
  const latencyBuffer = rem <= 20 ? 0.9 : rem <= 75 ? 0.55 : 0.35;
  const volBuffer = clamp((volatilityBps - 12) * 0.035, 0, 0.85);
  return clamp(LOCK_BASE_BUFFER_PCT + spreadBuffer + latencyBuffer + volBuffer, 0.9, 3.8);
}

function getMinLockProfitPct(rem: number, phase: S10Phase | "none"): number {
  if (phase === "terminal" || rem <= 24) return 0.45;
  if (phase === "conviction" || rem <= 75) return 0.85;
  if (phase === "inventory") return 1.15;
  return 1.45;
}

function getMakerTargetSetEdgePct(rem: number): number {
  if (rem <= MAKER_TERMINAL_START_REM) return 0.8;
  if (rem <= 40) return MAKER_TARGET_SET_EDGE_LATE;
  if (rem <= 150) return MAKER_TARGET_SET_EDGE_MID;
  return MAKER_TARGET_SET_EDGE_OPENING;
}

function getMakerRebalanceEdgePct(rem: number): number {
  if (rem <= 40) return 0.7;
  if (rem <= 120) return 1.0;
  return 1.4;
}

function getMakerModule(rem: number): S10MakerModule {
  if (rem < MAKER_MIN_REMAINING || rem > MAKER_MAX_REMAINING) return "idle";
  if (rem <= MAKER_TERMINAL_START_REM) return "terminal";
  if (rem <= 45) return "conviction";
  if (rem <= 150) return "balance";
  return "seed";
}

function getMakerWeakSideBlockReason(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  fairPct: number,
  module: S10MakerModule,
): string | null {
  if (module === "terminal") return null;
  const weakGapPct = 50 - fairPct;
  if (weakGapPct <= 0) return null;
  const strictBlock =
    ctx.rem <= MAKER_CONVICTION_WEAK_SIDE_BLOCK_REM_SEC &&
    weakGapPct >= MAKER_CONVICTION_WEAK_SIDE_BLOCK_GAP_PCT;
  const lateBlock =
    ctx.rem <= MAKER_LATE_WEAK_SIDE_BLOCK_REM_SEC &&
    weakGapPct >= MAKER_LATE_WEAK_SIDE_BLOCK_GAP_PCT;
  const broadBlock =
    ctx.rem <= MAKER_WEAK_SIDE_BLOCK_REM_SEC &&
    weakGapPct >= MAKER_WEAK_SIDE_BLOCK_GAP_PCT;
  if (!strictBlock && !lateBlock && !broadBlock) return null;
  return `weak-side ${direction} fair ${fairPct.toFixed(1)}% rem ${ctx.rem.toFixed(0)}s`;
}

function makerModuleToPhase(module: S10MakerModule): S10Phase | "none" {
  if (module === "seed") return "opening";
  if (module === "balance") return "inventory";
  if (module === "conviction" || module === "terminal") return module;
  return "none";
}

function getMakerFairEdgePct(module: S10MakerModule, rem: number): number {
  if (module === "terminal") return rem <= 7 ? 0.25 : 0.4;
  if (module === "conviction") return 1.25;
  if (module === "balance") return 2.0;
  if (module === "seed") return 2.9;
  return 999;
}

function getMakerQuoteTtlMs(module: S10MakerModule): number {
  if (module === "terminal") return MAKER_TERMINAL_TTL_MS;
  if (module === "conviction") return 1400;
  return MAKER_TTL_MS;
}

function getMakerMaxPricePct(module: S10MakerModule): number {
  return (module === "terminal" ? MAKER_TERMINAL_MAX_PRICE : MAKER_MAX_PRICE) * 100;
}

function getMakerMaxTailShares(module: S10MakerModule): number {
  if (module === "seed") return MAKER_MAX_SEED_TAIL_SHARES;
  if (module === "balance") return MAKER_MAX_BALANCE_TAIL_SHARES;
  if (module === "conviction" || module === "terminal") return MAKER_MAX_TERMINAL_TAIL_SHARES;
  return 0;
}

function getMakerMaxPairLossPct(module: S10MakerModule): number {
  if (module === "seed") return 3.0;
  if (module === "balance") return 5.0;
  if (module === "conviction") return 7.0;
  if (module === "terminal") return 12.0;
  return MAKER_MAX_PROJECTED_PAIR_LOSS_PCT;
}

function getMakerSoftPairLossPct(module: S10MakerModule): number {
  if (module === "seed") return 1.5;
  if (module === "balance") return 3.0;
  if (module === "conviction") return MAKER_SOFT_PROJECTED_PAIR_LOSS_PCT;
  if (module === "terminal") return 6.0;
  return MAKER_SOFT_PROJECTED_PAIR_LOSS_PCT;
}

function getMakerTailEvForPairLossPct(module: S10MakerModule): number {
  if (module === "terminal") return 8.0;
  if (module === "conviction") return 10.0;
  if (module === "balance") return MAKER_MIN_TAIL_EV_FOR_PAIR_LOSS_PCT;
  return 16.0;
}

function getTerminalMinConfidencePct(rem: number, currentBidPct: number): number {
  const base = rem <= 6 ? 99.15 : rem <= 10 ? 98.75 : 98.35;
  return currentBidPct >= MAKER_TERMINAL_CHASE_MIN_BID_PCT ? base : 99.6;
}

function getTerminalMinDiff(rem: number): number {
  if (rem <= 6) return 24;
  if (rem <= 10) return 32;
  return 44;
}

function getMakerInsuranceEdgeFloorPct(module: S10MakerModule, pairProfitPct: number | null): number {
  if (pairProfitPct != null && pairProfitPct >= MAKER_MIN_HEDGE_PAIR_PROFIT_PCT) {
    if (module === "seed") return -0.75;
    if (module === "balance") return -0.5;
    if (module === "conviction") return -0.25;
    return 0;
  }
  if (module === "balance") return -0.8;
  if (module === "conviction") return -0.25;
  return -0.35;
}

function getMakerBalancedRepairPairFloorPct(module: S10MakerModule): number {
  if (module === "seed" || module === "balance" || module === "conviction") {
    return MAKER_MIN_BALANCED_REPAIR_PAIR_PROFIT_PCT;
  }
  return MAKER_MIN_HEDGE_PAIR_PROFIT_PCT;
}

function getMakerBalancedRepairEdgeFloorPct(module: S10MakerModule): number {
  if (module === "seed") return -1.0;
  if (module === "balance") return -0.75;
  if (module === "conviction") return -0.25;
  return 0;
}

function getCurrentTail(ctx: StrategyTickContext): { direction: StrategyDirection | null; shares: number } {
  const imbalance = (ctx.position.upSize || 0) - (ctx.position.downSize || 0);
  if (Math.abs(imbalance) <= 0.0001) return { direction: null, shares: 0 };
  return { direction: imbalance > 0 ? "up" : "down", shares: Math.abs(imbalance) };
}

function getMakerTailRoom(ctx: StrategyTickContext, direction: StrategyDirection, module: S10MakerModule): number {
  const ownSize = getPositionSize(ctx, direction);
  const otherSize = getPositionSize(ctx, opposite(direction));
  if (ownSize < otherSize) return MAKER_MAX_SHARES_PER_SIDE;
  return Math.max(0, otherSize + getMakerMaxTailShares(module) - ownSize);
}

function getMakerInsuranceMaxShares(ctx: StrategyTickContext, direction: StrategyDirection, module: S10MakerModule): number {
  const tail = getCurrentTail(ctx);
  if (!tail.direction || tail.direction === direction || module === "terminal") return MAKER_MAX_SHARES_PER_SIDE;
  const baseRatio = module === "balance" ? 0.65 : module === "conviction" ? 0.55 : 0.5;
  const tailBoost = tail.shares >= 20 ? 0.12 : tail.shares >= 12 ? 0.06 : 0;
  const targetCoverRatio = clamp(baseRatio + tailBoost, 0.45, 0.78);
  return clamp(tail.shares * targetCoverRatio, MAKER_MIN_QUOTE_SHARES, MAKER_MAX_INSURANCE_SHARES);
}

function isMakerInsuranceQuote(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  module: S10MakerModule,
  projection: InventoryProjection,
): boolean {
  if (module === "terminal") return false;
  const tail = getCurrentTail(ctx);
  if (!tail.direction || tail.direction === direction || tail.shares < MAKER_MIN_INSURANCE_TAIL_SHARES) return false;
  const pairProfitPct = projection.pairedProfitPct;
  if (pairProfitPct == null || pairProfitPct < MAKER_MIN_HEDGE_PAIR_PROFIT_PCT) return false;
  if (projection.inventoryEvRoiPct != null && projection.inventoryEvRoiPct < -0.5) return false;
  return true;
}

interface MakerTerminalSignal {
  direction: StrategyDirection;
  confidencePct: number;
  minConfidencePct: number;
  edgePct: number;
  maxBidPct: number;
  maxShares: number;
  style: "chase" | "panic_probe";
  reason: string;
}

function getMakerTerminalSignal(
  ctx: StrategyTickContext,
  fairUp: number,
  upQuote: QuoteProxy,
  downQuote: QuoteProxy,
): MakerTerminalSignal | null {
  if (ctx.diff == null || ctx.rem > MAKER_TERMINAL_START_REM || ctx.rem < MAKER_MIN_REMAINING) return null;
  const absDiff = Math.abs(ctx.diff);
  if (absDiff < getTerminalMinDiff(ctx.rem)) return null;
  const direction: StrategyDirection = ctx.diff >= 0 ? "up" : "down";
  const confidencePct = direction === "up" ? fairUp : 100 - fairUp;
  const currentBidPct = direction === "up" ? upQuote.sellValuePct : downQuote.sellValuePct;
  const minConfidencePct = getTerminalMinConfidencePct(ctx.rem, currentBidPct);
  if (confidencePct < minConfidencePct) return null;
  const edgePct = getMakerFairEdgePct("terminal", ctx.rem);
  const maxBidPct = Math.min(getMakerMaxPricePct("terminal"), confidencePct - edgePct);
  const style = currentBidPct >= MAKER_TERMINAL_CHASE_MIN_BID_PCT ? "chase" : "panic_probe";
  const maxShares = style === "panic_probe" ? MAKER_TERMINAL_PANIC_MAX_SHARES : MAKER_MAX_TERMINAL_TAIL_SHARES;
  return {
    direction,
    confidencePct,
    minConfidencePct,
    edgePct,
    maxBidPct,
    maxShares,
    style,
    reason: `${style} ${direction} conf=${confidencePct.toFixed(1)}% min=${minConfidencePct.toFixed(1)}% diff=${ctx.diff.toFixed(0)} rem=${ctx.rem.toFixed(1)}s`,
  };
}

interface MakerInventoryAdjustment {
  overBy: number;
  underBy: number;
  canQuote: boolean;
  maxShares: number;
  sizeMultiplier: number;
  extraEdgePct: number;
  tag: string;
}

interface InventoryProjection {
  pairedShares: number;
  pairedProfitPct: number | null;
  tailDirection: StrategyDirection | null;
  tailShares: number;
  tailEvPct: number | null;
  inventoryEvUsd: number | null;
  inventoryEvRoiPct: number | null;
}

function isWeakSideLockException(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  projection: InventoryProjection,
): boolean {
  const currentTail = getCurrentTail(ctx);
  if (!currentTail.direction || currentTail.direction === direction) return false;
  if (projection.tailDirection === direction) return false;
  if (projection.tailShares >= currentTail.shares - 0.0001) return false;
  return (
    projection.pairedProfitPct != null &&
    projection.pairedProfitPct >= MAKER_WEAK_SIDE_LOCK_MIN_PAIR_PROFIT_PCT
  );
}

function isBalancedRepairQuote(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  module: S10MakerModule,
  projection: InventoryProjection,
): boolean {
  if (module === "terminal") return false;
  const currentTail = getCurrentTail(ctx);
  if (!currentTail.direction || currentTail.direction === direction) return false;
  if (currentTail.shares < MAKER_BALANCED_REPAIR_MIN_TAIL_SHARES) return false;
  if (projection.tailDirection === direction) return false;
  if (projection.tailShares >= currentTail.shares - 0.0001) return false;
  if (
    projection.pairedProfitPct == null ||
    projection.pairedProfitPct < getMakerBalancedRepairPairFloorPct(module)
  ) {
    return false;
  }
  if (
    projection.inventoryEvRoiPct != null &&
    projection.inventoryEvRoiPct < MAKER_BALANCED_REPAIR_INV_ROI_FLOOR_PCT
  ) {
    return false;
  }
  return true;
}

function getMakerShares(
  price: number,
  rem: number,
  sizeMultiplier = 1,
  maxShares = MAKER_MAX_SHARES_PER_SIDE,
  module: S10MakerModule = getMakerModule(rem),
): number {
  const moduleBoost =
    module === "terminal" ? (price >= 0.8 ? 1.75 : 0.45) :
    module === "conviction" ? 0.95 :
    module === "balance" ? 0.75 :
    module === "seed" ? 0.45 :
    0;
  const remBoost = rem <= 45 ? 1.1 : rem <= 120 ? 1.0 : 0.9;
  const notional = MAKER_BASE_NOTIONAL * moduleBoost * remBoost * sizeMultiplier;
  return Math.round(clamp(notional / Math.max(price, 0.03), MAKER_MIN_QUOTE_SHARES, Math.max(MAKER_MIN_QUOTE_SHARES, maxShares)) * 10000) / 10000;
}

function getMakerInventoryAdjustment(ctx: StrategyTickContext, direction: StrategyDirection): MakerInventoryAdjustment {
  const ownSize = getPositionSize(ctx, direction);
  const otherSize = getPositionSize(ctx, opposite(direction));
  const overBy = Math.max(0, ownSize - otherSize);
  const underBy = Math.max(0, otherSize - ownSize);

  if (overBy >= MAKER_HARD_IMBALANCE_SHARES) {
    return {
      overBy,
      underBy,
      canQuote: false,
      maxShares: 0,
      sizeMultiplier: 0,
      extraEdgePct: 999,
      tag: `hard-over ${overBy.toFixed(1)}`,
    };
  }

  if (overBy >= MAKER_SOFT_IMBALANCE_SHARES) {
    return {
      overBy,
      underBy,
      canQuote: false,
      maxShares: 0,
      sizeMultiplier: 0,
      extraEdgePct: 999,
      tag: `soft-over ${overBy.toFixed(1)}`,
    };
  }

  if (underBy >= MAKER_SOFT_IMBALANCE_SHARES) {
    return {
      overBy,
      underBy,
      canQuote: true,
      maxShares: Math.min(MAKER_MAX_SHARES_PER_SIDE * MAKER_REBALANCE_SIZE_MULT, underBy + MAKER_SOFT_IMBALANCE_SHARES),
      sizeMultiplier: MAKER_REBALANCE_SIZE_MULT,
      extraEdgePct: 0,
      tag: `rebalance ${underBy.toFixed(1)}`,
    };
  }

  return {
    overBy,
    underBy,
    canQuote: true,
    maxShares: MAKER_MAX_SHARES_PER_SIDE,
    sizeMultiplier: 1,
    extraEdgePct: 0,
    tag: "balanced",
  };
}

function getEntryAmount(opportunity: Omit<Opportunity, "amount" | "score">, profile: PhaseProfile): number {
  const edgeBoost = clamp((opportunity.edgePct - profile.minEdgePct) * 1.5, 0, 9);
  const fairBoost = clamp((opportunity.fairPct - profile.minFairPct) * 0.08, 0, 5);
  const pricePenalty = opportunity.costPct > 88 || opportunity.costPct < 8 ? -3 : 0;
  const raw = profile.baseAmount + edgeBoost + fairBoost + pricePenalty;
  return Math.round(clamp(raw, MIN_NOTIONAL, MAX_NOTIONAL) * 100) / 100;
}

function fmtPct(value: number | null): string {
  return value == null || !Number.isFinite(value) ? "-" : `${value.toFixed(2)}%`;
}

function round4(value: number): number {
  return Math.round(value * 10000) / 10000;
}

function avgCostAfterFill(
  currentSize: number,
  currentCostPct: number | null,
  fillShares: number,
  fillCostPct: number,
): number | null {
  if (!(fillShares > 0)) return currentCostPct;
  if (!(currentSize > 0.0001) || currentCostPct == null || !Number.isFinite(currentCostPct)) {
    return fillCostPct;
  }
  return ((currentSize * currentCostPct) + (fillShares * fillCostPct)) / (currentSize + fillShares);
}

function projectInventoryAfterFill(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  pricePct: number,
  shares: number,
  fairUp: number,
): InventoryProjection {
  const upSize = Math.max(0, ctx.position.upSize || 0);
  const downSize = Math.max(0, ctx.position.downSize || 0);
  const upCostPct = getPositionCostPct(ctx, "up");
  const downCostPct = getPositionCostPct(ctx, "down");
  const projectedUpSize = direction === "up" ? upSize + shares : upSize;
  const projectedDownSize = direction === "down" ? downSize + shares : downSize;
  const projectedUpCostPct = direction === "up"
    ? avgCostAfterFill(upSize, upCostPct, shares, pricePct)
    : upCostPct;
  const projectedDownCostPct = direction === "down"
    ? avgCostAfterFill(downSize, downCostPct, shares, pricePct)
    : downCostPct;
  const pairedShares = Math.min(projectedUpSize, projectedDownSize);
  const pairedProfitPct = pairedShares > 0.0001 && projectedUpCostPct != null && projectedDownCostPct != null
    ? 100 - projectedUpCostPct - projectedDownCostPct
    : null;
  const imbalance = projectedUpSize - projectedDownSize;
  const tailDirection: StrategyDirection | null = Math.abs(imbalance) > 0.0001
    ? (imbalance > 0 ? "up" : "down")
    : null;
  const tailShares = Math.abs(imbalance);
  const tailAvgCostPct = tailDirection === "up"
    ? projectedUpCostPct
    : tailDirection === "down"
      ? projectedDownCostPct
      : null;
  const tailFairPct = tailDirection === "up" ? fairUp : tailDirection === "down" ? 100 - fairUp : null;
  const tailEvPct = tailFairPct != null && tailAvgCostPct != null ? tailFairPct - tailAvgCostPct : null;
  const pairedEvUsd = pairedShares > 0 && pairedProfitPct != null ? pairedShares * pairedProfitPct / 100 : 0;
  const tailEvUsd = tailShares > 0 && tailEvPct != null ? tailShares * tailEvPct / 100 : 0;
  const costKnown =
    (!(projectedUpSize > 0.0001) || projectedUpCostPct != null) &&
    (!(projectedDownSize > 0.0001) || projectedDownCostPct != null);
  const inventoryCostUsd = costKnown
    ? (projectedUpSize > 0 && projectedUpCostPct != null ? projectedUpSize * projectedUpCostPct / 100 : 0) +
      (projectedDownSize > 0 && projectedDownCostPct != null ? projectedDownSize * projectedDownCostPct / 100 : 0)
    : 0;
  const inventoryEvUsd = pairedShares > 0 || tailShares > 0 ? pairedEvUsd + tailEvUsd : null;
  return {
    pairedShares,
    pairedProfitPct,
    tailDirection,
    tailShares,
    tailEvPct,
    inventoryEvUsd,
    inventoryEvRoiPct: inventoryEvUsd != null && inventoryCostUsd > 0
      ? (inventoryEvUsd / inventoryCostUsd) * 100
      : null,
  };
}

function formatProjection(projection: InventoryProjection): string {
  const pair = projection.pairedProfitPct == null ? "pair=-" : `pair=${projection.pairedProfitPct.toFixed(1)}%`;
  const tail = projection.tailDirection && projection.tailEvPct != null
    ? `tail=${projection.tailDirection}:${projection.tailEvPct.toFixed(1)}%/${projection.tailShares.toFixed(1)}`
    : "tail=flat";
  const inv = projection.inventoryEvRoiPct == null ? "invEV=-" : `invEV=${projection.inventoryEvRoiPct.toFixed(1)}%`;
  return `${pair} ${tail} ${inv}`;
}

function getMakerProjectionBlock(
  ctx: StrategyTickContext,
  direction: StrategyDirection,
  pricePct: number,
  shares: number,
  fairUp: number,
  module: S10MakerModule,
): { block: string | null; projection: InventoryProjection; summary: string } {
  const current = projectInventoryAfterFill(ctx, direction, pricePct, 0, fairUp);
  const projection = projectInventoryAfterFill(ctx, direction, pricePct, shares, fairUp);
  const directionFairPct = direction === "up" ? fairUp : 100 - fairUp;
  const currentTailShares = Math.abs((ctx.position.upSize || 0) - (ctx.position.downSize || 0));
  const quoteAddsTail = projection.tailDirection === direction && projection.tailShares > currentTailShares + 0.0001;
  const reducesTail = projection.tailShares < current.tailShares - 0.0001;
  const improvesInventoryRoi =
    current.inventoryEvRoiPct != null &&
    projection.inventoryEvRoiPct != null &&
    projection.inventoryEvRoiPct > current.inventoryEvRoiPct + 0.5;
  const improvesPair =
    current.pairedProfitPct != null &&
    projection.pairedProfitPct != null &&
    projection.pairedProfitPct > current.pairedProfitPct + 0.35;
  const insuranceReducing = isMakerInsuranceQuote(ctx, direction, module, projection);
  const pairSafeForHedge =
    projection.pairedProfitPct == null ||
    projection.pairedProfitPct >= MAKER_MIN_HEDGE_PAIR_PROFIT_PCT;
  const balancedRepair = isBalancedRepairQuote(ctx, direction, module, projection);
  const riskReducing =
    (reducesTail &&
      (improvesInventoryRoi || improvesPair) &&
      pairSafeForHedge) ||
    insuranceReducing ||
    balancedRepair;
  const maxTailShares = getMakerMaxTailShares(module) || MAKER_MAX_PROJECTED_TAIL_SHARES;
  const maxPairLossPct = getMakerMaxPairLossPct(module);
  const pairFloorPct = balancedRepair
    ? getMakerBalancedRepairPairFloorPct(module)
    : module === "terminal"
      ? -maxPairLossPct
      : 0;
  const softPairLossPct = getMakerSoftPairLossPct(module);
  const minTailEvForPairLossPct = getMakerTailEvForPairLossPct(module);
  const terminalChase =
    module === "terminal" &&
    quoteAddsTail &&
    directionFairPct >= getTerminalMinConfidencePct(ctx.rem, pricePct) &&
    projection.tailEvPct != null &&
    projection.tailEvPct >= MAKER_MIN_TERMINAL_TAIL_EV_PCT;
  const summary = `${module} ${formatProjection(projection)}${terminalChase ? " terminal_chase" : insuranceReducing ? " insurance" : balancedRepair ? " balanced_repair" : riskReducing ? " repair" : ""}`;

  if (
    quoteAddsTail &&
    directionFairPct <= MAKER_LOW_FAIR_TAIL_BLOCK_PCT &&
    projection.tailShares > MAKER_MAX_LOW_FAIR_TAIL_SHARES
  ) {
    return { block: `low-fair tail ${directionFairPct.toFixed(1)}%/${projection.tailShares.toFixed(1)}`, projection, summary };
  }

  if (
    quoteAddsTail &&
    projection.tailEvPct != null &&
    projection.tailEvPct < 0 &&
    projection.tailShares > MAKER_MAX_LOW_FAIR_TAIL_SHARES
  ) {
    return { block: `negative tail ${fmtPct(projection.tailEvPct)}/${projection.tailShares.toFixed(1)}`, projection, summary };
  }

  if (
    !terminalChase &&
    !balancedRepair &&
    reducesTail &&
    projection.pairedProfitPct != null &&
    projection.pairedProfitPct < MAKER_MIN_HEDGE_PAIR_PROFIT_PCT
  ) {
    return { block: `hedge pair ${projection.pairedProfitPct.toFixed(1)}%`, projection, summary };
  }

  if (
    !terminalChase &&
    projection.pairedProfitPct != null &&
    projection.pairedProfitPct < pairFloorPct
  ) {
    return { block: `pair floor ${projection.pairedProfitPct.toFixed(1)}%`, projection, summary };
  }

  if (
    !terminalChase &&
    projection.pairedProfitPct != null &&
    projection.pairedProfitPct < -softPairLossPct &&
    (projection.tailEvPct == null || projection.tailEvPct < minTailEvForPairLossPct) &&
    !riskReducing
  ) {
    return { block: `pair needs tailEV ${projection.pairedProfitPct.toFixed(1)}%/${fmtPct(projection.tailEvPct)}`, projection, summary };
  }

  if (
    !terminalChase &&
    projection.inventoryEvRoiPct != null &&
    projection.inventoryEvRoiPct < MAKER_MIN_PROJECTED_INV_EV_ROI_PCT &&
    !riskReducing
  ) {
    return { block: `invEV low ${projection.inventoryEvRoiPct.toFixed(1)}%`, projection, summary };
  }

  if (
    quoteAddsTail &&
    projection.tailShares > maxTailShares &&
    !riskReducing
  ) {
    return { block: `${module} tail cap ${projection.tailShares.toFixed(1)}/${maxTailShares}`, projection, summary };
  }

  if (
    ctx.rem <= 20 &&
    quoteAddsTail &&
    (projection.tailEvPct == null || projection.tailEvPct < MAKER_MIN_TERMINAL_TAIL_EV_PCT)
  ) {
    return { block: `terminal tailEV ${fmtPct(projection.tailEvPct)}`, projection, summary };
  }

  return { block: null, projection, summary };
}

export class S10FullSetArb implements IStrategy {
  readonly key: StrategyKey = "s10";
  readonly number: StrategyNumber = 10;
  readonly name = "双边库存";

  private s: S10State = createState();
  private candidate: Opportunity | null = null;
  private firstSeenAt = 0;
  private lastSeenAt = 0;

  private updateInventoryFormula(ctx: StrategyTickContext): void {
    const upSize = Math.max(0, ctx.position.upSize || 0);
    const downSize = Math.max(0, ctx.position.downSize || 0);
    const upCostPct = getPositionCostPct(ctx, "up");
    const downCostPct = getPositionCostPct(ctx, "down");
    const pairedShares = Math.min(upSize, downSize);
    const pairedAvgCostPct = pairedShares > 0.0001 && upCostPct != null && downCostPct != null
      ? upCostPct + downCostPct
      : null;
    const pairedProfitPct = pairedAvgCostPct != null ? 100 - pairedAvgCostPct : null;
    const imbalance = upSize - downSize;
    const tailDirection: StrategyDirection | null = Math.abs(imbalance) > 0.0001
      ? (imbalance > 0 ? "up" : "down")
      : null;
    const tailShares = Math.abs(imbalance);
    const tailAvgCostPct = tailDirection ? getPositionCostPct(ctx, tailDirection) : null;
    const fairUp = ctx.diff == null ? null : getFairProb(ctx.diff, ctx.rem);
    const tailFairPct = fairUp == null || !tailDirection
      ? null
      : tailDirection === "up" ? fairUp : 100 - fairUp;
    const tailEvPct = tailFairPct != null && tailAvgCostPct != null
      ? tailFairPct - tailAvgCostPct
      : null;
    const pairedEvUsd = pairedShares > 0 && pairedProfitPct != null ? pairedShares * pairedProfitPct / 100 : 0;
    const tailEvUsd = tailShares > 0 && tailEvPct != null ? tailShares * tailEvPct / 100 : 0;
    const inventoryCostUsd =
      (upSize > 0 && upCostPct != null ? upSize * upCostPct / 100 : 0) +
      (downSize > 0 && downCostPct != null ? downSize * downCostPct / 100 : 0);
    const inventoryEvUsd = pairedShares > 0 || tailShares > 0 ? pairedEvUsd + tailEvUsd : null;
    this.s.inventoryUpSize = round4(upSize);
    this.s.inventoryDownSize = round4(downSize);
    this.s.inventoryImbalance = round4(imbalance);
    this.s.pairedShares = round4(pairedShares);
    this.s.pairedAvgCostPct = pairedAvgCostPct != null ? round4(pairedAvgCostPct) : null;
    this.s.pairedProfitPct = pairedProfitPct != null ? round4(pairedProfitPct) : null;
    this.s.tailDirection = tailDirection;
    this.s.tailShares = round4(tailShares);
    this.s.tailAvgCostPct = tailAvgCostPct != null ? round4(tailAvgCostPct) : null;
    this.s.tailFairPct = tailFairPct != null ? round4(tailFairPct) : null;
    this.s.tailEvPct = tailEvPct != null ? round4(tailEvPct) : null;
    this.s.inventoryEvUsd = inventoryEvUsd != null ? round4(inventoryEvUsd) : null;
    this.s.inventoryEvRoiPct = inventoryEvUsd != null && inventoryCostUsd > 0
      ? round4((inventoryEvUsd / inventoryCostUsd) * 100)
      : null;
  }

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "S10 · 结算EV库存策略",
      lines: [
        { text: "高覆盖率：目标是多数 5m 盘口只要存在正 EV 就建一笔库存，而不是等待极端折价。", color: "#f0a500" },
        { text: "核心公式：adjustedEV = fairProb(direction) - buyCost(direction) - spread/latency/vol buffer。" },
        { text: "分阶段入场：opening 保守，inventory 主动，conviction/terminal 降低边际但提高方向确定性。", color: "#58a6ff" },
        { text: "默认持有到结算；盘中不因短期浮盈卖出，避免被假盘口回撤甩下车。", color: "#3fb950", marginTop: true },
        { text: "仍然不是无风险策略：只有模拟盘长期样本证明后，才考虑真实盘小额灰度。", color: "#f85149" },
      ],
    };
  }

  updateGuards(ctx: StrategyTickContext): void {
    this.updateInventoryFormula(ctx);
  }

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    this.s.decision = "scan";
    this.s.entryBlockedReason = "";
    this.s.lockReason = "";

    if (ctx.rem < WINDOW_MIN_REMAINING || ctx.rem > WINDOW_MAX_REMAINING) {
      this.resetCandidate();
      this.s.entryBlockedReason = ctx.rem < WINDOW_MIN_REMAINING
        ? `too late rem=${ctx.rem.toFixed(1)}s`
        : `waiting rem=${ctx.rem.toFixed(1)}s`;
      return null;
    }

    const fullSetEntry = this.findFullSetEntry(ctx);
    if (fullSetEntry) return fullSetEntry;

    const opportunity = this.findOpportunity(ctx);
    this.recordOpportunity(opportunity);

    if (!opportunity) {
      this.resetCandidate();
      return null;
    }

    const sameDirection = this.candidate?.direction === opportunity.direction;
    const now = ctx.now;
    if (!sameDirection || !this.firstSeenAt || now - this.lastSeenAt > MAX_SIGNAL_GAP_MS) {
      this.candidate = opportunity;
      this.firstSeenAt = now;
      this.s.candidateTicks = 1;
    } else {
      this.candidate = opportunity;
      this.s.candidateTicks += 1;
    }
    this.lastSeenAt = now;
    this.s.candidateDirection = opportunity.direction;
    this.s.candidateConfirmMs = now - this.firstSeenAt;

    if (this.s.candidateTicks < CONFIRM_TICKS || this.s.candidateConfirmMs < CONFIRM_MS) {
      this.s.entryBlockedReason = `confirming ${this.s.candidateTicks}/${CONFIRM_TICKS}`;
      return null;
    }

    this.s.decision = "enter";
    return {
      direction: opportunity.direction,
      amount: opportunity.amount,
      reason: `s10-${opportunity.phase} adjEV=${fmtPct(opportunity.edgePct)} raw=${fmtPct(opportunity.rawEdgePct)} fair=${fmtPct(opportunity.fairPct)} cost=${fmtPct(opportunity.costPct)} amount=${opportunity.amount}`,
    };
  }

  onEntryFilled(ctx: StrategyTickContext, direction: StrategyDirection): void {
    this.s.decision = "hold";
    this.s.entryTs = ctx.now;
    this.s.entryDirection = direction;
    const positionCostPct = getPositionCostPct(ctx, direction);
    const positionSize = getPositionSize(ctx, direction);
    const fallbackCostPct = this.s.costPct;
    const costPct = positionCostPct ?? fallbackCostPct;
    this.s.entryCostPct = costPct;
    this.s.firstLegAvgCostPct = costPct;
    this.s.firstLegSharesEstimate = positionSize > 0 ? positionSize : this.s.firstLegSharesEstimate;
    this.s.firstLegCostUsd = costPct != null && this.s.firstLegSharesEstimate > 0
      ? (this.s.firstLegSharesEstimate * costPct) / 100
      : this.s.firstLegCostUsd;
    this.s.lastScaleInAt = ctx.now;
    this.s.scaleInCount += 1;
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
    if (this.s.locked) return null;
    const lock = this.findLockOpportunity(ctx, direction);
    this.recordLockOpportunity(lock);
    if (!lock) return null;
    const heldMs = this.s.entryTs > 0 ? ctx.now - this.s.entryTs : LOCK_MIN_HOLD_MS;
    if (heldMs < LOCK_MIN_HOLD_MS || ctx.rem < LOCK_MIN_REMAINING) return null;

    if (lock.ready) {
      this.s.decision = "lockProfit";
      this.s.lockReason = lock.reason;
      return { signal: "lock", reason: lock.reason };
    }
    if (lock.defensive) {
      this.s.decision = "lockDefensive";
      this.s.lockReason = lock.reason;
      return { signal: "lock", reason: lock.reason };
    }
    return null;
  }

  checkScaleIn(ctx: StrategyTickContext, direction: StrategyDirection, currentPosition: number): EntrySignal | null {
    this.s.decision = "hold";
    const lock = this.findLockOpportunity(ctx, direction);
    this.recordLockOpportunity(lock);
    if (lock?.ready || lock?.defensive) {
      this.s.entryBlockedReason = `lock first ${fmtPct(lock.lockNetProfitPct)}`;
      return null;
    }
    if (ctx.rem < SCALE_IN_MIN_REMAINING) {
      this.s.entryBlockedReason = `scale wait rem=${ctx.rem.toFixed(1)}s`;
      return null;
    }
    if (this.s.lastScaleInAt > 0 && ctx.now - this.s.lastScaleInAt < SCALE_IN_MIN_GAP_MS) {
      this.s.entryBlockedReason = "scale cooldown";
      return null;
    }

    const opportunity = this.findOpportunity(ctx);
    this.recordOpportunity(opportunity);
    if (!opportunity) return null;
    if (opportunity.direction !== direction) {
      this.s.entryBlockedReason = `scale direction mismatch ${opportunity.direction}`;
      return null;
    }
    if (opportunity.edgePct < opportunity.bufferPct * 0.25 + SCALE_IN_EDGE_ADDON_PCT) {
      this.s.entryBlockedReason = `scale edge thin ${fmtPct(opportunity.edgePct)}`;
      return null;
    }

    const currentCostPct = getPositionCostPct(ctx, direction) ?? this.s.entryCostPct ?? opportunity.costPct;
    const estimatedNotional = currentPosition * currentCostPct / 100;
    this.s.estimatedWindowNotional = estimatedNotional;
    const remainingBudget = MAX_WINDOW_NOTIONAL_ESTIMATE - estimatedNotional;
    if (remainingBudget < MIN_NOTIONAL) {
      this.s.entryBlockedReason = `window cap ${estimatedNotional.toFixed(1)}`;
      return null;
    }

    const amount = Math.round(Math.min(opportunity.amount, remainingBudget) * 100) / 100;
    if (amount < MIN_NOTIONAL) {
      this.s.entryBlockedReason = `scale amount ${amount.toFixed(2)}`;
      return null;
    }
    this.s.amount = amount;
    this.s.decision = "enter";
    return {
      direction,
      amount,
      reason: `s10-scale ${opportunity.phase} adjEV=${fmtPct(opportunity.edgePct)} notional=${estimatedNotional.toFixed(1)}`,
    };
  }

  getMakerQuotes(ctx: StrategyTickContext): MakerQuoteSignal[] {
    const fairUp = ctx.diff == null ? null : getFairProb(ctx.diff, ctx.rem);
    const upQuote = getQuoteProxy(ctx, "up");
    const downQuote = getQuoteProxy(ctx, "down");
    const module = getMakerModule(ctx.rem);
    const targetEdgePct = getMakerTargetSetEdgePct(ctx.rem);
    this.s.makerTargetEdgePct = targetEdgePct;
    this.s.makerModule = module;
    this.s.phase = makerModuleToPhase(module);

    if (
      fairUp == null ||
      !upQuote ||
      !downQuote ||
      module === "idle"
    ) {
      this.s.makerMode = "idle";
      this.s.makerLastReason = fairUp == null ? "maker waiting fair" : `maker rem ${ctx.rem.toFixed(1)}s`;
      this.s.makerUpBidPct = null;
      this.s.makerDownBidPct = null;
      this.s.makerTotalBidCostPct = null;
      this.s.makerRiskReason = "";
      this.s.makerTerminalDirection = null;
      this.s.makerTerminalConfidencePct = null;
      this.s.makerTerminalEdgePct = null;
      this.s.makerTerminalMaxPricePct = null;
      this.s.makerTerminalReason = "";
      this.s.makerProjectedEvRoiPct = null;
      this.s.makerProjectedPairPct = null;
      this.s.makerProjectedTailEvPct = null;
      this.s.makerProjectedTailShares = 0;
      return [];
    }

    const fairDown = 100 - fairUp;
    const terminalSignal = module === "terminal" ? getMakerTerminalSignal(ctx, fairUp, upQuote, downQuote) : null;
    const fairEdge = terminalSignal?.edgePct ?? getMakerFairEdgePct(module, ctx.rem);
    const rebalanceEdge = getMakerRebalanceEdgePct(ctx.rem);
    const minPct = MAKER_MIN_PRICE * 100;
    const maxPct = getMakerMaxPricePct(module);
    const maxSetCostPct = 100 - targetEdgePct;
    const rebalancingUp = ctx.position.downSize > ctx.position.upSize + 1;
    const rebalancingDown = ctx.position.upSize > ctx.position.downSize + 1;

    this.s.makerTerminalDirection = terminalSignal?.direction ?? null;
    this.s.makerTerminalConfidencePct = terminalSignal ? round4(terminalSignal.confidencePct) : null;
    this.s.makerTerminalEdgePct = terminalSignal ? round4(terminalSignal.edgePct) : null;
    this.s.makerTerminalMaxPricePct = terminalSignal ? round4(terminalSignal.maxBidPct) : null;
    this.s.makerTerminalReason = terminalSignal?.reason ?? (module === "terminal" ? "terminal waiting high confidence" : "");

    let upBidPct = module === "terminal" ? 0 : Math.min(upQuote.sellValuePct, fairUp - fairEdge, maxPct);
    let downBidPct = module === "terminal" ? 0 : Math.min(downQuote.sellValuePct, fairDown - fairEdge, maxPct);

    if (module !== "terminal" && ctx.position.downCostPct != null && rebalancingUp) {
      upBidPct = Math.min(upQuote.sellValuePct, fairUp - rebalanceEdge, maxPct);
    }
    if (module !== "terminal" && ctx.position.upCostPct != null && rebalancingDown) {
      downBidPct = Math.min(downQuote.sellValuePct, fairDown - rebalanceEdge, maxPct);
    }

    if (module !== "terminal" && upBidPct + downBidPct > maxSetCostPct) {
      const excess = upBidPct + downBidPct - maxSetCostPct;
      const upEdge = fairUp - upBidPct;
      const downEdge = fairDown - downBidPct;
      if (rebalancingUp) downBidPct -= excess;
      else if (rebalancingDown) upBidPct -= excess;
      else if (upEdge < downEdge) upBidPct -= excess;
      else downBidPct -= excess;
    }

    if (module === "terminal" && terminalSignal) {
      if (terminalSignal.direction === "up") {
        upBidPct = Math.min(upQuote.sellValuePct, terminalSignal.maxBidPct, maxPct);
      } else {
        downBidPct = Math.min(downQuote.sellValuePct, terminalSignal.maxBidPct, maxPct);
      }
    }

    upBidPct = Math.floor(clamp(upBidPct, 0, maxPct) * 100) / 100;
    downBidPct = Math.floor(clamp(downBidPct, 0, maxPct) * 100) / 100;
    const totalBidCostPct = upBidPct + downBidPct;
    this.s.makerUpBidPct = upBidPct >= minPct ? upBidPct : null;
    this.s.makerDownBidPct = downBidPct >= minPct ? downBidPct : null;
    this.s.makerTotalBidCostPct = totalBidCostPct > 0 ? totalBidCostPct : null;

    const quotes: MakerQuoteSignal[] = [];
    const blockedReasons: string[] = [];
    let lastProjection: InventoryProjection | null = null;
    let lastAllowedProjection: InventoryProjection | null = null;

    const addQuote = (
      direction: StrategyDirection,
      bidPct: number,
      fairPct: number,
      currentBidPct: number,
      options: { maxShares?: number; edgePct?: number; reasonTag?: string } = {},
    ) => {
      const inventory = getMakerInventoryAdjustment(ctx, direction);
      if (!inventory.canQuote) {
        blockedReasons.push(`${direction}:${inventory.tag}`);
        return;
      }
      const edgePct = fairPct - bidPct;
      if (bidPct < minPct || bidPct > maxPct) {
        blockedReasons.push(`${direction}:price ${bidPct.toFixed(2)}`);
        return;
      }
      if (bidPct > currentBidPct + 0.05) {
        blockedReasons.push(`${direction}:above bid`);
        return;
      }
      const requiredEdgePct = (options.edgePct ?? fairEdge) + inventory.extraEdgePct;
      const price = Math.round((bidPct / 100) * 10000) / 10000;
      const tailRoomShares = getMakerTailRoom(ctx, direction, module);
      const insuranceMaxShares = getMakerInsuranceMaxShares(ctx, direction, module);
      const maxShares = options.maxShares != null
        ? Math.min(options.maxShares, inventory.maxShares, tailRoomShares, insuranceMaxShares)
        : Math.min(inventory.maxShares, tailRoomShares, insuranceMaxShares);
      if (maxShares < MAKER_MIN_QUOTE_SHARES) {
        blockedReasons.push(`${direction}:room ${maxShares.toFixed(1)}`);
        return;
      }
      const shares = getMakerShares(price, ctx.rem, inventory.sizeMultiplier, maxShares, module);
      if (shares < MAKER_MIN_QUOTE_SHARES) {
        blockedReasons.push(`${direction}:size ${shares.toFixed(1)}`);
        return;
      }
      const guard = getMakerProjectionBlock(ctx, direction, bidPct, shares, fairUp, module);
      lastProjection = guard.projection;
      if (guard.block) {
        blockedReasons.push(`${direction}:${guard.block}`);
        return;
      }
      const insuranceQuote = isMakerInsuranceQuote(ctx, direction, module, guard.projection);
      const balancedRepair = isBalancedRepairQuote(ctx, direction, module, guard.projection);
      const weakSideBlock = getMakerWeakSideBlockReason(ctx, direction, fairPct, module);
      const weakSideLock = weakSideBlock ? isWeakSideLockException(ctx, direction, guard.projection) : false;
      if (weakSideBlock && !weakSideLock && !balancedRepair) {
        blockedReasons.push(`${direction}:${weakSideBlock}`);
        return;
      }
      const edgeFloorPct = insuranceQuote
        ? getMakerInsuranceEdgeFloorPct(module, guard.projection.pairedProfitPct)
        : balancedRepair
          ? getMakerBalancedRepairEdgeFloorPct(module)
        : weakSideLock
          ? getMakerInsuranceEdgeFloorPct(module, guard.projection.pairedProfitPct)
        : requiredEdgePct;
      if (edgePct < edgeFloorPct) {
        blockedReasons.push(`${direction}:edge ${edgePct.toFixed(1)}%${insuranceQuote ? " insurance" : balancedRepair ? " balanced" : weakSideLock ? " lock" : ""}`);
        return;
      }
      if (module !== "terminal" && !insuranceQuote && !balancedRepair && edgePct < 0.4 && totalBidCostPct > maxSetCostPct) {
        blockedReasons.push(`${direction}:set thin`);
        return;
      }
      lastAllowedProjection = guard.projection;
      quotes.push({
        direction,
        price,
        shares,
        ttlMs: getMakerQuoteTtlMs(module),
        reason: `maker-${module}-${direction} bid=${bidPct.toFixed(2)} fair=${fairPct.toFixed(1)} set=${totalBidCostPct.toFixed(1)} edge=${edgeFloorPct.toFixed(1)} inv=${insuranceQuote ? "insurance" : balancedRepair ? "balanced_repair" : weakSideLock ? "lock_hedge" : inventory.tag}${options.reasonTag ? ` ${options.reasonTag}` : ""} ${guard.summary}`,
      });
    };

    if (module === "terminal") {
      if (terminalSignal?.direction === "up") {
        addQuote("up", upBidPct, fairUp, upQuote.sellValuePct, {
          maxShares: terminalSignal.maxShares,
          edgePct: terminalSignal.edgePct,
          reasonTag: terminalSignal.reason,
        });
      } else if (terminalSignal?.direction === "down") {
        addQuote("down", downBidPct, fairDown, downQuote.sellValuePct, {
          maxShares: terminalSignal.maxShares,
          edgePct: terminalSignal.edgePct,
          reasonTag: terminalSignal.reason,
        });
      } else {
        blockedReasons.push("terminal:no high-confidence side");
      }
    } else {
      addQuote("up", upBidPct, fairUp, upQuote.sellValuePct);
      addQuote("down", downBidPct, fairDown, downQuote.sellValuePct);
    }

    const currentTail = getCurrentTail(ctx);
    quotes.sort((a, b) => {
      const priority = (quote: MakerQuoteSignal) => {
        if (currentTail.direction && quote.direction === opposite(currentTail.direction)) return 0;
        if (currentTail.direction && quote.direction === currentTail.direction) return 2;
        return 1;
      };
      return priority(a) - priority(b);
    });

    this.s.makerMode = quotes.length >= 2 ? "dual_quote" : quotes.length === 1 ? "single_quote" : "watch";
    const imbalance = ctx.position.upSize - ctx.position.downSize;
    const projectionForState = lastAllowedProjection ?? lastProjection;
    this.s.makerRiskReason = blockedReasons.slice(0, 3).join("; ");
    this.s.makerProjectedEvRoiPct = projectionForState?.inventoryEvRoiPct != null ? round4(projectionForState.inventoryEvRoiPct) : null;
    this.s.makerProjectedPairPct = projectionForState?.pairedProfitPct != null ? round4(projectionForState.pairedProfitPct) : null;
    this.s.makerProjectedTailEvPct = projectionForState?.tailEvPct != null ? round4(projectionForState.tailEvPct) : null;
    this.s.makerProjectedTailShares = projectionForState ? round4(projectionForState.tailShares) : 0;
    this.s.makerLastReason = quotes.length
      ? `maker ${module} quotes=${quotes.length} setBid=${totalBidCostPct.toFixed(1)}% targetEdge=${targetEdgePct.toFixed(1)}% inv=${imbalance.toFixed(1)}`
      : `maker ${module} no quote setBid=${totalBidCostPct.toFixed(1)}% inv=${imbalance.toFixed(1)} ${this.s.makerRiskReason || this.s.makerTerminalReason}`;
    return quotes;
  }

  onMakerStatus(_ctx: StrategyTickContext, status: MakerStatusSnapshot): void {
    this.s.decision = "idle";
    this.s.status = status.activeOrders > 0 ? "maker_active" : "maker_watch";
    this.s.makerActiveOrders = status.activeOrders;
    this.s.makerUpOrders = status.upOrders;
    this.s.makerDownOrders = status.downOrders;
    this.s.makerFilledCount = status.filledCount;
    this.s.makerMergedCount = status.mergedCount;
    const statusReason = status.lastReason || "";
    const preserveDecisionReason = /^locked full-set/.test(statusReason) && /^maker /.test(this.s.makerLastReason);
    this.s.makerLastReason = preserveDecisionReason ? this.s.makerLastReason : statusReason || this.s.makerLastReason;
    this.s.reason = this.s.makerLastReason;
    if (status.upBidPct != null) this.s.makerUpBidPct = status.upBidPct;
    if (status.downBidPct != null) this.s.makerDownBidPct = status.downBidPct;
    if (status.totalBidCostPct != null) this.s.makerTotalBidCostPct = status.totalBidCostPct;
    if (status.targetEdgePct != null) this.s.makerTargetEdgePct = status.targetEdgePct;
    if (status.lastFill) {
      this.s.makerLastFill = `${status.lastFill.direction} ${status.lastFill.shares.toFixed(2)} @ ${(status.lastFill.price * 100).toFixed(1)}% ${status.lastFill.trigger}`;
    }
  }

  resetState(): void {
    this.s = createState();
    this.resetCandidate();
  }

  getStatePayload(): Record<string, unknown> {
    return { ...this.s };
  }

  private findFullSetEntry(ctx: StrategyTickContext): EntrySignal | null {
    const arb = ctx.fullSetArb;
    if (!arb?.ready || arb.windowStart <= 0) return null;
    const direction = arb.firstLegDirection;
    if (!direction || !(arb.targetShares > 0) || !(arb.firstLegCost != null && arb.firstLegCost > 0)) {
      return null;
    }

    const firstLegAvgAsk = direction === "up" ? arb.upAvgAsk : arb.downAvgAsk;
    const secondLegAvgAsk = direction === "up" ? arb.downAvgAsk : arb.upAvgAsk;
    const amount = Math.round(arb.firstLegCost * 100) / 100;
    if (amount < MIN_NOTIONAL) return null;

    this.s.ready = true;
    this.s.status = "fullset";
    this.s.decision = "enter";
    this.s.phase = "inventory";
    this.s.reason = arb.reason || "full set ready";
    this.s.bestDirection = direction;
    this.s.firstLegDirection = direction;
    this.s.secondLegDirection = opposite(direction);
    this.s.firstLegCost = arb.firstLegCost;
    this.s.secondLegCost = arb.secondLegCost;
    this.s.targetShares = arb.targetShares;
    this.s.totalCost = arb.totalCost;
    this.s.totalCostPct = arb.totalCostPct;
    this.s.grossProfitPct = arb.grossProfitPct;
    this.s.netProfitPct = arb.netProfitPct;
    this.s.costPct = firstLegAvgAsk != null ? firstLegAvgAsk * 100 : null;
    this.s.oppositeCostPct = secondLegAvgAsk != null ? secondLegAvgAsk * 100 : null;
    this.s.edgePct = arb.netProfitPct;
    this.s.rawEdgePct = arb.grossProfitPct;
    this.s.bufferPct = arb.feeBufferPct;
    this.s.amount = amount;
    this.s.lockReason = "";

    return {
      direction,
      amount,
      reason: `s10-fullset ${arb.reason} shares=${arb.targetShares.toFixed(2)} first=${amount.toFixed(2)}`,
    };
  }

  private findOpportunity(ctx: StrategyTickContext): Opportunity | null {
    const fairUp = ctx.diff == null ? null : getFairProb(ctx.diff, ctx.rem);
    const volatilityBps = getRecentVolatilityBps(ctx.kline1m);
    const profile = getPhaseProfile(ctx.rem, volatilityBps);
    const spreadPct = getSpreadPct(ctx);
    const bufferPct = getExecutionBufferPct(spreadPct, volatilityBps, ctx.rem);
    this.s.volatilityBps = volatilityBps;
    this.s.phase = profile.phase;
    this.s.minEdgePct = profile.minEdgePct;
    this.s.minRawEdgePct = profile.minRawEdgePct;
    this.s.minFairPct = profile.minFairPct;
    this.s.minDirectionalDiff = profile.minDirectionalDiff;
    this.s.maxSpreadPct = profile.maxSpreadPct;
    this.s.spreadPct = spreadPct;
    this.s.bufferPct = bufferPct;

    if (fairUp == null || ctx.diff == null) {
      this.s.entryBlockedReason = "fair probability unavailable";
      return null;
    }
    if (spreadPct == null || spreadPct > profile.maxSpreadPct) {
      this.s.entryBlockedReason = `spread ${spreadPct == null ? "-" : (spreadPct * 100).toFixed(2)}%`;
      return null;
    }

    const candidates: Opportunity[] = [];
    for (const direction of ["up", "down"] as const) {
      const quote = getQuoteProxy(ctx, direction);
      const marketPct = getMarketPct(ctx, direction);
      if (!quote || marketPct == null) continue;

      const fairPct = direction === "up" ? fairUp : 100 - fairUp;
      const directionalDiff = direction === "up" ? ctx.diff : -ctx.diff;
      const rawEdgePct = fairPct - quote.buyCostPct;
      const edgePct = rawEdgePct - bufferPct;
      const roiPct = quote.buyCostPct > 0 ? (edgePct / quote.buyCostPct) * 100 : 0;
      const strongLateFallback = ctx.rem <= 90
        && fairPct >= Math.max(70, profile.minFairPct)
        && rawEdgePct >= Math.max(0.8, profile.minRawEdgePct - 0.7)
        && directionalDiff >= profile.minDirectionalDiff;
      if (fairPct < profile.minFairPct) continue;
      if (quote.buyCostPct < MIN_COST_PCT || quote.buyCostPct > MAX_COST_PCT) continue;
      if (rawEdgePct < profile.minRawEdgePct && !strongLateFallback) continue;
      if (edgePct < profile.minEdgePct && !strongLateFallback) continue;
      if (directionalDiff < profile.minDirectionalDiff && fairPct < profile.minFairPct + 8) continue;

      const baseOpportunity = {
        direction,
        phase: profile.phase,
        fairPct,
        costPct: quote.buyCostPct,
        sellValuePct: quote.sellValuePct,
        marketPct,
        rawEdgePct,
        bufferPct,
        edgePct,
        roiPct,
        directionalDiff,
      };

      candidates.push({
        ...baseOpportunity,
        amount: getEntryAmount(baseOpportunity, profile),
        score: edgePct + Math.min(10, directionalDiff / 18) + Math.max(0, fairPct - marketPct) * 0.18,
      });
    }

    if (!candidates.length) {
      this.s.entryBlockedReason = `no adjustedEV >=${profile.minEdgePct.toFixed(1)}%`;
      return null;
    }

    return candidates.sort((a, b) => b.score - a.score)[0];
  }

  private findLockOpportunity(ctx: StrategyTickContext, direction: StrategyDirection): LockOpportunity | null {
    const firstLegQuote = getQuoteProxy(ctx, direction);
    const oppositeDirection = opposite(direction);
    const oppositeQuote = getQuoteProxy(ctx, oppositeDirection);
    if (!firstLegQuote || !oppositeQuote) {
      this.s.lockReason = "";
      return null;
    }

    const positionSize = getPositionSize(ctx, direction);
    const firstLegAvgCostPct = getPositionCostPct(ctx, direction) ?? this.s.firstLegAvgCostPct ?? this.s.entryCostPct;
    if (!(positionSize > 0.01) || firstLegAvgCostPct == null || !Number.isFinite(firstLegAvgCostPct)) {
      return null;
    }

    const volatilityBps = getRecentVolatilityBps(ctx.kline1m);
    const spreadPct = getSpreadPct(ctx);
    const lockBufferPct = getLockBufferPct(spreadPct, volatilityBps, ctx.rem);
    const phase = this.s.phase === "none" ? getPhaseProfile(ctx.rem, volatilityBps).phase : this.s.phase;
    const minLockProfitPct = getMinLockProfitPct(ctx.rem, phase);
    const lockTotalCostPct = firstLegAvgCostPct + oppositeQuote.buyCostPct;
    const lockGrossProfitPct = 100 - lockTotalCostPct;
    const lockNetProfitPct = lockGrossProfitPct - lockBufferPct;
    const lockEquivalentSellPct = 100 - oppositeQuote.buyCostPct - lockBufferPct;
    const lockAdvantagePct = lockEquivalentSellPct - firstLegQuote.sellValuePct;
    const fairUp = ctx.diff == null ? null : getFairProb(ctx.diff, ctx.rem);
    const fairPct = fairUp == null ? null : direction === "up" ? fairUp : 100 - fairUp;
    const holdScorePct = fairPct == null ? null : fairPct - firstLegAvgCostPct - lockBufferPct;
    const fairBreakdown = fairPct != null && fairPct <= firstLegAvgCostPct - DEFENSIVE_FAIR_BREAKDOWN_PCT;
    const ready = lockNetProfitPct >= minLockProfitPct && lockAdvantagePct >= LOCK_ADVANTAGE_FLOOR_PCT;
    const defensive = !ready
      && (ctx.rem <= DEFENSIVE_LOCK_REM || fairBreakdown)
      && lockNetProfitPct >= DEFENSIVE_LOCK_MAX_LOSS_PCT
      && lockAdvantagePct >= LOCK_ADVANTAGE_FLOOR_PCT;

    const reason = ready
      ? `dual-lock net=${fmtPct(lockNetProfitPct)} total=${fmtPct(lockTotalCostPct)} opp=${fmtPct(oppositeQuote.buyCostPct)}`
      : defensive
        ? `defensive-lock net=${fmtPct(lockNetProfitPct)} fair=${fmtPct(fairPct)} opp=${fmtPct(oppositeQuote.buyCostPct)}`
        : `seek-lock net=${fmtPct(lockNetProfitPct)} min=${fmtPct(minLockProfitPct)} opp=${fmtPct(oppositeQuote.buyCostPct)}`;

    return {
      direction,
      oppositeDirection,
      firstLegAvgCostPct,
      firstLegSellValuePct: firstLegQuote.sellValuePct,
      oppositeCostPct: oppositeQuote.buyCostPct,
      lockTotalCostPct,
      lockGrossProfitPct,
      lockBufferPct,
      lockNetProfitPct,
      lockAdvantagePct,
      minLockProfitPct,
      fairPct,
      holdScorePct,
      targetSharesEstimate: positionSize,
      ready,
      defensive,
      reason,
    };
  }

  private recordLockOpportunity(lock: LockOpportunity | null): void {
    if (lock) {
      this.s.ready = lock.ready || lock.defensive || this.s.ready;
      this.s.status = lock.ready ? "lock_ready" : lock.defensive ? "defensive_lock" : "seeking_lock";
      this.s.reason = lock.reason;
    }
    this.s.oppositeCostPct = lock?.oppositeCostPct ?? null;
    this.s.lockTotalCostPct = lock?.lockTotalCostPct ?? null;
    this.s.lockGrossProfitPct = lock?.lockGrossProfitPct ?? null;
    this.s.lockNetProfitPct = lock?.lockNetProfitPct ?? null;
    this.s.lockAdvantagePct = lock?.lockAdvantagePct ?? null;
    this.s.lockBufferPct = lock?.lockBufferPct ?? null;
    this.s.lockMinProfitPct = lock?.minLockProfitPct ?? getMinLockProfitPct(999, "opening");
    this.s.lockTargetSharesEstimate = lock?.targetSharesEstimate ?? 0;
    this.s.firstLegAvgCostPct = lock?.firstLegAvgCostPct ?? this.s.firstLegAvgCostPct;
    this.s.sellValuePct = lock?.firstLegSellValuePct ?? this.s.sellValuePct;
    if (lock?.fairPct != null) this.s.fairPct = lock.fairPct;
    if (lock?.reason) this.s.lockReason = lock.ready || lock.defensive ? lock.reason : "";
  }

  private recordOpportunity(opportunity: Opportunity | null): void {
    this.s.ready = opportunity != null;
    this.s.status = opportunity ? "edge" : "scan";
    this.s.reason = opportunity
      ? `edge=${fmtPct(opportunity.edgePct)} fair=${fmtPct(opportunity.fairPct)} cost=${fmtPct(opportunity.costPct)}`
      : this.s.entryBlockedReason;
    this.s.bestDirection = opportunity?.direction ?? "none";
    this.s.fairPct = opportunity?.fairPct ?? null;
    this.s.costPct = opportunity?.costPct ?? null;
    this.s.edgePct = opportunity?.edgePct ?? null;
    this.s.roiPct = opportunity?.roiPct ?? null;
    this.s.marketPct = opportunity?.marketPct ?? null;
    this.s.sellValuePct = opportunity?.sellValuePct ?? null;
    this.s.rawEdgePct = opportunity?.rawEdgePct ?? null;
    this.s.bufferPct = opportunity?.bufferPct ?? this.s.bufferPct;
    this.s.amount = opportunity?.amount ?? null;
    this.s.totalCostPct = opportunity?.costPct ?? null;
    this.s.grossProfitPct = opportunity?.edgePct ?? null;
    this.s.netProfitPct = opportunity?.edgePct ?? null;
    this.s.firstLegDirection = opportunity?.direction ?? null;
    this.s.upAvgAsk = opportunity?.direction === "up" ? opportunity.costPct / 100 : null;
    this.s.downAvgAsk = opportunity?.direction === "down" ? opportunity.costPct / 100 : null;
  }

  private resetCandidate(): void {
    this.candidate = null;
    this.firstSeenAt = 0;
    this.lastSeenAt = 0;
    this.s.candidateDirection = "none";
    this.s.candidateTicks = 0;
    this.s.candidateConfirmMs = 0;
  }
}
