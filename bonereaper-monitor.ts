import { dirname } from "path";
import { existsSync, mkdirSync, readFileSync, renameSync, statSync, writeFileSync } from "fs";

type Outcome = "up" | "down" | "";
type ActivitySide = "BUY" | "SELL" | "";

export interface BonereaperMarketSample {
  ts: number;
  exchangeTs: number;
  windowStart: number;
  windowEnd: number;
  remSec: number;
  priceToBeat: number | null;
  currentPrice: number | null;
  diff: number | null;
  upBid: number | null;
  upAsk: number | null;
  upMid: number | null;
  downBid: number | null;
  downAsk: number | null;
  downMid: number | null;
  spreadPct: number | null;
  marketUpPct: number | null;
  fairUpPct: number | null;
  biasUpPct: number | null;
  macd1mTrend: string | null;
  macdFast1mTrend: string | null;
  bookAgeMs: number | null;
  bookSource: string | null;
}

interface RawActivity {
  proxyWallet?: string;
  timestamp?: number;
  conditionId?: string;
  type?: string;
  size?: number;
  usdcSize?: number;
  transactionHash?: string;
  price?: number;
  asset?: string;
  side?: string;
  outcome?: string;
  outcomeIndex?: number;
  title?: string;
  slug?: string;
  eventSlug?: string;
}

interface StoredActivity {
  key: string;
  seenAt: number;
  timestamp: number;
  type: string;
  side: ActivitySide;
  outcome: Outcome;
  size: number;
  usdcSize: number;
  price: number | null;
  transactionHash: string;
}

export interface BonereaperTradeEvent {
  timestamp: number;
  type: string;
  side: ActivitySide;
  outcome: Outcome;
  size: number;
  usdcSize: number;
  price: number | null;
  remSec: number | null;
  diff: number | null;
  upMid: number | null;
  downMid: number | null;
  fairUpPct: number | null;
  biasUpPct: number | null;
  macdFast1mTrend: string | null;
  marketLagMs: number | null;
  inventoryBeforeUp: number;
  inventoryBeforeDown: number;
  inventoryAfterUp: number;
  inventoryAfterDown: number;
  tailAfterDirection: Outcome;
  tailAfterShares: number;
  pairedAfterShares: number;
  pairedCostPctAfter: number | null;
  motiveTags: string[];
}

export interface BonereaperWindowSummary {
  conditionId: string;
  slug: string;
  title: string;
  windowStart: number;
  windowEnd: number;
  firstTs: number | null;
  lastTs: number | null;
  updatedAt: number;
  activityCount: number;
  tradeCount: number;
  buyCount: number;
  upBuyCount: number;
  downBuyCount: number;
  upBuyShares: number;
  downBuyShares: number;
  upBuyUsdc: number;
  downBuyUsdc: number;
  upAvgBuy: number | null;
  downAvgBuy: number | null;
  pairedShares: number;
  pairedCostPct: number | null;
  pairedEdgePct: number | null;
  pairedPnlUsd: number | null;
  tailDirection: Outcome;
  tailShares: number;
  tailAvgCostPct: number | null;
  totalBuyUsdc: number;
  totalBuyShares: number;
  redeemCount: number;
  redeemShares: number;
  redeemUsdc: number;
  sellUsdc: number;
  approxPnlUsd: number | null;
  approxRoiPct: number | null;
  resultDirection: Outcome;
  terminalBuyCount: number;
  terminalBuyUsdc: number;
  terminalBuyShares: number;
  terminalAvgPrice: number | null;
  terminalDirection: Outcome;
  terminalChase: boolean;
  firstBuyDirection: Outcome;
  firstBuyRemSec: number | null;
  firstBuyDiff: number | null;
  firstBuyPrice: number | null;
  firstOppositeTs: number | null;
  firstOppositeRemSec: number | null;
  firstOppositeDiff: number | null;
  firstOppositePrice: number | null;
  firstOppositeReason: string;
  minDiff: number | null;
  maxDiff: number | null;
  diffFlipped: boolean;
  trendFollowBuyCount: number;
  contrarianBuyCount: number;
  hedgeBuyCount: number;
  tailAddBuyCount: number;
  panicCatchBuyCount: number;
  highConfidenceBuyCount: number;
  tailCorrect: boolean | null;
  dualBuy: boolean;
  likelyArb: boolean;
  pattern: string;
  recentTrades: StoredActivity[];
  tradeTape: BonereaperTradeEvent[];
  sampleCount: number;
}

interface StoredWindow {
  conditionId: string;
  slug: string;
  title: string;
  windowStart: number;
  activities: StoredActivity[];
  samples: BonereaperMarketSample[];
  summary: BonereaperWindowSummary;
}

interface PersistedState {
  address: string;
  updatedAt: number;
  windows: StoredWindow[];
}

export interface BonereaperMonitorSnapshot {
  address: string;
  running: boolean;
  pollMs: number;
  updatedAt: number;
  lastPollAt: number;
  lastSuccessAt: number;
  lastError: string;
  sourceUrl: string;
  current: BonereaperWindowSummary | null;
  recent: BonereaperWindowSummary[];
  stats: {
    windows: number;
    completed: number;
    dualBuy: number;
    dualBuyPct: number | null;
    likelyArb: number;
    likelyArbPct: number | null;
    terminalChase: number;
    terminalChasePct: number | null;
    reversalWindows: number;
    reversalWindowsPct: number | null;
    tailCorrect: number;
    tailCorrectPct: number | null;
    avgBuyUsdc: number | null;
    avgPairedCostPct: number | null;
    approxPnlUsd: number | null;
    approxRoiPct: number | null;
    winRatePct: number | null;
  };
}

export interface BonereaperMonitorOptions {
  file: string;
  address?: string;
  pollMs?: number;
  activityLimit?: number;
  maxWindows?: number;
  sampleWindows?: number;
  maxLoadBytes?: number;
  maxSamplesPerWindow?: number;
}

export interface BonereaperSnapshotContext {
  currentConditionId?: string;
  currentWindowStart?: number;
}

const DEFAULT_ADDRESS = "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30";
const DATA_API = "https://data-api.polymarket.com/activity";
const FIVE_MIN_SECONDS = 300;
const MARKET_MATCH_MAX_LAG_MS = 2500;
const DEFAULT_MAX_WINDOWS = 288;
const DEFAULT_SAMPLE_WINDOWS = 36;
const DEFAULT_MAX_LOAD_BYTES = 80 * 1024 * 1024;
const DEFAULT_MAX_SAMPLES_PER_WINDOW = 180;

function round(value: number, digits = 4): number {
  if (!Number.isFinite(value)) return 0;
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function pct(numerator: number, denominator: number): number | null {
  return denominator > 0 ? round((numerator / denominator) * 100, 2) : null;
}

function normalizeOutcome(value: unknown, outcomeIndex?: unknown): Outcome {
  const text = String(value || "").trim().toLowerCase();
  if (text === "up") return "up";
  if (text === "down") return "down";
  if (outcomeIndex === 0) return "up";
  if (outcomeIndex === 1) return "down";
  return "";
}

function normalizeSide(value: unknown): ActivitySide {
  const text = String(value || "").trim().toUpperCase();
  return text === "BUY" || text === "SELL" ? text : "";
}

function parseWindowStart(row: RawActivity): number | null {
  const slug = String(row.slug || row.eventSlug || "");
  const match = slug.match(/^btc-updown-5m-(\d+)$/);
  if (!match) return null;
  const value = Number(match[1]);
  return Number.isFinite(value) ? value : null;
}

function activityKey(row: RawActivity, normalized: StoredActivity): string {
  return [
    normalized.transactionHash || "nohash",
    normalized.timestamp,
    normalized.type,
    normalized.side,
    normalized.outcome,
    normalized.size,
    normalized.usdcSize,
    normalized.price ?? "",
    row.asset || "",
  ].join(":");
}

function normalizeActivity(row: RawActivity): StoredActivity | null {
  const timestamp = Number(row.timestamp);
  const type = String(row.type || "").trim().toUpperCase();
  if (!Number.isFinite(timestamp) || timestamp <= 0 || !type) return null;
  const size = Number(row.size) || 0;
  const usdcSize = Number(row.usdcSize) || 0;
  const price = Number(row.price);
  const item: StoredActivity = {
    key: "",
    seenAt: Date.now(),
    timestamp,
    type,
    side: normalizeSide(row.side),
    outcome: normalizeOutcome(row.outcome, row.outcomeIndex),
    size: Number.isFinite(size) ? size : 0,
    usdcSize: Number.isFinite(usdcSize) ? usdcSize : 0,
    price: Number.isFinite(price) && price > 0 ? price : null,
    transactionHash: String(row.transactionHash || ""),
  };
  item.key = activityKey(row, item);
  return item;
}

function sum(items: StoredActivity[], fn: (item: StoredActivity) => number): number {
  return items.reduce((acc, item) => acc + (Number(fn(item)) || 0), 0);
}

function sampleNumber(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function nearestSample(samples: BonereaperMarketSample[], targetMs: number): { sample: BonereaperMarketSample; lagMs: number } | null {
  let best: { sample: BonereaperMarketSample; lagMs: number } | null = null;
  for (const sample of samples) {
    const lagMs = Math.abs(sample.ts - targetMs);
    if (!best || lagMs < best.lagMs) best = { sample, lagMs };
  }
  if (!best || best.lagMs > MARKET_MATCH_MAX_LAG_MS) return null;
  return best;
}

function diffSign(diff: number | null): Outcome {
  if (diff == null || !Number.isFinite(diff) || Math.abs(diff) < 0.01) return "";
  return diff > 0 ? "up" : "down";
}

function opposite(direction: Outcome): Outcome {
  if (direction === "up") return "down";
  if (direction === "down") return "up";
  return "";
}

function directionMarketMid(sample: BonereaperMarketSample | null, direction: Outcome): number | null {
  if (!sample) return null;
  if (direction === "up") return sample.upMid;
  if (direction === "down") return sample.downMid;
  return null;
}

function buildMotiveTags(input: {
  activity: StoredActivity;
  sample: BonereaperMarketSample | null;
  lagMs: number | null;
  beforeUp: number;
  beforeDown: number;
  afterUp: number;
  afterDown: number;
  firstDirection: Outcome;
  firstDiff: number | null;
}): string[] {
  const tags: string[] = [];
  const { activity, sample, beforeUp, beforeDown, afterUp, afterDown, firstDirection, firstDiff } = input;
  if (activity.type !== "TRADE" || activity.side !== "BUY" || !activity.outcome) return tags;
  const rem = sample?.remSec ?? null;
  const price = activity.price;
  const marketMid = directionMarketMid(sample, activity.outcome);
  const beforeOwn = activity.outcome === "up" ? beforeUp : beforeDown;
  const beforeOther = activity.outcome === "up" ? beforeDown : beforeUp;
  const afterTail = Math.abs(afterUp - afterDown);
  const directionFromDiff = diffSign(sample?.diff ?? null);
  const directionFromFirstDiff = diffSign(firstDiff);

  if (rem != null && rem > 210) tags.push("early_seed");
  if (rem != null && rem <= 10 && price != null && price >= 0.85) tags.push("terminal_chase");
  else if (rem != null && rem <= 20 && price != null && price >= 0.8) tags.push("late_confidence");
  if (rem != null && rem <= 25 && price != null && price <= 0.35) tags.push("panic_catch");
  if (marketMid != null && price != null) {
    if (price <= marketMid - 0.025) tags.push("below_mid_fill");
    if (price >= marketMid + 0.025) tags.push("above_mid_pay");
  }
  if (directionFromDiff) {
    tags.push(directionFromDiff === activity.outcome ? "trend_follow" : "contrarian");
  }
  if (firstDirection && activity.outcome === opposite(firstDirection)) {
    tags.push("opposite_leg");
    if (directionFromDiff === activity.outcome && directionFromDiff !== directionFromFirstDiff) tags.push("reversal_response");
  }
  if (beforeOwn <= beforeOther + 1e-9) tags.push("hedge_or_pair");
  else tags.push("tail_add");
  if (afterTail >= 100 && beforeOwn > beforeOther) tags.push("large_tail");
  const fairUp = sample?.fairUpPct;
  if (fairUp != null && price != null) {
    const fair = activity.outcome === "up" ? fairUp / 100 : 1 - fairUp / 100;
    const edgePct = (fair - price) * 100;
    if (edgePct >= 5) tags.push("model_edge");
    if (edgePct <= -5) tags.push("negative_model_edge");
  }
  return [...new Set(tags)];
}

function avgPrice(usdc: number, shares: number): number | null {
  return shares > 0 ? round(usdc / shares, 6) : null;
}

function inferResultDirection(upShares: number, downShares: number, redeemShares: number): Outcome {
  if (redeemShares <= 0) return "";
  const upGap = Math.abs(upShares - redeemShares);
  const downGap = Math.abs(downShares - redeemShares);
  if (upGap <= downGap && upGap <= Math.max(2, upShares * 0.03)) return "up";
  if (downGap < upGap && downGap <= Math.max(2, downShares * 0.03)) return "down";
  return upShares >= downShares ? "up" : "down";
}

function summarizeWindow(window: StoredWindow): BonereaperWindowSummary {
  const activities = [...window.activities].sort((a, b) => a.timestamp - b.timestamp);
  const samples = [...(window.samples || [])].sort((a, b) => a.ts - b.ts);
  const trades = activities.filter((item) => item.type === "TRADE");
  const buys = trades.filter((item) => item.side === "BUY");
  const sells = trades.filter((item) => item.side === "SELL");
  const upBuys = buys.filter((item) => item.outcome === "up");
  const downBuys = buys.filter((item) => item.outcome === "down");
  const redeems = activities.filter((item) => item.type === "REDEEM");
  const windowEnd = window.windowStart + FIVE_MIN_SECONDS;
  const terminalBuys = buys.filter((item) => item.timestamp >= windowEnd - 10);

  const upBuyShares = sum(upBuys, (item) => item.size);
  const downBuyShares = sum(downBuys, (item) => item.size);
  const upBuyUsdc = sum(upBuys, (item) => item.usdcSize);
  const downBuyUsdc = sum(downBuys, (item) => item.usdcSize);
  const totalBuyUsdc = upBuyUsdc + downBuyUsdc;
  const totalBuyShares = upBuyShares + downBuyShares;
  const upAvg = avgPrice(upBuyUsdc, upBuyShares);
  const downAvg = avgPrice(downBuyUsdc, downBuyShares);
  const pairedShares = Math.min(upBuyShares, downBuyShares);
  const pairedCostPct = upAvg != null && downAvg != null ? round((upAvg + downAvg) * 100, 3) : null;
  const pairedEdgePct = pairedCostPct != null ? round(100 - pairedCostPct, 3) : null;
  const pairedPnlUsd = pairedEdgePct != null ? round(pairedShares * pairedEdgePct / 100, 4) : null;
  const tailDirection: Outcome = upBuyShares > downBuyShares + 1e-9 ? "up" : downBuyShares > upBuyShares + 1e-9 ? "down" : "";
  const tailShares = Math.abs(upBuyShares - downBuyShares);
  const tailAvg = tailDirection === "up" ? upAvg : tailDirection === "down" ? downAvg : null;

  const redeemShares = sum(redeems, (item) => item.size);
  const redeemUsdc = sum(redeems, (item) => item.usdcSize);
  const sellUsdc = sum(sells, (item) => item.usdcSize);
  const resultDirection = inferResultDirection(upBuyShares, downBuyShares, redeemShares);
  const completed = redeemUsdc > 0 || sellUsdc > 0;
  const approxPnlUsd = completed ? round(redeemUsdc + sellUsdc - totalBuyUsdc, 4) : null;
  const approxRoiPct = approxPnlUsd != null ? pct(approxPnlUsd, totalBuyUsdc) : null;

  const terminalBuyUsdc = sum(terminalBuys, (item) => item.usdcSize);
  const terminalBuyShares = sum(terminalBuys, (item) => item.size);
  const terminalAvg = avgPrice(terminalBuyUsdc, terminalBuyShares);
  const terminalUpShares = sum(terminalBuys.filter((item) => item.outcome === "up"), (item) => item.size);
  const terminalDownShares = sum(terminalBuys.filter((item) => item.outcome === "down"), (item) => item.size);
  const terminalDirection: Outcome = terminalUpShares > terminalDownShares ? "up" : terminalDownShares > terminalUpShares ? "down" : "";
  const terminalChase = terminalBuyUsdc > 0 && terminalAvg != null && terminalAvg >= 0.85;
  const diffValues = samples
    .map((sample) => sampleNumber(sample.diff))
    .filter((value): value is number => value != null);
  const minDiff = diffValues.length ? round(Math.min(...diffValues), 3) : null;
  const maxDiff = diffValues.length ? round(Math.max(...diffValues), 3) : null;

  let invUp = 0;
  let invDown = 0;
  let costUp = 0;
  let costDown = 0;
  let firstBuyDirection: Outcome = "";
  let firstBuyRemSec: number | null = null;
  let firstBuyDiff: number | null = null;
  let firstBuyPrice: number | null = null;
  let firstOppositeTs: number | null = null;
  let firstOppositeRemSec: number | null = null;
  let firstOppositeDiff: number | null = null;
  let firstOppositePrice: number | null = null;
  let firstOppositeReason = "";
  const tradeTape: BonereaperTradeEvent[] = [];

  for (const activity of activities) {
    if (activity.type !== "TRADE" || activity.side !== "BUY" || !activity.outcome) continue;
    const matched = nearestSample(samples, activity.timestamp * 1000);
    const sample = matched?.sample ?? null;
    const lagMs = matched?.lagMs ?? null;
    const beforeUp = invUp;
    const beforeDown = invDown;
    if (!firstBuyDirection) {
      firstBuyDirection = activity.outcome;
      firstBuyRemSec = sample?.remSec ?? null;
      firstBuyDiff = sample?.diff ?? null;
      firstBuyPrice = activity.price;
    }
    if (activity.outcome === "up") {
      invUp += activity.size;
      costUp += activity.usdcSize;
    } else if (activity.outcome === "down") {
      invDown += activity.size;
      costDown += activity.usdcSize;
    }
    const afterUp = invUp;
    const afterDown = invDown;
    const pairedAfterShares = Math.min(afterUp, afterDown);
    const upAvgAfter = avgPrice(costUp, afterUp);
    const downAvgAfter = avgPrice(costDown, afterDown);
    const pairedCostPctAfter = upAvgAfter != null && downAvgAfter != null
      ? round((upAvgAfter + downAvgAfter) * 100, 3)
      : null;
    const tailAfterDirection: Outcome = afterUp > afterDown + 1e-9 ? "up" : afterDown > afterUp + 1e-9 ? "down" : "";
    const tailAfterShares = Math.abs(afterUp - afterDown);
    const motiveTags = buildMotiveTags({
      activity,
      sample,
      lagMs,
      beforeUp,
      beforeDown,
      afterUp,
      afterDown,
      firstDirection: firstBuyDirection,
      firstDiff: firstBuyDiff,
    });
    if (
      firstBuyDirection &&
      activity.outcome === opposite(firstBuyDirection) &&
      firstOppositeTs == null
    ) {
      firstOppositeTs = activity.timestamp;
      firstOppositeRemSec = sample?.remSec ?? null;
      firstOppositeDiff = sample?.diff ?? null;
      firstOppositePrice = activity.price;
      firstOppositeReason = motiveTags.join(",") || "opposite_leg";
    }
    tradeTape.push({
      timestamp: activity.timestamp,
      type: activity.type,
      side: activity.side,
      outcome: activity.outcome,
      size: round(activity.size, 4),
      usdcSize: round(activity.usdcSize, 4),
      price: activity.price,
      remSec: sample?.remSec ?? null,
      diff: sample?.diff != null ? round(sample.diff, 3) : null,
      upMid: sample?.upMid != null ? round(sample.upMid, 4) : null,
      downMid: sample?.downMid != null ? round(sample.downMid, 4) : null,
      fairUpPct: sample?.fairUpPct != null ? round(sample.fairUpPct, 2) : null,
      biasUpPct: sample?.biasUpPct != null ? round(sample.biasUpPct, 2) : null,
      macdFast1mTrend: sample?.macdFast1mTrend ?? null,
      marketLagMs: lagMs,
      inventoryBeforeUp: round(beforeUp, 4),
      inventoryBeforeDown: round(beforeDown, 4),
      inventoryAfterUp: round(afterUp, 4),
      inventoryAfterDown: round(afterDown, 4),
      tailAfterDirection,
      tailAfterShares: round(tailAfterShares, 4),
      pairedAfterShares: round(pairedAfterShares, 4),
      pairedCostPctAfter,
      motiveTags,
    });
  }

  const trendFollowBuyCount = tradeTape.filter((item) => item.motiveTags.includes("trend_follow")).length;
  const contrarianBuyCount = tradeTape.filter((item) => item.motiveTags.includes("contrarian")).length;
  const hedgeBuyCount = tradeTape.filter((item) => item.motiveTags.includes("hedge_or_pair")).length;
  const tailAddBuyCount = tradeTape.filter((item) => item.motiveTags.includes("tail_add")).length;
  const panicCatchBuyCount = tradeTape.filter((item) => item.motiveTags.includes("panic_catch")).length;
  const highConfidenceBuyCount = tradeTape.filter((item) =>
    item.motiveTags.includes("terminal_chase") || item.motiveTags.includes("late_confidence")
  ).length;
  const firstTradeDiffSign = diffSign(firstBuyDiff);
  const lastDiff = tradeTape.map((item) => item.diff).filter((value): value is number => value != null).at(-1) ?? null;
  const diffFlipped = (minDiff != null && maxDiff != null && minDiff < 0 && maxDiff > 0)
    || (!!firstTradeDiffSign && !!diffSign(lastDiff) && firstTradeDiffSign !== diffSign(lastDiff));
  const dualBuy = upBuyShares > 0 && downBuyShares > 0;
  const likelyArb = dualBuy && pairedCostPct != null && pairedCostPct < 100 && tailShares <= Math.max(5, pairedShares * 0.2);
  const tailCorrect = resultDirection && tailDirection ? tailDirection === resultDirection : null;
  const pattern = terminalChase
    ? "terminal_chase"
    : diffFlipped && dualBuy
      ? "reversal_response"
    : likelyArb
      ? "paired_arb"
      : dualBuy
        ? "dual_inventory"
        : totalBuyShares > 0
          ? "single_direction"
          : "watch";

  return {
    conditionId: window.conditionId,
    slug: window.slug,
    title: window.title,
    windowStart: window.windowStart,
    windowEnd,
    firstTs: activities[0]?.timestamp ?? null,
    lastTs: activities.at(-1)?.timestamp ?? null,
    updatedAt: Date.now(),
    activityCount: activities.length,
    tradeCount: trades.length,
    buyCount: buys.length,
    upBuyCount: upBuys.length,
    downBuyCount: downBuys.length,
    upBuyShares: round(upBuyShares, 4),
    downBuyShares: round(downBuyShares, 4),
    upBuyUsdc: round(upBuyUsdc, 4),
    downBuyUsdc: round(downBuyUsdc, 4),
    upAvgBuy: upAvg,
    downAvgBuy: downAvg,
    pairedShares: round(pairedShares, 4),
    pairedCostPct,
    pairedEdgePct,
    pairedPnlUsd,
    tailDirection,
    tailShares: round(tailShares, 4),
    tailAvgCostPct: tailAvg != null ? round(tailAvg * 100, 3) : null,
    totalBuyUsdc: round(totalBuyUsdc, 4),
    totalBuyShares: round(totalBuyShares, 4),
    redeemCount: redeems.length,
    redeemShares: round(redeemShares, 4),
    redeemUsdc: round(redeemUsdc, 4),
    sellUsdc: round(sellUsdc, 4),
    approxPnlUsd,
    approxRoiPct,
    resultDirection,
    terminalBuyCount: terminalBuys.length,
    terminalBuyUsdc: round(terminalBuyUsdc, 4),
    terminalBuyShares: round(terminalBuyShares, 4),
    terminalAvgPrice: terminalAvg,
    terminalDirection,
    terminalChase,
    firstBuyDirection,
    firstBuyRemSec,
    firstBuyDiff: firstBuyDiff != null ? round(firstBuyDiff, 3) : null,
    firstBuyPrice,
    firstOppositeTs,
    firstOppositeRemSec,
    firstOppositeDiff: firstOppositeDiff != null ? round(firstOppositeDiff, 3) : null,
    firstOppositePrice,
    firstOppositeReason,
    minDiff,
    maxDiff,
    diffFlipped,
    trendFollowBuyCount,
    contrarianBuyCount,
    hedgeBuyCount,
    tailAddBuyCount,
    panicCatchBuyCount,
    highConfidenceBuyCount,
    tailCorrect,
    dualBuy,
    likelyArb,
    pattern,
    recentTrades: activities.slice(-12).reverse(),
    tradeTape: tradeTape.slice(-80).reverse(),
    sampleCount: samples.length,
  };
}

export class BonereaperMonitor {
  private readonly file: string;
  private readonly address: string;
  private readonly pollMs: number;
  private readonly activityLimit: number;
  private readonly maxWindows: number;
  private readonly sampleWindows: number;
  private readonly maxLoadBytes: number;
  private readonly maxSamplesPerWindow: number;
  private readonly windows = new Map<string, StoredWindow>();
  private timer: ReturnType<typeof setInterval> | null = null;
  private running = false;
  private lastPollAt = 0;
  private lastSuccessAt = 0;
  private lastError = "";
  private lastPersistAt = 0;
  private onUpdate: (() => void) | null = null;

  constructor(options: BonereaperMonitorOptions) {
    this.file = options.file;
    this.address = options.address || DEFAULT_ADDRESS;
    this.pollMs = Math.max(5000, Math.round(options.pollMs || 10000));
    this.activityLimit = Math.max(50, Math.round(options.activityLimit || 500));
    this.maxWindows = Math.max(
      20,
      Math.round(options.maxWindows || Number(process.env.BONEREAPER_MONITOR_MAX_WINDOWS || DEFAULT_MAX_WINDOWS)),
    );
    this.sampleWindows = Math.max(
      1,
      Math.round(options.sampleWindows || Number(process.env.BONEREAPER_MONITOR_SAMPLE_WINDOWS || DEFAULT_SAMPLE_WINDOWS)),
    );
    this.maxLoadBytes = Math.max(
      1024 * 1024,
      Math.round(options.maxLoadBytes || Number(process.env.BONEREAPER_MONITOR_MAX_LOAD_BYTES || DEFAULT_MAX_LOAD_BYTES)),
    );
    this.maxSamplesPerWindow = Math.max(
      20,
      Math.round(options.maxSamplesPerWindow || Number(process.env.BONEREAPER_MONITOR_MAX_SAMPLES_PER_WINDOW || DEFAULT_MAX_SAMPLES_PER_WINDOW)),
    );
    this.load();
  }

  start(onUpdate?: () => void): void {
    this.onUpdate = onUpdate || null;
    if (this.timer) return;
    this.running = true;
    void this.poll();
    this.timer = setInterval(() => void this.poll(), this.pollMs);
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.running = false;
  }

  async poll(): Promise<void> {
    this.lastPollAt = Date.now();
    try {
      const res = await fetch(this.activityUrl(), { headers: { accept: "application/json" } });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const rows = await res.json() as RawActivity[];
      if (!Array.isArray(rows)) throw new Error("activity response is not an array");
      const changed = this.ingest(rows);
      this.lastSuccessAt = Date.now();
      this.lastError = "";
      if (changed) {
        this.persist();
        this.onUpdate?.();
      }
    } catch (err) {
      this.lastError = err instanceof Error ? err.message : String(err);
    }
  }

  observeMarket(sample: BonereaperMarketSample): void {
    if (!Number.isFinite(sample.windowStart) || sample.windowStart <= 0) return;
    const conditionId = `sample:${sample.windowStart}`;
    let window = [...this.windows.values()].find((item) => item.windowStart === sample.windowStart);
    if (!window) {
      window = {
        conditionId,
        slug: `btc-updown-5m-${sample.windowStart}`,
        title: "",
        windowStart: sample.windowStart,
        activities: [],
        samples: [],
        summary: {} as BonereaperWindowSummary,
      };
      this.windows.set(conditionId, window);
    }
    if (!window.samples) window.samples = [];
    const last = window.samples.at(-1);
    if (last && sample.ts <= last.ts) return;
    window.samples.push(sample);
    if (window.samples.length > this.maxSamplesPerWindow) {
      window.samples.splice(0, window.samples.length - this.maxSamplesPerWindow);
    }
    window.summary = summarizeWindow(window);
    if (Date.now() - this.lastPersistAt > 15000) {
      this.persist();
      this.onUpdate?.();
    }
  }

  getSnapshot(context: BonereaperSnapshotContext = {}): BonereaperMonitorSnapshot {
    const all = [...this.windows.values()]
      .map((item) => item.summary)
      .sort((a, b) => b.windowStart - a.windowStart);
    const recent = all.filter((item) =>
      item.activityCount > 0 ||
      (context.currentConditionId && item.conditionId === context.currentConditionId) ||
      (context.currentWindowStart && item.windowStart === context.currentWindowStart)
    );
    const current = recent.find((item) =>
      (context.currentConditionId && item.conditionId === context.currentConditionId) ||
      (context.currentWindowStart && item.windowStart === context.currentWindowStart)
    ) || null;
    const analyzed = recent.filter((item) => item.activityCount > 0);
    const completed = analyzed.filter((item) => item.approxPnlUsd != null);
    const dual = analyzed.filter((item) => item.dualBuy);
    const likelyArb = analyzed.filter((item) => item.likelyArb);
    const terminal = analyzed.filter((item) => item.terminalChase);
    const reversal = analyzed.filter((item) => item.diffFlipped);
    const tailKnown = analyzed.filter((item) => item.tailCorrect != null);
    const tailCorrect = analyzed.filter((item) => item.tailCorrect === true);
    const pairedCostRows = analyzed.filter((item) => item.pairedCostPct != null);
    const totalBuy = analyzed.reduce((acc, item) => acc + item.totalBuyUsdc, 0);
    const totalPnl = completed.reduce((acc, item) => acc + (item.approxPnlUsd || 0), 0);
    const wins = completed.filter((item) => (item.approxPnlUsd || 0) > 0);
    return {
      address: this.address,
      running: this.running,
      pollMs: this.pollMs,
      updatedAt: Date.now(),
      lastPollAt: this.lastPollAt,
      lastSuccessAt: this.lastSuccessAt,
      lastError: this.lastError,
      sourceUrl: this.activityUrl(),
      current,
      recent: recent.slice(0, 120),
      stats: {
        windows: analyzed.length,
        completed: completed.length,
        dualBuy: dual.length,
        dualBuyPct: pct(dual.length, analyzed.length),
        likelyArb: likelyArb.length,
        likelyArbPct: pct(likelyArb.length, analyzed.length),
        terminalChase: terminal.length,
        terminalChasePct: pct(terminal.length, analyzed.length),
        reversalWindows: reversal.length,
        reversalWindowsPct: pct(reversal.length, analyzed.length),
        tailCorrect: tailCorrect.length,
        tailCorrectPct: pct(tailCorrect.length, tailKnown.length),
        avgBuyUsdc: analyzed.length ? round(totalBuy / analyzed.length, 2) : null,
        avgPairedCostPct: pairedCostRows.length
          ? round(pairedCostRows.reduce((acc, item) => acc + (item.pairedCostPct || 0), 0) / pairedCostRows.length, 3)
          : null,
        approxPnlUsd: completed.length ? round(totalPnl, 2) : null,
        approxRoiPct: totalBuy > 0 ? round((totalPnl / totalBuy) * 100, 2) : null,
        winRatePct: pct(wins.length, completed.length),
      },
    };
  }

  private activityUrl(): string {
    const params = new URLSearchParams({
      user: this.address,
      limit: String(this.activityLimit),
      offset: "0",
      sortBy: "TIMESTAMP",
      sortDirection: "DESC",
    });
    return `${DATA_API}?${params.toString()}`;
  }

  private ingest(rows: RawActivity[]): boolean {
    let changed = false;
    for (const row of rows) {
      const windowStart = parseWindowStart(row);
      const conditionId = String(row.conditionId || "");
      if (!windowStart || !conditionId) continue;
      const activity = normalizeActivity(row);
      if (!activity) continue;
      let window = this.windows.get(conditionId)
        || [...this.windows.values()].find((item) => item.windowStart === windowStart);
      if (!window) {
        const slug = String(row.slug || row.eventSlug || `btc-updown-5m-${windowStart}`);
        window = {
          conditionId,
          slug,
          title: String(row.title || ""),
          windowStart,
          activities: [],
          samples: [],
          summary: {} as BonereaperWindowSummary,
        };
        this.windows.set(conditionId, window);
      } else if (window.conditionId !== conditionId) {
        this.windows.delete(window.conditionId);
        window.conditionId = conditionId;
        window.slug = String(row.slug || row.eventSlug || window.slug || `btc-updown-5m-${windowStart}`);
        window.title = String(row.title || window.title || "");
        this.windows.set(conditionId, window);
      }
      if (window.activities.some((item) => item.key === activity.key)) continue;
      window.activities.push(activity);
      window.activities.sort((a, b) => a.timestamp - b.timestamp);
      if (window.activities.length > 360) {
        window.activities.splice(0, window.activities.length - 360);
      }
      window.summary = summarizeWindow(window);
      changed = true;
    }
    if (changed) this.trimWindows();
    return changed;
  }

  private trimWindows(): void {
    const ordered = [...this.windows.values()].sort((a, b) => b.windowStart - a.windowStart);
    for (const item of ordered.slice(this.maxWindows)) {
      this.windows.delete(item.conditionId);
    }
    ordered.slice(0, this.maxWindows).forEach((item, index) => {
      if (!Array.isArray(item.samples)) item.samples = [];
      if (index >= this.sampleWindows) {
        item.samples = [];
      } else if (item.samples.length > this.maxSamplesPerWindow) {
        item.samples = item.samples.slice(-this.maxSamplesPerWindow);
      }
    });
  }

  private load(): void {
    if (!existsSync(this.file)) return;
    try {
      const size = statSync(this.file).size;
      if (size > this.maxLoadBytes) {
        const archive = this.file.replace(/\.json$/i, `.archive-${new Date().toISOString().replace(/[:.]/g, "-")}.json`);
        renameSync(this.file, archive);
        console.warn(
          `[Bonereaper] monitor file too large ${(size / 1024 / 1024).toFixed(1)}MB > ${(this.maxLoadBytes / 1024 / 1024).toFixed(1)}MB; archived to ${archive}`,
        );
        return;
      }
      const raw = JSON.parse(readFileSync(this.file, "utf8")) as PersistedState;
      if (!Array.isArray(raw.windows)) return;
      for (const [index, item] of raw.windows.entries()) {
        if (!item?.conditionId || !Number.isFinite(Number(item.windowStart)) || !Array.isArray(item.activities)) continue;
        const samples = index < this.sampleWindows && Array.isArray(item.samples)
          ? item.samples.slice(-this.maxSamplesPerWindow)
          : [];
        const window: StoredWindow = {
          conditionId: item.conditionId,
          slug: item.slug || `btc-updown-5m-${item.windowStart}`,
          title: item.title || "",
          windowStart: Number(item.windowStart),
          activities: item.activities,
          samples,
          summary: item.summary || ({} as BonereaperWindowSummary),
        };
        if (window.samples.length || !window.summary) {
          window.summary = summarizeWindow(window);
        }
        this.windows.set(window.conditionId, window);
      }
      this.trimWindows();
    } catch {
      // Ignore corrupt monitor files; the next poll will rebuild fresh data.
    }
  }

  private persist(): void {
    try {
      const dir = dirname(this.file);
      if (!existsSync(dir)) mkdirSync(dir, { recursive: true });
      const windows = [...this.windows.values()]
        .sort((a, b) => b.windowStart - a.windowStart)
        .map((window, index) => ({
          ...window,
          samples: index < this.sampleWindows
            ? (window.samples || []).slice(-this.maxSamplesPerWindow)
            : [],
        }));
      const payload: PersistedState = {
        address: this.address,
        updatedAt: Date.now(),
        windows,
      };
      writeFileSync(this.file, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      this.lastPersistAt = Date.now();
    } catch {
      // The monitor should never take the trading dashboard down because of disk IO.
    }
  }
}
