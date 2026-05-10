/**
 * BTC 5分钟涨跌盘口监控 — 独立服务端
 * 启动: npx tsx server.ts
 */

import express from "express";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";
import {
  existsSync,
  readFileSync,
  writeFileSync,
  mkdirSync,
  appendFileSync,
  readdirSync,
  unlinkSync,
  renameSync,
} from "fs";
import { ethers } from "ethers";
import dotenv from "dotenv";
import {
  ClobClient,
  Side,
  OrderType,
  Chain,
  SignatureTypeV2 as SignatureType,
  AssetType,
  getContractConfig,
} from "@polymarket/clob-client-v2";
import {
  getAllStrategies,
  getStrategy,
  getAllDescriptions,
} from "./strategies/registry.js";
import type {
  FullSetArbSnapshot,
  MakerQuoteSignal,
  MakerStatusSnapshot,
  S10TailMultipliers,
  StrategyNumber,
  StrategyDirection,
  StrategyLifecycleState,
  StrategyKey,
} from "./strategies/types.js";
import { ALL_STRATEGY_KEYS } from "./strategies/types.js";
import { getFairProb } from "./strategies/fair-prob.js";
import {
  buildMacdSnapshot,
  isDirectionAgainstTrend,
} from "./strategies/indicators.js";
import { getRealFillFromTx } from "./chain-watcher.js";
import { PmPnlManager } from "./polymarket-pnl.js";
import {
  BonereaperMonitor,
  type BonereaperMarketSample,
} from "./bonereaper-monitor.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
dotenv.config({ path: resolve(__dirname, ".env") });

const SERVER_LIFECYCLE_LOG = resolve(__dirname, ".server-lifecycle.log");

function logLifecycle(event: string, meta: Record<string, unknown> = {}): void {
  try {
    appendFileSync(
      SERVER_LIFECYCLE_LOG,
      `${JSON.stringify({ ts: new Date().toISOString(), pid: process.pid, event, ...meta })}\n`,
      "utf8",
    );
  } catch {
    // Lifecycle logging must never bring down the trading process.
  }
}

function logMemorySnapshot(event = "memory"): void {
  const mem = process.memoryUsage();
  logLifecycle(event, {
    rssMb: Math.round(mem.rss / 1024 / 1024),
    heapUsedMb: Math.round(mem.heapUsed / 1024 / 1024),
    heapTotalMb: Math.round(mem.heapTotal / 1024 / 1024),
    externalMb: Math.round(mem.external / 1024 / 1024),
  });
}

const PERSIST_WRITE_RETRIES = Math.max(
  1,
  Math.min(5, Math.round(Number(process.env.PERSIST_WRITE_RETRIES ?? 3))),
);
const PERSIST_RETRY_DELAY_MS = Math.max(
  0,
  Math.min(500, Math.round(Number(process.env.PERSIST_RETRY_DELAY_MS ?? 80))),
);

function sleepSync(ms: number): void {
  if (!(ms > 0)) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function formatPersistError(err: unknown): string {
  if (err instanceof Error) {
    const code =
      typeof (err as NodeJS.ErrnoException).code === "string"
        ? ` ${(err as NodeJS.ErrnoException).code}`
        : "";
    return `${err.name}${code}: ${err.message}`;
  }
  return String(err);
}

function safeWriteTextFile(file: string, payload: string, label: string): boolean {
  let lastError: unknown = null;
  for (let attempt = 1; attempt <= PERSIST_WRITE_RETRIES; attempt += 1) {
    const tmp = `${file}.tmp-${process.pid}-${Date.now()}-${attempt}`;
    try {
      writeFileSync(tmp, payload, "utf-8");
      renameSync(tmp, file);
      return true;
    } catch (err) {
      lastError = err;
      try {
        if (existsSync(tmp)) unlinkSync(tmp);
      } catch {
        // best effort cleanup
      }
    }

    try {
      writeFileSync(file, payload, "utf-8");
      return true;
    } catch (err) {
      lastError = err;
    }

    if (attempt < PERSIST_WRITE_RETRIES) sleepSync(PERSIST_RETRY_DELAY_MS);
  }

  const message = formatPersistError(lastError);
  console.warn(`[Persist] ${label} write failed; kept in memory: ${message}`);
  logLifecycle("persistFailed", {
    label,
    file,
    attempts: PERSIST_WRITE_RETRIES,
    error: message,
  });
  return false;
}

// 防止 RPC 超时等未捕获的 Promise rejection 杀死进程
process.on("unhandledRejection", (reason) => {
  logLifecycle("unhandledRejection", {
    reason: reason instanceof Error ? reason.message : String(reason),
  });
  console.error(
    "[未处理异常]",
    reason instanceof Error ? reason.message : reason,
  );
});
process.on("uncaughtExceptionMonitor", (error, origin) => {
  logLifecycle("uncaughtExceptionMonitor", {
    origin,
    error: error?.stack || error?.message || String(error),
  });
  console.error("[致命异常]", origin, error?.stack || error?.message || error);
});
process.on("beforeExit", (code) => {
  logLifecycle("beforeExit", { code });
  console.error(`[进程退出前] code=${code}`);
});
process.on("exit", (code) => {
  logLifecycle("exit", { code });
  console.error(`[进程已退出] code=${code}`);
});

type AppMode = "full" | "headless";
type ClientDataMode = "full" | "low";
type ExecutionMode = "paper" | "live";

interface StrategyConfig {
  enabled: Record<StrategyKey, boolean>;
  amount: Record<StrategyKey, number>;
  slippage: number;
  autoClaimEnabled: boolean;
  maxRoundEntries: number;
  marketHoursOnly: boolean; // 动量策略只在美股开盘时段入场
  executionMode: ExecutionMode;
  s10TailMultipliers: S10TailMultipliers;
}

interface StrategyConfigUpdate {
  enabled?: Partial<Record<StrategyKey, unknown>>;
  amount?: Partial<Record<StrategyKey, unknown>>;
  slippage?: unknown;
  autoClaimEnabled?: unknown;
  maxRoundEntries?: unknown;
  marketHoursOnly?: unknown;
  executionMode?: unknown;
  s10TailMultipliers?: unknown;
}

interface StrategyRuntimeState {
  state: StrategyLifecycleState;
  activeStrategy: StrategyNumber | null;
  direction: StrategyDirection | null;
  buyAmount: number;
  posBeforeBuy: number;
  posBeforeSell: number;
  lockDirection: StrategyDirection | null;
  lockPosBeforeBuy: number;
  lockTargetShares: number;
  lockReason: string;
  locked: boolean;
  waitVerifyAfterSell: boolean;
  cleanupAfterVerify: boolean;
  actionTs: number;
  prevUpPct: number | null;
  buyLockUntil: number;
  positionsReady: boolean;
  roundEntryCount: number;
}

interface Kline {
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  closed: boolean;
}

interface TradeHistoryItem {
  id: string;
  ts: number;
  windowStart: number;
  side: "buy" | "sell";
  direction: StrategyDirection;
  amount: number;
  price?: number | null;
  worstPrice?: number | null;
  status: string;
  source: string;
  pnl?: number | null;
  txHash?: string;
  orderId?: string;
  exitReason?: string;
  roundEntry?: string;
  executionMode?: ExecutionMode;
  requestedAmount?: number | null;
  requestedShares?: number | null;
  filledShares?: number | null;
  filledNotional?: number | null;
  avgPrice?: number | null;
  topBid?: number | null;
  topAsk?: number | null;
  spread?: number | null;
  levelsUsed?: number | null;
  availableLiquidity?: number | null;
  priceImpactPct?: number | null;
  simLatencyMs?: number | null;
  simLatencyMode?: string | null;
  simLatencyBookP80Ms?: number | null;
  simLatencyRestP80Ms?: number | null;
  simLatencyWsP80Ms?: number | null;
  simLatencyPressureMs?: number | null;
  simLatencyJitterMs?: number | null;
  bookLatencyMs?: number | null;
  totalLatencyMs?: number | null;
  partial?: boolean;
  rejectReason?: string;
  bookTokenId?: string | null;
  bookWindowStart?: number | null;
  bookFetchedAt?: number | null;
  bookBids?: BookLevel[];
  bookAsks?: BookLevel[];
  paperBookExpectedBid?: number | null;
  paperBookExpectedAsk?: number | null;
  paperBookDiffPct?: number | null;
  paperBookCheckStatus?: string | null;
  paperBookCheckReason?: string | null;
  makerOrderId?: string | null;
  makerLimitPrice?: number | null;
  makerTrigger?: string | null;
  makerQueueFillRatio?: number | null;
  makerActiveMs?: number | null;
  paperAction?: "order" | "merge" | "settlement";
}

type ExecutionEventType =
  | "paper_order_filled"
  | "paper_order_rejected"
  | "paper_maker_rejected"
  | "paper_settled"
  | "paper_merged"
  | "live_order_submitted"
  | "live_order_rejected"
  | "live_order_mined"
  | "live_maker_posted"
  | "live_maker_canceled"
  | "live_maker_cancel_failed"
  | "live_maker_rejected"
  | "live_maker_filled";

interface ExecutionEventItem {
  id: string;
  ts: number;
  executionMode: ExecutionMode;
  event: ExecutionEventType;
  windowStart: number;
  source: string;
  strategy?: StrategyNumber | null;
  side?: "buy" | "sell" | null;
  direction?: StrategyDirection | null;
  orderId?: string | null;
  makerOrderId?: string | null;
  tokenId?: string | null;
  status?: string | null;
  reason?: string | null;
  price?: number | null;
  avgPrice?: number | null;
  worstPrice?: number | null;
  shares?: number | null;
  requestedShares?: number | null;
  filledShares?: number | null;
  remainingShares?: number | null;
  notional?: number | null;
  requestedNotional?: number | null;
  filledNotional?: number | null;
  topBid?: number | null;
  topAsk?: number | null;
  spread?: number | null;
  bookAgeMs?: number | null;
  bookSource?: string | null;
  bookCheckStatus?: string | null;
  bookCheckReason?: string | null;
  bookCheckDiffPct?: number | null;
  bookLatencyMs?: number | null;
  latencyMs?: number | null;
  totalLatencyMs?: number | null;
}

interface BookLevel {
  price: number;
  size: number;
}

interface BookSnapshot {
  tokenId: string;
  bids: BookLevel[];
  asks: BookLevel[];
  topBid: number;
  topAsk: number;
  fetchedAt: number;
  latencyMs: number;
}

interface LiveMarketRules {
  conditionId: string;
  minimumOrderSize: number | null;
  minimumTickSize: number | null;
  fetchedAt: number;
  source: "clob" | "fallback";
  error?: string | null;
}

interface LatencyStats {
  count: number;
  last: number | null;
  p50: number;
  p80: number;
  p95: number;
}

interface PaperLatencyEstimate {
  delayMs: number;
  mode: "dynamic" | "fixed";
  minMs: number;
  maxMs: number;
  bookP80Ms: number;
  restP80Ms: number;
  wsP80Ms: number;
  pressureMs: number;
  jitterMs: number;
  bookSamples: number;
  restSamples: number;
  wsSamples: number;
}

interface PaperWindowInfo {
  upTokenId: string;
  downTokenId: string;
  eventStartTime?: string;
  endDate?: string;
  settled?: boolean;
  result?: StrategyDirection | null;
  localResult?: StrategyDirection | null;
  priceToBeat?: number | null;
  closePrice?: number | null;
  closeDiff?: number | null;
  closeCapturedAt?: number;
  settlementSource?: string;
  upMark?: number | null;
  downMark?: number | null;
  markUpdatedAt?: number;
}

interface PaperAccountState {
  usdc: number;
  localSize: Record<string, number>;
  realizedPnl: number;
  resetAt: number;
  lastTradeAt: number;
  windows: Record<string, PaperWindowInfo>;
}

interface PendingTradeMeta {
  key: string;
  orderId?: string;
  ts: number;
  windowStart: number;
  side: "buy" | "sell";
  direction: StrategyDirection;
  amount: number;
  worstPrice: number;
  source: string;
  exitReason?: string;
  roundEntry?: string;
}

interface ConsumedPendingTradeMeta extends PendingTradeMeta {
  fillSize?: number | null;
  fillPrice?: number | null;
  fillAssetId?: string | null;
}

interface PaperMakerOrder {
  id: string;
  strategy: StrategyNumber;
  windowStart: number;
  direction: StrategyDirection;
  price: number;
  shares: number;
  remainingShares: number;
  createdAt: number;
  activeAt: number;
  expiresAt: number;
  reason: string;
  lastSeenBid: number | null;
  lastSeenAsk: number | null;
  lastTouchAt: number;
  touchStartedAt: number;
  touchCount: number;
}

interface LiveMakerOrder {
  id: string;
  orderId: string;
  strategy: StrategyNumber;
  windowStart: number;
  tokenId: string;
  direction: StrategyDirection;
  price: number;
  shares: number;
  remainingShares: number;
  createdAt: number;
  postedAt: number;
  expiresAt: number;
  reason: string;
  status: "open" | "canceling" | "canceled" | "filled" | "unknown";
  lastSeenMatchedShares: number;
  lastSyncAt: number;
}

interface ClientSession {
  dataMode: ClientDataMode;
  lastStateSentAt: number;
  stateTimer: NodeJS.Timeout | null;
  stateDirty: boolean;
  stateIncludeHistory: boolean;
}

interface StatePayloadOptions {
  includeHistory?: boolean;
  simple?: boolean;
}

function parseBooleanEnv(name: string, fallback: boolean): boolean {
  const raw = process.env[name]?.trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function parseNumberEnv(
  name: string,
  fallback: number,
  minimum?: number,
): number {
  const raw = process.env[name]?.trim();
  if (!raw) return fallback;
  const value = Number(raw);
  if (!Number.isFinite(value)) return fallback;
  if (minimum != null && value < minimum) return fallback;
  return value;
}

function parseBooleanLike(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (["1", "true", "yes", "on"].includes(normalized)) return true;
    if (["0", "false", "no", "off"].includes(normalized)) return false;
  }
  return null;
}

function parseNumberLike(value: unknown, minimum: number): number | null {
  const parsed =
    typeof value === "number"
      ? value
      : typeof value === "string"
        ? Number(value)
        : NaN;
  if (!Number.isFinite(parsed) || parsed < minimum) return null;
  return parsed;
}

function parseExecutionMode(value: unknown): ExecutionMode | null {
  return value === "paper" || value === "live" ? value : null;
}

const S10_TAIL_MULT_MIN = 0.05;
const S10_TAIL_MULT_MAX = 30;
const DEFAULT_S10_TAIL_MULTIPLIERS: S10TailMultipliers = {
  earlyProbe: 1,
  probe: 2,
  robust: 6,
  certainty: 10,
};

function clampS10TailMultiplier(value: number): number {
  return Math.min(S10_TAIL_MULT_MAX, Math.max(S10_TAIL_MULT_MIN, value));
}

function normalizeS10TailMultipliers(
  raw: unknown,
  fallback: S10TailMultipliers = DEFAULT_S10_TAIL_MULTIPLIERS,
): S10TailMultipliers {
  const source = isRecord(raw) ? raw : {};
  const read = (key: keyof S10TailMultipliers): number => {
    const value = Number(source[key]);
    return Number.isFinite(value)
      ? clampS10TailMultiplier(value)
      : fallback[key];
  };
  return {
    earlyProbe: read("earlyProbe"),
    probe: read("probe"),
    robust: read("robust"),
    certainty: read("certainty"),
  };
}

function parseS10TailMultipliersUpdate(
  raw: unknown,
  fallback: S10TailMultipliers,
): { value?: S10TailMultipliers; error?: string } {
  if (!isRecord(raw)) return { error: "s10TailMultipliers 配置格式错误" };
  const next = { ...fallback };
  for (const key of Object.keys(DEFAULT_S10_TAIL_MULTIPLIERS) as Array<keyof S10TailMultipliers>) {
    if (!(key in raw)) continue;
    const parsed = parseNumberLike(raw[key], S10_TAIL_MULT_MIN);
    if (parsed == null || parsed > S10_TAIL_MULT_MAX) {
      return { error: `S10 tail ${key} 倍数必须在 ${S10_TAIL_MULT_MIN}-${S10_TAIL_MULT_MAX}` };
    }
    next[key] = parsed;
  }
  return { value: next };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value != null && !Array.isArray(value);
}

const PORT = 3456;
const MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const CHAINLINK_WS_URL = "wss://ws-live-data.polymarket.com";
const USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const BINANCE_WS_URL =
  "wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m/btcusdt@kline_5m";
const GAMMA_URL = "https://gamma-api.polymarket.com";
const CLOB_URL = "https://clob.polymarket.com";
const HISTORY_RETENTION_MS = 130000;
const MAX_CHAINLINK_HISTORY_POINTS = 2000;
const MAX_BINANCE_HISTORY_POINTS = 4000;
const MAX_KLINE_1M = 200; // 保留200根1分钟K线
const MAX_KLINE_5M = 50; // 保留50根5分钟K线
const MAX_CONFIRMED_TRADE_IDS = 2000;
const CLAIM_CYCLE_DELAY_MS = 15000; // 查询间隔 15s（高频，保证前端金额实时）
const CLAIM_COOLDOWN_MS = 5 * 60 * 1000; // Claim 冷却：无论成功失败，5 分钟后才能再次 claim
const UNVERIFIED_SELL_BUFFER = 0.05;
const POST_TRADE_CALIBRATION_MS = 18000; // 下单后校准等待时长，买入锁也用此值
const STRAT_BUY_LOCK_MS = POST_TRADE_CALIBRATION_MS;
const STRATEGY_TICK_MS = 250;
const WAIT_FILL_TIMEOUT_MS = 10000;
const FILL_RECONCILE_TIMEOUT_MS = POST_TRADE_CALIBRATION_MS + 2000; // 校准完成后再等2秒确认
const BINANCE_ALIGN_WINDOW_MS = 60000;
const BINANCE_ALIGN_MIN_SPAN_MS = 10000;
const BINANCE_ALIGN_BUCKET_MS = 500;
const BINANCE_ALIGN_REFRESH_MS = 30000;
const BINANCE_OFFSET_EPSILON = 0.01;
const FULL_DATA_STATE_INTERVAL_MS = 200;
const LOW_DATA_STATE_INTERVAL_MS = 2000;
const MAX_WS_BUFFERED_BYTES = 512 * 1024;
const MAX_BOOK_STALE_MS = 2500;
const TRADE_HISTORY_FILE = resolve(__dirname, ".trade-history.json");
const PAPER_STATE_FILE = resolve(__dirname, ".paper-state.json");
const PAPER_TRADE_HISTORY_FILE = resolve(
  __dirname,
  ".paper-trade-history.json",
);
const EXECUTION_EVENTS_FILE = resolve(__dirname, ".execution-events.json");
const STRATEGY_CONFIG_FILE = resolve(__dirname, ".strategy-config.json");
const BACKTEST_DATA_DIR = resolve(__dirname, "backtest-data");
const BONEREAPER_MONITOR_FILE = resolve(
  BACKTEST_DATA_DIR,
  "bonereaper-btc5m-monitor.json",
);
const TRADE_HISTORY_MAX = 200;
const PAPER_TRADE_HISTORY_MAX = 5000;
const PAPER_WINDOW_SUMMARY_MAX = 288;
const EXECUTION_EVENTS_MAX = 5000;
const PENDING_TRADE_META_MAX_AGE_MS = 15 * 60 * 1000;

const PRIVATE_KEY = process.env.POLYMARKET_PRIVATE_KEY || "";
const PROXY_ADDRESS = process.env.POLYMARKET_PROXY_ADDRESS || "";
const APP_MODE: AppMode =
  process.env.APP_MODE === "headless" ? "headless" : "full";
const IS_FULL_MODE = APP_MODE === "full";
const PAPER_INITIAL_USDC = parseNumberEnv("PAPER_INITIAL_USDC", 1000, 1);
const PAPER_MIN_LATENCY_MS = parseNumberEnv("PAPER_MIN_LATENCY_MS", 120, 0);
const PAPER_MAX_LATENCY_MS = parseNumberEnv("PAPER_MAX_LATENCY_MS", 850, 0);
const PAPER_LATENCY_MODE: "dynamic" | "fixed" =
  process.env.PAPER_LATENCY_MODE?.trim().toLowerCase() === "fixed"
    ? "fixed"
    : "dynamic";
const PAPER_DYNAMIC_MAX_LATENCY_MS = parseNumberEnv(
  "PAPER_DYNAMIC_MAX_LATENCY_MS",
  Math.max(PAPER_MAX_LATENCY_MS, 2500),
  100,
);
const PAPER_LATENCY_SAMPLE_MAX = 180;
const PAPER_RESULT_RETRY_MS = 3000;
const PAPER_PENDING_SETTLEMENT_CONFIDENCE = 0.8;
const PAPER_FAST_SETTLE_MIN_DIFF = parseNumberEnv(
  "PAPER_FAST_SETTLE_MIN_DIFF",
  1,
  0,
);
const PAPER_FAST_SETTLE_MAX_PRICE_AGE_MS = parseNumberEnv(
  "PAPER_FAST_SETTLE_MAX_PRICE_AGE_MS",
  5000,
  500,
);
const PAPER_BOOK_AUDIT_LEVELS = Math.max(
  1,
  Math.round(parseNumberEnv("PAPER_BOOK_AUDIT_LEVELS", 20, 1)),
);
const PAPER_BOOK_WS_MAX_DIFF = parseNumberEnv(
  "PAPER_BOOK_WS_MAX_DIFF",
  0.08,
  0,
);
const PAPER_BOOK_CONFIRM_DELAY_MS = parseNumberEnv(
  "PAPER_BOOK_CONFIRM_DELAY_MS",
  250,
  0,
);
const TERMINAL_BOOK_GUARD_ENABLED = parseBooleanEnv(
  "TERMINAL_BOOK_GUARD_ENABLED",
  true,
);
const TERMINAL_BOOK_GUARD_SECONDS = parseNumberEnv(
  "TERMINAL_BOOK_GUARD_SECONDS",
  6,
  0,
);
const TERMINAL_BOOK_GUARD_DIFF = parseNumberEnv(
  "TERMINAL_BOOK_GUARD_DIFF",
  10,
  0,
);
const TERMINAL_BOOK_GUARD_WIN_MAX_ASK = parseNumberEnv(
  "TERMINAL_BOOK_GUARD_WIN_MAX_ASK",
  0.7,
  0,
);
const MACD_FILTER_ENABLED = parseBooleanEnv("MACD_FILTER_ENABLED", true);
const MACD_FILTER_REQUIRE_FAST_AGREE = parseBooleanEnv(
  "MACD_FILTER_REQUIRE_FAST_AGREE",
  true,
);
const MACD_FILTER_MIN_REMAINING = parseNumberEnv(
  "MACD_FILTER_MIN_REMAINING",
  12,
  0,
);
const MACD_FILTER_STRONG_DIFF_BYPASS = parseNumberEnv(
  "MACD_FILTER_STRONG_DIFF_BYPASS",
  120,
  0,
);
const S10_FULLSET_SCANNER_ENABLED = parseBooleanEnv(
  "S10_FULLSET_SCANNER_ENABLED",
  true,
);
const S10_FULLSET_REFRESH_MS = parseNumberEnv(
  "S10_FULLSET_REFRESH_MS",
  800,
  200,
);
const S10_FULLSET_MIN_PROFIT_PCT = parseNumberEnv(
  "S10_FULLSET_MIN_PROFIT_PCT",
  5,
  0,
);
const S10_FULLSET_TRIGGER_PROFIT_PCT = parseNumberEnv(
  "S10_FULLSET_TRIGGER_PROFIT_PCT",
  6.5,
  0,
);
const S10_FULLSET_FEE_BUFFER_PCT = parseNumberEnv(
  "S10_FULLSET_FEE_BUFFER_PCT",
  1.2,
  0,
);
const S10_FULLSET_MIN_SHARES = parseNumberEnv(
  "S10_FULLSET_MIN_SHARES",
  1,
  0.01,
);
const S10_FULLSET_MAX_BOOK_AGE_MS = parseNumberEnv(
  "S10_FULLSET_MAX_BOOK_AGE_MS",
  1800,
  100,
);
const S10_MAKER_ENGINE_ENABLED = parseBooleanEnv(
  "S10_MAKER_ENGINE_ENABLED",
  true,
);
const S10_MAKER_ONLY = parseBooleanEnv("S10_MAKER_ONLY", true);
const S10_MAKER_MAX_ACTIVE_ORDERS_PER_SIDE = Math.max(
  1,
  Math.round(parseNumberEnv("S10_MAKER_MAX_ACTIVE_ORDERS_PER_SIDE", 2, 1)),
);
const S10_MAKER_MIN_ACTIVE_MS = parseNumberEnv(
  "S10_MAKER_MIN_ACTIVE_MS",
  250,
  0,
);
const S10_MAKER_MAX_BOOK_AGE_MS = parseNumberEnv(
  "S10_MAKER_MAX_BOOK_AGE_MS",
  1800,
  100,
);
const S10_MAKER_PARTIAL_MIN_SHARES = parseNumberEnv(
  "S10_MAKER_PARTIAL_MIN_SHARES",
  0.5,
  0.01,
);
const S10_MAKER_MAX_WINDOW_NOTIONAL = parseNumberEnv(
  "S10_MAKER_MAX_WINDOW_NOTIONAL",
  420,
  1,
);
const S10_LIVE_MAKER_MAX_WINDOW_BALANCE_RATIO = parseNumberEnv(
  "S10_LIVE_MAKER_MAX_WINDOW_BALANCE_RATIO",
  0.6,
  0.05,
);
const S10_MAKER_FILL_COOLDOWN_MS = parseNumberEnv(
  "S10_MAKER_FILL_COOLDOWN_MS",
  1500,
  0,
);
const S10_MAKER_SOFT_IMBALANCE_SHARES = parseNumberEnv(
  "S10_MAKER_SOFT_IMBALANCE_SHARES",
  60,
  1,
);
const S10_MAKER_MAX_TAIL_AFTER_FILL_SHARES = parseNumberEnv(
  "S10_MAKER_MAX_TAIL_AFTER_FILL_SHARES",
  45,
  1,
);
const S10_MAKER_BOOK_TOUCH_MIN_ACTIVE_MS = parseNumberEnv(
  "S10_MAKER_BOOK_TOUCH_MIN_ACTIVE_MS",
  1200,
  0,
);
const S10_MAKER_BOOK_TOUCH_PRICE_EPS = parseNumberEnv(
  "S10_MAKER_BOOK_TOUCH_PRICE_EPS",
  0.005,
  0,
);
const S10_MAKER_DUPLICATE_PRICE_EPS = parseNumberEnv(
  "S10_MAKER_DUPLICATE_PRICE_EPS",
  0.0125,
  0,
);
const S10_MAKER_BOOK_TOUCH_MAX_RATIO = parseNumberEnv(
  "S10_MAKER_BOOK_TOUCH_MAX_RATIO",
  0.85,
  0.01,
);
const S10_MAKER_SMALL_TOUCH_NOTIONAL = parseNumberEnv(
  "S10_MAKER_SMALL_TOUCH_NOTIONAL",
  15,
  1,
);
const S10_MAKER_TOUCH_HOLD_FULL_MS = parseNumberEnv(
  "S10_MAKER_TOUCH_HOLD_FULL_MS",
  2200,
  200,
);
const PAPER_S10_LIVE_PARITY_ENABLED = parseBooleanEnv(
  "PAPER_S10_LIVE_PARITY_ENABLED",
  true,
);
const PAPER_S10_LIVE_PARITY_USE_LIVE_CAP = parseBooleanEnv(
  "PAPER_S10_LIVE_PARITY_USE_LIVE_CAP",
  true,
);
const PAPER_S10_LIVE_PARITY_TICK_SIZE = parseNumberEnv(
  "PAPER_S10_LIVE_PARITY_TICK_SIZE",
  0.01,
  0.0001,
);
let liveTradingEnabled = parseBooleanEnv("LIVE_TRADING_ENABLED", false);
const LIVE_MAX_BOOK_STALE_MS = parseNumberEnv(
  "LIVE_MAX_BOOK_STALE_MS",
  900,
  100,
);
const LIVE_BOOK_WS_MAX_DIFF = parseNumberEnv("LIVE_BOOK_WS_MAX_DIFF", 0.025, 0);
const LIVE_BOOK_CONFIRM_DELAY_MS = parseNumberEnv(
  "LIVE_BOOK_CONFIRM_DELAY_MS",
  160,
  0,
);
const LIVE_MIN_BUY_REMAINING_SEC = parseNumberEnv(
  "LIVE_MIN_BUY_REMAINING_SEC",
  12,
  0,
);
const LIVE_MAX_ORDER_USDC = parseNumberEnv("LIVE_MAX_ORDER_USDC", 25, 1);
const LIVE_STRATEGY_MAX_ORDER_USDC = parseNumberEnv(
  "LIVE_STRATEGY_MAX_ORDER_USDC",
  12,
  1,
);
const S10_TERMINAL_SWEEP_LIVE_MAX_ORDER_USDC = parseNumberEnv(
  "S10_TERMINAL_SWEEP_LIVE_MAX_ORDER_USDC",
  150,
  1,
);
const S10_TERMINAL_SWEEP_MAX_WINDOW_USDC = parseNumberEnv(
  "S10_TERMINAL_SWEEP_MAX_WINDOW_USDC",
  150,
  1,
);
const S10_TERMINAL_SWEEP_MAX_ORDERS_PER_WINDOW = Math.max(
  1,
  Math.round(parseNumberEnv("S10_TERMINAL_SWEEP_MAX_ORDERS_PER_WINDOW", 1, 1)),
);
const S10_TERMINAL_SWEEP_MIN_REMAINING_SEC = parseNumberEnv(
  "S10_TERMINAL_SWEEP_MIN_REMAINING_SEC",
  4,
  0,
);
const S10_TERMINAL_SWEEP_COOLDOWN_MS = parseNumberEnv(
  "S10_TERMINAL_SWEEP_COOLDOWN_MS",
  2500,
  0,
);
const LIVE_MIN_ORDER_SHARES_FALLBACK = parseNumberEnv(
  "LIVE_MIN_ORDER_SHARES_FALLBACK",
  5,
  0,
);
const LIVE_MARKET_RULES_CACHE_MS = parseNumberEnv(
  "LIVE_MARKET_RULES_CACHE_MS",
  60000,
  5000,
);
const LIVE_MAX_PRICE_IMPACT_PCT = parseNumberEnv(
  "LIVE_MAX_PRICE_IMPACT_PCT",
  0.35,
  0,
);
const LIVE_STRATEGY_ORDER_RETRIES = Math.max(
  0,
  Math.round(parseNumberEnv("LIVE_STRATEGY_ORDER_RETRIES", 1, 0)),
);
const LIVE_STRATEGY_RETRY_DELAY_MS = parseNumberEnv(
  "LIVE_STRATEGY_RETRY_DELAY_MS",
  350,
  0,
);
let s10LiveMakerEnabled = parseBooleanEnv("S10_LIVE_MAKER_ENABLED", false);
const S10_LIVE_MAKER_MIN_REMAINING_SEC = parseNumberEnv(
  "S10_LIVE_MAKER_MIN_REMAINING_SEC",
  4,
  0,
);
const S10_LIVE_MAKER_ORDER_SYNC_MS = parseNumberEnv(
  "S10_LIVE_MAKER_ORDER_SYNC_MS",
  2000,
  250,
);
const PAPER_PRE_SETTLEMENT_MERGE_ENABLED = parseBooleanEnv(
  "PAPER_PRE_SETTLEMENT_MERGE_ENABLED",
  false,
);
const POLYMARKET_CLOCK_MAX_AGE_MS = 15000;
const BONEREAPER_MONITOR_ENABLED = parseBooleanEnv(
  "BONEREAPER_MONITOR_ENABLED",
  true,
);
const BONEREAPER_MONITOR_ADDRESS =
  process.env.BONEREAPER_MONITOR_ADDRESS ||
  "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30";
const BONEREAPER_MONITOR_POLL_MS = parseNumberEnv(
  "BONEREAPER_MONITOR_POLL_MS",
  10000,
  5000,
);
const BONEREAPER_MARKET_SAMPLE_MS = parseNumberEnv(
  "BONEREAPER_MARKET_SAMPLE_MS",
  1000,
  250,
);

let polymarketClockOffsetMs = 0;
let polymarketClockSyncedAt = 0;
let polymarketClockSource = "";
let polymarketClockLatencyMs: number | null = null;
const bookLatencySamples: number[] = [];
const restCheckLatencySamples: number[] = [];
const wsUpdateIntervalSamples: number[] = [];
let lastWsBookUpdateAt = 0;
let fullSetArbSnapshot: FullSetArbSnapshot =
  createEmptyFullSetArbSnapshot("init");
let fullSetRefreshRunning = false;
let liveMarketRulesCache: LiveMarketRules | null = null;
const bonereaperMonitor = new BonereaperMonitor({
  file: BONEREAPER_MONITOR_FILE,
  address: BONEREAPER_MONITOR_ADDRESS,
  pollMs: BONEREAPER_MONITOR_POLL_MS,
});

function recordLatencySample(
  samples: number[],
  value: unknown,
  maxSize = PAPER_LATENCY_SAMPLE_MAX,
): void {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return;
  samples.push(Math.round(n));
  if (samples.length > maxSize) samples.splice(0, samples.length - maxSize);
}

function percentile(values: number[], p: number, fallback: number): number {
  const clean = values
    .filter((v) => Number.isFinite(v) && v >= 0)
    .sort((a, b) => a - b);
  if (!clean.length) return fallback;
  const idx = Math.min(
    clean.length - 1,
    Math.max(0, Math.ceil(clean.length * p) - 1),
  );
  return clean[idx];
}

function getLatencyStats(samples: number[], fallback: number): LatencyStats {
  return {
    count: samples.length,
    last: samples.length ? samples[samples.length - 1] : null,
    p50: percentile(samples, 0.5, fallback),
    p80: percentile(samples, 0.8, fallback),
    p95: percentile(samples, 0.95, fallback),
  };
}

function clampNumber(value: number, min: number, max: number): number {
  return Math.min(max, Math.max(min, value));
}

function recordWsLatencyUpdate(now = Date.now()): void {
  if (lastWsBookUpdateAt > 0) {
    const interval = now - lastWsBookUpdateAt;
    if (interval >= 5 && interval <= 10000)
      recordLatencySample(wsUpdateIntervalSamples, interval);
  }
  lastWsBookUpdateAt = now;
}

function getPolymarketNowMs(): number {
  return Date.now() + polymarketClockOffsetMs;
}

function getPolymarketClockAgeMs(now = Date.now()): number | null {
  return polymarketClockSyncedAt > 0 ? now - polymarketClockSyncedAt : null;
}

function updatePolymarketClockFromHeaders(
  headers: Headers,
  startedAt: number,
  endedAt: number,
  source: string,
): void {
  const dateHeader = headers.get("date");
  if (!dateHeader) return;
  const serverMs = Date.parse(dateHeader);
  if (!Number.isFinite(serverMs)) return;
  const midpoint = (startedAt + endedAt) / 2;
  const nextOffset = serverMs - midpoint;
  if (!Number.isFinite(nextOffset)) return;
  polymarketClockOffsetMs =
    polymarketClockSyncedAt > 0
      ? polymarketClockOffsetMs * 0.7 + nextOffset * 0.3
      : nextOffset;
  polymarketClockSyncedAt = endedAt;
  polymarketClockSource = source;
  polymarketClockLatencyMs = endedAt - startedAt;
}

function createEnvStrategyConfig(): StrategyConfig {
  const enabled = {} as Record<StrategyKey, boolean>;
  const amount = {} as Record<StrategyKey, number>;
  for (const key of ALL_STRATEGY_KEYS) {
    const upper = key.toUpperCase();
    enabled[key] = parseBooleanEnv(`STRATEGY_${upper}_ENABLED`, false);
    amount[key] = parseNumberEnv(`STRATEGY_${upper}_AMOUNT`, 1, 0.01);
  }
  return {
    enabled,
    amount,
    slippage: parseNumberEnv("ORDER_DEFAULT_SLIPPAGE", 0.05, 0),
    autoClaimEnabled: parseBooleanEnv("AUTO_CLAIM_ENABLED", false),
    maxRoundEntries: parseNumberEnv("MAX_ROUND_ENTRIES", 1, 1),
    marketHoursOnly: parseBooleanEnv("MARKET_HOURS_ONLY", false),
    executionMode: process.env.TRADING_MODE === "live" ? "live" : "paper",
    s10TailMultipliers: {
      earlyProbe: parseNumberEnv("S10_TAIL_EARLY_PROBE_MULT", DEFAULT_S10_TAIL_MULTIPLIERS.earlyProbe, S10_TAIL_MULT_MIN),
      probe: parseNumberEnv("S10_TAIL_PROBE_MULT", DEFAULT_S10_TAIL_MULTIPLIERS.probe, S10_TAIL_MULT_MIN),
      robust: parseNumberEnv("S10_TAIL_ROBUST_MULT", DEFAULT_S10_TAIL_MULTIPLIERS.robust, S10_TAIL_MULT_MIN),
      certainty: parseNumberEnv("S10_TAIL_CERTAINTY_MULT", DEFAULT_S10_TAIL_MULTIPLIERS.certainty, S10_TAIL_MULT_MIN),
    },
  };
}

function cloneStrategyConfig(config: StrategyConfig): StrategyConfig {
  return {
    enabled: { ...config.enabled },
    amount: { ...config.amount },
    slippage: config.slippage,
    autoClaimEnabled: config.autoClaimEnabled,
    maxRoundEntries: config.maxRoundEntries,
    marketHoursOnly: config.marketHoursOnly,
    executionMode: config.executionMode,
    s10TailMultipliers: { ...config.s10TailMultipliers },
  };
}

function loadPersistedStrategyConfig(config: StrategyConfig): void {
  if (!existsSync(STRATEGY_CONFIG_FILE)) return;
  try {
    const raw = JSON.parse(readFileSync(STRATEGY_CONFIG_FILE, "utf-8"));
    if (typeof raw.maxRoundEntries === "number" && raw.maxRoundEntries >= 1) {
      config.maxRoundEntries = Math.floor(raw.maxRoundEntries);
    }
    if (typeof raw.marketHoursOnly === "boolean") {
      config.marketHoursOnly = raw.marketHoursOnly;
    }
    if (typeof raw.autoClaimEnabled === "boolean") {
      config.autoClaimEnabled = raw.autoClaimEnabled;
    }
    if (typeof raw.slippage === "number" && raw.slippage >= 0) {
      config.slippage = raw.slippage;
    }
    if (isRecord(raw.enabled)) {
      for (const key of ALL_STRATEGY_KEYS) {
        if (!(key in raw.enabled)) continue;
        const parsed = parseBooleanLike(raw.enabled[key]);
        if (parsed != null) config.enabled[key] = parsed;
      }
    }
    if (isRecord(raw.amount)) {
      for (const key of ALL_STRATEGY_KEYS) {
        if (!(key in raw.amount)) continue;
        const parsed = parseNumberLike(raw.amount[key], 0.01);
        if (parsed != null) config.amount[key] = parsed;
      }
    }
    if (raw.executionMode === "paper" || raw.executionMode === "live") {
      config.executionMode = raw.executionMode;
    }
    if (isRecord(raw.s10TailMultipliers)) {
      config.s10TailMultipliers = normalizeS10TailMultipliers(
        raw.s10TailMultipliers,
        config.s10TailMultipliers,
      );
    }
  } catch {
    // ignore
  }
}

function savePersistedStrategyConfig(config: StrategyConfig): void {
  try {
    safeWriteTextFile(
      STRATEGY_CONFIG_FILE,
      JSON.stringify(
        {
          maxRoundEntries: config.maxRoundEntries,
          marketHoursOnly: config.marketHoursOnly,
          autoClaimEnabled: config.autoClaimEnabled,
          slippage: config.slippage,
          enabled: config.enabled,
          amount: config.amount,
          executionMode: config.executionMode,
          s10TailMultipliers: config.s10TailMultipliers,
        },
        null,
        2,
      ),
      "strategy-config",
    );
  } catch (err) {
    console.warn(
      `[StrategyConfig] 持久化保存失败: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

function applyStrategyConfigUpdate(
  current: StrategyConfig,
  rawUpdate: unknown,
): { config?: StrategyConfig; error?: string } {
  if (!isRecord(rawUpdate)) return { error: "配置格式错误" };
  const next = cloneStrategyConfig(current);

  if ("enabled" in rawUpdate) {
    if (!isRecord(rawUpdate.enabled)) return { error: "enabled 配置格式错误" };
    for (const key of ALL_STRATEGY_KEYS) {
      if (!(key in rawUpdate.enabled)) continue;
      const parsed = parseBooleanLike(rawUpdate.enabled[key]);
      if (parsed == null) return { error: `${key} 开关必须是布尔值` };
      next.enabled[key] = parsed;
    }
  }

  if ("amount" in rawUpdate) {
    if (!isRecord(rawUpdate.amount)) return { error: "amount 配置格式错误" };
    for (const key of ALL_STRATEGY_KEYS) {
      if (!(key in rawUpdate.amount)) continue;
      const parsed = parseNumberLike(rawUpdate.amount[key], 0.01);
      if (parsed == null) return { error: `${key} 金额必须大于等于 0.01` };
      next.amount[key] = parsed;
    }
  }

  if ("slippage" in rawUpdate) {
    const parsed = parseNumberLike(rawUpdate.slippage, 0);
    if (parsed == null) return { error: "slippage 必须大于等于 0" };
    next.slippage = parsed;
  }

  if ("autoClaimEnabled" in rawUpdate) {
    const parsed = parseBooleanLike(rawUpdate.autoClaimEnabled);
    if (parsed == null) return { error: "autoClaimEnabled 必须是布尔值" };
    next.autoClaimEnabled = parsed;
  }

  if ("maxRoundEntries" in rawUpdate) {
    const parsed = parseNumberLike(rawUpdate.maxRoundEntries, 1);
    if (parsed == null || !Number.isInteger(parsed))
      return { error: "maxRoundEntries 必须是大于等于1的整数" };
    next.maxRoundEntries = parsed;
  }

  if ("marketHoursOnly" in rawUpdate) {
    const parsed = parseBooleanLike(rawUpdate.marketHoursOnly);
    if (parsed == null) return { error: "marketHoursOnly 必须是布尔值" };
    next.marketHoursOnly = parsed;
  }

  if ("executionMode" in rawUpdate) {
    const parsed = parseExecutionMode(rawUpdate.executionMode);
    if (parsed == null) return { error: "executionMode 必须是 paper 或 live" };
    next.executionMode = parsed;
  }

  if ("s10TailMultipliers" in rawUpdate) {
    const parsed = parseS10TailMultipliersUpdate(
      rawUpdate.s10TailMultipliers,
      next.s10TailMultipliers,
    );
    if (!parsed.value) return { error: parsed.error || "S10 tail 倍数配置错误" };
    next.s10TailMultipliers = parsed.value;
  }

  return { config: next };
}

let strategyConfig = createEnvStrategyConfig();
loadPersistedStrategyConfig(strategyConfig);
let tradeHistory: TradeHistoryItem[] = loadTradeHistory();
let paperTradeHistory: TradeHistoryItem[] = loadPaperTradeHistory();
let executionEvents: ExecutionEventItem[] = loadExecutionEvents();
let paperAccount: PaperAccountState = loadPaperAccountState();
const pendingTradeMeta = new Map<string, PendingTradeMeta>();
let paperMakerOrders: PaperMakerOrder[] = [];
let paperMakerFilledCount = 0;
let paperMakerMergedCount = 0;
let paperMakerLastFill: MakerStatusSnapshot["lastFill"] = null;
let paperMakerLastReason = "";
let paperMakerLastFillAt: Record<StrategyDirection, number> = {
  up: 0,
  down: 0,
};
let liveMakerOrders: LiveMakerOrder[] = [];
let liveMakerFilledCount = 0;
let liveMakerCanceledCount = 0;
let liveMakerLastFill: MakerStatusSnapshot["lastFill"] = null;
let liveMakerLastReason = "";
let liveMakerLastFillAt: Record<StrategyDirection, number> = { up: 0, down: 0 };
let liveMakerLastSyncAt = 0;
let liveMakerReconciling = false;
let s10TerminalSweepInFlight = false;
let s10TerminalSweepLastAt = 0;
let s10TerminalSweepLastReason = "";
let s10TerminalSweepAttemptWindowStart = 0;
let s10TerminalSweepAttemptCount = 0;

function loadTradeHistory(): TradeHistoryItem[] {
  if (!existsSync(TRADE_HISTORY_FILE)) return [];
  try {
    const raw = JSON.parse(
      readFileSync(TRADE_HISTORY_FILE, "utf-8"),
    ) as unknown;
    if (!Array.isArray(raw)) return [];
    const filtered = raw
      .filter(
        (item): item is TradeHistoryItem =>
          isRecord(item) &&
          typeof item.id === "string" &&
          typeof item.ts === "number" &&
          typeof item.windowStart === "number" &&
          (item.side === "buy" || item.side === "sell") &&
          (item.direction === "up" || item.direction === "down") &&
          typeof item.amount === "number" &&
          typeof item.status === "string" &&
          typeof item.source === "string" &&
          (typeof item.price === "number" ||
            typeof item.worstPrice === "number"),
      )
      .slice(0, TRADE_HISTORY_MAX);
    return applyTradeHistoryMetrics(filtered);
  } catch (err) {
    console.warn(
      `[TradeHistory] 读取失败: ${err instanceof Error ? err.message : String(err)}`,
    );
    return [];
  }
}

function persistTradeHistory(): void {
  safeWriteTextFile(
    TRADE_HISTORY_FILE,
    `${JSON.stringify(tradeHistory, null, 2)}\n`,
    "trade-history",
  );
}

function getTradeHistoryPrice(item: TradeHistoryItem): number | null {
  const candidate =
    typeof item.price === "number"
      ? item.price
      : typeof item.worstPrice === "number"
        ? item.worstPrice
        : NaN;
  return Number.isFinite(candidate) ? candidate : null;
}

function roundMoney(value: number): number {
  return Math.round(value * 100) / 100;
}

function applyTradeHistoryMetrics(
  items: TradeHistoryItem[],
): TradeHistoryItem[] {
  const lots = new Map<string, Array<{ amount: number; price: number }>>();
  const ordered = [...items].sort((a, b) => a.ts - b.ts);
  for (const item of ordered) {
    const price = getTradeHistoryPrice(item);
    item.price = price;
    item.pnl = null;
    if (price == null || !Number.isFinite(item.amount) || item.amount <= 0)
      continue;
    const lotKey = `${Number.isFinite(item.windowStart) ? item.windowStart : 0}:${item.direction}`;
    const directionLots = lots.get(lotKey) ?? [];
    lots.set(lotKey, directionLots);
    if (item.side === "buy") {
      directionLots.push({ amount: item.amount, price });
      continue;
    }
    let remaining = item.amount;
    let realizedPnl = 0;
    let matchedAmount = 0;
    while (remaining > 1e-8 && directionLots.length > 0) {
      const lot = directionLots[0];
      const matched = Math.min(remaining, lot.amount);
      realizedPnl += (price - lot.price) * matched;
      lot.amount -= matched;
      remaining -= matched;
      matchedAmount += matched;
      if (lot.amount <= 1e-8) directionLots.shift();
    }
    if (matchedAmount > 0) {
      item.pnl = roundMoney(realizedPnl);
    }
  }
  return items.sort((a, b) => b.ts - a.ts);
}

function normalizeExecutionEventItem(item: unknown): ExecutionEventItem | null {
  if (!isRecord(item)) return null;
  if (
    typeof item.id !== "string" ||
    typeof item.ts !== "number" ||
    typeof item.windowStart !== "number" ||
    typeof item.event !== "string" ||
    typeof item.source !== "string" ||
    (item.executionMode !== "paper" && item.executionMode !== "live")
  )
    return null;
  return item as unknown as ExecutionEventItem;
}

function loadExecutionEvents(): ExecutionEventItem[] {
  if (!existsSync(EXECUTION_EVENTS_FILE)) return [];
  try {
    const raw = JSON.parse(readFileSync(EXECUTION_EVENTS_FILE, "utf-8"));
    if (!Array.isArray(raw)) return [];
    return raw
      .map(normalizeExecutionEventItem)
      .filter((item): item is ExecutionEventItem => item != null)
      .slice(0, EXECUTION_EVENTS_MAX);
  } catch (err) {
    console.warn(
      `[ExecutionEvents] 读取失败: ${err instanceof Error ? err.message : String(err)}`,
    );
    return [];
  }
}

function persistExecutionEvents(): void {
  safeWriteTextFile(
    EXECUTION_EVENTS_FILE,
    `${JSON.stringify(executionEvents, null, 2)}\n`,
    "execution-events",
  );
}

function getStrategyNumberFromSource(source: string): StrategyNumber | null {
  const match = source.match(/strategy(\d+)/);
  if (!match) return null;
  const n = Number(match[1]);
  return Number.isFinite(n) && n >= 1 && n <= 10 ? (n as StrategyNumber) : null;
}

function finiteOrNull(value: unknown): number | null {
  if (value == null || value === "") return null;
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function recordExecutionEvent(item: Omit<ExecutionEventItem, "id">): void {
  const record: ExecutionEventItem = {
    id: `${item.ts}-${item.executionMode}-${item.event}-${item.source}-${Math.random().toString(36).slice(2, 8)}`,
    ...item,
  };
  executionEvents.unshift(record);
  if (executionEvents.length > EXECUTION_EVENTS_MAX) {
    executionEvents = executionEvents.slice(0, EXECUTION_EVENTS_MAX);
  }
  try {
    persistExecutionEvents();
  } catch (err) {
    console.warn(
      `[ExecutionEvents] 保存失败: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  broadcastExecutionEvents();
}

function getExecutionEventFromTrade(
  record: TradeHistoryItem,
): ExecutionEventType {
  const status = String(record.status || "").toUpperCase();
  if (record.executionMode === "paper") {
    if (status.includes("REJECT")) return "paper_order_rejected";
    if (status.includes("SETTLED")) return "paper_settled";
    if (status.includes("MERGED")) return "paper_merged";
    return "paper_order_filled";
  }
  if (
    /^strategy10maker/.test(String(record.source || "")) &&
    status.includes("MINED")
  ) {
    return "live_maker_filled";
  }
  if (status.includes("REJECT")) return "live_order_rejected";
  return "live_order_mined";
}

function recordExecutionEventFromTrade(record: TradeHistoryItem): void {
  const price = finiteOrNull(
    record.avgPrice ?? record.price ?? record.worstPrice,
  );
  const filledShares = finiteOrNull(record.filledShares ?? record.amount);
  const filledNotional = finiteOrNull(
    record.filledNotional ??
      (filledShares != null && price != null ? filledShares * price : null),
  );
  recordExecutionEvent({
    ts: record.ts,
    executionMode: record.executionMode ?? "live",
    event: getExecutionEventFromTrade(record),
    windowStart: record.windowStart,
    source: record.source,
    strategy: getStrategyNumberFromSource(record.source),
    side: record.side,
    direction: record.direction,
    orderId: record.orderId ?? null,
    makerOrderId: record.makerOrderId ?? null,
    tokenId: record.bookTokenId ?? null,
    status: record.status,
    reason: record.rejectReason ?? record.exitReason ?? null,
    price: finiteOrNull(record.price),
    avgPrice: finiteOrNull(record.avgPrice),
    worstPrice: finiteOrNull(record.worstPrice),
    shares: finiteOrNull(record.amount),
    requestedShares: finiteOrNull(record.requestedShares),
    filledShares,
    notional: filledNotional,
    requestedNotional: finiteOrNull(record.requestedAmount),
    filledNotional,
    topBid: finiteOrNull(record.topBid),
    topAsk: finiteOrNull(record.topAsk),
    spread: finiteOrNull(record.spread),
    bookAgeMs: finiteOrNull(record.bookLatencyMs),
    bookSource: record.paperBookCheckStatus ? "paper-book-check" : null,
    bookCheckStatus: record.paperBookCheckStatus ?? null,
    bookCheckReason: record.paperBookCheckReason ?? null,
    bookCheckDiffPct: finiteOrNull(record.paperBookDiffPct),
    bookLatencyMs: finiteOrNull(record.bookLatencyMs),
    latencyMs: finiteOrNull(record.simLatencyMs),
    totalLatencyMs: finiteOrNull(record.totalLatencyMs),
  });
}

// ── Polymarket 真实盈亏管理器 ─────────────────────────────────
const pmPnlManager = new PmPnlManager(PROXY_ADDRESS);

function recordTradeHistory(item: Omit<TradeHistoryItem, "id">): void {
  const record: TradeHistoryItem = {
    id: `${item.ts}-${item.side}-${item.direction}-${item.source}-${Math.random().toString(36).slice(2, 8)}`,
    executionMode: item.executionMode ?? "live",
    ...item,
  };
  tradeHistory.unshift(record);
  if (tradeHistory.length > TRADE_HISTORY_MAX) {
    tradeHistory = tradeHistory.slice(0, TRADE_HISTORY_MAX);
  }
  tradeHistory = applyTradeHistoryMetrics(tradeHistory);
  try {
    persistTradeHistory();
  } catch (err) {
    console.warn(
      `[TradeHistory] 保存失败: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
  // 记录 txHash → source 映射，供 Polymarket PnL 标注策略来源
  recordExecutionEventFromTrade(record);
  if (record.txHash && record.source) {
    pmPnlManager.recordStrategySource(record.txHash, record.source);
  }
  // 触发增量同步（不阻塞）
  if (record.txHash) {
    console.log(
      `[PmPnl] 下单触发增量同步 (tx: ${record.txHash.slice(0, 10)}...)`,
    );
    pmPnlManager
      .syncIncremental()
      .then(() => {
        console.log(`[PmPnl] 下单增量完成 → broadcast`);
        broadcastPmPnl();
      })
      .catch((err) => {
        console.warn(
          `[PmPnl] 下单增量失败: ${err instanceof Error ? err.message : String(err)}`,
        );
      });
  } else {
    console.log(`[PmPnl] 跳过增量同步（无 txHash）`);
  }
  broadcastTradeHistory();
}

function createPaperAccountState(): PaperAccountState {
  return {
    usdc: PAPER_INITIAL_USDC,
    localSize: {},
    realizedPnl: 0,
    resetAt: Date.now(),
    lastTradeAt: 0,
    windows: {},
  };
}

function normalizeTradeHistoryItem(item: unknown): TradeHistoryItem | null {
  if (!isRecord(item)) return null;
  if (
    typeof item.id !== "string" ||
    typeof item.ts !== "number" ||
    typeof item.windowStart !== "number" ||
    (item.side !== "buy" && item.side !== "sell") ||
    (item.direction !== "up" && item.direction !== "down") ||
    typeof item.amount !== "number" ||
    typeof item.status !== "string" ||
    typeof item.source !== "string"
  )
    return null;
  return item as unknown as TradeHistoryItem;
}

function loadPaperTradeHistory(): TradeHistoryItem[] {
  if (!existsSync(PAPER_TRADE_HISTORY_FILE)) return [];
  try {
    const raw = JSON.parse(readFileSync(PAPER_TRADE_HISTORY_FILE, "utf-8"));
    if (!Array.isArray(raw)) return [];
    return applyTradeHistoryMetrics(
      raw
        .map(normalizeTradeHistoryItem)
        .filter((item): item is TradeHistoryItem => item != null)
        .slice(0, PAPER_TRADE_HISTORY_MAX),
    );
  } catch {
    return [];
  }
}

function persistPaperTradeHistory(): void {
  safeWriteTextFile(
    PAPER_TRADE_HISTORY_FILE,
    `${JSON.stringify(paperTradeHistory, null, 2)}\n`,
    "paper-trade-history",
  );
}

function loadPaperAccountState(): PaperAccountState {
  if (!existsSync(PAPER_STATE_FILE)) return createPaperAccountState();
  try {
    const raw = JSON.parse(readFileSync(PAPER_STATE_FILE, "utf-8"));
    if (!isRecord(raw)) return createPaperAccountState();
    const next = createPaperAccountState();
    if (typeof raw.usdc === "number" && Number.isFinite(raw.usdc))
      next.usdc = raw.usdc;
    if (typeof raw.realizedPnl === "number" && Number.isFinite(raw.realizedPnl))
      next.realizedPnl = raw.realizedPnl;
    if (typeof raw.resetAt === "number") next.resetAt = raw.resetAt;
    if (typeof raw.lastTradeAt === "number") next.lastTradeAt = raw.lastTradeAt;
    if (isRecord(raw.localSize)) {
      for (const [tokenId, size] of Object.entries(raw.localSize)) {
        const n = Number(size);
        if (tokenId && Number.isFinite(n) && n > 0) next.localSize[tokenId] = n;
      }
    }
    if (isRecord(raw.windows)) {
      for (const [windowStart, info] of Object.entries(raw.windows)) {
        if (
          isRecord(info) &&
          typeof info.upTokenId === "string" &&
          typeof info.downTokenId === "string"
        ) {
          next.windows[windowStart] = {
            upTokenId: info.upTokenId,
            downTokenId: info.downTokenId,
            eventStartTime:
              typeof info.eventStartTime === "string"
                ? info.eventStartTime
                : undefined,
            endDate:
              typeof info.endDate === "string" ? info.endDate : undefined,
            settled: info.settled === true,
            result:
              info.result === "up" || info.result === "down"
                ? info.result
                : null,
            localResult:
              info.localResult === "up" || info.localResult === "down"
                ? info.localResult
                : null,
            priceToBeat:
              typeof info.priceToBeat === "number" &&
              Number.isFinite(info.priceToBeat)
                ? info.priceToBeat
                : null,
            closePrice:
              typeof info.closePrice === "number" &&
              Number.isFinite(info.closePrice)
                ? info.closePrice
                : null,
            closeDiff:
              typeof info.closeDiff === "number" &&
              Number.isFinite(info.closeDiff)
                ? info.closeDiff
                : null,
            closeCapturedAt:
              typeof info.closeCapturedAt === "number" &&
              Number.isFinite(info.closeCapturedAt)
                ? info.closeCapturedAt
                : 0,
            settlementSource:
              typeof info.settlementSource === "string"
                ? info.settlementSource
                : "",
            upMark:
              typeof info.upMark === "number" && Number.isFinite(info.upMark)
                ? info.upMark
                : null,
            downMark:
              typeof info.downMark === "number" &&
              Number.isFinite(info.downMark)
                ? info.downMark
                : null,
            markUpdatedAt:
              typeof info.markUpdatedAt === "number" &&
              Number.isFinite(info.markUpdatedAt)
                ? info.markUpdatedAt
                : 0,
          };
        }
      }
    }
    return next;
  } catch {
    return createPaperAccountState();
  }
}

function persistPaperAccountState(): void {
  safeWriteTextFile(
    PAPER_STATE_FILE,
    `${JSON.stringify(paperAccount, null, 2)}\n`,
    "paper-account-state",
  );
}

function recordPaperTradeHistory(item: Omit<TradeHistoryItem, "id">): void {
  const record: TradeHistoryItem = {
    id: `${item.ts}-${item.side}-${item.direction}-${item.source}-${Math.random().toString(36).slice(2, 8)}`,
    executionMode: "paper",
    ...item,
  };
  paperTradeHistory.unshift(record);
  if (paperTradeHistory.length > PAPER_TRADE_HISTORY_MAX) {
    paperTradeHistory = paperTradeHistory.slice(0, PAPER_TRADE_HISTORY_MAX);
  }
  paperTradeHistory = applyTradeHistoryMetrics(paperTradeHistory);
  paperAccount.realizedPnl = roundMoney(
    paperTradeHistory.reduce(
      (sum, trade) => sum + (typeof trade.pnl === "number" ? trade.pnl : 0),
      0,
    ),
  );
  persistPaperTradeHistory();
  persistPaperAccountState();
  recordExecutionEventFromTrade(record);
  broadcastPaperTradeHistory();
}

function getPaperDirectionSize(direction: StrategyDirection | null): number {
  const tokenId = getDirectionTokenId(direction);
  return tokenId ? (paperAccount.localSize[tokenId] ?? 0) : 0;
}

function paperPositionKey(
  windowStart: number,
  direction: StrategyDirection,
): string {
  return `${Number.isFinite(windowStart) ? windowStart : 0}:${direction}`;
}

function getPaperOpenCostBasis(): Map<
  string,
  { shares: number; cost: number }
> {
  const lots = new Map<string, Array<{ amount: number; price: number }>>();
  const ordered = [...paperTradeHistory].sort((a, b) => a.ts - b.ts);
  for (const item of ordered) {
    const price = getTradeHistoryPrice(item);
    const amount = Number(item.amount);
    if (price == null || !Number.isFinite(amount) || amount <= 0) continue;
    if (String(item.status || "").includes("REJECT")) continue;
    const key = paperPositionKey(item.windowStart, item.direction);
    const positionLots = lots.get(key) ?? [];
    lots.set(key, positionLots);
    if (item.side === "buy") {
      positionLots.push({ amount, price });
      continue;
    }
    let remaining = amount;
    while (remaining > 1e-8 && positionLots.length > 0) {
      const lot = positionLots[0];
      const matched = Math.min(remaining, lot.amount);
      lot.amount -= matched;
      remaining -= matched;
      if (lot.amount <= 1e-8) positionLots.shift();
    }
  }

  const costs = new Map<string, { shares: number; cost: number }>();
  for (const [key, positionLots] of lots) {
    let shares = 0;
    let cost = 0;
    for (const lot of positionLots) {
      if (lot.amount <= 1e-8) continue;
      shares += lot.amount;
      cost += lot.amount * lot.price;
    }
    if (shares > 1e-8) costs.set(key, { shares, cost });
  }
  return costs;
}

function cleanPaperPrice(value: unknown): number | null {
  const n = Number(value);
  return Number.isFinite(n) ? clampNumber(n, 0, 1) : null;
}

function getPaperOpenPositions(): Array<{
  windowStart: number;
  direction: StrategyDirection;
  tokenId: string;
  size: number;
  info: PaperWindowInfo | null;
}> {
  const out: Array<{
    windowStart: number;
    direction: StrategyDirection;
    tokenId: string;
    size: number;
    info: PaperWindowInfo | null;
  }> = [];
  const seenTokens = new Set<string>();

  for (const [windowKey, info] of Object.entries(paperAccount.windows)) {
    const windowStart = Number(windowKey);
    if (!Number.isFinite(windowStart)) continue;
    const upSize = paperAccount.localSize[info.upTokenId] ?? 0;
    const downSize = paperAccount.localSize[info.downTokenId] ?? 0;
    seenTokens.add(info.upTokenId);
    seenTokens.add(info.downTokenId);
    if (upSize > 1e-8)
      out.push({
        windowStart,
        direction: "up",
        tokenId: info.upTokenId,
        size: upSize,
        info,
      });
    if (downSize > 1e-8)
      out.push({
        windowStart,
        direction: "down",
        tokenId: info.downTokenId,
        size: downSize,
        info,
      });
  }

  for (const [tokenId, size] of Object.entries(paperAccount.localSize)) {
    if (seenTokens.has(tokenId) || size <= 1e-8) continue;
    const direction =
      tokenId === state.upTokenId
        ? "up"
        : tokenId === state.downTokenId
          ? "down"
          : null;
    if (direction)
      out.push({
        windowStart: state.windowStart,
        direction,
        tokenId,
        size,
        info: null,
      });
  }

  return out;
}

function getPaperPositionValuation(
  pos: {
    windowStart: number;
    direction: StrategyDirection;
    tokenId: string;
    size: number;
    info: PaperWindowInfo | null;
  },
  currentUpMark: number | null,
  currentDownMark: number | null,
  costs: Map<string, { shares: number; cost: number }>,
): {
  equityPrice: number;
  markPrice: number;
  source: string;
  projected: boolean;
} {
  const currentMark =
    pos.tokenId === state.upTokenId
      ? currentUpMark
      : pos.tokenId === state.downTokenId
        ? currentDownMark
        : null;
  if (currentMark != null) {
    return {
      equityPrice: currentMark,
      markPrice: currentMark,
      source: "current-book",
      projected: false,
    };
  }

  const result =
    pos.info?.result === "up" || pos.info?.result === "down"
      ? pos.info.result
      : null;
  if (result) {
    const price = pos.direction === result ? 1 : 0;
    return {
      equityPrice: price,
      markPrice: price,
      source: "official-result",
      projected: false,
    };
  }
  const localResult =
    pos.info?.localResult === "up" || pos.info?.localResult === "down"
      ? pos.info.localResult
      : null;
  if (localResult) {
    const price = pos.direction === localResult ? 1 : 0;
    return {
      equityPrice: price,
      markPrice: price,
      source: "local-close",
      projected: true,
    };
  }

  const upMark = cleanPaperPrice(pos.info?.upMark);
  const downMark = cleanPaperPrice(pos.info?.downMark);
  const directionMark = pos.direction === "up" ? upMark : downMark;
  const oppositeMark = pos.direction === "up" ? downMark : upMark;
  if (directionMark != null) {
    const marketNowSec = Math.floor(getPolymarketNowMs() / 1000);
    const expired =
      pos.windowStart < state.windowStart ||
      pos.windowStart + 300 <= marketNowSec;
    if (
      expired &&
      oppositeMark != null &&
      Math.max(directionMark, oppositeMark) >=
        PAPER_PENDING_SETTLEMENT_CONFIDENCE
    ) {
      const equityPrice = directionMark >= oppositeMark ? 1 : 0;
      return {
        equityPrice,
        markPrice: directionMark,
        source: "pending-settlement",
        projected: true,
      };
    }
    return {
      equityPrice: directionMark,
      markPrice: directionMark,
      source: "gamma-mark",
      projected: false,
    };
  }

  const cost = costs.get(paperPositionKey(pos.windowStart, pos.direction));
  if (cost && cost.shares > 1e-8) {
    const avgCost = clampNumber(cost.cost / cost.shares, 0, 1);
    return {
      equityPrice: avgCost,
      markPrice: avgCost,
      source: "cost-basis",
      projected: false,
    };
  }

  return { equityPrice: 0, markPrice: 0, source: "unpriced", projected: false };
}

function getPaperSummary(): Record<string, unknown> {
  const upSize = state.upTokenId
    ? (paperAccount.localSize[state.upTokenId] ?? 0)
    : 0;
  const downSize = state.downTokenId
    ? (paperAccount.localSize[state.downTokenId] ?? 0)
    : 0;
  const bestBid = Number(state.bestBid);
  const bestAsk = Number(state.bestAsk);
  const currentUpMark =
    Number.isFinite(bestBid) && Number.isFinite(bestAsk)
      ? clampNumber((bestBid + bestAsk) / 2, 0, 1)
      : null;
  const currentDownMark = currentUpMark != null ? 1 - currentUpMark : null;
  const costs = getPaperOpenCostBasis();
  const openPositions = getPaperOpenPositions();

  let equity = paperAccount.usdc;
  let markEquity = paperAccount.usdc;
  let totalUpSize = 0;
  let totalDownSize = 0;
  let pendingSettlementShares = 0;
  let pendingSettlementValue = 0;
  let pendingSettlementMarkValue = 0;
  let pendingProjected = false;
  const pendingWindows = new Set<number>();
  const windowSummaries = Object.entries(paperAccount.windows)
    .map(([windowStart, info]) => ({
      windowStart: Number(windowStart),
      eventStartTime: info.eventStartTime || null,
      endDate: info.endDate || null,
      settled: info.settled === true,
      result: info.result ?? info.localResult ?? null,
      officialResult: info.result ?? null,
      localResult: info.localResult ?? null,
      settlementSource: info.settlementSource || "",
      priceToBeat: info.priceToBeat ?? null,
      closePrice: info.closePrice ?? null,
      closeDiff: info.closeDiff ?? null,
      closeCapturedAt: info.closeCapturedAt || 0,
      upMark: info.upMark ?? null,
      downMark: info.downMark ?? null,
      markUpdatedAt: info.markUpdatedAt || 0,
    }))
    .filter((item) => Number.isFinite(item.windowStart))
    .sort((a, b) => b.windowStart - a.windowStart)
    .slice(0, PAPER_WINDOW_SUMMARY_MAX);
  const positionSummaries = openPositions.map((pos) => {
    const valuation = getPaperPositionValuation(
      pos,
      currentUpMark,
      currentDownMark,
      costs,
    );
    const value = pos.size * valuation.equityPrice;
    const markValue = pos.size * valuation.markPrice;
    equity += value;
    markEquity += markValue;
    if (pos.direction === "up") totalUpSize += pos.size;
    else totalDownSize += pos.size;
    const pending = pos.windowStart !== state.windowStart;
    if (pending) {
      pendingWindows.add(pos.windowStart);
      pendingSettlementShares += pos.size;
      pendingSettlementValue += value;
      pendingSettlementMarkValue += markValue;
      pendingProjected = pendingProjected || valuation.projected;
    }
    return {
      windowStart: pos.windowStart,
      direction: pos.direction,
      size: pos.size,
      price: valuation.equityPrice,
      markPrice: valuation.markPrice,
      value: roundMoney(value),
      markValue: roundMoney(markValue),
      pending,
      projected: valuation.projected,
      source: valuation.source,
    };
  });

  return {
    usdc: roundMoney(paperAccount.usdc),
    equity: roundMoney(equity),
    markEquity: roundMoney(markEquity),
    initialUsdc: PAPER_INITIAL_USDC,
    realizedPnl: roundMoney(paperAccount.realizedPnl),
    totalPnl: roundMoney(equity - PAPER_INITIAL_USDC),
    markPnl: roundMoney(markEquity - PAPER_INITIAL_USDC),
    upLocalSize: upSize,
    downLocalSize: downSize,
    totalUpLocalSize: totalUpSize,
    totalDownLocalSize: totalDownSize,
    pendingSettlementShares,
    pendingSettlementValue: roundMoney(pendingSettlementValue),
    pendingSettlementMarkValue: roundMoney(pendingSettlementMarkValue),
    pendingSettlementWindows: pendingWindows.size,
    pendingSettlementProjected: pendingProjected,
    openPositions: positionSummaries.slice(0, 20),
    windowSummaries,
    lastTradeAt: paperAccount.lastTradeAt,
    resetAt: paperAccount.resetAt,
    latencyRangeMs: { min: PAPER_MIN_LATENCY_MS, max: PAPER_MAX_LATENCY_MS },
    latencyModel: getPaperLatencyModelSnapshot(),
  };
}

function rememberPaperWindow(info: {
  windowStart: number;
  upTokenId: string;
  downTokenId: string;
  eventStartTime?: string;
  endDate?: string;
}): void {
  const key = String(info.windowStart);
  const prev = paperAccount.windows[key];
  paperAccount.windows[String(info.windowStart)] = {
    ...prev,
    upTokenId: info.upTokenId,
    downTokenId: info.downTokenId,
    eventStartTime: info.eventStartTime || prev?.eventStartTime,
    endDate: info.endDate || prev?.endDate,
    settled: prev?.settled === true,
  };
  persistPaperAccountState();
}

function settlePaperWindow(
  windowStart: number,
  result: StrategyDirection | null,
  settlementSource = "official",
): boolean {
  if (!result) return false;
  const key = String(windowStart);
  const info = paperAccount.windows[key];
  if (!info || info.settled) return false;
  const sizes: Array<[StrategyDirection, string, number]> = [
    ["up", info.upTokenId, paperAccount.localSize[info.upTokenId] ?? 0],
    ["down", info.downTokenId, paperAccount.localSize[info.downTokenId] ?? 0],
  ];
  let changed = info.result !== result || info.settled !== true;
  if (settlementSource === "official") info.result = result;
  else info.localResult = result;
  info.settlementSource = settlementSource;
  info.upMark = result === "up" ? 1 : 0;
  info.downMark = result === "down" ? 1 : 0;
  info.markUpdatedAt = Date.now();
  const ts = Date.now();
  let recordedSettlement = false;
  for (const [direction, tokenId, size] of sizes) {
    if (size <= 0.000001) continue;
    const win = direction === result;
    const price = win ? 1 : 0;
    if (win) paperAccount.usdc = roundMoney(paperAccount.usdc + size);
    recordPaperTradeHistory({
      ts,
      windowStart,
      side: "sell",
      direction,
      amount: size,
      price,
      avgPrice: price,
      worstPrice: price,
      status: "SIM_SETTLED",
      source: "paper-settlement",
      exitReason:
        settlementSource === "official"
          ? `settled:${result}`
          : `${settlementSource}:${result}`,
      filledShares: size,
      filledNotional: win ? size : 0,
    });
    paperAccount.localSize[tokenId] = 0;
    recordedSettlement = true;
    changed = true;
  }
  info.settled = true;
  if (recordedSettlement) paperAccount.lastTradeAt = ts;
  persistPaperAccountState();
  broadcastState();
  return changed;
}

function mergePaperFullSet(
  windowStart: number,
  source: string,
  reason: string,
): number {
  const info = paperAccount.windows[String(windowStart)];
  if (!info) return 0;
  const upSize = paperAccount.localSize[info.upTokenId] ?? 0;
  const downSize = paperAccount.localSize[info.downTokenId] ?? 0;
  const shares = Math.min(upSize, downSize);
  if (!(shares > 0.01)) return 0;

  paperAccount.localSize[info.upTokenId] = Math.max(0, upSize - shares);
  paperAccount.localSize[info.downTokenId] = Math.max(0, downSize - shares);
  paperAccount.usdc = roundMoney(paperAccount.usdc + shares);
  paperAccount.lastTradeAt = Date.now();
  persistPaperAccountState();
  recordPaperTradeHistory({
    ts: Date.now(),
    windowStart,
    side: "sell",
    direction: "up",
    amount: shares,
    price: 1,
    avgPrice: 1,
    worstPrice: 1,
    status: "SIM_MERGED",
    source,
    exitReason: reason,
    paperAction: "merge",
    requestedShares: shares,
    filledShares: shares,
    filledNotional: shares,
    partial: false,
  });
  broadcastState();
  return shares;
}

function broadcastPmPnl(): void {
  // 全量：前端按 range 过滤，不截断
  const events = pmPnlManager.getEvents();
  const total = pmPnlManager.getTotalPnl();
  broadcast("pmPnl", {
    events,
    total,
    initialized: pmPnlManager.isInitialized(),
  });
}

function sendPmPnlToClient(ws: WebSocket): void {
  const events = pmPnlManager.getEvents();
  const total = pmPnlManager.getTotalPnl();
  send(ws, "pmPnl", {
    events,
    total,
    initialized: pmPnlManager.isInitialized(),
  });
}

function cleanupPendingTradeMeta(now = Date.now()): void {
  for (const [key, meta] of pendingTradeMeta) {
    if (now - meta.ts > PENDING_TRADE_META_MAX_AGE_MS) {
      pendingTradeMeta.delete(key);
    }
  }
}

function rememberPendingTradeMeta(meta: Omit<PendingTradeMeta, "key">): void {
  cleanupPendingTradeMeta(meta.ts);
  const key =
    meta.orderId ||
    `pending-${meta.ts}-${Math.random().toString(36).slice(2, 8)}`;
  pendingTradeMeta.set(key, { key, ...meta });
}

function forgetPendingTradeMeta(orderId: string | undefined): void {
  if (!orderId) return;
  pendingTradeMeta.delete(orderId);
}

function normalizeTradeSide(value: unknown): "buy" | "sell" | null {
  if (typeof value !== "string") return null;
  const normalized = value.trim().toLowerCase();
  if (normalized === "buy") return "buy";
  if (normalized === "sell") return "sell";
  return null;
}

function getDirectionByAssetId(assetId: string): StrategyDirection | null {
  if (assetId === state.upTokenId) return "up";
  if (assetId === state.downTokenId) return "down";
  return null;
}

function parseTradeEventTimestamp(evt: Record<string, unknown>): number {
  const raw =
    typeof evt.match_time === "string"
      ? evt.match_time
      : typeof evt.last_update === "string"
        ? evt.last_update
        : "";
  const parsed = raw ? Date.parse(raw) : NaN;
  return Number.isFinite(parsed) ? parsed : Date.now();
}

function readEventNumber(raw: unknown): number | null {
  const parsed =
    typeof raw === "number" ? raw : typeof raw === "string" ? Number(raw) : NaN;
  return Number.isFinite(parsed) ? parsed : null;
}

function buildConsumedMakerMeta(
  meta: PendingTradeMeta,
  makerOrder: Record<string, unknown>,
): ConsumedPendingTradeMeta {
  const fillSize =
    readEventNumber(makerOrder.matched_amount) ??
    readEventNumber(makerOrder.matchedAmount) ??
    readEventNumber(makerOrder.size_matched) ??
    readEventNumber(makerOrder.matched_size) ??
    null;
  const fillPrice = readEventNumber(makerOrder.price);
  const fillAssetId =
    typeof makerOrder.asset_id === "string" && makerOrder.asset_id
      ? makerOrder.asset_id
      : null;
  return {
    ...meta,
    fillSize,
    fillPrice,
    fillAssetId,
  };
}

function consumePendingTradeMeta(
  evt: Record<string, unknown>,
): ConsumedPendingTradeMeta | null {
  cleanupPendingTradeMeta();
  if (typeof evt.taker_order_id === "string" && evt.taker_order_id) {
    const meta = pendingTradeMeta.get(evt.taker_order_id);
    if (meta) {
      if (meta.source !== "strategy10maker")
        pendingTradeMeta.delete(evt.taker_order_id);
      return meta;
    }
  }
  if (Array.isArray(evt.maker_orders)) {
    for (const makerOrder of evt.maker_orders) {
      if (
        !isRecord(makerOrder) ||
        typeof makerOrder.order_id !== "string" ||
        !makerOrder.order_id
      )
        continue;
      const meta = pendingTradeMeta.get(makerOrder.order_id);
      if (!meta) continue;
      if (meta.source !== "strategy10maker")
        pendingTradeMeta.delete(makerOrder.order_id);
      return buildConsumedMakerMeta(meta, makerOrder);
    }
  }

  const side = normalizeTradeSide(evt.side);
  const assetId = typeof evt.asset_id === "string" ? evt.asset_id : "";
  const size = typeof evt.size === "number" ? evt.size : Number(evt.size);
  if (!side || !assetId || !Number.isFinite(size)) return null;

  let bestKey: string | null = null;
  let bestScore = Number.POSITIVE_INFINITY;
  for (const [key, meta] of pendingTradeMeta) {
    const directionTokenId =
      meta.direction === "up" ? state.upTokenId : state.downTokenId;
    if (directionTokenId !== assetId || meta.side !== side) continue;
    if (Date.now() - meta.ts > 60_000) continue;
    const tolerance = Math.max(0.01, meta.amount * 0.05);
    if (Math.abs(meta.amount - size) > tolerance) continue;
    const score =
      Math.abs(meta.amount - size) * 1000 +
      Math.abs(Date.now() - meta.ts) / 1000;
    if (score < bestScore) {
      bestScore = score;
      bestKey = key;
    }
  }
  if (!bestKey) return null;
  const meta = pendingTradeMeta.get(bestKey) || null;
  if (meta && meta.source !== "strategy10maker")
    pendingTradeMeta.delete(bestKey);
  return meta;
}

// ── Polymarket 认证 ───────────────────────────────────────────
const CREDS_FILE = resolve(__dirname, ".polymarket-creds.json");

interface PolymarketCreds {
  key: string;
  secret: string;
  passphrase: string;
  address: string;
}

function adaptSigner(wallet: ethers.Wallet) {
  return {
    _signTypedData: (
      domain: Record<string, unknown>,
      types: Record<string, unknown[]>,
      value: Record<string, unknown>,
    ) =>
      wallet.signTypedData(
        domain as ethers.TypedDataDomain,
        types as Record<string, ethers.TypedDataField[]>,
        value,
      ),
    getAddress: () => Promise.resolve(wallet.address),
  };
}

function loadCreds(): PolymarketCreds | null {
  if (!existsSync(CREDS_FILE)) return null;
  try {
    const creds: PolymarketCreds = JSON.parse(
      readFileSync(CREDS_FILE, "utf-8"),
    );
    if (creds.key && creds.secret && creds.passphrase) return creds;
  } catch {
    /* 忽略 */
  }
  return null;
}

async function createClobClient(): Promise<ClobClient | null> {
  const sigType = PROXY_ADDRESS
    ? SignatureType.POLY_GNOSIS_SAFE
    : SignatureType.EOA;
  const funderAddress = PROXY_ADDRESS || undefined;
  const saved = loadCreds();

  if (saved) {
    const creds = {
      key: saved.key,
      secret: saved.secret,
      passphrase: saved.passphrase,
    };
    if (PRIVATE_KEY) {
      const signer = adaptSigner(new ethers.Wallet(PRIVATE_KEY)) as any;
      return new ClobClient({
        host: CLOB_URL,
        chain: Chain.POLYGON,
        signer,
        creds,
        signatureType: sigType,
        funderAddress,
      });
    }
    return new ClobClient({
      host: CLOB_URL,
      chain: Chain.POLYGON,
      creds,
      signatureType: sigType,
      funderAddress,
    });
  }

  if (!PRIVATE_KEY) {
    console.warn("[Auth] 未配置 POLYMARKET_PRIVATE_KEY，下单功能不可用");
    return null;
  }

  console.log("[Auth] 首次使用，通过私钥生成 Polymarket API 凭证...");
  const wallet = new ethers.Wallet(PRIVATE_KEY);
  const signer = adaptSigner(wallet) as any;
  const client = new ClobClient({
    host: CLOB_URL,
    chain: Chain.POLYGON,
    signer,
    signatureType: sigType,
    funderAddress,
  });
  const creds = await client.createOrDeriveApiKey();
  safeWriteTextFile(
    CREDS_FILE,
    JSON.stringify(
      {
        key: creds.key,
        secret: creds.secret,
        passphrase: creds.passphrase,
        address: wallet.address,
      },
      null,
      2,
    ),
    "polymarket-creds",
  );
  console.log("[Auth] 凭证已保存到 .polymarket-creds.json");
  return new ClobClient({
    host: CLOB_URL,
    chain: Chain.POLYGON,
    signer,
    creds,
    signatureType: sigType,
    funderAddress,
  });
}

// ── HTTP 服务 ─────────────────────────────────────────────────
const app = express();
app.use(express.json());
if (IS_FULL_MODE) {
  app.use(express.static(__dirname));
  app.get("/", (_req, res) => {
    res.sendFile(resolve(__dirname, "index.html"));
  });
} else {
  app.get("/", (_req, res) => {
    res.json({
      name: "btc5m-web",
      mode: APP_MODE,
      stateUrl: "/api/state",
    });
  });
}

const server = createServer(app);
const wss = IS_FULL_MODE ? new WebSocketServer({ server }) : null;
const clientSessions = new Map<WebSocket, ClientSession>();

server.on("error", (err) => {
  logLifecycle("serverError", { error: err.message });
  console.error("[Server] 错误:", err.message);
  if ((err as NodeJS.ErrnoException).code === "EADDRINUSE") {
    console.error(
      `[Server] 端口 ${PORT} 已被占用：请先关闭旧的 BTC5m-Dash 进程，或换一个 PORT。`,
    );
    process.exitCode = 1;
  }
});
wss?.on("error", (err) => {
  logLifecycle("wssError", { error: err.message });
  console.error("[WSS] 错误:", err.message);
  if ((err as NodeJS.ErrnoException).code === "EADDRINUSE") {
    process.exitCode = 1;
  }
});

// ── CLOB Client（下单用） ──────────────────────────────────────
let clobClient: ClobClient | null = null;

async function ensureClobClient(): Promise<boolean> {
  if (clobClient) return true;
  try {
    clobClient = await createClobClient();
    return clobClient != null;
  } catch (err) {
    console.error("[CLOB] 初始化失败:", err);
    return false;
  }
}

// ── 盘口状态 ──────────────────────────────────────────────────
const state = {
  windowStart: 0,
  windowEnd: 0,
  upTokenId: "",
  downTokenId: "",
  conditionId: "",
  bids: new Map<string, string>(),
  asks: new Map<string, string>(),
  bestBid: "-",
  bestAsk: "-",
  lastPrice: "-",
  lastSide: "",
  lastPriceUpdatedAt: 0,
  updatedAt: 0,
  bookUpdatedAt: 0,
  bookEventTs: 0,
  bookSource: "",
  bookCheckAt: 0,
  bookCheckLatencyMs: null as number | null,
  bookCheckDiffPct: null as number | null,
  priceToBeat: null as number | null,
  currentPrice: null as number | null,
  currentPriceUpdatedAt: 0,
  binanceOffset: null as number | null,
  priceHistory: [] as Array<{ t: number; price: number }>,
  binanceHistory: [] as Array<{ t: number; price: number }>,
  kline1m: [] as Array<Kline>,
  kline5m: [] as Array<Kline>,
};

const strategyRuntime: StrategyRuntimeState = {
  state: "IDLE",
  activeStrategy: null,
  direction: null,
  buyAmount: 0,
  posBeforeBuy: 0,
  posBeforeSell: 0,
  lockDirection: null,
  lockPosBeforeBuy: 0,
  lockTargetShares: 0,
  lockReason: "",
  locked: false,
  waitVerifyAfterSell: false,
  cleanupAfterVerify: false,
  actionTs: 0,
  prevUpPct: null,
  buyLockUntil: 0,
  positionsReady: strategyConfig.executionMode === "paper" || !PROXY_ADDRESS,
  roundEntryCount: 0,
};

let macdFilterBlockedReason = "";
let macdFilterBlockedAt = 0;

// ── 持仓状态 ──────────────────────────────────────────────────
const wsStatus = {
  market: false,
  chainlink: false,
  user: false,
  binance: false,
};
function broadcastWsStatus() {
  broadcast("wsStatus", wsStatus as unknown as Record<string, unknown>);
}
const positions = {
  usdc: null as number | null,
  usdcAllowanceStatus: "未授权" as "已授权" | "未完全授权" | "未授权",
  usdcAllowanceMin: null as number | null,
  usdcAllowanceDetails: [] as Array<{ spender: string; amount: number | null }>,
  localSize: {} as Record<string, number>,
  apiSize: {} as Record<string, number>,
  apiVerified: {} as Record<string, boolean>,
  confirmedIds: new Set<string>(),
  confirmedIdOrder: [] as string[],
  lastTradeAt: null as number | null,
  lastApiSyncAt: null as number | null,
};

// ── 广播 ──────────────────────────────────────────────────────
function send(
  ws: WebSocket,
  type: string,
  data: Record<string, unknown>,
): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  ws.send(JSON.stringify({ type, ...data }));
}

function sendTradeHistoryToClient(ws: WebSocket): void {
  send(ws, "tradeHistory", { tradeHistory });
}

function broadcastTradeHistory(): void {
  broadcast("tradeHistory", { tradeHistory });
}

function sendPaperTradeHistoryToClient(ws: WebSocket): void {
  send(ws, "paperTradeHistory", { paperTradeHistory });
}

function broadcastPaperTradeHistory(): void {
  broadcast("paperTradeHistory", { paperTradeHistory });
}

function sendExecutionEventsToClient(ws: WebSocket): void {
  send(ws, "executionEvents", {
    executionEvents: executionEvents.slice(0, 500),
  });
}

function broadcastExecutionEvents(): void {
  broadcast("executionEvents", {
    executionEvents: executionEvents.slice(0, 500),
  });
}

function getBonereaperMonitorPayload(): Record<string, unknown> {
  return {
    bonereaperMonitor: bonereaperMonitor.getSnapshot({
      currentConditionId: state.conditionId,
      currentWindowStart: state.windowStart,
    }),
  };
}

function buildBonereaperMarketSample(): BonereaperMarketSample | null {
  if (!state.windowStart || !state.windowEnd) return null;
  const bid = Number(state.bestBid);
  const ask = Number(state.bestAsk);
  const upBid = Number.isFinite(bid) && bid > 0 ? bid : null;
  const upAsk = Number.isFinite(ask) && ask > 0 ? ask : null;
  const upMid =
    upBid != null && upAsk != null
      ? clampNumber((upBid + upAsk) / 2, 0, 1)
      : null;
  const downBid = upAsk != null ? clampNumber(1 - upAsk, 0, 1) : null;
  const downAsk = upBid != null ? clampNumber(1 - upBid, 0, 1) : null;
  const downMid = upMid != null ? clampNumber(1 - upMid, 0, 1) : null;
  const fair = computeFairProbPayload();
  const technical = computeTechnicalPayload() as {
    macd1m?: { trend?: string };
    macdFast1m?: { trend?: string };
  };
  return {
    ts: Date.now(),
    exchangeTs: getPolymarketNowMs(),
    windowStart: state.windowStart,
    windowEnd: state.windowEnd,
    remSec: getStrategyRemainingSeconds(),
    priceToBeat: Number.isFinite(Number(state.priceToBeat))
      ? Number(state.priceToBeat)
      : null,
    currentPrice: Number.isFinite(Number(state.currentPrice))
      ? Number(state.currentPrice)
      : null,
    diff: getStrategyDiff(),
    upBid,
    upAsk,
    upMid,
    downBid,
    downAsk,
    downMid,
    spreadPct: upBid != null && upAsk != null ? (upAsk - upBid) * 100 : null,
    marketUpPct: fair?.upPct ?? (upMid != null ? upMid * 100 : null),
    fairUpPct: fair?.fairUp ?? null,
    biasUpPct: fair?.biasUp ?? null,
    macd1mTrend: technical.macd1m?.trend ?? null,
    macdFast1mTrend: technical.macdFast1m?.trend ?? null,
    bookAgeMs: state.bookUpdatedAt ? Date.now() - state.bookUpdatedAt : null,
    bookSource: state.bookSource || null,
  };
}

function sendBonereaperMonitorToClient(ws: WebSocket): void {
  send(ws, "bonereaperMonitor", getBonereaperMonitorPayload());
}

function broadcastBonereaperMonitor(): void {
  broadcast("bonereaperMonitor", getBonereaperMonitorPayload());
}

function createClientSession(dataMode: ClientDataMode): ClientSession {
  return {
    dataMode,
    lastStateSentAt: 0,
    stateTimer: null,
    stateDirty: false,
    stateIncludeHistory: false,
  };
}

function normalizeClientDataMode(value: unknown): ClientDataMode {
  return value === "low" ? "low" : "full";
}

function resolveClientDataModeFromUrl(
  urlValue: string | undefined,
): ClientDataMode {
  if (!urlValue) return "full";
  try {
    const url = new URL(urlValue, `http://localhost:${PORT}`);
    return normalizeClientDataMode(url.searchParams.get("dataMode"));
  } catch {
    return "full";
  }
}

function getClientSession(ws: WebSocket): ClientSession {
  let session = clientSessions.get(ws);
  if (!session) {
    session = createClientSession("full");
    clientSessions.set(ws, session);
  }
  return session;
}

function clearStateTimer(session: ClientSession): void {
  if (session.stateTimer) {
    clearTimeout(session.stateTimer);
    session.stateTimer = null;
  }
}

function getStateIntervalMs(session: ClientSession): number {
  return session.dataMode === "low"
    ? LOW_DATA_STATE_INTERVAL_MS
    : FULL_DATA_STATE_INTERVAL_MS;
}

function shouldSendRealtimeEvent(
  type: string,
  ws: WebSocket,
  session: ClientSession,
): boolean {
  if (
    session.dataMode === "low" &&
    (type === "chainlinkPrice" || type === "binancePrice")
  ) {
    return false;
  }
  if (
    (type === "chainlinkPrice" || type === "binancePrice") &&
    ws.bufferedAmount > MAX_WS_BUFFERED_BYTES
  ) {
    return false;
  }
  return true;
}

function broadcast(type: string, data: Record<string, unknown>): void {
  if (!wss) return;
  const msg = JSON.stringify({ type, ...data });
  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    const session = getClientSession(client);
    if (!shouldSendRealtimeEvent(type, client, session)) continue;
    client.send(msg);
  }
}

function trimHistory<T extends { t: number }>(
  points: T[],
  cutoff: number,
  maxPoints: number,
): void {
  while (points.length > 0 && points[0].t < cutoff) points.shift();
  if (points.length > maxPoints) points.splice(0, points.length - maxPoints);
}

function rememberBounded(
  set: Set<string>,
  order: string[],
  key: string,
  maxSize: number,
): boolean {
  if (set.has(key)) return false;
  set.add(key);
  order.push(key);
  while (order.length > maxSize) {
    const oldest = order.shift();
    if (oldest !== undefined) set.delete(oldest);
  }
  return true;
}

function prunePositionCaches(activeTokenIds: string[]): void {
  const keep = new Set(activeTokenIds.filter(Boolean));
  for (const store of [
    positions.localSize,
    positions.apiSize,
    positions.apiVerified,
  ]) {
    for (const key of Object.keys(store)) {
      if (!keep.has(key)) delete store[key];
    }
  }
}

function getDirectionTokenId(direction: StrategyDirection | null): string {
  if (direction === "up") return state.upTokenId;
  if (direction === "down") return state.downTokenId;
  return "";
}

function getDirectionLocalSize(direction: StrategyDirection | null): number {
  const tokenId = getDirectionTokenId(direction);
  if (!tokenId) return 0;
  if (strategyConfig.executionMode === "paper")
    return paperAccount.localSize[tokenId] ?? 0;
  return positions.localSize[tokenId] ?? 0;
}

function getDirectionCostPctFromBasis(
  basis: Map<string, { shares: number; cost: number }> | null,
  direction: StrategyDirection,
): number | null {
  if (!basis) return null;
  const item = basis.get(paperPositionKey(state.windowStart, direction));
  if (!item || !(item.shares > 0)) return null;
  return clampNumber((item.cost / item.shares) * 100, 0, 100);
}

function getDirectionApiSize(direction: StrategyDirection | null): number {
  const tokenId = getDirectionTokenId(direction);
  if (!tokenId) return 0;
  if (strategyConfig.executionMode === "paper")
    return paperAccount.localSize[tokenId] ?? 0;
  return positions.apiSize[tokenId] ?? 0;
}

function isDirectionVerified(direction: StrategyDirection | null): boolean {
  const tokenId = getDirectionTokenId(direction);
  if (!tokenId) return false;
  if (strategyConfig.executionMode === "paper") return true;
  return positions.apiVerified[tokenId] ?? false;
}

function hasOpenPosition(): boolean {
  return (
    getDirectionLocalSize("up") > 0.01 || getDirectionLocalSize("down") > 0.01
  );
}

function getSingleOpenPositionDirection(): StrategyDirection | null {
  const upSize = getDirectionLocalSize("up");
  const downSize = getDirectionLocalSize("down");
  const hasUp = upSize > 0.01;
  const hasDown = downSize > 0.01;
  if (hasUp && !hasDown) return "up";
  if (hasDown && !hasUp) return "down";
  return null;
}

function hasEnoughUsdcForBuy(amount: number): boolean {
  if (strategyConfig.executionMode === "paper")
    return paperAccount.usdc + 1e-6 >= amount;
  if (positions.usdc == null || !Number.isFinite(amount)) return true;
  return positions.usdc + 1e-6 >= amount;
}

function hasPendingStrategyBuyLock(now = Date.now()): boolean {
  return now < strategyRuntime.buyLockUntil;
}

function getSellableShares(direction: StrategyDirection | null): number {
  const localSize = getDirectionLocalSize(direction);
  if (localSize <= 0) return 0;
  if (isDirectionVerified(direction)) return localSize;
  return Math.max(0, localSize - UNVERIFIED_SELL_BUFFER);
}

function getLatestBinancePrice(): number | null {
  const point = state.binanceHistory[state.binanceHistory.length - 1];
  return point?.price ?? null;
}

function getProbabilitySnapshot(): { upPct: number; dnPct: number } | null {
  if (!isProbabilityReady()) return null;
  const bid = Number(state.bestBid);
  const ask = Number(state.bestAsk);
  if (!Number.isFinite(bid) || !Number.isFinite(ask)) return null;
  const mid = (bid + ask) / 2;
  return {
    upPct: Math.round(mid * 100),
    dnPct: Math.round((1 - mid) * 100),
  };
}

function getStrategyDiff(): number | null {
  const latestBinancePrice = getLatestBinancePrice();
  if (
    latestBinancePrice == null ||
    state.priceToBeat == null ||
    state.binanceOffset == null
  )
    return null;
  return latestBinancePrice - (state.priceToBeat - state.binanceOffset);
}

function getDirectionAskEstimate(
  direction: StrategyDirection | null,
): number | null {
  const bid = Number(state.bestBid);
  const ask = Number(state.bestAsk);
  if (!Number.isFinite(bid) || !Number.isFinite(ask) || bid <= 0 || ask <= 0)
    return null;
  if (direction === "up") return ask;
  if (direction === "down") return clampNumber(1 - bid, 0.01, 0.99);
  return null;
}

function getTerminalBookDislocationReason(
  direction: StrategyDirection | null,
  side: "buy" | "sell" | null,
): string | null {
  if (!TERMINAL_BOOK_GUARD_ENABLED || side !== "buy" || !direction) return null;
  const rem = getStrategyRemainingSeconds();
  if (rem < 0 || rem > TERMINAL_BOOK_GUARD_SECONDS) return null;
  const diff = getStrategyDiff();
  if (diff == null || Math.abs(diff) < TERMINAL_BOOK_GUARD_DIFF) return null;
  const winningDirection: StrategyDirection = diff >= 0 ? "up" : "down";
  if (direction !== winningDirection) return null;
  const askEstimate = getDirectionAskEstimate(direction);
  if (askEstimate == null || askEstimate > TERMINAL_BOOK_GUARD_WIN_MAX_ASK)
    return null;
  return `terminal_book_dislocation:${direction}:ask=${askEstimate.toFixed(2)}:diff=${diff.toFixed(2)}:rem=${rem.toFixed(1)}`;
}

function isStrategyOrderSource(source: string | undefined): boolean {
  return String(source || "").startsWith("strategy");
}

function getEstimatedOrderNotional(
  side: "buy" | "sell",
  amount: number,
  book?: BookSnapshot | null,
): number {
  if (side === "buy") return amount;
  const bid = book?.topBid ?? 0;
  return bid > 0 ? amount * bid : amount;
}

function getLiveOrderGuardReason(
  input: PlaceOrderInput,
  book?: BookSnapshot | null,
): string | null {
  if (strategyConfig.executionMode !== "live") return null;
  if (!liveTradingEnabled)
    return "live_trading_disabled:switch to live mode to arm live trading";
  const source = input.source || "manual";
  if (source === "strategy10maker" && !s10LiveMakerEnabled) {
    return "s10_live_maker_disabled:switch to live mode to arm S10 live maker";
  }
  const clockAge = getPolymarketClockAgeMs();
  if (clockAge == null || clockAge > POLYMARKET_CLOCK_MAX_AGE_MS) {
    return `live_clock_stale:${clockAge == null ? "none" : Math.round(clockAge)}ms`;
  }
  const bookAge =
    state.bookUpdatedAt > 0 ? Date.now() - state.bookUpdatedAt : Infinity;
  if (bookAge > LIVE_MAX_BOOK_STALE_MS)
    return `live_ws_book_stale:${Math.round(bookAge)}ms`;
  const rem = getStrategyRemainingSeconds(getPolymarketNowMs());
  const minBuyRemaining =
    source === "strategy10maker"
      ? S10_LIVE_MAKER_MIN_REMAINING_SEC
      : source === "strategy10sweep"
        ? S10_TERMINAL_SWEEP_MIN_REMAINING_SEC
        : LIVE_MIN_BUY_REMAINING_SEC;
  if (input.side === "buy" && rem < minBuyRemaining) {
    return `live_too_late_to_buy:rem=${rem.toFixed(1)}s<${minBuyRemaining.toFixed(1)}s`;
  }
  const maxOrderUsdc =
    source === "strategy10sweep"
      ? S10_TERMINAL_SWEEP_LIVE_MAX_ORDER_USDC
      : isStrategyOrderSource(source)
        ? LIVE_STRATEGY_MAX_ORDER_USDC
        : LIVE_MAX_ORDER_USDC;
  const notional = getEstimatedOrderNotional(input.side, input.amount, book);
  if (notional > maxOrderUsdc + 1e-9) {
    return `live_order_notional_cap:${notional.toFixed(2)}>${maxOrderUsdc.toFixed(2)}`;
  }
  return null;
}

function calcMedian(values: number[]): number | null {
  if (!values.length) return null;
  const sorted = values.slice().sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

function calcTrimmedMean(values: number[], trimRatio = 0.15): number | null {
  if (!values.length) return null;
  const sorted = values.slice().sort((a, b) => a - b);
  const trim = sorted.length >= 8 ? Math.floor(sorted.length * trimRatio) : 0;
  const trimmed = trim > 0 ? sorted.slice(trim, sorted.length - trim) : sorted;
  if (!trimmed.length) return null;
  return trimmed.reduce((sum, value) => sum + value, 0) / trimmed.length;
}

function calculateBinanceOffset(allowLatestFallback = false): number | null {
  if (!state.binanceHistory.length || !state.priceHistory.length) {
    if (!allowLatestFallback) return null;
    const latestBinancePrice = getLatestBinancePrice();
    if (latestBinancePrice == null || state.currentPrice == null) return null;
    return state.currentPrice - latestBinancePrice;
  }

  const now = Date.now();
  const binanceRecent = state.binanceHistory.filter(
    (point) => point.t >= now - BINANCE_ALIGN_WINDOW_MS,
  );
  const chainlinkRecent = state.priceHistory.filter(
    (point) => point.t >= now - BINANCE_ALIGN_WINDOW_MS,
  );
  if (!binanceRecent.length || !chainlinkRecent.length) {
    if (!allowLatestFallback) return null;
    const latestBinancePrice = getLatestBinancePrice();
    if (latestBinancePrice == null || state.currentPrice == null) return null;
    return state.currentPrice - latestBinancePrice;
  }

  const binanceSpan =
    binanceRecent.length >= 2
      ? binanceRecent[binanceRecent.length - 1].t - binanceRecent[0].t
      : 0;
  const chainlinkSpan =
    chainlinkRecent.length >= 2
      ? chainlinkRecent[chainlinkRecent.length - 1].t - chainlinkRecent[0].t
      : 0;

  if (Math.min(binanceSpan, chainlinkSpan) < BINANCE_ALIGN_MIN_SPAN_MS) {
    if (!allowLatestFallback) return null;
    return (
      chainlinkRecent[chainlinkRecent.length - 1].price -
      binanceRecent[binanceRecent.length - 1].price
    );
  }

  const overlapStart = Math.max(binanceRecent[0].t, chainlinkRecent[0].t);
  const overlapEnd = Math.min(
    binanceRecent[binanceRecent.length - 1].t,
    chainlinkRecent[chainlinkRecent.length - 1].t,
  );
  const diffs: number[] = [];

  if (overlapEnd - overlapStart >= BINANCE_ALIGN_BUCKET_MS * 2) {
    let binanceIdx = 0;
    let chainlinkIdx = 0;
    for (
      let bucketStart = overlapStart;
      bucketStart <= overlapEnd;
      bucketStart += BINANCE_ALIGN_BUCKET_MS
    ) {
      const bucketEnd = bucketStart + BINANCE_ALIGN_BUCKET_MS;
      const binanceBucket: number[] = [];
      const chainlinkBucket: number[] = [];

      while (
        binanceIdx < binanceRecent.length &&
        binanceRecent[binanceIdx].t < bucketStart
      )
        binanceIdx++;
      while (
        chainlinkIdx < chainlinkRecent.length &&
        chainlinkRecent[chainlinkIdx].t < bucketStart
      )
        chainlinkIdx++;

      let i = binanceIdx;
      while (i < binanceRecent.length && binanceRecent[i].t < bucketEnd) {
        binanceBucket.push(binanceRecent[i].price);
        i++;
      }
      let j = chainlinkIdx;
      while (j < chainlinkRecent.length && chainlinkRecent[j].t < bucketEnd) {
        chainlinkBucket.push(chainlinkRecent[j].price);
        j++;
      }

      const binanceMedian = calcMedian(binanceBucket);
      const chainlinkMedian = calcMedian(chainlinkBucket);
      if (binanceMedian != null && chainlinkMedian != null) {
        diffs.push(chainlinkMedian - binanceMedian);
      }
    }
  }

  if (!diffs.length) {
    return (
      chainlinkRecent[chainlinkRecent.length - 1].price -
      binanceRecent[binanceRecent.length - 1].price
    );
  }
  if (diffs.length < 5) {
    return calcTrimmedMean(diffs, 0);
  }

  const median = calcMedian(diffs);
  if (median == null) return null;
  const absDeviations = diffs.map((diff) => Math.abs(diff - median));
  const mad = calcMedian(absDeviations) ?? 0;
  const threshold = Math.max(10, mad * 3);
  const filtered = diffs.filter((diff) => Math.abs(diff - median) <= threshold);
  const stable = filtered.length >= 3 ? filtered : diffs;
  return calcTrimmedMean(stable, 0.15);
}

function refreshBinanceOffset(
  reason: string,
  options: { allowLatestFallback?: boolean; forceLog?: boolean } = {},
): boolean {
  const nextOffset = calculateBinanceOffset(
    options.allowLatestFallback ?? false,
  );
  if (nextOffset == null) return false;

  const prevOffset = state.binanceOffset;
  const changed =
    prevOffset == null ||
    Math.abs(prevOffset - nextOffset) > BINANCE_OFFSET_EPSILON;
  state.binanceOffset = nextOffset;

  if (!changed) return true;

  if (options.forceLog || prevOffset == null) {
    const prefix = prevOffset == null ? "初始化偏移" : `${reason}更新`;
    console.log(
      `[BinanceOffset] ${prefix} ${nextOffset >= 0 ? "+" : ""}${nextOffset.toFixed(2)}`,
    );
  }

  broadcastState();
  return true;
}

function maybeInitializeBinanceOffset(): void {
  if (state.binanceOffset != null) return;
  void refreshBinanceOffset("初始化", {
    allowLatestFallback: true,
    forceLog: true,
  });
}

function resetStrategyRuntime(reason?: string): void {
  strategyRuntime.state = "IDLE";
  strategyRuntime.activeStrategy = null;
  strategyRuntime.direction = null;
  strategyRuntime.buyAmount = 0;
  strategyRuntime.posBeforeBuy = 0;
  strategyRuntime.posBeforeSell = 0;
  strategyRuntime.lockDirection = null;
  strategyRuntime.lockPosBeforeBuy = 0;
  strategyRuntime.lockTargetShares = 0;
  strategyRuntime.lockReason = "";
  strategyRuntime.locked = false;
  strategyRuntime.waitVerifyAfterSell = false;
  strategyRuntime.cleanupAfterVerify = false;
  strategyRuntime.actionTs = 0;
  strategyRuntime.prevUpPct = null;
  strategyRuntime.buyLockUntil = 0;
  strategyRuntime.roundEntryCount = 0;
  macdFilterBlockedReason = "";
  macdFilterBlockedAt = 0;
  for (const s of getAllStrategies()) s.resetState();
  if (reason) console.log(`[Strategy] 重置: ${reason}`);
}

function strategyKeyOf(strategy: StrategyNumber): StrategyKey {
  return `s${strategy}` as StrategyKey;
}

function transitionToDone(): void {
  if (
    strategyRuntime.roundEntryCount < strategyConfig.maxRoundEntries &&
    anyStrategyEnabled()
  ) {
    console.log(
      `[Strategy${strategyRuntime.activeStrategy ?? ""}] 完成，回到扫描(${strategyRuntime.roundEntryCount}/${strategyConfig.maxRoundEntries})`,
    );
    strategyRuntime.state = "SCANNING";
    strategyRuntime.activeStrategy = null;
    strategyRuntime.direction = null;
    strategyRuntime.buyAmount = 0;
    strategyRuntime.posBeforeBuy = 0;
    strategyRuntime.posBeforeSell = 0;
    strategyRuntime.lockDirection = null;
    strategyRuntime.lockPosBeforeBuy = 0;
    strategyRuntime.lockTargetShares = 0;
    strategyRuntime.lockReason = "";
    strategyRuntime.locked = false;
    strategyRuntime.waitVerifyAfterSell = false;
    strategyRuntime.cleanupAfterVerify = false;
    strategyRuntime.actionTs = 0;
  } else {
    strategyRuntime.state = "DONE";
  }
  broadcastState();
}

function anyStrategyEnabled(): boolean {
  return ALL_STRATEGY_KEYS.some((key) => strategyConfig.enabled[key]);
}

function hasConfirmedBuyPosition(): boolean {
  return (
    strategyRuntime.direction != null &&
    getDirectionLocalSize(strategyRuntime.direction) >
      strategyRuntime.posBeforeBuy + 0.01
  );
}

function oppositeDirection(direction: StrategyDirection): StrategyDirection {
  return direction === "up" ? "down" : "up";
}

function hasConfirmedLockPosition(): boolean {
  return (
    strategyRuntime.lockDirection != null &&
    getDirectionLocalSize(strategyRuntime.lockDirection) >
      strategyRuntime.lockPosBeforeBuy + 0.01
  );
}

function canReleaseUnconfirmedBuy(now = Date.now()): boolean {
  if (now - strategyRuntime.actionTs < FILL_RECONCILE_TIMEOUT_MS) return false;
  if (!strategyRuntime.direction) return true;
  if ((positions.lastApiSyncAt ?? 0) <= strategyRuntime.actionTs) return false;
  return (
    getDirectionApiSize(strategyRuntime.direction) <=
    strategyRuntime.posBeforeBuy + 0.01
  );
}

function buildLiveSafetyPayload(): Record<string, unknown> {
  const bookAgeMs =
    state.bookUpdatedAt > 0 ? Date.now() - state.bookUpdatedAt : null;
  const clockAgeMs = getPolymarketClockAgeMs();
  const cachedRules =
    liveMarketRulesCache &&
    liveMarketRulesCache.conditionId === state.conditionId
      ? liveMarketRulesCache
      : null;
  const s10MakerMode = S10_MAKER_ENGINE_ENABLED
    ? s10LiveMakerEnabled
      ? "live-enabled"
      : "paper-only"
    : "disabled";
  const ready =
    strategyConfig.executionMode !== "live"
      ? true
      : liveTradingEnabled &&
        bookAgeMs != null &&
        bookAgeMs <= LIVE_MAX_BOOK_STALE_MS &&
        clockAgeMs != null &&
        clockAgeMs <= POLYMARKET_CLOCK_MAX_AGE_MS;
  let reason = "ok";
  if (strategyConfig.executionMode === "live") {
    if (!liveTradingEnabled) reason = "live runtime switch off";
    else if (bookAgeMs == null || bookAgeMs > LIVE_MAX_BOOK_STALE_MS)
      reason = `book stale ${bookAgeMs == null ? "-" : Math.round(bookAgeMs)}ms`;
    else if (clockAgeMs == null || clockAgeMs > POLYMARKET_CLOCK_MAX_AGE_MS)
      reason = `clock stale ${clockAgeMs == null ? "-" : Math.round(clockAgeMs)}ms`;
  }
  return {
    ready,
    reason,
    liveTradingEnabled,
    s10LiveMakerEnabled,
    s10MakerMode,
    maxBookStaleMs: LIVE_MAX_BOOK_STALE_MS,
    bookWsMaxDiffPct: LIVE_BOOK_WS_MAX_DIFF * 100,
    maxOrderUsdc: LIVE_MAX_ORDER_USDC,
    strategyMaxOrderUsdc: LIVE_STRATEGY_MAX_ORDER_USDC,
    s10MakerOrderCap: getS10ConfiguredOrderCap(),
    s10MakerWindowCapConfigured: S10_MAKER_MAX_WINDOW_NOTIONAL,
    s10LiveMakerWindowCap: getS10MakerWindowNotionalCap("live"),
    s10PaperMakerLiveParityEnabled: PAPER_S10_LIVE_PARITY_ENABLED,
    s10PaperMakerLiveParityUseLiveCap: PAPER_S10_LIVE_PARITY_USE_LIVE_CAP,
    s10PaperMakerWindowCap: getS10MakerWindowNotionalCap("paper"),
    s10PaperMakerTickSize: getCachedOrFallbackLiveTickSize(),
    s10LiveMakerWindowBalanceRatio: S10_LIVE_MAKER_MAX_WINDOW_BALANCE_RATIO,
    s10MakerMinOrderNotionalFloor: getS10MakerMinimumOrderNotionalFloor(),
    minimumOrderSize:
      cachedRules?.minimumOrderSize ??
      (LIVE_MIN_ORDER_SHARES_FALLBACK > 0
        ? LIVE_MIN_ORDER_SHARES_FALLBACK
        : null),
    minimumOrderSizeSource: cachedRules?.source ?? "fallback",
    minimumTickSize: cachedRules?.minimumTickSize ?? null,
    marketRulesAgeMs: cachedRules ? Date.now() - cachedRules.fetchedAt : null,
    marketRulesError: cachedRules?.error ?? null,
    maxPriceImpactPct: LIVE_MAX_PRICE_IMPACT_PCT,
    minBuyRemainingSec: LIVE_MIN_BUY_REMAINING_SEC,
    s10MakerMinBuyRemainingSec: S10_LIVE_MAKER_MIN_REMAINING_SEC,
    s10LiveMakerActiveOrders: liveMakerOrders.filter(
      (order) => order.status === "open" || order.status === "unknown",
    ).length,
    bookAgeMs,
    clockAgeMs,
  };
}

function setLiveRuntimeSwitches(enabled: boolean, reason: string): void {
  const nextEnabled = !!enabled;
  const changed =
    liveTradingEnabled !== nextEnabled || s10LiveMakerEnabled !== nextEnabled;
  liveTradingEnabled = nextEnabled;
  s10LiveMakerEnabled = nextEnabled;
  if (changed) {
    console.log(
      `[LiveRuntime] ${nextEnabled ? "armed" : "disarmed"}: ${reason}`,
    );
  }
  if (!nextEnabled) {
    void cancelLiveMakerOrders(() => true, reason);
  }
}

function buildStrategyRuntimePayload(): Record<string, unknown> {
  const perStrategy: Record<string, Record<string, unknown>> = {};
  for (const s of getAllStrategies()) {
    perStrategy[s.key] = s.getStatePayload();
  }
  return {
    state: strategyRuntime.state,
    activeStrategy: strategyRuntime.activeStrategy,
    direction: strategyRuntime.direction,
    buyAmount: strategyRuntime.buyAmount,
    posBeforeBuy: strategyRuntime.posBeforeBuy,
    posBeforeSell: strategyRuntime.posBeforeSell,
    lockDirection: strategyRuntime.lockDirection,
    lockPosBeforeBuy: strategyRuntime.lockPosBeforeBuy,
    lockTargetShares: strategyRuntime.lockTargetShares,
    lockReason: strategyRuntime.lockReason,
    locked: strategyRuntime.locked,
    waitVerifyAfterSell: strategyRuntime.waitVerifyAfterSell,
    cleanupAfterVerify: strategyRuntime.cleanupAfterVerify,
    actionTs: strategyRuntime.actionTs,
    prevUpPct: strategyRuntime.prevUpPct,
    buyLockUntil: strategyRuntime.buyLockUntil,
    positionsReady: strategyRuntime.positionsReady,
    roundEntryCount: strategyRuntime.roundEntryCount,
    macdFilter: {
      enabled: MACD_FILTER_ENABLED,
      blockedReason: macdFilterBlockedReason,
      blockedAt: macdFilterBlockedAt || null,
    },
    liveSafety: buildLiveSafetyPayload(),
    perStrategy,
  };
}

/** 计算当前合理概率（供 s5/s10/s11 前端面板实时展示） */
function computeFairProbPayload(): {
  diff: number | null;
  rem: number;
  upPct: number | null;
  fairUp: number | null;
  biasUp: number | null;
} | null {
  const diff = getStrategyDiff();
  const rem = getStrategyRemainingSeconds();
  const snap = getProbabilitySnapshot();
  if (diff == null || !snap)
    return {
      diff,
      rem,
      upPct: snap?.upPct ?? null,
      fairUp: null,
      biasUp: null,
    };
  const fairUp = getFairProb(diff, rem);
  const biasUp = fairUp != null ? fairUp - snap.upPct : null;
  return { diff, rem, upPct: snap.upPct, fairUp, biasUp };
}

function computeTechnicalPayload(): Record<string, unknown> {
  return {
    macd1m: buildMacdSnapshot(state.kline1m, {
      fast: 12,
      slow: 26,
      signal: 9,
      confirmBars: 3,
      minHistBps: 0.02,
      minSlopeBps: 0,
    }),
    macdFast1m: buildMacdSnapshot(state.kline1m, {
      fast: 6,
      slow: 13,
      signal: 5,
      confirmBars: 3,
      minHistBps: 0.02,
      minSlopeBps: 0,
    }),
  };
}

function computeFullSetArbPayload(): FullSetArbSnapshot {
  return withFullSetAge(fullSetArbSnapshot);
}

function buildStatePayload(
  options: boolean | StatePayloadOptions = false,
): Record<string, unknown> {
  const normalized =
    typeof options === "boolean" ? { includeHistory: options } : options;
  const includeHistory = normalized.includeHistory === true;
  const simple = normalized.simple === true;
  const bids = [...state.bids.entries()]
    .map(([price, size]) => ({ price: Number(price), size: Number(size) }))
    .sort((a, b) => b.price - a.price)
    .slice(0, 8);
  const asks = [...state.asks.entries()]
    .map(([price, size]) => ({ price: Number(price), size: Number(size) }))
    .sort((a, b) => a.price - b.price)
    .slice(0, 8);

  const clockAgeMs = getPolymarketClockAgeMs();
  const payload: Record<string, unknown> = {
    windowStart: state.windowStart,
    windowEnd: state.windowEnd,
    bestBid: state.bestBid,
    bestAsk: state.bestAsk,
    probabilityReady: isProbabilityReady(),
    bookUpdatedAt: state.bookUpdatedAt,
    bookEventTs: state.bookEventTs || null,
    bookSource: state.bookSource || null,
    bookAgeMs: state.bookUpdatedAt ? Date.now() - state.bookUpdatedAt : null,
    bookStaleAfterMs: MAX_BOOK_STALE_MS,
    bookEventAgeMs: state.bookEventTs
      ? Math.max(0, Date.now() - state.bookEventTs)
      : null,
    bookCheckAt: state.bookCheckAt || null,
    bookCheckAgeMs: state.bookCheckAt ? Date.now() - state.bookCheckAt : null,
    bookCheckLatencyMs: state.bookCheckLatencyMs,
    bookCheckDiffPct: state.bookCheckDiffPct,
    lastPrice: state.lastPrice,
    lastSide: state.lastSide,
    lastPriceUpdatedAt: state.lastPriceUpdatedAt,
    updatedAt: state.updatedAt,
    priceToBeat: state.priceToBeat,
    currentPrice: state.currentPrice,
    currentPriceUpdatedAt: state.currentPriceUpdatedAt || null,
    binanceOffset: state.binanceOffset,
    klineCounts: { k1m: state.kline1m.length, k5m: state.kline5m.length },
    binanceDiff: getStrategyDiff(),
    fairProb: computeFairProbPayload(),
    technical: computeTechnicalPayload(),
    fullSetArb: computeFullSetArbPayload(),
    usdc: positions.usdc,
    usdcAllowanceStatus: positions.usdcAllowanceStatus,
    usdcAllowanceMin: positions.usdcAllowanceMin,
    upLocalSize: positions.localSize[state.upTokenId] ?? 0,
    downLocalSize: positions.localSize[state.downTokenId] ?? 0,
    upApiSize: positions.apiSize[state.upTokenId] ?? 0,
    downApiSize: positions.apiSize[state.downTokenId] ?? 0,
    upApiVerified: positions.apiVerified[state.upTokenId] ?? false,
    downApiVerified: positions.apiVerified[state.downTokenId] ?? false,
    lastTradeAt: positions.lastTradeAt,
    lastApiSyncAt: positions.lastApiSyncAt,
    executionMode: strategyConfig.executionMode,
    paper: getPaperSummary(),
    runtimeMode: APP_MODE,
    strategyConfig,
    strategy: buildStrategyRuntimePayload(),
    ts: Date.now(),
    exchangeTs: getPolymarketNowMs(),
    polymarketClockOffsetMs,
    polymarketClockAgeMs: clockAgeMs,
    polymarketClockSource: polymarketClockSource || null,
    polymarketClockLatencyMs,
    polymarketClockReady:
      clockAgeMs != null && clockAgeMs <= POLYMARKET_CLOCK_MAX_AGE_MS,
  };
  if (!simple) {
    payload.conditionId = state.conditionId;
    payload.upTokenId = state.upTokenId;
    payload.downTokenId = state.downTokenId;
    payload.bids = bids;
    payload.asks = asks;
    payload.usdcAllowanceDetails = positions.usdcAllowanceDetails;
  }
  if (includeHistory && !simple) {
    payload.priceHistory = state.priceHistory;
    payload.binanceHistory = state.binanceHistory;
  }
  return payload;
}

function sendStateToClient(
  ws: WebSocket,
  options: { includeHistory?: boolean } = {},
): void {
  const session = getClientSession(ws);
  const simple = session.dataMode === "low";
  send(
    ws,
    "state",
    buildStatePayload({
      includeHistory: options.includeHistory === true && !simple,
      simple,
    }),
  );
  session.lastStateSentAt = Date.now();
}

function scheduleStateToClient(ws: WebSocket, includeHistory = false): void {
  if (ws.readyState !== WebSocket.OPEN) return;
  const session = getClientSession(ws);
  session.stateDirty = true;
  session.stateIncludeHistory = session.stateIncludeHistory || includeHistory;
  if (session.stateTimer) return;
  const elapsed = Date.now() - session.lastStateSentAt;
  const waitMs = Math.max(0, getStateIntervalMs(session) - elapsed);
  session.stateTimer = setTimeout(() => {
    const latestSession = clientSessions.get(ws);
    if (!latestSession) return;
    latestSession.stateTimer = null;
    if (!latestSession.stateDirty || ws.readyState !== WebSocket.OPEN) return;
    const nextIncludeHistory = latestSession.stateIncludeHistory;
    latestSession.stateDirty = false;
    latestSession.stateIncludeHistory = false;
    if (ws.bufferedAmount > MAX_WS_BUFFERED_BYTES) {
      latestSession.stateDirty = true;
      latestSession.stateIncludeHistory = nextIncludeHistory;
      scheduleStateToClient(ws, nextIncludeHistory);
      return;
    }
    sendStateToClient(ws, { includeHistory: nextIncludeHistory });
  }, waitMs);
}

function broadcastState(includeHistory = false): void {
  if (!wss) return;
  for (const client of wss.clients) {
    if (client.readyState !== WebSocket.OPEN) continue;
    scheduleStateToClient(client, includeHistory);
  }
}

function applyClientConfig(ws: WebSocket, raw: unknown): void {
  if (!isRecord(raw) || raw.type !== "clientConfig") return;
  const session = getClientSession(ws);
  const nextMode = normalizeClientDataMode(raw.dataMode);
  if (session.dataMode === nextMode) return;
  session.dataMode = nextMode;
  session.stateDirty = false;
  session.stateIncludeHistory = false;
  clearStateTimer(session);
  console.log(`[WS] 客户端数据模式切换为 ${nextMode}`);
  send(ws, "clientConfig", { dataMode: nextMode });
  sendStateToClient(ws, { includeHistory: true });
}

async function fetchBookTopOfBook(
  tokenId: string,
): Promise<{ bestBid: number; bestAsk: number }> {
  const book = await fetchBookSnapshot(tokenId);
  return {
    bestBid: book.topBid,
    bestAsk: book.topAsk,
  };
}

async function fetchBookSnapshot(tokenId: string): Promise<BookSnapshot> {
  const startedAt = Date.now();
  const res = await fetch(`${CLOB_URL}/book?token_id=${tokenId}`);
  const endedAt = Date.now();
  updatePolymarketClockFromHeaders(res.headers, startedAt, endedAt, "clob");
  recordLatencySample(bookLatencySamples, endedAt - startedAt);
  const book = (await res.json()) as {
    bids?: { price: string; size?: string }[];
    asks?: { price: string; size?: string }[];
  };
  const bids = (book.bids || [])
    .map((b) => ({ price: Number(b.price), size: Number(b.size ?? 0) }))
    .filter((b) => b.price > 0 && b.size > 0)
    .sort((a, b) => b.price - a.price);
  const asks = (book.asks || [])
    .map((a) => ({ price: Number(a.price), size: Number(a.size ?? 0) }))
    .filter((a) => a.price > 0 && a.size > 0)
    .sort((a, b) => a.price - b.price);
  return {
    tokenId,
    bids,
    asks,
    topBid: bids[0]?.price ?? 0,
    topAsk: asks[0]?.price ?? 0,
    fetchedAt: Date.now(),
    latencyMs: endedAt - startedAt,
  };
}

function readLiveMarketRuleNumber(
  raw: Record<string, unknown>,
  keys: string[],
): number | null {
  for (const key of keys) {
    const value = raw[key];
    const parsed =
      typeof value === "number"
        ? value
        : typeof value === "string"
          ? Number(value)
          : NaN;
    if (Number.isFinite(parsed) && parsed > 0) return parsed;
  }
  return null;
}

function buildFallbackLiveMarketRules(
  conditionId = state.conditionId,
  error?: string,
): LiveMarketRules {
  return {
    conditionId,
    minimumOrderSize:
      LIVE_MIN_ORDER_SHARES_FALLBACK > 0
        ? LIVE_MIN_ORDER_SHARES_FALLBACK
        : null,
    minimumTickSize: null,
    fetchedAt: Date.now(),
    source: "fallback",
    error: error || null,
  };
}

async function fetchLiveMarketRules(): Promise<LiveMarketRules> {
  const conditionId = state.conditionId || "";
  const now = Date.now();
  if (
    liveMarketRulesCache &&
    liveMarketRulesCache.conditionId === conditionId &&
    now - liveMarketRulesCache.fetchedAt <= LIVE_MARKET_RULES_CACHE_MS
  ) {
    return liveMarketRulesCache;
  }

  if (!conditionId) {
    liveMarketRulesCache = buildFallbackLiveMarketRules(
      conditionId,
      "missing_condition_id",
    );
    return liveMarketRulesCache;
  }

  const startedAt = Date.now();
  try {
    const res = await fetch(`${CLOB_URL}/markets/${conditionId}`);
    const endedAt = Date.now();
    updatePolymarketClockFromHeaders(
      res.headers,
      startedAt,
      endedAt,
      "clob-market",
    );
    if (!res.ok) throw new Error(`market_rules_http_${res.status}`);
    const raw = (await res.json()) as unknown;
    if (!isRecord(raw)) throw new Error("market_rules_invalid_payload");
    const minimumOrderSize = readLiveMarketRuleNumber(raw, [
      "minimum_order_size",
      "minimumOrderSize",
      "min_order_size",
    ]);
    const minimumTickSize = readLiveMarketRuleNumber(raw, [
      "minimum_tick_size",
      "minimumTickSize",
      "min_tick_size",
    ]);
    liveMarketRulesCache = {
      conditionId,
      minimumOrderSize:
        minimumOrderSize ??
        (LIVE_MIN_ORDER_SHARES_FALLBACK > 0
          ? LIVE_MIN_ORDER_SHARES_FALLBACK
          : null),
      minimumTickSize,
      fetchedAt: Date.now(),
      source: "clob",
      error: null,
    };
    return liveMarketRulesCache;
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    if (
      liveMarketRulesCache &&
      liveMarketRulesCache.conditionId === conditionId
    ) {
      liveMarketRulesCache = {
        ...liveMarketRulesCache,
        fetchedAt: Date.now(),
        error: message,
      };
      return liveMarketRulesCache;
    }
    liveMarketRulesCache = buildFallbackLiveMarketRules(conditionId, message);
    return liveMarketRulesCache;
  }
}

function getLiveMinOrderSizeReason(
  shares: number,
  rules: LiveMarketRules,
): string | null {
  const minShares = rules.minimumOrderSize;
  if (minShares != null && minShares > 0 && shares + 1e-9 < minShares) {
    return `live_min_order_size:${shares.toFixed(2)}<${minShares.toFixed(2)}`;
  }
  return null;
}

function estimateLiveOrderSharesForMinSize(
  side: "buy" | "sell",
  amount: number,
  worstPrice: number,
  fillPreview?: { filledShares?: number; requestedShares?: number } | null,
): number {
  if (side === "sell") return amount;
  const byLimitPrice = worstPrice > 0 ? amount / worstPrice : 0;
  const byFill = Number(fillPreview?.filledShares);
  if (Number.isFinite(byFill) && byFill > 0)
    return Math.min(byLimitPrice, byFill);
  const requested = Number(fillPreview?.requestedShares);
  if (Number.isFinite(requested) && requested > 0)
    return Math.min(byLimitPrice, requested);
  return byLimitPrice;
}

function createEmptyFullSetArbSnapshot(reason: string): FullSetArbSnapshot {
  return {
    ready: false,
    status: "idle",
    reason,
    windowStart: 0,
    updatedAt: 0,
    ageMs: null,
    maxBudget: 0,
    targetShares: 0,
    totalCost: null,
    totalCostPct: null,
    grossProfit: null,
    grossProfitPct: null,
    netProfitPct: null,
    minProfitPct: S10_FULLSET_MIN_PROFIT_PCT,
    triggerProfitPct: S10_FULLSET_TRIGGER_PROFIT_PCT,
    feeBufferPct: S10_FULLSET_FEE_BUFFER_PCT,
    upAvgAsk: null,
    downAvgAsk: null,
    upTopAsk: null,
    downTopAsk: null,
    upLevelsUsed: 0,
    downLevelsUsed: 0,
    firstLegDirection: null,
    firstLegCost: null,
    secondLegDirection: null,
    secondLegCost: null,
    bookLatencyMs: null,
  };
}

function withFullSetAge(snapshot: FullSetArbSnapshot): FullSetArbSnapshot {
  return {
    ...snapshot,
    ageMs:
      snapshot.updatedAt > 0
        ? Math.max(0, Date.now() - snapshot.updatedAt)
        : null,
  };
}

function calcBuyCostForShares(
  levels: BookLevel[],
  targetShares: number,
): {
  ok: boolean;
  cost: number;
  avgPrice: number | null;
  levelsUsed: number;
  maxPrice: number | null;
  availableShares: number;
} {
  if (!(targetShares > 0)) {
    return {
      ok: false,
      cost: 0,
      avgPrice: null,
      levelsUsed: 0,
      maxPrice: null,
      availableShares: 0,
    };
  }
  let remaining = targetShares;
  let cost = 0;
  let levelsUsed = 0;
  let maxPrice: number | null = null;
  let availableShares = 0;
  for (const level of levels) {
    availableShares += level.size;
    if (remaining <= 1e-9) continue;
    const take = Math.min(remaining, level.size);
    cost += take * level.price;
    remaining -= take;
    levelsUsed++;
    maxPrice = level.price;
  }
  const filled = targetShares - Math.max(0, remaining);
  return {
    ok: remaining <= 1e-8,
    cost,
    avgPrice: filled > 0 ? cost / filled : null,
    levelsUsed,
    maxPrice,
    availableShares,
  };
}

function getCandidateShareSizes(
  upAsks: BookLevel[],
  downAsks: BookLevel[],
  maxBudget: number,
): number[] {
  const sizes = new Set<number>();
  let upCum = 0;
  for (const level of upAsks.slice(0, 12)) {
    upCum += level.size;
    if (upCum >= S10_FULLSET_MIN_SHARES) sizes.add(Number(upCum.toFixed(4)));
  }
  let downCum = 0;
  for (const level of downAsks.slice(0, 12)) {
    downCum += level.size;
    if (downCum >= S10_FULLSET_MIN_SHARES)
      sizes.add(Number(downCum.toFixed(4)));
  }
  const topCost = (upAsks[0]?.price ?? 0) + (downAsks[0]?.price ?? 0);
  if (topCost > 0) sizes.add(Number((maxBudget / topCost).toFixed(4)));
  sizes.add(S10_FULLSET_MIN_SHARES);
  return [...sizes]
    .filter((v) => Number.isFinite(v) && v >= S10_FULLSET_MIN_SHARES)
    .sort((a, b) => a - b);
}

function buildFullSetOpportunity(
  upBook: BookSnapshot,
  downBook: BookSnapshot,
  maxBudget: number,
): FullSetArbSnapshot {
  const now = Date.now();
  const base: FullSetArbSnapshot = {
    ...createEmptyFullSetArbSnapshot("scanning"),
    status: "scanning",
    windowStart: state.windowStart,
    updatedAt: now,
    maxBudget,
    upTopAsk: upBook.topAsk || null,
    downTopAsk: downBook.topAsk || null,
    bookLatencyMs: Math.max(upBook.latencyMs, downBook.latencyMs),
  };
  if (!upBook.asks.length || !downBook.asks.length) {
    return { ...base, status: "empty_book", reason: "missing up/down ask" };
  }
  if (!(maxBudget > 0)) {
    return { ...base, status: "no_budget", reason: "S10 amount is zero" };
  }

  let best: FullSetArbSnapshot | null = null;
  const maxCostPct =
    100 - S10_FULLSET_MIN_PROFIT_PCT - S10_FULLSET_FEE_BUFFER_PCT;
  for (const shares of getCandidateShareSizes(
    upBook.asks,
    downBook.asks,
    maxBudget,
  )) {
    const up = calcBuyCostForShares(upBook.asks, shares);
    const down = calcBuyCostForShares(downBook.asks, shares);
    if (!up.ok || !down.ok) continue;
    const totalCost = up.cost + down.cost;
    if (totalCost > maxBudget + 1e-9) continue;
    const totalCostPct = (totalCost / shares) * 100;
    const grossProfit = shares - totalCost;
    const grossProfitPct = 100 - totalCostPct;
    const netProfitPct = grossProfitPct - S10_FULLSET_FEE_BUFFER_PCT;
    if (totalCostPct > maxCostPct) continue;
    const firstLegDirection: StrategyDirection =
      up.avgPrice != null &&
      down.avgPrice != null &&
      up.avgPrice <= down.avgPrice
        ? "up"
        : "down";
    const candidate: FullSetArbSnapshot = {
      ...base,
      ready: netProfitPct >= S10_FULLSET_TRIGGER_PROFIT_PCT,
      status:
        netProfitPct >= S10_FULLSET_TRIGGER_PROFIT_PCT ? "ready" : "watching",
      reason: `net=${netProfitPct.toFixed(2)}% cost=${totalCostPct.toFixed(2)}%`,
      targetShares: shares,
      totalCost,
      totalCostPct,
      grossProfit,
      grossProfitPct,
      netProfitPct,
      upAvgAsk: up.avgPrice,
      downAvgAsk: down.avgPrice,
      upLevelsUsed: up.levelsUsed,
      downLevelsUsed: down.levelsUsed,
      firstLegDirection,
      firstLegCost: firstLegDirection === "up" ? up.cost : down.cost,
      secondLegDirection: firstLegDirection === "up" ? "down" : "up",
      secondLegCost: firstLegDirection === "up" ? down.cost : up.cost,
    };
    if (!best || (candidate.grossProfit ?? 0) > (best.grossProfit ?? 0))
      best = candidate;
  }

  if (best) return best;
  const topCostPct = ((upBook.topAsk || 0) + (downBook.topAsk || 0)) * 100;
  return {
    ...base,
    status: "no_edge",
    reason:
      topCostPct > 0
        ? `top full-set cost=${topCostPct.toFixed(2)}%`
        : "no usable depth",
    totalCostPct: topCostPct > 0 ? topCostPct : null,
    grossProfitPct: topCostPct > 0 ? 100 - topCostPct : null,
    netProfitPct:
      topCostPct > 0 ? 100 - topCostPct - S10_FULLSET_FEE_BUFFER_PCT : null,
  };
}

async function refreshFullSetArbSnapshot(): Promise<void> {
  if (!S10_FULLSET_SCANNER_ENABLED) {
    fullSetArbSnapshot = {
      ...createEmptyFullSetArbSnapshot("scanner disabled"),
      status: "disabled",
    };
    return;
  }
  if (fullSetRefreshRunning) return;
  if (!state.windowStart || !state.upTokenId || !state.downTokenId) {
    fullSetArbSnapshot = createEmptyFullSetArbSnapshot("waiting for window");
    return;
  }
  fullSetRefreshRunning = true;
  try {
    const [upBook, downBook] = await Promise.all([
      fetchBookSnapshot(state.upTokenId),
      fetchBookSnapshot(state.downTokenId),
    ]);
    const budget = Math.max(
      S10_FULLSET_MIN_SHARES * 0.02,
      Number(strategyConfig.amount.s10) || 0,
    );
    fullSetArbSnapshot = buildFullSetOpportunity(upBook, downBook, budget);
    if ((fullSetArbSnapshot.bookLatencyMs ?? 0) > S10_FULLSET_MAX_BOOK_AGE_MS) {
      fullSetArbSnapshot = {
        ...fullSetArbSnapshot,
        ready: false,
        status: "slow_book",
        reason: `book latency ${fullSetArbSnapshot.bookLatencyMs}ms`,
      };
    }
  } catch (err) {
    fullSetArbSnapshot = {
      ...createEmptyFullSetArbSnapshot(
        err instanceof Error ? err.message : String(err),
      ),
      status: "error",
      windowStart: state.windowStart,
      updatedAt: Date.now(),
      maxBudget: Number(strategyConfig.amount.s10) || 0,
    };
  } finally {
    fullSetRefreshRunning = false;
  }
}

// ── Gamma API ─────────────────────────────────────────────────
async function fetchMarket(windowStart: number): Promise<{
  conditionId: string;
  upTokenId: string;
  downTokenId: string;
  windowStart: number;
  windowEnd: number;
  eventStartTime: string;
  endDate: string;
} | null> {
  const slug = `btc-updown-5m-${windowStart}`;
  const startedAt = Date.now();
  try {
    const res = await fetch(`${GAMMA_URL}/events?slug=${slug}`);
    const endedAt = Date.now();
    updatePolymarketClockFromHeaders(res.headers, startedAt, endedAt, "gamma");
    const events = (await res.json()) as Record<string, unknown>[];
    if (!events?.length) {
      console.warn(
        `[Window] 市场未找到 slug=${slug} 耗时:${Date.now() - startedAt}ms`,
      );
      return null;
    }
    const event = events[0];
    const market = ((event.markets || []) as Record<string, unknown>[])[0];
    if (!market) {
      console.warn(
        `[Window] 市场缺少盘口 slug=${slug} 耗时:${Date.now() - startedAt}ms`,
      );
      return null;
    }
    const tokens = JSON.parse(
      (market.clobTokenIds as string) || "[]",
    ) as string[];
    const outcomes = JSON.parse(
      (market.outcomes as string) || "[]",
    ) as string[];
    const upIdx = outcomes.findIndex((o) => o.toLowerCase() === "up");
    const eventStartTime =
      (market.eventStartTime as string) ||
      new Date(windowStart * 1000).toISOString();
    const endDate =
      (market.endDate as string) ||
      new Date((windowStart + 300) * 1000).toISOString();
    const parsedEnd = Math.floor(Date.parse(endDate) / 1000);
    return {
      conditionId: market.conditionId as string,
      upTokenId: tokens[upIdx >= 0 ? upIdx : 0],
      downTokenId: tokens[upIdx >= 0 ? 1 - upIdx : 1],
      windowStart,
      windowEnd:
        Number.isFinite(parsedEnd) && parsedEnd > windowStart
          ? parsedEnd
          : windowStart + 300,
      eventStartTime,
      endDate,
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(
      `[Window] 市场查询失败 slug=${slug} 耗时:${Date.now() - startedAt}ms 原因:${msg}`,
    );
    return null;
  }
}

// ── 基准价 ────────────────────────────────────────────────────
interface CryptoPricePayload {
  openPrice?: number;
  closePrice?: number;
  timestamp?: number;
  completed?: boolean;
  incomplete?: boolean;
  cached?: boolean;
}

async function fetchCryptoPricePayload(
  eventStartTime: string,
  endDate: string,
): Promise<CryptoPricePayload | null> {
  const url = `https://polymarket.com/api/crypto/crypto-price?symbol=BTC&eventStartTime=${encodeURIComponent(eventStartTime)}&variant=fiveminute&endDate=${encodeURIComponent(endDate)}`;
  return (await fetch(url).then((r) => r.json())) as CryptoPricePayload;
}

async function fetchCryptoPrice(
  eventStartTime: string,
  endDate: string,
): Promise<void> {
  try {
    const data = await fetchCryptoPricePayload(eventStartTime, endDate);
    if (data.openPrice != null) state.priceToBeat = data.openPrice;
  } catch {
    /* 静默 */
  }
}

// ── 持仓 API 查询 ──────────────────────────────────────────────
async function syncPositionsFromApi(): Promise<boolean> {
  if (!PROXY_ADDRESS) {
    strategyRuntime.positionsReady = true;
    return true;
  }
  try {
    const pos = (await fetch(
      `https://data-api.polymarket.com/positions?user=${PROXY_ADDRESS}&sizeThreshold=0.01`,
    ).then((r) => r.json())) as Array<{ asset: string; size: number }>;
    const apiMap: Record<string, number> = {};
    for (const p of pos) {
      apiMap[p.asset] = p.size;
      positions.apiSize[p.asset] = p.size;
    }
    for (const tokenId of [state.upTokenId, state.downTokenId]) {
      if (!tokenId) continue;
      if (!(tokenId in apiMap)) positions.apiSize[tokenId] = 0;
      const apiVal = apiMap[tokenId] ?? 0;
      const localVal = positions.localSize[tokenId] ?? 0;
      const msSinceTrade = Date.now() - (positions.lastTradeAt ?? 0);
      if (msSinceTrade < POST_TRADE_CALIBRATION_MS) continue;
      if (Math.abs(apiVal - localVal) <= 0.5) {
        positions.localSize[tokenId] = apiVal;
        positions.apiVerified[tokenId] = true;
      }
    }
    positions.lastApiSyncAt = Date.now();
    strategyRuntime.positionsReady = true;
    return true;
  } catch {
    return false;
  }
}

// ── USDC 余额查询 ──────────────────────────────────────────────
async function syncUsdcBalance(): Promise<void> {
  if (!PROXY_ADDRESS) return;
  try {
    if (!(await ensureClobClient())) return;
    const resp = (await clobClient!.getBalanceAllowance({
      asset_type: AssetType.COLLATERAL,
    })) as {
      balance?: string;
      allowance?: string;
      allowances?: Record<string, string>;
    };

    positions.usdc =
      resp.balance != null
        ? parseFloat(ethers.formatUnits(resp.balance, 6))
        : null;

    const allowanceMap =
      resp.allowances && typeof resp.allowances === "object"
        ? Object.entries(resp.allowances)
        : resp.allowance != null
          ? [["default", resp.allowance]]
          : [];

    const details = allowanceMap.map(([spender, raw]) => {
      const amount =
        raw != null ? parseFloat(ethers.formatUnits(raw, 6)) : null;
      return { spender, amount: Number.isFinite(amount) ? amount : null };
    });

    positions.usdcAllowanceDetails = details;

    if (!details.length) {
      positions.usdcAllowanceStatus = "未授权";
      positions.usdcAllowanceMin = null;
      return;
    }

    const positiveCount = details.filter(
      (item) => (item.amount ?? 0) > 0,
    ).length;
    const minAllowance = details.reduce<number | null>((min, item) => {
      if (item.amount == null) return min;
      return min == null ? item.amount : Math.min(min, item.amount);
    }, null);

    positions.usdcAllowanceMin = minAllowance;
    positions.usdcAllowanceStatus =
      positiveCount === 0
        ? "未授权"
        : positiveCount === details.length
          ? "已授权"
          : "未完全授权";
  } catch (e) {
    console.error(
      "[USDC] 余额/授权查询失败:",
      e instanceof Error ? ((e as any).shortMessage ?? e.message) : String(e),
    );
  }
}

// ── 退避重连工具 ──────────────────────────────────────────────
function backoffDelay(attempt: number): number {
  const delays = [0, 1000, 2000, 4000, 8000, 30000];
  return delays[Math.min(attempt, delays.length - 1)];
}

// ── User WS（监听成交） ────────────────────────────────────────
let userWs: WebSocket | null = null;
let userWsPingTimer: ReturnType<typeof setInterval> | null = null;
let userWsAttempt = 0;

function startUserWs(): void {
  if (!existsSync(CREDS_FILE)) {
    console.log("[UserWS] 未找到凭证文件，跳过");
    return;
  }
  const creds = JSON.parse(readFileSync(CREDS_FILE, "utf-8")) as {
    key: string;
    secret: string;
    passphrase: string;
  };

  userWs = new WebSocket(USER_WS_URL);

  userWs.on("open", () => {
    console.log(userWsAttempt === 0 ? "[UserWS] 已连接" : "[UserWS] 重连成功");
    userWsAttempt = 0;
    wsStatus.user = true;
    broadcastWsStatus();
    userWs!.send(
      JSON.stringify({
        auth: {
          apiKey: creds.key,
          secret: creds.secret,
          passphrase: creds.passphrase,
        },
        type: "user",
      }),
    );
    userWsPingTimer = setInterval(() => {
      if (userWs?.readyState === WebSocket.OPEN) userWs.send("PING");
    }, 10000);
  });

  userWs.on("message", (data) => {
    const msg = data.toString();
    if (msg === "PONG") return;
    try {
      const arr = JSON.parse(msg);
      const events = Array.isArray(arr) ? arr : [arr];
      for (const evt of events) {
        if (!isRecord(evt)) continue;
        if (
          (evt.type === "TRADE" || evt.event_type === "trade") &&
          evt.status === "MINED"
        ) {
          const tradeId = evt.id as string;
          if (
            !rememberBounded(
              positions.confirmedIds,
              positions.confirmedIdOrder,
              tradeId,
              MAX_CONFIRMED_TRADE_IDS,
            )
          )
            continue;
          const eventAssetId =
            typeof evt.asset_id === "string" ? evt.asset_id : "";
          const eventSize =
            typeof evt.size === "number"
              ? evt.size
              : parseFloat(String(evt.size ?? ""));
          const eventSide = normalizeTradeSide(evt.side);
          const eventPrice =
            typeof evt.price === "number"
              ? evt.price
              : parseFloat(String(evt.price ?? ""));
          if (
            !eventAssetId ||
            !eventSide ||
            !Number.isFinite(eventSize) ||
            eventSize <= 0
          )
            continue;
          const pendingMeta = consumePendingTradeMeta(evt);
          const assetId = pendingMeta?.fillAssetId || eventAssetId;
          const size =
            pendingMeta?.fillSize != null && pendingMeta.fillSize > 0
              ? pendingMeta.fillSize
              : eventSize;
          const price =
            pendingMeta?.fillPrice != null && pendingMeta.fillPrice > 0
              ? pendingMeta.fillPrice
              : eventPrice;
          if (!assetId || !Number.isFinite(size) || size <= 0) continue;
          const side = pendingMeta?.side ?? eventSide;
          const direction =
            pendingMeta?.direction ?? getDirectionByAssetId(assetId);
          const orderId =
            pendingMeta?.orderId ??
            (typeof evt.taker_order_id === "string" && evt.taker_order_id
              ? evt.taker_order_id
              : undefined);
          const liveMakerOrder = orderId
            ? liveMakerOrders.find((candidate) => candidate.orderId === orderId)
            : undefined;
          const requestedShares =
            liveMakerOrder?.shares ?? pendingMeta?.amount ?? null;
          const makerLimitPrice =
            liveMakerOrder?.price ?? pendingMeta?.worstPrice ?? null;
          const requestedAmount =
            requestedShares != null && makerLimitPrice != null
              ? requestedShares * makerLimitPrice
              : null;
          const txHash =
            typeof evt.transaction_hash === "string" && evt.transaction_hash
              ? evt.transaction_hash
              : undefined;
          if (!(assetId in positions.localSize))
            positions.localSize[assetId] = 0;
          positions.localSize[assetId] =
            side === "buy"
              ? positions.localSize[assetId] + size
              : Math.max(0, positions.localSize[assetId] - size);
          positions.apiVerified[assetId] = false;
          positions.lastTradeAt = parseTradeEventTimestamp(evt);
          if (direction && Number.isFinite(price) && price > 0) {
            recordTradeHistory({
              ts: positions.lastTradeAt,
              windowStart: pendingMeta?.windowStart ?? state.windowStart,
              side,
              direction,
              amount: size,
              price,
              avgPrice: price,
              worstPrice: pendingMeta?.worstPrice ?? null,
              status: "MINED",
              source: pendingMeta?.source ?? "manual",
              txHash,
              orderId,
              exitReason: pendingMeta?.exitReason,
              roundEntry: pendingMeta?.roundEntry,
              executionMode: "live",
              requestedAmount,
              requestedShares,
              filledShares: size,
              filledNotional: size * price,
              makerOrderId: liveMakerOrder?.id ?? null,
              makerLimitPrice,
              makerTrigger: liveMakerOrder ? "user_ws_mined" : null,
              makerActiveMs: liveMakerOrder
                ? Date.now() - liveMakerOrder.postedAt
                : null,
              totalLatencyMs: liveMakerOrder
                ? Date.now() - liveMakerOrder.createdAt
                : null,
              bookTokenId: assetId,
              bookWindowStart: pendingMeta?.windowStart ?? state.windowStart,
            });
          }
          if (orderId && direction) {
            noteLiveMakerFill(
              orderId,
              side,
              direction,
              size,
              Number.isFinite(price) ? price : 0,
              positions.lastTradeAt,
            );
            if (pendingMeta && size >= pendingMeta.amount - 0.01)
              forgetPendingTradeMeta(orderId);
          }
          console.log(
            `[UserWS] MINED ${side.toUpperCase()} ${size} @ ${Number.isFinite(price) ? price : "-"}` +
              ` asset: ...${assetId.slice(-6)}` +
              `${orderId ? ` order:${orderId}` : ""}` +
              `${txHash ? ` tx:${txHash.slice(0, 10)}...` : ""}`,
          );
          broadcastState();

          // 买入后异步链上校准：WS 推送的 size 有 ~1% 偏差，用链上真实值修正
          if (side === "buy" && txHash && PROXY_ADDRESS) {
            const wsSize = size;
            const targetAssetId = assetId;
            void (async () => {
              const realFill = await getRealFillFromTx(txHash, PROXY_ADDRESS);
              if (realFill == null) {
                console.log(
                  `[ChainWatcher] ⚠ 校准失败 tx:${txHash.slice(0, 10)}... 将由 REST 兜底`,
                );
                return;
              }
              const delta = realFill - wsSize;
              if (Math.abs(delta) < 0.000001) {
                console.log(
                  `[ChainWatcher] ✓ 买入校准 ${targetAssetId.slice(-6)} WS:${wsSize} = 链上:${realFill}`,
                );
              } else {
                positions.localSize[targetAssetId] =
                  (positions.localSize[targetAssetId] ?? 0) + delta;
                console.log(
                  `[ChainWatcher] ✓ 买入校准 ${targetAssetId.slice(-6)} WS:${wsSize} → 链上:${realFill} (delta:${delta >= 0 ? "+" : ""}${delta.toFixed(6)})`,
                );
              }
              positions.apiSize[targetAssetId] =
                positions.localSize[targetAssetId];
              positions.apiVerified[targetAssetId] = true;
              broadcastState();
            })();
          }
        }
      }
    } catch {
      /* 忽略 */
    }
  });

  userWs.on("close", () => {
    if (userWsPingTimer) clearInterval(userWsPingTimer);
    const delay = backoffDelay(userWsAttempt++);
    console.log(`[UserWS] 断开，${delay}ms 后重连 (第${userWsAttempt}次)`);
    wsStatus.user = false;
    broadcastWsStatus();
    if (!stopped) setTimeout(startUserWs, delay);
  });
  userWs.on("error", (err) => {
    console.error("[UserWS] 错误:", err.message);
  });
}

// ── Market WS ─────────────────────────────────────────────────
let marketWs: WebSocket | null = null;
let marketPingTimer: ReturnType<typeof setInterval> | null = null;
let marketRenderTimer: ReturnType<typeof setInterval> | null = null;
let marketValidationTimer: ReturnType<typeof setInterval> | null = null;
let lastBestBidAskTimestamp = 0;
let bestBidAskPausedUntil = 0;
let marketValidationMismatchStreak = 0;
let marketReconnectPending = false;
let marketBestReady = false;

function isProbabilityReady(now = Date.now()): boolean {
  if (!wsStatus.market) return false;
  if (!marketBestReady) return false;
  if (now < bestBidAskPausedUntil) return false;
  if (!state.bookUpdatedAt || now - state.bookUpdatedAt > MAX_BOOK_STALE_MS)
    return false;
  const bid = Number(state.bestBid);
  const ask = Number(state.bestAsk);
  return Number.isFinite(bid) && Number.isFinite(ask);
}

function parseEventTimestamp(value: unknown): number {
  const n = typeof value === "number" ? value : Number(value);
  if (Number.isFinite(n) && n > 0) {
    return n < 1_000_000_000_000 ? n * 1000 : n;
  }
  if (typeof value === "string") {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

function applyBestBidAskUpdate(
  bestBid: unknown,
  bestAsk: unknown,
  timestamp: unknown,
): boolean {
  if (typeof bestBid !== "string" || typeof bestAsk !== "string") return false;
  if (Date.now() < bestBidAskPausedUntil) return false;
  const ts = parseEventTimestamp(timestamp);
  if (ts > 0 && ts < lastBestBidAskTimestamp) return false;
  if (ts > 0) lastBestBidAskTimestamp = ts;
  state.bestBid = bestBid;
  state.bestAsk = bestAsk;
  state.bookUpdatedAt = Date.now();
  state.bookEventTs = ts;
  state.bookSource = "ws";
  recordWsLatencyUpdate(state.bookUpdatedAt);
  marketBestReady = true;
  scheduleStrategyTick(); // 盘口更新（概率变化）立即触发策略检查
  return true;
}

function applyBookSnapshot(
  book: BookSnapshot,
  source: "ws" | "rest",
  eventTs = 0,
): void {
  state.bids.clear();
  state.asks.clear();
  for (const b of book.bids) state.bids.set(String(b.price), String(b.size));
  for (const a of book.asks) state.asks.set(String(a.price), String(a.size));
  state.bestBid = String(book.topBid > 0 ? book.topBid : 0);
  state.bestAsk = String(book.topAsk > 0 ? book.topAsk : 1);
  state.bookUpdatedAt = Date.now();
  state.bookEventTs = eventTs;
  state.bookSource = source;
  if (source === "ws") recordWsLatencyUpdate(state.bookUpdatedAt);
  state.updatedAt = Date.now();
  marketBestReady = book.topBid > 0 || book.topAsk > 0;
  scheduleStrategyTick();
}

function clearProbabilityForMs(ms: number, reason: string): void {
  const until = Date.now() + ms;
  if (until > bestBidAskPausedUntil) bestBidAskPausedUntil = until;
  marketBestReady = false;
  lastBestBidAskTimestamp = 0;
  lastWsBookUpdateAt = 0;
  state.bestBid = "-";
  state.bestAsk = "-";
  state.bookUpdatedAt = 0;
  state.bookEventTs = 0;
  state.bookSource = "";
  state.bookCheckAt = 0;
  state.bookCheckLatencyMs = null;
  state.bookCheckDiffPct = null;
  state.updatedAt = Date.now();
  console.warn(`[概率校验] ${reason}，清空概率 ${ms}ms`);
  broadcastState();
}

function requestMarketReconnect(
  reason: string,
  options?: { clearProbabilityMs?: number },
): void {
  clearProbabilityForMs(options?.clearProbabilityMs ?? 0, reason);
  marketValidationMismatchStreak = 0;
  if (marketReconnectPending) return;
  marketReconnectPending = true;
  console.warn(`[MarketWS] 触发重连: ${reason}`);
  if (marketWs) {
    marketWs.close();
    return;
  }
  if (reconnectTimer) clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(() => {
    void subscribeWindow(Math.max(subscribedWindow, getCurrentWindowStart()));
  }, 1000);
}

async function validateMarketProbability(
  expectedWindowStart: number,
  upTokenId: string,
): Promise<void> {
  if (marketReconnectPending) return;
  if (subscribedWindow !== expectedWindowStart) return;
  if (!marketWs || marketWs.readyState !== WebSocket.OPEN) return;

  try {
    const book = await fetchBookSnapshot(upTokenId);
    const bestBid = book.topBid;
    const bestAsk = book.topAsk;
    if (
      subscribedWindow !== expectedWindowStart ||
      upTokenId !== state.upTokenId
    )
      return;
    if (!(bestBid > 0) || !(bestAsk > 0)) return;

    const wsBid = Number(state.bestBid);
    const wsAsk = Number(state.bestAsk);
    if (
      !Number.isFinite(wsBid) ||
      !Number.isFinite(wsAsk) ||
      wsBid <= 0 ||
      wsAsk <= 0
    ) {
      applyBookSnapshot(book, "rest");
      broadcastState();
      marketValidationMismatchStreak = 0;
      return;
    }

    const restMid = (bestBid + bestAsk) / 2;
    const wsMid = (wsBid + wsAsk) / 2;
    const diffPct = Math.abs(restMid - wsMid) * 100;
    const now = Date.now();
    const bookAgeMs =
      state.bookUpdatedAt > 0 ? now - state.bookUpdatedAt : Infinity;

    state.bookCheckAt = now;
    state.bookCheckLatencyMs = book.latencyMs;
    state.bookCheckDiffPct = diffPct;
    recordLatencySample(restCheckLatencySamples, book.latencyMs);

    if (bookAgeMs > 1200) {
      applyBookSnapshot(book, "rest");
      broadcastState();
      marketValidationMismatchStreak = 0;
      return;
    }

    if (diffPct > 3) {
      marketValidationMismatchStreak++;
      console.warn(
        `[MarketValidation] REST/WS diff ${diffPct.toFixed(2)}%, fallback to REST ${marketValidationMismatchStreak}/3`,
      );
      if (marketValidationMismatchStreak >= 3 || bookAgeMs > 300) {
        applyBookSnapshot(book, "rest");
        broadcastState();
        marketValidationMismatchStreak = 0;
      }
      return;
    }

    if (diffPct > 3) {
      marketValidationMismatchStreak++;
      console.warn(
        `[概率校验] REST偏差 ${diffPct.toFixed(2)}%，连续 ${marketValidationMismatchStreak}/3`,
      );
      if (marketValidationMismatchStreak >= 3) {
        requestMarketReconnect(`概率连续3次偏差>${3}%`);
      }
      return;
    }

    marketValidationMismatchStreak = 0;
  } catch (err) {
    if (
      subscribedWindow !== expectedWindowStart ||
      upTokenId !== state.upTokenId
    )
      return;
    requestMarketReconnect(
      `REST校验失败: ${err instanceof Error ? err.message : String(err)}`,
      { clearProbabilityMs: 2000 },
    );
  }
}

let _marketWsConnectedOnce = false;
function startMarketWs(
  expectedWindowStart: number,
  upTokenId: string,
  downTokenId: string,
  onClose: () => void,
): WebSocket {
  const ws = new WebSocket(MARKET_WS_URL);
  ws.on("open", () => {
    if (ws !== marketWs || subscribedWindow !== expectedWindowStart) return;
    console.log(
      _marketWsConnectedOnce ? "[MarketWS] 重连成功" : "[MarketWS] 已连接",
    );
    _marketWsConnectedOnce = true;
    marketReconnectPending = false;
    marketValidationMismatchStreak = 0;
    marketBestReady = false;
    wsStatus.market = true;
    broadcastWsStatus();
    ws.send(
      JSON.stringify({
        assets_ids: [upTokenId, downTokenId],
        type: "market",
        custom_feature_enabled: true,
      }),
    );
    marketRenderTimer = setInterval(broadcastState, 1000);
    marketValidationTimer = setInterval(() => {
      void validateMarketProbability(expectedWindowStart, upTokenId);
    }, 1000);
    marketPingTimer = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN) ws.send("PING");
    }, 10000);
  });
  ws.on("message", (data) => {
    if (
      ws !== marketWs ||
      subscribedWindow !== expectedWindowStart ||
      state.upTokenId !== upTokenId
    )
      return;
    const msg = data.toString();
    if (msg === "PONG" || msg === "[]") return;
    try {
      const events = Array.isArray(JSON.parse(msg))
        ? JSON.parse(msg)
        : [JSON.parse(msg)];
      for (const evt of events) {
        if (evt.bids !== undefined && evt.asks !== undefined) {
          if (evt.asset_id && evt.asset_id !== upTokenId) continue;
          const eventTs = parseEventTimestamp(evt.timestamp);
          const bids = ((evt.bids || []) as { price: string; size: string }[])
            .map((b) => ({ price: Number(b.price), size: Number(b.size) }))
            .filter((b) => b.price > 0 && b.size > 0)
            .sort((a, b) => b.price - a.price);
          const asks = ((evt.asks || []) as { price: string; size: string }[])
            .map((a) => ({ price: Number(a.price), size: Number(a.size) }))
            .filter((a) => a.price > 0 && a.size > 0)
            .sort((a, b) => a.price - b.price);
          applyBookSnapshot(
            {
              tokenId: upTokenId,
              bids,
              asks,
              topBid: bids[0]?.price ?? 0,
              topAsk: asks[0]?.price ?? 0,
              fetchedAt: Date.now(),
              latencyMs: 0,
            },
            "ws",
            eventTs,
          );
          broadcastState();
        } else if (evt.event_type === "best_bid_ask") {
          if (evt.asset_id && evt.asset_id !== upTokenId) continue;
          if (!applyBestBidAskUpdate(evt.best_bid, evt.best_ask, evt.timestamp))
            continue;
          state.updatedAt = Date.now();
          broadcastState();
        } else if (evt.event_type === "price_change" && evt.price_changes) {
          for (const change of evt.price_changes as Record<string, string>[]) {
            if (change.asset_id !== upTokenId) continue;
            if (change.price && change.size !== undefined) {
              state.lastPrice = Number(change.price).toFixed(2);
              state.lastSide = change.side;
              state.lastPriceUpdatedAt = Date.now();
              // 同步更新盘口深度
              const size = Number(change.size);
              const map = change.side === "BUY" ? state.bids : state.asks;
              if (size > 0) map.set(change.price, change.size);
              else map.delete(change.price);
            }
          }
          state.bookUpdatedAt = Date.now();
          state.bookEventTs = parseEventTimestamp(evt.timestamp);
          state.bookSource = "ws";
          recordWsLatencyUpdate(state.bookUpdatedAt);
          state.updatedAt = Date.now();
          broadcastState();
        }
      }
    } catch {
      /* 忽略 */
    }
  });
  ws.on("close", () => {
    if (ws !== marketWs || subscribedWindow !== expectedWindowStart) return;
    if (marketPingTimer) clearInterval(marketPingTimer);
    if (marketRenderTimer) clearInterval(marketRenderTimer);
    if (marketValidationTimer) {
      clearInterval(marketValidationTimer);
      marketValidationTimer = null;
    }
    marketBestReady = false;
    lastWsBookUpdateAt = 0;
    state.bestBid = "-";
    state.bestAsk = "-";
    state.bookUpdatedAt = 0;
    state.bookEventTs = 0;
    state.bookSource = "";
    state.bookCheckAt = 0;
    state.bookCheckLatencyMs = null;
    state.bookCheckDiffPct = null;
    state.updatedAt = Date.now();
    console.log("[MarketWS] 连接断开，1秒后重连");
    wsStatus.market = false;
    broadcastWsStatus();
    broadcastState();
    broadcast("marketDown", {});
    onClose();
  });
  ws.on("error", (err) => {
    console.error("[MarketWS] 错误:", err.message);
  });
  return ws;
}

// ── Chainlink WS ──────────────────────────────────────────────
let chainlinkWs: WebSocket | null = null;

function startChainlinkWs(
  expectedWindowStart: number,
  eventSlug: string,
  onClose: () => void,
  attempt = 0,
): WebSocket {
  const ws = new WebSocket(CHAINLINK_WS_URL);
  ws.on("open", () => {
    if (ws !== chainlinkWs || subscribedWindow !== expectedWindowStart) return;
    console.log(
      attempt === 0 ? "[ChainlinkWS] 已连接" : "[ChainlinkWS] 重连成功",
    );
    wsStatus.chainlink = true;
    broadcastWsStatus();
    ws.send(
      JSON.stringify({
        action: "subscribe",
        subscriptions: [
          {
            topic: "crypto_prices_chainlink",
            type: "update",
            filters: JSON.stringify({ symbol: "btc/usd" }),
          },
          {
            topic: "activity",
            type: "orders_matched",
            filters: JSON.stringify({ event_slug: eventSlug }),
          },
        ],
      }),
    );
  });
  ws.on("message", (data) => {
    if (ws !== chainlinkWs || subscribedWindow !== expectedWindowStart) return;
    try {
      const msg = JSON.parse(data.toString()) as {
        topic?: string;
        type?: string;
        timestamp?: number;
        payload?: { value?: number; timestamp?: number };
      };
      if (msg.topic === "crypto_prices_chainlink" && msg.type === "update") {
        const val = msg.payload?.value;
        if (val != null) {
          state.currentPrice = val;
          const now = msg.payload?.timestamp ?? msg.timestamp ?? Date.now();
          state.currentPriceUpdatedAt = Date.now();
          state.priceHistory.push({ t: now, price: val });
          trimHistory(
            state.priceHistory,
            now - HISTORY_RETENTION_MS,
            MAX_CHAINLINK_HISTORY_POINTS,
          );
          maybeInitializeBinanceOffset();
          broadcast("chainlinkPrice", { t: now, price: val });
          broadcastState();
          scheduleStrategyTick(); // 价格更新立即触发策略检查
        }
      }
    } catch {
      /* 忽略 */
    }
  });
  ws.on("close", () => {
    if (ws !== chainlinkWs || subscribedWindow !== expectedWindowStart) return;
    const delay = backoffDelay(attempt);
    console.log(
      `[ChainlinkWS] 连接断开，${delay}ms 后重连 (第${attempt + 1}次)`,
    );
    wsStatus.chainlink = false;
    broadcastWsStatus();
    broadcast("chainlinkDown", {});
    onClose();
  });
  ws.on("error", (err) => {
    console.error("[ChainlinkWS] 错误:", err.message);
  });
  return ws;
}

// ── 币安 WS ───────────────────────────────────────────────────
let binanceWs: WebSocket | null = null;
let binanceWsAttempt = 0;

function updateKlineArray(arr: Kline[], k: Kline, maxSize: number): void {
  const last = arr[arr.length - 1];
  if (last && last.openTime === k.openTime) {
    // 同一根 K 线更新中
    arr[arr.length - 1] = k;
  } else {
    arr.push(k);
    if (arr.length > maxSize) arr.splice(0, arr.length - maxSize);
  }
}

/** 从 Binance REST 拉取历史 K 线（用于启动预填充和重连后补缺口） */
async function fetchHistoricalKlines(
  interval: "1m" | "5m",
  limit: number,
): Promise<Kline[] | null> {
  try {
    const url = `https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=${interval}&limit=${limit}`;
    const res = await fetch(url);
    if (!res.ok) return null;
    const raw = (await res.json()) as Array<Array<string | number>>;
    return raw.map((r) => ({
      openTime: Number(r[0]),
      open: parseFloat(String(r[1])),
      high: parseFloat(String(r[2])),
      low: parseFloat(String(r[3])),
      close: parseFloat(String(r[4])),
      volume: parseFloat(String(r[5])),
      closed: true, // REST 返回的都是已收盘的
    }));
  } catch (err) {
    console.warn(
      `[Binance] 拉取历史 ${interval} K 线失败: ${(err as Error).message}`,
    );
    return null;
  }
}

/** 合并历史 K 线到现有数组，去重并保留最新 maxSize 根 */
function mergeKlines(
  existing: Kline[],
  fetched: Kline[],
  maxSize: number,
): void {
  const map = new Map<number, Kline>();
  for (const k of existing) map.set(k.openTime, k);
  for (const k of fetched) {
    // 历史数据只在当前没有或为未收盘时覆盖
    const curr = map.get(k.openTime);
    if (!curr || !curr.closed) map.set(k.openTime, k);
  }
  const sorted = [...map.values()].sort((a, b) => a.openTime - b.openTime);
  existing.length = 0;
  const start = Math.max(0, sorted.length - maxSize);
  for (let i = start; i < sorted.length; i++) existing.push(sorted[i]);
}

async function loadHistoricalKlines(): Promise<void> {
  const [k1m, k5m] = await Promise.all([
    fetchHistoricalKlines("1m", MAX_KLINE_1M),
    fetchHistoricalKlines("5m", MAX_KLINE_5M),
  ]);
  if (k1m) {
    mergeKlines(state.kline1m, k1m, MAX_KLINE_1M);
    console.log(`[Binance] 1m K 线预填充 ${state.kline1m.length} 根`);
  }
  if (k5m) {
    mergeKlines(state.kline5m, k5m, MAX_KLINE_5M);
    console.log(`[Binance] 5m K 线预填充 ${state.kline5m.length} 根`);
  }
}

function startBinanceWs(): void {
  binanceWs = new WebSocket(BINANCE_WS_URL);
  binanceWs.on("open", () => {
    console.log(
      binanceWsAttempt === 0 ? "[BinanceWS] 已连接" : "[BinanceWS] 重连成功",
    );
    binanceWsAttempt = 0;
    wsStatus.binance = true;
    broadcastWsStatus();
    // 连接成功后异步拉取历史 K 线，填充 / 补缺口
    void loadHistoricalKlines();
  });
  binanceWs.on("message", (data) => {
    try {
      const raw = JSON.parse(data.toString()) as {
        stream?: string;
        data?: Record<string, unknown>;
      };
      const stream = raw.stream;
      const payload = raw.data;
      if (!stream || !payload) return;

      if (stream.endsWith("@aggTrade")) {
        const p = payload as { p?: string; T?: number };
        const price = parseFloat(p.p ?? "");
        const t = p.T ?? Date.now();
        if (!price) return;
        state.binanceHistory.push({ t, price });
        trimHistory(
          state.binanceHistory,
          t - HISTORY_RETENTION_MS,
          MAX_BINANCE_HISTORY_POINTS,
        );
        maybeInitializeBinanceOffset();
        broadcast("binancePrice", { t, price });
        scheduleStrategyTick(); // 价格变化立即触发策略检查
        return;
      }

      if (stream.endsWith("@kline_1m") || stream.endsWith("@kline_5m")) {
        const kData = (payload as { k?: Record<string, unknown> }).k;
        if (!kData) return;
        const kline: Kline = {
          openTime: Number(kData.t),
          open: parseFloat(String(kData.o)),
          high: parseFloat(String(kData.h)),
          low: parseFloat(String(kData.l)),
          close: parseFloat(String(kData.c)),
          volume: parseFloat(String(kData.v)),
          closed: Boolean(kData.x),
        };
        if (stream.endsWith("@kline_1m")) {
          updateKlineArray(state.kline1m, kline, MAX_KLINE_1M);
        } else {
          updateKlineArray(state.kline5m, kline, MAX_KLINE_5M);
        }
        scheduleStrategyTick(); // K线更新立即触发策略检查
        return;
      }
    } catch {
      /* 忽略 */
    }
  });
  binanceWs.on("close", () => {
    const delay = backoffDelay(binanceWsAttempt++);
    console.log(
      `[BinanceWS] 断开，${delay}ms 后重连 (第${binanceWsAttempt}次)`,
    );
    wsStatus.binance = false;
    broadcastWsStatus();
    if (!stopped) setTimeout(startBinanceWs, delay);
  });
  binanceWs.on("error", (err) => {
    console.error("[BinanceWS] 错误:", err.message);
  });
}

// ── 最近4轮结果查询 ───────────────────────────────────────────
function parseOutcomeMarks(
  event: Record<string, unknown> | undefined,
): { up: number; down: number } | null {
  const market = ((event?.markets as Record<string, unknown>[] | undefined) ||
    [])[0];
  if (!market) return null;

  let outcomes: string[] = [];
  let outcomePrices: string[] = [];

  try {
    outcomes = JSON.parse(String(market.outcomes || "[]")) as string[];
  } catch {
    /* 忽略 */
  }
  try {
    outcomePrices = JSON.parse(
      String(market.outcomePrices || "[]"),
    ) as string[];
  } catch {
    /* 忽略 */
  }

  if (!outcomes.length || outcomes.length !== outcomePrices.length) return null;

  const upIdx = outcomes.findIndex((o) => o.toLowerCase() === "up");
  const downIdx = outcomes.findIndex((o) => o.toLowerCase() === "down");
  if (upIdx < 0 || downIdx < 0) return null;

  const upPrice = Number(outcomePrices[upIdx]);
  const downPrice = Number(outcomePrices[downIdx]);
  if (!Number.isFinite(upPrice) || !Number.isFinite(downPrice)) return null;
  return { up: clampNumber(upPrice, 0, 1), down: clampNumber(downPrice, 0, 1) };
}

function parseResolvedOutcome(
  event: Record<string, unknown> | undefined,
): "up" | "down" | null {
  const marks = parseOutcomeMarks(event);
  if (!marks) return null;

  if (marks.up >= 0.999 && marks.down <= 0.001) return "up";
  if (marks.down >= 0.999 && marks.up <= 0.001) return "down";
  return null;
}

function hasOpenPaperPositionForWindow(windowStart: number): boolean {
  const info = paperAccount.windows[String(windowStart)];
  if (!info) return false;
  return (
    (paperAccount.localSize[info.upTokenId] ?? 0) > 1e-8 ||
    (paperAccount.localSize[info.downTokenId] ?? 0) > 1e-8
  );
}

function capturePaperWindowCloseEstimate(windowStart: number): boolean {
  const info = paperAccount.windows[String(windowStart)];
  if (!info || info.settled) return false;
  const priceToBeat = state.priceToBeat;
  const closePrice = state.currentPrice;
  const priceAgeMs =
    state.currentPriceUpdatedAt > 0
      ? Date.now() - state.currentPriceUpdatedAt
      : Infinity;
  if (
    !Number.isFinite(priceToBeat) ||
    !Number.isFinite(closePrice) ||
    priceAgeMs > PAPER_FAST_SETTLE_MAX_PRICE_AGE_MS
  ) {
    return false;
  }

  const diff = closePrice - priceToBeat;
  const localResult: StrategyDirection = diff > 0 ? "up" : "down";
  info.localResult = localResult;
  info.priceToBeat = priceToBeat;
  info.closePrice = closePrice;
  info.closeDiff = diff;
  info.closeCapturedAt = Date.now();
  info.upMark = localResult === "up" ? 1 : 0;
  info.downMark = localResult === "down" ? 1 : 0;
  info.markUpdatedAt = Date.now();

  if (
    Math.abs(diff) >= PAPER_FAST_SETTLE_MIN_DIFF &&
    hasOpenPaperPositionForWindow(windowStart)
  ) {
    console.log(
      `[Paper] 本地收盘预估 window=${windowStart} result=${localResult} diff=${diff.toFixed(2)} priceAge=${Math.round(priceAgeMs)}ms`,
    );
  }
  persistPaperAccountState();
  broadcastState();
  return true;
}

function applyPaperWindowClose(
  windowStart: number,
  priceToBeat: number,
  closePrice: number,
  settlementSource: string,
): boolean {
  const info = paperAccount.windows[String(windowStart)];
  if (!info || info.settled) return false;
  if (!Number.isFinite(priceToBeat) || !Number.isFinite(closePrice))
    return false;
  const diff = closePrice - priceToBeat;
  const localResult: StrategyDirection = diff > 0 ? "up" : "down";
  info.localResult = localResult;
  info.priceToBeat = priceToBeat;
  info.closePrice = closePrice;
  info.closeDiff = diff;
  info.closeCapturedAt = Date.now();
  info.upMark = localResult === "up" ? 1 : 0;
  info.downMark = localResult === "down" ? 1 : 0;
  info.markUpdatedAt = Date.now();

  if (
    Math.abs(diff) >= PAPER_FAST_SETTLE_MIN_DIFF &&
    hasOpenPaperPositionForWindow(windowStart)
  ) {
    console.log(
      `[Paper] ${settlementSource} 结算 window=${windowStart} result=${localResult} diff=${diff.toFixed(2)}`,
    );
    return settlePaperWindow(windowStart, localResult, settlementSource);
  }

  persistPaperAccountState();
  broadcastState();
  return true;
}

async function capturePaperWindowCloseFromCryptoApi(
  windowStart: number,
): Promise<boolean> {
  const info = paperAccount.windows[String(windowStart)];
  if (!info || info.settled) return false;
  const eventStartTime =
    info.eventStartTime || new Date(windowStart * 1000).toISOString();
  const endDate =
    info.endDate || new Date((windowStart + 300) * 1000).toISOString();
  try {
    const data = await fetchCryptoPricePayload(eventStartTime, endDate);
    const openPrice = Number(data?.openPrice);
    const closePrice = Number(data?.closePrice);
    if (
      data?.completed !== true ||
      !Number.isFinite(openPrice) ||
      !Number.isFinite(closePrice)
    )
      return false;
    return applyPaperWindowClose(
      windowStart,
      openPrice,
      closePrice,
      "crypto-close",
    );
  } catch (err) {
    console.warn(
      `[Paper] 收盘价补结算失败 window=${windowStart}: ${err instanceof Error ? err.message : String(err)}`,
    );
    return false;
  }
}

function updatePaperWindowOutcomeMark(
  windowStart: number,
  event: Record<string, unknown> | undefined,
  result: StrategyDirection | null,
): boolean {
  const info = paperAccount.windows[String(windowStart)];
  if (!info) return false;
  const marks = parseOutcomeMarks(event);
  let changed = false;
  if (marks) {
    if (info.upMark !== marks.up) {
      info.upMark = marks.up;
      changed = true;
    }
    if (info.downMark !== marks.down) {
      info.downMark = marks.down;
      changed = true;
    }
    info.markUpdatedAt = Date.now();
  }
  if (result && info.result !== result) {
    if (info.localResult && info.localResult !== result) {
      console.warn(
        `[Paper] 本地收盘结果与官方结果不一致 window=${windowStart} local=${info.localResult} official=${result}`,
      );
    }
    info.result = result;
    changed = true;
  }
  return changed;
}

let recentResultsRetryTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleRecentResultsRetry(currentWindow: number): void {
  if (recentResultsRetryTimer) clearTimeout(recentResultsRetryTimer);
  recentResultsRetryTimer = setTimeout(() => {
    recentResultsRetryTimer = null;
    if (stopped || subscribedWindow !== currentWindow) return;
    fetchRecentResults(currentWindow, true);
  }, PAPER_RESULT_RETRY_MS);
}

async function fetchRecentResults(
  currentWindow: number,
  immediate = false,
): Promise<void> {
  if (!immediate) await new Promise((r) => setTimeout(r, 5000));
  if (stopped) return;
  try {
    const slugs = [1, 2, 3, 4].map(
      (i) => `btc-updown-5m-${currentWindow - i * 300}`,
    );
    const query = slugs.map((s) => `slug=${s}`).join("&");
    const events = (await fetch(`${GAMMA_URL}/events?${query}`).then((r) =>
      r.json(),
    )) as Record<string, unknown>[];
    let paperMarkChanged = false;
    let retryUnsettledPaper = false;
    const results: Array<{
      windowStart: number;
      timeRange: string;
      result: StrategyDirection | null;
      localResult: StrategyDirection | null;
      upMark: number | null;
      downMark: number | null;
    }> = [];
    for (const slug of slugs) {
      const event = events.find(
        (e: Record<string, unknown>) => e.slug === slug,
      ) as Record<string, unknown> | undefined;
      const ws = parseInt(slug.split("-").pop()!);
      const timeRange = `${new Date(ws * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}→${new Date((ws + 300) * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}`;
      const result = parseResolvedOutcome(event);
      const marks = parseOutcomeMarks(event);
      paperMarkChanged =
        updatePaperWindowOutcomeMark(ws, event, result) || paperMarkChanged;
      if (result) {
        const settled = settlePaperWindow(ws, result);
        paperMarkChanged = settled || paperMarkChanged;
      } else if (hasOpenPaperPositionForWindow(ws)) {
        const closed = await capturePaperWindowCloseFromCryptoApi(ws);
        paperMarkChanged = closed || paperMarkChanged;
        if (hasOpenPaperPositionForWindow(ws)) retryUnsettledPaper = true;
      }
      const info = paperAccount.windows[String(ws)];
      results.push({
        windowStart: ws,
        timeRange,
        result,
        localResult:
          info?.localResult === "up" || info?.localResult === "down"
            ? info.localResult
            : null,
        upMark: marks?.up ?? null,
        downMark: marks?.down ?? null,
      });
    }
    const summary = results
      .map(
        (item) =>
          `${item.timeRange}${item.result === "up" ? "涨赢" : item.result === "down" ? "跌赢" : "待确认"}`,
      )
      .join(" | ");
    console.log(`[Result] ${summary}`);
    if (paperMarkChanged) {
      persistPaperAccountState();
      broadcastState();
    }
    if (retryUnsettledPaper) scheduleRecentResultsRetry(currentWindow);
    broadcast("recentResults", { results });
  } catch (e) {
    console.error(`[Result] 请求失败:`, (e as Error).message);
    scheduleRecentResultsRetry(currentWindow);
  }
}

// ── 窗口切换 ──────────────────────────────────────────────────
let subscribedWindow = 0;
let stopped = false;
let switchTimer: ReturnType<typeof setTimeout> | null = null;
let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

function disconnectWindowStreams(): void {
  const hadMarketFeed = !!marketWs || wsStatus.market;
  const hadChainlinkFeed = !!chainlinkWs || wsStatus.chainlink;
  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }
  if (marketPingTimer) {
    clearInterval(marketPingTimer);
    marketPingTimer = null;
  }
  if (marketRenderTimer) {
    clearInterval(marketRenderTimer);
    marketRenderTimer = null;
  }
  if (marketValidationTimer) {
    clearInterval(marketValidationTimer);
    marketValidationTimer = null;
  }
  if (marketWs) {
    marketWs.removeAllListeners("close");
    marketWs.close();
    marketWs = null;
  }
  if (chainlinkWs) {
    chainlinkWs.removeAllListeners("close");
    chainlinkWs.close();
    chainlinkWs = null;
  }
  if (wsStatus.market || wsStatus.chainlink) {
    wsStatus.market = false;
    wsStatus.chainlink = false;
    broadcastWsStatus();
  }
  if (hadMarketFeed) broadcast("marketDown", {});
  if (hadChainlinkFeed) broadcast("chainlinkDown", {});
}

function clearWindowRuntimeState(): void {
  state.bids.clear();
  state.asks.clear();
  state.bestBid = "-";
  state.bestAsk = "-";
  state.bookUpdatedAt = 0;
  state.bookEventTs = 0;
  state.bookSource = "";
  state.bookCheckAt = 0;
  state.bookCheckLatencyMs = null;
  state.bookCheckDiffPct = null;
  state.lastPrice = "-";
  state.lastSide = "";
  state.lastPriceUpdatedAt = 0;
  lastWsBookUpdateAt = 0;
  state.priceToBeat = null;
  state.currentPrice = null;
  state.currentPriceUpdatedAt = 0;
  state.binanceOffset = null;
  state.updatedAt = Date.now();
  marketBestReady = false;
  marketValidationMismatchStreak = 0;
  bestBidAskPausedUntil = 0;
  paperMakerOrders = [];
  paperMakerLastFillAt = { up: 0, down: 0 };
  paperMakerLastReason = "";
  strategyRuntime.positionsReady =
    strategyConfig.executionMode === "paper" || !PROXY_ADDRESS;
  resetStrategyRuntime();
  broadcastState();
}

function getCurrentWindowStart(now = getPolymarketNowMs()): number {
  return Math.floor(now / 1000 / 300) * 300;
}

async function advanceToLiveWindow(targetWindowStart: number): Promise<void> {
  const switchStartedAt = Date.now();
  let attempt = 0;
  let clearedExpiredWindow = false;
  while (!stopped) {
    const desiredWindow = Math.max(targetWindowStart, getCurrentWindowStart());
    if (attempt === 0) {
      console.log(
        `[Window] 切换开始 ${subscribedWindow || "-"} -> ${desiredWindow}`,
      );
    }
    if (!clearedExpiredWindow && desiredWindow > subscribedWindow) {
      if (subscribedWindow > 0) {
        capturePaperWindowCloseEstimate(subscribedWindow);
        void capturePaperWindowCloseFromCryptoApi(subscribedWindow);
      }
      disconnectWindowStreams();
      clearWindowRuntimeState();
      clearedExpiredWindow = true;
    }
    const subscribeStartedAt = Date.now();
    await subscribeWindow(desiredWindow);
    if (subscribedWindow === desiredWindow) {
      console.log(
        `[Window] 切换成功 windowStart=${desiredWindow} 耗时:${Date.now() - switchStartedAt}ms`,
      );
      return;
    }

    const delay = Math.min(1000 * Math.max(++attempt, 1), 5000);
    console.warn(
      `[Window] 切换重试 windowStart=${desiredWindow} ${delay}ms 后继续`,
    );
    await new Promise((r) => setTimeout(r, delay));
  }
}

function scheduleNextWindow(windowEnd: number): void {
  if (switchTimer) clearTimeout(switchTimer);
  const msUntilEnd = windowEnd * 1000 - getPolymarketNowMs();
  switchTimer = setTimeout(
    async () => {
      if (stopped) return;
      await advanceToLiveWindow(windowEnd);
    },
    Math.max(0, msUntilEnd),
  );
}

async function subscribeWindow(windowStart: number): Promise<void> {
  const startedAt = Date.now();
  const info = await fetchMarket(windowStart);
  if (!info) {
    broadcast("error", { message: `未找到市场 windowStart=${windowStart}` });
    console.warn(
      `[Window] 订阅失败 windowStart=${windowStart} 耗时:${Date.now() - startedAt}ms`,
    );
    return;
  }

  const isNewWindow = subscribedWindow !== windowStart;
  const prevWindowStart = subscribedWindow;
  subscribedWindow = windowStart;

  state.windowStart = info.windowStart;
  state.windowEnd = info.windowEnd;
  state.upTokenId = info.upTokenId;
  state.downTokenId = info.downTokenId;
  state.conditionId = info.conditionId;
  rememberPaperWindow({
    windowStart: info.windowStart,
    upTokenId: info.upTokenId,
    downTokenId: info.downTokenId,
    eventStartTime: info.eventStartTime,
    endDate: info.endDate,
  });
  state.bids.clear();
  state.asks.clear();
  state.bestBid = "-";
  state.bestAsk = "-";
  state.bookUpdatedAt = 0;
  state.bookEventTs = 0;
  state.bookSource = "";
  state.bookCheckAt = 0;
  state.bookCheckLatencyMs = null;
  state.bookCheckDiffPct = null;
  fullSetArbSnapshot = {
    ...createEmptyFullSetArbSnapshot("window switching"),
    status: "switching",
    windowStart: info.windowStart,
  };
  state.lastPrice = "-";
  state.lastSide = "";
  state.lastPriceUpdatedAt = 0;
  state.binanceOffset = null;
  state.updatedAt = Date.now();
  lastBestBidAskTimestamp = 0;
  lastWsBookUpdateAt = 0;
  marketBestReady = false;
  bestBidAskPausedUntil = 0;
  marketValidationMismatchStreak = 0;
  marketReconnectPending = false;

  if (isNewWindow) {
    if (prevWindowStart > 0) fetchRecentResults(windowStart);
    state.priceToBeat = null;
    state.currentPrice = null;
    strategyRuntime.positionsReady =
      strategyConfig.executionMode === "paper" || !PROXY_ADDRESS;
    resetStrategyRuntime(`切换到窗口 ${windowStart}`);
    prunePositionCaches([info.upTokenId, info.downTokenId]);
    positions.localSize[info.upTokenId] = 0;
    positions.localSize[info.downTokenId] = 0;
    positions.apiSize[info.upTokenId] = 0;
    positions.apiSize[info.downTokenId] = 0;
    positions.apiVerified[info.upTokenId] = false;
    positions.apiVerified[info.downTokenId] = false;
    const thisWindow = info.windowStart;
    const tryFetch = () => {
      if (stopped || subscribedWindow !== thisWindow) return;
      fetchCryptoPrice(info.eventStartTime, info.endDate).then(() => {
        if (
          state.priceToBeat == null &&
          !stopped &&
          subscribedWindow === thisWindow
        )
          setTimeout(tryFetch, 1000);
        else broadcastState();
      });
    };
    tryFetch();
    syncPositionsFromApi().then(() => broadcastState());
  }

  broadcast("window", {
    windowStart: info.windowStart,
    windowEnd: info.windowEnd,
    conditionId: info.conditionId,
    upTokenId: info.upTokenId,
    downTokenId: info.downTokenId,
    ts: Date.now(),
    exchangeTs: getPolymarketNowMs(),
    polymarketClockOffsetMs,
    polymarketClockAgeMs: getPolymarketClockAgeMs(),
    polymarketClockSource: polymarketClockSource || null,
  });

  if (marketWs || chainlinkWs || reconnectTimer) {
    disconnectWindowStreams();
  }

  marketWs = startMarketWs(
    info.windowStart,
    info.upTokenId,
    info.downTokenId,
    () => {
      if (stopped) return;
      if (reconnectTimer) clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(() => {
        void subscribeWindow(
          Math.max(subscribedWindow, getCurrentWindowStart()),
        );
      }, 1000);
    },
  );

  const eventSlug = `btc-updown-5m-${info.windowStart}`;
  let clAttempt = 0;
  const reconnectChainlink = () => {
    if (stopped) return;
    const delay = backoffDelay(clAttempt);
    clAttempt++;
    setTimeout(() => {
      if (stopped) return;
      chainlinkWs = startChainlinkWs(
        subscribedWindow,
        `btc-updown-5m-${subscribedWindow}`,
        reconnectChainlink,
        clAttempt,
      );
    }, delay);
  };
  chainlinkWs = startChainlinkWs(
    info.windowStart,
    eventSlug,
    reconnectChainlink,
    0,
  );

  scheduleNextWindow(info.windowEnd);
}

// ── Claim 查询 ────────────────────────────────────────────────
interface ClaimPosition {
  conditionId: string;
  title: string;
  currentValue: number;
  size: number;
}
let claimablePositions: ClaimPosition[] = [];
let claimableTotal = 0;
let claimCycleTimer: ReturnType<typeof setTimeout> | null = null;
let claimCycleRunning = false;
let claimNextCheckAt = 0;
let claimCooldownUntil = 0; // Claim 冷却截止时间戳（5 分钟）
let claimLastReason = ""; // 最近一次跳过的原因，供前端显示

function broadcastClaimCooldown(running = false): void {
  broadcast("claimCooldown", {
    running,
    nextCheckAt: claimNextCheckAt,
    cooldownUntil: claimCooldownUntil,
    reason: claimLastReason,
  });
}

function resetClaimableState(): void {
  claimablePositions = [];
  claimableTotal = 0;
  broadcast("claimable", {
    total: claimableTotal,
    positions: claimablePositions,
  });
}

async function syncClaimable(
  options: { clearOnError?: boolean } = {},
): Promise<boolean> {
  if (!PROXY_ADDRESS) {
    resetClaimableState();
    return false;
  }
  try {
    const pos = (await fetch(
      `https://data-api.polymarket.com/positions?user=${PROXY_ADDRESS}&sizeThreshold=.01&redeemable=true&limit=100&offset=0`,
    ).then((r) => r.json())) as Array<{
      conditionId: string;
      title: string;
      currentValue: number;
      size: number;
      curPrice: number;
    }>;
    claimablePositions = pos
      .filter((p) => p.curPrice === 1)
      .map((p) => ({
        conditionId: p.conditionId,
        title: p.title,
        currentValue: p.currentValue,
        size: p.size,
      }));
    claimableTotal = claimablePositions.reduce((s, p) => s + p.currentValue, 0);
    broadcast("claimable", {
      total: claimableTotal,
      positions: claimablePositions,
    });
    return true;
  } catch (err) {
    if (options.clearOnError) resetClaimableState();
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`[Claim] 查询可领取仓位失败: ${msg}`);
    return false;
  }
}

function scheduleClaimCycle(delayMs = CLAIM_CYCLE_DELAY_MS): void {
  if (stopped || !PROXY_ADDRESS) {
    if (claimCycleTimer) clearTimeout(claimCycleTimer);
    claimCycleTimer = null;
    claimNextCheckAt = 0;
    broadcastClaimCooldown(false);
    return;
  }
  if (claimCycleTimer) clearTimeout(claimCycleTimer);
  claimNextCheckAt = Date.now() + Math.max(0, delayMs);
  broadcastClaimCooldown(false);
  claimCycleTimer = setTimeout(
    () => {
      void autoClaimCycle().catch((err) => {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[自动Claim] 后台领取异常: ${msg}`);
        scheduleClaimCycle();
      });
    },
    Math.max(0, delayMs),
  );
}

async function autoClaimCycle(): Promise<void> {
  if (stopped || claimCycleRunning) return;
  claimCycleRunning = true;
  claimNextCheckAt = 0;
  broadcastClaimCooldown(true);
  try {
    // Step 1: 每次循环都查询最新可领取金额（高频刷新，前端显示实时）
    const synced = await syncClaimable({ clearOnError: true });
    if (!synced) return;

    // Step 2: 决定是否执行 claim
    if (!strategyConfig.autoClaimEnabled || !PRIVATE_KEY) {
      claimLastReason = "";
      return;
    }
    if (!claimablePositions.length || claimInProgress) {
      claimLastReason = "";
      return;
    }

    // 冷却期：上次 claim 后 5 分钟内不再执行
    if (Date.now() < claimCooldownUntil) {
      const remain = Math.ceil((claimCooldownUntil - Date.now()) / 1000);
      claimLastReason = `冷却中 剩余${remain}s`;
      return;
    }

    // 策略忙：入场/持仓/出场中 → 不触发 Safe 交易（避免 nonce 冲突）
    const strategyBusy =
      strategyRuntime.state !== "IDLE" &&
      strategyRuntime.state !== "DONE" &&
      strategyRuntime.state !== "SCANNING";
    if (strategyBusy || hasOpenPosition()) {
      claimLastReason = `策略忙(${strategyRuntime.state})`;
      console.log(`[自动Claim] ${claimLastReason}，跳过本次`);
      return;
    }

    // Step 3: claim 前再刷新一次金额（确保 conditionId 和数量最新）
    await syncClaimable({ clearOnError: false });
    if (!claimablePositions.length) {
      claimLastReason = "";
      return;
    }

    console.log(
      `[自动Claim] 检测到 ${claimablePositions.length} 个可领取仓位，后台开始领取...`,
    );
    claimLastReason = "";
    claimCooldownUntil = Date.now() + CLAIM_COOLDOWN_MS; // 进入冷却（无论下面成功失败）
    await runClaim({ refreshAfter: false });
  } finally {
    claimCycleRunning = false;
    scheduleClaimCycle();
  }
}

// ── Claim 核心逻辑 ───────────────────────────────────────────
let claimInProgress = false;

// 复用 provider：避免每次 claim 都 new 一个触发网络探测循环
// 用完整 Network 对象 + staticNetwork 对象版本，跳过启动时的 eth_chainId 探测
const CLAIM_NETWORK = new ethers.Network("polygon", 137);
let cachedClaimProvider: ethers.JsonRpcProvider | null = null;
function getClaimProvider(): ethers.JsonRpcProvider {
  if (!cachedClaimProvider) {
    cachedClaimProvider = new ethers.JsonRpcProvider(
      "https://polygon-bor-rpc.publicnode.com",
      CLAIM_NETWORK,
      { staticNetwork: CLAIM_NETWORK },
    );
    // 静默 RPC error（原来的 console.error 会反复打印相同错误）
    cachedClaimProvider.on("error", () => {
      /* 由调用方处理 */
    });
  }
  return cachedClaimProvider;
}

async function runClaim(
  options: { refreshAfter?: boolean } = {},
): Promise<{ title: string; txHash?: string; error?: string }[]> {
  const { refreshAfter = true } = options;
  if (!PROXY_ADDRESS || !PRIVATE_KEY) return [];
  if (claimInProgress) return [];
  if (!claimablePositions.length) return [];
  claimInProgress = true;

  const contracts = getContractConfig(137);
  const CTF = contracts.conditionalTokens;
  const USDC_ADDR = contracts.collateral; // V2 升级后为 pUSD
  const ZERO_BYTES32 =
    "0x0000000000000000000000000000000000000000000000000000000000000000";

  const provider = getClaimProvider();
  const wallet = new ethers.Wallet(PRIVATE_KEY, provider);

  const ctfIface = new ethers.Interface([
    "function redeemPositions(address collateralToken, bytes32 parentCollectionId, bytes32 conditionId, uint256[] indexSets)",
  ]);
  const safeIface = new ethers.Interface([
    "function nonce() view returns (uint256)",
    "function getTransactionHash(address to, uint256 value, bytes calldata data, uint8 operation, uint256 safeTxGas, uint256 baseGas, uint256 gasPrice, address gasToken, address refundReceiver, uint256 nonce) view returns (bytes32)",
    "function execTransaction(address to, uint256 value, bytes calldata data, uint8 operation, uint256 safeTxGas, uint256 baseGas, uint256 gasPrice, address gasToken, address payable refundReceiver, bytes memory signatures) public payable returns (bool)",
  ]);
  const safe = new ethers.Contract(PROXY_ADDRESS, safeIface, wallet);

  const snapshot = [...claimablePositions];
  const total = snapshot.length;
  const results: { title: string; txHash?: string; error?: string }[] = [];
  console.log(
    `[Claim] 开始领取 共${total}个: ${snapshot.map((p) => p.title).join(" | ")}`,
  );
  try {
    for (let i = 0; i < snapshot.length; i++) {
      const p = snapshot[i];
      console.log(
        `[Claim] (${i + 1}/${total}) ${p.title} 金额:${p.currentValue.toFixed(2)} conditionId:${p.conditionId}`,
      );
      broadcast("claimProgress", {
        current: i,
        total,
        title: p.title,
        status: "running",
      });
      try {
        const calldata = ctfIface.encodeFunctionData("redeemPositions", [
          USDC_ADDR,
          ZERO_BYTES32,
          p.conditionId,
          [1, 2],
        ]);
        const nonce = await safe.nonce();
        console.log(`[Claim] nonce:${nonce} 构建交易中...`);
        const txHash = await safe.getTransactionHash(
          CTF,
          0,
          calldata,
          0,
          0,
          0,
          0,
          ethers.ZeroAddress,
          ethers.ZeroAddress,
          nonce,
        );
        const sig = await wallet.signMessage(ethers.getBytes(txHash));
        const v = parseInt(sig.slice(-2), 16) + 4;
        const adjustedSig = sig.slice(0, -2) + v.toString(16).padStart(2, "0");
        console.log(`[Claim] 发送交易...`);
        const tx = await safe.execTransaction(
          CTF,
          0,
          calldata,
          0,
          0,
          0,
          0,
          ethers.ZeroAddress,
          ethers.ZeroAddress,
          adjustedSig,
        );
        console.log(`[Claim] 等待上链 txHash:${tx.hash}`);
        const receipt = await Promise.race([
          tx.wait(),
          new Promise<null>((_, reject) =>
            setTimeout(() => reject(new Error("等待上链超时(30s)")), 30000),
          ),
        ]);
        if (!receipt) throw new Error("等待上链超时(30s)");
        console.log(`[Claim] ✓ 成功 ${p.title} → ${tx.hash}`);
        results.push({ title: p.title, txHash: tx.hash });
        broadcast("claimProgress", {
          current: i + 1,
          total,
          title: p.title,
          status: "success",
        });
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        console.error(`[Claim] ✗ 失败 ${p.title}: ${msg}`);
        results.push({ title: p.title, error: msg });
        broadcast("claimProgress", {
          current: i + 1,
          total,
          title: p.title,
          status: "error",
          error: msg,
        });
      }
    }
  } finally {
    claimInProgress = false;
  }
  console.log(
    `[Claim] 完成 成功:${results.filter((r) => r.txHash).length} 失败:${results.filter((r) => r.error).length}`,
  );
  if (refreshAfter) {
    await syncClaimable({ clearOnError: true });
    await syncUsdcBalance();
    broadcastState();
  }
  return results;
}

function extractOrderError(result: unknown): string {
  const obj =
    result && typeof result === "object"
      ? (result as Record<string, unknown>)
      : {};
  const candidates = [obj.error, obj.message, obj.errorMsg, obj.errorMessage];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim())
      return candidate.trim();
  }
  return "";
}

function extractOrderId(result: unknown): string {
  const obj =
    result && typeof result === "object"
      ? (result as Record<string, unknown>)
      : {};
  const candidates = [obj.orderID, obj.orderId, obj.id];
  for (const candidate of candidates) {
    if (typeof candidate === "string" && candidate.trim())
      return candidate.trim();
  }
  const nested = isRecord(obj.order) ? obj.order : null;
  if (typeof nested?.id === "string" && nested.id.trim())
    return nested.id.trim();
  return "";
}

function fmtOrderField(value: unknown): string {
  if (value == null || value === "") return "-";
  return String(value);
}

function getDecimalPlaces(value: string | number): number {
  const text = String(value);
  const [, decimals = ""] = text.split(".");
  return decimals.replace(/0+$/, "").length;
}

function floorToDecimals(value: number, decimals: number): number {
  const factor = 10 ** decimals;
  return Math.floor((value + Number.EPSILON) * factor) / factor;
}

function appendMakerReason(reason: string | undefined, tag: string): string {
  return [reason || "", tag].filter(Boolean).join(" ");
}

function isRiskReducingMakerQuote(quote: MakerQuoteSignal): boolean {
  const reason = String(quote.reason || "");
  return /inv=(insurance|balanced_repair|lock_hedge)|repairTo=|balanced_repair|lock_hedge| insurance/.test(
    reason,
  );
}

function getPostOnlySafeMakerPrice(
  price: number,
  topBid: number,
  topAsk: number,
  tickSize: number,
  decimals: number,
): { price: number; adjusted: boolean } | null {
  if (!(price > 0) || !(topBid > 0)) return null;
  const askCap = topAsk > 0 ? topAsk - tickSize : topBid;
  const safeCap = Math.min(topBid, askCap);
  if (!(safeCap > 0)) return null;
  const safePrice = floorToDecimals(Math.min(price, safeCap), decimals);
  if (!(safePrice > 0) || safePrice >= 1) return null;
  return {
    price: safePrice,
    adjusted: safePrice < price - 1e-9,
  };
}

function isOrderWindowStale(now = getPolymarketNowMs()): boolean {
  if (!state.windowStart || !state.windowEnd) return true;
  if (state.windowEnd * 1000 <= now) return true;
  return state.windowStart < getCurrentWindowStart(now);
}

function getStrategyRemainingSeconds(now = getPolymarketNowMs()): number {
  return state.windowEnd ? state.windowEnd - Math.floor(now / 1000) : 0;
}

function buildStrategyDiffHistory(): import("./strategies/types.js").StrategyDiffSample[] {
  if (
    !state.windowEnd ||
    state.priceToBeat == null ||
    state.binanceOffset == null
  )
    return [];
  const threshold = state.priceToBeat - state.binanceOffset;
  const startMs = Math.max(
    state.windowStart > 0 ? state.windowStart * 1000 : 0,
    Date.now() - HISTORY_RETENTION_MS,
  );
  const endMs = state.windowEnd * 1000;
  return state.binanceHistory
    .filter(
      (point) =>
        point.t >= startMs && point.t <= endMs && Number.isFinite(point.price),
    )
    .map((point) => ({
      t: point.t,
      rem: Math.max(0, (endMs - point.t) / 1000),
      diff: point.price - threshold,
    }))
    .filter(
      (point) => Number.isFinite(point.rem) && Number.isFinite(point.diff),
    );
}

function toStrategyBookLevels(
  levels: BookLevel[],
): import("./strategies/types.js").StrategyBookLevel[] {
  return levels.slice(0, 24).map((level) => ({
    price: level.price,
    size: level.size,
  }));
}

function buildTickContext(
  rem: number,
  upPct: number | null,
  dnPct: number | null,
  diff: number | null,
  now: number,
): import("./strategies/types.js").StrategyTickContext {
  const bestBid = Number(state.bestBid);
  const bestAsk = Number(state.bestAsk);
  const paperCostBasis =
    strategyConfig.executionMode === "paper" ? getPaperOpenCostBasis() : null;
  const upBook = getStateBookLevelsForDirection("up");
  const downBook = getStateBookLevelsForDirection("down");
  const macd1m = buildMacdSnapshot(state.kline1m, {
    fast: 12,
    slow: 26,
    signal: 9,
    confirmBars: 3,
    minHistBps: 0.02,
    minSlopeBps: 0,
  });
  const macdFast1m = buildMacdSnapshot(state.kline1m, {
    fast: 6,
    slow: 13,
    signal: 5,
    confirmBars: 3,
    minHistBps: 0.02,
    minSlopeBps: 0,
  });
  return {
    rem,
    upPct,
    dnPct,
    diff,
    now,
    prevUpPct: strategyRuntime.prevUpPct,
    bestBid: Number.isFinite(bestBid) && bestBid >= 0 ? bestBid : null,
    bestAsk: Number.isFinite(bestAsk) && bestAsk > 0 ? bestAsk : null,
    kline1m: state.kline1m,
    kline5m: state.kline5m,
    macd1m,
    macdFast1m,
    fullSetArb: computeFullSetArbPayload(),
    marketHoursOnly: strategyConfig.marketHoursOnly,
    configuredAmount: Number(strategyConfig.amount.s10) || undefined,
    s10TailMultipliers: strategyConfig.s10TailMultipliers,
    bookAgeMs: state.bookUpdatedAt > 0 ? now - state.bookUpdatedAt : null,
    bookSource: state.bookSource || null,
    diffHistory: buildStrategyDiffHistory(),
    book: {
      up: {
        bids: toStrategyBookLevels(upBook.bids),
        asks: toStrategyBookLevels(upBook.asks),
      },
      down: {
        bids: toStrategyBookLevels(downBook.bids),
        asks: toStrategyBookLevels(downBook.asks),
      },
    },
    position: {
      upSize: getDirectionLocalSize("up"),
      downSize: getDirectionLocalSize("down"),
      upCostPct: getDirectionCostPctFromBasis(paperCostBasis, "up"),
      downCostPct: getDirectionCostPctFromBasis(paperCostBasis, "down"),
    },
  };
}

function getS10MakerOwnedPosition(
  mode: ExecutionMode,
  windowStart: number,
): import("./strategies/types.js").StrategyTickContext["position"] {
  const sourceHistory = mode === "paper" ? paperTradeHistory : tradeHistory;
  const inventory: Record<StrategyDirection, { shares: number; cost: number }> =
    {
      up: { shares: 0, cost: 0 },
      down: { shares: 0, cost: 0 },
    };

  for (const trade of sourceHistory) {
    if (trade.windowStart !== windowStart) continue;
    if (!/^strategy10(?:maker|sweep)/.test(String(trade.source || "")))
      continue;
    const status = String(trade.status || "");
    if (!status.includes("MINED") && !status.includes("FILLED")) continue;
    const direction = trade.direction;
    const side = trade.side;
    const shares = Number(trade.filledShares ?? trade.amount);
    const price = Number(trade.avgPrice ?? trade.price);
    if (
      (direction !== "up" && direction !== "down") ||
      (side !== "buy" && side !== "sell")
    )
      continue;
    if (
      !Number.isFinite(shares) ||
      shares <= 0 ||
      !Number.isFinite(price) ||
      price < 0
    )
      continue;

    const bucket = inventory[direction];
    if (side === "buy") {
      bucket.shares += shares;
      bucket.cost += shares * price;
    } else {
      const reduce = Math.min(bucket.shares, shares);
      const avgCost = bucket.shares > 0 ? bucket.cost / bucket.shares : 0;
      bucket.shares = Math.max(0, bucket.shares - reduce);
      bucket.cost = Math.max(0, bucket.cost - reduce * avgCost);
    }
  }

  const costPct = (item: { shares: number; cost: number }) =>
    item.shares > 0
      ? clampNumber((item.cost / item.shares) * 100, 0, 100)
      : null;
  return {
    upSize: Math.round(inventory.up.shares * 10000) / 10000,
    downSize: Math.round(inventory.down.shares * 10000) / 10000,
    upCostPct: costPct(inventory.up),
    downCostPct: costPct(inventory.down),
  };
}

function withS10MakerOwnedPosition(
  ctx: import("./strategies/types.js").StrategyTickContext,
): import("./strategies/types.js").StrategyTickContext {
  return {
    ...ctx,
    position: getS10MakerOwnedPosition(
      strategyConfig.executionMode,
      state.windowStart,
    ),
  };
}

function getMacdEntryBlockReason(
  ctx: import("./strategies/types.js").StrategyTickContext,
  direction: StrategyDirection,
): string | null {
  if (!MACD_FILTER_ENABLED) return null;
  if (ctx.rem <= MACD_FILTER_MIN_REMAINING) return null;
  if (ctx.diff != null && Math.abs(ctx.diff) >= MACD_FILTER_STRONG_DIFF_BYPASS)
    return null;
  if (!ctx.macd1m.ready || ctx.macd1m.trend === "neutral") return null;
  if (!isDirectionAgainstTrend(direction, ctx.macd1m.trend)) return null;

  if (
    MACD_FILTER_REQUIRE_FAST_AGREE &&
    (!ctx.macdFast1m.ready || ctx.macdFast1m.trend !== ctx.macd1m.trend)
  ) {
    return null;
  }

  const trendText = ctx.macd1m.trend === "bullish" ? "上行" : "下行";
  const dirText = direction === "up" ? "买涨" : "买跌";
  return `MACD${trendText}过滤${dirText} hist=${ctx.macd1m.histogramBps?.toFixed(3) ?? "-"}bps fast=${ctx.macdFast1m.trend}`;
}

function checkEntry(ctx: import("./strategies/types.js").StrategyTickContext): {
  strategy: StrategyNumber;
  dir: StrategyDirection;
  amount?: number;
  reason?: string;
  source?: string;
  maxPrice?: number;
} | null {
  macdFilterBlockedReason = "";
  for (const s of getAllStrategies()) {
    if (!strategyConfig.enabled[s.key]) continue;
    if (s.key === "s10" && S10_MAKER_ENGINE_ENABLED && S10_MAKER_ONLY) continue;
    const signal = s.checkEntry(ctx);
    if (!signal) continue;
    const macdBlockReason =
      s.key === "s10" ? null : getMacdEntryBlockReason(ctx, signal.direction);
    if (macdBlockReason) {
      const nextReason = `S${s.number} ${macdBlockReason}`;
      const now = Date.now();
      if (
        nextReason !== macdFilterBlockedReason ||
        now - macdFilterBlockedAt > 5000
      ) {
        console.log(`[MACD] ${nextReason}`);
      }
      macdFilterBlockedReason = nextReason;
      macdFilterBlockedAt = now;
      continue;
    }
    return {
      strategy: s.number,
      dir: signal.direction,
      amount: signal.amount,
      reason: signal.reason,
      source: signal.source,
      maxPrice: signal.maxPrice,
    };
  }
  return null;
}

function isStrategyRuntimeBusyForTerminalSweep(): boolean {
  return (
    strategyRuntime.state === "BUYING" ||
    strategyRuntime.state === "WAIT_FILL" ||
    strategyRuntime.state === "RECONCILING_FILL" ||
    strategyRuntime.state === "LOCKING" ||
    strategyRuntime.state === "WAIT_LOCK_FILL" ||
    strategyRuntime.state === "SELLING" ||
    strategyRuntime.state === "WAIT_SELL_FILL"
  );
}

function getS10TerminalSweepWindowNotional(
  mode: ExecutionMode,
  windowStart: number,
): number {
  const sourceHistory = mode === "paper" ? paperTradeHistory : tradeHistory;
  return sourceHistory.reduce((sum, trade) => {
    if (
      trade.windowStart !== windowStart ||
      trade.side !== "buy" ||
      trade.source !== "strategy10sweep"
    )
      return sum;
    const status = String(trade.status || "").toUpperCase();
    if (
      !status.includes("FILLED") &&
      !status.includes("MINED") &&
      !status.includes("MATCHED")
    )
      return sum;
    return (
      sum +
      (Number(
        trade.filledNotional ??
          trade.requestedAmount ??
          (trade.amount || 0) * (trade.price || 0),
      ) || 0)
    );
  }, 0);
}

function getS10TerminalSweepWindowFillCount(
  mode: ExecutionMode,
  windowStart: number,
): number {
  const sourceHistory = mode === "paper" ? paperTradeHistory : tradeHistory;
  return sourceHistory.reduce((sum, trade) => {
    if (
      trade.windowStart !== windowStart ||
      trade.side !== "buy" ||
      trade.source !== "strategy10sweep"
    )
      return sum;
    const status = String(trade.status || "").toUpperCase();
    if (
      !status.includes("FILLED") &&
      !status.includes("MINED") &&
      !status.includes("MATCHED")
    )
      return sum;
    return sum + 1;
  }, 0);
}

function getS10TerminalSweepAttemptCount(windowStart: number): number {
  if (s10TerminalSweepAttemptWindowStart !== windowStart) {
    s10TerminalSweepAttemptWindowStart = windowStart;
    s10TerminalSweepAttemptCount = 0;
  }
  return s10TerminalSweepAttemptCount;
}

function reconcileS10TerminalSweep(
  ctx: import("./strategies/types.js").StrategyTickContext,
): void {
  if (!strategyConfig.enabled.s10) return;
  if (s10TerminalSweepInFlight) return;
  const now = Date.now();
  if (now - s10TerminalSweepLastAt < S10_TERMINAL_SWEEP_COOLDOWN_MS) return;
  if (isStrategyRuntimeBusyForTerminalSweep() || hasPendingStrategyBuyLock(now))
    return;
  if (isOrderWindowStale(getPolymarketNowMs())) {
    s10TerminalSweepLastReason = "window_stale";
    return;
  }
  const freshRem = getStrategyRemainingSeconds(getPolymarketNowMs());
  if (freshRem < S10_TERMINAL_SWEEP_MIN_REMAINING_SEC) {
    s10TerminalSweepLastReason = `too_late:rem=${freshRem.toFixed(1)}s<${S10_TERMINAL_SWEEP_MIN_REMAINING_SEC.toFixed(1)}s`;
    return;
  }
  const filledCount = getS10TerminalSweepWindowFillCount(
    strategyConfig.executionMode,
    state.windowStart,
  );
  const attemptCount = getS10TerminalSweepAttemptCount(state.windowStart);
  if (
    Math.max(filledCount, attemptCount) >=
    S10_TERMINAL_SWEEP_MAX_ORDERS_PER_WINDOW
  ) {
    s10TerminalSweepLastReason = `order_count_cap:${Math.max(filledCount, attemptCount)}/${S10_TERMINAL_SWEEP_MAX_ORDERS_PER_WINDOW}`;
    return;
  }

  const s10 = getStrategy("s10");
  const signal = s10?.checkOverlayEntry?.(withS10MakerOwnedPosition(ctx));
  if (!signal || signal.direction == null) return;

  const preOrderRem = getStrategyRemainingSeconds(getPolymarketNowMs());
  if (
    preOrderRem < S10_TERMINAL_SWEEP_MIN_REMAINING_SEC ||
    isOrderWindowStale(getPolymarketNowMs())
  ) {
    s10TerminalSweepLastReason = `pre_order_too_late:rem=${preOrderRem.toFixed(1)}s`;
    return;
  }
  let amount = Number(signal.amount);
  if (!Number.isFinite(amount) || amount <= 0) return;
  const used = getS10TerminalSweepWindowNotional(
    strategyConfig.executionMode,
    state.windowStart,
  );
  const remainingCap = Math.max(0, S10_TERMINAL_SWEEP_MAX_WINDOW_USDC - used);
  if (remainingCap < 5) {
    s10TerminalSweepLastReason = `window_cap:${used.toFixed(2)}/${S10_TERMINAL_SWEEP_MAX_WINDOW_USDC.toFixed(2)}`;
    return;
  }
  amount = Math.min(amount, remainingCap);
  if (!hasEnoughUsdcForBuy(amount)) {
    s10TerminalSweepLastReason = `insufficient_usdc:${amount.toFixed(2)}`;
    return;
  }

  s10TerminalSweepInFlight = true;
  s10TerminalSweepLastAt = now;
  s10TerminalSweepAttemptCount += 1;
  s10TerminalSweepLastReason = signal.reason || "terminal sweep";
  console.log(
    `[Strategy10Sweep] trigger ${signal.direction} amount=${amount.toFixed(2)} maxPrice=${signal.maxPrice ?? "-"} ${signal.reason || ""}`,
  );
  void (async () => {
    try {
      const orderResult = await executeStrategyOrder({
        direction: signal.direction,
        side: "buy",
        amount,
        maxPrice: signal.maxPrice,
        slippage: strategyConfig.slippage,
        source: signal.source || "strategy10sweep",
        exitReason: signal.reason,
        roundEntry: "terminal-sweep",
      });
      if (!orderResult.success) {
        s10TerminalSweepLastReason = `rejected:${orderResult.errorMessage || "order_failed"}`;
        console.log(
          `[Strategy10Sweep] rejected ${orderResult.errorMessage || "order_failed"}`,
        );
      } else {
        s10TerminalSweepLastReason = `filled:${amount.toFixed(2)} ${signal.reason || ""}`;
      }
    } finally {
      s10TerminalSweepInFlight = false;
      broadcastState();
    }
  })();
}

function checkExit(
  ctx: import("./strategies/types.js").StrategyTickContext,
): import("./strategies/types.js").ExitSignal {
  const stratNum = strategyRuntime.activeStrategy;
  const direction = strategyRuntime.direction;
  if (!stratNum || !direction) return null;
  const key = strategyKeyOf(stratNum);
  const s = getStrategy(key);
  if (!s) return null;
  return s.checkExit(ctx, direction);
}

interface PlaceOrderInput {
  direction: StrategyDirection;
  side: "buy" | "sell";
  amount: number;
  slippage?: number;
  maxPrice?: number;
  source?: string;
  exitReason?: string;
  roundEntry?: string;
}

interface OrderExecutionResult {
  success: boolean;
  statusCode: number;
  body: Record<string, unknown>;
  errorMessage?: string;
}

function sampleFixedPaperLatencyMs(): number {
  const min = Math.max(0, Math.min(PAPER_MIN_LATENCY_MS, PAPER_MAX_LATENCY_MS));
  const max = Math.max(
    min,
    Math.max(PAPER_MIN_LATENCY_MS, PAPER_MAX_LATENCY_MS),
  );
  return Math.round(min + Math.random() * (max - min));
}

function getPaperLatencyModelSnapshot(now = Date.now()): PaperLatencyEstimate {
  const minMs = Math.max(0, PAPER_MIN_LATENCY_MS);
  const maxMs = Math.max(
    minMs,
    PAPER_LATENCY_MODE === "dynamic"
      ? PAPER_DYNAMIC_MAX_LATENCY_MS
      : PAPER_MAX_LATENCY_MS,
  );
  const fixedMid = Math.round(
    (Math.max(minMs, PAPER_MAX_LATENCY_MS) + minMs) / 2,
  );
  const book = getLatencyStats(bookLatencySamples, fixedMid);
  const rest = getLatencyStats(restCheckLatencySamples, book.p80);
  const ws = getLatencyStats(wsUpdateIntervalSamples, 180);
  const bookAgeMs = state.bookUpdatedAt > 0 ? now - state.bookUpdatedAt : 600;
  const checkAgeMs = state.bookCheckAt > 0 ? now - state.bookCheckAt : 1200;
  const networkMs = Math.max(book.p80, rest.p80);
  const feedMs = Math.min(700, Math.max(ws.p80 * 0.45, bookAgeMs * 0.35));
  const staleCheckPenalty = Math.min(500, Math.max(0, checkAgeMs - 1200) * 0.2);
  const pressureMs = Math.round(Math.max(0, feedMs + staleCheckPenalty));
  const estimatedMs = clampNumber(
    Math.round(networkMs + pressureMs),
    minMs,
    maxMs,
  );
  return {
    delayMs: estimatedMs,
    mode: PAPER_LATENCY_MODE,
    minMs,
    maxMs,
    bookP80Ms: Math.round(book.p80),
    restP80Ms: Math.round(rest.p80),
    wsP80Ms: Math.round(ws.p80),
    pressureMs,
    jitterMs: 0,
    bookSamples: book.count,
    restSamples: rest.count,
    wsSamples: ws.count,
  };
}

function samplePaperLatency(): PaperLatencyEstimate {
  if (PAPER_LATENCY_MODE === "fixed") {
    const delayMs = sampleFixedPaperLatencyMs();
    return {
      delayMs,
      mode: "fixed",
      minMs: Math.max(0, PAPER_MIN_LATENCY_MS),
      maxMs: Math.max(PAPER_MIN_LATENCY_MS, PAPER_MAX_LATENCY_MS),
      bookP80Ms: 0,
      restP80Ms: 0,
      wsP80Ms: 0,
      pressureMs: 0,
      jitterMs: delayMs,
      bookSamples: bookLatencySamples.length,
      restSamples: restCheckLatencySamples.length,
      wsSamples: wsUpdateIntervalSamples.length,
    };
  }

  const estimate = getPaperLatencyModelSnapshot();
  const jitterRange = Math.max(45, Math.min(450, estimate.delayMs * 0.35));
  const jitterMs = Math.round((Math.random() - 0.35) * jitterRange);
  return {
    ...estimate,
    jitterMs,
    delayMs: clampNumber(
      Math.round(estimate.delayMs + jitterMs),
      estimate.minMs,
      estimate.maxMs,
    ),
  };
}

function getStateTopBookForDirection(
  direction: StrategyDirection,
): { bid: number; ask: number; ageMs: number } | null {
  const upBid = Number(state.bestBid);
  const upAsk = Number(state.bestAsk);
  if (
    !Number.isFinite(upBid) ||
    !Number.isFinite(upAsk) ||
    upBid < 0 ||
    upAsk <= 0 ||
    upAsk < upBid
  )
    return null;
  const ageMs =
    state.bookUpdatedAt > 0 ? Date.now() - state.bookUpdatedAt : Infinity;
  if (direction === "up") return { bid: upBid, ask: upAsk, ageMs };
  return {
    bid: clampNumber(1 - upAsk, 0.01, 0.99),
    ask: clampNumber(1 - upBid, 0.01, 0.99),
    ageMs,
  };
}

function getStateBookLevelsForDirection(direction: StrategyDirection): {
  bids: BookLevel[];
  asks: BookLevel[];
} {
  const upBids = [...state.bids.entries()]
    .map(([price, size]) => ({ price: Number(price), size: Number(size) }))
    .filter((level) => level.price > 0 && level.size > 0)
    .sort((a, b) => b.price - a.price);
  const upAsks = [...state.asks.entries()]
    .map(([price, size]) => ({ price: Number(price), size: Number(size) }))
    .filter((level) => level.price > 0 && level.size > 0)
    .sort((a, b) => a.price - b.price);
  if (direction === "up") return { bids: upBids, asks: upAsks };
  return {
    bids: upAsks
      .map((level) => ({
        price: clampNumber(1 - level.price, 0.01, 0.99),
        size: level.size,
      }))
      .sort((a, b) => b.price - a.price),
    asks: upBids
      .map((level) => ({
        price: clampNumber(1 - level.price, 0.01, 0.99),
        size: level.size,
      }))
      .sort((a, b) => a.price - b.price),
  };
}

function getLastObservedPriceForDirection(
  direction: StrategyDirection,
): number | null {
  const last = Number(state.lastPrice);
  if (!Number.isFinite(last) || last <= 0) return null;
  return direction === "up" ? last : clampNumber(1 - last, 0.01, 0.99);
}

function getLastBookTouchForMakerDirection(
  direction: StrategyDirection,
): { price: number; touchedAt: number } | null {
  const last = Number(state.lastPrice);
  const side = String(state.lastSide || "").toUpperCase();
  const touchedAt = Number(state.lastPriceUpdatedAt || 0);
  if (!Number.isFinite(last) || last <= 0 || touchedAt <= 0) return null;
  if (direction === "up") {
    if (side !== "BUY") return null;
    return { price: last, touchedAt };
  }
  if (side !== "SELL") return null;
  return { price: clampNumber(1 - last, 0.01, 0.99), touchedAt };
}

function getBookSidePrice(book: BookSnapshot, side: "buy" | "sell"): number {
  return side === "buy" ? book.topAsk : book.topBid;
}

function inspectBookSnapshot(
  book: BookSnapshot,
  direction: StrategyDirection,
  side: "buy" | "sell",
  maxBookStaleMs: number,
  maxBookWsDiff: number,
): {
  ok: boolean;
  status: string;
  reason: string | null;
  expectedBid: number | null;
  expectedAsk: number | null;
  diffPct: number | null;
} {
  const expected = getStateTopBookForDirection(direction);
  if (!expected) {
    return {
      ok: false,
      status: "rejected",
      reason: "missing_ws_book",
      expectedBid: null,
      expectedAsk: null,
      diffPct: null,
    };
  }
  if (expected.ageMs > maxBookStaleMs) {
    return {
      ok: false,
      status: "rejected",
      reason: "ws_book_stale",
      expectedBid: expected.bid,
      expectedAsk: expected.ask,
      diffPct: null,
    };
  }
  const actual = getBookSidePrice(book, side);
  const expectedSidePrice = side === "buy" ? expected.ask : expected.bid;
  if (!(actual > 0) || !(expectedSidePrice > 0)) {
    return {
      ok: false,
      status: "rejected",
      reason: "empty_book",
      expectedBid: expected.bid,
      expectedAsk: expected.ask,
      diffPct: null,
    };
  }
  const diff = Math.abs(actual - expectedSidePrice);
  const diffPct = diff * 100;
  if (diff > maxBookWsDiff) {
    return {
      ok: false,
      status: "rejected",
      reason: "book_ws_mismatch",
      expectedBid: expected.bid,
      expectedAsk: expected.ask,
      diffPct,
    };
  }
  return {
    ok: true,
    status: "ok",
    reason: null,
    expectedBid: expected.bid,
    expectedAsk: expected.ask,
    diffPct,
  };
}

function inspectPaperBookSnapshot(
  book: BookSnapshot,
  direction: StrategyDirection,
  side: "buy" | "sell",
) {
  return inspectBookSnapshot(
    book,
    direction,
    side,
    MAX_BOOK_STALE_MS,
    PAPER_BOOK_WS_MAX_DIFF,
  );
}

function inspectLiveBookSnapshot(
  book: BookSnapshot,
  direction: StrategyDirection,
  side: "buy" | "sell",
) {
  return inspectBookSnapshot(
    book,
    direction,
    side,
    LIVE_MAX_BOOK_STALE_MS,
    LIVE_BOOK_WS_MAX_DIFF,
  );
}

async function fetchCheckedPaperBookSnapshot(
  tokenId: string,
  direction: StrategyDirection,
  side: "buy" | "sell",
): Promise<{
  book: BookSnapshot;
  ok: boolean;
  status: string;
  reason: string | null;
  expectedBid: number | null;
  expectedAsk: number | null;
  diffPct: number | null;
}> {
  const first = await fetchBookSnapshot(tokenId);
  const firstCheck = inspectPaperBookSnapshot(first, direction, side);
  if (firstCheck.ok || PAPER_BOOK_CONFIRM_DELAY_MS <= 0) {
    return { book: first, ...firstCheck };
  }

  await new Promise((r) => setTimeout(r, PAPER_BOOK_CONFIRM_DELAY_MS));
  const second = await fetchBookSnapshot(tokenId);
  const secondCheck = inspectPaperBookSnapshot(second, direction, side);
  if (secondCheck.ok) {
    return {
      book: second,
      ...secondCheck,
      status: "confirmed",
      reason: firstCheck.reason
        ? `confirmed_after_${firstCheck.reason}`
        : "confirmed_after_retry",
    };
  }

  console.warn(
    `[Paper] Reject suspicious book direction=${direction} side=${side} token=${tokenId.slice(0, 10)} reason=${secondCheck.reason || firstCheck.reason || "unknown"} diff=${secondCheck.diffPct == null ? "-" : secondCheck.diffPct.toFixed(2)}pct topBid=${second.topBid} topAsk=${second.topAsk}`,
  );
  return {
    book: second,
    ...secondCheck,
    reason: secondCheck.reason || firstCheck.reason,
  };
}

async function fetchCheckedLiveBookSnapshot(
  tokenId: string,
  direction: StrategyDirection,
  side: "buy" | "sell",
): Promise<{
  book: BookSnapshot;
  ok: boolean;
  status: string;
  reason: string | null;
  expectedBid: number | null;
  expectedAsk: number | null;
  diffPct: number | null;
}> {
  const first = await fetchBookSnapshot(tokenId);
  const firstCheck = inspectLiveBookSnapshot(first, direction, side);
  if (firstCheck.ok || LIVE_BOOK_CONFIRM_DELAY_MS <= 0) {
    return { book: first, ...firstCheck };
  }

  await new Promise((r) => setTimeout(r, LIVE_BOOK_CONFIRM_DELAY_MS));
  const second = await fetchBookSnapshot(tokenId);
  const secondCheck = inspectLiveBookSnapshot(second, direction, side);
  if (secondCheck.ok) {
    return {
      book: second,
      ...secondCheck,
      status: "confirmed",
      reason: firstCheck.reason
        ? `confirmed_after_${firstCheck.reason}`
        : "confirmed_after_retry",
    };
  }

  console.warn(
    `[LiveGuard] Reject suspicious book direction=${direction} side=${side} token=${tokenId.slice(0, 10)} reason=${secondCheck.reason || firstCheck.reason || "unknown"} diff=${secondCheck.diffPct == null ? "-" : secondCheck.diffPct.toFixed(2)}pct topBid=${second.topBid} topAsk=${second.topAsk}`,
  );
  return {
    book: second,
    ...secondCheck,
    reason: secondCheck.reason || firstCheck.reason,
  };
}

function getAuditBookLevels(levels: BookLevel[]): BookLevel[] {
  return levels.slice(0, PAPER_BOOK_AUDIT_LEVELS).map((level) => ({
    price: level.price,
    size: level.size,
  }));
}

function sumBookLiquidity(
  levels: BookLevel[],
  side: "buy" | "sell",
  worstPrice: number,
): number {
  return levels.reduce((sum, level) => {
    if (side === "buy" && level.price <= worstPrice)
      return sum + level.price * level.size;
    if (side === "sell" && level.price >= worstPrice) return sum + level.size;
    return sum;
  }, 0);
}

function buildRejectedPaperFill(input: {
  side: "buy" | "sell";
  amount: number;
  worstPrice: number;
  book: BookSnapshot;
  rejectReason: string;
}) {
  const levels = input.side === "buy" ? input.book.asks : input.book.bids;
  const topPrice = getBookSidePrice(input.book, input.side);
  return {
    success: false,
    rejectReason: input.rejectReason,
    requestedShares:
      input.side === "buy" && topPrice > 0
        ? input.amount / topPrice
        : input.side === "sell"
          ? input.amount
          : 0,
    filledShares: 0,
    filledNotional: 0,
    avgPrice: 0,
    levelsUsed: 0,
    availableLiquidity: sumBookLiquidity(levels, input.side, input.worstPrice),
    priceImpactPct: 0,
  };
}

function simulatePaperFill(input: {
  side: "buy" | "sell";
  amount: number;
  worstPrice: number;
  book: BookSnapshot;
}) {
  const levels = input.side === "buy" ? input.book.asks : input.book.bids;
  const topPrice = input.side === "buy" ? input.book.topAsk : input.book.topBid;
  const availableLiquidity = sumBookLiquidity(
    levels,
    input.side,
    input.worstPrice,
  );
  if (!(topPrice > 0)) {
    return {
      success: false,
      rejectReason: "empty_book",
      requestedShares: 0,
      filledShares: 0,
      filledNotional: 0,
      avgPrice: 0,
      levelsUsed: 0,
      availableLiquidity,
      priceImpactPct: 0,
    };
  }
  let filledShares = 0;
  let filledNotional = 0;
  let levelsUsed = 0;
  if (input.side === "buy") {
    if (availableLiquidity + 1e-9 < input.amount) {
      return {
        success: false,
        rejectReason: "insufficient_depth_fok",
        requestedShares: input.amount / topPrice,
        filledShares: 0,
        filledNotional: 0,
        avgPrice: 0,
        levelsUsed: 0,
        availableLiquidity,
        priceImpactPct: 0,
      };
    }
    let remainingNotional = input.amount;
    for (const level of levels) {
      if (level.price > input.worstPrice || remainingNotional <= 1e-9) break;
      const levelNotional = level.price * level.size;
      const takeNotional = Math.min(remainingNotional, levelNotional);
      filledShares += takeNotional / level.price;
      filledNotional += takeNotional;
      remainingNotional -= takeNotional;
      levelsUsed++;
    }
  } else {
    if (availableLiquidity + 1e-9 < input.amount) {
      return {
        success: false,
        rejectReason: "insufficient_depth_fok",
        requestedShares: input.amount,
        filledShares: 0,
        filledNotional: 0,
        avgPrice: 0,
        levelsUsed: 0,
        availableLiquidity,
        priceImpactPct: 0,
      };
    }
    let remainingShares = input.amount;
    for (const level of levels) {
      if (level.price < input.worstPrice || remainingShares <= 1e-9) break;
      const takeShares = Math.min(remainingShares, level.size);
      filledShares += takeShares;
      filledNotional += takeShares * level.price;
      remainingShares -= takeShares;
      levelsUsed++;
    }
  }
  const avgPrice = filledShares > 0 ? filledNotional / filledShares : 0;
  const priceImpactPct =
    topPrice > 0 && avgPrice > 0 ? Math.abs(avgPrice - topPrice) * 100 : 0;
  return {
    success: filledShares > 0,
    requestedShares:
      input.side === "buy" ? input.amount / topPrice : input.amount,
    filledShares,
    filledNotional,
    avgPrice,
    levelsUsed,
    availableLiquidity,
    priceImpactPct,
  };
}

function buildMakerStatusSnapshot(
  _ctx: import("./strategies/types.js").StrategyTickContext,
): MakerStatusSnapshot {
  const active = paperMakerOrders.filter(
    (order) => order.windowStart === state.windowStart,
  );
  const upActive = active.filter((order) => order.direction === "up");
  const downActive = active.filter((order) => order.direction === "down");
  const sumRemainingShares = (orders: PaperMakerOrder[]): number =>
    orders.reduce((sum, order) => sum + order.remainingShares, 0);
  const sumRemainingNotional = (orders: PaperMakerOrder[]): number =>
    orders.reduce((sum, order) => sum + order.remainingShares * order.price, 0);
  const upBid = active
    .filter((order) => order.direction === "up")
    .reduce((max, order) => Math.max(max, order.price * 100), 0);
  const downBid = active
    .filter((order) => order.direction === "down")
    .reduce((max, order) => Math.max(max, order.price * 100), 0);
  return {
    activeOrders: active.length,
    upOrders: upActive.length,
    downOrders: downActive.length,
    activeShares: sumRemainingShares(active),
    activeNotional: sumRemainingNotional(active),
    upActiveShares: sumRemainingShares(upActive),
    downActiveShares: sumRemainingShares(downActive),
    upActiveNotional: sumRemainingNotional(upActive),
    downActiveNotional: sumRemainingNotional(downActive),
    upBidPct: upBid > 0 ? upBid : null,
    downBidPct: downBid > 0 ? downBid : null,
    totalBidCostPct: upBid > 0 && downBid > 0 ? upBid + downBid : null,
    targetEdgePct: null,
    filledCount: paperMakerFilledCount,
    mergedCount: paperMakerMergedCount,
    lastFill: paperMakerLastFill,
    lastReason: paperMakerLastReason,
  };
}

function getPaperMakerWindowNotional(windowStart: number): number {
  const filled = paperTradeHistory.reduce((sum, trade) => {
    if (
      trade.windowStart === windowStart &&
      trade.side === "buy" &&
      /^strategy10maker/.test(String(trade.source || "")) &&
      String(trade.status || "").includes("FILLED")
    ) {
      return (
        sum + (Number(trade.filledNotional ?? trade.requestedAmount ?? 0) || 0)
      );
    }
    return sum;
  }, 0);
  const active = paperMakerOrders.reduce(
    (sum, order) =>
      order.windowStart === windowStart
        ? sum + order.remainingShares * order.price
        : sum,
    0,
  );
  return filled + active;
}

function recordPaperMakerFill(
  order: PaperMakerOrder,
  filledShares: number,
  trigger: string,
  queueFillRatio: number,
  quote: { bid: number; ask: number; ageMs: number } | null,
): void {
  const tokenId = getDirectionTokenId(order.direction);
  if (!tokenId || filledShares <= 0) return;
  const notional = roundMoney(filledShares * order.price);
  if (paperAccount.usdc + 1e-9 < notional) {
    paperMakerLastReason = "maker fill skipped: insufficient paper USDC";
    return;
  }
  const currentSize = paperAccount.localSize[tokenId] ?? 0;
  paperAccount.usdc = roundMoney(paperAccount.usdc - notional);
  paperAccount.localSize[tokenId] = currentSize + filledShares;
  paperAccount.lastTradeAt = Date.now();
  order.remainingShares = Math.max(0, order.remainingShares - filledShares);
  paperMakerFilledCount++;
  paperMakerLastFillAt[order.direction] = Date.now();
  paperMakerLastFill = {
    direction: order.direction,
    price: order.price,
    shares: filledShares,
    trigger,
    ts: Date.now(),
  };
  paperMakerLastReason = `maker filled ${order.direction} ${(order.price * 100).toFixed(1)}% ${trigger}`;
  const auditBook = getStateBookLevelsForDirection(order.direction);
  const latencySnapshot = getPaperLatencyModelSnapshot();
  recordPaperTradeHistory({
    ts: Date.now(),
    windowStart: order.windowStart,
    side: "buy",
    direction: order.direction,
    amount: filledShares,
    price: order.price,
    avgPrice: order.price,
    worstPrice: order.price,
    status: "SIM_MAKER_FILLED",
    source: "strategy10maker",
    exitReason: `${trigger}; ${order.reason}`,
    requestedAmount: notional,
    requestedShares: order.shares,
    filledShares,
    filledNotional: notional,
    topBid: quote?.bid ?? null,
    topAsk: quote?.ask ?? null,
    spread: quote ? quote.ask - quote.bid : null,
    levelsUsed: 1,
    availableLiquidity:
      auditBook.bids.find(
        (level) => Math.abs(level.price - order.price) < 0.0001,
      )?.size ?? null,
    priceImpactPct: 0,
    simLatencyMs: Math.max(0, order.activeAt - order.createdAt),
    simLatencyMode: "maker",
    simLatencyBookP80Ms: latencySnapshot.bookP80Ms,
    simLatencyRestP80Ms: latencySnapshot.restP80Ms,
    simLatencyWsP80Ms: latencySnapshot.wsP80Ms,
    simLatencyPressureMs: latencySnapshot.pressureMs,
    simLatencyJitterMs: 0,
    bookLatencyMs: quote?.ageMs ?? null,
    totalLatencyMs: Date.now() - order.createdAt,
    partial: order.remainingShares > 0.01,
    makerOrderId: order.id,
    makerLimitPrice: order.price,
    makerTrigger: trigger,
    makerQueueFillRatio: queueFillRatio,
    makerActiveMs: Date.now() - order.activeAt,
    bookTokenId: tokenId,
    bookWindowStart: state.windowStart,
    bookFetchedAt: state.bookUpdatedAt || Date.now(),
    bookBids: getAuditBookLevels(auditBook.bids),
    bookAsks: getAuditBookLevels(auditBook.asks),
    paperBookExpectedBid: quote?.bid ?? null,
    paperBookExpectedAsk: quote?.ask ?? null,
    paperBookDiffPct: null,
    paperBookCheckStatus: "maker_ws_touch",
    paperBookCheckReason: trigger,
  });
  persistPaperAccountState();
  broadcastState();
}

function makerOrderFillProbe(
  order: PaperMakerOrder,
  now: number,
): {
  fillShares: number;
  trigger: string;
  ratio: number;
  quote: { bid: number; ask: number; ageMs: number } | null;
} | null {
  const quote = getStateTopBookForDirection(order.direction);
  if (!quote || quote.ageMs > S10_MAKER_MAX_BOOK_AGE_MS) return null;
  if (now < order.activeAt || now - order.activeAt < S10_MAKER_MIN_ACTIVE_MS) {
    order.lastSeenBid = quote.bid;
    order.lastSeenAsk = quote.ask;
    return null;
  }

  const crossed = quote.ask > 0 && quote.ask <= order.price;
  const sweptThrough =
    order.lastSeenBid != null &&
    order.lastSeenBid >= order.price &&
    quote.bid < order.price - 0.0001;
  const touch = getLastBookTouchForMakerDirection(order.direction);
  const activeMs = Math.max(0, now - order.activeAt);
  const pinnedAtLimit =
    activeMs >= S10_MAKER_BOOK_TOUCH_MIN_ACTIVE_MS &&
    Math.abs(quote.bid - order.price) <= S10_MAKER_BOOK_TOUCH_PRICE_EPS;
  const touchedOwnBid =
    touch != null &&
    touch.touchedAt > order.lastTouchAt &&
    touch.touchedAt >= order.createdAt &&
    activeMs >= S10_MAKER_BOOK_TOUCH_MIN_ACTIVE_MS &&
    Math.abs(touch.price - order.price) <= S10_MAKER_BOOK_TOUCH_PRICE_EPS;
  if (touchedOwnBid || pinnedAtLimit || crossed || sweptThrough) {
    if (!order.touchStartedAt)
      order.touchStartedAt = touchedOwnBid && touch ? touch.touchedAt : now;
  } else if (quote.bid < order.price - S10_MAKER_BOOK_TOUCH_PRICE_EPS) {
    order.touchStartedAt = 0;
  }
  order.lastSeenBid = quote.bid;
  order.lastSeenAsk = quote.ask;
  if (!crossed && !sweptThrough && !touchedOwnBid && !pinnedAtLimit)
    return null;
  if (now - paperMakerLastFillAt[order.direction] < S10_MAKER_FILL_COOLDOWN_MS)
    return null;
  if (touchedOwnBid && touch) {
    order.lastTouchAt = touch.touchedAt;
    order.touchCount += 1;
  } else if (pinnedAtLimit) {
    order.touchCount += 1;
  }

  const orderNotional = order.remainingShares * order.price;
  const smallOrderBoost =
    orderNotional <= 6
      ? 0.48
      : orderNotional <= S10_MAKER_SMALL_TOUCH_NOTIONAL
        ? 0.34
        : orderNotional <= S10_MAKER_SMALL_TOUCH_NOTIONAL * 2
          ? 0.18
          : 0;
  const holdMs = order.touchStartedAt
    ? Math.max(0, now - order.touchStartedAt)
    : 0;
  const holdBoost =
    clampNumber(holdMs / S10_MAKER_TOUCH_HOLD_FULL_MS, 0, 1) * 0.35;
  const ageRatio = clampNumber(activeMs / 9000, 0.06, 0.45);
  const touchRepeatBoost = clampNumber((order.touchCount - 1) * 0.08, 0, 0.24);
  const triggerBoost = crossed
    ? 0.78
    : sweptThrough
      ? 0.62
      : touchedOwnBid
        ? 0.22
        : 0.14;
  const priceDepthBoost =
    crossed || sweptThrough ? Math.max(0, (order.price - quote.bid) * 2.2) : 0;
  const touchMaxRatio =
    orderNotional <= S10_MAKER_SMALL_TOUCH_NOTIONAL
      ? 1
      : S10_MAKER_BOOK_TOUCH_MAX_RATIO;
  const ratio =
    crossed || sweptThrough
      ? 1
      : clampNumber(
          triggerBoost +
            ageRatio +
            priceDepthBoost +
            smallOrderBoost +
            holdBoost +
            touchRepeatBoost,
          orderNotional <= S10_MAKER_SMALL_TOUCH_NOTIONAL ? 0.35 : 0.12,
          touchMaxRatio,
        );
  const rawFillShares = Math.min(
    order.remainingShares,
    Math.max(S10_MAKER_PARTIAL_MIN_SHARES, order.remainingShares * ratio),
  );
  const ownedPosition = getS10MakerOwnedPosition("paper", order.windowStart);
  const ownSize =
    order.direction === "up" ? ownedPosition.upSize : ownedPosition.downSize;
  const otherSize =
    order.direction === "up" ? ownedPosition.downSize : ownedPosition.upSize;
  const tailRoom = Math.max(
    0,
    otherSize + S10_MAKER_MAX_TAIL_AFTER_FILL_SHARES - ownSize,
  );
  const fillShares = Math.min(rawFillShares, tailRoom);
  if (fillShares < S10_MAKER_PARTIAL_MIN_SHARES) return null;
  const actualRatio =
    order.remainingShares > 0 ? fillShares / order.remainingShares : ratio;
  const trigger = crossed
    ? "crossed_ask"
    : sweptThrough
      ? "bid_swept"
      : pinnedAtLimit && !touchedOwnBid
        ? "book_hold"
        : "book_touch";
  return { fillShares, trigger, ratio: actualRatio, quote };
}

function cancelOverexposedPaperMakerOrders(
  ctx: import("./strategies/types.js").StrategyTickContext,
): void {
  const imbalance = ctx.position.upSize - ctx.position.downSize;
  const overDirection: StrategyDirection | null =
    imbalance >= S10_MAKER_SOFT_IMBALANCE_SHARES
      ? "up"
      : imbalance <= -S10_MAKER_SOFT_IMBALANCE_SHARES
        ? "down"
        : null;
  if (!overDirection) return;
  const before = paperMakerOrders.length;
  paperMakerOrders = paperMakerOrders.filter(
    (order) =>
      !(
        order.windowStart === state.windowStart &&
        order.direction === overDirection
      ),
  );
  const canceled = before - paperMakerOrders.length;
  if (canceled > 0) {
    paperMakerLastReason = `maker cancel ${canceled} ${overDirection} orders; inv=${imbalance.toFixed(1)}`;
  }
}

function cancelStalePaperMakerOrders(quotes: MakerQuoteSignal[]): void {
  const current = paperMakerOrders.filter(
    (order) => order.windowStart === state.windowStart,
  );
  if (!current.length) return;
  const desiredMaxPrice: Record<StrategyDirection, number | null> = {
    up: null,
    down: null,
  };
  for (const quote of quotes) {
    desiredMaxPrice[quote.direction] = Math.max(
      desiredMaxPrice[quote.direction] ?? 0,
      quote.price,
    );
  }
  const before = paperMakerOrders.length;
  paperMakerOrders = paperMakerOrders.filter((order) => {
    if (order.windowStart !== state.windowStart) return true;
    const allowedPrice = desiredMaxPrice[order.direction];
    if (allowedPrice == null) return false;
    return order.price <= allowedPrice + 0.0025;
  });
  const canceled = before - paperMakerOrders.length;
  if (canceled > 0) {
    const quoteText = quotes.length
      ? quotes
          .map((q) => `${q.direction}@${(q.price * 100).toFixed(1)}%`)
          .join(",")
      : "none";
    paperMakerLastReason = `maker cancel ${canceled} stale/risk orders; desired=${quoteText}`;
  }
}

function reconcilePaperMakerOrders(
  ctx: import("./strategies/types.js").StrategyTickContext,
): void {
  if (
    strategyConfig.executionMode !== "paper" ||
    !S10_MAKER_ENGINE_ENABLED ||
    !strategyConfig.enabled.s10
  ) {
    paperMakerOrders = [];
    return;
  }
  const s10 = getStrategy("s10");
  const makerCtx = withS10MakerOwnedPosition(ctx);
  const now = Date.now();
  paperMakerOrders = paperMakerOrders.filter(
    (order) =>
      order.windowStart === state.windowStart &&
      order.remainingShares > 0.01 &&
      order.expiresAt > now,
  );
  cancelOverexposedPaperMakerOrders(makerCtx);

  const quotes = s10?.getMakerQuotes?.(makerCtx) ?? [];
  const placeableQuotes: MakerQuoteSignal[] = [];
  for (const quote of quotes) {
    const top = getStateTopBookForDirection(quote.direction);
    if (!top || top.ageMs > S10_MAKER_MAX_BOOK_AGE_MS) {
      if (PAPER_S10_LIVE_PARITY_ENABLED)
        recordPaperMakerParityReject(quote, "stale ws book", top);
      continue;
    }
    const effectiveQuote = normalizePaperMakerQuoteForLiveParity(quote, top);
    if (effectiveQuote) placeableQuotes.push(effectiveQuote);
  }
  cancelStalePaperMakerOrders(placeableQuotes);
  for (const effectiveQuote of placeableQuotes) {
    const top = getStateTopBookForDirection(effectiveQuote.direction);
    if (
      !top ||
      top.ageMs > S10_MAKER_MAX_BOOK_AGE_MS ||
      effectiveQuote.price > top.bid + 0.0001
    )
      continue;
    const usedWindowNotional = getPaperMakerWindowNotional(state.windowStart);
    const quoteNotional = effectiveQuote.price * effectiveQuote.shares;
    const windowCap = getS10MakerWindowNotionalCap("paper");
    if (usedWindowNotional + quoteNotional > windowCap) {
      paperMakerLastReason = `maker window cap ${usedWindowNotional.toFixed(1)}/${windowCap.toFixed(1)}`;
      break;
    }
    const activeSameSide = paperMakerOrders
      .filter(
        (order) =>
          order.windowStart === state.windowStart &&
          order.direction === effectiveQuote.direction,
      )
      .sort((a, b) => b.createdAt - a.createdAt);
    if (activeSameSide.length >= S10_MAKER_MAX_ACTIVE_ORDERS_PER_SIDE) continue;
    const duplicate = activeSameSide.some(
      (order) =>
        Math.abs(order.price - effectiveQuote.price) <
        S10_MAKER_DUPLICATE_PRICE_EPS,
    );
    if (duplicate) continue;
    const latency = samplePaperLatency();
    const activeAt = now + latency.delayMs;
    paperMakerOrders.push({
      id: `pmk-${state.windowStart}-${effectiveQuote.direction}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`,
      strategy: 10,
      windowStart: state.windowStart,
      direction: effectiveQuote.direction,
      price: clampNumber(effectiveQuote.price, 0.01, 0.99),
      shares: effectiveQuote.shares,
      remainingShares: effectiveQuote.shares,
      createdAt: now,
      activeAt,
      expiresAt: activeAt + Math.max(800, effectiveQuote.ttlMs ?? 2500),
      reason: effectiveQuote.reason || "",
      lastSeenBid: top.bid,
      lastSeenAsk: top.ask,
      lastTouchAt: 0,
      touchStartedAt: 0,
      touchCount: 0,
    });
  }

  for (const order of [...paperMakerOrders]) {
    const probe = makerOrderFillProbe(order, now);
    if (!probe) continue;
    recordPaperMakerFill(
      order,
      probe.fillShares,
      probe.trigger,
      probe.ratio,
      probe.quote,
    );
  }
  paperMakerOrders = paperMakerOrders.filter(
    (order) => order.remainingShares > 0.01 && order.expiresAt > now,
  );

  if (PAPER_PRE_SETTLEMENT_MERGE_ENABLED && state.windowStart) {
    const merged = mergePaperFullSet(
      state.windowStart,
      "strategy10maker-merge",
      "maker dual inventory merge",
    );
    if (merged > 0) {
      paperMakerMergedCount++;
      paperMakerLastReason = `maker merged ${merged.toFixed(4)} shares`;
    }
  } else if (!PAPER_PRE_SETTLEMENT_MERGE_ENABLED && state.windowStart) {
    const upLocked = state.upTokenId
      ? (paperAccount.localSize[state.upTokenId] ?? 0)
      : 0;
    const downLocked = state.downTokenId
      ? (paperAccount.localSize[state.downTokenId] ?? 0)
      : 0;
    const lockedShares = Math.min(upLocked, downLocked);
    const imbalance = upLocked - downLocked;
    if (lockedShares > 0.01) {
      paperMakerLastReason = `locked full-set ${lockedShares.toFixed(4)} shares; merge disabled; inv=${imbalance.toFixed(1)}`;
    }
  }

  s10?.onMakerStatus?.(makerCtx, buildMakerStatusSnapshot(makerCtx));
}

function buildLiveMakerStatusSnapshot(
  lastReason = liveMakerLastReason,
): MakerStatusSnapshot {
  const active = liveMakerOrders.filter(
    (order) =>
      order.windowStart === state.windowStart &&
      (order.status === "open" || order.status === "unknown"),
  );
  const upActive = active.filter((order) => order.direction === "up");
  const downActive = active.filter((order) => order.direction === "down");
  const sumRemainingShares = (orders: LiveMakerOrder[]): number =>
    orders.reduce((sum, order) => sum + order.remainingShares, 0);
  const sumRemainingNotional = (orders: LiveMakerOrder[]): number =>
    orders.reduce((sum, order) => sum + order.remainingShares * order.price, 0);
  const upBid = active
    .filter((order) => order.direction === "up")
    .reduce((max, order) => Math.max(max, order.price * 100), 0);
  const downBid = active
    .filter((order) => order.direction === "down")
    .reduce((max, order) => Math.max(max, order.price * 100), 0);
  return {
    activeOrders: active.length,
    upOrders: upActive.length,
    downOrders: downActive.length,
    activeShares: sumRemainingShares(active),
    activeNotional: sumRemainingNotional(active),
    upActiveShares: sumRemainingShares(upActive),
    downActiveShares: sumRemainingShares(downActive),
    upActiveNotional: sumRemainingNotional(upActive),
    downActiveNotional: sumRemainingNotional(downActive),
    upBidPct: upBid > 0 ? upBid : null,
    downBidPct: downBid > 0 ? downBid : null,
    totalBidCostPct: upBid > 0 && downBid > 0 ? upBid + downBid : null,
    targetEdgePct: null,
    filledCount: liveMakerFilledCount,
    mergedCount: 0,
    lastFill: liveMakerLastFill,
    lastReason,
  };
}

function publishLiveMakerStatus(
  ctx: import("./strategies/types.js").StrategyTickContext,
  reason?: string,
): void {
  const s10 = getStrategy("s10");
  if (reason) liveMakerLastReason = reason;
  s10?.onMakerStatus?.(
    ctx,
    buildLiveMakerStatusSnapshot(reason ?? liveMakerLastReason),
  );
}

function recordLiveMakerExecutionEvent(
  event: ExecutionEventType,
  input: {
    quote?: MakerQuoteSignal;
    order?: LiveMakerOrder;
    tokenId?: string | null;
    orderId?: string | null;
    status?: string | null;
    reason?: string | null;
    top?: { bid: number; ask: number; ageMs: number } | null;
    checkedBook?: {
      book?: BookSnapshot;
      status?: string;
      reason?: string;
      diffPct?: number | null;
    } | null;
    filledShares?: number | null;
    filledNotional?: number | null;
    latencyMs?: number | null;
  },
): void {
  const direction = input.order?.direction ?? input.quote?.direction ?? null;
  const price = input.order?.price ?? input.quote?.price ?? null;
  const shares = input.order?.remainingShares ?? input.quote?.shares ?? null;
  const orderId = input.order?.orderId ?? input.orderId ?? null;
  const tokenId =
    input.order?.tokenId ??
    input.tokenId ??
    (direction ? getDirectionTokenId(direction) : null);
  const topBid = input.top?.bid ?? input.checkedBook?.book?.topBid ?? null;
  const topAsk = input.top?.ask ?? input.checkedBook?.book?.topAsk ?? null;
  recordExecutionEvent({
    ts: Date.now(),
    executionMode: "live",
    event,
    windowStart: input.order?.windowStart ?? state.windowStart,
    source: "strategy10maker",
    strategy: 10,
    side: "buy",
    direction,
    orderId,
    makerOrderId: input.order?.id ?? null,
    tokenId,
    status: input.status ?? null,
    reason: input.reason ?? input.order?.reason ?? input.quote?.reason ?? null,
    price: finiteOrNull(price),
    worstPrice: finiteOrNull(price),
    shares: finiteOrNull(shares),
    requestedShares: finiteOrNull(input.order?.shares ?? input.quote?.shares),
    filledShares: finiteOrNull(input.filledShares),
    remainingShares: finiteOrNull(input.order?.remainingShares),
    notional: finiteOrNull(
      price != null && shares != null ? price * shares : null,
    ),
    requestedNotional: finiteOrNull(
      price != null && (input.order?.shares ?? input.quote?.shares) != null
        ? price * (input.order?.shares ?? input.quote?.shares ?? 0)
        : null,
    ),
    filledNotional: finiteOrNull(input.filledNotional),
    topBid: finiteOrNull(topBid),
    topAsk: finiteOrNull(topAsk),
    spread: finiteOrNull(
      topBid != null && topAsk != null ? topAsk - topBid : null,
    ),
    bookAgeMs: finiteOrNull(input.top?.ageMs),
    bookSource: "ws/rest",
    bookCheckStatus: input.checkedBook?.status ?? null,
    bookCheckReason: input.checkedBook?.reason ?? null,
    bookCheckDiffPct: finiteOrNull(input.checkedBook?.diffPct),
    bookLatencyMs: finiteOrNull(input.checkedBook?.book?.latencyMs),
    latencyMs: finiteOrNull(input.latencyMs),
  });
}

function rejectLiveMakerQuote(
  quote: MakerQuoteSignal,
  reason: string,
  extra: {
    tokenId?: string | null;
    top?: { bid: number; ask: number; ageMs: number } | null;
    checkedBook?: {
      book?: BookSnapshot;
      status?: string;
      reason?: string;
      diffPct?: number | null;
    } | null;
  } = {},
): void {
  liveMakerLastReason = `live maker skip ${quote.direction}: ${reason}`;
  recordLiveMakerExecutionEvent("live_maker_rejected", {
    quote,
    tokenId: extra.tokenId ?? null,
    reason,
    status: "REJECTED",
    top: extra.top ?? null,
    checkedBook: extra.checkedBook ?? null,
  });
}

function getLiveMakerWindowNotional(windowStart: number): number {
  const filled = tradeHistory.reduce((sum, trade) => {
    if (
      trade.windowStart === windowStart &&
      trade.side === "buy" &&
      /^strategy10maker/.test(String(trade.source || "")) &&
      String(trade.status || "").includes("MINED")
    ) {
      return (
        sum +
        (Number(
          trade.filledNotional ??
            trade.requestedAmount ??
            (trade.amount || 0) * (trade.price || 0),
        ) || 0)
      );
    }
    return sum;
  }, 0);
  const active = liveMakerOrders.reduce(
    (sum, order) =>
      order.windowStart === windowStart &&
      (order.status === "open" || order.status === "unknown")
        ? sum + order.remainingShares * order.price
        : sum,
    0,
  );
  return filled + active;
}

function getS10ConfiguredOrderCap(): number {
  const configuredCap = Math.min(
    Number(strategyConfig.amount.s10) || LIVE_STRATEGY_MAX_ORDER_USDC,
    LIVE_STRATEGY_MAX_ORDER_USDC,
    LIVE_MAX_ORDER_USDC,
  );
  const minLiveOrderNotional = getS10MakerMinimumOrderNotionalFloor();
  return Math.min(
    Math.max(configuredCap, minLiveOrderNotional),
    LIVE_STRATEGY_MAX_ORDER_USDC,
    LIVE_MAX_ORDER_USDC,
  );
}

function getS10MakerMinimumOrderNotionalFloor(): number {
  const minShares = getCachedOrFallbackLiveMinimumOrderSize();
  if (minShares == null || !(minShares > 0)) return 0;
  return Math.ceil(minShares * 0.99 * 100) / 100;
}

function getS10MakerWindowNotionalCap(mode: ExecutionMode): number {
  if (
    mode !== "live" &&
    !(PAPER_S10_LIVE_PARITY_ENABLED && PAPER_S10_LIVE_PARITY_USE_LIVE_CAP)
  ) {
    return S10_MAKER_MAX_WINDOW_NOTIONAL;
  }
  const configuredOrderCap = getS10ConfiguredOrderCap();
  const liveUsdc =
    positions.usdc != null &&
    Number.isFinite(positions.usdc) &&
    positions.usdc > 0
      ? positions.usdc
      : null;
  const balance =
    mode === "live"
      ? liveUsdc
      : (liveUsdc ?? (paperAccount.usdc > 0 ? paperAccount.usdc : null));
  const fallbackCap = Math.max(
    configuredOrderCap,
    configuredOrderCap * S10_MAKER_MAX_ACTIVE_ORDERS_PER_SIDE,
  );
  const balanceCap =
    balance != null
      ? Math.max(
          configuredOrderCap,
          balance * S10_LIVE_MAKER_MAX_WINDOW_BALANCE_RATIO,
        )
      : fallbackCap;
  return Math.max(
    configuredOrderCap,
    Math.min(S10_MAKER_MAX_WINDOW_NOTIONAL, balanceCap),
  );
}

function getCachedOrFallbackLiveMinimumOrderSize(): number | null {
  const cachedRules =
    liveMarketRulesCache &&
    liveMarketRulesCache.conditionId === state.conditionId
      ? liveMarketRulesCache
      : null;
  return (
    cachedRules?.minimumOrderSize ??
    (LIVE_MIN_ORDER_SHARES_FALLBACK > 0 ? LIVE_MIN_ORDER_SHARES_FALLBACK : null)
  );
}

function getCachedOrFallbackLiveTickSize(): number {
  const cachedRules =
    liveMarketRulesCache &&
    liveMarketRulesCache.conditionId === state.conditionId
      ? liveMarketRulesCache
      : null;
  const tick = cachedRules?.minimumTickSize ?? PAPER_S10_LIVE_PARITY_TICK_SIZE;
  return Number.isFinite(tick) && tick > 0 ? tick : 0.01;
}

function recordPaperMakerParityReject(
  quote: MakerQuoteSignal,
  reason: string,
  top?: { bid: number; ask: number; ageMs: number } | null,
): void {
  paperMakerLastReason = `paper parity skip ${quote.direction}: ${reason}`;
  recordExecutionEvent({
    ts: Date.now(),
    executionMode: "paper",
    event: "paper_maker_rejected",
    windowStart: state.windowStart,
    source: "strategy10maker",
    strategy: 10,
    side: "buy",
    direction: quote.direction,
    status: "PAPER_PARITY_REJECTED",
    reason,
    price: finiteOrNull(quote.price),
    worstPrice: finiteOrNull(quote.price),
    shares: finiteOrNull(quote.shares),
    requestedShares: finiteOrNull(quote.shares),
    notional: finiteOrNull(quote.price * quote.shares),
    requestedNotional: finiteOrNull(quote.price * quote.shares),
    topBid: finiteOrNull(top?.bid),
    topAsk: finiteOrNull(top?.ask),
    spread: finiteOrNull(top ? top.ask - top.bid : null),
    bookAgeMs: finiteOrNull(top?.ageMs),
    bookSource: "ws",
    bookCheckStatus: "paper_live_parity",
    bookCheckReason: reason,
  });
}

function normalizePaperMakerQuoteForLiveParity(
  quote: MakerQuoteSignal,
  top: { bid: number; ask: number; ageMs: number },
): MakerQuoteSignal | null {
  if (!PAPER_S10_LIVE_PARITY_ENABLED) return quote;
  const tickSize = getCachedOrFallbackLiveTickSize();
  const priceDecimals = getDecimalPlaces(tickSize);
  const safePrice = getPostOnlySafeMakerPrice(
    quote.price,
    top.bid,
    top.ask,
    tickSize,
    priceDecimals,
  );
  if (!safePrice) {
    recordPaperMakerParityReject(quote, "post-only no safe price", top);
    return null;
  }
  const priceSafeQuote: MakerQuoteSignal = safePrice.adjusted
    ? {
        ...quote,
        price: safePrice.price,
        reason: appendMakerReason(
          quote.reason,
          `post_only_px=${safePrice.price.toFixed(priceDecimals)}`,
        ),
      }
    : quote;

  const normalizedPrice = floorToDecimals(
    clampNumber(priceSafeQuote.price, 0.01, 0.99),
    priceDecimals,
  );
  if (normalizedPrice <= 0 || normalizedPrice >= 1) {
    recordPaperMakerParityReject(priceSafeQuote, "normalized invalid", top);
    return null;
  }

  const maxConfiguredNotional = getS10ConfiguredOrderCap();
  const cappedShares = Math.min(
    priceSafeQuote.shares,
    Math.floor(
      (maxConfiguredNotional / Math.max(normalizedPrice, 0.01)) * 100,
    ) / 100,
  );
  let normalizedShares = floorToDecimals(cappedShares, 2);
  const minShares = getCachedOrFallbackLiveMinimumOrderSize();
  if (minShares != null && normalizedShares + 1e-9 < minShares) {
    if (
      isRiskReducingMakerQuote(priceSafeQuote) &&
      normalizedPrice * minShares <= maxConfiguredNotional + 1e-6
    ) {
      normalizedShares = minShares;
    } else {
      recordPaperMakerParityReject(
        priceSafeQuote,
        `live_min_order_size:${normalizedShares.toFixed(2)}<${minShares.toFixed(2)}`,
        top,
      );
      return null;
    }
  }
  if (normalizedPrice * normalizedShares > maxConfiguredNotional + 1e-6) {
    recordPaperMakerParityReject(
      priceSafeQuote,
      `live_order_notional_cap:${(normalizedPrice * normalizedShares).toFixed(2)}>${maxConfiguredNotional.toFixed(2)}`,
      top,
    );
    return null;
  }
  if (normalizedShares <= 0.01) {
    recordPaperMakerParityReject(
      priceSafeQuote,
      "live_order_notional_cap_too_small",
      top,
    );
    return null;
  }

  const reasonParts = [priceSafeQuote.reason || ""];
  if (Math.abs(normalizedPrice - quote.price) > 1e-9)
    reasonParts.push(
      `paper_live_tick=${normalizedPrice.toFixed(priceDecimals)}`,
    );
  if (Math.abs(normalizedShares - quote.shares) > 1e-9)
    reasonParts.push(`paper_live_cap=$${maxConfiguredNotional.toFixed(2)}`);
  if (
    minShares != null &&
    normalizedShares >= minShares &&
    quote.shares + 1e-9 < minShares
  ) {
    reasonParts.push(`paper_live_min=${minShares.toFixed(2)}`);
  }
  return {
    ...priceSafeQuote,
    price: normalizedPrice,
    shares: normalizedShares,
    reason: reasonParts.filter(Boolean).join(" "),
  };
}

function noteLiveMakerFill(
  orderId: string,
  side: "buy" | "sell",
  direction: StrategyDirection,
  size: number,
  price: number,
  ts: number,
): void {
  const order = liveMakerOrders.find(
    (candidate) => candidate.orderId === orderId,
  );
  if (!order || side !== "buy" || order.direction !== direction || !(size > 0))
    return;
  order.remainingShares = Math.max(0, order.remainingShares - size);
  order.lastSeenMatchedShares = Math.max(
    order.lastSeenMatchedShares,
    order.shares - order.remainingShares,
  );
  order.lastSyncAt = Date.now();
  if (Date.now() - liveMakerLastFillAt[direction] >= 0) {
    liveMakerFilledCount++;
  }
  liveMakerLastFillAt[direction] = Date.now();
  liveMakerLastFill = {
    direction,
    price: price > 0 ? price : order.price,
    shares: size,
    trigger: "user_ws_mined",
    ts,
  };
  liveMakerLastReason = `live maker filled ${direction} ${(order.price * 100).toFixed(1)}% user_ws`;
  if (order.remainingShares <= 0.01) {
    order.status = "filled";
    forgetPendingTradeMeta(order.orderId);
    liveMakerOrders = liveMakerOrders.filter(
      (candidate) => candidate.orderId !== order.orderId,
    );
  }
}

async function cancelLiveMakerOrder(
  order: LiveMakerOrder,
  reason: string,
): Promise<boolean> {
  if (
    order.status === "canceling" ||
    order.status === "canceled" ||
    order.status === "filled"
  )
    return true;
  order.status = "canceling";
  try {
    if (!(await ensureClobClient())) throw new Error("clob_client_unavailable");
    const result = await clobClient!.cancelOrder({ orderID: order.orderId });
    const error = extractOrderError(result);
    if (error) throw new Error(error);
    order.status = "canceled";
    liveMakerCanceledCount++;
    liveMakerLastReason = `live maker cancel ${order.direction} ${(order.price * 100).toFixed(1)}% ${reason}`;
    recordLiveMakerExecutionEvent("live_maker_canceled", {
      order,
      reason,
      status: "CANCELED",
    });
    return true;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    const lower = msg.toLowerCase();
    if (
      lower.includes("not found") ||
      lower.includes("already") ||
      lower.includes("closed") ||
      lower.includes("canceled")
    ) {
      order.status = "canceled";
      liveMakerLastReason = `live maker cancel assumed ${order.direction} ${reason}`;
      recordLiveMakerExecutionEvent("live_maker_canceled", {
        order,
        reason: `assumed:${reason}`,
        status: "CANCELED_ASSUMED",
      });
      return true;
    }
    order.status = "unknown";
    liveMakerLastReason = `live maker cancel failed ${order.direction}: ${msg}`;
    recordLiveMakerExecutionEvent("live_maker_cancel_failed", {
      order,
      reason: msg,
      status: "CANCEL_FAILED",
    });
    console.warn(
      `[S10LiveMaker] cancel failed order=${order.orderId} reason=${reason}: ${msg}`,
    );
    return false;
  }
}

async function cancelLiveMakerOrders(
  predicate: (order: LiveMakerOrder) => boolean,
  reason: string,
): Promise<number> {
  const targets = liveMakerOrders.filter(
    (order) =>
      (order.status === "open" || order.status === "unknown") &&
      predicate(order),
  );
  let canceled = 0;
  for (const order of targets) {
    if (await cancelLiveMakerOrder(order, reason)) canceled++;
  }
  liveMakerOrders = liveMakerOrders.filter(
    (order) =>
      order.status !== "canceled" &&
      order.status !== "filled" &&
      order.remainingShares > 0.01,
  );
  return canceled;
}

async function cancelOverexposedLiveMakerOrders(
  ctx: import("./strategies/types.js").StrategyTickContext,
): Promise<void> {
  const imbalance = ctx.position.upSize - ctx.position.downSize;
  const overDirection: StrategyDirection | null =
    imbalance >= S10_MAKER_SOFT_IMBALANCE_SHARES
      ? "up"
      : imbalance <= -S10_MAKER_SOFT_IMBALANCE_SHARES
        ? "down"
        : null;
  if (!overDirection) return;
  const canceled = await cancelLiveMakerOrders(
    (order) =>
      order.windowStart === state.windowStart &&
      order.direction === overDirection,
    `overexposed inv=${imbalance.toFixed(1)}`,
  );
  if (canceled > 0)
    liveMakerLastReason = `live maker cancel ${canceled} ${overDirection} orders; inv=${imbalance.toFixed(1)}`;
}

async function cancelStaleLiveMakerOrders(
  quotes: MakerQuoteSignal[],
): Promise<void> {
  const desiredMaxPrice: Record<StrategyDirection, number | null> = {
    up: null,
    down: null,
  };
  for (const quote of quotes) {
    desiredMaxPrice[quote.direction] = Math.max(
      desiredMaxPrice[quote.direction] ?? 0,
      quote.price,
    );
  }
  const canceled = await cancelLiveMakerOrders((order) => {
    if (order.windowStart !== state.windowStart) return true;
    if (order.expiresAt <= Date.now()) return true;
    const allowedPrice = desiredMaxPrice[order.direction];
    if (allowedPrice == null) return true;
    return order.price > allowedPrice + 0.0025;
  }, "stale/risk");
  if (canceled > 0) {
    const quoteText = quotes.length
      ? quotes
          .map((q) => `${q.direction}@${(q.price * 100).toFixed(1)}%`)
          .join(",")
      : "none";
    liveMakerLastReason = `live maker cancel ${canceled} stale/risk orders; desired=${quoteText}`;
  }
}

async function syncLiveMakerOrdersFromRest(): Promise<void> {
  if (Date.now() - liveMakerLastSyncAt < S10_LIVE_MAKER_ORDER_SYNC_MS) return;
  liveMakerLastSyncAt = Date.now();
  if (!liveMakerOrders.length || !(await ensureClobClient())) return;
  const tokenIds = [state.upTokenId, state.downTokenId].filter(Boolean);
  const openById = new Map<string, Record<string, unknown>>();
  for (const tokenId of tokenIds) {
    try {
      const orders = (await clobClient!.getOpenOrders(
        { asset_id: tokenId },
        true,
      )) as unknown;
      if (!Array.isArray(orders)) continue;
      for (const order of orders) {
        if (!isRecord(order) || typeof order.id !== "string") continue;
        openById.set(order.id, order);
      }
    } catch (err) {
      console.warn(
        `[S10LiveMaker] open order sync failed token=${tokenId.slice(0, 8)}: ${err instanceof Error ? err.message : String(err)}`,
      );
    }
  }

  for (const order of liveMakerOrders) {
    const open = openById.get(order.orderId);
    if (!open) {
      if (Date.now() - order.postedAt > 1500) order.status = "unknown";
      continue;
    }
    const originalSize = Number(open.original_size);
    const matchedSize = Number(open.size_matched);
    if (Number.isFinite(originalSize) && originalSize > 0)
      order.shares = originalSize;
    if (Number.isFinite(matchedSize) && matchedSize >= 0) {
      order.lastSeenMatchedShares = matchedSize;
      order.remainingShares = Math.max(0, order.shares - matchedSize);
    }
    if (typeof open.status === "string")
      order.status = open.status.toLowerCase().includes("open")
        ? "open"
        : "unknown";
    order.lastSyncAt = Date.now();
  }
  liveMakerOrders = liveMakerOrders.filter((order) => {
    if (order.status === "filled" || order.status === "canceled") return false;
    if (order.remainingShares <= 0.01) {
      forgetPendingTradeMeta(order.orderId);
      return false;
    }
    return true;
  });
}

async function placeLiveMakerQuote(quote: MakerQuoteSignal): Promise<void> {
  const tokenId = getDirectionTokenId(quote.direction);
  if (!tokenId) {
    rejectLiveMakerQuote(quote, "no token");
    return;
  }
  if (!(await ensureClobClient())) {
    rejectLiveMakerQuote(quote, "clob unavailable", { tokenId });
    return;
  }
  const top = getStateTopBookForDirection(quote.direction);
  const fallbackTickSize = getCachedOrFallbackLiveTickSize();
  const fallbackPriceDecimals = getDecimalPlaces(fallbackTickSize);
  let workingQuote = quote;
  if (top && top.ageMs <= S10_MAKER_MAX_BOOK_AGE_MS) {
    const wsSafePrice = getPostOnlySafeMakerPrice(
      workingQuote.price,
      top.bid,
      top.ask,
      fallbackTickSize,
      fallbackPriceDecimals,
    );
    if (!wsSafePrice) {
      rejectLiveMakerQuote(workingQuote, "post-only no ws safe price", {
        tokenId,
        top,
      });
      return;
    }
    if (wsSafePrice.adjusted) {
      workingQuote = {
        ...workingQuote,
        price: wsSafePrice.price,
        reason: appendMakerReason(
          workingQuote.reason,
          `ws_post_only_px=${wsSafePrice.price.toFixed(fallbackPriceDecimals)}`,
        ),
      };
    }
  }

  const checkedBook = await fetchCheckedLiveBookSnapshot(
    tokenId,
    quote.direction,
    "buy",
  );
  if (!checkedBook.ok) {
    rejectLiveMakerQuote(
      workingQuote,
      checkedBook.reason || "book_check_failed",
      { tokenId, top, checkedBook },
    );
    return;
  }
  const restSafePrice = getPostOnlySafeMakerPrice(
    workingQuote.price,
    checkedBook.book.topBid,
    checkedBook.book.topAsk,
    fallbackTickSize,
    fallbackPriceDecimals,
  );
  if (!restSafePrice) {
    rejectLiveMakerQuote(workingQuote, "post-only no rest safe price", {
      tokenId,
      top,
      checkedBook,
    });
    return;
  }
  if (restSafePrice.adjusted) {
    workingQuote = {
      ...workingQuote,
      price: restSafePrice.price,
      reason: appendMakerReason(
        workingQuote.reason,
        `rest_post_only_px=${restSafePrice.price.toFixed(fallbackPriceDecimals)}`,
      ),
    };
  }

  const maxConfiguredNotional = getS10ConfiguredOrderCap();
  const cappedShares = Math.min(
    workingQuote.shares,
    Math.floor(
      (maxConfiguredNotional / Math.max(workingQuote.price, 0.01)) * 10000,
    ) / 10000,
  );
  const effectiveQuote: MakerQuoteSignal = {
    ...workingQuote,
    shares: cappedShares,
    reason:
      workingQuote.shares !== cappedShares
        ? appendMakerReason(
            workingQuote.reason,
            `live_cap=$${maxConfiguredNotional.toFixed(2)}`,
          )
        : workingQuote.reason,
  };
  if (!(effectiveQuote.shares > 0.01)) {
    rejectLiveMakerQuote(
      effectiveQuote,
      `live_order_notional_cap_too_small:${maxConfiguredNotional.toFixed(2)}`,
      { tokenId, top, checkedBook },
    );
    return;
  }

  const notional = effectiveQuote.price * effectiveQuote.shares;
  const guard = getLiveOrderGuardReason(
    {
      direction: effectiveQuote.direction,
      side: "buy",
      amount: notional,
      slippage: 0,
      source: "strategy10maker",
    },
    checkedBook.book,
  );
  if (guard) {
    rejectLiveMakerQuote(effectiveQuote, guard, { tokenId, top, checkedBook });
    return;
  }

  try {
    const tickSize = await clobClient!.getTickSize(tokenId);
    const numericTickSize = Number(tickSize);
    const priceDecimals = getDecimalPlaces(tickSize);
    const finalSafePrice = getPostOnlySafeMakerPrice(
      effectiveQuote.price,
      checkedBook.book.topBid,
      checkedBook.book.topAsk,
      Number.isFinite(numericTickSize) && numericTickSize > 0
        ? numericTickSize
        : fallbackTickSize,
      priceDecimals,
    );
    if (!finalSafePrice) {
      rejectLiveMakerQuote(effectiveQuote, "post-only no final safe price", {
        tokenId,
        top,
        checkedBook,
      });
      return;
    }
    const normalizedPrice = floorToDecimals(
      finalSafePrice.price,
      priceDecimals,
    );
    let normalizedShares = floorToDecimals(effectiveQuote.shares, 2);
    if (normalizedPrice <= 0 || normalizedShares <= 0 || normalizedPrice >= 1) {
      rejectLiveMakerQuote(effectiveQuote, "normalized invalid", {
        tokenId,
        top,
        checkedBook,
      });
      return;
    }
    const marketRules = await fetchLiveMarketRules();
    const minSizeReason = getLiveMinOrderSizeReason(
      normalizedShares,
      marketRules,
    );
    if (minSizeReason) {
      const minShares = marketRules.minimumOrderSize;
      if (
        minShares != null &&
        isRiskReducingMakerQuote(effectiveQuote) &&
        normalizedPrice * minShares <= maxConfiguredNotional + 1e-6
      ) {
        normalizedShares = minShares;
      } else {
        rejectLiveMakerQuote(effectiveQuote, minSizeReason, {
          tokenId,
          top,
          checkedBook,
        });
        return;
      }
    }
    if (normalizedPrice * normalizedShares > maxConfiguredNotional + 1e-6) {
      rejectLiveMakerQuote(
        effectiveQuote,
        `live_order_notional_cap:${(normalizedPrice * normalizedShares).toFixed(2)}>${maxConfiguredNotional.toFixed(2)}`,
        { tokenId, top, checkedBook },
      );
      return;
    }
    const postedReason = finalSafePrice.adjusted
      ? appendMakerReason(
          effectiveQuote.reason,
          `final_post_only_px=${normalizedPrice.toFixed(priceDecimals)}`,
        )
      : effectiveQuote.reason;
    const postedQuote: MakerQuoteSignal = {
      ...effectiveQuote,
      price: normalizedPrice,
      shares: normalizedShares,
      reason: postedReason,
    };
    const localTtlMs = Math.max(800, effectiveQuote.ttlMs ?? 2500);
    const expiration = Math.ceil(
      (getPolymarketNowMs() + Math.max(75_000, localTtlMs + 75_000)) / 1000,
    );
    const result = await clobClient!.createAndPostOrder(
      {
        tokenID: tokenId,
        price: normalizedPrice,
        side: Side.BUY,
        size: normalizedShares,
        expiration,
      },
      { tickSize, negRisk: false },
      OrderType.GTD,
      true,
    );
    const error = extractOrderError(result);
    if (error) throw new Error(error);
    const orderId = extractOrderId(result);
    if (!orderId) throw new Error("post_order_missing_order_id");
    const now = Date.now();
    const order: LiveMakerOrder = {
      id: `lmk-${state.windowStart}-${effectiveQuote.direction}-${now}-${Math.random().toString(36).slice(2, 7)}`,
      orderId,
      strategy: 10,
      windowStart: state.windowStart,
      tokenId,
      direction: effectiveQuote.direction,
      price: normalizedPrice,
      shares: normalizedShares,
      remainingShares: normalizedShares,
      createdAt: now,
      postedAt: now,
      expiresAt: now + localTtlMs,
      reason: postedReason || "",
      status: "open",
      lastSeenMatchedShares: 0,
      lastSyncAt: 0,
    };
    liveMakerOrders.push(order);
    rememberPendingTradeMeta({
      orderId,
      ts: now,
      windowStart: state.windowStart,
      side: "buy",
      direction: effectiveQuote.direction,
      amount: normalizedShares,
      worstPrice: normalizedPrice,
      source: "strategy10maker",
      exitReason: postedReason || "",
    });
    liveMakerLastReason = `live maker posted ${effectiveQuote.direction} ${(normalizedPrice * 100).toFixed(1)}%/${normalizedShares.toFixed(2)}`;
    recordLiveMakerExecutionEvent("live_maker_posted", {
      order,
      quote: postedQuote,
      tokenId,
      orderId,
      status: "POSTED",
      reason: postedReason || "",
      top,
      checkedBook,
    });
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    liveMakerLastReason = `live maker post failed ${quote.direction}: ${msg}`;
    recordLiveMakerExecutionEvent("live_maker_rejected", {
      quote: effectiveQuote,
      tokenId,
      status: "POST_FAILED",
      reason: msg,
      top,
      checkedBook,
    });
    console.warn(
      `[S10LiveMaker] post failed ${effectiveQuote.direction} ${(effectiveQuote.price * 100).toFixed(2)}%: ${msg}`,
    );
  }
}

async function reconcileLiveMakerOrders(
  ctx: import("./strategies/types.js").StrategyTickContext,
): Promise<void> {
  if (liveMakerReconciling) return;
  if (
    strategyConfig.executionMode !== "live" ||
    !S10_MAKER_ENGINE_ENABLED ||
    !strategyConfig.enabled.s10 ||
    !S10_MAKER_ONLY
  ) {
    return;
  }
  liveMakerReconciling = true;
  try {
    const s10 = getStrategy("s10");
    const makerCtx = withS10MakerOwnedPosition(ctx);
    const quotes = s10?.getMakerQuotes?.(makerCtx) ?? [];
    const quoteText = quotes.length
      ? quotes
          .map(
            (quote) =>
              `${quote.direction}@${(quote.price * 100).toFixed(1)}%/${quote.shares.toFixed(1)}`,
          )
          .join(",")
      : "none";

    if (!liveTradingEnabled || !s10LiveMakerEnabled) {
      await cancelLiveMakerOrders(
        (order) => order.windowStart === state.windowStart,
        "live disabled",
      );
      publishLiveMakerStatus(
        ctx,
        `${!liveTradingEnabled ? "live trading disabled" : "s10 live maker disabled"}; decision=${quoteText}`,
      );
      return;
    }

    await syncLiveMakerOrdersFromRest();
    await cancelLiveMakerOrders(
      (order) =>
        order.windowStart !== state.windowStart ||
        order.expiresAt <= Date.now(),
      "expired/window",
    );
    await cancelOverexposedLiveMakerOrders(makerCtx);
    await cancelStaleLiveMakerOrders(quotes);

    for (const quote of quotes) {
      const usedWindowNotional = getLiveMakerWindowNotional(state.windowStart);
      const quoteNotional = quote.price * quote.shares;
      const windowCap = getS10MakerWindowNotionalCap("live");
      if (usedWindowNotional + quoteNotional > windowCap) {
        liveMakerLastReason = `live maker window cap ${usedWindowNotional.toFixed(1)}/${windowCap.toFixed(1)}`;
        break;
      }
      const activeSameSide = liveMakerOrders
        .filter(
          (order) =>
            order.windowStart === state.windowStart &&
            order.direction === quote.direction &&
            (order.status === "open" || order.status === "unknown"),
        )
        .sort((a, b) => b.createdAt - a.createdAt);
      if (activeSameSide.length >= S10_MAKER_MAX_ACTIVE_ORDERS_PER_SIDE)
        continue;
      const duplicate = activeSameSide.some(
        (order) =>
          Math.abs(order.price - quote.price) < S10_MAKER_DUPLICATE_PRICE_EPS,
      );
      if (duplicate) continue;
      await placeLiveMakerQuote(quote);
    }

    liveMakerOrders = liveMakerOrders.filter(
      (order) =>
        order.remainingShares > 0.01 &&
        order.status !== "canceled" &&
        order.status !== "filled",
    );
    publishLiveMakerStatus(
      makerCtx,
      liveMakerLastReason || `live maker decision=${quoteText}`,
    );
  } finally {
    liveMakerReconciling = false;
  }
}

function publishS10LiveMakerStatus(
  ctx: import("./strategies/types.js").StrategyTickContext,
): void {
  if (
    strategyConfig.executionMode !== "live" ||
    !S10_MAKER_ENGINE_ENABLED ||
    !strategyConfig.enabled.s10 ||
    !S10_MAKER_ONLY
  ) {
    return;
  }
  const s10 = getStrategy("s10");
  const makerCtx = withS10MakerOwnedPosition(ctx);
  const quotes = s10?.getMakerQuotes?.(makerCtx) ?? [];
  const upBid = quotes
    .filter((quote) => quote.direction === "up")
    .reduce((max, quote) => Math.max(max, quote.price * 100), 0);
  const downBid = quotes
    .filter((quote) => quote.direction === "down")
    .reduce((max, quote) => Math.max(max, quote.price * 100), 0);
  const quoteText = quotes.length
    ? quotes
        .map(
          (quote) =>
            `${quote.direction}@${(quote.price * 100).toFixed(1)}%/${quote.shares.toFixed(1)}`,
        )
        .join(",")
    : "none";
  const reason = s10LiveMakerEnabled
    ? `live maker engine active; decision=${quoteText}`
    : `live maker disabled; decision=${quoteText}`;
  s10?.onMakerStatus?.(makerCtx, {
    ...buildLiveMakerStatusSnapshot(reason),
    upBidPct: upBid > 0 ? upBid : null,
    downBidPct: downBid > 0 ? downBid : null,
    totalBidCostPct: upBid > 0 && downBid > 0 ? upBid + downBid : null,
  });
  void reconcileLiveMakerOrders(ctx);
}

async function placePaperOrder(
  input: PlaceOrderInput,
): Promise<OrderExecutionResult> {
  const { direction, side, amount, source = "manual" } = input;
  const startedAt = Date.now();
  const slippageVal =
    typeof input.slippage === "number" && input.slippage >= 0
      ? input.slippage
      : strategyConfig.slippage;
  if (!direction || !side || !amount || amount <= 0)
    return {
      success: false,
      statusCode: 400,
      body: { error: "参数错误", executionMode: "paper" },
      errorMessage: "参数错误",
    };
  if (isOrderWindowStale())
    return {
      success: false,
      statusCode: 409,
      body: { error: "当前市场窗口已过期", executionMode: "paper" },
      errorMessage: "当前市场窗口已过期",
    };
  if (!isProbabilityReady())
    return {
      success: false,
      statusCode: 409,
      body: { error: "盘口概率暂不可用", executionMode: "paper" },
      errorMessage: "盘口概率暂不可用",
    };
  const tokenId = getDirectionTokenId(direction);
  if (!tokenId)
    return {
      success: false,
      statusCode: 400,
      body: { error: "当前窗口市场未就绪", executionMode: "paper" },
      errorMessage: "当前窗口市场未就绪",
    };
  if (side === "buy" && paperAccount.usdc + 1e-9 < amount)
    return {
      success: false,
      statusCode: 409,
      body: { error: "模拟盘余额不足", executionMode: "paper" },
      errorMessage: "模拟盘余额不足",
    };
  if (side === "sell" && getPaperDirectionSize(direction) + 1e-9 < amount)
    return {
      success: false,
      statusCode: 409,
      body: { error: "模拟盘仓位不足", executionMode: "paper" },
      errorMessage: "模拟盘仓位不足",
    };

  const latencyEstimate = samplePaperLatency();
  const simLatencyMs = latencyEstimate.delayMs;
  if (simLatencyMs > 0) await new Promise((r) => setTimeout(r, simLatencyMs));
  const checkedBook = await fetchCheckedPaperBookSnapshot(
    tokenId,
    direction,
    side,
  );
  const book = checkedBook.book;
  const signalMaxPrice =
    typeof input.maxPrice === "number" && input.maxPrice > 0
      ? input.maxPrice
      : null;
  const worstPrice =
    side === "buy"
      ? Math.min(signalMaxPrice ?? book.topAsk + slippageVal, 0.99)
      : Math.max(book.topBid - slippageVal, 0.01);
  const fill = checkedBook.ok
    ? simulatePaperFill({ side, amount, worstPrice, book })
    : buildRejectedPaperFill({
        side,
        amount,
        worstPrice,
        book,
        rejectReason: checkedBook.reason || "book_check_failed",
      });
  const totalLatencyMs = Date.now() - startedAt;
  const spread =
    book.topBid > 0 && book.topAsk > 0 ? book.topAsk - book.topBid : null;
  const status = fill.success ? "SIM_FILLED" : "SIM_REJECTED";
  const tradeTs = Date.now();
  const recordBase = {
    ts: tradeTs,
    windowStart: state.windowStart,
    side,
    direction,
    amount: fill.success ? fill.filledShares : 0,
    price: fill.success ? fill.avgPrice : null,
    avgPrice: fill.success ? fill.avgPrice : null,
    worstPrice,
    status,
    source,
    exitReason: input.exitReason ?? fill.rejectReason,
    roundEntry: input.roundEntry,
    requestedAmount: side === "buy" ? amount : null,
    requestedShares: fill.requestedShares,
    filledShares: fill.filledShares,
    filledNotional: fill.filledNotional,
    topBid: book.topBid || null,
    topAsk: book.topAsk || null,
    spread,
    levelsUsed: fill.levelsUsed,
    availableLiquidity: fill.availableLiquidity,
    priceImpactPct: fill.priceImpactPct,
    simLatencyMs,
    simLatencyMode: latencyEstimate.mode,
    simLatencyBookP80Ms: latencyEstimate.bookP80Ms,
    simLatencyRestP80Ms: latencyEstimate.restP80Ms,
    simLatencyWsP80Ms: latencyEstimate.wsP80Ms,
    simLatencyPressureMs: latencyEstimate.pressureMs,
    simLatencyJitterMs: latencyEstimate.jitterMs,
    bookLatencyMs: book.latencyMs,
    totalLatencyMs,
    partial: false,
    rejectReason: fill.rejectReason,
    bookTokenId: tokenId,
    bookWindowStart: state.windowStart,
    bookFetchedAt: book.fetchedAt,
    bookBids: getAuditBookLevels(book.bids),
    bookAsks: getAuditBookLevels(book.asks),
    paperBookExpectedBid: checkedBook.expectedBid,
    paperBookExpectedAsk: checkedBook.expectedAsk,
    paperBookDiffPct: checkedBook.diffPct,
    paperBookCheckStatus: checkedBook.status,
    paperBookCheckReason: checkedBook.reason,
  } satisfies Omit<TradeHistoryItem, "id">;
  if (!fill.success) {
    recordPaperTradeHistory(recordBase);
    const paperRejectBody = {
      error: fill.rejectReason || "paper FOK not filled",
      executionMode: "paper",
      result: {
        status,
        bookCheckStatus: checkedBook.status,
        bookCheckReason: checkedBook.reason,
        bookDiffPct: checkedBook.diffPct,
      },
      bestBid: book.topBid,
      bestAsk: book.topAsk,
      worstPrice,
      fill,
    };
    return {
      success: false,
      statusCode: 409,
      body: paperRejectBody,
      errorMessage: fill.rejectReason || "paper FOK not filled",
    };
  }
  const currentSize = paperAccount.localSize[tokenId] ?? 0;
  if (side === "buy") {
    paperAccount.usdc = roundMoney(paperAccount.usdc - fill.filledNotional);
    paperAccount.localSize[tokenId] = currentSize + fill.filledShares;
  } else {
    paperAccount.usdc = roundMoney(paperAccount.usdc + fill.filledNotional);
    paperAccount.localSize[tokenId] = Math.max(
      0,
      currentSize - fill.filledShares,
    );
  }
  paperAccount.lastTradeAt = tradeTs;
  persistPaperAccountState();
  recordPaperTradeHistory(recordBase);
  broadcastState();
  return {
    success: true,
    statusCode: 200,
    body: {
      success: true,
      executionMode: "paper",
      result: {
        status,
        filledShares: fill.filledShares,
        filledNotional: fill.filledNotional,
        avgPrice: fill.avgPrice,
        levelsUsed: fill.levelsUsed,
        simLatencyMs,
        simLatencyMode: latencyEstimate.mode,
        simLatencyBookP80Ms: latencyEstimate.bookP80Ms,
        simLatencyRestP80Ms: latencyEstimate.restP80Ms,
        simLatencyWsP80Ms: latencyEstimate.wsP80Ms,
        bookLatencyMs: book.latencyMs,
        totalLatencyMs,
        bookCheckStatus: checkedBook.status,
        bookCheckReason: checkedBook.reason,
        bookDiffPct: checkedBook.diffPct,
      },
      bestBid: book.topBid,
      bestAsk: book.topAsk,
      worstPrice,
    },
  };
}

async function executeOrder(
  input: PlaceOrderInput,
): Promise<OrderExecutionResult> {
  const dislocationReason = getTerminalBookDislocationReason(
    input.direction,
    input.side,
  );
  if (dislocationReason) {
    return {
      success: false,
      statusCode: 409,
      body: {
        error: dislocationReason,
        executionMode: strategyConfig.executionMode,
      },
      errorMessage: dislocationReason,
    };
  }
  const liveGuardReason = getLiveOrderGuardReason(input);
  if (liveGuardReason) {
    return {
      success: false,
      statusCode: 409,
      body: {
        error: liveGuardReason,
        executionMode: strategyConfig.executionMode,
      },
      errorMessage: liveGuardReason,
    };
  }
  return strategyConfig.executionMode === "paper"
    ? placePaperOrder(input)
    : placeOrder(input);
}

function isRetryableOrderFailure(result: OrderExecutionResult): boolean {
  const msg = String(
    result.errorMessage ||
      (isRecord(result.body) ? result.body.error : "") ||
      "",
  ).toLowerCase();
  if (!msg) return false;
  if (
    msg.includes("insufficient_depth") ||
    msg.includes("terminal_book_dislocation") ||
    msg.includes("live_trading_disabled") ||
    msg.includes("s10_live_maker_disabled") ||
    msg.includes("live_order_notional_cap") ||
    msg.includes("live_min_order_size") ||
    msg.includes("live_price_impact") ||
    msg.includes("live_too_late") ||
    msg.includes("余额不足") ||
    msg.includes("insufficient")
  ) {
    return false;
  }
  return (
    result.statusCode >= 500 ||
    msg.includes("timeout") ||
    msg.includes("network") ||
    msg.includes("fetch") ||
    msg.includes("book_stale") ||
    msg.includes("missing_ws_book") ||
    msg.includes("book_ws_mismatch") ||
    msg.includes("429")
  );
}

async function executeStrategyOrder(
  input: PlaceOrderInput,
): Promise<OrderExecutionResult> {
  const attempts =
    strategyConfig.executionMode === "live"
      ? LIVE_STRATEGY_ORDER_RETRIES + 1
      : 1;
  let last: OrderExecutionResult | null = null;
  for (let attempt = 1; attempt <= attempts; attempt++) {
    const result = await executeOrder(input);
    if (
      result.success ||
      attempt >= attempts ||
      !isRetryableOrderFailure(result)
    )
      return result;
    last = result;
    console.warn(
      `[OrderRetry:${input.source || "strategy"}] attempt ${attempt}/${attempts} failed: ${result.errorMessage || "unknown"}, retrying`,
    );
    if (LIVE_STRATEGY_RETRY_DELAY_MS > 0) {
      await new Promise((r) => setTimeout(r, LIVE_STRATEGY_RETRY_DELAY_MS));
    }
  }
  return last ?? executeOrder(input);
}

async function placeOrder(
  input: PlaceOrderInput,
): Promise<OrderExecutionResult> {
  const { direction, side, amount, source = "manual" } = input;
  const slippageVal =
    typeof input.slippage === "number" && input.slippage >= 0
      ? input.slippage
      : strategyConfig.slippage;
  const orderTag = `[Order:${source}]`;

  if (!direction || !side || !amount || amount <= 0) {
    return {
      success: false,
      statusCode: 400,
      body: { error: "参数错误" },
      errorMessage: "参数错误",
    };
  }
  if (!(await ensureClobClient())) {
    return {
      success: false,
      statusCode: 500,
      body: { error: "CLOB 客户端未初始化，请检查 POLYMARKET_PRIVATE_KEY" },
      errorMessage: "CLOB 客户端未初始化，请检查 POLYMARKET_PRIVATE_KEY",
    };
  }
  if (isOrderWindowStale()) {
    return {
      success: false,
      statusCode: 409,
      body: { error: "当前市场窗口已过期，等待切换到新窗口" },
      errorMessage: "当前市场窗口已过期，等待切换到新窗口",
    };
  }
  if (!isProbabilityReady()) {
    return {
      success: false,
      statusCode: 409,
      body: { error: "盘口概率暂不可用，等待WS恢复" },
      errorMessage: "盘口概率暂不可用，等待WS恢复",
    };
  }

  const tokenId = direction === "up" ? state.upTokenId : state.downTokenId;
  if (!tokenId) {
    return {
      success: false,
      statusCode: 400,
      body: { error: "当前窗口市场未就绪" },
      errorMessage: "当前窗口市场未就绪",
    };
  }

  let checkedBook: Awaited<ReturnType<typeof fetchCheckedLiveBookSnapshot>>;
  try {
    checkedBook = await fetchCheckedLiveBookSnapshot(tokenId, direction, side);
  } catch {
    return {
      success: false,
      statusCode: 500,
      body: { error: "无法获取盘口价格" },
      errorMessage: "无法获取盘口价格",
    };
  }
  const book = checkedBook.book;
  const bestBid = book.topBid;
  const bestAsk = book.topAsk;
  if (!checkedBook.ok) {
    return {
      success: false,
      statusCode: 409,
      body: {
        error: checkedBook.reason || "live_book_check_failed",
        bestBid,
        bestAsk,
        bookCheckStatus: checkedBook.status,
        bookCheckReason: checkedBook.reason,
        bookDiffPct: checkedBook.diffPct,
      },
      errorMessage: checkedBook.reason || "live_book_check_failed",
    };
  }

  const signalMaxPrice =
    typeof input.maxPrice === "number" && input.maxPrice > 0
      ? input.maxPrice
      : null;
  const worstPrice =
    side === "buy"
      ? Math.min(signalMaxPrice ?? bestAsk + slippageVal, 0.99)
      : Math.max(bestBid - slippageVal, 0.01);
  const liveGuardAfterBook = getLiveOrderGuardReason(input, book);
  if (liveGuardAfterBook) {
    return {
      success: false,
      statusCode: 409,
      body: { error: liveGuardAfterBook, bestBid, bestAsk, worstPrice },
      errorMessage: liveGuardAfterBook,
    };
  }
  const depthCheck = simulatePaperFill({ side, amount, worstPrice, book });
  if (!depthCheck.success) {
    return {
      success: false,
      statusCode: 409,
      body: {
        error: depthCheck.rejectReason || "live_depth_rejected",
        bestBid,
        bestAsk,
        worstPrice,
        availableLiquidity: depthCheck.availableLiquidity,
        requestedShares: depthCheck.requestedShares,
        bookCheckStatus: checkedBook.status,
      },
      errorMessage: depthCheck.rejectReason || "live_depth_rejected",
    };
  }
  const marketRules = await fetchLiveMarketRules();
  const estimatedSharesForMinSize = estimateLiveOrderSharesForMinSize(
    side,
    amount,
    worstPrice,
    depthCheck,
  );
  const minSizeReason = getLiveMinOrderSizeReason(
    estimatedSharesForMinSize,
    marketRules,
  );
  if (minSizeReason) {
    return {
      success: false,
      statusCode: 409,
      body: {
        error: minSizeReason,
        bestBid,
        bestAsk,
        worstPrice,
        requestedShares: estimatedSharesForMinSize,
        minimumOrderSize: marketRules.minimumOrderSize,
        marketRulesSource: marketRules.source,
        fillPreview: depthCheck,
      },
      errorMessage: minSizeReason,
    };
  }
  if (depthCheck.priceImpactPct > LIVE_MAX_PRICE_IMPACT_PCT) {
    const reason = `live_price_impact:${depthCheck.priceImpactPct.toFixed(3)}>${LIVE_MAX_PRICE_IMPACT_PCT.toFixed(3)}`;
    return {
      success: false,
      statusCode: 409,
      body: {
        error: reason,
        bestBid,
        bestAsk,
        worstPrice,
        fillPreview: depthCheck,
      },
      errorMessage: reason,
    };
  }

  try {
    const tickSize = await clobClient!.getTickSize(tokenId);
    const priceDecimals = getDecimalPlaces(tickSize);
    const normalizedAmount = floorToDecimals(amount, 2);
    const normalizedWorstPrice = floorToDecimals(worstPrice, priceDecimals);
    const orderDebug = `tickSize:${tickSize} amount:${amount}->${normalizedAmount} worstPrice:${worstPrice}->${normalizedWorstPrice}`;
    if (normalizedAmount <= 0 || normalizedWorstPrice <= 0) {
      console.warn(`${orderTag} 参数精度处理后无效 ${orderDebug}`);
      return {
        success: false,
        statusCode: 400,
        body: {
          error: "下单参数精度处理后无效",
          bestBid,
          bestAsk,
          worstPrice: normalizedWorstPrice,
        },
        errorMessage: "下单参数精度处理后无效",
      };
    }

    const signedOrder = await clobClient!.createMarketOrder(
      {
        tokenID: tokenId,
        side: side === "buy" ? Side.BUY : Side.SELL,
        amount: normalizedAmount,
        price: normalizedWorstPrice,
      },
      { tickSize, negRisk: false },
    );
    const result = await clobClient!.postOrder(signedOrder, OrderType.FOK);
    const sideZh = side === "buy" ? "买入" : "卖出";
    const dirZh = direction === "up" ? "涨" : "跌";
    const rawStatus = result?.status ?? "未知";
    const orderError = extractOrderError(result);

    if (result?.status === 400 || orderError) {
      console.warn(
        `${orderTag} ${sideZh}${dirZh} ${normalizedAmount} 状态:${rawStatus} 原因:${orderError || "-"} ${orderDebug}`,
      );
      return {
        success: false,
        statusCode: 400,
        body: {
          error: orderError || `下单被拒绝 status=${rawStatus}`,
          result,
          bestBid,
          bestAsk,
          worstPrice: normalizedWorstPrice,
        },
        errorMessage: orderError || `下单被拒绝 status=${rawStatus}`,
      };
    }

    const statusZh = rawStatus === "matched" ? "成功" : rawStatus;
    console.log(
      `${orderTag} ${sideZh}${dirZh} ${normalizedAmount} 状态:${statusZh} 成交:${fmtOrderField(result?.takingAmount)} 花费:${fmtOrderField(result?.makingAmount)} ${orderDebug}`,
    );
    rememberPendingTradeMeta({
      orderId:
        typeof result?.orderID === "string" && result.orderID
          ? result.orderID
          : undefined,
      ts: Date.now(),
      windowStart: state.windowStart,
      side,
      direction,
      amount: normalizedAmount,
      worstPrice: normalizedWorstPrice,
      source,
      exitReason: input.exitReason,
      roundEntry: input.roundEntry,
    });
    if (!(typeof result?.orderID === "string" && result.orderID)) {
      console.warn(
        `${orderTag} 下单回包缺少 orderID，MINED 事件将退化为按方向/数量匹配`,
      );
    }
    broadcastState();
    return {
      success: true,
      statusCode: 200,
      body: {
        success: true,
        executionMode: "live",
        result,
        bestBid,
        bestAsk,
        worstPrice: normalizedWorstPrice,
        liveBookCheckStatus: checkedBook.status,
        liveBookCheckReason: checkedBook.reason,
        liveBookDiffPct: checkedBook.diffPct,
        fillPreview: depthCheck,
      },
    };
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    console.error(`${orderTag} 失败:`, msg);
    return {
      success: false,
      statusCode: 500,
      body: { error: msg },
      errorMessage: msg,
    };
  }
}

async function strategyBuy(
  direction: StrategyDirection,
  amount: number,
  source?: string,
  reason?: string,
  maxPrice?: number,
): Promise<void> {
  strategyRuntime.posBeforeBuy = getDirectionLocalSize(direction);
  strategyRuntime.actionTs = Date.now();
  strategyRuntime.buyLockUntil = Date.now() + STRAT_BUY_LOCK_MS;
  strategyRuntime.state = "WAIT_FILL";
  broadcastState();

  const orderResult = await executeStrategyOrder({
    direction,
    side: "buy",
    amount,
    slippage: strategyConfig.slippage,
    maxPrice,
    source: source || `strategy${strategyRuntime.activeStrategy ?? ""}`,
    exitReason: reason,
    roundEntry: `${strategyRuntime.roundEntryCount}/${strategyConfig.maxRoundEntries}`,
  });

  if (!orderResult.success) {
    console.log(
      `[Strategy${strategyRuntime.activeStrategy ?? ""}] 买入失败: ${orderResult.errorMessage || "下单失败"}`,
    );
    strategyRuntime.buyLockUntil = 0;
    strategyRuntime.state = "SCANNING";
    strategyRuntime.activeStrategy = null;
    broadcastState();
  }
}

async function strategyLockBuy(
  direction: StrategyDirection,
  targetShares: number,
  reason?: string,
): Promise<void> {
  if (targetShares <= 0) {
    strategyRuntime.state = "HOLDING";
    broadcastState();
    return;
  }
  const tokenId = getDirectionTokenId(direction);
  if (!tokenId) {
    strategyRuntime.state = "HOLDING";
    broadcastState();
    return;
  }
  let bestAsk = 0;
  try {
    ({ bestAsk } = await fetchBookTopOfBook(tokenId));
  } catch (err) {
    console.log(
      `[Strategy${strategyRuntime.activeStrategy ?? ""}] 锁仓盘口获取失败: ${err instanceof Error ? err.message : String(err)}`,
    );
    strategyRuntime.state = "HOLDING";
    broadcastState();
    return;
  }
  const worstPrice = Math.min(bestAsk + strategyConfig.slippage, 0.99);
  const buyAmount = Math.ceil(targetShares * worstPrice * 100) / 100;
  strategyRuntime.lockDirection = direction;
  strategyRuntime.lockPosBeforeBuy = getDirectionLocalSize(direction);
  strategyRuntime.lockTargetShares = targetShares;
  strategyRuntime.lockReason = reason || "";
  strategyRuntime.actionTs = Date.now();
  strategyRuntime.buyLockUntil = Date.now() + STRAT_BUY_LOCK_MS;
  strategyRuntime.state = "WAIT_LOCK_FILL";
  broadcastState();

  const orderResult = await executeStrategyOrder({
    direction,
    side: "buy",
    amount: buyAmount,
    slippage: strategyConfig.slippage,
    source: `strategy${strategyRuntime.activeStrategy ?? ""}lock`,
    exitReason: reason,
    roundEntry: `${strategyRuntime.roundEntryCount}/${strategyConfig.maxRoundEntries}`,
  });

  if (!orderResult.success) {
    console.log(
      `[Strategy${strategyRuntime.activeStrategy ?? ""}] 锁仓买入失败: ${orderResult.errorMessage || "下单失败"}`,
    );
    strategyRuntime.lockDirection = null;
    strategyRuntime.lockPosBeforeBuy = 0;
    strategyRuntime.lockTargetShares = 0;
    strategyRuntime.lockReason = "";
    strategyRuntime.buyLockUntil = 0;
    strategyRuntime.state = "HOLDING";
    broadcastState();
  }
}

async function strategySell(
  direction: StrategyDirection,
  exitReason?: string,
): Promise<void> {
  const totalPos = getDirectionLocalSize(direction);
  const shares = getSellableShares(direction);
  if (shares <= 0) {
    transitionToDone();
    return;
  }

  strategyRuntime.posBeforeSell = totalPos;
  strategyRuntime.waitVerifyAfterSell = !isDirectionVerified(direction);
  strategyRuntime.actionTs = Date.now();
  strategyRuntime.state = "WAIT_SELL_FILL";
  broadcastState();

  const orderResult = await executeStrategyOrder({
    direction,
    side: "sell",
    amount: shares,
    slippage: strategyConfig.slippage,
    source: `strategy${strategyRuntime.activeStrategy ?? ""}`,
    exitReason,
    roundEntry: `${strategyRuntime.roundEntryCount}/${strategyConfig.maxRoundEntries}`,
  });

  if (!orderResult.success) {
    console.log(
      `[Strategy${strategyRuntime.activeStrategy ?? ""}] 卖出失败: ${orderResult.errorMessage || "下单失败"}`,
    );
    strategyRuntime.waitVerifyAfterSell = false;
    strategyRuntime.state = "HOLDING";
    broadcastState();
  }
}

// ── 回测数据收集 ─────────────────────────────────────────────
const BACKTEST_STATE_FILE = resolve(__dirname, ".backtest-state.json");

function loadBacktestState(): boolean {
  try {
    if (!existsSync(BACKTEST_STATE_FILE)) return true; // 首次运行默认开启
    const data = JSON.parse(readFileSync(BACKTEST_STATE_FILE, "utf-8"));
    return typeof data.collecting === "boolean" ? data.collecting : true;
  } catch {
    return true;
  }
}

function persistBacktestState(): void {
  try {
    safeWriteTextFile(
      BACKTEST_STATE_FILE,
      JSON.stringify({ collecting: backtestCollecting }, null, 2) + "\n",
      "backtest-state",
    );
  } catch (err) {
    console.warn(
      `[Backtest] 状态保存失败: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

let backtestCollecting = loadBacktestState();
let backtestLastTickTs = 0;
let backtestLastCleanupDate = "";
let backtestWriteSuspendedReason = "";
let backtestLastWriteWarnTs = 0;
const BACKTEST_RETENTION_DAYS = 30;

// 启动时确保数据目录存在（防止首次写入失败）
if (backtestCollecting) {
  try {
    mkdirSync(BACKTEST_DATA_DIR, { recursive: true });
  } catch {
    /* 忽略 */
  }
}

function setBacktestCollecting(enabled: boolean): void {
  backtestCollecting = enabled;
  if (enabled) backtestWriteSuspendedReason = "";
  console.log(`[Backtest] 数据收集${enabled ? "已开启" : "已关闭"}`);
  persistBacktestState();
  if (enabled) {
    mkdirSync(BACKTEST_DATA_DIR, { recursive: true });
    cleanupOldBacktestFiles();
  }
  broadcastBacktestStatus();
}

function cleanupOldBacktestFiles(): void {
  try {
    if (!existsSync(BACKTEST_DATA_DIR)) return;
    const files = readdirSync(BACKTEST_DATA_DIR).filter((f) =>
      /^\d{4}-\d{2}-\d{2}\.jsonl$/.test(f),
    );
    if (files.length <= BACKTEST_RETENTION_DAYS) return;
    files.sort(); // 按日期字典序升序，旧的在前
    const toDelete = files.slice(0, files.length - BACKTEST_RETENTION_DAYS);
    for (const f of toDelete) {
      try {
        unlinkSync(resolve(BACKTEST_DATA_DIR, f));
        console.log(`[Backtest] 已清理旧数据文件: ${f}`);
      } catch {}
    }
  } catch (err) {
    console.warn(`[Backtest] 清理旧文件失败: ${(err as Error).message}`);
  }
}

function broadcastBacktestStatus(): void {
  broadcast("backtestStatus", { collecting: backtestCollecting });
}

function getBacktestFilePath(): string {
  const dateStr = new Date().toISOString().slice(0, 10);
  return resolve(BACKTEST_DATA_DIR, `${dateStr}.jsonl`);
}

function backtestAppend(record: Record<string, unknown>): void {
  if (backtestWriteSuspendedReason) return;
  try {
    appendFileSync(getBacktestFilePath(), JSON.stringify(record) + "\n", "utf8");
  } catch (err) {
    const error = err as NodeJS.ErrnoException;
    const message = error?.message || String(err);
    if (error?.code === "ENOSPC") {
      backtestWriteSuspendedReason = message;
      backtestCollecting = false;
      try {
        console.warn(`[Backtest] write suspended: ${message}`);
      } catch {
        /* stdout/stderr may also be out of space */
      }
      broadcastBacktestStatus();
      return;
    }
    const now = Date.now();
    if (now - backtestLastWriteWarnTs >= 60_000) {
      backtestLastWriteWarnTs = now;
      try {
        console.warn(`[Backtest] 写入失败: ${message}`);
      } catch {
        /* stdout/stderr may also be out of space */
      }
    }
  }
}

function backtestTick(): void {
  if (!backtestCollecting) return;
  const now = Date.now();
  if (now - backtestLastTickTs < 1000) return;

  const snapshot = getProbabilitySnapshot();
  const diff = getStrategyDiff();
  const rem = getStrategyRemainingSeconds(getPolymarketNowMs());
  if (snapshot == null || diff == null || !state.windowStart) return;

  // 每天首次写入时清理旧文件（超过保留天数的）
  const todayStr = new Date().toISOString().slice(0, 10);
  if (todayStr !== backtestLastCleanupDate) {
    cleanupOldBacktestFiles();
    backtestLastCleanupDate = todayStr;
  }

  backtestAppend({
    type: "tick",
    ts: now,
    windowStart: state.windowStart,
    diff: Math.round(diff * 100) / 100,
    upPct: snapshot.upPct,
    rem,
  });
  backtestLastTickTs = now;
}

// 事件驱动的策略调度器：短时间内多次事件只触发一次 tick
let strategyTickScheduled = false;
let strategyTickLastRunTs = 0;
const STRATEGY_TICK_MIN_GAP_MS = 10; // 连续 tick 最小间隔（防止风暴）

function scheduleStrategyTick(): void {
  if (strategyTickScheduled) return;
  strategyTickScheduled = true;
  const now = Date.now();
  const elapsed = now - strategyTickLastRunTs;
  if (elapsed >= STRATEGY_TICK_MIN_GAP_MS) {
    setImmediate(() => {
      strategyTickScheduled = false;
      strategyTickLastRunTs = Date.now();
      runStrategyTick();
    });
  } else {
    setTimeout(() => {
      strategyTickScheduled = false;
      strategyTickLastRunTs = Date.now();
      runStrategyTick();
    }, STRATEGY_TICK_MIN_GAP_MS - elapsed);
  }
}

function runStrategyTick(): void {
  const snapshot = getProbabilitySnapshot();
  const upPct = snapshot?.upPct ?? null;
  const dnPct = snapshot?.dnPct ?? null;
  const diff = getStrategyDiff();
  const now = Date.now();
  const marketNow = getPolymarketNowMs();
  const rem = getStrategyRemainingSeconds(marketNow);
  const currentPosition = getDirectionLocalSize(strategyRuntime.direction);
  const ctx = buildTickContext(rem, upPct, dnPct, diff, now);
  const finalize = () => {
    strategyRuntime.prevUpPct = upPct;
    // 通知已启用且需要 finalizeTick 的策略（s1/s2 记录 lastDiff）
    for (const s of getAllStrategies()) {
      if (
        strategyConfig.enabled[s.key] &&
        "finalizeTick" in s &&
        typeof (s as any).finalizeTick === "function"
      ) {
        (s as any).finalizeTick(diff);
      }
    }
  };

  if (isOrderWindowStale(marketNow)) {
    finalize();
    return;
  }

  if (!strategyRuntime.positionsReady) {
    finalize();
    return;
  }

  // 更新已启用策略的守卫状态（冷却锁等）
  for (const s of getAllStrategies()) {
    if (strategyConfig.enabled[s.key]) s.updateGuards(ctx);
    // 策略6 的因子评分作为市场观察数据无论是否启用都要计算
    else if (
      s.key === "s6" &&
      "computeFactors" in s &&
      typeof (s as any).computeFactors === "function"
    ) {
      (s as any).computeFactors(ctx);
    }
  }

  reconcilePaperMakerOrders(ctx);
  publishS10LiveMakerStatus(ctx);
  reconcileS10TerminalSweep(ctx);

  if (strategyRuntime.cleanupAfterVerify && strategyRuntime.direction) {
    if (!isDirectionVerified(strategyRuntime.direction)) {
      finalize();
      return;
    }
    if (currentPosition < 0.01) {
      strategyRuntime.cleanupAfterVerify = false;
      transitionToDone();
      finalize();
      return;
    }
    strategyRuntime.cleanupAfterVerify = false;
    strategyRuntime.state = "SELLING";
    console.log(
      `[Strategy${strategyRuntime.activeStrategy ?? ""}] 仓位已校准，剩余 ${currentPosition.toFixed(2)}，执行清仓卖出`,
    );
    broadcastState();
    void strategySell(
      strategyRuntime.direction,
      `校准清仓 剩余${currentPosition.toFixed(2)}`,
    );
    finalize();
    return;
  }

  if (strategyRuntime.state === "IDLE") {
    if (upPct == null || diff == null) {
      finalize();
      return;
    }
    if (anyStrategyEnabled()) {
      strategyRuntime.state = "SCANNING";
      broadcastState();
    }
    finalize();
    return;
  }

  if (strategyRuntime.state === "SCANNING") {
    if (!anyStrategyEnabled()) {
      strategyRuntime.state = "IDLE";
      broadcastState();
      finalize();
      return;
    }
    if (
      hasOpenPosition() &&
      !strategyRuntime.activeStrategy &&
      strategyConfig.enabled.s10
    ) {
      const resumeDirection = getSingleOpenPositionDirection();
      if (resumeDirection) {
        strategyRuntime.activeStrategy = 10;
        strategyRuntime.direction = resumeDirection;
        strategyRuntime.roundEntryCount = Math.max(
          strategyRuntime.roundEntryCount,
          1,
        );
        strategyRuntime.buyAmount = 0;
        strategyRuntime.state = "HOLDING";
        console.log(
          `[Strategy10] 恢复当前窗口模拟持仓 ${resumeDirection} size=${getDirectionLocalSize(resumeDirection).toFixed(4)}`,
        );
        broadcastState();
      }
      finalize();
      return;
    }
    if (
      hasOpenPosition() ||
      hasPendingStrategyBuyLock(now) ||
      upPct == null ||
      dnPct == null ||
      diff == null
    ) {
      finalize();
      return;
    }
    if (strategyRuntime.roundEntryCount >= strategyConfig.maxRoundEntries) {
      finalize();
      return;
    }
    const entry = checkEntry(ctx);
    if (!entry) {
      finalize();
      return;
    }
    const configuredAmount =
      strategyConfig.amount[strategyKeyOf(entry.strategy)];
    const buyAmount =
      entry.amount != null && Number.isFinite(entry.amount) && entry.amount > 0
        ? entry.amount
        : configuredAmount;
    if (!hasEnoughUsdcForBuy(buyAmount)) {
      finalize();
      return;
    }
    strategyRuntime.roundEntryCount++;
    strategyRuntime.activeStrategy = entry.strategy;
    strategyRuntime.direction = entry.dir;
    strategyRuntime.buyAmount = buyAmount;
    strategyRuntime.state = "BUYING";
    console.log(
      `[Strategy${entry.strategy}] 触发入场(${strategyRuntime.roundEntryCount}/${strategyConfig.maxRoundEntries}) ${entry.dir === "up" ? "买涨" : "买跌"} 金额:${buyAmount}`,
    );
    broadcastState();
    void strategyBuy(
      entry.dir,
      buyAmount,
      entry.source,
      entry.reason,
      entry.maxPrice,
    );
    finalize();
    return;
  }

  if (strategyRuntime.state === "WAIT_FILL") {
    if (hasConfirmedBuyPosition()) {
      strategyRuntime.buyLockUntil = 0;
      strategyRuntime.state = "HOLDING";
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 买入成交确认`,
      );
      // 通知策略买入成交（用于初始化追踪峰值等）
      if (strategyRuntime.activeStrategy && strategyRuntime.direction) {
        const activeStrat = getStrategy(
          strategyKeyOf(strategyRuntime.activeStrategy),
        );
        if (activeStrat?.onEntryFilled)
          activeStrat.onEntryFilled(ctx, strategyRuntime.direction);
      }
      broadcastState();
    } else if (now - strategyRuntime.actionTs > WAIT_FILL_TIMEOUT_MS) {
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 买入超过10s未确认，进入延迟确认等待`,
      );
      strategyRuntime.state = "RECONCILING_FILL";
      broadcastState();
      finalize();
      return;
    } else {
      finalize();
      return;
    }
  }

  if (strategyRuntime.state === "RECONCILING_FILL") {
    if (hasConfirmedBuyPosition()) {
      strategyRuntime.buyLockUntil = 0;
      strategyRuntime.state = "HOLDING";
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 延迟确认成功，恢复持仓管理`,
      );
      if (strategyRuntime.activeStrategy && strategyRuntime.direction) {
        const activeStrat = getStrategy(
          strategyKeyOf(strategyRuntime.activeStrategy),
        );
        if (activeStrat?.onEntryFilled)
          activeStrat.onEntryFilled(ctx, strategyRuntime.direction);
      }
      broadcastState();
    } else if (canReleaseUnconfirmedBuy(now)) {
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 超过15s且API确认无仓位，恢复扫描`,
      );
      strategyRuntime.state = "SCANNING";
      strategyRuntime.activeStrategy = null;
      strategyRuntime.direction = null;
      strategyRuntime.buyAmount = 0;
      strategyRuntime.posBeforeBuy = 0;
      strategyRuntime.actionTs = 0;
      strategyRuntime.buyLockUntil = 0;
      broadcastState();
      finalize();
      return;
    } else {
      finalize();
      return;
    }
  }

  if (strategyRuntime.state === "HOLDING") {
    if (currentPosition <= 0) {
      transitionToDone();
      finalize();
      return;
    }
    if (upPct == null || dnPct == null || diff == null) {
      finalize();
      return;
    }
    if (
      strategyRuntime.activeStrategy &&
      strategyRuntime.direction &&
      strategyRuntime.roundEntryCount < strategyConfig.maxRoundEntries &&
      !hasPendingStrategyBuyLock(now) &&
      !(
        strategyRuntime.activeStrategy === 10 &&
        S10_MAKER_ENGINE_ENABLED &&
        S10_MAKER_ONLY
      )
    ) {
      const activeStrat = getStrategy(
        strategyKeyOf(strategyRuntime.activeStrategy),
      );
      const scaleIn = activeStrat?.checkScaleIn?.(
        ctx,
        strategyRuntime.direction,
        currentPosition,
      );
      if (scaleIn && scaleIn.direction === strategyRuntime.direction) {
        const configuredAmount =
          strategyConfig.amount[strategyKeyOf(strategyRuntime.activeStrategy)];
        const buyAmount =
          scaleIn.amount != null &&
          Number.isFinite(scaleIn.amount) &&
          scaleIn.amount > 0
            ? scaleIn.amount
            : configuredAmount;
        if (hasEnoughUsdcForBuy(buyAmount)) {
          strategyRuntime.roundEntryCount++;
          strategyRuntime.buyAmount = buyAmount;
          strategyRuntime.state = "BUYING";
          console.log(
            `[Strategy${strategyRuntime.activeStrategy}] 分批加仓(${strategyRuntime.roundEntryCount}/${strategyConfig.maxRoundEntries}) ${strategyRuntime.direction === "up" ? "买涨" : "买跌"} 金额:${buyAmount} ${scaleIn.reason || ""}`,
          );
          broadcastState();
          void strategyBuy(strategyRuntime.direction, buyAmount);
          finalize();
          return;
        }
      }
    }
    const skipLegacyS10Exit =
      strategyRuntime.activeStrategy === 10 &&
      S10_MAKER_ENGINE_ENABLED &&
      S10_MAKER_ONLY;
    const exit = skipLegacyS10Exit ? null : checkExit(ctx);
    if (exit && strategyRuntime.direction) {
      if (exit.signal === "lock") {
        const lockDir = oppositeDirection(strategyRuntime.direction);
        const targetShares = currentPosition;
        console.log(
          `[Strategy${strategyRuntime.activeStrategy ?? ""}] 锁仓触发: ${exit.reason} 反边:${lockDir} 数量:${targetShares.toFixed(2)}`,
        );
        strategyRuntime.state = "LOCKING";
        broadcastState();
        void strategyLockBuy(lockDir, targetShares, exit.reason);
        finalize();
        return;
      }
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] ${exit.signal === "tp" ? "止盈" : "止损"}触发: ${exit.reason}`,
      );
      strategyRuntime.state = "SELLING";
      broadcastState();
      void strategySell(strategyRuntime.direction, exit.reason);
    }
    finalize();
    return;
  }

  if (strategyRuntime.state === "WAIT_LOCK_FILL") {
    if (hasConfirmedLockPosition()) {
      const lockedDirection = strategyRuntime.lockDirection;
      strategyRuntime.buyLockUntil = 0;
      strategyRuntime.locked = true;
      strategyRuntime.state = "DONE";
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 锁仓确认，完整套已建立`,
      );
      if (strategyRuntime.activeStrategy && lockedDirection) {
        const activeStrat = getStrategy(
          strategyKeyOf(strategyRuntime.activeStrategy),
        );
        if (activeStrat?.onLockFilled)
          activeStrat.onLockFilled(ctx, lockedDirection);
      }
      if (
        strategyRuntime.activeStrategy === 10 &&
        strategyConfig.executionMode === "paper" &&
        PAPER_PRE_SETTLEMENT_MERGE_ENABLED
      ) {
        const mergedShares = mergePaperFullSet(
          state.windowStart,
          "strategy10merge",
          strategyRuntime.lockReason || "full-set paper merge",
        );
        if (mergedShares > 0)
          console.log(
            `[Strategy10] 模拟 merge 完成 shares=${mergedShares.toFixed(4)}`,
          );
      }
      broadcastState();
      finalize();
      return;
    }
    if (now - strategyRuntime.actionTs > WAIT_FILL_TIMEOUT_MS) {
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 锁仓买入超时，恢复持仓管理`,
      );
      strategyRuntime.lockDirection = null;
      strategyRuntime.lockPosBeforeBuy = 0;
      strategyRuntime.lockTargetShares = 0;
      strategyRuntime.lockReason = "";
      strategyRuntime.buyLockUntil = 0;
      strategyRuntime.state = "HOLDING";
      broadcastState();
      finalize();
      return;
    }
    finalize();
    return;
  }

  if (strategyRuntime.state === "WAIT_SELL_FILL") {
    if (currentPosition < strategyRuntime.posBeforeSell - 0.01) {
      if (currentPosition < 0.01) {
        strategyRuntime.waitVerifyAfterSell = false;
        strategyRuntime.cleanupAfterVerify = false;
        console.log(
          `[Strategy${strategyRuntime.activeStrategy ?? ""}] 卖出确认，完成`,
        );
        transitionToDone();
        finalize();
        return;
      }

      if (strategyRuntime.waitVerifyAfterSell) {
        strategyRuntime.waitVerifyAfterSell = false;
        if (isDirectionVerified(strategyRuntime.direction)) {
          console.log(
            `[Strategy${strategyRuntime.activeStrategy ?? ""}] 卖出后已校准，剩余 ${currentPosition.toFixed(2)}，立即执行清仓`,
          );
          strategyRuntime.cleanupAfterVerify = false;
          strategyRuntime.state = "SELLING";
          broadcastState();
          if (strategyRuntime.direction)
            void strategySell(
              strategyRuntime.direction,
              `校准清仓 剩余${currentPosition.toFixed(2)}`,
            );
          finalize();
          return;
        }
        strategyRuntime.cleanupAfterVerify = true;
        strategyRuntime.state = "DONE";
        console.log(
          `[Strategy${strategyRuntime.activeStrategy ?? ""}] 卖出确认，等待校准后检查剩余仓位`,
        );
        broadcastState();
        finalize();
        return;
      }

      strategyRuntime.waitVerifyAfterSell = false;
      strategyRuntime.state = "HOLDING";
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 卖出确认，剩余 ${currentPosition.toFixed(2)} 继续处理`,
      );
      broadcastState();
      finalize();
      return;
    }

    if (now - strategyRuntime.actionTs > WAIT_FILL_TIMEOUT_MS) {
      console.log(
        `[Strategy${strategyRuntime.activeStrategy ?? ""}] 卖出超时，回持仓`,
      );
      strategyRuntime.waitVerifyAfterSell = false;
      strategyRuntime.state = "HOLDING";
      broadcastState();
    }
    finalize();
    return;
  }

  finalize();
}

function buildApiStatePayload(): Record<string, unknown> {
  return {
    ...buildStatePayload(true),
    tradeHistory,
    paperTradeHistory,
    executionEvents: executionEvents.slice(0, 500),
    wsStatus,
    claimable: {
      total: claimableTotal,
      positions: claimablePositions,
    },
    claimCooldown: {
      running: claimCycleRunning || claimInProgress,
      nextCheckAt: claimNextCheckAt,
      cooldownUntil: claimCooldownUntil,
      reason: claimLastReason,
    },
  };
}

app.get("/api/state", (_req, res) => {
  res.json(buildApiStatePayload());
});

app.get("/api/execution-log", (req, res) => {
  const mode = typeof req.query.mode === "string" ? req.query.mode : "";
  const event = typeof req.query.event === "string" ? req.query.event : "";
  const source = typeof req.query.source === "string" ? req.query.source : "";
  const windowStart =
    typeof req.query.windowStart === "string"
      ? Number(req.query.windowStart)
      : NaN;
  const strategy =
    typeof req.query.strategy === "string" ? Number(req.query.strategy) : NaN;
  const limitRaw =
    typeof req.query.limit === "string" ? Number(req.query.limit) : 500;
  const limit = Math.round(
    clampNumber(Number.isFinite(limitRaw) ? limitRaw : 500, 1, 2000),
  );
  let rows = executionEvents;
  if (mode === "paper" || mode === "live")
    rows = rows.filter((row) => row.executionMode === mode);
  if (event) rows = rows.filter((row) => row.event === event);
  if (source) rows = rows.filter((row) => row.source === source);
  if (Number.isFinite(windowStart))
    rows = rows.filter((row) => row.windowStart === windowStart);
  if (Number.isFinite(strategy))
    rows = rows.filter((row) => row.strategy === strategy);
  res.json({
    count: rows.length,
    events: rows.slice(0, limit),
    latest: executionEvents[0] ?? null,
  });
});

app.get("/api/bonereaper", (_req, res) => {
  res.json(getBonereaperMonitorPayload().bonereaperMonitor);
});

app.get("/api/strategy/descriptions", (_req, res) => {
  res.json(getAllDescriptions());
});

app.get("/api/backtest/status", (_req, res) => {
  res.json({ collecting: backtestCollecting });
});

app.post("/api/backtest/toggle", (_req, res) => {
  setBacktestCollecting(!backtestCollecting);
  res.json({ collecting: backtestCollecting });
});

app.post("/api/paper/reset", (_req, res) => {
  paperAccount = createPaperAccountState();
  paperTradeHistory = [];
  if (state.windowStart && state.upTokenId && state.downTokenId) {
    rememberPaperWindow({
      windowStart: state.windowStart,
      upTokenId: state.upTokenId,
      downTokenId: state.downTokenId,
    });
  }
  persistPaperAccountState();
  persistPaperTradeHistory();
  resetStrategyRuntime("模拟盘重置");
  broadcastState();
  broadcastPaperTradeHistory();
  res.json({ success: true, paper: getPaperSummary(), paperTradeHistory });
});

app.post("/api/strategy/config", (req, res) => {
  const prevExecutionMode = strategyConfig.executionMode;
  const { config, error } = applyStrategyConfigUpdate(strategyConfig, req.body);
  if (!config) {
    res.status(400).json({ error: error || "配置错误" });
    return;
  }

  strategyConfig = config;
  setLiveRuntimeSwitches(
    config.executionMode === "live",
    `execution mode ${config.executionMode}`,
  );
  if (prevExecutionMode !== config.executionMode) {
    strategyRuntime.positionsReady =
      config.executionMode === "paper" || !PROXY_ADDRESS;
    resetStrategyRuntime(`执行模式切换为 ${config.executionMode}`);
  }
  savePersistedStrategyConfig(config);
  const configSummary = ALL_STRATEGY_KEYS.map(
    (k) => `${k}:${config.enabled[k] ? "on" : "off"}(${config.amount[k]})`,
  ).join(" ");
  const s10Tail = config.s10TailMultipliers;
  console.log(
    `[StrategyConfig] 已更新 ${configSummary} mode:${config.executionMode} maxRound:${config.maxRoundEntries} s10Tail=${s10Tail.earlyProbe}/${s10Tail.probe}/${s10Tail.robust}/${s10Tail.certainty} 当前进程生效`,
  );
  broadcastState();
  res.json({ success: true, strategyConfig });
});

// ── REST：Claim 接口（已弃用）────────────────────────────────
// 自动 Claim 已迁移至 Polymarket 官网（Settings → Auto Redeem）
app.post("/api/claim", async (_req, res) => {
  res.status(410).json({
    error: "本地 Claim 功能已移除",
    hint: "请在 Polymarket 官网 Settings 中开启 Auto Redeem",
  });
});

// ── REST：下单接口 ────────────────────────────────────────────
app.post("/api/order", async (req, res) => {
  const { direction, side, amount, slippage } = req.body as {
    direction: "up" | "down";
    side: "buy" | "sell";
    amount: number;
    slippage?: number;
  };
  const result = await executeOrder({
    direction,
    side,
    amount,
    slippage,
    source: "manual",
  });
  res.status(result.statusCode).json(result.body);
});

// ── 浏览器 WS 连接 ────────────────────────────────────────────
if (wss) {
  wss.on("connection", (ws, req) => {
    const dataMode = resolveClientDataModeFromUrl(req.url);
    clientSessions.set(ws, createClientSession(dataMode));
    console.log(
      `[WS] 浏览器已连接，当前: ${wss!.clients.size} mode=${dataMode}`,
    );
    send(ws, "clientConfig", { dataMode });
    sendStateToClient(ws, { includeHistory: true });
    sendTradeHistoryToClient(ws);
    sendPaperTradeHistoryToClient(ws);
    sendExecutionEventsToClient(ws);
    sendPmPnlToClient(ws);
    sendBonereaperMonitorToClient(ws);
    send(ws, "wsStatus", wsStatus as unknown as Record<string, unknown>);
    send(ws, "claimable", {
      total: claimableTotal,
      positions: claimablePositions,
    });
    send(ws, "claimCooldown", {
      running: claimCycleRunning || claimInProgress,
      nextCheckAt: claimNextCheckAt,
      cooldownUntil: claimCooldownUntil,
      reason: claimLastReason,
    });
    send(ws, "backtestStatus", { collecting: backtestCollecting });
    ws.on("message", (raw) => {
      try {
        applyClientConfig(ws, JSON.parse(raw.toString()));
      } catch {
        // 忽略非 JSON 或非配置消息
      }
    });
    ws.on("close", () => {
      const session = clientSessions.get(ws);
      if (session) {
        clearStateTimer(session);
        clientSessions.delete(ws);
      }
      console.log(`[WS] 浏览器断开，当前: ${wss!.clients.size}`);
    });
  });
}

// ── 启动 ──────────────────────────────────────────────────────
server.listen(PORT, async () => {
  logLifecycle("listen", { port: PORT, appMode: APP_MODE });
  logMemorySnapshot("listenMemory");
  console.log(`\n BTC 5m 盘口监控服务已启动`);
  console.log(`  by 岳来岳会赚 | X: @188888_x`);
  console.log(`  运行模式:   ${APP_MODE}`);
  console.log(`  状态接口:   http://localhost:${PORT}/api/state`);
  if (IS_FULL_MODE) {
    console.log(`  浏览器打开: http://localhost:${PORT}`);
    console.log(`  WS 地址:    ws://localhost:${PORT}`);
  }
  console.log("");

  await ensureClobClient();
  startUserWs();
  startBinanceWs();
  await syncPositionsFromApi();
  await syncUsdcBalance();

  if (BONEREAPER_MONITOR_ENABLED) {
    bonereaperMonitor.start(() => broadcastBonereaperMonitor());
    setInterval(() => {
      const sample = buildBonereaperMarketSample();
      if (sample) bonereaperMonitor.observeMarket(sample);
    }, BONEREAPER_MARKET_SAMPLE_MS);
    console.log(
      `[Bonereaper] monitor enabled address=${BONEREAPER_MONITOR_ADDRESS} poll=${BONEREAPER_MONITOR_POLL_MS}ms`,
    );
  }

  setInterval(async () => {
    await syncPositionsFromApi();
    broadcastState();
  }, 2000);
  setInterval(async () => {
    await syncUsdcBalance();
    broadcastState();
  }, 5000);
  setInterval(() => {
    refreshBinanceOffset("定时", { allowLatestFallback: false });
  }, BINANCE_ALIGN_REFRESH_MS);
  setInterval(() => {
    void refreshFullSetArbSnapshot();
  }, S10_FULLSET_REFRESH_MS);
  setInterval(() => {
    runStrategyTick();
    backtestTick();
  }, STRATEGY_TICK_MS);
  setInterval(() => {
    const mem = process.memoryUsage();
    if (mem.rss > 900 * 1024 * 1024 || mem.heapUsed > 650 * 1024 * 1024) {
      logMemorySnapshot("memoryHigh");
    }
  }, 60 * 1000);
  // Claim 功能已移至 Polymarket 官网（Settings → Auto Redeem），本地不再自动执行

  // Polymarket 真实盈亏：启动全量加载 + 每 30 秒增量同步（外部下单也能快速反映）
  // positions 变化较慢（只在结算时），每 5 分钟同步一次就够
  pmPnlManager
    .init()
    .then(() => broadcastPmPnl())
    .catch((err) => {
      console.warn(
        `[PmPnl] 启动加载失败: ${err instanceof Error ? err.message : String(err)}`,
      );
    });
  setInterval(async () => {
    const beforeCount = pmPnlManager.getTotalPnl(0).positionCount;
    await pmPnlManager.syncIncremental();
    const afterCount = pmPnlManager.getTotalPnl(0).positionCount;
    const delta = afterCount - beforeCount;
    console.log(
      `[PmPnl] 定时增量 tick: ${delta > 0 ? `+${delta}` : "无新"} 笔（总 ${afterCount}）`,
    );
    broadcastPmPnl();
  }, 30 * 1000);
  setInterval(
    async () => {
      await pmPnlManager.syncPositions();
      broadcastPmPnl();
    },
    5 * 60 * 1000,
  );

  const currentWindow = getCurrentWindowStart();
  fetchRecentResults(currentWindow, true);
  await advanceToLiveWindow(currentWindow);
  void refreshFullSetArbSnapshot();
});

function shutdown(reason: string) {
  logLifecycle("shutdown", { reason });
  console.error(`[进程信号] ${reason}`);
  stopped = true;
  if (switchTimer) clearTimeout(switchTimer);
  if (reconnectTimer) clearTimeout(reconnectTimer);
  if (claimCycleTimer) clearTimeout(claimCycleTimer);
  if (marketWs) marketWs.close();
  if (chainlinkWs) chainlinkWs.close();
  if (userWs) (userWs as WebSocket).close();
  if (binanceWs) binanceWs.close();
  server.close();
  process.exit(0);
}

process.on("SIGINT", () => {
  shutdown("SIGINT");
});
process.on("SIGTERM", () => {
  shutdown("SIGTERM");
});
