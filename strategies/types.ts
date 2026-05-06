export const ALL_STRATEGY_KEYS = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10"] as const;
export const ALL_STRATEGY_NUMBERS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10] as const;

export type StrategyKey = typeof ALL_STRATEGY_KEYS[number];
export type StrategyNumber = typeof ALL_STRATEGY_NUMBERS[number];

export type StrategyDirection = "up" | "down";
export type TrendBias = "bullish" | "bearish" | "neutral";
export type StrategyLifecycleState =
  | "IDLE"
  | "SCANNING"
  | "BUYING"
  | "WAIT_FILL"
  | "RECONCILING_FILL"
  | "HOLDING"
  | "LOCKING"
  | "WAIT_LOCK_FILL"
  | "SELLING"
  | "WAIT_SELL_FILL"
  | "DONE";

export interface Kline {
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  closed: boolean;
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

export interface FullSetArbSnapshot {
  ready: boolean;
  status: string;
  reason: string;
  windowStart: number;
  updatedAt: number;
  ageMs: number | null;
  maxBudget: number;
  targetShares: number;
  totalCost: number | null;
  totalCostPct: number | null;
  grossProfit: number | null;
  grossProfitPct: number | null;
  netProfitPct: number | null;
  minProfitPct: number;
  triggerProfitPct: number;
  feeBufferPct: number;
  upAvgAsk: number | null;
  downAvgAsk: number | null;
  upTopAsk: number | null;
  downTopAsk: number | null;
  upLevelsUsed: number;
  downLevelsUsed: number;
  firstLegDirection: StrategyDirection | null;
  firstLegCost: number | null;
  secondLegDirection: StrategyDirection | null;
  secondLegCost: number | null;
  bookLatencyMs: number | null;
}

export interface StrategyTickContext {
  rem: number;
  upPct: number | null;
  dnPct: number | null;
  diff: number | null;
  now: number;
  prevUpPct: number | null;
  bestBid: number | null;
  bestAsk: number | null;
  kline1m: readonly Kline[];
  kline5m: readonly Kline[];
  macd1m: MacdSnapshot;
  macdFast1m: MacdSnapshot;
  fullSetArb: FullSetArbSnapshot;
  marketHoursOnly: boolean;
  position: {
    upSize: number;
    downSize: number;
    upCostPct: number | null;
    downCostPct: number | null;
  };
}

export interface EntrySignal {
  direction: StrategyDirection;
  amount?: number;
  reason?: string;
}

export interface MakerQuoteSignal {
  direction: StrategyDirection;
  price: number;
  shares: number;
  ttlMs?: number;
  reason?: string;
}

export interface MakerStatusSnapshot {
  activeOrders: number;
  upOrders: number;
  downOrders: number;
  upBidPct: number | null;
  downBidPct: number | null;
  totalBidCostPct: number | null;
  targetEdgePct: number | null;
  filledCount: number;
  mergedCount: number;
  lastFill: {
    direction: StrategyDirection;
    price: number;
    shares: number;
    trigger: string;
    ts: number;
  } | null;
  lastReason: string;
}

export interface ExitSignalResult {
  signal: "tp" | "sl" | "lock";
  reason: string;
}

export type ExitSignal = ExitSignalResult | null;

export interface StrategyDescriptionLine {
  text: string;
  color?: string;
  marginTop?: boolean;
}

export interface StrategyDescription {
  key: StrategyKey;
  number: StrategyNumber;
  name: string;
  title: string;
  lines: StrategyDescriptionLine[];
}

export interface IStrategy {
  readonly key: StrategyKey;
  readonly number: StrategyNumber;
  readonly name: string;

  getDescription(): StrategyDescription;

  updateGuards(ctx: StrategyTickContext): void;

  checkEntry(ctx: StrategyTickContext): EntrySignal | null;

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal;

  resetState(): void;

  getStatePayload(): Record<string, unknown>;

  onEntryFilled?(ctx: StrategyTickContext, direction: StrategyDirection): void;

  onLockFilled?(ctx: StrategyTickContext, direction: StrategyDirection): void;

  checkScaleIn?(ctx: StrategyTickContext, direction: StrategyDirection, currentPosition: number): EntrySignal | null;

  getMakerQuotes?(ctx: StrategyTickContext): MakerQuoteSignal[];

  onMakerStatus?(ctx: StrategyTickContext, status: MakerStatusSnapshot): void;
}
