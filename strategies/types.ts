/**
 * 策略模块共享类型
 */

// ── 策略注册（添加/删除策略只需改这里）─────────────────────
// 添加策略：在 ALL_STRATEGY_KEYS 加 key，在 ALL_STRATEGY_NUMBERS 加 number
// StrategyKey 和 StrategyNumber 会自动从数组推导
export const ALL_STRATEGY_KEYS = ["s1", "s2", "s3", "s4", "s5"] as const;
export const ALL_STRATEGY_NUMBERS = [1, 2, 3, 4, 5] as const;

export type StrategyKey = typeof ALL_STRATEGY_KEYS[number];
export type StrategyNumber = typeof ALL_STRATEGY_NUMBERS[number];

export type StrategyDirection = "up" | "down";
export type StrategyLifecycleState =
  | "IDLE"
  | "SCANNING"
  | "BUYING"
  | "WAIT_FILL"
  | "RECONCILING_FILL"
  | "HOLDING"
  | "SELLING"
  | "WAIT_SELL_FILL"
  | "DONE";

/** Binance K 线 */
export interface Kline {
  openTime: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
  closed: boolean;
}

/** 每个 tick 传给策略的只读市场快照 */
export interface StrategyTickContext {
  rem: number;
  upPct: number | null;
  dnPct: number | null;
  diff: number | null;
  now: number;
  prevUpPct: number | null;
  kline1m: readonly Kline[];   // Binance 1分钟K线（最新在末尾）
  kline5m: readonly Kline[];   // Binance 5分钟K线
  marketHoursOnly: boolean;    // 动量策略是否只在美股开盘时段入场
}

/** 策略入场信号 */
export interface EntrySignal {
  direction: StrategyDirection;
}

/** 策略出场信号 */
export interface ExitSignalResult {
  signal: "tp" | "sl";
  reason: string;
}

export type ExitSignal = ExitSignalResult | null;

/** 前端 hover 提示的描述行 */
export interface StrategyDescriptionLine {
  text: string;
  color?: string;
  marginTop?: boolean;
}

/** 策略描述（用于前端动态生成 UI） */
export interface StrategyDescription {
  key: StrategyKey;
  number: StrategyNumber;
  name: string;
  title: string;
  lines: StrategyDescriptionLine[];
}

/** 策略接口 — 每个策略必须实现 */
export interface IStrategy {
  readonly key: StrategyKey;
  readonly number: StrategyNumber;
  readonly name: string;

  /** 返回前端 hover 描述 */
  getDescription(): StrategyDescription;

  /** 每个 tick 更新内部守卫状态（冷却锁等），在 checkEntry 之前调用 */
  updateGuards(ctx: StrategyTickContext): void;

  /** 检查入场条件（SCANNING 阶段调用） */
  checkEntry(ctx: StrategyTickContext): EntrySignal | null;

  /** 检查出场条件（HOLDING 阶段调用） */
  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal;

  /** 窗口切换时重置策略私有状态 */
  resetState(): void;

  /** 序列化策略私有状态，用于广播给前端 */
  getStatePayload(): Record<string, unknown>;

  /** 通知策略已进入持仓（买入成交后调用） */
  onEntryFilled?(ctx: StrategyTickContext, direction: StrategyDirection): void;
}
