/**
 * 策略1 · 常规加强 — 基于策略1改进：追踪止损 + 概率回撤止盈 + 单方向锁定
 *
 * 入场逻辑与策略1相同（差价穿越），但出场机制完全不同：
 * - 追踪止损：记录持仓期间 diff 峰值，从峰值回撤超阈值时止损
 * - 概率回撤止盈：记录持仓期间概率峰值，从峰值回落超阈值时止盈
 * - 单方向锁定：过热时只锁定该方向，不影响另一方向
 */

import type {
  IStrategy, StrategyKey, StrategyNumber, StrategyDirection,
  StrategyTickContext, EntrySignal, ExitSignal, StrategyDescription,
} from "./types.js";

// ── 入场参数 ────────────────────────────────────────────────
const ENTRY_DIFF = 35;                   // diff 穿越此阈值时触发入场（买涨 >35 / 买跌 <-35）
const ENTRY_PROB_CAP = 80;               // 入场时概率必须低于此值，避免追高
const WINDOW_MAX_REMAINING = 210;        // 入场扫描起始：剩余 ≤210s（3分30秒）开始检测
const WINDOW_MIN_REMAINING = 50;         // 入场扫描截止：剩余 ≤50s 停止检测

// ── 冷却参数 ────────────────────────────────────────────────
const NEUTRAL_DIFF = 25;                 // |diff| ≤ 此值视为中性区
const NEUTRAL_HOLD_MS = 3000;            // diff 在中性区持续此毫秒数后解除冷却锁
const OVERHEAT_DIFF = 55;                // diff 达到此值进入过热区
const OVERHEAT_PROB = 85;                // 概率达到此值且 diff 过热时触发锁定

// ── 出场参数 ────────────────────────────────────────────────
const TRAILING_STOP_RETRACEMENT = 20;    // diff 从峰值回撤超过此值触发止损（调大=更宽容，调小=更灵敏）
const TRAILING_STOP_MIN_DIFF = 5;        // diff 绝对值低于此值直接止损（兜底，防止趋势完全消失）
const MIN_HOLD_MS = 3000;                // 最低持仓时间（毫秒），期间只看兜底止损，不触发追踪止损和回撤止盈
const PROB_PEAK_RETRACEMENT = 8;         // 概率从峰值回落超过此百分点触发止盈（调大=持仓更久，调小=出场更快）
const PROB_PEAK_MIN_THRESHOLD = 85;      // 概率峰值至少达到此值后才启用回撤止盈（防止低概率区小波动误触）
const FORCE_EXIT_REM = 10;               // 窗口结束前此秒数内强制平仓（避免进入不确定的结算阶段）
const TP_LADDER_START = 90;              // 阶梯止盈起始概率（每1%一档升至100%）

interface S1State {
  lastDiff: number | null;
  // 单方向锁定
  upBlocked: boolean;
  downBlocked: boolean;
  neutralSince: number;
  upProbSeen: boolean;
  downProbSeen: boolean;
  // 追踪止损/止盈 — 持仓期间记录
  peakDiff: number | null;          // 持仓期间有利方向 diff 峰值
  peakProbability: number | null;   // 持仓期间概率峰值
  entryTs: number;                  // 入场成交时间戳
}

function createState(): S1State {
  return {
    lastDiff: null,
    upBlocked: false,
    downBlocked: false,
    neutralSince: 0,
    upProbSeen: false,
    downProbSeen: false,
    peakDiff: null,
    peakProbability: null,
    entryTs: 0,
  };
}

export class S1Enhanced implements IStrategy {
  readonly key: StrategyKey = "s1";
  readonly number: StrategyNumber = 1;
  readonly name = "常规加强";

  private s: S1State = createState();

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略1 · 常规加强",
      lines: [
        { text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~${WINDOW_MIN_REMAINING}s 时检测` },
        { text: `📈 买涨：差价重新上穿 +${ENTRY_DIFF} 且 涨概率 <${ENTRY_PROB_CAP}%` },
        { text: `📉 买跌：差价重新下穿 -${ENTRY_DIFF} 且 跌概率 <${ENTRY_PROB_CAP}%` },
        { text: `阶梯止盈：${WINDOW_MAX_REMAINING}s≥${TP_LADDER_START}% → ${FORCE_EXIT_REM}s≥100%`, color: "#3fb950", marginTop: true },
        { text: `回撤止盈：概率峰值≥${PROB_PEAK_MIN_THRESHOLD}%后回落${PROB_PEAK_RETRACEMENT}%触发`, color: "#3fb950" },
        { text: `追踪止损：diff从峰值回撤${TRAILING_STOP_RETRACEMENT}或diff≤${TRAILING_STOP_MIN_DIFF}`, color: "#f85149" },
        { text: `窗口结束前${FORCE_EXIT_REM}s强制平仓`, color: "#f85149" },
        { text: "过热时仅锁定单方向，不影响另一方向", color: "#888", marginTop: true },
      ],
    };
  }

  updateGuards(ctx: StrategyTickContext): void {
    const { rem, upPct, dnPct, diff, now } = ctx;
    if (diff == null || upPct == null || dnPct == null) {
      this.s.neutralSince = 0;
      return;
    }

    // 中性区解锁
    if (Math.abs(diff) <= NEUTRAL_DIFF) {
      if (!this.s.neutralSince) {
        this.s.neutralSince = now;
      } else if (now - this.s.neutralSince >= NEUTRAL_HOLD_MS) {
        if (this.s.upBlocked) {
          console.log("[Strategy1] 差价回归中性，解除买涨冷却锁");
          this.s.upBlocked = false;
        }
        if (this.s.downBlocked) {
          console.log("[Strategy1] 差价回归中性，解除买跌冷却锁");
          this.s.downBlocked = false;
        }
        this.s.upProbSeen = false;
        this.s.downProbSeen = false;
        this.s.neutralSince = 0;
      }
    } else {
      this.s.neutralSince = 0;
    }

    if (rem > WINDOW_MAX_REMAINING || rem <= WINDOW_MIN_REMAINING) return;
    if (Math.abs(diff) <= NEUTRAL_DIFF) return;

    // 高概率标记（仅在非中性区时记录）
    if (diff < ENTRY_DIFF && upPct >= ENTRY_PROB_CAP) {
      this.s.upProbSeen = true;
    }
    if (diff > -ENTRY_DIFF && dnPct >= ENTRY_PROB_CAP) {
      this.s.downProbSeen = true;
    }

    // 高概率先见 — 单方向锁定
    if (this.s.upProbSeen && !this.s.upBlocked) {
      console.log("[Strategy1] 买涨方向先进入高概率区，锁定买涨直到回归中性");
      this.s.upBlocked = true;
    }
    if (this.s.downProbSeen && !this.s.downBlocked) {
      console.log("[Strategy1] 买跌方向先进入高概率区，锁定买跌直到回归中性");
      this.s.downBlocked = true;
    }

    // 过热区 — 单方向锁定
    if (!this.s.upBlocked && diff >= OVERHEAT_DIFF && upPct >= OVERHEAT_PROB) {
      console.log("[Strategy1] 买涨方向进入过热区，锁定买涨");
      this.s.upBlocked = true;
    }
    if (!this.s.downBlocked && diff <= -OVERHEAT_DIFF && dnPct >= OVERHEAT_PROB) {
      console.log("[Strategy1] 买跌方向进入过热区，锁定买跌");
      this.s.downBlocked = true;
    }
  }

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    if (rem > WINDOW_MAX_REMAINING || rem <= WINDOW_MIN_REMAINING) return null;

    const lastDiff = this.s.lastDiff;
    if (
      !this.s.upBlocked
      && lastDiff != null
      && lastDiff <= ENTRY_DIFF
      && diff > ENTRY_DIFF
      && upPct < ENTRY_PROB_CAP
    ) {
      return { direction: "up" };
    }
    if (
      !this.s.downBlocked
      && lastDiff != null
      && lastDiff >= -ENTRY_DIFF
      && diff < -ENTRY_DIFF
      && dnPct < ENTRY_PROB_CAP
    ) {
      return { direction: "down" };
    }
    return null;
  }

  /** 买入成交后调用，初始化追踪峰值（每次入场都重置） */
  onEntryFilled(ctx: StrategyTickContext, direction: StrategyDirection): void {
    const { diff, upPct, dnPct, now } = ctx;
    this.s.peakDiff = diff != null ? (direction === "up" ? diff : -diff) : null;
    this.s.peakProbability = direction === "up" ? upPct : dnPct;
    this.s.entryTs = now;
  }

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { rem, upPct, dnPct, diff, now } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    const myPct = direction === "up" ? upPct : dnPct;

    // 有利方向的 diff（买涨时 diff 为正，买跌时取反）
    const favorableDiff = direction === "up" ? diff : -diff;

    // 更新峰值
    if (this.s.peakDiff == null || favorableDiff > this.s.peakDiff) {
      this.s.peakDiff = favorableDiff;
    }
    if (this.s.peakProbability == null || myPct > this.s.peakProbability) {
      this.s.peakProbability = myPct;
    }

    // 窗口结束前强制平仓
    if (rem <= FORCE_EXIT_REM && rem > 0) {
      return myPct >= 70
        ? { signal: "tp", reason: `强制平仓 rem=${rem}s 概率${myPct}%≥70%` }
        : { signal: "sl", reason: `强制平仓 rem=${rem}s 概率${myPct}%<70%` };
    }

    // 阶梯止盈
    const span = WINDOW_MAX_REMAINING - FORCE_EXIT_REM;
    const elapsed = Math.max(0, WINDOW_MAX_REMAINING - rem);
    const tpThreshold = TP_LADDER_START + Math.floor(elapsed / span * (100 - TP_LADDER_START));
    const tpCapped = Math.min(tpThreshold, 100);
    if (myPct >= tpCapped) {
      return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥${tpCapped}% rem=${rem}s` };
    }

    // 概率回撤止盈
    if (
      this.s.peakProbability >= PROB_PEAK_MIN_THRESHOLD
      && myPct <= this.s.peakProbability - PROB_PEAK_RETRACEMENT
    ) {
      return { signal: "tp", reason: `回撤止盈 概率${myPct}% 峰值${this.s.peakProbability}% 回落${this.s.peakProbability - myPct}% rem=${rem}s` };
    }

    // 兜底止损（不受最低持仓时间限制）
    if (direction === "up" && diff <= TRAILING_STOP_MIN_DIFF) {
      return { signal: "sl", reason: `兜底止损 diff=${Math.round(diff)}≤${TRAILING_STOP_MIN_DIFF} rem=${rem}s` };
    }
    if (direction === "down" && diff >= -TRAILING_STOP_MIN_DIFF) {
      return { signal: "sl", reason: `兜底止损 diff=${Math.round(diff)}≥${-TRAILING_STOP_MIN_DIFF} rem=${rem}s` };
    }

    // 最低持仓时间内跳过追踪止损
    if (this.s.entryTs > 0 && now - this.s.entryTs < MIN_HOLD_MS) return null;

    // 追踪止损
    if (this.s.peakDiff != null && this.s.peakDiff - favorableDiff >= TRAILING_STOP_RETRACEMENT) {
      return { signal: "sl", reason: `追踪止损 diff=${Math.round(diff)} 峰值${Math.round(this.s.peakDiff)} 回撤${Math.round(this.s.peakDiff - favorableDiff)} rem=${rem}s` };
    }

    return null;
  }

  /** 每个 tick 结束后由外部调用 */
  finalizeTick(diff: number | null): void {
    this.s.lastDiff = diff;
  }

  resetState(): void {
    this.s = createState();
  }

  getStatePayload(): Record<string, unknown> {
    return {
      lastDiff: this.s.lastDiff,
      upBlocked: this.s.upBlocked,
      downBlocked: this.s.downBlocked,
      neutralSince: this.s.neutralSince,
      upProbSeen: this.s.upProbSeen,
      downProbSeen: this.s.downProbSeen,
      peakDiff: this.s.peakDiff,
      peakProbability: this.s.peakProbability,
    };
  }
}
