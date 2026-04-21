/**
 * 策略2 · 常规 — 差价穿越入场
 */

import type {
  IStrategy, StrategyKey, StrategyNumber, StrategyDirection,
  StrategyTickContext, EntrySignal, ExitSignal, StrategyDescription,
} from "./types.js";

// ── 入场参数 ────────────────────────────────────────────────
const ENTRY_DIFF = 40;                   // diff 穿越此阈值时触发入场（买涨 >40 / 买跌 <-40）
const ENTRY_PROB_CAP = 75;               // 入场时概率必须低于此值，避免追高
const WINDOW_MAX_REMAINING = 168;        // 入场扫描起始：剩余 ≤168s 开始检测
const WINDOW_MIN_REMAINING = 48;         // 入场扫描截止：剩余 ≤48s 停止检测

// ── 出场参数 ────────────────────────────────────────────────
const STOP_LOSS_DIFF = 5;                // diff 绝对值低于此值止损
const TP_LADDER_FLOOR = 8;               // 阶梯止盈截止：剩余 <此秒数时持仓到结束

// ── 冷却参数 ────────────────────────────────────────────────
const NEUTRAL_DIFF = 25;                 // |diff| ≤ 此值视为中性区
const NEUTRAL_HOLD_MS = 3000;            // diff 在中性区持续此毫秒数后解除冷却锁
const OVERHEAT_DIFF = 55;                // diff 达到此值进入过热区
const OVERHEAT_PROB = 80;                // 概率达到此值且 diff 过热时触发锁定（双方向同时锁）

interface S1State {
  lastDiff: number | null;
  upBlocked: boolean;
  downBlocked: boolean;
  neutralSince: number;
  upProbSeen: boolean;
  downProbSeen: boolean;
}

function createState(): S1State {
  return {
    lastDiff: null,
    upBlocked: false,
    downBlocked: false,
    neutralSince: 0,
    upProbSeen: false,
    downProbSeen: false,
  };
}

export class S2Regular implements IStrategy {
  readonly key: StrategyKey = "s2";
  readonly number: StrategyNumber = 2;
  readonly name = "常规";

  private s: S1State = createState();

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略2 · 常规",
      lines: [
        { text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~${WINDOW_MIN_REMAINING}s 时检测` },
        { text: `📈 买涨：差价重新上穿 +${ENTRY_DIFF} 且 涨概率 <${ENTRY_PROB_CAP}%` },
        { text: `📉 买跌：差价重新下穿 -${ENTRY_DIFF} 且 跌概率 <${ENTRY_PROB_CAP}%` },
        { text: `止盈阶梯：${WINDOW_MAX_REMAINING}s≥90% → ${TP_LADDER_FLOOR}s≥100% / <${TP_LADDER_FLOOR}s 持仓到结束`, color: "#3fb950", marginTop: true },
        { text: `止损：差价 <=${STOP_LOSS_DIFF}`, color: "#f85149" },
        { text: "入场前若先进入高概率区或过热区，策略2本轮会整体锁定，需回到中性区后才重新开放", color: "#888", marginTop: true },
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
        if (this.s.upBlocked || this.s.downBlocked || this.s.upProbSeen || this.s.downProbSeen) {
          console.log("[Strategy2] 差价回归中性，解除策略2冷却锁");
        }
        this.s.upBlocked = false;
        this.s.downBlocked = false;
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

    // 高概率先见锁定
    if (
      !this.s.upBlocked
      && !this.s.downBlocked
      && Math.abs(diff) > NEUTRAL_DIFF
      && (this.s.upProbSeen || this.s.downProbSeen)
    ) {
      this.lockUntilNeutral("[Strategy2] 入场前先进入高概率区，整轮等待回归中性后再开放");
    }

    // 过热区锁定
    if (!this.s.upBlocked && diff >= OVERHEAT_DIFF && upPct >= OVERHEAT_PROB) {
      this.lockUntilNeutral("[Strategy2] 买涨方向进入过热区，整轮等待回归中性后再开放");
    }
    if (!this.s.downBlocked && diff <= -OVERHEAT_DIFF && dnPct >= OVERHEAT_PROB) {
      this.lockUntilNeutral("[Strategy2] 买跌方向进入过热区，整轮等待回归中性后再开放");
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

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    const myPct = direction === "up" ? upPct : dnPct;

    // 止盈阶梯
    if (rem >= TP_LADDER_FLOOR) {
      const span = WINDOW_MAX_REMAINING - TP_LADDER_FLOOR;
      const elapsed = Math.max(0, WINDOW_MAX_REMAINING - Math.max(rem, TP_LADDER_FLOOR));
      const tpThreshold = 90 + Math.floor(elapsed / span * 10);
      const tpCapped = Math.min(tpThreshold, 100);
      if (myPct >= tpCapped) {
        return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥${tpCapped}% rem=${rem}s` };
      }
    }
    // 止损
    if (direction === "up" && diff <= STOP_LOSS_DIFF) {
      return { signal: "sl", reason: `止损 diff=${Math.round(diff)}≤${STOP_LOSS_DIFF}` };
    }
    if (direction === "down" && diff >= -STOP_LOSS_DIFF) {
      return { signal: "sl", reason: `止损 diff=${Math.round(diff)}≥${-STOP_LOSS_DIFF}` };
    }
    return null;
  }

  /** 每个 tick 结束后由外部调用，记录 lastDiff */
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
    };
  }

  private lockUntilNeutral(message: string): void {
    if (!this.s.upBlocked || !this.s.downBlocked) {
      console.log(message);
    }
    this.s.upBlocked = true;
    this.s.downBlocked = true;
  }
}
