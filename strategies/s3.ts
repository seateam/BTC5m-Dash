/**
 * 策略3 · 扫尾 — 窗口尾部大差价入场
 */

import type {
  IStrategy, StrategyKey, StrategyNumber, StrategyDirection,
  StrategyTickContext, EntrySignal, ExitSignal, StrategyDescription,
} from "./types.js";

const WINDOW_MAX_REMAINING = 60;
const ENTRY_DIFF = 50;
const ENTRY_PROB_CAP = 95;
const STOP_LOSS_DIFF = 5;

export class S3Sweep implements IStrategy {
  readonly key: StrategyKey = "s3";
  readonly number: StrategyNumber = 3;
  readonly name = "扫尾";

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略3 · 扫尾",
      lines: [
        { text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~0s 时检测` },
        { text: `📈 买涨：差价 >+${ENTRY_DIFF} 且 涨概率 <${ENTRY_PROB_CAP}%` },
        { text: `📉 买跌：差价 <-${ENTRY_DIFF} 且 跌概率 <${ENTRY_PROB_CAP}%` },
        { text: "止盈：>40s ≥98% / >20s ≥99% / >10s ≥100% / <10s 持仓到结束", color: "#3fb950", marginTop: true },
        { text: `止损：差价 <=${STOP_LOSS_DIFF}`, color: "#f85149" },
      ],
    };
  }

  updateGuards(_ctx: StrategyTickContext): void {}

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    if (rem > WINDOW_MAX_REMAINING || rem <= 0) return null;

    if (diff > ENTRY_DIFF && upPct < ENTRY_PROB_CAP) return { direction: "up" };
    if (diff < -ENTRY_DIFF && dnPct < ENTRY_PROB_CAP) return { direction: "down" };
    return null;
  }

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    const myPct = direction === "up" ? upPct : dnPct;

    if (rem >= 40 && myPct >= 98) return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥98% rem=${rem}s` };
    if (rem >= 20 && rem < 40 && myPct >= 99) return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥99% rem=${rem}s` };
    if (rem >= 10 && rem < 20 && myPct >= 100) return { signal: "tp", reason: `阶梯止盈 概率${myPct}%≥100% rem=${rem}s` };

    if (direction === "up" && diff <= STOP_LOSS_DIFF) {
      return { signal: "sl", reason: `止损 diff=${Math.round(diff)}≤${STOP_LOSS_DIFF}` };
    }
    if (direction === "down" && diff >= -STOP_LOSS_DIFF) {
      return { signal: "sl", reason: `止损 diff=${Math.round(diff)}≥${-STOP_LOSS_DIFF}` };
    }
    return null;
  }

  resetState(): void {}

  getStatePayload(): Record<string, unknown> {
    return {};
  }
}
