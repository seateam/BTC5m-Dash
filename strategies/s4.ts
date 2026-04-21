/**
 * 策略4 · 反转抓取 — 窗口末尾概率反转
 */

import type {
  IStrategy, StrategyKey, StrategyNumber, StrategyDirection,
  StrategyTickContext, EntrySignal, ExitSignal, StrategyDescription,
} from "./types.js";

const WINDOW_MAX_REMAINING = 5;
const WINDOW_MIN_REMAINING = 1;
const ENTRY_UP_FROM = 30;        // 涨概率从 <此值
const ENTRY_UP_TO = 60;          // 涨至 >此值 触发买涨
const ENTRY_DN_FROM = 70;        // 涨概率从 >此值
const ENTRY_DN_TO = 40;          // 跌至 <此值 触发买跌
const TP_PROB = 85;              // 止盈概率
const SL_PROB = 50;              // 止损概率

export class S4Reversal implements IStrategy {
  readonly key: StrategyKey = "s4";
  readonly number: StrategyNumber = 4;
  readonly name = "反转";

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略4 · 反转抓取",
      lines: [
        { text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~${WINDOW_MIN_REMAINING}s 时检测` },
        { text: `📈 买涨：涨概率从 <${ENTRY_UP_FROM}% 涨至 >${ENTRY_UP_TO}%` },
        { text: `📉 买跌：涨概率从 >${ENTRY_DN_FROM}% 跌至 <${ENTRY_DN_TO}%` },
        { text: `✓ 止盈：概率 >=${TP_PROB}%`, color: "#3fb950", marginTop: true },
        { text: `✗ 止损：概率 <=${SL_PROB}%`, color: "#f85149" },
      ],
    };
  }

  updateGuards(_ctx: StrategyTickContext): void {}

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    const { rem, upPct, prevUpPct } = ctx;
    if (upPct == null || prevUpPct == null) return null;
    if (rem > WINDOW_MAX_REMAINING || rem < WINDOW_MIN_REMAINING) return null;

    if (prevUpPct < ENTRY_UP_FROM && upPct > ENTRY_UP_TO) return { direction: "up" };
    if (prevUpPct > ENTRY_DN_FROM && upPct < ENTRY_DN_TO) return { direction: "down" };
    return null;
  }

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { upPct, dnPct } = ctx;
    if (upPct == null || dnPct == null) return null;
    const myPct = direction === "up" ? upPct : dnPct;

    if (myPct >= TP_PROB) return { signal: "tp", reason: `止盈 概率${myPct}%≥${TP_PROB}%` };
    if (myPct <= SL_PROB) return { signal: "sl", reason: `止损 概率${myPct}%≤${SL_PROB}%` };
    return null;
  }

  resetState(): void {}

  getStatePayload(): Record<string, unknown> {
    return {};
  }
}
