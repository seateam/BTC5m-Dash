/**
 * 策略5 · 概率追赶 — diff穿越时概率偏低，等概率追赶diff后止盈
 *
 * 核心逻辑：diff突破阈值的瞬间，如果概率还没跟上（和历史合理概率有偏差），
 * 说明市场反应慢，入场买入等概率追赶。
 *
 * 数据验证：4.5天数据显示，偏差≥10%时92%获利率，30秒内平均追赶16%。
 */

import type {
  IStrategy, StrategyKey, StrategyNumber, StrategyDirection,
  StrategyTickContext, EntrySignal, ExitSignal, StrategyDescription,
} from "./types.js";
import { getFairProb } from "./fair-prob.js";

// ── 入场参数 ────────────────────────────────────────────────
const ENTRY_DIFF = 25;                   // diff 穿越此阈值时检查偏差
const ENTRY_BIAS_MIN = 10;              // 概率偏差至少此百分点才入场（合理概率 - 实际概率 ≥ 10）
const WINDOW_MAX_REMAINING = 240;        // 入场扫描起始：剩余 ≤240s
const WINDOW_MIN_REMAINING = 30;         // 入场扫描截止：剩余 ≤30s

// ── 出场参数 ────────────────────────────────────────────────
const TP_BIAS_CLOSE = 3;                 // 偏差缩小到此值以下止盈（概率已追赶到位）
const SL_BIAS_EXPAND = 1.5;              // 偏差扩大到入场时的此倍数止损（概率反向走）
const MAX_HOLD_SECONDS = 30;             // 最大持仓秒数，超时按当前价平仓
const FORCE_EXIT_REM = 8;               // 窗口结束前强制平仓

interface S5State {
  lastDiff: number | null;
  entryBias: number;       // 入场时的偏差值
  entryTs: number;         // 入场时间戳
}

function createState(): S5State {
  return {
    lastDiff: null,
    entryBias: 0,
    entryTs: 0,
  };
}

export class S5ProbChase implements IStrategy {
  readonly key: StrategyKey = "s5";
  readonly number: StrategyNumber = 5;
  readonly name = "概率追赶";

  private s: S5State = createState();

  getDescription(): StrategyDescription {
    return {
      key: this.key,
      number: this.number,
      name: this.name,
      title: "策略5 · 概率追赶",
      lines: [
        { text: `⏱ 剩余 ${WINDOW_MAX_REMAINING}s~${WINDOW_MIN_REMAINING}s 时检测` },
        { text: `📈 diff穿越±${ENTRY_DIFF}且概率偏差≥${ENTRY_BIAS_MIN}%时入场` },
        { text: "偏差 = 历史合理概率 - 当前概率（概率还没跟上diff）" },
        { text: `止盈：偏差缩小到<${TP_BIAS_CLOSE}%（概率追赶到位）`, color: "#3fb950", marginTop: true },
        { text: `止损：偏差扩大到入场时的${SL_BIAS_EXPAND}倍`, color: "#f85149" },
        { text: `超时：持仓超${MAX_HOLD_SECONDS}秒或剩余<${FORCE_EXIT_REM}秒平仓`, color: "#f85149" },
        { text: "基于 diff+rem 二维映射表判断合理概率", color: "#888", marginTop: true },
      ],
    };
  }

  updateGuards(_ctx: StrategyTickContext): void {}

  checkEntry(ctx: StrategyTickContext): EntrySignal | null {
    const { rem, upPct, dnPct, diff } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;
    if (rem > WINDOW_MAX_REMAINING || rem <= WINDOW_MIN_REMAINING) return null;

    const lastDiff = this.s.lastDiff;
    if (lastDiff == null) return null;

    // 买涨穿越
    if (lastDiff <= ENTRY_DIFF && diff > ENTRY_DIFF) {
      const fair = getFairProb(diff, rem);
      if (fair != null && fair - upPct >= ENTRY_BIAS_MIN) {
        this.s.entryBias = fair - upPct;
        return { direction: "up" };
      }
    }

    // 买跌穿越
    if (lastDiff >= -ENTRY_DIFF && diff < -ENTRY_DIFF) {
      const fair = getFairProb(diff, rem);
      if (fair != null) {
        const fairDn = 100 - fair;
        const bias = fairDn - dnPct;
        if (bias >= ENTRY_BIAS_MIN) {
          this.s.entryBias = bias;
          return { direction: "down" };
        }
      }
    }

    return null;
  }

  onEntryFilled(ctx: StrategyTickContext, _direction: StrategyDirection): void {
    this.s.entryTs = ctx.now;
  }

  checkExit(ctx: StrategyTickContext, direction: StrategyDirection): ExitSignal {
    const { rem, upPct, dnPct, diff, now } = ctx;
    if (upPct == null || dnPct == null || diff == null) return null;

    const myPct = direction === "up" ? upPct : dnPct;
    const fair = getFairProb(diff, rem);

    // 强制平仓
    if (rem <= FORCE_EXIT_REM && rem > 0) {
      return { signal: myPct >= 50 ? "tp" : "sl", reason: `强制平仓 剩余${rem}秒 概率${myPct}%` };
    }

    // 超时平仓
    if (this.s.entryTs > 0 && now - this.s.entryTs > MAX_HOLD_SECONDS * 1000) {
      return { signal: myPct >= 50 ? "tp" : "sl", reason: `超时平仓 持仓${Math.round((now - this.s.entryTs) / 1000)}秒 概率${myPct}%` };
    }

    if (fair == null) return null;

    // 当前偏差
    const currentBias = direction === "up"
      ? fair - upPct
      : (100 - fair) - dnPct;

    // 止盈：偏差缩小到阈值以下（概率追赶到位）
    if (currentBias <= TP_BIAS_CLOSE) {
      return { signal: "tp", reason: `概率追赶到位 偏差${currentBias}%<${TP_BIAS_CLOSE}% 概率${myPct}%` };
    }

    // 止损：偏差扩大到入场时的倍数（概率反向走）
    if (this.s.entryBias > 0 && currentBias >= this.s.entryBias * SL_BIAS_EXPAND) {
      return { signal: "sl", reason: `偏差扩大 ${currentBias}%≥${Math.round(this.s.entryBias * SL_BIAS_EXPAND)}% 入场偏差${this.s.entryBias}%` };
    }

    // 止损：diff反转（趋势完全消失）
    if (direction === "up" && diff <= 0) {
      return { signal: "sl", reason: `diff反转 差价${Math.round(diff)}≤0` };
    }
    if (direction === "down" && diff >= 0) {
      return { signal: "sl", reason: `diff反转 差价${Math.round(diff)}≥0` };
    }

    return null;
  }

  finalizeTick(diff: number | null): void {
    this.s.lastDiff = diff;
  }

  resetState(): void {
    this.s = createState();
  }

  getStatePayload(): Record<string, unknown> {
    return {
      lastDiff: this.s.lastDiff,
      entryBias: this.s.entryBias,
      entryTs: this.s.entryTs,
    };
  }
}
