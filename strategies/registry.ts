/**
 * 策略注册表 — 统一管理所有策略实例
 */

import type { IStrategy, StrategyKey, StrategyDescription } from "./types.js";
import { ALL_STRATEGY_KEYS } from "./types.js";
import { S1Enhanced } from "./s1.js";
import { S2Regular } from "./s2.js";
import { S3Sweep } from "./s3.js";
import { S4Reversal } from "./s4.js";
import { S5ProbChase } from "./s5.js";

const strategies: Map<StrategyKey, IStrategy> = new Map();

export function registerAllStrategies(): void {
  const all: IStrategy[] = [new S1Enhanced(), new S2Regular(), new S3Sweep(), new S4Reversal(), new S5ProbChase()];
  for (const s of all) strategies.set(s.key, s);
}

export function getStrategy(key: StrategyKey): IStrategy | undefined {
  return strategies.get(key);
}

export function getAllStrategies(): IStrategy[] {
  return ALL_STRATEGY_KEYS
    .map((key) => strategies.get(key))
    .filter((s): s is IStrategy => s != null);
}

export function getAllStrategyKeys(): StrategyKey[] {
  return ALL_STRATEGY_KEYS.filter((key) => strategies.has(key));
}

export function getAllDescriptions(): StrategyDescription[] {
  return getAllStrategies().map((s) => s.getDescription());
}

// 初始化
registerAllStrategies();
