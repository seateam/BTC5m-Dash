/**
 * Polymarket 真实盈亏模块
 *
 * 数据来源：
 *   - /trades       所有 CLOB 买卖成交
 *   - /activity     REDEEM 类型 = Claim 到账
 *   - /positions    当前未处理的持仓（未卖 / 未 claim）
 *
 * 设计：
 *   - 启动时全量加载（分页到底）
 *   - 增量同步用 lastSyncTs 过滤，只拉新的
 *   - 每 5 分钟兜底一次全量
 *   - 按 (conditionId, outcome) 配对算每个仓位的完整盈亏
 */

import { readFileSync, writeFileSync, existsSync } from "fs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const __dirname = dirname(fileURLToPath(import.meta.url));

// ── 常量 ────────────────────────────────────────────────────
const API_BASE = "https://data-api.polymarket.com";
const API_HEADERS = { "User-Agent": "Mozilla/5.0" };
const PAGE_SIZE = 100;
const REQUEST_TIMEOUT_MS = 15000;
const CRYPTO_FEE_RATE = 0.072;

// ── 类型 ────────────────────────────────────────────────────
export interface PmTrade {
  proxyWallet: string;
  side: "BUY" | "SELL";
  asset: string;
  conditionId: string;
  size: number;
  price: number;
  timestamp: number;       // Unix 秒
  outcome: string;         // "Up" / "Down"
  outcomeIndex: number;
  title: string;
  slug: string;
  eventSlug: string;
  transactionHash: string;
}

export interface PmRedeem {
  proxyWallet: string;
  conditionId: string;
  timestamp: number;       // Unix 秒
  size: number;
  usdcSize: number;        // Claim 到账金额
  transactionHash: string;
  title: string;
  slug: string;
  eventSlug: string;
}

export interface PmPosition {
  proxyWallet: string;
  conditionId: string;
  asset: string;
  size: number;
  avgPrice: number;
  initialValue: number;
  currentValue: number;
  cashPnl: number;
  realizedPnl: number;
  redeemable: boolean;
  outcome: string;
  outcomeIndex: number;
  title: string;
  endDate: string;
}

export interface PositionSummary {
  conditionId: string;
  outcome: string;
  outcomeIndex: number;
  title: string;
  slug: string;
  windowStart: number;          // 从 slug 解析
  firstTs: number;              // 首笔时间（秒）
  lastTs: number;               // 最后一笔时间（秒）
  buys: PmTrade[];
  sells: PmTrade[];
  redeems: PmRedeem[];
  buyCost: number;              // 买入总支出（不含费）
  sellRevenue: number;          // 卖出总收入（不含费）
  redeemRevenue: number;        // Claim 总回款
  totalFee: number;             // 总手续费
  netPnl: number;               // 真实净盈亏 = 卖+Claim-买-费
  status: "claimed" | "sold" | "pending" | "settled_lost";
  strategySource?: string;      // 本地 .strategy-sources.json 里的来源
  // 未决仓位的额外信息（来自 /positions）
  currentValue?: number;
  currentRedeemable?: boolean;
}

/** 展平后的单笔行（每笔 BUY/SELL/REDEEM/LOST 一条） */
export interface PnlEvent {
  ts: number;                   // 秒
  kind: "BUY" | "SELL" | "REDEEM" | "LOST";   // LOST = 结算归零（虚拟事件）
  outcome: string;              // Up / Down
  outcomeIndex: number;
  conditionId: string;
  title: string;
  size: number;
  price: number;                // REDEEM 时 = 1（兑付按 1 USDC/份）
  cost: number;                 // BUY=支出, SELL=收入, REDEEM=到账
  fee: number;                  // BUY/SELL 手续费, REDEEM=0
  netAmount: number;            // 现金净变化（出=负, 入=正, 含费）
  transactionHash: string;
  strategySource?: string;
  positionPnl?: number;         // 该笔所属仓位的累计盈亏（SELL/REDEEM 时填，BUY 时为空）
  positionStatus?: "claimed" | "sold" | "pending" | "settled_lost";
}

// ── 网络工具 ─────────────────────────────────────────────────
async function fetchJson<T>(url: string): Promise<T> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
  try {
    const res = await fetch(url, { headers: API_HEADERS, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return (await res.json()) as T;
  } finally {
    clearTimeout(timer);
  }
}

async function fetchPaged<T>(path: string, extraQs: string = ""): Promise<T[]> {
  const items: T[] = [];
  let offset = 0;
  while (true) {
    const qs = `limit=${PAGE_SIZE}&offset=${offset}${extraQs ? "&" + extraQs : ""}`;
    const url = `${API_BASE}/${path}${path.includes("?") ? "&" : "?"}${qs}`;
    const batch = await fetchJson<T[]>(url);
    if (!Array.isArray(batch) || batch.length === 0) break;
    items.push(...batch);
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  return items;
}

// ── API 封装 ─────────────────────────────────────────────────
export async function fetchAllTrades(proxy: string): Promise<PmTrade[]> {
  return fetchPaged<PmTrade>(`trades?user=${proxy}`);
}

export async function fetchAllRedeems(proxy: string): Promise<PmRedeem[]> {
  // Polymarket 会返回大量 size=0 的空 redeem 记录（同一条 tx 的多方向拆分噪声），过滤掉
  const all = await fetchPaged<PmRedeem>(`activity?user=${proxy}&type=REDEEM`);
  return all.filter(r => (r.usdcSize > 0) || (r.size > 0));
}

export async function fetchAllPositions(proxy: string): Promise<PmPosition[]> {
  return fetchPaged<PmPosition>(`positions?user=${proxy}`);
}

/** 增量拉取：只要 timestamp > sinceSec 的数据 */
export async function fetchTradesSince(proxy: string, sinceSec: number): Promise<PmTrade[]> {
  // Polymarket API 按时间倒序返回。拉第一页，如果最后一条仍 > sinceSec，继续下一页
  const collected: PmTrade[] = [];
  let offset = 0;
  while (true) {
    const url = `${API_BASE}/trades?user=${proxy}&limit=${PAGE_SIZE}&offset=${offset}`;
    const batch = await fetchJson<PmTrade[]>(url);
    if (!Array.isArray(batch) || batch.length === 0) break;
    const fresh = batch.filter(t => t.timestamp > sinceSec);
    collected.push(...fresh);
    if (fresh.length < batch.length) break;  // 有旧数据出现，停止翻页
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  return collected;
}

export async function fetchRedeemsSince(proxy: string, sinceSec: number): Promise<PmRedeem[]> {
  const collected: PmRedeem[] = [];
  let offset = 0;
  while (true) {
    const url = `${API_BASE}/activity?user=${proxy}&type=REDEEM&limit=${PAGE_SIZE}&offset=${offset}`;
    const batch = await fetchJson<PmRedeem[]>(url);
    if (!Array.isArray(batch) || batch.length === 0) break;
    const fresh = batch.filter(r => r.timestamp > sinceSec);
    collected.push(...fresh);
    if (fresh.length < batch.length) break;
    if (batch.length < PAGE_SIZE) break;
    offset += PAGE_SIZE;
  }
  return collected.filter(r => (r.usdcSize > 0) || (r.size > 0));
}

// ── 手续费公式 ───────────────────────────────────────────────
function feeOf(side: "BUY" | "SELL", size: number, price: number): number {
  return side === "BUY"
    ? size * CRYPTO_FEE_RATE * (1 - price)
    : size * CRYPTO_FEE_RATE * price;
}

// ── 仓位配对 ─────────────────────────────────────────────────
/**
 * 把 trades + redeems 按 (conditionId, outcome) 分组，每组算完整盈亏
 *
 * 注意：redeem 事件不含 outcome 信息，通过 conditionId 归属到这个市场。
 * 如果你在同一个 conditionId 下同时买了 Up 和 Down（罕见），redeem
 * 会归属到所有出现过的 outcome（只会有一边赢，另一边 usdcSize=0 不影响）。
 */
export function summarizePositions(
  trades: PmTrade[],
  redeems: PmRedeem[],
  positions: PmPosition[],
  strategySources: Map<string, string>,
): PositionSummary[] {
  type Key = string;
  const mk = (c: string, o: string): Key => `${c}::${o}`;
  const groups = new Map<Key, PositionSummary>();

  // 1. 先按 (conditionId, outcome) 分组所有 trades
  for (const t of trades) {
    const k = mk(t.conditionId, t.outcome);
    let g = groups.get(k);
    if (!g) {
      const ws = parseWindowStartFromSlug(t.slug);
      g = {
        conditionId: t.conditionId,
        outcome: t.outcome,
        outcomeIndex: t.outcomeIndex,
        title: t.title,
        slug: t.slug,
        windowStart: ws,
        firstTs: t.timestamp,
        lastTs: t.timestamp,
        buys: [],
        sells: [],
        redeems: [],
        buyCost: 0, sellRevenue: 0, redeemRevenue: 0, totalFee: 0, netPnl: 0,
        status: "pending",
      };
      groups.set(k, g);
    }
    if (t.side === "BUY") g.buys.push(t);
    else g.sells.push(t);
    g.firstTs = Math.min(g.firstTs, t.timestamp);
    g.lastTs = Math.max(g.lastTs, t.timestamp);
  }

  // 2. redeem 按 conditionId 归属（一个 conditionId 下可能有多个 outcome 组）
  const redeemsByCond = new Map<string, PmRedeem[]>();
  for (const r of redeems) {
    const arr = redeemsByCond.get(r.conditionId) ?? [];
    arr.push(r);
    redeemsByCond.set(r.conditionId, arr);
  }

  // 3. 计算每组的盈亏
  for (const g of groups.values()) {
    const rs = redeemsByCond.get(g.conditionId) ?? [];
    // 同 conditionId 的 redeem 全挂到这里（赢的那一边）
    g.redeems = rs;
    if (rs.length) g.lastTs = Math.max(g.lastTs, ...rs.map(r => r.timestamp));

    g.buyCost = g.buys.reduce((s, b) => s + b.size * b.price, 0);
    g.sellRevenue = g.sells.reduce((s, x) => s + x.size * x.price, 0);
    g.redeemRevenue = rs.reduce((s, r) => s + r.usdcSize, 0);
    g.totalFee =
      g.buys.reduce((s, b) => s + feeOf("BUY", b.size, b.price), 0) +
      g.sells.reduce((s, x) => s + feeOf("SELL", x.size, x.price), 0);

    g.netPnl = g.sellRevenue + g.redeemRevenue - g.buyCost - g.totalFee;

    // 状态判断
    if (rs.length > 0) g.status = "claimed";
    else if (g.sells.length > 0) g.status = "sold";
    else g.status = "pending";

    // 策略来源：用第一笔 buy 的 txHash 查
    if (g.buys.length) {
      const src = strategySources.get(g.buys[0].transactionHash.toLowerCase());
      if (src) g.strategySource = src;
    }
  }

  // 4. 未决仓位信息：从 /positions 补
  for (const p of positions) {
    const k = mk(p.conditionId, p.outcome);
    const g = groups.get(k);
    if (!g) continue;
    g.currentValue = p.currentValue;
    g.currentRedeemable = p.redeemable;
    // 已结算但归零的，从 pending 升级为 settled_lost
    if (g.status === "pending" && p.redeemable && p.currentValue === 0) {
      g.status = "settled_lost";
      // 这种情况 cashPnl 就是 -initialValue（仓位价值归零）
      // 已体现在 g.netPnl 里（sell=0, redeem=0, buyCost - fee 就是亏）
    }
  }

  // 5. 按最近时间倒序返回
  return [...groups.values()].sort((a, b) => b.lastTs - a.lastTs);
}

function parseWindowStartFromSlug(slug: string): number {
  // slug 格式 "btc-updown-5m-1776762000"
  const m = slug.match(/(\d{10,})$/);
  return m ? parseInt(m[1], 10) : 0;
}

// ── 策略来源映射（本地持久化）────────────────────────────────
const STRATEGY_SOURCES_FILE = resolve(__dirname, ".strategy-sources.json");

export function loadStrategySources(): Map<string, string> {
  try {
    if (!existsSync(STRATEGY_SOURCES_FILE)) return new Map();
    const data = JSON.parse(readFileSync(STRATEGY_SOURCES_FILE, "utf-8"));
    if (typeof data !== "object" || data == null) return new Map();
    return new Map(Object.entries(data as Record<string, string>).map(([k, v]) => [k.toLowerCase(), v]));
  } catch (err) {
    console.warn(`[PmPnl] 加载 strategy-sources 失败: ${err instanceof Error ? err.message : String(err)}`);
    return new Map();
  }
}

export function saveStrategySources(map: Map<string, string>): void {
  try {
    const obj: Record<string, string> = {};
    for (const [k, v] of map) obj[k] = v;
    writeFileSync(STRATEGY_SOURCES_FILE, JSON.stringify(obj, null, 2) + "\n", "utf-8");
  } catch (err) {
    console.warn(`[PmPnl] 保存 strategy-sources 失败: ${err instanceof Error ? err.message : String(err)}`);
  }
}

// ── 管理器：状态 + 同步 ──────────────────────────────────────
export class PmPnlManager {
  private trades: PmTrade[] = [];
  private redeems: PmRedeem[] = [];
  private positions: PmPosition[] = [];
  private strategySources: Map<string, string> = loadStrategySources();
  private lastSyncTs = 0;      // 秒
  private syncing = false;
  private initialized = false;

  constructor(private proxy: string) {}

  /** 记录一笔交易的策略来源（txHash → source） */
  recordStrategySource(txHash: string, source: string): void {
    if (!txHash) return;
    this.strategySources.set(txHash.toLowerCase(), source);
    saveStrategySources(this.strategySources);
  }

  /** 启动时全量加载 */
  async init(): Promise<void> {
    if (this.initialized) return;
    if (!this.proxy) { this.initialized = true; return; }
    try {
      const [trades, redeems, positions] = await Promise.all([
        fetchAllTrades(this.proxy).catch(() => [] as PmTrade[]),
        fetchAllRedeems(this.proxy).catch(() => [] as PmRedeem[]),
        fetchAllPositions(this.proxy).catch(() => [] as PmPosition[]),
      ]);
      this.trades = trades;
      this.redeems = redeems;
      this.positions = positions;
      // 记录最新 timestamp（秒）
      const maxTrade = trades.length ? Math.max(...trades.map(t => t.timestamp)) : 0;
      const maxRedeem = redeems.length ? Math.max(...redeems.map(r => r.timestamp)) : 0;
      this.lastSyncTs = Math.max(maxTrade, maxRedeem);
      this.initialized = true;
      console.log(`[PmPnl] 初始化完成: ${trades.length} trades, ${redeems.length} redeems, ${positions.length} positions, lastSyncTs=${this.lastSyncTs}`);
    } catch (err) {
      console.warn(`[PmPnl] 初始化失败: ${err instanceof Error ? err.message : String(err)}`);
      this.initialized = true;  // 不阻塞服务，后续同步会重试
    }
  }

  /** 增量同步（自 lastSyncTs 之后） */
  async syncIncremental(): Promise<void> {
    if (!this.proxy || this.syncing) return;
    this.syncing = true;
    try {
      const [newTrades, newRedeems] = await Promise.all([
        fetchTradesSince(this.proxy, this.lastSyncTs).catch(() => [] as PmTrade[]),
        fetchRedeemsSince(this.proxy, this.lastSyncTs).catch(() => [] as PmRedeem[]),
      ]);
      if (newTrades.length) {
        // 去重（timestamp+txHash 唯一）
        const seen = new Set(this.trades.map(t => t.transactionHash + ":" + t.side));
        for (const t of newTrades) {
          if (!seen.has(t.transactionHash + ":" + t.side)) this.trades.push(t);
        }
      }
      if (newRedeems.length) {
        const seen = new Set(this.redeems.map(r => r.transactionHash));
        for (const r of newRedeems) {
          if (!seen.has(r.transactionHash)) this.redeems.push(r);
        }
      }
      const maxTrade = this.trades.length ? Math.max(...this.trades.map(t => t.timestamp)) : this.lastSyncTs;
      const maxRedeem = this.redeems.length ? Math.max(...this.redeems.map(r => r.timestamp)) : this.lastSyncTs;
      this.lastSyncTs = Math.max(maxTrade, maxRedeem);
      if (newTrades.length || newRedeems.length) {
        console.log(`[PmPnl] 增量: +${newTrades.length} trades, +${newRedeems.length} redeems`);
      }
    } catch (err) {
      console.warn(`[PmPnl] 增量同步失败: ${err instanceof Error ? err.message : String(err)}`);
    } finally {
      this.syncing = false;
    }
  }

  /** 兜底：重新拉 positions（用于检测结算归零） */
  async syncPositions(): Promise<void> {
    if (!this.proxy) return;
    try {
      this.positions = await fetchAllPositions(this.proxy);
    } catch (err) {
      console.warn(`[PmPnl] positions 同步失败: ${err instanceof Error ? err.message : String(err)}`);
    }
  }

  /** 返回按仓位聚合后的快照 */
  getSummaries(limit?: number): PositionSummary[] {
    const all = summarizePositions(this.trades, this.redeems, this.positions, this.strategySources);
    return limit ? all.slice(0, limit) : all;
  }

  /** 返回展平的每笔事件，按时间倒序。默认只返回近 7 天。 */
  getEvents(opts?: { limit?: number; sinceDays?: number }): PnlEvent[] {
    const sinceDays = opts?.sinceDays ?? 7;
    const limit = opts?.limit;
    const nowSec = Math.floor(Date.now() / 1000);
    const sinceSec = sinceDays > 0 ? nowSec - sinceDays * 86400 : 0;
    const summaries = summarizePositions(this.trades, this.redeems, this.positions, this.strategySources);
    // 每个仓位分拆：BUY + SELL + REDEEM 各自成行，附带仓位信息
    const events: PnlEvent[] = [];
    for (const s of summaries) {
      for (const b of s.buys) {
        const fee = b.size * CRYPTO_FEE_RATE * (1 - b.price);
        const cost = b.size * b.price;
        events.push({
          ts: b.timestamp,
          kind: "BUY",
          outcome: b.outcome,
          outcomeIndex: b.outcomeIndex,
          conditionId: b.conditionId,
          title: b.title,
          size: b.size,
          price: b.price,
          cost, fee,
          netAmount: -(cost + fee),
          transactionHash: b.transactionHash,
          strategySource: s.strategySource,
        });
      }
      for (const x of s.sells) {
        const fee = x.size * CRYPTO_FEE_RATE * x.price;
        const revenue = x.size * x.price;
        events.push({
          ts: x.timestamp,
          kind: "SELL",
          outcome: x.outcome,
          outcomeIndex: x.outcomeIndex,
          conditionId: x.conditionId,
          title: x.title,
          size: x.size,
          price: x.price,
          cost: revenue, fee,
          netAmount: revenue - fee,
          transactionHash: x.transactionHash,
          strategySource: s.strategySource,
          positionPnl: s.netPnl,
          positionStatus: s.status,
        });
      }
      for (const r of s.redeems) {
        events.push({
          ts: r.timestamp,
          kind: "REDEEM",
          outcome: s.outcome,
          outcomeIndex: s.outcomeIndex,
          conditionId: r.conditionId,
          title: r.title,
          size: r.size,
          price: 1,
          cost: r.usdcSize, fee: 0,
          netAmount: r.usdcSize,
          transactionHash: r.transactionHash,
          strategySource: s.strategySource,
          positionPnl: s.netPnl,
          positionStatus: s.status,
        });
      }

      // 虚拟"结算归零"事件：BUY 存在 + 没 SELL 没 REDEEM + 窗口已过结算时间
      // windowStart 从 slug 解析，结算时间 = windowStart + 300 秒
      if (s.buys.length > 0 && s.sells.length === 0 && s.redeems.length === 0 && s.windowStart > 0) {
        const settleTs = s.windowStart + 300;
        if (nowSec >= settleTs) {
          // 合成一条 LOST 行
          const totalSize = s.buys.reduce((sum, b) => sum + b.size, 0);
          events.push({
            ts: settleTs,
            kind: "LOST",
            outcome: s.outcome,
            outcomeIndex: s.outcomeIndex,
            conditionId: s.conditionId,
            title: s.title,
            size: totalSize,
            price: 0,
            cost: 0, fee: 0,
            netAmount: 0,              // 归零不产生现金流（钱在买入时就已付出）
            transactionHash: s.buys[0].transactionHash,
            strategySource: s.strategySource,
            positionPnl: s.netPnl,     // 此仓位真实盈亏 = -买入成本 - 手续费
            positionStatus: "settled_lost",
          });
        }
      }
    }
    const filtered = sinceSec > 0 ? events.filter(e => e.ts >= sinceSec) : events;
    filtered.sort((a, b) => b.ts - a.ts);
    return limit ? filtered.slice(0, limit) : filtered;
  }

  /** 总盈亏（默认仅近 7 天；不传 sinceDays=0 为全部） */
  getTotalPnl(sinceDays: number = 7): { totalBuy: number; totalSell: number; totalRedeem: number; totalFee: number; netPnl: number; positionCount: number } {
    const sinceSec = sinceDays > 0 ? Math.floor(Date.now() / 1000) - sinceDays * 86400 : 0;
    let totalBuy = 0, totalSell = 0, totalRedeem = 0, totalFee = 0;
    let count = 0;
    for (const t of this.trades) {
      if (sinceSec > 0 && t.timestamp < sinceSec) continue;
      if (t.side === "BUY") totalBuy += t.size * t.price;
      else totalSell += t.size * t.price;
      totalFee += feeOf(t.side, t.size, t.price);
      count++;
    }
    for (const r of this.redeems) {
      if (sinceSec > 0 && r.timestamp < sinceSec) continue;
      totalRedeem += r.usdcSize;
    }
    return {
      totalBuy, totalSell, totalRedeem, totalFee,
      netPnl: totalSell + totalRedeem - totalBuy - totalFee,
      positionCount: count,
    };
  }

  isInitialized(): boolean {
    return this.initialized;
  }
}
