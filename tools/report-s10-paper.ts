import { existsSync, readFileSync } from "fs";
import { resolve } from "path";

interface PaperTrade {
  ts: number;
  windowStart: number;
  side: "buy" | "sell";
  direction: "up" | "down";
  amount: number;
  status: string;
  source: string;
  requestedAmount?: number | null;
  filledNotional?: number | null;
  filledShares?: number | null;
  avgPrice?: number | null;
  price?: number | null;
  pnl?: number | null;
  rejectReason?: string | null;
}

interface WindowStats {
  windowStart: number;
  buys: number;
  sells: number;
  rejects: number;
  buyNotional: number;
  sellNotional: number;
  settledPnl: number;
  upBuys: number;
  downBuys: number;
  merges: number;
  epochMergedPnl: number;
  epochPairedShares: number;
}

interface Lot {
  shares: number;
  cost: number;
}

function readJsonArray(path: string): PaperTrade[] {
  if (!existsSync(path)) return [];
  const raw = JSON.parse(readFileSync(path, "utf8"));
  return Array.isArray(raw) ? raw as PaperTrade[] : [];
}

function money(value: number): string {
  const sign = value > 0 ? "+" : "";
  return `${sign}$${value.toFixed(2)}`;
}

function pct(value: number): string {
  return `${value.toFixed(1)}%`;
}

const cwd = process.cwd();
const historyFile = resolve(cwd, ".paper-trade-history.json");
const markerFile = resolve(cwd, "backtest-data", "s10-paper-test-start.json");
const allTrades = readJsonArray(historyFile);
const showAll = process.argv.includes("--all");
let sinceTs = 0;
if (!showAll && existsSync(markerFile)) {
  try {
    const marker = JSON.parse(readFileSync(markerFile, "utf8"));
    if (typeof marker.startTs === "number" && Number.isFinite(marker.startTs)) sinceTs = marker.startTs;
  } catch {
    sinceTs = 0;
  }
}
const s10Trades = allTrades.filter((trade) =>
  /^strategy10/.test(String(trade.source || "")) && Number(trade.ts) >= sinceTs
);
const windows = new Map<number, WindowStats>();

for (const trade of s10Trades) {
  const windowStart = Number(trade.windowStart);
  if (!Number.isFinite(windowStart)) continue;
  const item = windows.get(windowStart) ?? {
    windowStart,
    buys: 0,
    sells: 0,
    rejects: 0,
    buyNotional: 0,
    sellNotional: 0,
    settledPnl: 0,
    upBuys: 0,
    downBuys: 0,
    merges: 0,
    epochMergedPnl: 0,
    epochPairedShares: 0,
  };
  if (String(trade.status || "").includes("REJECT")) item.rejects++;
  if (trade.side === "buy" && String(trade.status || "").includes("FILLED")) {
    item.buys++;
    if (trade.direction === "up") item.upBuys++;
    if (trade.direction === "down") item.downBuys++;
    item.buyNotional += Number(trade.filledNotional ?? trade.requestedAmount ?? 0) || 0;
  }
  if (String(trade.status || "").includes("MERGED")) item.merges++;
  if (trade.side === "sell" || String(trade.status || "").includes("SETTLED")) {
    item.sells++;
    item.sellNotional += Number(trade.filledNotional ?? 0) || 0;
  }
  windows.set(windowStart, item);
}

function consumeLots(lots: Lot[], shares: number): number {
  let remaining = shares;
  let cost = 0;
  while (remaining > 1e-9 && lots.length) {
    const lot = lots[0];
    const take = Math.min(remaining, lot.shares);
    const ratio = take / lot.shares;
    cost += lot.cost * ratio;
    lot.shares -= take;
    lot.cost -= lot.cost * ratio;
    remaining -= take;
    if (lot.shares <= 1e-9) lots.shift();
  }
  return cost;
}

const lotsByWindow = new Map<number, { up: Lot[]; down: Lot[] }>();
for (const trade of [...s10Trades].sort((a, b) => a.ts - b.ts)) {
  const windowStart = Number(trade.windowStart);
  const row = windows.get(windowStart);
  if (!row) continue;
  const lots = lotsByWindow.get(windowStart) ?? { up: [], down: [] };
  lotsByWindow.set(windowStart, lots);
  const status = String(trade.status || "");
  if (trade.side === "buy" && status.includes("FILLED")) {
    const shares = Number(trade.amount || trade.filledShares || 0);
    const cost = Number(trade.filledNotional ?? trade.requestedAmount ?? 0) || 0;
    if (shares > 0 && cost > 0) lots[trade.direction].push({ shares, cost });
    continue;
  }
  if (status.includes("MERGED")) {
    const requestedShares = Number(trade.amount || trade.filledShares || 0);
    const availableUp = lots.up.reduce((sum, lot) => sum + lot.shares, 0);
    const availableDown = lots.down.reduce((sum, lot) => sum + lot.shares, 0);
    const pairedShares = Math.min(requestedShares, availableUp, availableDown);
    if (pairedShares <= 1e-9) continue;
    const upCost = consumeLots(lots.up, pairedShares);
    const downCost = consumeLots(lots.down, pairedShares);
    const pnl = pairedShares - upCost - downCost;
    row.epochPairedShares += pairedShares;
    row.epochMergedPnl += pnl;
    row.settledPnl += pnl;
    continue;
  }
  if (status.includes("SETTLED")) {
    const shares = Number(trade.amount || trade.filledShares || 0);
    if (shares <= 0) continue;
    const dirLots = lots[trade.direction];
    const available = dirLots.reduce((sum, lot) => sum + lot.shares, 0);
    const closeShares = Math.min(shares, available);
    if (closeShares <= 1e-9) continue;
    const cost = consumeLots(dirLots, closeShares);
    const proceeds = (Number(trade.price) || 0) * closeShares;
    row.settledPnl += proceeds - cost;
  }
}

const windowRows = [...windows.values()].sort((a, b) => a.windowStart - b.windowStart);
const filledWindows = windowRows.filter((row) => row.buys > 0).length;
const rejectedWindows = windowRows.filter((row) => row.rejects > 0 && row.buys === 0).length;
const settledWindows = windowRows.filter((row) => row.sells > 0).length;
const dualWindows = windowRows.filter((row) => row.upBuys > 0 && row.downBuys > 0).length;
const mergedWindows = windowRows.filter((row) => row.merges > 0).length;
const buyCount = windowRows.reduce((sum, row) => sum + row.buys, 0);
const rejectCount = windowRows.reduce((sum, row) => sum + row.rejects, 0);
const buyNotional = windowRows.reduce((sum, row) => sum + row.buyNotional, 0);
const settledPnl = windowRows.reduce((sum, row) => sum + row.settledPnl, 0);
const winRows = windowRows.filter((row) => row.settledPnl > 0);
const lossRows = windowRows.filter((row) => row.settledPnl < 0);
const coverage = windowRows.length ? (filledWindows / windowRows.length) * 100 : 0;
const fillRate = buyCount + rejectCount > 0 ? (buyCount / (buyCount + rejectCount)) * 100 : 0;
const roi = buyNotional > 0 ? (settledPnl / buyNotional) * 100 : 0;

console.log("S10 paper report");
console.log(`history: ${historyFile}`);
if (sinceTs > 0) console.log(`since: ${new Date(sinceTs).toLocaleString()} (${sinceTs})`);
console.log(`windows: ${windowRows.length}, filled windows: ${filledWindows}, rejected-only windows: ${rejectedWindows}`);
console.log(`coverage: ${pct(coverage)}, dual windows: ${dualWindows}, merged windows: ${mergedWindows}`);
console.log(`fills: ${buyCount}, rejects: ${rejectCount}, fill rate: ${pct(fillRate)}`);
console.log(`buy notional: ${money(buyNotional)}, settled windows: ${settledWindows}, settled pnl: ${money(settledPnl)}, ROI: ${pct(roi)}`);
console.log(`wins/losses: ${winRows.length}/${lossRows.length}`);
console.log("");
console.log("recent windows:");
for (const row of windowRows.slice(-12).reverse()) {
  const time = new Date(row.windowStart * 1000).toLocaleString();
  console.log(`${time} buy=${row.buys} up=${row.upBuys} down=${row.downBuys} merge=${row.merges} paired=${row.epochPairedShares.toFixed(2)} reject=${row.rejects} notional=${money(row.buyNotional)} pnl=${money(row.settledPnl)}`);
}
