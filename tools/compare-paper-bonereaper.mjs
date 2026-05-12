import { readFileSync } from "fs";
import { resolve } from "path";

const BR_FILE = resolve("backtest-data", "bonereaper-btc5m-monitor.json");
const PAPER_FILE = resolve(".paper-trade-history.json");

function n(value) {
  const out = Number(value);
  return Number.isFinite(out) ? out : 0;
}

function safeDiv(a, b) {
  return b > 0 ? a / b : 0;
}

function round(value, digits = 2) {
  const scale = 10 ** digits;
  return Math.round(n(value) * scale) / scale;
}

function sideOfTrade(trade) {
  const raw = String(trade.outcome ?? trade.direction ?? "").toLowerCase();
  if (raw === "up" || raw.includes("涨")) return "up";
  if (raw === "down" || raw.includes("跌")) return "down";
  return null;
}

function emptyAgg(windowStart) {
  return {
    windowStart,
    upShares: 0,
    downShares: 0,
    upNotional: 0,
    downNotional: 0,
    count: 0,
  };
}

function addTrade(agg, side, shares, notional) {
  if (side === "up") {
    agg.upShares += shares;
    agg.upNotional += notional;
  } else if (side === "down") {
    agg.downShares += shares;
    agg.downNotional += notional;
  }
  agg.count += 1;
}

function finalize(agg) {
  const totalNotional = agg.upNotional + agg.downNotional;
  const totalShares = agg.upShares + agg.downShares;
  return {
    ...agg,
    totalNotional,
    totalShares,
    upAvg: safeDiv(agg.upNotional, agg.upShares),
    downAvg: safeDiv(agg.downNotional, agg.downShares),
    upNotionalShare: safeDiv(agg.upNotional, totalNotional),
    upShareRatio: safeDiv(agg.upShares, totalShares),
  };
}

const brRaw = JSON.parse(readFileSync(BR_FILE, "utf8"));
const paperRaw = JSON.parse(readFileSync(PAPER_FILE, "utf8"));

const brByWindow = new Map();
for (const window of brRaw.windows || []) {
  const agg = emptyAgg(Number(window.windowStart));
  for (const trade of window.activities || []) {
    if (String(trade.side).toUpperCase() !== "BUY") continue;
    const side = sideOfTrade(trade);
    if (!side) continue;
    const shares = n(trade.size);
    const notional = n(trade.usdcSize) || shares * n(trade.price);
    if (!(shares > 0) || !(notional > 0)) continue;
    addTrade(agg, side, shares, notional);
  }
  brByWindow.set(agg.windowStart, finalize(agg));
}

const paperByWindow = new Map();
for (const trade of paperRaw || []) {
  const source = String(trade.source || "");
  const reason = String(trade.exitReason || "");
  if (
    !/^strategy10(?:maker|bonereaper)/.test(source) &&
    !reason.includes("br-clone")
  ) {
    continue;
  }
  if (String(trade.side || "").toLowerCase() !== "buy") continue;
  if (!String(trade.status || "").includes("FILLED")) continue;
  const side = sideOfTrade(trade);
  if (!side) continue;
  const windowStart = Number(trade.windowStart);
  if (!Number.isFinite(windowStart)) continue;
  const agg = paperByWindow.get(windowStart) || emptyAgg(windowStart);
  const shares = n(trade.filledShares ?? trade.amount);
  const notional = n(trade.filledNotional ?? trade.requestedAmount) || shares * n(trade.price);
  if (!(shares > 0) || !(notional > 0)) continue;
  addTrade(agg, side, shares, notional);
  paperByWindow.set(windowStart, agg);
}

const rows = [];
for (const [windowStart, br] of brByWindow) {
  const paper = paperByWindow.get(windowStart);
  if (!paper) continue;
  const p = finalize(paper);
  if (!(br.totalNotional > 0) || !(p.totalNotional > 0)) continue;
  rows.push({
    windowStart,
    brCount: br.count,
    pCount: p.count,
    brTotal: br.totalNotional,
    pTotal: p.totalNotional,
    totalScalePct: safeDiv(p.totalNotional, br.totalNotional) * 100,
    brUpNotSharePct: br.upNotionalShare * 100,
    pUpNotSharePct: p.upNotionalShare * 100,
    upNotShareErrPct: Math.abs(p.upNotionalShare - br.upNotionalShare) * 100,
    brUpAvg: br.upAvg,
    pUpAvg: p.upAvg,
    upAvgErr: Math.abs(p.upAvg - br.upAvg),
    brDownAvg: br.downAvg,
    pDownAvg: p.downAvg,
    downAvgErr: Math.abs(p.downAvg - br.downAvg),
    brUpShares: br.upShares,
    pUpShares: p.upShares,
    brDownShares: br.downShares,
    pDownShares: p.downShares,
  });
}

rows.sort((a, b) => a.windowStart - b.windowStart);
const recent = rows.slice(-12).map((row) => ({
  window: row.windowStart,
  brCount: row.brCount,
  pCount: row.pCount,
  brTotal: round(row.brTotal, 2),
  pTotal: round(row.pTotal, 2),
  scalePct: round(row.totalScalePct, 2),
  brUpPct: round(row.brUpNotSharePct, 1),
  pUpPct: round(row.pUpNotSharePct, 1),
  upPctErr: round(row.upNotShareErrPct, 1),
  brUpAvg: round(row.brUpAvg, 3),
  pUpAvg: round(row.pUpAvg, 3),
  brDnAvg: round(row.brDownAvg, 3),
  pDnAvg: round(row.pDownAvg, 3),
  upSh: `${round(row.pUpShares, 1)}/${round(row.brUpShares, 1)}`,
  dnSh: `${round(row.pDownShares, 1)}/${round(row.brDownShares, 1)}`,
}));

const usable = rows.filter((row) => row.pTotal > 0 && row.brTotal > 0);
const mean = (values) =>
  values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;

console.log(
  JSON.stringify(
    {
      windows: usable.length,
      metrics: {
        avgScalePct: round(mean(usable.map((row) => row.totalScalePct)), 2),
        avgUpNotShareErrPct: round(mean(usable.map((row) => row.upNotShareErrPct)), 2),
        avgUpAvgErr: round(mean(usable.map((row) => row.upAvgErr)), 4),
        avgDownAvgErr: round(mean(usable.map((row) => row.downAvgErr)), 4),
      },
      recent,
    },
    null,
    2,
  ),
);
