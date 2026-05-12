import { readFileSync } from "fs";
import { resolve } from "path";

const DATA_FILE = resolve("backtest-data", "bonereaper-btc5m-monitor.json");
const CHECKPOINT_REMS = [295, 285, 270, 240, 210, 180, 150, 120, 90, 60, 30, 15, 5, 0];

const BUDGET_CURVE = [
  [0, 0],
  [5, 0.012],
  [10, 0.022],
  [20, 0.045],
  [30, 0.065],
  [60, 0.12],
  [90, 0.23],
  [120, 0.34],
  [150, 0.42],
  [180, 0.54],
  [210, 0.75],
  [240, 0.89],
  [270, 0.985],
  [300, 1],
];

function finite(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function interp(points, x) {
  if (x <= points[0][0]) return points[0][1];
  for (let i = 1; i < points.length; i++) {
    const [x0, y0] = points[i - 1];
    const [x1, y1] = points[i];
    if (x <= x1) return y0 + ((y1 - y0) * (x - x0)) / Math.max(1e-9, x1 - x0);
  }
  return points.at(-1)[1];
}

function safeDiv(numerator, denominator) {
  return denominator > 0 ? numerator / denominator : 0;
}

function quantile(values, q) {
  const arr = values.filter(Number.isFinite).sort((a, b) => a - b);
  if (!arr.length) return null;
  return arr[Math.min(arr.length - 1, Math.floor((arr.length - 1) * q))];
}

function mean(values) {
  const arr = values.filter(Number.isFinite);
  return arr.length ? arr.reduce((sum, value) => sum + value, 0) / arr.length : null;
}

function summarize(values) {
  return {
    n: values.filter(Number.isFinite).length,
    mean: mean(values),
    median: quantile(values, 0.5),
    p90: quantile(values, 0.9),
  };
}

function buysOf(window) {
  return (window.activities || [])
    .filter((item) => item.side === "BUY" && (item.outcome === "up" || item.outcome === "down"))
    .sort((a, b) => a.timestamp - b.timestamp || String(a.outcome).localeCompare(String(b.outcome)));
}

function samplesOf(window) {
  return (window.samples || []).slice().sort((a, b) => a.ts - b.ts);
}

function sampleAt(window, rem, maxDistance = 18) {
  const samples = samplesOf(window);
  if (!samples.length) return null;
  let best = null;
  let bestDistance = Infinity;
  for (const sample of samples) {
    const distance = Math.abs(Number(sample.remSec) - rem);
    if (distance < bestDistance) {
      best = sample;
      bestDistance = distance;
    }
  }
  return bestDistance <= maxDistance ? best : null;
}

function cumulativeAt(window, rem) {
  const cutoff = window.windowStart + 300 - rem;
  const out = {
    upShares: 0,
    downShares: 0,
    upUsdc: 0,
    downUsdc: 0,
    buyCount: 0,
  };
  for (const buy of buysOf(window)) {
    if (buy.timestamp > cutoff) continue;
    const size = finite(buy.size) ?? 0;
    const usdc = finite(buy.usdcSize) ?? 0;
    if (buy.outcome === "up") {
      out.upShares += size;
      out.upUsdc += usdc;
    } else if (buy.outcome === "down") {
      out.downShares += size;
      out.downUsdc += usdc;
    }
    out.buyCount += 1;
  }
  return {
    ...out,
    totalUsdc: out.upUsdc + out.downUsdc,
    upAvg: safeDiv(out.upUsdc, out.upShares),
    downAvg: safeDiv(out.downUsdc, out.downShares),
  };
}

function finalCumulative(window) {
  return cumulativeAt(window, 0);
}

function getDiffSignal(window, rem) {
  const sample = sampleAt(window, rem);
  if (!sample) return null;
  return finite(sample.diff);
}

function predictUpShare(window, rem) {
  const elapsed = 300 - rem;
  const earlyWeight = clamp((elapsed - 45) / 135, 0, 1);
  const lateWeight = clamp((elapsed - 150) / 110, 0, 1);
  const diffNow = getDiffSignal(window, rem);
  const diff60 = getDiffSignal(window, 60);
  const diff30 = getDiffSignal(window, 30);
  const diff15 = getDiffSignal(window, 15);
  const usableDiff =
    diffNow ??
    (rem > 90 ? null : diff60) ??
    (rem > 45 ? null : diff30) ??
    diff15 ??
    0;
  const lateDiff = diff15 ?? diff30 ?? diff60 ?? usableDiff;
  const directionComponent = clamp(0.515 + usableDiff * 0.00105, 0.18, 0.86);
  const lateComponent = clamp(0.515 + lateDiff * 0.001, 0.12, 0.9);
  const target = 0.5 * (1 - earlyWeight) + directionComponent * earlyWeight;
  return clamp(target * (1 - lateWeight * 0.35) + lateComponent * lateWeight * 0.35, 0.08, 0.92);
}

function predictFinalBudget(window, allWindows, useOracleBudget) {
  const actual = finalCumulative(window).totalUsdc;
  if (useOracleBudget) return actual;

  const early = cumulativeAt(window, 180).totalUsdc;
  const mid = cumulativeAt(window, 120).totalUsdc;
  if (mid > 0) return clamp(mid / 0.54, 450, 12000);
  if (early > 0) return clamp(early / 0.34, 450, 12000);

  const finals = allWindows
    .filter((item) => item.windowStart !== window.windowStart)
    .map((item) => finalCumulative(item).totalUsdc)
    .filter((value) => value > 0)
    .sort((a, b) => a - b);
  return finals.length ? finals[Math.floor(finals.length * 0.5)] : 2500;
}

function estimateBuyPrice(window, rem, direction, useOracleAvg) {
  if (useOracleAvg) {
    const actual = finalCumulative(window);
    const avg = direction === "up" ? actual.upAvg : actual.downAvg;
    if (avg > 0) return clamp(avg, 0.01, 0.99);
  }
  const sample = sampleAt(window, rem);
  const ask = direction === "up" ? finite(sample?.upAsk) : finite(sample?.downAsk);
  const bid = direction === "up" ? finite(sample?.upBid) : finite(sample?.downBid);
  const mid = direction === "up" ? finite(sample?.upMid) : finite(sample?.downMid);
  const basis = ask ?? mid ?? bid ?? 0.5;
  return clamp(basis, 0.03, 0.97);
}

function simulatePrediction(window, allWindows, uptoRem, useOracleBudget) {
  const finalBudget = predictFinalBudget(window, allWindows, useOracleBudget);
  const elapsed = 300 - uptoRem;
  const totalFrac = interp(BUDGET_CURVE, elapsed);
  const ratio = predictUpShare(window, uptoRem);
  if (useOracleBudget) {
    const actual = finalCumulative(window);
    const upAvg = actual.upAvg || 0.5;
    const downAvg = actual.downAvg || 0.5;
    const denominator = Math.max(0.01, ratio * upAvg + (1 - ratio) * downAvg);
    const totalShares = finalBudget / denominator;
    return {
      upUsdc: ratio * totalShares * upAvg * totalFrac,
      downUsdc: (1 - ratio) * totalShares * downAvg * totalFrac,
      upShares: ratio * totalShares * totalFrac,
      downShares: (1 - ratio) * totalShares * totalFrac,
      totalUsdc: finalBudget * totalFrac,
      upAvg,
      downAvg,
      upShare: ratio,
      finalBudget,
    };
  }
  const rems = CHECKPOINT_REMS.slice().sort((a, b) => b - a);
  const out = {
    upUsdc: 0,
    downUsdc: 0,
    upShares: 0,
    downShares: 0,
    totalUsdc: 0,
  };
  let prevUpUsdc = 0;
  let prevDownUsdc = 0;
  for (const rem of rems) {
    if (rem > uptoRem) continue;
    const elapsed = 300 - rem;
    const targetTotal = finalBudget * interp(BUDGET_CURVE, elapsed);
    const upShare = predictUpShare(window, rem);
    const targetUpUsdc = targetTotal * upShare;
    const targetDownUsdc = targetTotal * (1 - upShare);
    const deltaUp = Math.max(0, targetUpUsdc - prevUpUsdc);
    const deltaDown = Math.max(0, targetDownUsdc - prevDownUsdc);
    if (deltaUp > 0) out.upShares += deltaUp / estimateBuyPrice(window, rem, "up", useOracleBudget);
    if (deltaDown > 0) out.downShares += deltaDown / estimateBuyPrice(window, rem, "down", useOracleBudget);
    prevUpUsdc = Math.max(prevUpUsdc, targetUpUsdc);
    prevDownUsdc = Math.max(prevDownUsdc, targetDownUsdc);
    out.upUsdc = prevUpUsdc;
    out.downUsdc = prevDownUsdc;
    out.totalUsdc = out.upUsdc + out.downUsdc;
  }
  return {
    ...out,
    upAvg: safeDiv(out.upUsdc, out.upShares),
    downAvg: safeDiv(out.downUsdc, out.downShares),
    upShare: safeDiv(out.upShares, out.upShares + out.downShares),
    finalBudget,
  };
}

function predictAt(window, allWindows, rem, useOracleBudget) {
  const pred = simulatePrediction(window, allWindows, rem, useOracleBudget);
  return {
    ...pred,
    rem,
  };
}

function firstBuyInfo(window) {
  const buys = buysOf(window);
  if (!buys.length) return null;
  const first = buys[0];
  return {
    direction: first.outcome,
    rem: window.windowStart + 300 - first.timestamp,
    price: finite(first.price),
    usdc: finite(first.usdcSize),
  };
}

function score(useOracleBudget) {
  const data = JSON.parse(readFileSync(DATA_FILE, "utf8"));
  const windows = (data.windows || [])
    .filter((window) => buysOf(window).length >= 5)
    .sort((a, b) => a.windowStart - b.windowStart);
  const errors = {
    upShares: [],
    downShares: [],
    upRatio: [],
    totalUsdc: [],
    upAvg: [],
    downAvg: [],
    firstRem: [],
    firstDirection: [],
  };
  const rows = [];

  for (const window of windows) {
    const finalActual = finalCumulative(window);
    if (finalActual.totalUsdc <= 0) continue;
    const finalPred = predictAt(window, windows, 0, useOracleBudget);
    const first = firstBuyInfo(window);
    if (first) {
      errors.firstRem.push(Math.abs(first.rem - 295));
      const predFirstDirection = predictUpShare(window, 295) >= 0.5 ? "up" : "down";
      errors.firstDirection.push(predFirstDirection === first.direction ? 0 : 1);
    }

    const actualUpRatio = safeDiv(finalActual.upShares, finalActual.upShares + finalActual.downShares);
    errors.upRatio.push(Math.abs(finalPred.upShare - actualUpRatio));
    errors.totalUsdc.push(Math.abs(finalPred.totalUsdc - finalActual.totalUsdc) / Math.max(50, finalActual.totalUsdc));
    errors.upAvg.push(Math.abs((finalPred.upAvg || 0) - (finalActual.upAvg || 0)));
    errors.downAvg.push(Math.abs((finalPred.downAvg || 0) - (finalActual.downAvg || 0)));

    for (const rem of CHECKPOINT_REMS) {
      const actual = cumulativeAt(window, rem);
      const pred = predictAt(window, windows, rem, useOracleBudget);
      errors.upShares.push(Math.abs(pred.upShares - actual.upShares) / Math.max(5, finalActual.upShares));
      errors.downShares.push(Math.abs(pred.downShares - actual.downShares) / Math.max(5, finalActual.downShares));
    }

    rows.push({
      time: new Date(window.windowStart * 1000).toISOString().slice(11, 16),
      firstActual: first ? `${first.direction}@${first.rem}s` : "-",
      firstPred: `${predictUpShare(window, 295) >= 0.5 ? "up" : "down"}@295s`,
      upActual: Math.round(finalActual.upShares),
      downActual: Math.round(finalActual.downShares),
      upPred: Math.round(finalPred.upShares),
      downPred: Math.round(finalPred.downShares),
      upRatioActual: `${(actualUpRatio * 100).toFixed(0)}%`,
      upRatioPred: `${(finalPred.upShare * 100).toFixed(0)}%`,
      totalActual: Math.round(finalActual.totalUsdc),
      totalPred: Math.round(finalPred.totalUsdc),
      pattern: window.summary?.pattern || "",
    });
  }

  return {
    mode: useOracleBudget ? "shape_with_actual_budget" : "live_like_budget_estimate",
    windows: windows.length,
    metrics: {
      firstRemAbsSec: summarize(errors.firstRem),
      firstDirectionMatchPct: (1 - (mean(errors.firstDirection) ?? 1)) * 100,
      checkpointUpShareErrorPctOfFinal: summarize(errors.upShares.map((value) => value * 100)),
      checkpointDownShareErrorPctOfFinal: summarize(errors.downShares.map((value) => value * 100)),
      finalUpRatioAbsPct: summarize(errors.upRatio.map((value) => value * 100)),
      finalTotalUsdcPctError: summarize(errors.totalUsdc.map((value) => value * 100)),
      finalUpAvgAbsPrice: summarize(errors.upAvg),
      finalDownAvgAbsPrice: summarize(errors.downAvg),
    },
    recentRows: rows.slice(-18),
  };
}

console.log(JSON.stringify({
  shape: score(true),
  liveLike: score(false),
}, null, 2));
