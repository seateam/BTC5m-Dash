const DEFAULT_ADDRESS = "0xeebde7a0e019a63e6b476eb425505b7b3e6eba30";
const address = process.argv[2] || DEFAULT_ADDRESS;

type AnyRow = Record<string, any>;

async function getJson<T>(url: string): Promise<T> {
  const res = await fetch(url, { headers: { accept: "application/json" } });
  if (!res.ok) throw new Error(`${res.status} ${url}`);
  return await res.json() as T;
}

async function fetchPages(base: string, limit: number, maxOffset: number, concurrency = 20): Promise<AnyRow[]> {
  const offsets: number[] = [];
  for (let offset = 0; offset <= maxOffset; offset += limit) offsets.push(offset);
  const out: AnyRow[] = [];
  for (let i = 0; i < offsets.length; i += concurrency) {
    const chunk = offsets.slice(i, i + concurrency);
    const pages = await Promise.all(chunk.map((offset) =>
      getJson<AnyRow[]>(`${base}&limit=${limit}&offset=${offset}`).catch(() => []),
    ));
    for (const page of pages) out.push(...page);
  }
  return out;
}

function sum<T>(items: T[], fn: (item: T) => number): number {
  return items.reduce((acc, item) => acc + (Number(fn(item)) || 0), 0);
}

function groupBy<T>(items: T[], fn: (item: T) => string): Map<string, T[]> {
  const map = new Map<string, T[]>();
  for (const item of items) {
    const key = fn(item);
    const arr = map.get(key);
    if (arr) arr.push(item);
    else map.set(key, [item]);
  }
  return map;
}

function pct(value: number, base: number): number | null {
  return base > 0 ? Number((100 * value / base).toFixed(2)) : null;
}

function bucket(row: AnyRow): string {
  const slug = String(row.slug || row.eventSlug || "");
  const title = String(row.title || "");
  if (/updown-5m|5m-/.test(slug)) return "5m";
  if (/updown-15m|15m-/.test(slug)) return "15m";
  if (/updown-4h|4h-/.test(slug)) return "4h";
  if (/Bitcoin|Ethereum|Solana|XRP/i.test(title)) return "crypto_other";
  return "other";
}

const [value, traded, positions, closed, activity, merges, redeems, splits] = await Promise.all([
  getJson<AnyRow[]>(`https://data-api.polymarket.com/value?user=${address}`).catch(() => []),
  getJson<AnyRow>(`https://data-api.polymarket.com/traded?user=${address}`).catch((err) => ({ error: String(err) })),
  getJson<AnyRow[]>(`https://data-api.polymarket.com/positions?user=${address}&limit=100&offset=0`).catch(() => []),
  fetchPages(`https://data-api.polymarket.com/closed-positions?user=${address}&sortBy=TIMESTAMP&sortDirection=DESC`, 50, 9950),
  fetchPages(`https://data-api.polymarket.com/activity?user=${address}&sortBy=TIMESTAMP&sortDirection=DESC`, 500, 10000),
  getJson<AnyRow[]>(`https://data-api.polymarket.com/activity?user=${address}&type=MERGE&limit=500&offset=0&sortBy=TIMESTAMP&sortDirection=DESC`).catch(() => []),
  getJson<AnyRow[]>(`https://data-api.polymarket.com/activity?user=${address}&type=REDEEM&limit=500&offset=0&sortBy=TIMESTAMP&sortDirection=DESC`).catch(() => []),
  getJson<AnyRow[]>(`https://data-api.polymarket.com/activity?user=${address}&type=SPLIT&limit=500&offset=0&sortBy=TIMESTAMP&sortDirection=DESC`).catch(() => []),
]);

const closedCost = sum(closed, (row) => Number(row.avgPrice) * Number(row.totalBought));
const closedPnl = sum(closed, (row) => row.realizedPnl);
const closedWins = closed.filter((row) => Number(row.realizedPnl) > 0);
const conditionGroups = [...groupBy(closed, (row) => row.conditionId).entries()].map(([conditionId, rows]) => {
  const cost = sum(rows, (row) => Number(row.avgPrice) * Number(row.totalBought));
  const pnl = sum(rows, (row) => row.realizedPnl);
  const outcomeCount = new Set(rows.map((row) => row.outcomeIndex)).size;
  return {
    conditionId,
    title: rows[0]?.title,
    positions: rows.length,
    bothOutcomes: outcomeCount > 1,
    cost: Number(cost.toFixed(2)),
    pnl: Number(pnl.toFixed(2)),
    roiPct: pct(pnl, cost),
  };
});

const openConditionGroups = [...groupBy(positions, (row) => row.conditionId).entries()].map(([conditionId, rows]) => ({
  conditionId,
  title: rows[0]?.title,
  bothOutcomes: new Set(rows.map((row) => row.outcomeIndex)).size > 1,
  initialValue: Number(sum(rows, (row) => row.initialValue).toFixed(2)),
  currentValue: Number(sum(rows, (row) => row.currentValue).toFixed(2)),
  cashPnl: Number(sum(rows, (row) => row.cashPnl).toFixed(2)),
}));

const activityTypes = [...groupBy(activity, (row) => row.type).entries()]
  .map(([type, rows]) => ({ type, count: rows.length }))
  .sort((a, b) => b.count - a.count);

const tradeGroups = [...groupBy(activity.filter((row) => row.type === "TRADE"), (row) => row.conditionId).entries()]
  .map(([conditionId, rows]) => {
    const prices = rows.map((row) => Number(row.price)).filter(Number.isFinite).sort((a, b) => a - b);
    return {
      conditionId,
      title: rows[0]?.title,
      trades: rows.length,
      outcomeCount: new Set(rows.map((row) => row.outcomeIndex)).size,
      buyCash: Number(sum(rows.filter((row) => row.side === "BUY"), (row) => row.usdcSize).toFixed(2)),
      sellCash: Number(sum(rows.filter((row) => row.side === "SELL"), (row) => row.usdcSize).toFixed(2)),
      minPrice: prices[0] ?? null,
      maxPrice: prices.at(-1) ?? null,
    };
  })
  .sort((a, b) => b.trades - a.trades)
  .slice(0, 15);

const bucketStats = [...groupBy(closed, bucket).entries()]
  .map(([name, rows]) => {
    const cost = sum(rows, (row) => Number(row.avgPrice) * Number(row.totalBought));
    const pnl = sum(rows, (row) => row.realizedPnl);
    return { bucket: name, count: rows.length, cost: Number(cost.toFixed(2)), pnl: Number(pnl.toFixed(2)), roiPct: pct(pnl, cost) };
  })
  .sort((a, b) => b.pnl - a.pnl);

console.log(JSON.stringify({
  address,
  value,
  traded,
  currentPositions: positions.length,
  openConditionCount: openConditionGroups.length,
  openBothOutcomeConditions: openConditionGroups.filter((row) => row.bothOutcomes).length,
  topOpenConditions: openConditionGroups.sort((a, b) => b.initialValue - a.initialValue).slice(0, 10),
  closedSampleCount: closed.length,
  closedSampleCost: Number(closedCost.toFixed(2)),
  closedSamplePnl: Number(closedPnl.toFixed(2)),
  closedSampleRoiPct: pct(closedPnl, closedCost),
  closedPositionWinRatePct: pct(closedWins.length, closed.length),
  closedConditionCount: conditionGroups.length,
  closedBothOutcomeConditionPct: pct(conditionGroups.filter((row) => row.bothOutcomes).length, conditionGroups.length),
  activityFetched: activity.length,
  activityTypes,
  recentMergeCount: merges.length,
  recentMergeCash: Number(sum(merges, (row) => row.usdcSize).toFixed(2)),
  recentRedeemCount: redeems.length,
  recentRedeemCash: Number(sum(redeems, (row) => row.usdcSize).toFixed(2)),
  recentSplitCount: splits.length,
  bucketStats,
  topRecentTradeConditions: tradeGroups,
  topBothClosedConditions: conditionGroups.filter((row) => row.bothOutcomes).sort((a, b) => b.pnl - a.pnl).slice(0, 15),
}, null, 2));
