import WebSocket from "ws";
import { createWriteStream, mkdirSync } from "fs";
import { resolve } from "path";

type Direction = "up" | "down";
type Side = "BUY" | "SELL";

interface StatePayload {
  ts: number;
  windowStart: number;
  windowEnd: number;
  conditionId: string;
  upTokenId: string;
  downTokenId: string;
  bestBid: string;
  bestAsk: string;
  priceToBeat: number | null;
  currentPrice: number | null;
  currentPriceUpdatedAt: number;
}

interface BookLevel {
  price: number;
  size: number;
}

interface TopBook {
  bid: number;
  ask: number;
  bidSize: number;
  askSize: number;
  mid: number;
  source: string;
  updatedAt: number;
}

const LOCAL_STATE_URL = process.env.MONITOR_STATE_URL || "http://localhost:3456/api/state";
const MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const CLOB_URL = "https://clob.polymarket.com";
const DATA_API_URL = "https://data-api.polymarket.com";
const WATCH_MS = Number(process.env.MONITOR_WATCH_MS || 15000);
const REST_INTERVAL_MS = Number(process.env.MONITOR_REST_INTERVAL_MS || 200);
const FOCUS_DIR = process.env.MONITOR_FOCUS_DIR === "up" || process.env.MONITOR_FOCUS_DIR === "down"
  ? process.env.MONITOR_FOCUS_DIR
  : "";
const FOCUS_MAX_ASK = Number(process.env.MONITOR_FOCUS_MAX_ASK || 0.7);

function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function getState(): Promise<StatePayload> {
  const res = await fetch(LOCAL_STATE_URL);
  if (!res.ok) throw new Error(`state ${res.status}`);
  return await res.json() as StatePayload;
}

function parseLevels(levels: unknown): BookLevel[] {
  if (!Array.isArray(levels)) return [];
  return levels
    .map((level) => {
      const item = level as { price?: unknown; size?: unknown };
      return { price: Number(item.price), size: Number(item.size ?? 0) };
    })
    .filter(level => level.price > 0 && level.size > 0);
}

async function fetchBook(tokenId: string): Promise<{ bids: BookLevel[]; asks: BookLevel[]; latencyMs: number }> {
  const startedAt = Date.now();
  const res = await fetch(`${CLOB_URL}/book?token_id=${tokenId}`);
  const latencyMs = Date.now() - startedAt;
  if (!res.ok) throw new Error(`book ${res.status}`);
  const data = await res.json() as { bids?: unknown; asks?: unknown };
  const bids = parseLevels(data.bids).sort((a, b) => b.price - a.price);
  const asks = parseLevels(data.asks).sort((a, b) => a.price - b.price);
  return { bids, asks, latencyMs };
}

function topFromLevels(bids: BookLevel[], asks: BookLevel[], source: string): TopBook | null {
  const bid = bids[0];
  const ask = asks[0];
  if (!bid || !ask) return null;
  return {
    bid: bid.price,
    ask: ask.price,
    bidSize: bid.size,
    askSize: ask.size,
    mid: (bid.price + ask.price) / 2,
    source,
    updatedAt: Date.now(),
  };
}

function directionForToken(state: StatePayload, tokenId: string): Direction | null {
  if (tokenId === state.upTokenId) return "up";
  if (tokenId === state.downTokenId) return "down";
  return null;
}

function tokenForDirection(state: StatePayload, direction: Direction): string {
  return direction === "up" ? state.upTokenId : state.downTokenId;
}

function outcomeName(direction: Direction): string {
  return direction === "up" ? "Up" : "Down";
}

function leadingDirection(state: StatePayload): Direction | null {
  if (typeof state.priceToBeat !== "number" || typeof state.currentPrice !== "number") return null;
  return state.currentPrice >= state.priceToBeat ? "up" : "down";
}

function priceDiff(state: StatePayload): number | null {
  if (typeof state.priceToBeat !== "number" || typeof state.currentPrice !== "number") return null;
  return state.currentPrice - state.priceToBeat;
}

function round(value: number, decimals = 4): number {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
}

function summarizeTrades(trades: unknown[], windowEnd: number): Array<Record<string, unknown>> {
  const buckets = new Map<string, { timestampMin: number; timestampMax: number; outcome: string; side: string; price: number; count: number; size: number }>();
  for (const raw of trades) {
    const trade = raw as { timestamp?: unknown; outcome?: unknown; side?: unknown; price?: unknown; size?: unknown };
    const timestamp = Number(trade.timestamp);
    if (!(timestamp >= windowEnd - 20 && timestamp <= windowEnd + 5)) continue;
    const outcome = String(trade.outcome || "");
    const side = String(trade.side || "");
    const price = round(Number(trade.price), 4);
    const size = Number(trade.size);
    if (!outcome || !side || !Number.isFinite(price) || !Number.isFinite(size)) continue;
    const key = `${timestamp}:${outcome}:${side}:${price}`;
    const prev = buckets.get(key) || { timestampMin: timestamp, timestampMax: timestamp, outcome, side, price, count: 0, size: 0 };
    prev.timestampMin = Math.min(prev.timestampMin, timestamp);
    prev.timestampMax = Math.max(prev.timestampMax, timestamp);
    prev.count += 1;
    prev.size += size;
    buckets.set(key, prev);
  }
  return Array.from(buckets.values())
    .sort((a, b) => a.timestampMin - b.timestampMin || a.outcome.localeCompare(b.outcome) || a.price - b.price)
    .map(item => ({ ...item, size: round(item.size, 6) }));
}

async function main(): Promise<void> {
  mkdirSync(resolve(process.cwd(), "backtest-data"), { recursive: true });
  let state = await getState();
  const outputPath = resolve(process.cwd(), "backtest-data", `last10-monitor-${state.windowStart}.jsonl`);
  const stream = createWriteStream(outputPath, { flags: "a" });
  const topByToken = new Map<string, TopBook>();
  const maps = new Map<string, { bids: Map<number, number>; asks: Map<number, number> }>();
  const anomalies: Array<Record<string, unknown>> = [];
  let restTimer: ReturnType<typeof setInterval> | null = null;
  let stateTimer: ReturnType<typeof setInterval> | null = null;

  function log(kind: string, data: Record<string, unknown>): void {
    stream.write(`${JSON.stringify({ t: Date.now(), kind, ...data })}\n`);
  }

  function setTop(tokenId: string, top: TopBook): void {
    const dir = directionForToken(state, tokenId);
    topByToken.set(tokenId, top);
    const remMs = state.windowEnd * 1000 - Date.now();
    log("top", { tokenId, dir, remMs, ...top });

    const leader = leadingDirection(state);
    const diff = priceDiff(state);
    if (!dir || remMs > WATCH_MS + 1000 || remMs < -3000) return;
    const isFocusedLow = FOCUS_DIR === dir && top.ask <= FOCUS_MAX_ASK;
    if (!isFocusedLow) {
      if (!leader || dir !== leader) return;
      if (Math.abs(diff ?? 0) < 5) return;
    }
    if (top.mid < 0.6 || top.ask < 0.6) {
      const event = {
        t: Date.now(),
        type: isFocusedLow ? "focused_token_low_price" : "leading_token_low_price",
        dir,
        remMs,
        diff,
        bid: top.bid,
        ask: top.ask,
        mid: top.mid,
        bidSize: top.bidSize,
        askSize: top.askSize,
        source: top.source,
      };
      anomalies.push(event);
      console.log(`[anomaly] rem=${(remMs / 1000).toFixed(2)}s ${dir} bid=${top.bid} ask=${top.ask} mid=${top.mid.toFixed(3)} source=${top.source} diff=${diff?.toFixed(2)}`);
      log("anomaly", event);
    }
  }

  function applyLevels(tokenId: string, bids: BookLevel[], asks: BookLevel[], source: string): void {
    maps.set(tokenId, {
      bids: new Map(bids.map(level => [level.price, level.size])),
      asks: new Map(asks.map(level => [level.price, level.size])),
    });
    const top = topFromLevels(bids, asks, source);
    if (top) setTop(tokenId, top);
  }

  function applyPriceChange(tokenId: string, side: Side, priceRaw: unknown, sizeRaw: unknown): void {
    const price = Number(priceRaw);
    const size = Number(sizeRaw);
    if (!(price > 0) || !Number.isFinite(size)) return;
    const book = maps.get(tokenId) || { bids: new Map<number, number>(), asks: new Map<number, number>() };
    const levels = side === "BUY" ? book.bids : book.asks;
    if (size > 0) levels.set(price, size);
    else levels.delete(price);
    maps.set(tokenId, book);
    const bids = Array.from(book.bids, ([p, s]) => ({ price: p, size: s })).sort((a, b) => b.price - a.price);
    const asks = Array.from(book.asks, ([p, s]) => ({ price: p, size: s })).sort((a, b) => a.price - b.price);
    const top = topFromLevels(bids, asks, "ws-change");
    if (top) setTop(tokenId, top);
  }

  async function pollRestOnce(): Promise<void> {
    for (const [dir, tokenId] of [["up", state.upTokenId], ["down", state.downTokenId]] as Array<[Direction, string]>) {
      try {
        const book = await fetchBook(tokenId);
        const top = topFromLevels(book.bids, book.asks, "rest");
        log("restBook", {
          dir,
          tokenId,
          remMs: state.windowEnd * 1000 - Date.now(),
          latencyMs: book.latencyMs,
          bids: book.bids.slice(0, 5),
          asks: book.asks.slice(0, 5),
          top,
        });
        if (top) setTop(tokenId, { ...top, source: "rest" });
      } catch (err) {
        log("restError", { dir, tokenId, error: err instanceof Error ? err.message : String(err) });
      }
    }
  }

  const remMs = state.windowEnd * 1000 - Date.now();
  console.log(`[monitor] window=${state.windowStart} rem=${(remMs / 1000).toFixed(1)}s out=${outputPath}`);
  log("start", { state, watchMs: WATCH_MS, restIntervalMs: REST_INTERVAL_MS });

  stateTimer = setInterval(async () => {
    try {
      const next = await getState();
      if (next.windowStart === state.windowStart) state = next;
      log("state", {
        remMs: state.windowEnd * 1000 - Date.now(),
        currentPrice: state.currentPrice,
        priceToBeat: state.priceToBeat,
        diff: priceDiff(state),
        bestBid: state.bestBid,
        bestAsk: state.bestAsk,
      });
    } catch (err) {
      log("stateError", { error: err instanceof Error ? err.message : String(err) });
    }
  }, 250);

  const ws = new WebSocket(MARKET_WS_URL);
  ws.on("open", () => {
    ws.send(JSON.stringify({
      assets_ids: [state.upTokenId, state.downTokenId],
      type: "market",
      custom_feature_enabled: true,
    }));
    log("wsOpen", {});
  });
  ws.on("message", (data) => {
    const text = data.toString();
    if (text === "PONG" || text === "[]") return;
    try {
      const events = Array.isArray(JSON.parse(text)) ? JSON.parse(text) : [JSON.parse(text)];
      for (const evt of events as Array<Record<string, unknown>>) {
        const assetId = String(evt.asset_id || "");
        if (evt.bids !== undefined && evt.asks !== undefined && assetId) {
          applyLevels(assetId, parseLevels(evt.bids).sort((a, b) => b.price - a.price), parseLevels(evt.asks).sort((a, b) => a.price - b.price), "ws-book");
        } else if (evt.event_type === "best_bid_ask" && assetId) {
          const bid = Number(evt.best_bid);
          const ask = Number(evt.best_ask);
          if (bid > 0 && ask > 0) {
            setTop(assetId, { bid, ask, bidSize: 0, askSize: 0, mid: (bid + ask) / 2, source: "ws-bba", updatedAt: Date.now() });
          }
        } else if (evt.event_type === "price_change" && Array.isArray(evt.price_changes)) {
          for (const change of evt.price_changes as Array<Record<string, unknown>>) {
            const tokenId = String(change.asset_id || "");
            const side = String(change.side || "") as Side;
            if (tokenId && (side === "BUY" || side === "SELL")) applyPriceChange(tokenId, side, change.price, change.size);
          }
        }
      }
    } catch (err) {
      log("wsParseError", { error: err instanceof Error ? err.message : String(err), text: text.slice(0, 300) });
    }
  });
  ws.on("error", (err) => log("wsError", { error: err.message }));
  const pingTimer = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.send("PING");
  }, 10000);

  const waitMs = Math.max(0, state.windowEnd * 1000 - Date.now() - WATCH_MS);
  if (waitMs > 0) await sleep(waitMs);
  console.log(`[monitor] entering last ${(WATCH_MS / 1000).toFixed(0)}s`);
  await pollRestOnce();
  restTimer = setInterval(() => { void pollRestOnce(); }, REST_INTERVAL_MS);

  await sleep(Math.max(0, state.windowEnd * 1000 - Date.now() + 3500));
  if (restTimer) clearInterval(restTimer);
  if (stateTimer) clearInterval(stateTimer);
  clearInterval(pingTimer);
  ws.close();

  let tradesSummary: Array<Record<string, unknown>> = [];
  try {
    const tradesRes = await fetch(`${DATA_API_URL}/trades?market=${encodeURIComponent(state.conditionId)}&limit=1000`);
    const trades = await tradesRes.json() as unknown[];
    tradesSummary = summarizeTrades(trades, state.windowEnd);
    log("tradesSummary", { tradesSummary });
  } catch (err) {
    log("tradesError", { error: err instanceof Error ? err.message : String(err) });
  }

  let cryptoClose: Record<string, unknown> | null = null;
  try {
    const startIso = new Date(state.windowStart * 1000).toISOString();
    const endIso = new Date(state.windowEnd * 1000).toISOString();
    const url = `https://polymarket.com/api/crypto/crypto-price?symbol=BTC&eventStartTime=${encodeURIComponent(startIso)}&variant=fiveminute&endDate=${encodeURIComponent(endIso)}`;
    cryptoClose = await (await fetch(url)).json() as Record<string, unknown>;
    log("cryptoClose", { cryptoClose });
  } catch (err) {
    log("cryptoError", { error: err instanceof Error ? err.message : String(err) });
  }

  const summary = {
    outputPath,
    windowStart: state.windowStart,
    windowEnd: state.windowEnd,
    anomalies,
    anomalyCount: anomalies.length,
    lastTop: {
      up: topByToken.get(state.upTokenId),
      down: topByToken.get(state.downTokenId),
    },
    cryptoClose,
    tradesSummary,
  };
  log("summary", summary);
  stream.end();
  console.log(JSON.stringify(summary, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
