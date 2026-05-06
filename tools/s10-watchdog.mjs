import { appendFileSync, existsSync } from "node:fs";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { resolve } from "node:path";

const execFileAsync = promisify(execFile);

const ROOT = process.cwd();
const STATE_URL = process.env.WATCHDOG_STATE_URL || "http://localhost:3456/api/state";
const PORT = Number(process.env.WATCHDOG_PORT || 3456);
const INTERVAL_MS = Number(process.env.WATCHDOG_INTERVAL_MS || 15_000);
const STALE_BOOK_MS = Number(process.env.WATCHDOG_STALE_BOOK_MS || 6_000);
const RESTART_COOLDOWN_MS = Number(process.env.WATCHDOG_RESTART_COOLDOWN_MS || 120_000);
const LOG_FILE = resolve(ROOT, ".watchdog.log");
const SERVER_LOG = resolve(ROOT, ".server.log");
const SERVER_ERR = resolve(ROOT, ".server.err.log");

let failCount = 0;
let staleCount = 0;
let zeroWindowCount = 0;
let lastRestartAt = 0;

function nowIso() {
  return new Date().toISOString();
}

function log(level, message, meta = {}) {
  const line = JSON.stringify({ ts: nowIso(), level, message, ...meta });
  appendFileSync(LOG_FILE, `${line}\n`, "utf8");
  console.log(line);
}

function num(value) {
  const n = Number(value);
  return Number.isFinite(n) ? n : null;
}

async function fetchState() {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8_000);
  try {
    const res = await fetch(STATE_URL, { signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(timer);
  }
}

async function restartServer(reason) {
  const now = Date.now();
  if (now - lastRestartAt < RESTART_COOLDOWN_MS) {
    log("warn", "restart skipped by cooldown", { reason });
    return false;
  }
  lastRestartAt = now;
  log("warn", "restarting server", { reason });
  const command = [
    `$old = Get-NetTCPConnection -LocalPort ${PORT} -State Listen -ErrorAction SilentlyContinue | Select-Object -First 1 -ExpandProperty OwningProcess`,
    `if ($old) { Stop-Process -Id $old -Force }`,
    `Start-Sleep -Seconds 1`,
    `Start-Process -FilePath 'npx.cmd' -ArgumentList @('tsx','server.ts') -WorkingDirectory '${ROOT.replaceAll("'", "''")}' -WindowStyle Hidden -RedirectStandardOutput '${SERVER_LOG.replaceAll("'", "''")}' -RedirectStandardError '${SERVER_ERR.replaceAll("'", "''")}'`,
  ].join("\n");
  try {
    await execFileAsync("powershell.exe", ["-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", command], {
      cwd: ROOT,
      timeout: 30_000,
      windowsHide: true,
    });
    log("info", "restart command completed", { reason });
    failCount = 0;
    staleCount = 0;
    zeroWindowCount = 0;
    return true;
  } catch (err) {
    log("error", "restart command failed", { reason, error: err instanceof Error ? err.message : String(err) });
    return false;
  }
}

function summarize(state) {
  const s10 = state.strategy?.perStrategy?.s10 || {};
  return {
    mode: state.executionMode,
    windowStart: state.windowStart,
    probabilityReady: state.probabilityReady,
    bid: state.bestBid,
    ask: state.bestAsk,
    bookAgeMs: state.bookAgeMs,
    bookSource: state.bookSource,
    runtimeState: state.strategy?.state,
    s10MakerMode: s10.makerMode,
    s10Reason: s10.makerLastReason,
    s10Orders: s10.makerActiveOrders,
    paperUp: state.paper?.upLocalSize,
    paperDown: state.paper?.downLocalSize,
    paperUsdc: state.paper?.usdc,
  };
}

async function checkOnce() {
  let state;
  try {
    state = await fetchState();
    failCount = 0;
  } catch (err) {
    failCount++;
    log("error", "state fetch failed", { failCount, error: err instanceof Error ? err.message : String(err) });
    if (failCount >= 3) await restartServer("api unreachable");
    return;
  }

  const summary = summarize(state);
  const bid = num(state.bestBid);
  const ask = num(state.bestAsk);
  const hasBook = bid != null && ask != null && bid >= 0 && ask > 0;
  const bookAge = num(state.bookAgeMs);

  if (!(state.windowStart > 0)) zeroWindowCount++;
  else zeroWindowCount = 0;

  if (!hasBook || bookAge == null || bookAge > STALE_BOOK_MS) staleCount++;
  else staleCount = 0;

  if (zeroWindowCount >= 4) {
    log("error", "window not subscribed", { zeroWindowCount, ...summary });
    await restartServer("windowStart stayed zero");
    return;
  }

  if (staleCount >= 4) {
    log("error", "book stale or missing", { staleCount, ...summary });
    await restartServer("book stale or missing");
    return;
  }

  const s10Enabled = !!state.strategyConfig?.enabled?.s10;
  const rem = state.windowEnd ? state.windowEnd - Math.floor((state.exchangeTs || Date.now()) / 1000) : null;
  const s10 = state.strategy?.perStrategy?.s10 || {};
  if (
    state.executionMode === "paper" &&
    s10Enabled &&
    state.probabilityReady &&
    rem != null &&
    rem >= 30 &&
    rem <= 285 &&
    String(s10.makerMode || "") === "idle"
  ) {
    log("warn", "s10 maker idle during tradable window", { rem, ...summary });
    return;
  }

  log("info", "ok", summary);
}

if (!existsSync(resolve(ROOT, "server.ts"))) {
  log("error", "server.ts not found; run watchdog from repo root", { cwd: ROOT });
  process.exit(1);
}

log("info", "watchdog started", { stateUrl: STATE_URL, intervalMs: INTERVAL_MS, root: ROOT });
await checkOnce();
setInterval(() => {
  checkOnce().catch((err) => log("error", "check crashed", { error: err instanceof Error ? err.message : String(err) }));
}, INTERVAL_MS);
