/**
 * Host watchdog — out-of-band substrate monitoring (F0).
 *
 * The May 30 incident: Docker died, every agent (including the whole
 * escalation chain) lives in Docker, and the host crashed at startup before
 * its channel adapters existed — so nothing could tell J the system was
 * dead. Two weeks of silence.
 *
 * This module is the host-side answer. It runs a five-minute tick that:
 *   - probes the container runtime (alert on down-edge, re-alert every 6h,
 *     one recovery notice; orphan cleanup + status reset on the up-edge,
 *     which also covers normal healthy boots)
 *   - detects work starvation: a session with due inbound messages and no
 *     running container for 3 consecutive ticks (host-sweep wakes are
 *     failing — OneCLI down, image broken, runtime half-dead)
 *   - touches data/host-heartbeat every tick; the external tripwire
 *     (ops/tripwire in the Engram repo) treats a stale heartbeat as "the
 *     watchdog itself is dead", which is how watching-the-watcher
 *     terminates without a NOC
 *
 * Alerts go directly through the delivery adapter — no session, no
 * outbound.db row, no destination ACL (those govern container traffic).
 * Destination comes from WATCHDOG_ALERT_CHANNEL_TYPE / _PLATFORM_ID /
 * _THREAD_ID (process.env, then .env). Unset → log-only mode; the
 * heartbeat still advances so the tripwire layer keeps working.
 *
 * Shape mirrors host-sweep: a pure decision function
 * (decideWatchdogAlerts) plus a thin I/O tick. All filesystem, DB, docker,
 * and adapter calls live in the tick.
 */
import fs from 'fs';
import path from 'path';

import { DATA_DIR } from './config.js';
import { isContainerRuntimeUp, cleanupOrphans } from './container-runtime.js';
import { isContainerRunning } from './container-runner.js';
import { getActiveSessions, resetAllContainerStatuses } from './db/sessions.js';
import { countDueMessages } from './db/session-db.js';
import { getDeliveryAdapter } from './delivery.js';
import { readEnvFile } from './env.js';
import { log } from './log.js';
import { openInboundDb } from './session-manager.js';

export const TICK_MS = 5 * 60_000;
export const REALERT_MS = 6 * 60 * 60_000;
export const STARVATION_TICKS = 3;

export type WatchdogConditionType = 'container-runtime-down' | 'work-starvation';

export interface WatchdogAlertEntry {
  firstDetectedAt: number;
  lastAlertAt: number;
}

export interface WatchdogState {
  alerts: Partial<Record<WatchdogConditionType, WatchdogAlertEntry>>;
  starvationTicks: Record<string, number>;
}

export interface WatchdogAction {
  type: WatchdogConditionType;
  message: string;
}

export interface WatchdogDecision {
  alerts: WatchdogAction[];
  recoveries: WatchdogAction[];
  nextState: WatchdogState;
}

export function freshWatchdogState(): WatchdogState {
  return { alerts: {}, starvationTicks: {} };
}

function fmtDuration(ms: number): string {
  const minutes = Math.max(0, Math.round(ms / 60_000));
  if (minutes < 60) return `${minutes}m`;
  return `${Math.floor(minutes / 60)}h ${minutes % 60}m`;
}

function iso(ms: number): string {
  return new Date(ms).toISOString();
}

const CONDITION_LABEL: Record<WatchdogConditionType, string> = {
  'container-runtime-down': 'container runtime down',
  'work-starvation': 'work starvation',
};

function runtimeDownMessage(entry: WatchdogAlertEntry, now: number): string {
  return (
    `WATCHDOG: container runtime down (docker unreachable) since ${iso(entry.firstDetectedAt)}` +
    ` (${fmtDuration(now - entry.firstDetectedAt)}). Agents cannot run.` +
    ` Remediation: open -a Docker, or start Docker Desktop.`
  );
}

function starvationMessage(ids: string[], entry: WatchdogAlertEntry, now: number): string {
  return (
    `WATCHDOG: work starvation — sessions with queued messages and no running container` +
    ` since ${iso(entry.firstDetectedAt)} (${fmtDuration(now - entry.firstDetectedAt)}): ${ids.join(', ')}.` +
    ` Wakes are failing; check docker, OneCLI, and the container image.`
  );
}

function recoveryMessage(type: WatchdogConditionType, entry: WatchdogAlertEntry, now: number): string {
  return (
    `WATCHDOG RECOVERED: ${CONDITION_LABEL[type]}.` +
    ` Was active ${fmtDuration(now - entry.firstDetectedAt)} (since ${iso(entry.firstDetectedAt)}).`
  );
}

/**
 * Pure decision for one watchdog tick. Conditions are evaluated
 * independently — policy like "skip starvation reads while the runtime is
 * down" belongs to the caller, not here.
 */
export function decideWatchdogAlerts(args: {
  now: number;
  runtimeUp: boolean;
  starvedSessionIds: string[];
  prevState: WatchdogState;
}): WatchdogDecision {
  const { now, runtimeUp, starvedSessionIds, prevState } = args;
  const alerts: WatchdogAction[] = [];
  const recoveries: WatchdogAction[] = [];
  const nextAlerts: WatchdogState['alerts'] = { ...prevState.alerts };

  // Starvation accounting: counters advance for sessions starved THIS tick,
  // reset (drop) for everything else.
  const nextTicks: Record<string, number> = {};
  for (const id of starvedSessionIds) {
    nextTicks[id] = (prevState.starvationTicks[id] ?? 0) + 1;
  }
  const starvingIds = Object.entries(nextTicks)
    .filter(([, n]) => n >= STARVATION_TICKS)
    .map(([id]) => id)
    .sort();

  const active: Array<{
    type: WatchdogConditionType;
    build: (entry: WatchdogAlertEntry) => string;
  }> = [];
  if (!runtimeUp) {
    active.push({ type: 'container-runtime-down', build: (e) => runtimeDownMessage(e, now) });
  }
  if (starvingIds.length > 0) {
    active.push({ type: 'work-starvation', build: (e) => starvationMessage(starvingIds, e, now) });
  }

  for (const condition of active) {
    const prev = nextAlerts[condition.type];
    if (!prev) {
      const entry = { firstDetectedAt: now, lastAlertAt: now };
      nextAlerts[condition.type] = entry;
      alerts.push({ type: condition.type, message: condition.build(entry) });
    } else if (now - prev.lastAlertAt >= REALERT_MS) {
      const entry = { ...prev, lastAlertAt: now };
      nextAlerts[condition.type] = entry;
      alerts.push({ type: condition.type, message: condition.build(entry) });
    }
  }

  const activeTypes = new Set(active.map((c) => c.type));
  for (const key of Object.keys(nextAlerts) as WatchdogConditionType[]) {
    if (activeTypes.has(key)) continue;
    const entry = nextAlerts[key];
    if (entry) recoveries.push({ type: key, message: recoveryMessage(key, entry, now) });
    delete nextAlerts[key];
  }

  return { alerts, recoveries, nextState: { alerts: nextAlerts, starvationTicks: nextTicks } };
}

// ─── state persistence ───────────────────────────────────────────────────────

export function loadWatchdogState(filePath: string): WatchdogState {
  try {
    const raw = JSON.parse(fs.readFileSync(filePath, 'utf8')) as unknown;
    if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return freshWatchdogState();
    const obj = raw as Record<string, unknown>;
    const alerts = obj.alerts && typeof obj.alerts === 'object' && !Array.isArray(obj.alerts) ? obj.alerts : {};
    const ticks =
      obj.starvationTicks && typeof obj.starvationTicks === 'object' && !Array.isArray(obj.starvationTicks)
        ? obj.starvationTicks
        : {};
    return {
      alerts: alerts as WatchdogState['alerts'],
      starvationTicks: ticks as Record<string, number>,
    };
  } catch {
    return freshWatchdogState();
  }
}

export function saveWatchdogState(filePath: string, state: WatchdogState): void {
  try {
    fs.writeFileSync(filePath, JSON.stringify(state, null, 2), 'utf8');
  } catch (err) {
    log.warn('Failed to persist watchdog state', { filePath, err });
  }
}

// ─── alert destination ───────────────────────────────────────────────────────

interface AlertDestination {
  channelType: string;
  platformId: string;
  threadId: string | null;
}

function getWatchdogAlertDestination(): AlertDestination | null {
  const env = readEnvFile(['WATCHDOG_ALERT_CHANNEL_TYPE', 'WATCHDOG_ALERT_PLATFORM_ID', 'WATCHDOG_ALERT_THREAD_ID']);
  const channelType = process.env.WATCHDOG_ALERT_CHANNEL_TYPE || env.WATCHDOG_ALERT_CHANNEL_TYPE;
  const platformId = process.env.WATCHDOG_ALERT_PLATFORM_ID || env.WATCHDOG_ALERT_PLATFORM_ID;
  if (!channelType || !platformId) return null;
  const threadId = process.env.WATCHDOG_ALERT_THREAD_ID || env.WATCHDOG_ALERT_THREAD_ID || null;
  return { channelType, platformId, threadId };
}

let warnedNoDestination = false;

async function sendAlert(text: string): Promise<void> {
  const destination = getWatchdogAlertDestination();
  if (!destination) {
    if (!warnedNoDestination) {
      log.warn('Watchdog alert destination not configured (WATCHDOG_ALERT_*) — log-only mode', { text });
      warnedNoDestination = true;
    } else {
      log.warn('Watchdog alert (log-only)', { text });
    }
    return;
  }
  const adapter = getDeliveryAdapter();
  if (!adapter) {
    log.warn('Watchdog alert has no delivery adapter yet', { text });
    return;
  }
  try {
    await adapter.deliver(
      destination.channelType,
      destination.platformId,
      destination.threadId,
      'text',
      JSON.stringify({ text }),
    );
    log.info('Watchdog alert delivered', { text });
  } catch (err) {
    log.error('Watchdog alert delivery failed', { text, err });
  }
}

// ─── tick loop ───────────────────────────────────────────────────────────────

function statePath(): string {
  return path.join(DATA_DIR, 'watchdog-state.json');
}

function heartbeatFilePath(): string {
  return path.join(DATA_DIR, 'host-heartbeat');
}

function writeHeartbeat(): void {
  try {
    fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.writeFileSync(heartbeatFilePath(), new Date().toISOString(), 'utf8');
  } catch (err) {
    log.warn('Failed to write watchdog heartbeat', { err });
  }
}

function computeStarvedSessionIds(): string[] {
  const starved: string[] = [];
  for (const session of getActiveSessions()) {
    let inDb;
    try {
      inDb = openInboundDb(session.agent_group_id, session.id);
    } catch {
      continue;
    }
    try {
      if (countDueMessages(inDb) > 0 && !isContainerRunning(session.id)) {
        starved.push(session.id);
      }
    } catch {
      /* session DB mid-teardown — skip this tick */
    } finally {
      inDb.close();
    }
  }
  return starved;
}

let running = false;
// Last observed runtime state. null = unknown (fresh start). The up-edge
// (null/false → true) triggers orphan cleanup + container-status reset —
// the same actions the old fatal startup path ran, now covering both
// healthy boots and mid-flight recoveries.
let lastRuntimeUp: boolean | null = null;

export function startHostWatchdog(): void {
  if (running) return;
  running = true;
  lastRuntimeUp = null;
  void tick();
}

export function stopHostWatchdog(): void {
  running = false;
}

async function tick(): Promise<void> {
  if (!running) return;

  // Heartbeat first: it means "the watchdog is alive", not "all healthy".
  writeHeartbeat();

  try {
    const runtimeUp = isContainerRuntimeUp();

    if (runtimeUp && lastRuntimeUp !== true) {
      cleanupOrphans();
      const resetCount = resetAllContainerStatuses();
      if (resetCount > 0) {
        log.info('Reset stale container statuses after runtime up-edge', { count: resetCount });
      }
    }
    lastRuntimeUp = runtimeUp;

    // Starvation reads are skipped while the runtime is down — the
    // runtime-down condition already covers the outage, and counting every
    // session as starved would just double-alert.
    const starvedSessionIds = runtimeUp ? computeStarvedSessionIds() : [];

    const prevState = loadWatchdogState(statePath());
    const decision = decideWatchdogAlerts({
      now: Date.now(),
      runtimeUp,
      starvedSessionIds,
      prevState,
    });

    for (const alert of decision.alerts) {
      log.warn('Watchdog condition', { type: alert.type });
      await sendAlert(alert.message);
    }
    for (const recovery of decision.recoveries) {
      log.info('Watchdog condition cleared', { type: recovery.type });
      await sendAlert(recovery.message);
    }

    saveWatchdogState(statePath(), decision.nextState);
  } catch (err) {
    log.error('Watchdog tick error', { err });
  }

  setTimeout(tick, TICK_MS);
}
