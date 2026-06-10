import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import fs from 'fs';
import path from 'path';

const TMP_DATA_DIR = await vi.hoisted(async () => {
  const { mkdtempSync } = await import('fs');
  const { tmpdir } = await import('os');
  const { join } = await import('path');
  return mkdtempSync(join(tmpdir(), 'watchdog-test-'));
});

vi.mock('./config.js', async (importOriginal) => {
  const orig = await importOriginal<typeof import('./config.js')>();
  return { ...orig, DATA_DIR: TMP_DATA_DIR };
});

vi.mock('./container-runtime.js', () => ({
  isContainerRuntimeUp: vi.fn(() => false),
  cleanupOrphans: vi.fn(),
}));

vi.mock('./db/sessions.js', () => ({
  getActiveSessions: vi.fn(() => []),
  resetAllContainerStatuses: vi.fn(() => 0),
}));

import {
  decideWatchdogAlerts,
  freshWatchdogState,
  loadWatchdogState,
  saveWatchdogState,
  startHostWatchdog,
  stopHostWatchdog,
  REALERT_MS,
  STARVATION_TICKS,
  type WatchdogState,
} from './host-watchdog.js';
import { isContainerRuntimeUp, cleanupOrphans } from './container-runtime.js';
import { resetAllContainerStatuses } from './db/sessions.js';
import { setDeliveryAdapter } from './delivery.js';

const T0 = Date.parse('2026-06-10T12:00:00Z');

describe('decideWatchdogAlerts — runtime condition', () => {
  it('fires one alert on the down-edge', () => {
    const d = decideWatchdogAlerts({
      now: T0,
      runtimeUp: false,
      starvedSessionIds: [],
      prevState: freshWatchdogState(),
    });
    expect(d.alerts).toHaveLength(1);
    expect(d.alerts[0].type).toBe('container-runtime-down');
    expect(d.alerts[0].message).toContain('container runtime');
    expect(d.recoveries).toHaveLength(0);
    expect(d.nextState.alerts['container-runtime-down']).toEqual({
      firstDetectedAt: T0,
      lastAlertAt: T0,
    });
  });

  it('stays silent while the condition persists inside the re-alert window', () => {
    const first = decideWatchdogAlerts({
      now: T0,
      runtimeUp: false,
      starvedSessionIds: [],
      prevState: freshWatchdogState(),
    });
    const second = decideWatchdogAlerts({
      now: T0 + 5 * 60_000,
      runtimeUp: false,
      starvedSessionIds: [],
      prevState: first.nextState,
    });
    expect(second.alerts).toHaveLength(0);
    expect(second.recoveries).toHaveLength(0);
    // firstDetectedAt is preserved
    expect(second.nextState.alerts['container-runtime-down']?.firstDetectedAt).toBe(T0);
  });

  it('re-alerts once the re-alert window elapses', () => {
    const first = decideWatchdogAlerts({
      now: T0,
      runtimeUp: false,
      starvedSessionIds: [],
      prevState: freshWatchdogState(),
    });
    const later = decideWatchdogAlerts({
      now: T0 + REALERT_MS,
      runtimeUp: false,
      starvedSessionIds: [],
      prevState: first.nextState,
    });
    expect(later.alerts).toHaveLength(1);
    expect(later.nextState.alerts['container-runtime-down']).toEqual({
      firstDetectedAt: T0,
      lastAlertAt: T0 + REALERT_MS,
    });
  });

  it('fires exactly one recovery on the up-edge and clears state', () => {
    const down = decideWatchdogAlerts({
      now: T0,
      runtimeUp: false,
      starvedSessionIds: [],
      prevState: freshWatchdogState(),
    });
    const up = decideWatchdogAlerts({
      now: T0 + 10 * 60_000,
      runtimeUp: true,
      starvedSessionIds: [],
      prevState: down.nextState,
    });
    expect(up.alerts).toHaveLength(0);
    expect(up.recoveries).toHaveLength(1);
    expect(up.recoveries[0].type).toBe('container-runtime-down');
    expect(up.nextState.alerts['container-runtime-down']).toBeUndefined();

    // A further healthy tick produces nothing.
    const again = decideWatchdogAlerts({
      now: T0 + 15 * 60_000,
      runtimeUp: true,
      starvedSessionIds: [],
      prevState: up.nextState,
    });
    expect(again.alerts).toHaveLength(0);
    expect(again.recoveries).toHaveLength(0);
  });
});

describe('decideWatchdogAlerts — starvation condition', () => {
  function tickStarved(state: WatchdogState, now: number, ids: string[]) {
    return decideWatchdogAlerts({ now, runtimeUp: true, starvedSessionIds: ids, prevState: state });
  }

  it('alerts only after the threshold of consecutive starved ticks', () => {
    let state = freshWatchdogState();
    let now = T0;
    for (let i = 1; i < STARVATION_TICKS; i++) {
      const d = tickStarved(state, now, ['sess-1']);
      expect(d.alerts).toHaveLength(0);
      state = d.nextState;
      now += 5 * 60_000;
    }
    const crossing = tickStarved(state, now, ['sess-1']);
    expect(crossing.alerts).toHaveLength(1);
    expect(crossing.alerts[0].type).toBe('work-starvation');
    expect(crossing.alerts[0].message).toContain('sess-1');
  });

  it('a healthy tick resets the counter', () => {
    let state = freshWatchdogState();
    state = tickStarved(state, T0, ['sess-1']).nextState;
    state = tickStarved(state, T0 + 5 * 60_000, ['sess-1']).nextState;
    // Wake succeeded this tick — session no longer starved.
    state = tickStarved(state, T0 + 10 * 60_000, []).nextState;
    expect(state.starvationTicks['sess-1']).toBeUndefined();
    // Two more starved ticks still under threshold: no alert.
    state = tickStarved(state, T0 + 15 * 60_000, ['sess-1']).nextState;
    const d = tickStarved(state, T0 + 20 * 60_000, ['sess-1']);
    expect(d.alerts).toHaveLength(0);
  });

  it('recovers once when no session remains past the threshold', () => {
    let state = freshWatchdogState();
    let now = T0;
    for (let i = 0; i < STARVATION_TICKS; i++) {
      state = tickStarved(state, now, ['sess-1']).nextState;
      now += 5 * 60_000;
    }
    expect(state.alerts['work-starvation']).toBeDefined();
    const recovered = tickStarved(state, now, []);
    expect(recovered.recoveries).toHaveLength(1);
    expect(recovered.recoveries[0].type).toBe('work-starvation');
    expect(recovered.nextState.alerts['work-starvation']).toBeUndefined();
  });

  it('conditions are independent — runtime down and starvation co-fire', () => {
    let state = freshWatchdogState();
    let now = T0;
    for (let i = 1; i < STARVATION_TICKS; i++) {
      state = decideWatchdogAlerts({
        now,
        runtimeUp: true,
        starvedSessionIds: ['sess-1'],
        prevState: state,
      }).nextState;
      now += 5 * 60_000;
    }
    const d = decideWatchdogAlerts({
      now,
      runtimeUp: false,
      starvedSessionIds: ['sess-1'],
      prevState: state,
    });
    const types = d.alerts.map((a) => a.type).sort();
    expect(types).toEqual(['container-runtime-down', 'work-starvation']);
  });
});

describe('watchdog state persistence', () => {
  const stateFile = path.join(TMP_DATA_DIR, 'roundtrip-state.json');

  afterEach(() => {
    fs.rmSync(stateFile, { force: true });
  });

  it('round-trips state through the file', () => {
    const state: WatchdogState = {
      alerts: { 'container-runtime-down': { firstDetectedAt: T0, lastAlertAt: T0 } },
      starvationTicks: { 'sess-9': 2 },
    };
    saveWatchdogState(stateFile, state);
    expect(loadWatchdogState(stateFile)).toEqual(state);
  });

  it('missing file yields fresh state', () => {
    expect(loadWatchdogState(path.join(TMP_DATA_DIR, 'nope.json'))).toEqual(freshWatchdogState());
  });

  it('malformed file yields fresh state without throwing', () => {
    fs.writeFileSync(stateFile, '{not json', 'utf8');
    expect(loadWatchdogState(stateFile)).toEqual(freshWatchdogState());
  });

  it('structurally wrong JSON yields fresh state', () => {
    fs.writeFileSync(stateFile, JSON.stringify([1, 2, 3]), 'utf8');
    expect(loadWatchdogState(stateFile)).toEqual(freshWatchdogState());
  });
});

describe('startHostWatchdog — degraded startup', () => {
  const deliver =
    vi.fn<
      (
        channelType: string,
        platformId: string,
        threadId: string | null,
        kind: string,
        content: string,
      ) => Promise<string | undefined>
    >(async () => undefined);

  beforeEach(() => {
    vi.clearAllMocks();
    fs.rmSync(path.join(TMP_DATA_DIR, 'watchdog-state.json'), { force: true });
    fs.rmSync(path.join(TMP_DATA_DIR, 'host-heartbeat'), { force: true });
    process.env.WATCHDOG_ALERT_CHANNEL_TYPE = 'discord';
    process.env.WATCHDOG_ALERT_PLATFORM_ID = 'discord:guild:chan';
    setDeliveryAdapter({ deliver });
  });

  afterEach(() => {
    stopHostWatchdog();
    delete process.env.WATCHDOG_ALERT_CHANNEL_TYPE;
    delete process.env.WATCHDOG_ALERT_PLATFORM_ID;
  });

  it('runtime down at boot: alerts through the adapter, writes heartbeat, does not throw', async () => {
    vi.mocked(isContainerRuntimeUp).mockReturnValue(false);

    startHostWatchdog();
    await vi.waitFor(() => expect(deliver).toHaveBeenCalledTimes(1));

    const [channelType, platformId, threadId, kind, content] = deliver.mock.calls[0];
    expect(channelType).toBe('discord');
    expect(platformId).toBe('discord:guild:chan');
    expect(threadId).toBeNull();
    expect(kind).toBe('text');
    expect(JSON.parse(content).text).toContain('container runtime');

    // Heartbeat written even while degraded — the tripwire depends on it.
    expect(fs.existsSync(path.join(TMP_DATA_DIR, 'host-heartbeat'))).toBe(true);
    // No recovery actions while down.
    expect(cleanupOrphans).not.toHaveBeenCalled();
    expect(resetAllContainerStatuses).not.toHaveBeenCalled();
  });

  it('runtime up at boot: runs orphan cleanup + status reset once, no alert', async () => {
    vi.mocked(isContainerRuntimeUp).mockReturnValue(true);

    startHostWatchdog();
    await vi.waitFor(() => expect(cleanupOrphans).toHaveBeenCalledTimes(1));
    expect(resetAllContainerStatuses).toHaveBeenCalledTimes(1);
    expect(deliver).not.toHaveBeenCalled();
  });

  it('recovery after degraded boot: one recovery notice plus cleanup', async () => {
    vi.mocked(isContainerRuntimeUp).mockReturnValue(false);
    startHostWatchdog();
    await vi.waitFor(() => expect(deliver).toHaveBeenCalledTimes(1));
    // The tick persists state after sending; wait for it so the restarted
    // watchdog sees the prior down state (persistence is the contract).
    await vi.waitFor(() =>
      expect(fs.existsSync(path.join(TMP_DATA_DIR, 'watchdog-state.json'))).toBe(true),
    );
    stopHostWatchdog();

    // Runtime comes back; state survived on disk.
    vi.mocked(isContainerRuntimeUp).mockReturnValue(true);
    startHostWatchdog();
    await vi.waitFor(() => expect(deliver).toHaveBeenCalledTimes(2));
    const recovered = JSON.parse(deliver.mock.calls[1][4]);
    expect(recovered.text).toContain('RECOVERED');
    expect(cleanupOrphans).toHaveBeenCalledTimes(1);
    expect(resetAllContainerStatuses).toHaveBeenCalledTimes(1);
  });

  it('unset destination degrades to log-only without error', async () => {
    delete process.env.WATCHDOG_ALERT_CHANNEL_TYPE;
    delete process.env.WATCHDOG_ALERT_PLATFORM_ID;
    vi.mocked(isContainerRuntimeUp).mockReturnValue(false);

    startHostWatchdog();
    await vi.waitFor(() =>
      expect(fs.existsSync(path.join(TMP_DATA_DIR, 'host-heartbeat'))).toBe(true),
    );
    expect(deliver).not.toHaveBeenCalled();
  });
});
