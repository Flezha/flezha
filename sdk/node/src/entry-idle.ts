/**
 * Idle observation for entry channels: the policy half of the input-liveness
 * signal the core records.
 *
 * The core knows when a frame last arrived and owns the episode latch over it
 * (`flz_core::entry_input::IdleWatch`, reached by watch id); only the
 * application knows whether the silence since then is anomalous — a quiet
 * overnight market is not a dead feed. So the SDK ships no default threshold
 * and no default action: `Runner.onEntryIdle` takes both from the caller and
 * reports episodes.
 *
 * The tick is a timer on this loop, `unref`ed so a drained process still
 * exits. Under `workers(n)` the loop dispatches nothing and the tick is exact;
 * under `workers(0)` a synchronous handler holds the loop that would tick, so
 * an episode is reported at the first tick the loop reaches afterwards — and
 * once, because the latch compares stamps, not clock readings.
 */

import { log } from './logger.js';
import type { Native } from './native.js';

/**
 * What an application does about an entry channel that has gone quiet.
 *
 * Registered per channel with a threshold; the SDK reports **episodes**, not
 * ticks: `idle` fires once when the silence first exceeds the threshold and
 * not again while it continues, and `resumed` fires once when a frame finally
 * arrives, re-arming the next episode. Silence before the very first frame is
 * never an episode — an operative waiting for its subscriptions to take is not
 * an incident.
 *
 * Both calls run on the SDK's timer. A throw is caught and logged; delivery
 * and later episodes are unaffected.
 */
export interface IdleObserver {
  /** The channel has been silent for `gapMs`, which first exceeded the registered threshold. */
  idle(gapMs: number): void;

  /**
   * A frame arrived, ending an episode that had lasted `gapMs`. Optional — an
   * alarm that only needs raising implements `idle` alone.
   *
   * `gapMs` is measured to the newest frame the tick saw, so a burst that
   * resumes and continues within one tick reports the silence plus that burst
   * — bounded by the tick, i.e. at most a quarter of the threshold.
   */
  resumed?(gapMs: number): void;
}

/** The addon calls the watcher makes: the two clock reads and the three watch calls. */
export type IdleNative = Pick<
  Native,
  'monotonicNanos' | 'entryLastDeliveryNanos' | 'idleWatchNew' | 'idleWatchPoll' | 'idleWatchFree'
>;

/**
 * Tick floor and ceiling, the same in every SDK. The floor bounds how often a
 * fleet of observers wakes; the ceiling bounds how late an episode is reported.
 */
const TICK_MIN_MS = 250;
const TICK_MAX_MS = 1_000;

/** The two poll answers that are episode boundaries; `0` is none, `-1` an id no watch has. */
const IDLE = 1;
const RESUMED = 2;

/**
 * The tick a `thresholdMs` asks for: a quarter of it, so an episode is
 * reported within 25% of the threshold, bounded at both ends.
 */
export function tickPeriodMs(thresholdMs: number): number {
  return Math.min(TICK_MAX_MS, Math.max(TICK_MIN_MS, thresholdMs / 4));
}

/**
 * When `channel` last received a frame, or `null` before its first frame (and
 * for a name no declared operative has — both mean "nothing has arrived here").
 *
 * The core's stamp is monotonic; the wall-clock instant is derived from the
 * gap, so a clock step between then and now cannot skew it.
 */
export function lastEntryDelivery(native: IdleNative, channel: string): Date | null {
  const last = native.entryLastDeliveryNanos(channel);
  if (last === 0n) return null;
  const gap = native.monotonicNanos() - last;
  return new Date(Date.now() - Number(gap > 0n ? gap : 0n) / 1e6);
}

interface Watch {
  channel: string;
  id: bigint;
  observer: IdleObserver;
}

/** One runner's idle observers and the timer that evaluates them. */
export class EntryIdleWatcher {
  private readonly watches: Watch[] = [];
  private timer: NodeJS.Timeout | null = null;
  private periodMs = Infinity;

  constructor(private readonly native: IdleNative) {}

  /**
   * Register `observer` against `channel`. Legal before and after `run()`:
   * the core interns the observation by name, so a registration made before
   * the class is declared watches the same channel it later gets. Several
   * observers may watch one channel.
   */
  register(
    channel: string,
    thresholdMs: number,
    observer: IdleObserver | ((gapMs: number) => void),
  ): void {
    if (!(Number.isFinite(thresholdMs) && thresholdMs > 0)) {
      throw new RangeError(
        `onEntryIdle: the threshold must be a positive number of milliseconds, got ${thresholdMs}`,
      );
    }
    const id = this.native.idleWatchNew(channel, BigInt(Math.round(thresholdMs * 1e6)));
    if (id === 0n) throw new Error(`onEntryIdle: the core refused the channel name \`${channel}\``);
    this.watches.push({
      channel,
      id,
      observer: typeof observer === 'function' ? { idle: observer } : observer,
    });
    const wanted = tickPeriodMs(thresholdMs);
    if (wanted < this.periodMs) this.schedule(wanted);
  }

  /** Evaluate every watch once and deliver the episodes the core found. */
  tick(): void {
    for (const watch of this.watches) {
      const { status, gapNanos } = this.native.idleWatchPoll(watch.id);
      if (status === IDLE) this.deliver(watch, 'idle', gapNanos);
      else if (status === RESUMED) this.deliver(watch, 'resumed', gapNanos);
    }
  }

  /** Stop the tick and drop every registration, in the core too. Idempotent. */
  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.periodMs = Infinity;
    for (const watch of this.watches.splice(0)) this.native.idleWatchFree(watch.id);
  }

  /** Run at the shortest period any registration asks for. */
  private schedule(periodMs: number): void {
    if (this.timer) clearInterval(this.timer);
    this.periodMs = periodMs;
    this.timer = setInterval(() => this.tick(), periodMs);
    // The watch must not be the reason a drained process stays alive.
    this.timer.unref();
  }

  /**
   * Invoke the observer, contained: application code runs on the SDK's own
   * timer, so a throw must become a log line rather than a dead ticker.
   */
  private deliver(watch: Watch, episode: 'idle' | 'resumed', gapNanos: bigint): void {
    const gapMs = Number(gapNanos) / 1e6;
    try {
      if (episode === 'idle') watch.observer.idle(gapMs);
      else watch.observer.resumed?.(gapMs);
    } catch (error) {
      const reason = error instanceof Error ? error.message : String(error);
      log().warn(
        `entry-idle observer threw on the ${episode} episode of channel '${watch.channel}': ${reason}`,
      );
    }
  }
}
