import { Timers } from './timers';

/**
 * Timer stub: records what was scheduled so a test fires it by hand instead of
 * waiting on the clock. Shared by every suite that drives timed traffic.
 */
export class FakeTimers implements Timers {
  /** Handle -> the scheduled callback and its interval. Presence means active. */
  readonly active = new Map<number, { fn: () => void; ms: number }>();
  private next = 1;

  set(fn: () => void, ms: number): unknown {
    const handle = this.next++;
    this.active.set(handle, { fn, ms });
    return handle;
  }

  clear(handle: unknown): void {
    this.active.delete(handle as number);
  }

  /**
   * Fire every still-active timer, `times` rounds. The active set is re-read
   * each round, so a timer cleared by its own callback does not fire again.
   */
  tick(times = 1): void {
    for (let round = 0; round < times; round++) {
      [...this.active.values()].forEach((timer) => timer.fn());
    }
  }

  /** One round. */
  fireAll(): void {
    this.tick(1);
  }

  /** The interval of the only active timer, for a test that scheduled one. */
  onlyInterval(): number {
    const intervals = [...this.active.values()].map((timer) => timer.ms);
    if (intervals.length !== 1) {
      throw new Error(`expected exactly one active timer, found ${intervals.length}`);
    }
    return intervals[0];
  }
}
