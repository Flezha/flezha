/**
 * Timer surface, injected so tests drive scheduling without wall-clock waits.
 *
 * Shared by every component that originates timed traffic: the per-connection
 * subscription generators and the manual bulk driver.
 */
export interface Timers {
  set(fn: () => void, ms: number): unknown;
  clear(handle: unknown): void;
}

export const REAL_TIMERS: Timers = {
  set: (fn, ms) => setInterval(fn, ms),
  clear: (handle) => clearInterval(handle as NodeJS.Timeout),
};
