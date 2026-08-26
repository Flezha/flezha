/**
 * Event-loop delay observation.
 *
 * A synchronous compute-bound handler is the one failure this SDK cannot
 * prevent: the loop it runs on is the loop that would dispatch everything
 * else, and there is no way to preempt JavaScript. What the SDK can do is name
 * it at its source, so the diagnosis is not left to a controller-side
 * `entity_dispatch_stalled` verdict half a minute later.
 */

import { monitorEventLoopDelay, type IntervalHistogram } from 'node:perf_hooks';

import { log } from './logger.js';

/**
 * The delay past which a loop is reported. Well above scheduling noise and a
 * garbage-collection pause, and well below the controller's stall verdict, so
 * the host's warning arrives first.
 */
export const DELAY_WARN_MS = 500;

/** How often the histogram is read and reset. */
const SAMPLE_INTERVAL_MS = 5_000;

export class LoopWatch {
  private histogram: IntervalHistogram | null = null;
  private timer: NodeJS.Timeout | null = null;
  private lastProcedure = '<none>';

  constructor(private readonly loopName: string) {}

  /** The procedure most recently dispatched here — what the warning names. */
  observe(procedure: string): void {
    this.lastProcedure = procedure;
  }

  start(): void {
    if (this.timer) return;
    this.histogram = monitorEventLoopDelay({ resolution: 20 });
    this.histogram.enable();
    this.timer = setInterval(() => this.sample(), SAMPLE_INTERVAL_MS);
    // The watch must not be the reason a drained process stays alive.
    this.timer.unref();
  }

  private sample(): void {
    const histogram = this.histogram;
    if (!histogram) return;
    const worstMs = histogram.max / 1e6;
    histogram.reset();
    if (worstMs < DELAY_WARN_MS) return;
    log().warn(
      `dispatch loop ${this.loopName} was blocked for ${worstMs.toFixed(0)}ms while running ` +
        `\`${this.lastProcedure}\`. A handler that computes synchronously holds the loop that ` +
        'would dispatch everything else. Move dispatch onto worker threads — .workers(n) or ' +
        'FLEZHA_NODE_WORKERS — so the work has its own loops and a wedged one can be replaced.',
    );
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    this.histogram?.disable();
    this.histogram = null;
  }
}
