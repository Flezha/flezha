// A worked operative: one cell per user, journaled state, an idle timer, and
// an emit on close.
//
// Everything the SDK asks an author to know is here — the change fold, the
// deterministic clock, keying on the wiring, and a timer that closes a session
// when its user goes quiet.

import { defineStream, defineTasklet } from '@flezha/sdk';

/** Milliseconds of silence after which a session is closed. */
const IDLE_MS = 30_000n;

/**
 * One session per user id.
 *
 * State is mutated only through `ctx.change`: the fold below is what rebuilds
 * a session after a restart, so a direct assignment would be live now and gone
 * then. `ctx.now()` rather than `Date.now()`, because a replay must register
 * the same deadline the live run did.
 */
export const Session = defineTasklet({
  name: 'session',
  // Cells appear on first sight of a key and are evicted after an idle
  // spell. The key itself comes from the wiring, below.
  dynamicCells: { idleTtlMs: 300_000 },

  initialState: () => ({ events: [], deadline: 0n, closed: false }),

  applyChange: (state, change) => {
    switch (change.kind) {
      case 'touch':
        state.events.push(change.event);
        state.deadline = change.deadline;
        state.closed = false;
        break;
      case 'close':
        state.closed = true;
        break;
      default:
        throw new Error(`session: no fold for change ${JSON.stringify(change)}`);
    }
  },

  run: (event, ctx) => {
    // Cancel the deadline this input replaces, then arm the next one. Both are
    // anchored on the core-evaluated clock, so a replay arms the same instant.
    if (ctx.state.deadline !== 0n) {
      ctx.cancelTimer('processing_time', ctx.state.deadline);
    }
    const deadline = ctx.timerAfter(IDLE_MS);
    ctx.change({ kind: 'touch', event: event.action, deadline });
  },

  onTimer: (firing, ctx) => {
    if (ctx.state.closed) return;
    ctx.change({ kind: 'close' });
    // `ctx.now()` inside a timer callback IS the scheduled time, never the
    // wall clock at firing — which is what makes the emitted summary the same
    // on a replay as it was live.
    ctx.emit({
      user: ctx.cellKey,
      events: ctx.state.events.length,
      closedAt: firing.scheduledAt.toString(),
      id: ctx.stableId(),
    });
  },
});

export default defineStream({
  factoryId: 'sessions',
  channels: (c) => {
    // Neither channel names a codec: the activity event is read as JSON, and
    // the summary object is sent as JSON.
    c.input('activity')
      // Keying is declared on the wiring, which is what makes `Session`
      // dynamic: one cell per user, created on first sight.
      .keyBy((event) => event.user)
      .into(Session)
      .keyed()
      .sink('closed_sessions');
  },
});
