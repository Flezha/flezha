import { defineTasklet } from '@flezha/sdk';

/**
 * Step one: put the money for the order aside, and release it again if the
 * order is undone.
 *
 * The reservation id comes from `ctx.stableId()`, and the step both remembers
 * it and hands it on, beside the order.
 */
export const ReserveFunds = defineTasklet({
  name: 'reserve_funds',

  // What this step remembers, and how the id is written down.
  initState: () => ({ id: '' }),
  applyChange: (state, change) => {
    state.id = change.id;
  },

  // The step: reserve the money under an id, remember the id, and hand the order on.
  run: (order, ctx) => {
    const id = `res-${ctx.stableId()}`; // the same id when the step runs again
    ctx.change({ id });
    ctx.emit({ order, reservationId: id });
  },

  // The undo: release the money, by the id the state kept.
  compensate: (_outputs, ctx) => {
    console.log(`[undo] released ${ctx.state.id}`);
  },
});
