import { defineTasklet } from '@flezha/sdk';

/**
 * Step two: place the order with the paper exchange, keeping the id it answers
 * with, and cancel the order again if it is undone. It has the same shape as
 * step one.
 */
export const PlaceOrder = defineTasklet({
  name: 'place_order',

  // What this step remembers, and how the id is written down.
  initState: () => ({ orderId: '' }),
  applyChange: (state, change) => {
    state.orderId = change.orderId;
  },

  // The step: place the order under an id, remember the id, and hand the order on.
  run: (reserved, ctx) => {
    const orderId = `ord-${ctx.stableId()}`;
    ctx.change({ orderId });
    ctx.emit({ order: reserved.order, orderId });
  },

  // The undo: cancel the order, by the id the state kept.
  compensate: (_outputs, ctx) => {
    console.log(`[undo] cancelled ${ctx.state.orderId}`);
  },
});
