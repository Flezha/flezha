import { defineTasklet } from '@flezha/sdk';

/**
 * Writes the placed order into the desk's book, on its own arm of the fan-out.
 *
 * Confirming the fill and booking the order are two independent consequences
 * of the same event, and neither waits for the other, which is what a branch is
 * for.
 *
 * The plainest shape a tasklet has: a value in, nothing kept, nothing out.
 *
 * It writes no `compensate` on purpose. A rollback undoes what the desk did to
 * the world, not what it wrote down about it: the attempt happened, and the
 * book says so even when the order was cancelled again.
 */
export const RecordOrder = defineTasklet({
  name: 'record_order',

  run: (placed) => {
    const { symbol, quantity } = placed.order;
    console.log(`[book] ${symbol} ${quantity} as ${placed.orderId}`);
  },
});
