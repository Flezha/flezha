import { setTimeout as sleep } from 'node:timers/promises';

import { defineTasklet } from '@flezha/sdk';

const EXCHANGE_MAX = 100;

/**
 * Asks the exchange whether the order filled, on one arm of the fan-out. The
 * paper exchange rejects anything above its own size, which is what makes this
 * the step that fails, and takes ten seconds to answer, which leaves time to
 * stop the runner in the middle of an order.
 *
 * The wait is awaited, so it holds nothing: the event loop serves every other
 * cell meanwhile, and the price stream keeps flowing. A blocking wait would
 * stall every cell on the loop for the ten seconds.
 *
 * It remembers nothing and has nothing to undo, so it writes only `run`. It
 * ends the instance with a result rather than emitting one: the order's outcome
 * is the instance's own answer, which `flz relay info` reads back, so the
 * desk needs no output here.
 *
 * A failure here is retried first, and once the retries are spent the instance
 * ends abnormally. Everything that finished is then undone in reverse, across
 * both arms: the order is cancelled and the money released, while the desk's
 * book is skipped because it writes no `compensate`.
 */
export const ConfirmFill = defineTasklet({
  name: 'confirm_fill',

  run: async (placed, ctx) => {
    await sleep(10_000); // the paper exchange takes ten seconds to answer
    const { symbol, quantity } = placed.order;
    if (quantity > EXCHANGE_MAX) {
      // tried again, then the order is undone
      throw new Error(`the exchange fills at most ${EXCHANGE_MAX} a time, and this is ${quantity}`);
    }
    ctx.complete(`filled ${symbol} ${quantity} as ${placed.orderId}`);
  },
});
