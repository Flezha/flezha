import { defineTasklet } from '@flezha/sdk';

const BUY_ABOVE_PCT = 2.0;
const UNITS_PER_PERCENT = 10;

/**
 * Buys when a price is far enough above its symbol's average.
 *
 * It remembers nothing: the move it is handed already carries the average, so
 * the decision is a function of one value, and a restart costs this stage
 * nothing because there is nothing to bring back. That is why it writes no
 * `initState`, beside `TrackPrices`, which keeps the window.
 *
 * It does two things per input, both through the context: it starts an
 * relay that runs in steps and it emits.
 */
export const BuyAboveAverage = defineTasklet({
  name: 'buy_above_average',

  run: (move, ctx) => {
    const percent = ((move.price - move.average) / move.average) * 100.0;
    if (percent < BUY_ABOVE_PCT) return;

    const quantity = Math.round(percent * UNITS_PER_PERCENT); // a stronger move buys more

    const order = { symbol: move.symbol, quantity };
    ctx.spawnSteps('order_executor', order); // one steps relay per order

    ctx.emit(
      `${move.symbol} ${move.price.toFixed(4)} is ${percent.toFixed(2)}% above its average ` +
        `of ${move.average.toFixed(4)}, buying ${quantity}`,
    );
  },
});
