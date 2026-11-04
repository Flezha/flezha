import { defineTasklet } from '@flezha/sdk';

const WINDOW = 10;

/**
 * Keeps a symbol's last ten prices, and compares each new trade with their
 * average.
 *
 * One cell per symbol, because the relay keys the channel by symbol, and the
 * window is that cell's state. Every change to it is recorded as it happens, so
 * a runner that is killed and started again finds the window it had rather
 * than an empty one.
 *
 * A trade is the object the exchange sent, of which the desk reads `symbol` and
 * `price`; the move it hands on is an object of the symbol, the price and the
 * average.
 */
export const TrackPrices = defineTasklet({
  name: 'track_prices',

  initState: () => ({ prices: [] }), // what each symbol's cell remembers

  applyChange: (state, change) => {
    state.prices.push(change.price); // how a price joins the window
    if (state.prices.length > WINDOW) state.prices.shift();
  },

  run: (trade, ctx) => {
    const { price } = trade;
    if (typeof price !== 'number') {
      throw new TypeError(`the price is not a number: ${JSON.stringify(price)}`); // JSON checks no types
    }
    const { prices } = ctx.state;
    if (prices.length > 0) {
      // a symbol's first trade has nothing to compare with
      const average = prices.reduce((sum, each) => sum + each, 0) / prices.length;
      ctx.emit({ symbol: trade.symbol, price, average });
    }
    ctx.change({ price });
  },
});
