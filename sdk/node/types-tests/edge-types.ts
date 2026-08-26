/**
 * Compile-time refusals, pinned.
 *
 * Rust pins its refusals with compile-fail tests; TypeScript can pin the same
 * class of defect with `@ts-expect-error`. Each directive below asserts that
 * the line beneath it does NOT compile — so a refusal that stops working
 * fails the build with "Unused '@ts-expect-error' directive" rather than
 * silently becoming a runtime error, or nothing at all.
 *
 * These are the defects static typing can see. Everything it cannot — cycles,
 * missing entry channels, unservable clocks, keyed-channel rules — is refused
 * at registration and covered by `test/authoring.test.mjs`.
 */

import { defineOperative, defineProcedure } from '../src/index.js';

interface Trade {
  symbol: string;
  price: number;
}

interface Candle {
  symbol: string;
  close: number;
}

const Ohlc = defineProcedure({
  name: 'ohlc',
  handle: (trade: Trade): Candle => ({ symbol: trade.symbol, close: trade.price }),
});

const Publish = defineProcedure({
  name: 'publish',
  handle: (candle: Candle): string => `${candle.symbol}:${candle.close}`,
});

const Count = defineProcedure({
  name: 'count',
  initialState: () => ({ total: 0 }),
  applyChange: (state, change: { by: number }) => {
    state.total += change.by;
  },
  handle: (_input: Trade, ctx) => {
    ctx.change({ by: 1 });
  },
});

// The topology this is all about: it compiles, and every refusal below is a
// deviation from it.
export const wellTyped = defineOperative({
  factoryId: 'well_typed',
  channels: (c) => {
    c.input<Trade>('trades').into(Ohlc).into(Publish).sink('out');
  },
});

export const refusals = defineOperative({
  factoryId: 'refusals',
  channels: (c) => {
    // A producer wired to a consumer whose declared input it does not satisfy.
    // @ts-expect-error the entry carries Trade; `Publish` consumes Candle
    c.input<Trade>('a').into(Publish);

    // The same defect one hop in: `Ohlc` emits Candle, `Ohlc` consumes Trade.
    // @ts-expect-error a Candle does not satisfy Trade
    c.input<Trade>('b').into(Ohlc).into(Ohlc, 'second');

    // A transform reshapes the edge, and the reshaped type is what is checked.
    c.input<Trade>('c')
      .map((trade) => ({ symbol: trade.symbol, close: trade.price }))
      .into(Publish);

    // @ts-expect-error the transform produced a Candle, not a Trade
    c.input<Trade>('d').map((trade) => ({ symbol: trade.symbol, close: trade.price })).into(Ohlc);

    // A routing key is a string.
    // @ts-expect-error keyBy answers a string, never the raw value
    c.input<Trade>('e').keyBy((trade) => trade.price).into(Ohlc);

    // A sink encoder is applied to the edge's own value.
    // @ts-expect-error the edge carries a string here, not a Trade
    c.input<Trade>('f').into(Ohlc).into(Publish).sink('g', (value: Trade) => Buffer.from(value.symbol));

    // A wrong overflow policy is a typo the compiler catches.
    // @ts-expect-error `drop_everything` is not an overflow policy
    c.input<Trade>('h').onFull('drop_everything').into(Ohlc);

    // And a change the fold does not accept.
    c.input<Trade>('i').into(Count);
  },
});

const BadChange = defineProcedure({
  name: 'bad_change',
  initialState: () => ({ total: 0 }),
  applyChange: (state, change: { by: number }) => {
    state.total += change.by;
  },
  handle: (_input: Trade, ctx) => {
    // @ts-expect-error the declared change carries `by`, not `amount`
    ctx.change({ amount: 1 });
  },
});

const BadEmit = defineProcedure<Trade, Candle>({
  name: 'bad_emit',
  handle: (trade, ctx) => {
    // @ts-expect-error the declared output is Candle, not a string
    ctx.emit(trade.symbol);
  },
});

export const moreRefusals = defineOperative({
  factoryId: 'more_refusals',
  channels: (c) => {
    c.input<Trade>('j').branch([(edge) => edge.into(BadChange), (edge) => edge.into(BadEmit)]);
  },
});
