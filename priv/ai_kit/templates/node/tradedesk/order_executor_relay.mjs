import { defineRelay } from '@flezha/sdk';

import { ConfirmFill } from './confirm_fill.mjs';
import { PlaceOrder } from './place_order.mjs';
import { RecordOrder } from './record_order.mjs';
import { ReserveFunds } from './reserve_funds.mjs';

/**
 * The desk's steps relay: one instance per order, which runs its steps
 * once and then ends.
 *
 * A steps relay declares its steps the way a flow relay declares
 * its graph, on the same builder, and the controller schedules, restarts and
 * relocates it in the same way. What differs is the lifecycle: each step's
 * output is written down as it commits, so an instance that is interrupted
 * resumes at the step it reached rather than from the beginning, and the
 * instance ends by itself once the chain has run.
 *
 * The order an instance is started with is its first message on `order`, which
 * names no codec, so it arrives as the object its JSON decodes to:
 * `--text '{"symbol": "EUR/USD", "quantity": 10}' --channel order`.
 *
 * When a step fails and its retries are spent, the instance ends abnormally
 * instead, and the default rollback undoes every step that finished, in
 * reverse, through their `compensate` hooks: the order is cancelled, then the
 * money is released. A step with no `compensate` is skipped, which is why the
 * desk's book is left alone.
 *
 * Two steps in order, then a fan-out: confirming the fill and booking the order
 * are two independent consequences of the placement, and neither waits for the
 * other. Each arm of `branch` receives the same edge, so the declaration is
 * exactly two `into` calls on the placed channel, written as one.
 *
 * Nothing ends on an output: the last step ends the instance with its result,
 * which `flz relay info` shows. Nothing names a channel either, so each one
 * carries the name of the step that produces it.
 */
export default defineRelay({
  factoryId: 'order_executor',
  // one instance per order; each step runs once
  // a failed step undoes the finished ones
  steps: { each: 'once', onFailure: 'undo_all' },
  channels: (c) => {
    c.input('order') // the order to carry out
      .into(ReserveFunds) // step one
      .into(PlaceOrder) // step two, once step one is done
      .branch([
        (fill) => fill.into(ConfirmFill), // two arms, side by side
        (book) => book.into(RecordOrder),
      ]);
  },
});
