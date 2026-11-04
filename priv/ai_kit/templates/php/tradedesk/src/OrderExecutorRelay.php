<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Attribute\Relay;
use Flezha\ChannelBuilder;
use Flezha\Stream;

/**
 * The desk's steps relay: one instance per order, which runs its steps and then ends.
 *
 * A steps relay declares its steps the way a flow relay declares its graph, on the
 * same builder, and the controller schedules, restarts and relocates it in the same way. What
 * differs is the lifecycle: each step's output is written down as it commits, so an instance that
 * is interrupted resumes at the step it reached rather than from the beginning.
 *
 * The order an instance is started with is its first message on `order`, which names no codec,
 * so it arrives as the array its JSON decodes to:
 * `--text '{"symbol": "EUR/USD", "quantity": 10}' --channel order`.
 *
 * `'each' => 'per_input'` keeps the instance alive once its steps have run, until `confirm_fill`'s
 * timer fires and ends it with `complete`. A `once` instance would end, timers and all, as soon as
 * the last step committed.
 *
 * When a step fails and its retries are spent, the instance ends abnormally instead, and the
 * default rollback undoes every step that finished, in reverse, through their `compensate`
 * methods: the order is cancelled, then the money is released. A step with no `compensate` is
 * skipped, which is why the desk's book is left alone.
 */
// one instance per order; it stays until confirm_fill completes it
// a failed step undoes the finished ones
#[Relay(factoryId: 'order_executor', run: ['steps' => ['each' => 'per_input', 'on_failure' => 'undo_all']])]
final class OrderExecutorRelay extends \Flezha\Relay
{
    /**
     * Two steps in order, then a fan-out: confirming the fill and booking the order are two
     * independent consequences of the placement, and neither waits for the other. Each arm of
     * `branch` receives the same edge, so the declaration is exactly two `into` calls on the
     * placed channel, written as one.
     *
     * Nothing ends on an output, and nothing names a channel, so each channel carries the name of
     * the step that produces it.
     */
    public static function channels(ChannelBuilder $c): void
    {
        $c->input('order')                                            // the order to carry out
            ->into(ReserveFunds::class)                               // step one
            ->into(PlaceOrder::class)                                 // step two, once step one is done
            ->branch(
                fn (Stream $fill) => $fill->into(ConfirmFill::class),   // two arms, side by side
                fn (Stream $book) => $book->into(RecordOrder::class),
            );
    }
}
