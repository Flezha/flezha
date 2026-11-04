<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * Buys when a price is far enough above its symbol's average.
 *
 * It remembers nothing: the move it is handed already carries the average, so the decision is a
 * function of one value, and a restart costs this stage nothing because there is nothing to bring
 * back. That is why it writes no `initState`, beside {@see TrackPrices}, which keeps the window.
 *
 * It does two things per input, both through the context: it starts a steps relay and it
 * emits.
 */
#[Tasklet\Spec(name: 'buy_above_average')]
final class BuyAboveAverage extends Tasklet
{
    private const BUY_ABOVE_PCT = 2.0;
    private const UNITS_PER_PERCENT = 10;

    public function run(mixed $input, Ctx $ctx): void
    {
        $percent = ($input['price'] - $input['average']) / $input['average'] * 100.0;
        if ($percent < self::BUY_ABOVE_PCT) {
            return;
        }
        $quantity = (int) round($percent * self::UNITS_PER_PERCENT);   // a stronger move buys more

        $order = ['symbol' => $input['symbol'], 'quantity' => $quantity];
        $ctx->spawnSteps('order_executor', $order);        // one steps relay per order

        $ctx->emit(sprintf(
            '%s %.4f is %.2f%% above its average of %.4f, buying %d',
            $input['symbol'],
            $input['price'],
            $percent,
            $input['average'],
            $quantity,
        ));
    }
}
