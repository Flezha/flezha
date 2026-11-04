<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * Keeps a symbol's last ten prices, and compares each new trade with their average.
 *
 * One cell per symbol, because the relay keys the channel by symbol, and the window is that
 * cell's state. Every change to it is recorded as it happens, so a runner that is killed and
 * started again finds the window it had rather than an empty one.
 *
 * A trade is the array the exchange sent, of which the desk reads `symbol` and `price`; the
 * move it hands on is an array of the symbol, the price and the average.
 */
#[Tasklet\Spec(name: 'track_prices')]
final class TrackPrices extends Tasklet
{
    private const WINDOW = 10;

    public function initState(?string $cellKey): mixed
    {
        return ['prices' => []];                             // what each symbol's cell remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['prices'][] = $change['price'];               // how a price joins the window
        if (count($state['prices']) > self::WINDOW) {
            array_shift($state['prices']);
        }
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        if (!is_int($input['price']) && !is_float($input['price'])) {   // JSON checks no types
            throw new \InvalidArgumentException('the price is not a number: ' . json_encode($input['price']));
        }
        $prices = $ctx->state['prices'];
        if ($prices !== []) {                                // a symbol's first trade has nothing to compare with
            $ctx->emit([
                'symbol' => $input['symbol'],
                'price' => $input['price'],
                'average' => array_sum($prices) / count($prices),
            ]);
        }
        $ctx->change(['price' => $input['price']]);
    }
}
