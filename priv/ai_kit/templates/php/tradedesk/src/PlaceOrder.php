<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * Step two: place the order with the paper exchange, keeping the id it answers with, and cancel
 * the order again if it is undone. It has the same shape as step one.
 */
#[Tasklet\Spec(name: 'place_order')]
final class PlaceOrder extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['order_id' => ''];                           // what this step remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['order_id'] = $change['order_id'];            // how the id is written down
    }

    // The step: place the order under an id, remember the id, and hand the order on.
    public function run(mixed $input, Ctx $ctx): void
    {
        $orderId = 'ord-' . $ctx->stableId();
        $ctx->change(['order_id' => $orderId]);
        $ctx->emit(['order' => $input['order'], 'order_id' => $orderId]);
    }

    // The undo: cancel the order, by the id the state kept.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo "[undo] cancelled {$ctx->state['order_id']}\n";
    }
}
