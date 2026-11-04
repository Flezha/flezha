<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * Step one: put the money for the order aside, and release it again if the order is undone.
 *
 * The reservation id comes from `$ctx->stableId()`, and the step both remembers it and hands it
 * on, beside the order.
 */
#[Tasklet\Spec(name: 'reserve_funds')]
final class ReserveFunds extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['id' => ''];                                 // what this step remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['id'] = $change['id'];                        // how the id is written down
    }

    // The step: reserve the money under an id, remember the id, and hand the order on.
    public function run(mixed $input, Ctx $ctx): void
    {
        $id = 'res-' . $ctx->stableId();                     // the same id when the step runs again
        $ctx->change(['id' => $id]);
        $ctx->emit(['order' => $input, 'reservation_id' => $id]);
    }

    // The undo: release the money, by the id the state kept.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo "[undo] released {$ctx->state['id']}\n";
    }
}
