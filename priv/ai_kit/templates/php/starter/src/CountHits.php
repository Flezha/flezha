<?php

declare(strict_types=1);

namespace Flezha\Examples\Starter;

use Flezha\Attribute\Task;
use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * Counts the visits of one page and answers the new count.
 *
 * The relay keys the channel by page, so each page has a cell of its own, and
 * `['total' => ...]` is that cell's state. The state changes only through the
 * change `run` records, which `applyChange` applies, so a runner that restarts
 * finds the count it had.
 */
#[Task(name: 'count')] // state is saved under this name, so choose it once
final class CountHits extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['total' => 0]; // what each page's cell remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['total']++; // how a visit changes it
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->change([]);
        $ctx->emit(['page' => $input['page'], 'total' => $ctx->state['total']]);
    }
}
