<?php

declare(strict_types=1);

namespace Flezha\Examples\Tradedesk;

use Flezha\Ctx;
use Flezha\Tasklet;
use Flezha\TimerFiring;

/**
 * Asks the exchange whether the order filled, on one arm of the fan-out. The paper exchange
 * rejects anything above its own size, which is what makes this the step that fails, and answers
 * ten seconds later, which leaves time to stop the runner in the middle of an order.
 *
 * The answer is a timer rather than a wait: a PHP runner serves every relay on one thread, so
 * `sleep()` here would hold up the price stream too. `run` keeps the order in the cell and asks
 * for a timer, and `onTimer` reads the answer. The timer is saved with the cell, so it still
 * fires after a runner restart. It ends the instance with a result rather than emitting one: the
 * order's outcome is the instance's own answer, which `flz relay info` reads back.
 *
 * A refusal is retried first, and once the retries are spent the instance ends abnormally.
 * Everything that finished is then undone in reverse, across both arms: the order is cancelled and
 * the money released, while the desk's book is skipped because it writes no `compensate`.
 */
#[Tasklet\Spec(name: 'confirm_fill')]
final class ConfirmFill extends Tasklet
{
    private const EXCHANGE_MAX = 100;

    public function initState(?string $cellKey): mixed
    {
        return ['placed' => null];                           // the order the exchange is asked about
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['placed'] = $change;
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->change($input);
        $ctx->timerAfter(10_000);                            // the paper exchange answers ten seconds later
    }

    public function onTimer(TimerFiring $timer, Ctx $ctx): void
    {
        $placed = $ctx->state['placed'];
        $quantity = $placed['order']['quantity'];
        if ($quantity > self::EXCHANGE_MAX) {
            throw new \RuntimeException(                     // tried again, then the order is undone
                'the exchange fills at most ' . self::EXCHANGE_MAX . " a time, and this is $quantity",
            );
        }
        $ctx->complete("filled {$placed['order']['symbol']} $quantity as {$placed['order_id']}");
    }
}
