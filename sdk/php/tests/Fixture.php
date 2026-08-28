<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Clock;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\Overflow;
use Flezha\Procedure;
use Flezha\StreamOperative;

/**
 * The procedures and operatives the unit suite declares against.
 *
 * One file, so a test that needs "a stateful keyed procedure" names one rather
 * than declaring a fourth almost-identical class.
 */
#[Proc]
final class Echoer extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Proc(name: 'counter')]
final class Counter extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['count' => 0, 'seen' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['count']++;
        $state['seen'][] = $change['push'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push' => $input]);

        return null;
    }

    public function onTimer(\Flezha\Timer $timer, Ctx $ctx): void
    {
        $ctx->change(['push' => 'timer:' . $timer->scheduledAt]);
    }
}

#[Proc(dynamicEntities: true)]
final class Keyed extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['key' => $entityKey, 'values' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['values'][] = $change;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change($input);
        $ctx->emit($input, key: (string) $ctx->entityKey);

        return null;
    }
}

#[Operative(factoryId: 'echo_operative')]
final class EchoOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')->into(Echoer::class)->named('out')->sink('echo_sink');
    }
}

#[Operative(factoryId: 'counter_operative')]
final class CounterOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')->into(Counter::class)->named('out')->sink('counter_sink');
    }
}

#[Operative(factoryId: 'keyed_operative')]
final class KeyedOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')
            ->onFull(Overflow::DropNothing)
            ->filter(static fn (mixed $v): bool => $v !== 'drop')
            ->keyBy(static fn (mixed $v): string => explode(':', (string) $v)[0])
            ->into(Keyed::class)
            ->named('out')
            ->keyed()
            ->sink('keyed_sink');
    }
}

#[Proc]
final class Poison extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        if ($input === 'poison') {
            throw new \Flezha\Exception\NonRetryableException('poisoned payload');
        }

        return $input;
    }
}

#[Operative(factoryId: 'poison_operative')]
final class PoisonOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')->into(Poison::class)->named('out')->sink('poison_sink');
    }
}

#[Proc(name: 'saga_step')]
final class SagaStep extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return ['step' => $input, 'receipt' => $ctx->stableId()];
    }

    public function compensate(array $outputs, Ctx $ctx): void
    {
        foreach ($outputs as $output) {
            $ctx->emit(['undo' => $output['receipt']]);
        }
    }
}

#[\Flezha\Attribute\Operative(factoryId: 'saga_operative')]
final class SagaOperative extends \Flezha\WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('orders')->into(SagaStep::class)->named('done')->sink('saga_out');
    }
}

#[\Flezha\Attribute\Operative(factoryId: 'run_sugar_operative')]
final class RunSugarOperative extends \Flezha\WorkflowOperative
{
    public static function run(mixed $args, \Flezha\WorkflowCtx $ctx): mixed
    {
        $ctx->complete(['seeded' => $args]);

        return null;
    }
}

#[Proc(name: 'timing', clock: Clock::EventTime)]
final class Timing extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['fired' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['fired'][] = $change;
    }

    public function eventTime(mixed $input): int
    {
        return (int) $input;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        // Both kinds, and a cancellation by the value a registration answered:
        // the processing-time one is anchored on `now()`, never an ambient
        // read, which is what makes a replayed invocation register the same
        // timestamp the live one did.
        $after = $ctx->timerAfter(1_000);
        $at = $ctx->timerAtEventTime($ctx->now() + 5_000);
        $ctx->cancelTimer('processing_time', $after);

        return null;
    }

    public function onTimer(\Flezha\Timer $timer, Ctx $ctx): void
    {
        $ctx->change(['kind' => $timer->kind, 'at' => $timer->scheduledAt, 'now' => $ctx->now()]);
    }
}
