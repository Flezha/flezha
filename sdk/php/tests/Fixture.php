<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Clock;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\Graph\Edge;
use Flezha\Overflow;
use Flezha\Procedure;
use Flezha\StreamOperative;
use Flezha\WorkflowOperative;

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
        $c->input('input')->into(Echoer::class)->sink('out');
    }
}

#[Operative(factoryId: 'counter_operative')]
final class CounterOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')->into(Counter::class)->sink('out');
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
            ->keyed()
            ->sink('out');
    }
}

/**
 * The keyed plugin emitter: one output channel, carrying the identity stamps.
 */
#[Proc(name: 'stamp_emitter', dynamicEntities: true)]
final class StampEmitter extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        if ($input === 'twice') {
            $ctx->emit('a', channel: 'keyed_out');
            $ctx->emit('b', channel: 'keyed_out');
        } elseif ($input === 'spoof') {
            $ctx->emit('v', channel: 'keyed_out', metadata: ['flz.seq' => '999.0']);
        } elseif ($input === 'fanned') {
            $ctx->emit('x', channel: 'keyed_out');
        } else {
            $ctx->emit('k', channel: 'keyed_out', metadata: ['user' => 'meta']);
        }

        return null;
    }
}

/**
 * One keyed plugin channel: the topology every SDK can express, and where the
 * per-emit ordinal has to be right.
 */
#[Operative(factoryId: 'stamp_operative')]
final class StampOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')
            ->keyBy(static fn (mixed $v): string => 'ek')
            ->into(StampEmitter::class)
            ->keyed()
            ->sink('keyed_out');
    }
}

#[Proc]
final class PlainEmitter extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit('p', channel: 'plain_out');

        return null;
    }
}

#[Proc]
final class FanTarget extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return null;
    }
}

/**
 * A procedure owns ONE output channel, so a keyed and a non-keyed plugin
 * channel are two procedures. The `flatMap` arm fans onto a CONSUMER edge,
 * which is where a transform chain lives: the plugin frame is serialized once
 * per emit from the value as emitted, in every SDK.
 *
 * The sink is attached last, on the held handle. A transform declared behind an
 * attached sink is `TransformAfterTerminal` in every SDK — the sink serializes
 * the channel's own value, so the order says which value that is.
 */
#[Operative(factoryId: 'mixed_stamp_operative')]
final class MixedStampOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')
            ->keyBy(static fn (mixed $v): string => 'ek')
            ->branch(
                static fn (Edge $e) => $e->into(PlainEmitter::class)
                    ->sink('plain_out'),
                static function (Edge $e): void {
                    $keyed = $e->into(StampEmitter::class)->keyed();
                    $keyed->flatMap(static fn (mixed $v): array => [$v . '1', $v . '2'])
                        ->into(FanTarget::class);
                    $keyed->sink('keyed_out');
                },
            );
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
        $c->input('input')->into(Poison::class)->sink('out');
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
        $c->input('orders')->into(SagaStep::class)->sink('done');
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

// -- the structured-workflow emit arm ----------------------------------------
//
// One released value is ONE edge output on a workflow class, untransformed and
// unkeyed, naming the producing channel and its first consumer: the driver fans
// it out from the journaled handoff. On a stream class the same topology
// releases one output per consumer, transformed and keyed. The plugin output
// rides alongside either way.

#[Proc(name: 'wf_producer')]
final class WfProducer extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit($input);

        return null;
    }
}

/** What a consumer of the `handoff` channel was handed, in delivery order. */
final class Delivered
{
    /** @var list<mixed> */
    public static array $values = [];
}

#[Proc(name: 'wf_b')]
final class WfB extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        Delivered::$values[] = $input;

        return null;
    }
}

#[Proc(name: 'wf_c')]
final class WfC extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        Delivered::$values[] = $input;

        return null;
    }
}

trait WorkflowEmitGraph
{
    public static function channels(Builder $c): void
    {
        $edge = $c->input('in')->into(WfProducer::class)->named('handoff');
        $edge->map(static fn (mixed $v): mixed => $v + 1)->into(WfB::class);
        $edge->into(WfC::class);
        $edge->sink();
    }
}

/** An entry naming no codec, into a consumer that records what it was handed. */
#[Operative(factoryId: 'json_entry_operative')]
final class JsonEntryOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in')->into(WfB::class);
    }
}

#[Operative(factoryId: 'wf_emit')]
final class WfEmitOperative extends WorkflowOperative
{
    use WorkflowEmitGraph;
}

#[Operative(factoryId: 'wf_emit_stream')]
final class WfEmitStreamOperative extends StreamOperative
{
    use WorkflowEmitGraph;
}
