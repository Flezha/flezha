<?php

declare(strict_types=1);

/**
 * The durable-loop workflow twins — the chat loop with suspension, its
 * stateless twin and the rewindable variant — and the chains and loop
 * `mix bench.workflow` drives. The reference's own partition
 * (test/apps/rust_runner/src/workflow_loop.rs); loaded after
 * fixtures_workflow.php, whose gate and bases these share.
 */

namespace Flezha\TestApp;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Attribute\Workflow;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\LoopMode;
use Flezha\Procedure;
use Flezha\WorkflowOperative;

// -- the durable loop family -------------------------------------------------
//
// chat_loop_workflow is the durable-loop + suspension shape
// (workflow_loop.rs:8). One iteration per inbox item: TURN commits the turn to
// Entity state, so a resumed iteration is visible in the WAL, and REPLY emits
// the answer to the sink. Between turns the loop finds an empty inbox at a
// committed boundary and PARKS — the instance is evicted and rehydrated by the
// next message. The turn whose payload is the crash turn parks while the
// release variable is unset, so a SIGKILL lands with the earlier iterations
// durable and that one in flight. Keying the park on the payload rather than on
// an invocation count keeps it deterministic across the park/rehydrate cycles,
// which re-run the driver but not the turns.
//
// stateless_loop_workflow is the same loop MINUS its stateful first procedure:
// ChatReply alone holds no per-Entity state, so nothing writes an Entity WAL
// and the instance's proc-journal reaches the driver only if it rides the bind
// itself.
//
// chat_rewind_workflow is the loop whose replying procedure can be undone. The
// undo handle lives in Entity state, because a rewind's compensation is rebuilt
// from the journal and is handed NO retained outputs, so the hook reads the
// last thing it said, retracts it and pops it.

#[Proc(name: 'chat_turn')]
final class ChatTurn extends DurableStep
{
    public const CRASH_TURN = 'mid-crash';

    protected function gated(mixed $input): bool
    {
        return $input === self::CRASH_TURN;
    }

    /**
     * Keyed by the turn, so the record count is the number of DISTINCT turns
     * that ran — a re-run of a committed iteration would show up.
     */
    protected function changeValue(mixed $input): string
    {
        return (string) $input;
    }
}

#[Proc(name: 'chat_reply')]
final class ChatReply extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit("reply:$input");

        return null;
    }
}

#[Proc(name: 'chat_reply_undoable')]
final class ChatReplyUndoable extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['said' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        if (array_key_exists('said', $change)) {
            $state['said'][] = $change['said'];

            return;
        }
        array_pop($state['said']);
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['said' => $input]);
        $ctx->emit("reply:$input");

        return null;
    }

    public function compensate(array $outputs, Ctx $ctx): void
    {
        $said = $ctx->state['said'];
        if ($said === []) {
            return;
        }
        $turn = $said[array_key_last($said)];
        $ctx->change(['retracted' => true]);
        $ctx->emit("retract:$turn");
    }
}

#[Operative(factoryId: 'chat_loop_workflow')]
// A chat session ends on an external stop, not on its own; the duration
// backstop keeps a stray instance from outliving the test run.
#[Workflow(loopMode: LoopMode::PerInput, maxDurationMs: 600_000)]
final class ChatLoopWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(ChatTurn::class)->named('mid')
            ->into(ChatReply::class)->named('out')
            ->sink('chat_sink', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'stateless_loop_workflow')]
#[Workflow(loopMode: LoopMode::PerInput, maxDurationMs: 600_000)]
final class StatelessLoopWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(ChatReply::class)->named('out')
            ->sink('chat_sink', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'chat_rewind_workflow')]
#[Workflow(loopMode: LoopMode::PerInput, maxDurationMs: 600_000)]
final class ChatRewindWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(ChatTurn::class)->named('mid')
            ->into(ChatReplyUndoable::class)->named('out')
            ->sink('chat_sink', Wire::bytes(...));
    }
}

// -- the bench chains --------------------------------------------------------
//
// What `mix bench.workflow --scenario chain` drives (workflow_loop.rs:232): K
// durable pass-through steps in a line, the payload unchanged, so the sink
// receives the seed input and the bench reads its own seq and send timestamp
// back. Each chain position needs its own procedure and a PHP procedure is a
// class, so the sixteen are written out; the edge names follow the reference,
// which spells them from the step TYPE rather than from the procedure name.

#[Proc(name: 'bench_s0')]
final class BenchS0 extends DurableStep
{
}

#[Proc(name: 'bench_s1')]
final class BenchS1 extends DurableStep
{
}

#[Proc(name: 'bench_s2')]
final class BenchS2 extends DurableStep
{
}

#[Proc(name: 'bench_s3')]
final class BenchS3 extends DurableStep
{
}

#[Proc(name: 'bench_s4')]
final class BenchS4 extends DurableStep
{
}

#[Proc(name: 'bench_s5')]
final class BenchS5 extends DurableStep
{
}

#[Proc(name: 'bench_s6')]
final class BenchS6 extends DurableStep
{
}

#[Proc(name: 'bench_s7')]
final class BenchS7 extends DurableStep
{
}

#[Proc(name: 'bench_s8')]
final class BenchS8 extends DurableStep
{
}

#[Proc(name: 'bench_s9')]
final class BenchS9 extends DurableStep
{
}

#[Proc(name: 'bench_s10')]
final class BenchS10 extends DurableStep
{
}

#[Proc(name: 'bench_s11')]
final class BenchS11 extends DurableStep
{
}

#[Proc(name: 'bench_s12')]
final class BenchS12 extends DurableStep
{
}

#[Proc(name: 'bench_s13')]
final class BenchS13 extends DurableStep
{
}

#[Proc(name: 'bench_s14')]
final class BenchS14 extends DurableStep
{
}

#[Proc(name: 'bench_s15')]
final class BenchS15 extends DurableStep
{
}

/** A linear chain `in → BenchS0 → … → bench_sink`, STEPS positions long. */
abstract class BenchChain extends WorkflowOperative
{
    /** @var list<class-string<DurableStep>> */
    public const STEP_CLASSES = [
        BenchS0::class, BenchS1::class, BenchS2::class, BenchS3::class,
        BenchS4::class, BenchS5::class, BenchS6::class, BenchS7::class,
        BenchS8::class, BenchS9::class, BenchS10::class, BenchS11::class,
        BenchS12::class, BenchS13::class, BenchS14::class, BenchS15::class,
    ];

    protected const STEPS = 1;

    public static function channels(Builder $c): void
    {
        $edge = $c->input('in', Wire::text(...))->into(self::STEP_CLASSES[0]);
        for ($at = 1; $at < static::STEPS; $at++) {
            $edge = $edge->named("edge_BenchS$at")->into(self::STEP_CLASSES[$at]);
        }
        $edge->named('out')->sink('bench_sink', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'bench_chain_1')]
final class BenchChain1 extends BenchChain
{
    protected const STEPS = 1;
}

#[Operative(factoryId: 'bench_chain_4')]
final class BenchChain4 extends BenchChain
{
    protected const STEPS = 4;
}

#[Operative(factoryId: 'bench_chain_16')]
final class BenchChain16 extends BenchChain
{
    protected const STEPS = 16;
}

#[Operative(factoryId: 'bench_chain_optout_4', persistent: false)]
final class BenchChainOptout4 extends BenchChain
{
    protected const STEPS = 4;
}

// -- bench_loop_workflow -----------------------------------------------------
//
// The long-lived loop the `turns` scenario drives (workflow_loop.rs:359): the
// chat loop's shape with a BOUNDED per-turn state, so a million-turn soak
// snapshots the same few bytes at every iteration, and a wake deadline for the
// timer-wake measurements.

#[Proc(name: 'bench_turn')]
final class BenchTurn extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['turns' => 0, 'last' => ''];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['turns']++;
        $state['last'] = $change['turn'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['turn' => $input]);
        $ctx->emit($input);

        return null;
    }
}

#[Operative(factoryId: 'bench_loop_workflow')]
final class BenchLoopWorkflow extends WorkflowOperative
{
    private const WAKE_AFTER_ENV = 'FLEZHA_BENCH_WAKE_AFTER_MS';

    /**
     * Built rather than written as an attribute: a PHP attribute argument is a
     * constant expression, and the wake deadline is an axis of the bench run.
     * A day, not the chat loop's ten minutes — a soak outlives 600 s.
     */
    protected static function workflowDeclaration(): Workflow
    {
        $wake = (int) (getenv(self::WAKE_AFTER_ENV) ?: '0');

        return new Workflow(
            loopMode: LoopMode::PerInput,
            maxDurationMs: 24 * 3_600_000,
            wakeAfterMs: $wake > 0 ? $wake : null,
        );
    }

    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(BenchTurn::class)->named('mid')
            ->into(ChatReply::class)->named('out')
            ->sink('bench_sink', Wire::bytes(...));
    }
}
