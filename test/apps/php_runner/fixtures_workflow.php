<?php

declare(strict_types=1);

/**
 * Twins of the reference's crash-resume workflow fixtures — the commit chain,
 * the saga, the transactional outbox, the branch, the diamond and the retry
 * ladder, plus the run-once echo — under the same factory ids, procedure
 * names, channel names and output bytes as
 * test/apps/rust_runner/src/workflow.rs. Loaded after fixtures.php, whose
 * `Wire` codecs these share; fixtures_workflow_loop.php carries the loop and
 * bench families and is loaded after this file, as the reference splits them.
 *
 * Entry payloads and plugin frames are UTF-8 text on the wire, as in the
 * reference: its bare `sink(name)` serialises a String as UTF-8, while a bare
 * sink here falls back to the class codec, which is JSON — so every sink below
 * declares an encoder. An inter-procedure hop between two PHP procedures
 * carries JSON, and the core derives that channel's journal codec from the
 * channel's kind, so no fixture declares one.
 *
 * A fixture is a class here rather than a factory call, so the reference's
 * macros are abstract bases with overridable hooks.
 */

namespace Flezha\TestApp;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Attribute\Workflow;
use Flezha\Codec\Json;
use Flezha\Ctx;
use Flezha\Definition;
use Flezha\Graph\Builder;
use Flezha\Procedure;
use Flezha\WorkflowCtx;
use Flezha\WorkflowOperative;

// -- the release gate --------------------------------------------------------

/**
 * Hold this activation until a runner boots with the release variable set.
 *
 * The harness kills the first runner and launches its replacement with the
 * variable present, so the gate is a boot-time decision: never returning on the
 * first runner, returning at once on the second.
 *
 * This app's manifest declares a thread-pool dispatch model, so the obligation
 * is to park a POOL WORKER and never the driver thread: the core owns the
 * driver, the heartbeat and the control channel on threads of its own, and
 * `SwooleExecutor` grows the pool on the core's starvation signal. The park is
 * a read on a socket nothing ever writes — no timer, no spin, off the CPU
 * until the process ends.
 */
final class WorkflowGate
{
    public const RELEASE_ENV = 'FLEZHA_WORKFLOW_E2E_RELEASE';

    public static function released(): bool
    {
        return getenv(self::RELEASE_ENV) !== false;
    }

    public static function blockUntilReleased(): void
    {
        if (self::released()) {
            return;
        }

        $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
        if ($pair === false) {
            throw new \RuntimeException('the release gate could not open the socket it parks on');
        }

        $write = null;
        $except = null;
        while (!self::released()) {
            $read = [$pair[0]];
            // Nothing holds the far end open to write it, so this answers only
            // when a signal interrupts the wait — in which case park again. PHP
            // warns on that interruption and the warning is the noise, not the
            // event, so it is suppressed here and nowhere else.
            @stream_select($read, $write, $except, null);
        }
    }

    /** Read at handler time: each scenario arms its own runner. */
    public static function armed(string $name): bool
    {
        return getenv($name) !== false;
    }
}

// -- the three bases ---------------------------------------------------------

/**
 * The reference's `durable_step!` (workflow.rs:66-108): a step committing one
 * idempotent Change keyed by its own name, so a re-run after a resume folds to
 * the state it already had.
 *
 * `release()` and {@see UndoingStep}'s compensation are the two axes the macro
 * itself parameterises; `changeValue()` and `gated()` are the two the reference
 * varies by writing the procedure out by hand instead.
 */
abstract class DurableStep extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['items' => []];
    }

    /**
     * A sorted, de-duplicated list. The reference's `SeenState` is a
     * `BTreeSet<String>`, chosen because a set keeps snapshot bytes
     * deterministic (workflow.rs:50); a PHP array keeps insertion order, so the
     * fold sorts for the same determinism and skips a repeat for the same
     * idempotent re-run.
     */
    public function applyChange(mixed &$state, mixed $change): void
    {
        if (in_array($change['insert'], $state['items'], true)) {
            return;
        }
        $state['items'][] = $change['insert'];
        sort($state['items']);
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        // A gated step waits BEFORE its change and its emit, so the kill window
        // always finds it as the uncommitted producer while its sibling arm has
        // committed.
        if ($this->gated($input)) {
            WorkflowGate::blockUntilReleased();
        }
        $ctx->change(['insert' => $this->changeValue($input)]);
        $this->release($input, $ctx);

        return null;
    }

    protected function gated(mixed $input): bool
    {
        return false;
    }

    /** What this step records: its own node name, unless a step says otherwise. */
    protected function changeValue(mixed $input): string
    {
        return Definition::of(static::class)->name;
    }

    protected function release(mixed $input, Ctx $ctx): void
    {
        $ctx->emit($input);
    }
}

/**
 * A durable step that compensates.
 *
 * Declared here rather than on {@see DurableStep} because `Definition` reads
 * which class declared a hook: a `compensate` on the durable base would make
 * every durable step compensable and contradict
 * test/fixtures/declarations/commit_block_workflow.json.
 */
abstract class UndoingStep extends DurableStep
{
    /**
     * A crash-resumed compensation reconstructs completed stages with EMPTY
     * outputs, so a hook that records an undo does so unconditionally.
     */
    public function compensate(array $outputs, Ctx $ctx): void
    {
        if ($this->compensationGated()) {
            WorkflowGate::blockUntilReleased();
        }
        $record = $this->undoRecord();
        if ($record !== null) {
            $ctx->emit($record);
        }
    }

    protected function compensationGated(): bool
    {
        return false;
    }

    protected function undoRecord(): ?string
    {
        return null;
    }
}

/** A terminal step that always fails, which is how a saga is made to unwind. */
abstract class FailingStep extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        throw new \RuntimeException(Definition::of(static::class)->name . ' always fails');
    }
}

// -- echo_workflow -----------------------------------------------------------
//
// The run-sugar shape (workflow.rs:14): emits its args back once and completes,
// so the SDK ships the completion and the controller deregisters it.

#[Operative(factoryId: 'echo_workflow')]
final class EchoWorkflow extends WorkflowOperative
{
    public static function run(mixed $args, WorkflowCtx $ctx): mixed
    {
        $ctx->emit('echo:' . Json::encode($args));
        $ctx->complete();

        return null;
    }
}

// -- commit_block_workflow / sync_durable_workflow ---------------------------
//
// Proc A commits durable state, proc B parks until the resumed runner sets the
// release variable — so a SIGKILL lands with A's state already on the WAL and B
// never finished. `sync_durable_workflow` is the same chain declaring
// SYNCHRONOUS remote durability, which is what lets the kill-the-disk drill
// destroy the state directory and still resume with A committed.

#[Proc(name: 'commit_a')]
final class CommitA extends DurableStep
{
}

#[Proc(name: 'block_b')]
final class BlockB extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        WorkflowGate::blockUntilReleased();

        return null;
    }
}

abstract class CommitBlockChain extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(CommitA::class)->named('mid')
            ->into(BlockB::class);
    }
}

#[Operative(factoryId: 'commit_block_workflow')]
final class CommitBlockWorkflow extends CommitBlockChain
{
}

#[Operative(factoryId: 'sync_durable_workflow')]
#[Workflow(syncDurability: true)]
final class SyncDurableWorkflow extends CommitBlockChain
{
}

// -- optout_commit_block -----------------------------------------------------
//
// The opt-out twin with a live mid-chain signal: proc A's output also reaches a
// plugin sink, so the crash-resume drill can observe the mid-run point without
// durable state. A crash re-runs the chain from the top, so the sink sees A's
// marker once per run and no journal file exists.

#[Operative(factoryId: 'optout_commit_block', persistent: false)]
final class OptoutCommitBlock extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(CommitA::class)
            ->sink('mid', static fn (mixed $v): string => "a-ran:$v")
            ->into(BlockB::class);
    }
}

// -- tier_commit_block_workflow ----------------------------------------------
//
// The effectively-once fixture. Proc A commits durable state AND emits
// `committed:<input>` under a stable key on a keyed hybrid channel, captured
// into the atomic effect record; proc B parks. A SIGKILL lands with A's effect
// record durable and B unfinished, and the resume re-runs A — the change is
// idempotent and the re-emit deterministic.

#[Proc(name: 'tier_commit_a')]
final class TierCommitA extends DurableStep
{
    protected function release(mixed $input, Ctx $ctx): void
    {
        $ctx->emit("committed:$input", 'job');
    }
}

#[Operative(factoryId: 'tier_commit_block_workflow')]
final class TierCommitBlockWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->input('in', Wire::text(...))
            ->into(TierCommitA::class)->keyed()
            ->sink('mid', Wire::bytes(...))
            ->into(BlockB::class);
    }
}

// -- saga_block_workflow -----------------------------------------------------
//
// SA and SB commit durable state and compensate; SC fails, triggering
// reverse-order rollback (SB then SA). SA's compensation PARKS, so a SIGKILL
// lands mid-rollback with SB's done-marker already durable — the respawn
// finishes SA and SKIPS the already-done SB.

#[Proc(name: 'saga_a')]
final class SagaA extends UndoingStep
{
    protected function compensationGated(): bool
    {
        return true;
    }
}

#[Proc(name: 'saga_b')]
final class SagaB extends UndoingStep
{
}

#[Proc(name: 'saga_c')]
final class SagaC extends FailingStep
{
}

#[Operative(factoryId: 'saga_block_workflow')]
final class SagaBlockWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(SagaA::class)->named('s1')
            ->into(SagaB::class)->named('s2')
            ->into(SagaC::class);
    }
}

// -- outbox_block_workflow ---------------------------------------------------
//
// The transactional outbox. Proc A commits durable state AND emits
// plugin-bound output on a hybrid channel — STAGED, so it reaches the sink only
// after the controller has journaled and fsynced A's committed entry with the
// output inside it and flushed the outbox. Proc B parks, so a SIGKILL lands
// with A committed and flushed and B unfinished.

#[Operative(factoryId: 'outbox_block_workflow')]
final class OutboxBlockWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(CommitA::class)
            ->sink('mid', static fn (mixed $v): string => "a-out:$v")
            ->into(BlockB::class);
    }
}

// -- undo_outbox_workflow ----------------------------------------------------
//
// The compensation half of the transactional outbox. Proc UA commits, emits its
// forward output on a hybrid plugin channel, and compensates by EMITTING an
// undo record — plugin-bound, so it is staged and rides a compensated journal
// entry rather than the wire. Proc C fails immediately, so the chain always
// rolls back; the hook PARKS, so on the first runner the undo is
// emitted-but-never-journaled when the SIGKILL lands, proving staged undo
// output cannot leak. The respawn re-runs the outstanding hook, whose done
// marker never landed, and the undo is delivered exactly once.

#[Proc(name: 'undo_outbox_a')]
final class UndoOutboxA extends UndoingStep
{
    protected function compensationGated(): bool
    {
        return true;
    }

    protected function undoRecord(): ?string
    {
        return 'undone';
    }
}

#[Proc(name: 'undo_outbox_c')]
final class UndoOutboxC extends FailingStep
{
}

#[Operative(factoryId: 'undo_outbox_workflow')]
final class UndoOutboxWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(UndoOutboxA::class)
            ->sink('mid', static fn (mixed $v): string => "out:$v")
            ->into(UndoOutboxC::class);
    }
}

// -- branch_workflow ---------------------------------------------------------
//
//     BranchA ─┬─▶ BranchB ─▶ BranchD
//              └─▶ BranchC
//
// The procedure TREE (workflow.rs:352). Every procedure commits durable state
// and compensates by emitting an undo record, so both tree behaviours are
// observable at one sink:
//
//   * crash-resume — BranchD PARKS, so the SIGKILL lands with A, B and C
//     committed and only D's subtree outstanding. The respawn re-runs D alone,
//     seeded from B's journaled edge, so the sibling branch's `c:` marker
//     appears exactly ONCE across both runs.
//   * compensation — with FLEZHA_BRANCH_FAIL armed, BranchD fails instead of
//     parking. A, B and C have all completed, so the rollback unwinds the two
//     branches before the root they share: `undo-c`, `undo-b`, `undo-a`.
//
// The inter-proc channels are hybrid, which is what makes a non-leaf
// procedure's forward and undo output visible at the sink.

#[Proc(name: 'branch_a')]
final class BranchA extends UndoingStep
{
    protected function undoRecord(): ?string
    {
        return 'undo-a';
    }
}

#[Proc(name: 'branch_b')]
final class BranchB extends UndoingStep
{
    protected function undoRecord(): ?string
    {
        return 'undo-b';
    }
}

/** The sibling leaf: emits straight to the sink, and its undo does too. */
#[Proc(name: 'branch_c')]
final class BranchC extends UndoingStep
{
    protected function release(mixed $input, Ctx $ctx): void
    {
        $ctx->emit("c:$input");
    }

    protected function undoRecord(): ?string
    {
        return 'undo-c';
    }
}

/** The deep leaf under BranchB: parks (crash-resume), or fails (compensation). */
#[Proc(name: 'branch_d')]
final class BranchD extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        if (WorkflowGate::armed('FLEZHA_BRANCH_FAIL')) {
            throw new \RuntimeException('branch_d failed');
        }
        WorkflowGate::blockUntilReleased();
        $ctx->emit("d:$input");

        return null;
    }
}

#[Operative(factoryId: 'branch_workflow')]
// Fail fast: the compensation scenario is about the rollback order, not the
// ladder that precedes it.
#[Workflow(retryAttempts: 0)]
final class BranchWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        // One channel, two consumers: THE fan-out. Both branches read exactly
        // what BranchA released on it (held edge, wired twice).
        $mid = $c->input('in', Wire::text(...))
            ->into(BranchA::class)
            ->sink('mid', static fn (mixed $v): string => "a:$v");
        $mid->into(BranchB::class)
            ->sink('bmid', static fn (mixed $v): string => "b:$v")
            ->into(BranchD::class)
            ->sink('d_out', Wire::bytes(...));
        $mid->into(BranchC::class)->sink('c_out', Wire::bytes(...));
    }
}

// -- diamond_workflow --------------------------------------------------------
//
//     DiamA ─┬─▶ DiamB1 ─▶ DiamB2(parks) ─┬─▶ DiamJoin
//            └─▶ DiamC ───────────────────┘
//
// The fan-in DAG (workflow.rs:475). The join accumulates BOTH arms' outputs and
// is dispatched exactly once, after both commit. DiamB2 parks one level BELOW
// its sibling arm — siblings commit at one level barrier — so a SIGKILL lands
// with A, B1 and C committed and B2 mid-run: the respawn re-runs B2 alone, and
// the join runs once with C's JOURNALED handoff banked and B2's fresh output
// appended, so `c:` appears exactly once across both runs.

#[Proc(name: 'diam_a')]
final class DiamA extends DurableStep
{
}

#[Proc(name: 'diam_b1')]
final class DiamB1 extends DurableStep
{
}

#[Proc(name: 'diam_b2')]
final class DiamB2 extends DurableStep
{
    protected function gated(mixed $input): bool
    {
        return true;
    }

    protected function changeValue(mixed $input): string
    {
        return 'diam_b';
    }

    protected function release(mixed $input, Ctx $ctx): void
    {
        $ctx->emit("b:$input");
    }
}

#[Proc(name: 'diam_c')]
final class DiamC extends DurableStep
{
    protected function release(mixed $input, Ctx $ctx): void
    {
        $ctx->emit("c:$input");
    }
}

/** The join: one `j:`-tagged record per accumulated contribution. */
#[Proc(name: 'diam_join')]
final class DiamJoin extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit("j:$input");

        return null;
    }
}

#[Operative(factoryId: 'diamond_workflow')]
final class DiamondWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        // Held edge wired twice = the fan-out; two arms naming DiamJoin with no
        // node name = the fan-in, since two unnamed mentions are one node.
        $fan = $c->input('in', Wire::text(...))
            ->into(DiamA::class)
            ->sink('dfan', static fn (mixed $v): string => "a:$v");
        $fan->into(DiamB1::class)->named('db1')
            ->into(DiamB2::class)
            ->sink('db2', Wire::bytes(...))
            ->into(DiamJoin::class);
        $fan->into(DiamC::class)
            ->sink('dc', Wire::bytes(...))
            ->into(DiamJoin::class)
            ->sink('j_out', Wire::bytes(...));
    }
}

// -- retry_workflow ----------------------------------------------------------
//
// The per-entity retry ladder (workflow.rs:607). Proc RA commits durable state,
// forwards on a hybrid channel, and compensates by emitting an undo record;
// `retry_flaky` fails its first FLEZHA_WORKFLOW_RETRY_FAILS attempts (none by
// default — each scenario arms it) and then emits `attempt:<n>:<input>`. One
// armed failure is absorbed by the ladder, so the run completes and the sink
// sees attempt 2; arming more failures than the ladder has attempts exhausts
// it, RA rolls back and the run reports an error.

#[Proc(name: 'retry_a')]
final class RetryA extends UndoingStep
{
    protected function undoRecord(): ?string
    {
        return 'undone';
    }
}

#[Proc(name: 'retry_flaky')]
final class RetryFlaky extends Procedure
{
    private const FAILS_ENV = 'FLEZHA_WORKFLOW_RETRY_FAILS';

    /**
     * Attempts made in this interpreter. A retry re-runs the procedure of the
     * same instance in place, so one counter is the whole ladder's view under
     * the single-loop executor this fixture runs on; a thread pool would give
     * each worker its own.
     */
    private static int $attempts = 0;

    public function run(mixed $input, Ctx $ctx): mixed
    {
        self::$attempts++;
        $armed = (int) (getenv(self::FAILS_ENV) ?: '0');
        if (self::$attempts <= $armed) {
            throw new \RuntimeException('flaky attempt ' . self::$attempts);
        }
        $ctx->emit('attempt:' . self::$attempts . ":$input");

        return null;
    }
}

#[Operative(factoryId: 'retry_workflow')]
final class RetryWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))
            ->into(RetryA::class)
            ->sink('mid', static fn (mixed $v): string => "res:$v")
            ->into(RetryFlaky::class)
            ->sink('out', Wire::bytes(...));
    }
}


// -- registration ------------------------------------------------------------

final class WorkflowFixtures
{
    /** @return list<class-string<\Flezha\Operative>> */
    public static function all(): array
    {
        return [
            EchoWorkflow::class,
            CommitBlockWorkflow::class,
            SyncDurableWorkflow::class,
            OptoutCommitBlock::class,
            TierCommitBlockWorkflow::class,
            SagaBlockWorkflow::class,
            OutboxBlockWorkflow::class,
            UndoOutboxWorkflow::class,
            ChatLoopWorkflow::class,
            StatelessLoopWorkflow::class,
            ChatRewindWorkflow::class,
            RetryWorkflow::class,
            BranchWorkflow::class,
            DiamondWorkflow::class,
            BenchChain1::class,
            BenchChain4::class,
            BenchChain16::class,
            BenchChainOptout4::class,
            BenchLoopWorkflow::class,
        ];
    }
}
