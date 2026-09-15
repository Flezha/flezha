<?php

declare(strict_types=1);

namespace Flezha\Testing;

use Flezha\BeyondWindow;
use Flezha\ChannelCodec;
use Flezha\CheckpointOutputs;
use Flezha\Codec\Json;
use Flezha\Ctx;
use Flezha\Definition;
use Flezha\Dispatch\Dispatcher;
use Flezha\Dispatch\RegisteredClass;
use Flezha\Dispatch\Restore;
use Flezha\Graph\Builder;
use Flezha\Graph\ClassSpec;
use Flezha\Graph\ProcNode;
use Flezha\Procedure;
use Flezha\SpawnBridge;
use Flezha\Timer;

/**
 * Driving one procedure without a controller, for the author's own tests.
 *
 * Not a second implementation of the dispatcher: it drives
 * {@see Dispatcher::invoke()} — the function every activation kind that
 * reaches a handler goes through — over a one-procedure `RegisteredClass`, and
 * {@see assertReplayEquals()} folds through {@see Restore::fold()}, the
 * function a real restore calls. So what it proves is what the runner does,
 * rather than something that agrees with it.
 */
final class Harness implements SpawnBridge
{
    private readonly Definition $definition;
    private readonly ProcNode $node;
    private readonly RegisteredClass $registered;

    /** @var list<string> the encoded change log, as the WAL would hold it */
    public array $log = [];

    private int $changeSeq = 0;

    /** @param class-string<Procedure> $procedureClass */
    public function __construct(
        string $procedureClass,
        private readonly ?string $entityKey = null,
        private readonly string $operative = 'harness',
    ) {
        $this->definition = Definition::of($procedureClass);

        // The real channel builder, not a hand-written spec: one entry channel
        // into the procedure, and one plugin sink so `ctx->emit` has somewhere
        // to go. A procedure under test emits to be observed, not to be routed,
        // and the sink names JSON so `outputs` reports exactly the value a
        // handler emitted.
        $builder = new Builder('<harness>');
        $builder->input('in')->into($procedureClass)->sink('out', codec: ChannelCodec::Json);
        $this->node = $builder->procedures[0];

        $spec = new ClassSpec(
            factoryId: '<harness>',
            kind: 'stream',
            persistent: true,
            effectivelyOnce: false,
            checkpointAck: false,
            checkpointEvery: null,
            checkpointOutputs: CheckpointOutputs::AsProduced,
            checkpointMaxDelayMs: null,
            correctionWindowMs: null,
            beyondWindowPolicy: BeyondWindow::ProcessAsArrival,
            compactAfterBytes: null,
            syncDurability: false,
            procedures: $builder->procedures,
            channels: $builder->channels,
        );

        // The dispatcher's own entity store, so the state this harness reports
        // is the state a runner would hold.
        $this->registered = new RegisteredClass($spec);
    }

    /** The entity's live state, as the dispatcher left it. */
    public function state(): mixed
    {
        return $this->registered->stateOf($this->node, $this->entityKey);
    }

    /** Run the procedure against one input. */
    public function feed(mixed $input, int $now = 0, bool $replay = false): Outcome
    {
        return $this->drive(
            $now,
            $replay,
            fn (Ctx $ctx): mixed => $this->definition->instance->run($input, $ctx),
        );
    }

    /** Fire a per-entity timer. `now` IS the scheduled time, as it is live. */
    public function fireTimer(string $kind, int $scheduledAt): Outcome
    {
        return $this->drive($scheduledAt, false, function (Ctx $ctx) use ($kind, $scheduledAt): mixed {
            $this->definition->instance->onTimer(new Timer($kind, $scheduledAt), $ctx);

            return null;
        });
    }

    /**
     * Run the declared compensation over recorded forward outputs.
     *
     * @param list<mixed> $outputs
     */
    public function compensate(array $outputs): Outcome
    {
        return $this->drive(0, false, function (Ctx $ctx) use ($outputs): mixed {
            $this->definition->instance->compensate($outputs, $ctx);

            return null;
        });
    }

    /**
     * Rebuild the state the way a restore does, from a snapshot and a change
     * log — the same call the dispatcher answers `restore_fold` with.
     *
     * @param list<string> $changes
     */
    public function restore(?string $snapshot = null, array $changes = []): void
    {
        $this->registered->setState($this->node, $this->entityKey, Restore::fold(
            $this->definition,
            [
                'snapshot' => $snapshot === null ? null : ['bytes' => $snapshot, 'from_version' => null],
                'changes' => array_map(
                    static fn (string $bytes): array => ['bytes' => $bytes, 'from_version' => null],
                    $changes,
                ),
            ],
            $this->entityKey,
            $this->definition->name,
        ));
    }

    /**
     * Fold every recorded change into a fresh initial state and require the
     * answer to equal the live one.
     *
     * PHP has no cheap deep read-only view of a state value — the Node SDK's
     * guard is a `Proxy` and there is no counterpart here that does not cost
     * an allocation per field read — so this is what makes the
     * mutate-only-through-changes rule safe. It catches both failures at once:
     * a handler that mutated the state without recording a change, and an
     * `applyChange` that is not a pure fold.
     */
    public function assertReplayEquals(): bool
    {
        $replayed = Restore::fold(
            $this->definition,
            ['changes' => array_map(
                static fn (string $bytes): array => ['bytes' => $bytes, 'from_version' => null],
                $this->log,
            )],
            $this->entityKey,
            $this->definition->name,
        );

        if ($replayed != $this->state()) {
            throw new ReplayMismatch($this->definition->name, $this->state(), $replayed);
        }

        return true;
    }

    /**
     * The spawn bridge a `Ctx` asks. A harness has no controller, so a spawn
     * answers what one would have and records nothing.
     */
    public function spawnWorkflow(string $instanceId, string $factoryId, string $payload, string $ifRunning): string
    {
        return 'spawned';
    }

    /**
     * Drive `procedure_class` over `$inputs` and assert the replay agrees.
     *
     * @param class-string<Procedure> $procedureClass
     * @param list<mixed>             $inputs
     */
    public static function replayEquals(string $procedureClass, array $inputs, ?string $entityKey = null): bool
    {
        $harness = new self($procedureClass, $entityKey);
        foreach ($inputs as $input) {
            $harness->feed($input);
        }

        return $harness->assertReplayEquals();
    }

    private function drive(int $now, bool $replay, callable $body): Outcome
    {
        $returned = null;
        $ctx = Dispatcher::invoke(
            [
                'operative' => $this->operative,
                'entity_key' => $this->entityKey,
                'change_seq' => $this->changeSeq,
            ],
            $this->registered,
            $this->node,
            $this,
            $now,
            $replay,
            function (Ctx $ctx) use ($body, &$returned): void {
                $returned = $body($ctx);
                $ctx->emitReturned($returned);
            },
        );

        array_push($this->log, ...$ctx->changes);
        $this->changeSeq += count($ctx->changes);

        return new Outcome(
            outputs: array_map(
                static fn (array $output): mixed => Json::decode((string) $output['payload']),
                $ctx->outputs,
            ),
            changes: array_map($this->decodeChange(...), $ctx->changes),
            timers: $ctx->timers,
            returned: $returned,
        );
    }

    private function decodeChange(string $bytes): mixed
    {
        return $this->definition->decode('change', $bytes);
    }
}
