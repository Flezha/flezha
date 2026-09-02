<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\AckGranularity;
use Flezha\BeyondWindow;
use Flezha\CheckpointOutputs;
use Flezha\Definition;
use Flezha\Exception\DeclarationException;
use Flezha\Overflow;
use Flezha\Procedure;

/**
 * The fluent channel builder an operative's `channels()` receives.
 *
 * Class-wide declarations (the tier, the correction window, compaction) are
 * methods on the builder rather than a second attribute, because they are
 * what the graph is being declared with — an author writes them in the same
 * block as the wiring they qualify.
 */
final class Builder
{
    /** @var list<ProcNode> */
    public array $procedures = [];
    /** @var list<ChannelSpec> */
    public array $channels = [];

    public bool $effectivelyOnce = false;
    public AckGranularity $ackGranularity = AckGranularity::PerInput;
    public ?int $checkpointEvery = null;
    public CheckpointOutputs $checkpointOutputs = CheckpointOutputs::AsProduced;
    public ?int $checkpointMaxDelayMs = null;
    public ?int $correctionWindowMs = null;
    public BeyondWindow $beyondWindowPolicy = BeyondWindow::ProcessAsArrival;
    public ?int $compactAfterBytes = null;
    public bool $syncDurability = false;

    public function __construct(public readonly string $factoryId)
    {
    }

    /** Deliver each input exactly once, against a durable consumed offset. */
    public function effectivelyOnce(): self
    {
        $this->effectivelyOnce = true;

        return $this;
    }

    /**
     * Acknowledge at the checkpoint rather than per input; the interval closes
     * only on an explicit `ctx.checkpoint()`. Requires `effectivelyOnce()`.
     */
    public function checkpointAck(): self
    {
        return $this->amendCheckpoint(static function (): void {
        });
    }

    /** Close the checkpoint interval every `$count` inputs, and on demand. */
    public function checkpointEvery(int $count): self
    {
        return $this->amendCheckpoint(function () use ($count): void {
            $this->checkpointEvery = $count;
        });
    }

    /** Hold outputs until the interval closes, for at most `$maxDelayMs`. */
    public function outputsAtCheckpoint(?int $maxDelayMs = null): self
    {
        return $this->amendCheckpoint(function () use ($maxDelayMs): void {
            $this->checkpointOutputs = CheckpointOutputs::AtCheckpoint;
            if ($maxDelayMs !== null) {
                $this->checkpointMaxDelayMs = $maxDelayMs;
            }
        });
    }

    /**
     * Turn the checkpoint granularity on (idempotent) and amend its policy, so
     * each option is one call that preserves the others.
     */
    private function amendCheckpoint(callable $amend): self
    {
        $this->ackGranularity = AckGranularity::Checkpoint;
        $amend();

        return $this;
    }

    /** Accept late inputs for this long after their event time. */
    public function correctionWindow(int $milliseconds): self
    {
        $this->correctionWindowMs = $milliseconds;

        return $this;
    }

    public function beyondWindowPolicy(BeyondWindow $policy): self
    {
        $this->beyondWindowPolicy = $policy;

        return $this;
    }

    public function compactAfterBytes(int $bytes): self
    {
        $this->compactAfterBytes = $bytes;

        return $this;
    }

    /** Acknowledge only once the change log is durable on the controller. */
    public function syncDurability(): self
    {
        $this->syncDurability = true;

        return $this;
    }

    /**
     * Declare a controller-fed entry channel. `$decode` turns the wire bytes
     * into the value a handler sees; its absence means the JSON default.
     *
     * @param (callable(string):mixed)|null $decode
     */
    public function input(string $name, ?callable $decode = null): Edge
    {
        $channel = new ChannelSpec(
            index: count($this->channels),
            name: $name,
            kind: 'entry',
            decode: $decode,
        );
        $this->channels[] = $channel;

        return new Edge($this, ['entry', $channel], []);
    }

    /**
     * The node for a procedure. Two unnamed mentions of one procedure are one
     * node — the naming convention every SDK mirrors — so a diamond that fans
     * out and back in has one instance, not two.
     *
     * @param class-string<Procedure> $class
     */
    public function nodeFor(string $class, ?string $name = null): ProcNode
    {
        $definition = Definition::of($class);
        $identity = $name ?? $definition->name;

        foreach ($this->procedures as $node) {
            if ($node->name !== $identity) {
                continue;
            }
            if ($node->class === $class) {
                return $node;
            }
            throw new DeclarationException(
                $this->factoryId,
                $identity,
                'two different procedures claim this node identity; name one of them on the '
                . '`into(...)` so each node has its own'
            );
        }

        $node = new ProcNode(count($this->procedures), $identity, $class, $definition);
        $this->procedures[] = $node;

        return $node;
    }

    /**
     * The channel this node produces on, created on first use.
     *
     * Lazily, so a leaf node never carries a phantom channel. Named by the
     * node's identity until a `named(...)` override, and reused by every later
     * call on an edge whose source is this node — which is what makes a fan-out
     * one channel with two consumers rather than two channels claiming one
     * producer.
     */
    public function outputChannelFor(ProcNode $node): ChannelSpec
    {
        if ($node->output !== null) {
            return $node->output;
        }

        $channel = new ChannelSpec(
            index: count($this->channels),
            name: $node->name,
            kind: 'inter_proc',
            producer: $node->index,
        );
        $this->channels[] = $channel;
        $node->output = $channel;

        return $channel;
    }
}
