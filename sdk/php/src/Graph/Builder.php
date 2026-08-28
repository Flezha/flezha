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

    /** Close the checkpoint interval every `$count` inputs, and on demand. */
    public function checkpointEvery(int $count): self
    {
        $this->effectivelyOnce = true;
        $this->ackGranularity = AckGranularity::Checkpoint;
        $this->checkpointEvery = $count;

        return $this;
    }

    /** Hold outputs until the interval closes, for at most `$maxDelayMs`. */
    public function outputsAtCheckpoint(?int $maxDelayMs = null): self
    {
        $this->checkpointOutputs = CheckpointOutputs::AtCheckpoint;
        if ($maxDelayMs !== null) {
            $this->checkpointMaxDelayMs = $maxDelayMs;
        }

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
            throw new DeclarationException($this->factoryId, $identity,
                'two different procedures claim this node identity; name one of them on the '
                . '`into(...)` so each node has its own');
        }

        $node = new ProcNode(count($this->procedures), $identity, $class, $definition);
        $this->procedures[] = $node;

        return $node;
    }

    /** @param list<int> $consumers @param list<callable(mixed):mixed> $transforms */
    public function addChannel(
        string $name,
        string $kind,
        ?int $producer,
        array $consumers,
        Overflow $overflow,
        ?int $retryMax,
        ?int $retryBackoffMs,
        bool $keyed,
        array $transforms,
        ?callable $keyBy,
        ?callable $encode = null,
    ): ChannelSpec {
        $channel = new ChannelSpec(
            index: count($this->channels),
            name: $name,
            kind: $kind,
            producer: $producer,
            consumers: $consumers,
            overflow: $overflow,
            retryMax: $retryMax,
            retryBackoffMs: $retryBackoffMs,
            keyed: $keyed,
            transforms: $transforms,
            keyBy: $keyBy,
            encode: $encode,
        );
        $this->channels[] = $channel;

        return $channel;
    }
}
