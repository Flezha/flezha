<?php

declare(strict_types=1);

namespace Flezha\Graph;

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
    /**
     * The links each fluent sequence off a channel handle was declared with,
     * in declaration order — the record's `chains`, recorded and never judged.
     *
     * Every `map` / `filter` / `flatMap` / `keyBy` / `named` answers a new
     * sequence over the previous, which is what lets a shared prefix branch
     * without the arms colliding; `superseded` marks the prefix a later link
     * built on, so only a tip reaches the record.
     *
     * @var list<array{source: array{string, ChannelSpec|ProcNode}, links: list<array<string, mixed>>, superseded: bool, edge: ?ConsumerEdge}>
     */
    private array $chains = [];

    private bool $sealed = false;

    public bool $effectivelyOnce = false;
    public bool $checkpointAck = false;
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
     *
     * Each of the three below writes its own raw flag and nothing else: the
     * acknowledgement granularity is DERIVED by the core, which is the one
     * place any checkpoint key implies it.
     */
    public function checkpointAck(): self
    {
        $this->checkpointAck = true;

        return $this;
    }

    /** Close the checkpoint interval every `$count` inputs, and on demand. */
    public function checkpointEvery(int $count): self
    {
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
            // The node table is keyed by identity and physically cannot hold
            // two, so the record the core would judge is unwritable. The
            // refusal answers under the core's own name all the same.
            throw new DeclarationException(
                $this->factoryId,
                $identity,
                'two different procedures claim this node identity; name one of them on the '
                . '`into(...)` so each node has its own',
                ['NodeIdentityCollision']
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

    /**
     * Record one more link on the sequence at `$base`, or open one on
     * `$source`, and answer the new sequence's id.
     *
     * The links are copied rather than appended in place: a handle held over a
     * branch point extends the prefix twice, and two sequences sharing one
     * list would each see the other's links.
     *
     * `$edge` is the consumer an `into` sequence closes on, held aside for the
     * core to name back on the edge it derives from this chain.
     *
     * @param array{string, ChannelSpec|ProcNode} $source
     * @param array<string, mixed>                $link
     */
    public function linkChain(array $source, ?int $base, array $link, ?ConsumerEdge $edge = null): int
    {
        $links = $base === null ? [] : $this->chains[$base]['links'];
        if ($base !== null) {
            $this->chains[$base]['superseded'] = true;
        }
        $links[] = $link;
        $this->chains[] = ['source' => $source, 'links' => $links, 'superseded' => false, 'edge' => $edge];

        return count($this->chains) - 1;
    }

    /**
     * File every sequence on the channel it was declared off, in declaration
     * order — the `chains` the record carries.
     *
     * A channel named only by a sequence no terminator closed is created here,
     * so a dangling chain is submitted rather than dropped on the way to the
     * core. Idempotent: a spec is assembled once, but a builder is reachable.
     */
    public function seal(): self
    {
        if ($this->sealed) {
            return $this;
        }
        $this->sealed = true;
        foreach ($this->chains as $chain) {
            if ($chain['superseded']) {
                continue;
            }
            $channel = $this->channelOf($chain['source']);
            $channel->chains[] = $chain['links'];
            $channel->chainEdges[] = $chain['edge'];
        }

        return $this;
    }

    /**
     * The channel an edge's values leave on: the entry's own, or the producing
     * node's, created on first use.
     *
     * @param array{string, ChannelSpec|ProcNode} $source
     */
    private function channelOf(array $source): ChannelSpec
    {
        $from = $source[1];

        return $from instanceof ProcNode ? $this->outputChannelFor($from) : $from;
    }
}
