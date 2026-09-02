<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\Dropped;
use Flezha\Fanned;
use Flezha\Overflow;
use Flezha\Exception\DeclarationException;
use Flezha\Procedure;

/**
 * One edge under construction.
 *
 * Immutable: every fluent call answers a new edge, so a shared prefix can be
 * branched without the arms colliding — which is what makes `branch(...)`
 * build exactly the topology the separate `into(...)` statements would.
 */
final class Edge
{
    /**
     * @param array{string, ChannelSpec|ProcNode} $from
     * @param array<string, mixed>                $pending
     */
    public function __construct(
        private readonly Builder $builder,
        private readonly array $from,
        private readonly array $pending,
    ) {
    }

    /** Override the channel name this edge creates. */
    public function named(string $name): self
    {
        return $this->with(['channelName' => $name]);
    }

    /** Transform every value on this edge. Evaluated producer-side. */
    public function map(callable $transform): self
    {
        return $this->with(['transforms' => [...$this->transforms(), $transform]]);
    }

    /**
     * Drop the values a predicate rejects, before this edge's key is computed.
     * A dropped value is consumed and reaches no handler.
     */
    public function filter(callable $predicate): self
    {
        return $this->map(
            static fn (mixed $value): mixed => $predicate($value) ? $value : Dropped::get(),
        );
    }

    /** Transform one value into many; each reaches the consumer on its own. */
    public function flatMap(callable $transform): self
    {
        return $this->map(
            static fn (mixed $value): Fanned => new Fanned(array_values([...$transform($value)])),
        );
    }

    /** Route by a computed key: the consumer becomes a dynamic keyed procedure. */
    public function keyBy(callable $key): self
    {
        return $this->with(['keyBy' => $key]);
    }

    public function onFull(Overflow $policy): self
    {
        return $this->with(['overflow' => $policy]);
    }

    public function retryMax(int $attempts): self
    {
        return $this->with(['retryMax' => $attempts]);
    }

    public function retryBackoffMs(int $milliseconds): self
    {
        return $this->with(['retryBackoffMs' => $milliseconds]);
    }

    /** Stamp `flz.entity` / `flz.seq` / `flz.key` onto this channel's plugin frames. */
    public function keyed(): self
    {
        return $this->with(['keyed' => true]);
    }

    /**
     * Fan out: each arm receives this same edge, so `branch` builds exactly
     * the topology the separate `into(...)` statements would.
     *
     * @param callable(self):mixed ...$arms
     */
    public function branch(callable ...$arms): self
    {
        if (count($arms) < 2) {
            throw new DeclarationException(
                $this->builder->factoryId,
                $this->pending['channelName'] ?? $this->sourceName(),
                sprintf(
                    'branch(...) requires at least two arms; %d given — a single arm is '
                    . '`into(...)` written the long way',
                    count($arms),
                ),
            );
        }

        foreach ($arms as $arm) {
            $arm($this);
        }

        return $this;
    }

    /**
     * Wire this edge into a procedure.
     *
     * @param class-string<Procedure> $class
     */
    public function into(string $class, ?string $nodeName = null): self
    {
        $node = $this->builder->nodeFor($class, $nodeName);

        // The transforms and the key belong to this edge, not to the channel:
        // one channel may feed several procedures, and a chain declared before
        // one `into` must not reach another.
        $this->channel()->consumers[] = new ConsumerEdge(
            $node->index,
            $this->transforms(),
            $this->pending['keyBy'] ?? null,
        );

        return new self($this->builder, ['procedure', $node], []);
    }

    /**
     * Attach this edge's channel to the named plugin sink, and answer the edge.
     *
     * `sink(...)->into(...)` wires a consumer behind the sink, so one emit
     * reaches both. A channel with no consumer behind it is the plugin-bound
     * terminal — terminality is the absence of further wiring, not a different
     * method.
     *
     * @param (callable(mixed):string)|null $encode
     */
    public function sink(string $name, ?callable $encode = null): self
    {
        if ($this->from[0] !== 'procedure') {
            throw new DeclarationException(
                $this->builder->factoryId,
                $name,
                'a plugin sink is fed by a procedure; wire the entry channel into one first'
            );
        }

        $channel = $this->channel();
        if ($channel->sink !== null) {
            throw new DeclarationException(
                $this->builder->factoryId,
                $channel->name,
                'declares a plugin sink more than once'
            );
        }

        $channel->sink = $name;
        $channel->encode = $encode;

        return $this;
    }

    /**
     * The channel this edge's values leave on, with the pending options
     * applied. An entry edge already has one; a procedure edge's is created on
     * first use and reused after.
     */
    private function channel(): ChannelSpec
    {
        if ($this->from[0] === 'entry') {
            $channel = $this->from[1];
        } else {
            $channel = $this->builder->outputChannelFor($this->from[1]);
            if (isset($this->pending['channelName'])) {
                $channel->name = $this->pending['channelName'];
            }
        }
        $this->applyPending($channel);

        return $channel;
    }

    /** @return list<callable(mixed):mixed> */
    private function transforms(): array
    {
        return $this->pending['transforms'] ?? [];
    }

    /** @param array<string, mixed> $next */
    private function with(array $next): self
    {
        return new self($this->builder, $this->from, [...$this->pending, ...$next]);
    }

    private function sourceName(): string
    {
        return $this->from[1]->name;
    }

    private function applyPending(ChannelSpec $channel): void
    {
        $channel->overflow = $this->pending['overflow'] ?? $channel->overflow;
        $channel->retryMax = $this->pending['retryMax'] ?? $channel->retryMax;
        $channel->retryBackoffMs = $this->pending['retryBackoffMs'] ?? $channel->retryBackoffMs;
        $channel->keyed = $channel->keyed || ($this->pending['keyed'] ?? false);
    }
}
