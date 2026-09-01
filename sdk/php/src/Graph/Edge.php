<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\Dropped;
use Flezha\Fanned;
use Flezha\Exception\DeclarationException;
use Flezha\Overflow;
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

        if ($this->from[0] === 'entry') {
            // One entry channel may feed several procedures; the transforms
            // and the key belong to the channel, which is what the prepare
            // crossing names.
            $channel = $this->from[1];
            $channel->consumers[] = $node->index;
            $this->applyPending($channel);
        } else {
            $this->channel(
                'inter_proc',
                $this->pending['channelName'] ?? ($this->from[1]->name . '__' . $node->name),
                [$node->index],
            );
        }

        return new self($this->builder, ['procedure', $node], []);
    }

    /**
     * Terminate this edge on a plugin-bound channel.
     *
     * @param (callable(mixed):string)|null $encode
     */
    public function sink(string $name, ?callable $encode = null): void
    {
        if ($this->from[0] !== 'procedure') {
            throw new DeclarationException(
                $this->builder->factoryId,
                $name,
                'a plugin sink is fed by a procedure; wire the entry channel into one first'
            );
        }

        // The channel keeps its `named()` name; `$name` is the sink's, which is
        // the default when the edge was not named. The channel name is what a
        // frame carries and what selects a forward target's entry channel.
        $this->channel('plugin', $this->pending['channelName'] ?? $name, [], $encode);
    }

    /**
     * Create the channel this edge terminates on, carrying everything the
     * fluent calls put aside.
     *
     * One place, because an inter-procedure hop and a plugin sink differ only
     * in their kind, their name and whether they name a consumer — and a
     * setting applied on one but not the other is a fluent call that silently
     * does nothing on half the edges.
     *
     * @param list<int>                     $consumers
     * @param (callable(mixed):string)|null $encode
     */
    private function channel(string $kind, string $name, array $consumers, ?callable $encode = null): void
    {
        $this->builder->addChannel(
            name: $name,
            kind: $kind,
            producer: $this->from[1]->index,
            consumers: $consumers,
            overflow: $this->pending['overflow'] ?? Overflow::DropNothing,
            retryMax: $this->pending['retryMax'] ?? null,
            retryBackoffMs: $this->pending['retryBackoffMs'] ?? null,
            keyed: $this->pending['keyed'] ?? false,
            transforms: $this->transforms(),
            keyBy: $this->pending['keyBy'] ?? null,
            encode: $encode,
        );
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
        $channel->keyBy = $this->pending['keyBy'] ?? $channel->keyBy;
        $channel->transforms = [...$channel->transforms, ...$this->transforms()];
    }
}
