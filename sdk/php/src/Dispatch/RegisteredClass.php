<?php

declare(strict_types=1);

namespace Flezha\Dispatch;

use Flezha\Graph\ChannelSpec;
use Flezha\Graph\ClassSpec;
use Flezha\Graph\ProcNode;

/**
 * A declared class, with the lookups dispatch needs on the hot path, and the
 * live state of every entity it owns.
 */
final class RegisteredClass
{
    /** @var array<int, list<ChannelSpec>> the channels each procedure produces on */
    public readonly array $outgoing;

    /** @var array<string, mixed> */
    private array $entities = [];

    public function __construct(public readonly ClassSpec $spec)
    {
        $outgoing = [];
        foreach ($spec->procedures as $node) {
            $outgoing[$node->index] = array_values(array_filter(
                $spec->channels,
                static fn (ChannelSpec $c): bool => $c->producer === $node->index,
            ));
        }
        $this->outgoing = $outgoing;
    }

    /**
     * The entity's live state, created from the declaration on first touch.
     *
     * No lock, and none is missing: a `Swoole\Thread` is a separate
     * interpreter with its own object graph, so two threads never share this
     * map — and per-entity exclusivity, which is the core's rather than this
     * map's, is what makes the entity a thread holds the only one in play
     * while it holds it.
     */
    public function stateOf(ProcNode $node, ?string $entityKey): mixed
    {
        $key = $node->index . "\0" . $entityKey;
        if (!array_key_exists($key, $this->entities)) {
            $this->entities[$key] = $node->definition->initialState($entityKey);
        }

        return $this->entities[$key];
    }

    public function setState(ProcNode $node, ?string $entityKey, mixed $state): void
    {
        $this->entities[$node->index . "\0" . $entityKey] = $state;
    }
}
