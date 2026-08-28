<?php

declare(strict_types=1);

namespace Flezha\Graph;

use Flezha\Overflow;

/**
 * A channel as the graph holds it. Its position in the list IS its id — the
 * core routes on that index, and the declaration order is what fixes it.
 */
final class ChannelSpec
{
    /**
     * @param list<int>                   $consumers
     * @param list<callable(mixed):mixed> $transforms
     */
    public function __construct(
        public readonly int $index,
        public readonly string $name,
        /** `entry` | `inter_proc` | `plugin` */
        public readonly string $kind,
        public ?int $producer = null,
        public array $consumers = [],
        public Overflow $overflow = Overflow::DropNothing,
        public ?int $retryMax = null,
        public ?int $retryBackoffMs = null,
        public bool $keyed = false,
        public array $transforms = [],
        /** @var (callable(mixed):string)|null */
        public $keyBy = null,
        /** @var (callable(string):mixed)|null */
        public $decode = null,
        /** @var (callable(mixed):string)|null */
        public $encode = null,
    ) {
    }
}
