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
    /** @param list<ConsumerEdge> $consumers */
    public function __construct(
        public readonly int $index,
        /** Rewritten by a `named(...)` override on an edge off this channel. */
        public string $name,
        /** `entry` | `inter_proc` | `plugin` */
        public readonly string $kind,
        public ?int $producer = null,
        /**
         * The edges the core derived from this channel's chains. Filled from
         * the derived declaration, never here: which chain became an edge, and
         * which re-declaration survives, is the core's answer.
         *
         * @var list<ConsumerEdge>
         */
        public array $consumers = [],
        /**
         * The links of every fluent sequence declared off this channel's
         * handle, in declaration order — the record's own `chains`.
         *
         * @var list<list<array<string, mixed>>>
         */
        public array $chains = [],
        /**
         * Parallel to `$chains`: the consumer each sequence closed on, at the
         * position the core reads that sequence at, so a derived edge naming a
         * chain finds the closures the author declared with it.
         *
         * @var list<?ConsumerEdge>
         */
        public array $chainEdges = [],
        public Overflow $overflow = Overflow::DropNothing,
        public ?int $retryMax = null,
        public ?int $retryBackoffMs = null,
        public bool $keyed = false,
        /** The plugin sink attached to this channel; null means none. */
        public ?string $sink = null,
        /**
         * The entry payload's decoder, which `input` always sets. A channel
         * between procedures has none: its payload is the class codec's.
         *
         * @var (callable(string):mixed)|null
         */
        public $decode = null,
        /**
         * The plugin sink's payload encoder, which `sink` always sets. It
         * belongs to the sink frame alone: an inter-procedure frame is decoded
         * with the class codec, so encoding one with this hands the consuming
         * procedure bytes it cannot read.
         *
         * @var (callable(mixed):string)|null
         */
        public $sinkEncode = null,
    ) {
    }
}
