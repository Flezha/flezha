<?php

declare(strict_types=1);

namespace Flezha;

/**
 * The Flezha PHP SDK: a poll host over `flz_core`.
 *
 * The SDK contains no kernel code. Transport, framing, ordering, per-entity
 * exclusivity, durability, retries, the retry-to-park ladder, timers, credit,
 * routing, the workflow driver and reconnect all live in the core; this
 * package brings the authoring surface, the codec for user types, the
 * concurrency host and idiomatic errors.
 *
 *     #[Proc]
 *     final class Echo_ extends Procedure
 *     {
 *         public function run(mixed $input, Ctx $ctx): mixed { return $input; }
 *     }
 *
 *     #[Operative(factoryId: 'echo')]
 *     final class EchoOperative extends StreamOperative
 *     {
 *         public static function channels(Builder $c): void
 *         {
 *             $c->input('input')->into(Echo_::class)->sink('out');
 *         }
 *     }
 *
 *     Flezha::createRunner('default')->defineStream(EchoOperative::class)->run();
 */
final class Flezha
{
    private function __construct()
    {
    }

    /** Start here. */
    public static function createRunner(string $namespace): Runner
    {
        return new Runner($namespace);
    }
}
