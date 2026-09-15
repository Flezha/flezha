<?php

declare(strict_types=1);

/**
 * The two-procedure word count, run in this process with no controller.
 *
 *   scripts/build-php-sdk.sh
 *   php sdk/php/examples/E07_LocalMode.php
 *
 * `Split` and `Tally` are ordinary procedures with nothing local-mode about
 * them: the same classes deploy against a controller unchanged. What local
 * mode supplies is the sink — the values `Tally` releases arrive here instead
 * of on a socket.
 *
 * State is held in memory and nothing is durable; the run says so on stderr.
 */

namespace Flezha\Examples\E07;

require_once __DIR__ . '/../vendor/autoload.php';

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\ChannelCodec;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\LocalOperative;
use Flezha\Procedure;
use Flezha\SinkOutput;
use Flezha\StreamOperative;

/** One line into its words: the hop to `Tally` is the runtime's job. */
#[Proc]
final class Split extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        foreach (preg_split('/\s+/', trim((string) $input), -1, PREG_SPLIT_NO_EMPTY) ?: [] as $word) {
            $ctx->emit(strtolower($word));
        }

        return null;
    }
}

/** One Entity per word, minted on first sight of it. */
#[Proc(dynamicEntities: true)]
final class Tally extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return 0;
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state = (int) $change;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $seen = ((int) $ctx->state) + 1;
        $ctx->change($seen);

        return (string) $input . ' ' . $seen;
    }
}

#[Operative(factoryId: 'local_word_count_example')]
final class WordCountOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('lines', codec: ChannelCodec::Text)
            ->into(Split::class)
            // One Entity per word: the hop from `Split` to `Tally` is the
            // runtime's, not this file's.
            ->keyBy(static fn (string $word): string => $word)
            ->into(Tally::class)->keyed()
            ->sink('totals');
    }
}

if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    $run = LocalOperative::start(WordCountOperative::class);

    try {
        $run->onSink('totals', static fn (SinkOutput $out) => print($out->payload . PHP_EOL));
        $run->feed('lines', 'the quick brown fox');
        $run->feed('lines', 'the lazy dog');
        $run->runUntilIdle(10_000);
    } finally {
        $run->close();
    }
}
