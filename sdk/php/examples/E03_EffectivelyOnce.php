<?php

declare(strict_types=1);

/**
 * The effectively-once tier, with checkpoint acknowledgement.
 *
 * The tier IS durable state, so it needs persistence; every plugin-bound
 * channel must be `keyed()`, because deduplication is by output identity. With
 * `checkpointEvery`, changes stage without advancing the watermark until the
 * interval closes — every four inputs here, or on an explicit
 * `$ctx->checkpoint()`.
 */

namespace Flezha\Examples\E03;

require_once __DIR__ . '/../vendor/autoload.php';

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Flezha;
use Flezha\Graph\Builder;
use Flezha\Procedure;
use Flezha\StreamOperative;

#[Proc(dynamicEntities: true)]
final class Ledger extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['balance' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['balance'] += (int) $change;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change((int) $input['delta']);

        // Close the interval as soon as the balance is settled, rather than
        // waiting for the count: an explicit checkpoint is what a business
        // boundary looks like.
        if ($input['settled'] ?? false) {
            $ctx->checkpoint();
        }

        return ['account' => $ctx->entityKey, 'balance' => $ctx->state['balance']];
    }
}

#[Operative(factoryId: 'ledger')]
final class LedgerOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->checkpointEvery(4);
        $c->input('entries')
            ->keyBy(static fn (array $e): string => (string) $e['account'])
            ->into(Ledger::class)->named('balances')->keyed()
            ->sink('balances_out');
    }
}

if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    Flezha::createRunner($argv[1] ?? 'default')->defineStream(LedgerOperative::class)->run();
}
