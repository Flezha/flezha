<?php

declare(strict_types=1);

/**
 * Durable state, and the one rule that goes with it: state is mutated only
 * through a recorded change, because the change log IS how the state is
 * rebuilt after a restart.
 *
 * `Harness::assertReplayEquals()` in your own suite is what makes that rule
 * safe — it catches a handler that mutated the state without recording a
 * change, and an `applyChange` that is not a pure fold.
 */

namespace Flezha\Examples\E02;

require_once __DIR__ . '/../vendor/autoload.php';

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Codable;
use Flezha\Ctx;
use Flezha\Flezha;
use Flezha\Graph\Builder;
use Flezha\Procedure;
use Flezha\StreamOperative;

/** The durable bytes carry this array, never the class name — so a rename is safe. */
final class Totals implements Codable
{
    public function __construct(public int $count = 0, public int $amount = 0)
    {
    }

    public function toArray(): array
    {
        return ['count' => $this->count, 'amount' => $this->amount];
    }

    public static function fromArray(array $data): static
    {
        return new self((int) $data['count'], (int) $data['amount']);
    }
}

#[Proc(dynamicEntities: true, stateType: Totals::class)]
final class Counter extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return new Totals();
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state->count++;
        $state->amount += (int) $change;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change((int) $input['amount']);

        return ['customer' => $ctx->entityKey, 'total' => $ctx->state->amount];
    }
}

#[Operative(factoryId: 'order_totals')]
final class OrderTotalsOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('orders')
            ->keyBy(static fn (array $order): string => (string) $order['customer'])
            ->into(Counter::class)->named('totals')->keyed()
            ->sink('totals_out');
    }
}

if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    Flezha::createRunner($argv[1] ?? 'default')->defineStream(OrderTotalsOperative::class)->run();
}
