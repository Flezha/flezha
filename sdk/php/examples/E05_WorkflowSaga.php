<?php

declare(strict_types=1);

/**
 * A structured workflow chain, and the compensation an abort runs.
 *
 * Each stage is a procedure; the core's driver hands out one step activation
 * per stage and answers its barrier on completion. If a later stage fails, the
 * driver hands out `CompensateRun` for the stages that already committed, in
 * reverse — the SDK implements no driver of its own.
 */

namespace Flezha\Examples\E05;

require_once __DIR__ . '/../vendor/autoload.php';

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Attribute\Workflow;
use Flezha\Ctx;
use Flezha\Flezha;
use Flezha\Graph\Builder;
use Flezha\Procedure;
use Flezha\RollbackMode;
use Flezha\WorkflowOperative;

#[Proc]
final class ReserveStock extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return ['order' => $input['order'], 'reservation' => $ctx->stableId()];
    }

    public function compensate(array $outputs, Ctx $ctx): void
    {
        foreach ($outputs as $output) {
            $ctx->emit(['undo' => 'reservation', 'id' => $output['reservation']]);
        }
    }
}

#[Proc]
final class ChargeCard extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return ['order' => $input['order'], 'charge' => $ctx->stableId()];
    }

    public function compensate(array $outputs, Ctx $ctx): void
    {
        foreach ($outputs as $output) {
            $ctx->emit(['undo' => 'charge', 'id' => $output['charge']]);
        }
    }
}

#[Operative(factoryId: 'checkout')]
#[Workflow(rollbackMode: RollbackMode::AllOrNothing, retryAttempts: 3, retryBackoffMs: 200)]
final class CheckoutWorkflow extends WorkflowOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('orders')
            ->into(ReserveStock::class)->named('reserved')
            ->into(ChargeCard::class)->named('charged')
            ->sink('checkout_out');
    }
}

if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    Flezha::createRunner($argv[1] ?? 'default')->defineWorkflow(CheckoutWorkflow::class)->run();
}
