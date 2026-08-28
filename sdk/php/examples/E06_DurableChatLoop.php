<?php

declare(strict_types=1);

/**
 * Run-sugar: one imperative body that receives, emits and completes.
 *
 * `recv()` takes a timeout and answers null on expiry rather than parking
 * indefinitely, so the body observes cancellation between attempts — which is
 * what makes a long-lived body stoppable. Under `SyncExecutor` a run-sugar body
 * owns the process for its duration; under `SwooleExecutor` it owns one thread
 * of the pool. Size such a deployment by concurrent instances, not by cores.
 */

namespace Flezha\Examples\E06;

require_once __DIR__ . '/../vendor/autoload.php';

use Flezha\Attribute\Operative;
use Flezha\Attribute\Workflow;
use Flezha\Flezha;
use Flezha\LoopMode;
use Flezha\WorkflowCtx;
use Flezha\WorkflowOperative;

#[Operative(factoryId: 'chat')]
#[Workflow(loopMode: LoopMode::Once, maxDurationMs: 3_600_000)]
final class ChatWorkflow extends WorkflowOperative
{
    public static function run(mixed $args, WorkflowCtx $ctx): mixed
    {
        $history = [];

        while (!$ctx->isCancelled()) {
            $message = $ctx->recv(1_000);
            if ($message === null) {
                continue;
            }
            if (($message['kind'] ?? null) === 'end') {
                break;
            }

            $history[] = $message['text'];
            $ctx->emit(['reply' => 'noted: ' . $message['text'], 'seen' => count($history)]);
        }

        $ctx->complete(['messages' => count($history)]);

        return null;
    }
}

if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    Flezha::createRunner($argv[1] ?? 'default')->defineWorkflow(ChatWorkflow::class)->run();
}
