<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * Answers a message with the demo model, sending the answer as it grows, and keeps the
 * conversation so the next answer knows what came before.
 */
#[Tasklet\Spec(name: 'reply')]
final class Reply extends Tasklet
{
    public function initState(?string $cellKey): mixed
    {
        return ['messages' => []];                   // what the session remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        match ($change['kind']) {
            'said' => $state['messages'][] = $change['message'],
            'unsaid' => array_pop($state['messages']),
        };
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        // Each piece streams out as it is made; then the whole answer.
        $ctx->emit(DemoModel::answer($ctx->state['messages'], $input, $ctx->part(...)));
        $ctx->change(['kind' => 'said', 'message' => $input]);
    }

    // The undo, when a turn is taken back: the conversation forgets its message.
    public function compensate(array $outputs, Ctx $ctx): void
    {
        echo '[undo] forgot "' . end($ctx->state['messages']) . "\"\n";
        $ctx->change(['kind' => 'unsaid']);
    }
}
