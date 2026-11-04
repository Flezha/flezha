<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/** Answers a message 202 when it goes on to the session, and 429 when its user is over the budget. */
#[Tasklet\Spec(name: 'admit')]
final class Admit extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $user = $input['message']['user'];
        $ctx->emit(Budget::admitted($input)
            ? ['status' => 202, 'body' => "sent to $user"]
            : [
                'status' => 429,
                'body' => "$user has used {$input['spent']} of today's " . Budget::DAILY_TOKENS . ' tokens',
            ], key: $user);
    }
}
