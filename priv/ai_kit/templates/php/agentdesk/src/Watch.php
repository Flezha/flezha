<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Ctx;
use Flezha\Tasklet;

/** Answers a request for a session's events with its stream, and any other request with 404. */
#[Tasklet\Spec(name: 'watch')]
final class Watch extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        // keyed by the user: an output of an effectively-once relay needs a key
        $ctx->emit($input->listens()
            ? ['stream' => $input->user()]
            : ['status' => 404, 'body' => 'no such route'], key: $input->user());
    }
}
