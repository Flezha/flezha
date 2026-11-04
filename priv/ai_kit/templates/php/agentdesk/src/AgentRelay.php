<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Attribute\Relay;
use Flezha\ChannelBuilder;
use Flezha\ChannelCodec;

/**
 * One conversation: a relay that runs in steps, one turn per message it is sent, for as long
 * as the session lasts. Every session is an instance of its own, with its own state.
 */
#[Relay(
    factoryId: 'agent',
    run: ['steps' => [
        'each' => 'per_input',                       // one turn per message
        'on_failure' => 'keep_progress',             // a failed turn fails alone
    ]],
    durable: ['local' => ['sleep_after' => 10_000]], // an idle session sleeps after 10 s
)]
final class AgentRelay extends \Flezha\Relay
{
    public static function channels(ChannelBuilder $c): void
    {
        $c->input('turn', codec: ChannelCodec::Text) // entry 1: `flz relay call`
            ->into(Reply::class)
            ->output('answer');                      // to whoever called, and whoever listens
        $c->input('message')                         // entry 2: the gateway, into the same Reply
            ->map(fn (array $message) => $message['text'])
            ->into(Reply::class);
    }
}
