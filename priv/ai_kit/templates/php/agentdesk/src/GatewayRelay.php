<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Attribute\Relay;
use Flezha\ChannelBuilder;

/**
 * The HTTP side of the desk: a flow relay that carries the HTTP plugin, answers every
 * request, and calls the session a message is for.
 */
#[Relay(
    factoryId: 'gateway',
    run: ['flow' => ['exactly' => 'effectively_once']], // each request and answer on disk before it runs
)]
final class GatewayRelay extends \Flezha\Relay
{
    private const DEADLINE_MS = 60_000;

    public static function channels(ChannelBuilder $c): void
    {
        $user = fn (array $value) => $value['user'];
        // every request arrives on "http"; kept as a handle to split it
        $http = $c->input('http', Request::decode(...));
        // route 1: GET .../events gets the session's stream
        $http->filter(fn (Request $request) => !$request->sends())
            ->into(Watch::class)
            ->keyed()
            ->output();                              // a stream, or 404
        // route 2: POST .../messages is checked against the user's budget
        $verdicts = $http->filter(fn (Request $request) => $request->sends())
            ->map(fn (Request $request) => $request->message())
            ->keyBy($user)                           // one cell per user
            ->into(Budget::class);
        // a verdict: the message plus its user's tokens today; used twice
        // use 1: answer the POST
        $verdicts->into(Admit::class)
            ->keyed()
            ->output();                              // 202, or 429 over the budget
        // use 2: pass an admitted message on to the user's session
        $verdicts->filter(Budget::admitted(...))
            ->map(fn (array $verdict) => $verdict['message'])
            ->keyBy($user)                           // the user's session
            // call the "agent" relay; its answer comes back here
            ->call(factory: 'agent', channel: 'message', deadlineMs: self::DEADLINE_MS)
            ->map(Budget::used(...))                 // the session's answer, as tokens used
            ->keyBy($user)
            ->into(Budget::class);                   // back into the same budget
    }
}
