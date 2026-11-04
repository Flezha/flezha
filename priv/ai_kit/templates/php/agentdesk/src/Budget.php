<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Ctx;
use Flezha\Response;
use Flezha\Tasklet;

/**
 * Keeps the tokens a user's answers used today, and admits a message while they are under the
 * day's budget. One cell per user.
 *
 * What reaches a user's cell is a message to admit, or the tokens an answer used; what it hands on
 * is a verdict, the message and the tokens the user had spent when it arrived.
 */
#[Tasklet\Spec(name: 'budget')]
final class Budget extends Tasklet
{
    public const DAILY_TOKENS = 40;

    /** @param array{message: array{user: string, text: string}, spent: int} $verdict */
    public static function admitted(array $verdict): bool
    {
        return $verdict['spent'] < self::DAILY_TOKENS;
    }

    /** @return array{user: string, tokens: int} */
    public static function used(Response $response): array    // a failed turn used none
    {
        return [
            'user' => $response->request['user'],
            'tokens' => $response->ok() ? $response->value['tokens'] : 0,
        ];
    }

    public function initState(?string $cellKey): mixed
    {
        return ['day' => '', 'tokens' => 0];         // what each user's cell remembers
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state = ['day' => $change['day'], 'tokens' => self::on($state, $change['day']) + $change['tokens']];
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $today = gmdate('Y-m-d', intdiv($ctx->now(), 1000));
        if (isset($input['tokens'])) {               // the tokens an answer used: count them
            $ctx->change(['day' => $today, 'tokens' => $input['tokens']]);
        } else {                                     // a message to admit: hand on its verdict
            $ctx->emit(['message' => $input, 'spent' => self::on($ctx->state, $today)]);
        }
    }

    /** @param array{day: string, tokens: int} $spent */
    private static function on(array $spent, string $today): int
    {
        return $spent['day'] === $today ? $spent['tokens'] : 0;
    }
}
