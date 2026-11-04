<?php

declare(strict_types=1);

namespace Flezha\Examples\Agentdesk;

use Flezha\Exception\NonRetryableException;

/**
 * The demo model: a canned answer, one token at a time, and the number of tokens it used. It
 * stands in for a real model so the example runs anywhere; a real model's client would be called
 * here instead, handing each token it streams to `$part` the same way.
 */
final class DemoModel
{
    private const MAX_MESSAGE = 200;
    private const PER_TOKEN_US = 50_000;

    /**
     * @param list<string>                                  $earlier
     * @param callable(array{text: string, tokens: int}):void $part
     * @return array{text: string, tokens: int}
     */
    public static function answer(array $earlier, string $message, callable $part): array
    {
        $length = mb_strlen($message);
        if ($length > self::MAX_MESSAGE) {
            throw new NonRetryableException(
                'the demo model reads at most ' . self::MAX_MESSAGE . " characters, and this message has $length",
            );
        }
        $text = 'Message ' . (count($earlier) + 1) . ": you said \"$message\". I am a demo model.";
        $tokens = preg_split('/(?<= )/', $text);
        foreach ($tokens as $i => $token) {
            $part(['text' => $token, 'tokens' => $i + 1]);
            usleep(self::PER_TOKEN_US);
        }

        return ['text' => $text, 'tokens' => count($tokens)];
    }
}
