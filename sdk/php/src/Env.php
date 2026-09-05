<?php

declare(strict_types=1);

namespace Flezha;

/**
 * The one parse rule per knob type, shared with every other Flezha runtime.
 *
 * A knob's name, type and default are declared once in `Flezha.Config.Keys`;
 * what this class owns is the rule that declaration promises, so a PHP runner
 * answers `"0"`, `"on"` or a typo exactly as the controller does.
 * `test/fixtures/knob_parse_cases.txt` is that rule written down.
 */
final class Env
{
    private const TRUE_WORDS = ['1', 'true', 'yes', 'on'];
    private const FALSE_WORDS = ['0', 'false', 'no', 'off'];

    /** A count of one or more, else null — `"0"` already means unset. */
    public static function parsePositive(?string $raw): ?int
    {
        $value = self::whole($raw);

        return $value !== null && $value > 0 ? $value : null;
    }

    /** A whole number of milliseconds, zero included. */
    public static function parseMs(?string $raw): ?int
    {
        $value = self::whole($raw);

        return $value !== null && $value >= 0 ? $value : null;
    }

    /** A whole number, negative included. */
    public static function parseInt(?string $raw): ?int
    {
        return self::whole($raw);
    }

    /** `1/true/yes/on` and `0/false/no/off`, case-insensitively. */
    public static function parseBool(?string $raw): ?bool
    {
        if ($raw === null) {
            return null;
        }
        $text = strtolower(trim($raw));
        if (in_array($text, self::TRUE_WORDS, true)) {
            return true;
        }
        if (in_array($text, self::FALSE_WORDS, true)) {
            return false;
        }

        return null;
    }

    /** A count from the environment, with $fallback for anything that is not one. */
    public static function positive(string $name, int $fallback): int
    {
        $raw = getenv($name);

        return self::parsePositive($raw === false ? null : $raw) ?? $fallback;
    }

    /** A flag from the environment, with $fallback for anything that is not one. */
    public static function flag(string $name, bool $fallback): bool
    {
        $raw = getenv($name);

        return self::parseBool($raw === false ? null : $raw) ?? $fallback;
    }

    private static function whole(?string $raw): ?int
    {
        if ($raw === null) {
            return null;
        }
        $text = trim($raw);

        return preg_match('/^-?\d+$/', $text) === 1 ? (int) $text : null;
    }
}
