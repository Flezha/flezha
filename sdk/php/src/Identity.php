<?php

declare(strict_types=1);

namespace Flezha;

use Flezha\Exception\FlezhaException;

/**
 * Entity identity bytes and the replay-stable identifier derived from them.
 *
 * Both algorithms are fixed by the SDK contract rather than by a library, so
 * every SDK derives the same value from the same entity: the identity is
 * length-prefixed `instance | procedure | key`, and the identifier is FNV-1a
 * 128 over `identity | change seq | per-invocation ordinal`.
 *
 * PHP has no 128-bit integer, so the hash is carried as four 32-bit limbs.
 * The FNV prime is `2^88 + 0x13b`, which makes the multiply a small-word
 * multiply plus a shift rather than a general 128×128 — the standard
 * decomposition, and the reason no arbitrary-precision extension is needed.
 */
final class Identity
{
    /** `0x6c62272e07bb014262b821756295c58d`, most significant limb first. */
    private const OFFSET_BASIS = [0x6C62272E, 0x07BB0142, 0x62B82175, 0x6295C58D];

    private const MASK32 = 0xFFFFFFFF;

    private function __construct()
    {
    }

    /**
     * `[u8 len][instance][u8 len][procedure][u16 len][key]`, big-endian
     * lengths. The layout every SDK writes, so an identifier derived here
     * matches one derived there for the same entity.
     */
    public static function entity(string $instanceId, string $procedureName, string $entityKey): string
    {
        if (strlen($instanceId) > 255 || strlen($procedureName) > 255) {
            throw new FlezhaException('operative class / procedure id must be at most 255 bytes');
        }
        if (strlen($entityKey) > 0xFFFF) {
            throw new FlezhaException('entity key must be at most 65535 bytes');
        }

        return pack('C', strlen($instanceId)) . $instanceId
            . pack('C', strlen($procedureName)) . $procedureName
            . pack('n', strlen($entityKey)) . $entityKey;
    }

    /**
     * Thirty-two lowercase hex characters. Repeated calls in one invocation
     * differ; the n-th call of a replayed invocation equals the n-th call of
     * the original, which is what makes it the replacement for a random UUID.
     */
    public static function stableId(string $identity, int $changeSeq, int $ordinal): string
    {
        $hash = self::hash(self::OFFSET_BASIS, $identity);
        $hash = self::hash($hash, pack('J', $changeSeq));
        $hash = self::hash($hash, pack('N', $ordinal));

        return vsprintf('%08x%08x%08x%08x', $hash);
    }

    /**
     * FNV-1a 128 over `$bytes`, seeded with `$seed`.
     *
     * @param array{int, int, int, int} $seed  limbs, most significant first
     * @return array{int, int, int, int}
     */
    public static function hash(array $seed, string $bytes): array
    {
        [$h3, $h2, $h1, $h0] = $seed;

        for ($i = 0, $length = strlen($bytes); $i < $length; $i++) {
            $h0 ^= ord($bytes[$i]);

            // h * (2^88 + 0x13b) = (h * 0x13b) + (h << 88), both mod 2^128.
            $carry = 0;
            $p0 = $h0 * 0x13B + $carry;
            $carry = $p0 >> 32;
            $p0 &= self::MASK32;
            $p1 = $h1 * 0x13B + $carry;
            $carry = $p1 >> 32;
            $p1 &= self::MASK32;
            $p2 = $h2 * 0x13B + $carry;
            $carry = $p2 >> 32;
            $p2 &= self::MASK32;
            $p3 = ($h3 * 0x13B + $carry) & self::MASK32;

            // 88 = 64 + 24: the shifted value's low two limbs are zero, and
            // only the original's low 40 bits survive the truncation.
            $s2 = ($h0 << 24) & self::MASK32;
            $s3 = (($h0 >> 8) | ($h1 << 24)) & self::MASK32;

            $h0 = $p0;
            $sum = $p1;
            $h1 = $sum & self::MASK32;
            $sum = $p2 + $s2 + ($sum >> 32);
            $h2 = $sum & self::MASK32;
            $h3 = ($p3 + $s3 + ($sum >> 32)) & self::MASK32;
        }

        return [$h3, $h2, $h1, $h0];
    }
}
