<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Codec\ActivationCodec;
use Flezha\Ffi\Core;

/**
 * The cross-language byte pin.
 *
 * The fixtures are read out of `flz_core/src/schema.rs` rather than copied
 * here, so a schema change is a red PHP build in the commit that makes it —
 * which is the whole of task "CI catches an unsynchronised bump". A copy would
 * only fail once someone remembered to update it.
 *
 * Both directions are pinned against the bytes rather than against each other:
 * decode-then-encode round trips pass a mistake that is symmetric, and every
 * mistake in a hand-written codec is.
 */
final class SchemaFixtureTest extends RepositoryTestCase
{
    private function schemaSource(): string
    {
        return file_get_contents($this->repositoryFile('flz_core/src/schema.rs')) ?: '';
    }

    public function testTheShimSpeaksTheCoresSchemaVersion(): void
    {
        preg_match('/pub const SCHEMA_VERSION: u32 = (\d+);/', $this->schemaSource(), $matches);

        self::assertNotEmpty($matches, 'flz_core/src/schema.rs no longer declares SCHEMA_VERSION');
        self::assertSame(
            (int) $matches[1],
            Core::SCHEMA_VERSION,
            'the core bumped its activation schema and this package still speaks the old one; '
            . 'move Core::SCHEMA_VERSION and re-check the fixtures below',
        );
    }

    public function testTheWorkActivationFixtureDecodesToTheValuesTheCoreEncoded(): void
    {
        $activation = ActivationCodec::decodeActivation(self::bytes($this->pinnedHex()[0]));

        self::assertSame([
            'token' => 42,
            'operative' => 'op-1',
            'procedure' => 3,
            'entity_key' => 'k',
            'kind' => 'work',
            'items' => [
                [
                    'kind' => 'input',
                    'channel' => 1,
                    'payload' => "\xAA\xBB",
                    'metadata' => ['flz.offset' => '7'],
                    'consumed_offset' => 7,
                    'replay' => true,
                    'now' => 1_000,
                ],
                ['kind' => 'timer', 'timer_kind' => 'event_time', 'scheduled_at' => 2_000],
            ],
            'change_seq' => 9,
            'factory' => 'f',
        ], $activation);
    }

    public function testTheResultFixtureIsEncodedByteForByte(): void
    {
        $encoded = ActivationCodec::encodeResult([
            'token' => 42,
            'outcome' => 'work',
            'effects' => [[
                'verdict' => 'ok',
                'changes' => ["\x01"],
                'outputs' => [[
                    'target' => 'plugin',
                    'plugin_channel' => 'out',
                    'key' => 'k2',
                    'event_time' => null,
                    'metadata' => [],
                    'payload' => "\x02",
                ]],
                'consumed_offset' => 7,
                'event_time' => 1_000,
                'timers' => [['kind' => 'processing_time', 'scheduled_at' => 2_000, 'register' => false]],
                'checkpoint' => true,
            ]],
        ]);

        self::assertSame($this->pinnedHex()[1], bin2hex($encoded));
    }

    /**
     * Every command activation kind, against the same pinned table the Rust
     * fixture walks — envelope prefix included, so a shim cannot pass by
     * agreeing on the payload and disagreeing on the target.
     */
    public function testEveryCommandKindDecodes(): void
    {
        $expected = [
            'restore_fold' => [
                'snapshot' => ['bytes' => "\x0F", 'from_version' => null],
                'changes' => [
                    ['bytes' => "\x01", 'from_version' => null],
                    ['bytes' => "\x02", 'from_version' => 1],
                ],
            ],
            'snapshot_request' => [],
            'live_state_read' => [],
            'apply_state' => ['state_json' => '{}'],
            'migration_serialize' => [],
            'key_request' => ['key_request' => [
                'channel' => 2,
                'payloads' => ['p'],
                'want_keys' => true,
                'want_event_time' => false,
            ]],
            'workflow_run' => ['args' => 'go'],
            'compensate_run' => ['outputs' => ["\x0A"]],
        ];

        $tails = $this->commandKindHex();
        self::assertCount(count($expected), $tails, 'the core changed its command-kind fixture table');

        foreach (array_combine(array_keys($expected), $tails) as $kind => $tail) {
            $activation = ActivationCodec::decodeActivation(self::bytes('01016f0000' . $tail));

            self::assertSame([
                'token' => 1,
                'operative' => 'o',
                'procedure' => 0,
                'entity_key' => null,
                'kind' => $kind,
                ...$expected[$kind],
                'change_seq' => 0,
                'factory' => 'f',
            ], $activation, "the $kind fixture");
        }
    }

    /** @return array{string, string} the work-activation and result fixtures */
    private function pinnedHex(): array
    {
        preg_match_all('/^\s+"([0-9a-f]{16,})"$/m', $this->schemaSource(), $matches);

        self::assertCount(
            2,
            $matches[1],
            'flz_core/src/schema.rs no longer pins exactly the two long fixtures this reads',
        );

        return [$matches[1][0], $matches[1][1]];
    }

    /** @return list<string> the eight command-kind encodings, in declaration order */
    private function commandKindHex(): array
    {
        $source = $this->schemaSource();
        $start = strpos($source, 'fn command_kind_bytes_are_pinned');
        self::assertNotFalse($start, 'flz_core/src/schema.rs no longer pins the command kinds');

        $table = substr($source, $start, (int) strpos($source, 'for (kind, expected) in kinds') - $start);
        preg_match_all('/"([0-9a-f]{6,})"/', $table, $matches);

        return $matches[1];
    }

    private static function bytes(string $hex): string
    {
        return (string) hex2bin($hex);
    }
}
