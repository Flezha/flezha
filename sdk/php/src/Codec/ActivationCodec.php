<?php

declare(strict_types=1);

namespace Flezha\Codec;

use Flezha\Exception\DecodeException;

/**
 * The `Activation` / `ActivationResult` boundary contract, in PHP.
 *
 * Field order and variant index ARE the wire contract — the shapes here mirror
 * `flz_core/src/schema.rs` line for line, and the hex fixtures in that file's
 * test module are the cross-language pin every shim asserts against.
 *
 * Only the two directions a host actually uses exist: the core encodes
 * activations and decodes results, so this decodes the first and encodes the
 * second. Pinning each against the fixture bytes is a stronger check than a
 * round trip through both, which a symmetric mistake would pass.
 *
 * Activations decode to arrays rather than objects. The dispatcher reads a
 * handful of keys per activation and the shape is the schema's, not this
 * host's, so a parallel object model would be a second spelling of a contract
 * that already has one.
 */
final class ActivationCodec
{
    /** `ActivationKind` variants, in declaration order. */
    private const KINDS = [
        'work',
        'restore_fold',
        'snapshot_request',
        'live_state_read',
        'apply_state',
        'migration_serialize',
        'key_request',
        'workflow_run',
        'compensate_run',
    ];

    /** `ActivationOutcome` variants, in declaration order. */
    private const OUTCOMES = [
        'work',
        'restored',
        'snapshot',
        'state_json',
        'state_applied',
        'migration_state',
        'prepared',
        'failed',
        'workflow_done',
    ];

    /** `TimerKind` variants, in declaration order. */
    private const TIMER_KINDS = ['event_time', 'processing_time'];

    /** `Verdict` variants, in declaration order. */
    private const VERDICTS = ['ok', 'retryable', 'non_retryable'];

    private function __construct()
    {
    }

    /**
     * One activation, as the dispatcher reads it.
     *
     * @return array<string, mixed>
     */
    public static function decodeActivation(string $bytes): array
    {
        $c = new PostcardCursor($bytes);

        $activation = [
            'token' => $c->varint('activation token'),
            'operative' => $c->bytes('target operative'),
            'procedure' => $c->varint('target procedure'),
            'entity_key' => $c->some('target entity key') ? $c->bytes('target entity key') : null,
        ];

        $variant = $c->variant('activation kind');
        $kind = self::KINDS[$variant]
            ?? throw new DecodeException("activation kind $variant is not one this shim knows");
        $activation['kind'] = $kind;
        $activation += self::decodeKind($kind, $c);

        $activation['change_seq'] = $c->varint('change seq');
        $activation['factory'] = $c->bytes('factory');
        $c->finish('the activation');

        return $activation;
    }

    /**
     * One released `Output`, in the array shape {@see \Flezha\Ctx} builds and
     * this codec already encodes. The exact inverse of `encodeOutput`, and
     * what local mode hands a host for a value its graph released on a sink.
     *
     * @return array<string, mixed>
     */
    public static function decodeOutput(string $bytes): array
    {
        $c = new PostcardCursor($bytes);
        $output = $c->variant('output target') === 1
            ? ['target' => 'plugin', 'channel' => null, 'consumer' => null,
                'plugin_channel' => $c->bytes('plugin channel')]
            : ['target' => 'edge', 'channel' => $c->varint('edge channel'),
                'consumer' => $c->varint('edge consumer'), 'plugin_channel' => null];

        $output['key'] = $c->some('output key') ? $c->bytes('output key') : null;
        $output['event_time'] = $c->optionalVarint('output event time');
        $output['metadata'] = $c->metadata('output metadata');
        $output['payload'] = $c->bytes('output payload');
        $c->finish('the output');

        return $output;
    }

    /**
     * Just the correlation token, without decoding the rest.
     *
     * The one place that needs it is the activation budget: a loop that has
     * spent its budget hands the activation back untouched, and paying for a
     * full decode to name a token it will not use is the wrong trade.
     */
    public static function tokenOf(string $bytes): int
    {
        return (new PostcardCursor($bytes))->varint('activation token');
    }

    /**
     * The answer to one activation, as the core folds it.
     *
     * @param array<string, mixed> $result
     */
    public static function encodeResult(array $result): string
    {
        $outcome = (string) $result['outcome'];
        $index = array_search($outcome, self::OUTCOMES, true);
        if ($index === false) {
            throw new DecodeException("activation outcome `$outcome` is not one the schema has");
        }

        return Postcard::varint((int) $result['token'])
            . Postcard::variant($index)
            . self::encodeOutcome($outcome, $result);
    }

    /**
     * The fields that follow an activation's kind discriminant.
     *
     * @return array<string, mixed>
     */
    private static function decodeKind(string $kind, PostcardCursor $c): array
    {
        return match ($kind) {
            'work' => ['items' => $c->seq('work items', self::decodeWorkItem(...))],
            'restore_fold' => [
                'snapshot' => $c->some('restore snapshot') ? self::decodeRestoreBody($c) : null,
                'changes' => $c->seq('restore changes', self::decodeRestoreBody(...)),
            ],
            'apply_state' => ['state_json' => $c->bytes('state json')],
            'key_request' => [
                'key_request' => [
                    'channel' => $c->varint('key request channel'),
                    'payloads' => $c->seq('key request payloads', fn (PostcardCursor $p): string => $p->bytes('payload')),
                    'want_keys' => $c->bool('want keys'),
                    'want_event_time' => $c->bool('want event time'),
                ],
            ],
            'workflow_run' => ['args' => $c->bytes('workflow args')],
            'compensate_run' => [
                'outputs' => $c->seq('compensate outputs', fn (PostcardCursor $p): string => $p->bytes('output')),
            ],
            default => [],
        };
    }

    /** @return array<string, mixed> */
    private static function decodeWorkItem(PostcardCursor $c): array
    {
        return match ($variant = $c->variant('work item')) {
            0 => [
                'kind' => 'input',
                'channel' => $c->varint('input channel'),
                'payload' => $c->bytes('input payload'),
                'metadata' => $c->metadata('input metadata'),
                'consumed_offset' => $c->optionalVarint('consumed offset'),
                'replay' => $c->bool('replay'),
                'now' => $c->varint('now'),
            ],
            1 => [
                'kind' => 'timer',
                'timer_kind' => self::TIMER_KINDS[$c->variant('timer kind')]
                    ?? throw new DecodeException('a timer firing named a kind this shim does not know'),
                'scheduled_at' => $c->varint('timer scheduled_at'),
            ],
            default => throw new DecodeException("work item variant $variant is not one the schema has"),
        };
    }

    /** @return array{bytes: string, from_version: int|null} */
    private static function decodeRestoreBody(PostcardCursor $c): array
    {
        return [
            'bytes' => $c->bytes('restore body'),
            'from_version' => $c->some('restore body from_version')
                ? $c->varint('restore body from_version')
                : null,
        ];
    }

    /** @param array<string, mixed> $result */
    private static function encodeOutcome(string $outcome, array $result): string
    {
        return match ($outcome) {
            'work' => Postcard::seq($result['effects'] ?? [], self::encodeEffect(...)),
            'snapshot' => Postcard::bytes((string) ($result['state'] ?? '')),
            'state_json', 'migration_state' => Postcard::bytes((string) ($result['json'] ?? '')),
            'prepared' => Postcard::seq($result['prepared'] ?? [], self::encodePrepared(...)),
            'failed' => self::encodeWorkError(
                (string) ($result['error'] ?? ''),
                (bool) ($result['non_retryable'] ?? false),
            ),
            'workflow_done' => Postcard::bool((bool) ($result['workflow_completed'] ?? false))
                . Postcard::bytes((string) ($result['workflow_result'] ?? '')),
            default => '',
        };
    }

    /** @param array<string, mixed> $effect */
    private static function encodeEffect(array $effect): string
    {
        $verdict = (string) ($effect['verdict'] ?? 'ok');
        $index = array_search($verdict, self::VERDICTS, true);
        if ($index === false) {
            throw new DecodeException("item verdict `$verdict` is not one the schema has");
        }

        return Postcard::variant($index)
            . ($index === 0 ? '' : Postcard::bytes((string) ($effect['message'] ?? '')))
            . Postcard::seq($effect['changes'] ?? [], Postcard::bytes(...))
            . Postcard::seq($effect['outputs'] ?? [], self::encodeOutput(...))
            . Postcard::optionalVarint($effect['consumed_offset'] ?? null)
            . Postcard::varint((int) ($effect['event_time'] ?? 0))
            . Postcard::seq($effect['timers'] ?? [], self::encodeTimerOp(...))
            . Postcard::bool((bool) ($effect['checkpoint'] ?? false))
            // The core mints the DLQ entry when its own ladder parks, so a
            // host effect never carries one.
            . Postcard::option(null);
    }

    /** @param array<string, mixed> $output */
    private static function encodeOutput(array $output): string
    {
        $target = ($output['target'] ?? 'edge') === 'plugin'
            ? Postcard::variant(1) . Postcard::bytes((string) $output['plugin_channel'])
            : Postcard::variant(0)
                . Postcard::varint((int) $output['channel'])
                . Postcard::varint((int) $output['consumer']);

        return $target
            . Postcard::option($output['key'] === null ? null : Postcard::bytes((string) $output['key']))
            . Postcard::optionalVarint($output['event_time'] ?? null)
            . Postcard::metadata($output['metadata'] ?? [])
            . Postcard::bytes((string) $output['payload']);
    }

    /** @param array<string, mixed> $timer */
    private static function encodeTimerOp(array $timer): string
    {
        $index = array_search((string) $timer['kind'], self::TIMER_KINDS, true);
        if ($index === false) {
            throw new DecodeException("timer kind `{$timer['kind']}` is not one the schema has");
        }

        return Postcard::variant($index)
            . Postcard::varint((int) $timer['scheduled_at'])
            . Postcard::bool((bool) $timer['register']);
    }

    /**
     * One payload's prepare answer, or the refusal that sends it up the input
     * channel's retry ladder.
     *
     * @param array<string, mixed> $answer
     */
    private static function encodePrepared(array $answer): string
    {
        if (isset($answer['error'])) {
            return Postcard::variant(1)
                . self::encodeWorkError((string) $answer['error'], (bool) ($answer['non_retryable'] ?? false));
        }

        return Postcard::variant(0)
            . Postcard::seq($answer['keys'] ?? [], Postcard::bytes(...))
            . Postcard::optionalVarint($answer['event_time'] ?? null);
    }

    private static function encodeWorkError(string $message, bool $nonRetryable): string
    {
        return Postcard::bytes($message) . Postcard::bool($nonRetryable);
    }
}
