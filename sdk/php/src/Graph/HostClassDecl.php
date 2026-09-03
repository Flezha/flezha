<?php

declare(strict_types=1);

namespace Flezha\Graph;

/**
 * The declaration the core deserializes.
 *
 * Every key here is pinned by `test/fixtures/host_class_decl_keys.txt`, which
 * this package's own test reads: a key the core learns that PHP cannot spell
 * fails that test, which is what makes "one change, every SDK" a red build
 * rather than a silently narrower SDK.
 */
final class HostClassDecl
{
    /** The PHP SDK's byte in the core's `sdk_lang` allocation table. */
    public const SDK_LANG_PHP = 6;

    /**
     * This SDK's own released version, stamped into every durable record it
     * writes; the core packs it. Sole source of truth: `composer.json` declares
     * no `version` key, because Composer resolves a published package's version
     * from its VCS tag and a key in the file is how a stale one ships. So the
     * release checklist bumps this constant, and no gate can compare it against
     * package metadata that does not exist.
     */
    public const SDK_VERSION = '0.1.0';

    /**
     * JSON. The byte names the encoding, not the SDK — the cross-language gate
     * is `sdk_lang`.
     */
    public const FORMAT_ID_JSON = 6;

    private function __construct()
    {
    }

    /** @return array<string, mixed> */
    public static function of(ClassSpec $spec): array
    {
        $workflow = $spec->workflow;

        return self::compact([
            'factory_id' => $spec->factoryId,
            'persistent' => $spec->persistent,
            'sdk_lang' => self::SDK_LANG_PHP,
            'sdk_version' => self::SDK_VERSION,
            'change_format_id' => self::FORMAT_ID_JSON,
            ...self::streamTier($spec),
            'compact_after_bytes' => $spec->compactAfterBytes,
            'sync_durability' => $spec->syncDurability,
            'channels' => array_map(self::channel(...), $spec->channels),
            'procedures' => array_map(self::procedure(...), $spec->procedures),
            'workflow' => $workflow === null ? null : self::compact([
                'loop_mode' => $workflow->loopMode->value,
                'rollback_mode' => $workflow->rollbackMode->value,
                'retry_attempts' => $workflow->retryAttempts,
                'retry_backoff_ms' => $workflow->retryBackoffMs,
                'barrier_retry_attempts' => $workflow->barrierRetryAttempts,
                'max_duration_ms' => $workflow->maxDurationMs,
                'wake_after_ms' => $workflow->wakeAfterMs,
            ]),
        ]);
    }

    /**
     * The tier, checkpoint and correction keys, which only a stream class puts
     * on the wire. The core refuses `effectively_once` and
     * `correction_window_ms` on a workflow class outright, and the checkpoint
     * family rests on a checkpoint acknowledgement that refusal leaves
     * unreachable there.
     *
     * The spec keeps the flag either way: `Ctx::stampIdentity` reads it to
     * decide whether a keyed plugin frame carries its own identity, so gating
     * the declaration must not gate the in-process tier.
     *
     * @return array<string, mixed>
     */
    private static function streamTier(ClassSpec $spec): array
    {
        if ($spec->kind !== 'stream') {
            return [];
        }

        return [
            'effectively_once' => $spec->effectivelyOnce,
            'ack_granularity' => $spec->ackGranularity->value,
            'checkpoint_every' => $spec->checkpointEvery,
            'checkpoint_outputs' => $spec->checkpointOutputs->value,
            'checkpoint_max_delay_ms' => $spec->checkpointMaxDelayMs,
            'correction_window_ms' => $spec->correctionWindowMs,
            'beyond_window_policy' => $spec->beyondWindowPolicy->value,
        ];
    }

    /** @return array<string, mixed> */
    private static function channel(ChannelSpec $channel): array
    {
        return self::compact([
            'name' => $channel->name,
            'kind' => Validate::declaredKind($channel),
            'producer' => $channel->producer,
            'consumers' => array_map(
                static fn (ConsumerEdge $edge): int => $edge->procedure,
                $channel->consumers
            ),
            'retry_max' => $channel->retryMax,
            'retry_backoff_ms' => $channel->retryBackoffMs,
            'overflow' => $channel->overflow->value,
            'keyed' => $channel->keyed,
        ]);
    }

    /** @return array<string, mixed> */
    private static function procedure(ProcNode $node): array
    {
        $definition = $node->definition;
        $decl = $definition->decl;

        return self::compact([
            'name' => $node->name,
            'entity_mode' => $definition->entityMode(),
            'entity_keys' => $decl->entityKeys ?? [],
            'idle_ttl_ms' => $decl->idleTtlMs,
            'max_entities' => $decl->maxEntities,
            'entities_for_overridden' => $definition->declares('entitiesFor'),
            'compensate_overridden' => $definition->declares('compensate'),
            'clock' => $decl->clock->value,
            'mailbox_capacity' => $decl->mailboxCapacity ?? 1024,
            'schema_version' => $decl->schemaVersion,
            'min_readable_schema_version' => $decl->minReadableSchemaVersion,
        ]);
    }

    /**
     * Drop the keys whose value is null, so the JSON omits them and the core's
     * own defaults apply.
     *
     * @param array<string, mixed> $values
     * @return array<string, mixed>
     */
    private static function compact(array $values): array
    {
        return array_filter($values, static fn (mixed $value): bool => $value !== null);
    }
}
