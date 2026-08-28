<?php

declare(strict_types=1);

namespace Flezha\Dispatch;

use Flezha\Definition;
use Flezha\Exception\FlezhaException;

/**
 * Rebuilding an entity's state from what was persisted.
 *
 * One implementation, and both of its callers are the real thing — the
 * dispatcher answering a restore fold, and the test harness constructing from
 * a payload — which is what makes the harness prove what a warm restart does
 * rather than a second implementation that agrees with it.
 */
final class Restore
{
    private function __construct()
    {
    }

    /**
     * The declared starting value, the snapshot baseline when there is one,
     * then every change folded on top in order.
     *
     * @param array{snapshot?: array{bytes: string, from_version: int|null}|null, changes?: list<array{bytes: string, from_version: int|null}>} $payload
     */
    public static function fold(
        Definition $definition,
        array $payload,
        ?string $entityKey,
        string $procedure,
    ): mixed {
        $state = $definition->initialState($entityKey);

        $snapshot = $payload['snapshot'] ?? null;
        if ($snapshot !== null) {
            $state = self::body($definition, 'state', $snapshot, $procedure);
        }

        foreach ($payload['changes'] ?? [] as $body) {
            if (!$definition->declares('applyChange')) {
                throw new FlezhaException(sprintf(
                    '%s was handed a change log but declares no applyChange; a procedure that '
                    . 'records no changes should have no durable records to fold',
                    $procedure,
                ));
            }
            $definition->foldChange($state, self::body($definition, 'change', $body, $procedure));
        }

        return $state;
    }

    /**
     * Decode one durable body, choosing between the three things it could be:
     * a payload from an older schema version, one the procedure declared its
     * own codec for, or the JSON default.
     *
     * @param array{bytes: string, from_version: int|null} $body
     */
    public static function body(Definition $definition, string $which, array $body, string $procedure): mixed
    {
        $fromVersion = $body['from_version'];
        $bytes = $body['bytes'];

        if ($fromVersion !== null) {
            $hook = $which === 'state' ? 'migrateState' : 'migrateChange';
            if (!$definition->declares($hook)) {
                throw new FlezhaException(sprintf(
                    '%s was handed a %s record written at schema version %d and declares no %s; '
                    . 'declare one, or raise minReadableSchemaVersion so the record is refused '
                    . 'rather than misread',
                    $procedure,
                    $which,
                    $fromVersion,
                    $hook,
                ));
            }

            return $definition->instance->{$hook}($fromVersion, $bytes);
        }

        return $definition->decode($which, $bytes);
    }
}
