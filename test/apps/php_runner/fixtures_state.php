<?php

declare(strict_types=1);

/**
 * Fixtures whose subject is persisted Entity state: the keyed upsert the bench
 * shares, and the counter and resume twins — the same partition as
 * test/apps/python_runner/fixtures_state.py and the reference's state.rs.
 */

namespace Flezha\TestApp;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Codec\Json;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\Overflow;
use Flezha\Procedure;
use Flezha\StreamOperative;

// -- bench_keyed_nopersist / bench_keyed / the tier stores -------------------
//
// The reference's `TierStoreState` and its upsert fold: `tier_store`,
// `checkpoint_store` and `bench_checkpoint` are the same keyed upsert under
// three acknowledgement postures; only `run` differs.

#[Proc]
abstract class KeyedUpsert extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['values' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['values'][$change['key']] = $change['value'];
    }

    /** Record and emit one `key:value` input under its natural key. */
    protected function upsert(mixed $input, Ctx $ctx): void
    {
        [$key, $value] = self::split($input);
        $ctx->change(['key' => $key, 'value' => $value]);
        $ctx->emitKeyed($key, "$key=$value");
    }

    /** A `key:value` input as its two halves; the value keeps any later colon. */
    protected static function split(mixed $input): array
    {
        [$key, $value] = array_pad(explode(':', (string) $input, 2), 2, null);

        return [(string) $key, $value];
    }
}

#[Proc(name: 'tier_store_procedure')]
final class TierStore extends KeyedUpsert
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        // Dead-letter e2e: a "poison" value fails every attempt, exhausting
        // the retry budget so the tier-on atomic park advances the entity
        // past it.
        if (self::split($input)[1] === 'poison') {
            throw new \RuntimeException('poison value');
        }
        $this->upsert($input, $ctx);

        return null;
    }
}

/** `tier_store`'s twin, whose interval closes on a `commit:` input. */
#[Proc(name: 'checkpoint_store_procedure')]
final class CheckpointStore extends KeyedUpsert
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        if (self::split($input)[0] === 'commit') {
            $ctx->checkpoint();

            return null;
        }
        $this->upsert($input, $ctx);

        return null;
    }
}

/** `checkpoint_store` driven by a DECLARED cadence rather than an input. */
#[Proc(name: 'bench_checkpoint_procedure')]
final class BenchCheckpoint extends KeyedUpsert
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $this->upsert($input, $ctx);

        return null;
    }
}

/**
 * The same keyed upsert under both durability postures, so the round-trip
 * bench can isolate the cost of per-Entity persistence.
 */
abstract class KeyedUpsertOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))->onFull(Overflow::DropNothing)
            ->into(TierStore::class)->named('sink')->keyed()
            ->sink('tier_sink', Wire::bytes(...));
    }
}

/**
 * Per-entity persistence off: the heap-only posture the bench isolates against
 * the durable one.
 */
#[Operative(factoryId: 'bench_keyed_nopersist', persistent: false)]
final class BenchKeyedNoPersistOperative extends KeyedUpsertOperative
{
}

#[Operative(factoryId: 'bench_keyed')]
final class BenchKeyedOperative extends KeyedUpsertOperative
{
}

/**
 * The effectively-once reference: a keyed upsert under the tier. A 300 ms
 * backoff base keeps the min-watermark freeze observable in the DLQ e2e.
 * Declared DropNothing: a durable-input-log operative that shed at its mailbox
 * would hold the min-watermark on the shed offset forever while the log behind
 * it kept growing.
 */
#[Operative(factoryId: 'tier_store')]
final class TierStoreOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->input('input', Wire::text(...))->onFull(Overflow::DropNothing)->retryBackoffMs(300)
            ->into(TierStore::class)->named('sink')->keyed()
            ->sink('tier_sink', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'checkpoint_store')]
final class CheckpointStoreOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce()->checkpointAck();
        $c->input('input', Wire::text(...))
            ->into(CheckpointStore::class)->named('sink')->keyed()
            ->sink('tier_sink', Wire::bytes(...));
    }
}

/**
 * The same procedure declared `outputsAtCheckpoint`: the interval's outputs are
 * held with its changes and ship inside its checkpoint record.
 * `checkpointEvery` is deliberately absent so the test drives the close itself
 * — with `commit:` for the batch case, and with the delay alone for the
 * quiet-Entity case.
 */
#[Operative(factoryId: 'checkpoint_batched_store')]
final class CheckpointBatchedOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce()->checkpointAck()->outputsAtCheckpoint(1_500);
        $c->input('input', Wire::text(...))
            ->into(CheckpointStore::class)->named('sink')->keyed()
            ->sink('tier_sink', Wire::bytes(...));
    }
}

/**
 * `checkpoint_store` driven by a DECLARED cadence instead of a `commit:` input:
 * acknowledging every `FLEZHA_BENCH_CHECKPOINT_EVERY` inputs (default 500) per
 * Entity. `FLEZHA_BENCH_CHECKPOINT_OUTPUTS=at_checkpoint` (with
 * `FLEZHA_BENCH_CHECKPOINT_DELAY_MS`) additionally holds the interval's outputs
 * until it closes.
 */
#[Operative(factoryId: 'bench_checkpoint')]
final class BenchCheckpointOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce()->checkpointEvery(Wire::int(getenv('FLEZHA_BENCH_CHECKPOINT_EVERY')) ?: 500);
        if (getenv('FLEZHA_BENCH_CHECKPOINT_OUTPUTS') === 'at_checkpoint') {
            $c->outputsAtCheckpoint(Wire::int(getenv('FLEZHA_BENCH_CHECKPOINT_DELAY_MS')) ?: 50);
        }
        $c->input('input', Wire::text(...))->onFull(Overflow::DropNothing)
            ->into(BenchCheckpoint::class)->named('sink')->keyed()
            ->sink('tier_sink', Wire::bytes(...));
    }
}

// -- store_input -------------------------------------------------------------

/** The durable-state reference: every input is appended through a change. */
#[Proc(name: 'store_input_procedure')]
final class StoreInput extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['binary_inputs' => [], 'text_inputs' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['text_inputs'][] = $change['push_text'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push_text' => $input]);

        return $input;
    }
}

#[Operative(factoryId: 'store_input')]
final class StoreInputOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(StoreInput::class)->named('sink')
            ->sink('store_input_sink', Wire::bytes(...));
    }
}

// -- stateful_counter / persistence_off_counter ------------------------------

#[Proc(name: 'counter_entry_procedure')]
final class CounterEntry extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->emit(Wire::int($input));

        return null;
    }
}

#[Proc(name: 'counter_procedure')]
final class Counter extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['value' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['value'] += $change['delta'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['delta' => $input]);

        return null;
    }
}

#[Operative(factoryId: 'stateful_counter')]
final class StatefulCounterOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(CounterEntry::class)->named('ticks')
            ->into(Counter::class);
    }
}

/**
 * The same topology with per-Entity persistence off, under the reference's
 * node names: the toggle is a class property, not a different computation.
 */
#[Operative(factoryId: 'persistence_off_counter', persistent: false)]
final class PersistenceOffCounterOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(CounterEntry::class, 'persistence_off_entry_procedure')->named('ticks')
            ->into(Counter::class, 'persistence_off_counter_procedure');
    }
}

// -- coalescing_counter ------------------------------------------------------

/** Wire format: `<interval>:<ts>:<delta>`; a missing or unparsable part reads 0. */
#[Proc(name: 'tick_entry_procedure')]
final class TickEntry extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        [$interval, $ts, $delta] = array_pad(explode(':', (string) $input, 3), 3, '');

        return ['interval' => Wire::int($interval), 'ts' => Wire::int($ts), 'delta' => Wire::int($delta)];
    }
}

/**
 * The reference coalesces this change on `(interval, ts)` through a `Change`
 * trait method. The SDK carries no coalesce-key seam, so the change ships
 * uncoalesced — a throughput property, not a semantic one: the fold is
 * idempotent per `(interval, ts)` either way, so replay produces the same
 * state.
 */
#[Proc(name: 'tick_procedure')]
final class Tick extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['last' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['last'] = $change['delta'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change($input);

        return null;
    }
}

#[Operative(factoryId: 'coalescing_counter')]
final class CoalescingCounterOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(TickEntry::class)->named('ticks')
            ->into(Tick::class);
    }
}

// -- resume_probe ------------------------------------------------------------

/**
 * Emits the running total, so a resume is observable on the wire. Default +
 * change replay rebuilds the total on restore; no snapshot fast path needed.
 */
#[Proc(name: 'resume_probe_procedure')]
final class ResumeProbe extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['total' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['total'] += $change['add'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $delta = Wire::int($input);
        $total = $ctx->state['total'] + $delta;
        $ctx->change(['add' => $delta]);

        return (string) $total;
    }
}

#[Operative(factoryId: 'resume_probe')]
final class ResumeProbeOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(ResumeProbe::class)->named('total')
            ->sink('resume_probe_out', Wire::bytes(...));
    }
}

// -- schema_evolution_v1 / schema_evolution_v2 -------------------------------
//
// The same class either side of a deploy that edits its own state type. Both
// halves declare the SAME procedure name, so an operative started under v1 and
// restarted under v2 restores from the very WAL v1 wrote — which is what a
// deploy does, with the two binaries collapsed into one fixture file.
//
// v1: `{total}`. v2: `{total, currency}` at schema version 2, reading down to 1
// through a declared migration. Output is `total:currency` so the e2e can read
// the migrated field off the wire.

#[Proc(name: 'evolving_cart')]
final class EvolvingCartV1 extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['total' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['total'] += $change['add'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $delta = Wire::int($input);
        $total = $ctx->state['total'] + $delta;
        $ctx->change(['add' => $delta]);

        return "$total:";
    }
}

#[Proc(name: 'evolving_cart', schemaVersion: 2, minReadableSchemaVersion: 1)]
final class EvolvingCartV2 extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['total' => 0, 'currency' => ''];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['total'] += $change['add'];
    }

    public function migrateState(int $fromVersion, string $bytes): mixed
    {
        if ($fromVersion !== 1) {
            throw new \RuntimeException("no migration from $fromVersion");
        }
        $old = Json::decode($bytes) ?? [];

        return ['total' => $old['total'] ?? 0, 'currency' => 'EUR'];
    }

    public function migrateChange(int $fromVersion, string $bytes): mixed
    {
        if ($fromVersion !== 1) {
            throw new \RuntimeException("no migration from $fromVersion");
        }
        $old = Json::decode($bytes) ?? [];

        return ['add' => $old['add'] ?? 0, 'note' => 'migrated'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $delta = Wire::int($input);
        $total = $ctx->state['total'] + $delta;
        $currency = $ctx->state['currency'] ?: 'EUR';
        $ctx->change(['add' => $delta, 'note' => 'live']);

        return "$total:$currency";
    }
}

#[Operative(factoryId: 'schema_evolution_v1')]
final class SchemaEvolutionV1Operative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(EvolvingCartV1::class)->named('total')
            ->sink('schema_evolution_out', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'schema_evolution_v2')]
final class SchemaEvolutionV2Operative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(EvolvingCartV2::class)->named('total')
            ->sink('schema_evolution_out', Wire::bytes(...));
    }
}
