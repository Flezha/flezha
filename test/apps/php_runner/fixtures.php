<?php

declare(strict_types=1);

/**
 * Twins of the rust_runner e2e fixtures (test/apps/rust_runner/src/main.rs)
 * under the same factory ids, procedure names, channel names and output bytes.
 *
 * Entry payloads and plugin frames are UTF-8 text on the wire, the same as the
 * reference: the SDK's default codec is JSON, so every fixture that exchanges
 * bytes with another SDK or with a plugin declares its own decoder and
 * encoder. An inter-procedure hop between two PHP procedures carries JSON,
 * which no one else reads.
 */

namespace Flezha\TestApp;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Exception\NonRetryableException;
use Flezha\Graph\Builder;
use Flezha\Overflow;
use Flezha\Procedure;
use Flezha\StreamOperative;
use Flezha\Timer;

/** The wire codecs every fixture that talks to another SDK declares. */
final class Wire
{
    /** Entry bytes as the text the reference sends. */
    public static function text(string $bytes): string
    {
        return $bytes;
    }

    /** A value as the bytes the reference's plugin frames carry. */
    public static function bytes(mixed $value): string
    {
        return (string) $value;
    }

    /** The key convention every keyed fixture shares: text before the first ':'. */
    public static function prefix(mixed $input): string
    {
        return explode(':', (string) $input)[0];
    }

    /** An entry as the reference's `i64`: an unparsable input reads 0. */
    public static function int(mixed $input): int
    {
        return preg_match('/^-?\d+$/', (string) $input) === 1 ? (int) $input : 0;
    }
}

// -- do_nothing / bench_echo -------------------------------------------------

#[Proc(name: 'do_nothing_procedure')]
final class DoNothing extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Operative(factoryId: 'do_nothing')]
final class DoNothingOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(DoNothing::class)->named('echo')
            ->sink('do_nothing_echo', Wire::bytes(...));
    }
}

/**
 * Echo with a declared DropNothing on its entry mailbox. `do_nothing` is the
 * merge-blocking bench factory and must not change; this keeps the round-trip
 * bench's back-pressure posture independent of it.
 */
#[Operative(factoryId: 'bench_echo')]
final class BenchEchoOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))->onFull(Overflow::DropNothing)
            ->into(DoNothing::class)->named('echo')
            ->sink('bench_echo_sink', Wire::bytes(...));
    }
}

// -- compare_filter ----------------------------------------------------------

/**
 * State seeded by the controller's init values and then only read: there is
 * nothing to journal, so the procedure declares no change fold.
 */
#[Proc(name: 'compare_filter_procedure')]
final class CompareFilter extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['comparable' => 'default', 'inverted' => false];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $ctx->state['comparable'] === $input ? $input : null;
    }
}

#[Operative(factoryId: 'compare_filter')]
final class CompareFilterOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(CompareFilter::class)->named('sink')
            ->sink('compare_filter_sink', Wire::bytes(...));
    }
}

// -- static_entity / sugar_static_entity / mixed_operative -------------------

#[Proc(name: 'static_entry_procedure', entityKeys: ['a', 'b', 'c', 'd'])]
class StaticEntry extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['seen' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['seen'][] = $change['push'];
    }

    public function entitiesFor(mixed $input): array
    {
        $first = substr((string) $input, 0, 1);

        return [in_array($first, ['a', 'b', 'c', 'd'], true) ? $first : 'd'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push' => $input]);

        return $input;
    }
}

/** A subclass keeps every declaration its parent made, but not its node name. */
#[Proc(name: 'macro_static_entry_procedure')]
final class MacroStaticEntry extends StaticEntry
{
}

#[Proc]
class PassThrough extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Proc(name: 'static_downstream_procedure')]
final class StaticDownstream extends PassThrough
{
}

#[Proc(name: 'macro_static_downstream_procedure')]
final class MacroStaticDownstream extends PassThrough
{
}

#[Operative(factoryId: 'static_entity')]
final class StaticEntityOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(StaticEntry::class)->named('hop')
            ->into(StaticDownstream::class)->named('kafka')
            ->sink('test_kafka', Wire::bytes(...));
    }
}

/**
 * The reference pipeline sugar leaves the hop unnamed and names the plugin
 * channel after the plugin.
 */
#[Operative(factoryId: 'sugar_static_entity')]
final class SugarStaticEntityOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(MacroStaticEntry::class)
            ->into(MacroStaticDownstream::class)->named('test_kafka')
            ->sink('test_kafka', Wire::bytes(...));
    }
}

#[Proc(name: 'mixed_macro_entry_procedure')]
final class MixedMacroEntry extends PassThrough
{
}

#[Proc(name: 'mixed_hand_downstream')]
final class MixedHandDownstream extends PassThrough
{
}

#[Operative(factoryId: 'mixed_operative')]
final class MixedOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(MixedMacroEntry::class)->named('hop')
            ->into(MixedHandDownstream::class)->named('kafka')
            ->sink('test_kafka', Wire::bytes(...));
    }
}

// -- sugar_do_nothing / sugar_store_input ------------------------------------

#[Proc(name: 'sugar_do_nothing')]
final class SugarDoNothing extends PassThrough
{
}

#[Operative(factoryId: 'sugar_do_nothing')]
final class SugarDoNothingOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(SugarDoNothing::class)->named('sugar_do_nothing_echo')
            ->sink('sugar_do_nothing_echo', Wire::bytes(...));
    }
}

#[Proc(name: 'sugar_store_input')]
final class SugarStoreInput extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['text_inputs' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['text_inputs'][] = $change['push'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push' => $input]);

        return $input;
    }
}

#[Operative(factoryId: 'sugar_store_input')]
final class SugarStoreInputOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(SugarStoreInput::class)->named('sugar_store_sink')
            ->sink('sugar_store_sink', Wire::bytes(...));
    }
}

// -- hybrid_channel ----------------------------------------------------------
//
// One emit reaching two destinations: an inter-procedure consumer and a plugin
// wire with its own serializer. The serializer refuses a sentinel value, so an
// operator can observe end to end that a refusal is isolated to the emit rather
// than failing the item.

#[Proc(name: 'hybrid_producer_procedure')]
final class HybridProducer extends Procedure
{
    public const SENTINEL = 0xDEADBEEF;

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $parsed = filter_var((string) $input, FILTER_VALIDATE_INT);
        $value = is_int($parsed) && $parsed >= 0 && $parsed <= 0xFFFFFFFF ? $parsed : 0;

        try {
            $ctx->emit($value);
        } catch (\Throwable $e) {
            fwrite(STDERR, "hybrid_producer_procedure: serializer refused an emit: {$e->getMessage()}\n");
        }

        return null;
    }
}

#[Proc(name: 'hybrid_consumer_procedure')]
final class HybridConsumer extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        echo "hybrid_consumer_procedure: received $input\n";

        return null;
    }
}

#[Operative(factoryId: 'hybrid_channel')]
final class HybridChannelOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))->into(HybridProducer::class)->branch(
            // The consumer edge first, so a refused plugin serialization leaves
            // the in-process fan-out already recorded — the reference's isolation.
            static fn ($edge) => $edge->named('hybrid_indicator')->into(HybridConsumer::class),
            static fn ($edge) => $edge->sink('hybrid_indicators', static function (mixed $value): string {
                if ($value === HybridProducer::SENTINEL) {
                    throw new \RuntimeException('hybrid_indicators: serializer rejected sentinel 0xDEADBEEF');
                }

                return pack('N', $value);
            }),
        );
    }
}

// -- entity_stress -----------------------------------------------------------

#[Proc(name: 'stress_a', entityKeys: ['p0', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])]
final class StressA extends Procedure
{
    public function entitiesFor(mixed $input): array
    {
        return ['p0', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Proc(entityKeys: ['p0', 'p1', 'p2', 'p3', 'p4', 'p5', 'p6', 'p7'])]
class StressToP0 extends Procedure
{
    public function entitiesFor(mixed $input): array
    {
        return ['p0'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Proc(name: 'stress_b')]
final class StressB extends StressToP0
{
}

#[Proc(name: 'stress_c')]
final class StressC extends StressToP0
{
}

#[Proc(name: 'stress_d')]
final class StressD extends StressToP0
{
}

#[Operative(factoryId: 'entity_stress')]
final class EntityStressOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(StressA::class)->named('a_to_b')
            ->into(StressB::class)->named('b_to_c')
            ->into(StressC::class)->named('c_to_d')
            ->into(StressD::class)->named('echo')
            ->sink('entity_stress_echo', Wire::bytes(...));
    }
}

// -- poison_probe / wedge_probe ----------------------------------------------

#[Proc(name: 'poison_probe_procedure')]
final class PoisonProbe extends Procedure
{
    private static bool $antidoteTaken = false;

    public function run(mixed $input, Ctx $ctx): mixed
    {
        if ($input === 'antidote') {
            self::$antidoteTaken = true;
        }
        if ($input === 'poison' && !self::$antidoteTaken) {
            throw new \RuntimeException('poison payload');
        }

        return "ok:$input";
    }
}

#[Operative(factoryId: 'poison_probe')]
final class PoisonProbeOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))->retryBackoffMs(25)
            ->into(PoisonProbe::class)->named('out')
            ->sink('probe_sink', Wire::bytes(...));
    }
}

/**
 * The `pool_wedge` fixture: the two ways a runner stops doing work, on one
 * operative.
 *
 *   "wedge" — blocks the dispatch inside user code forever, so the SDK's
 *     dispatch counter stops advancing while the heartbeat keeps reporting the
 *     runner alive: connected, heartbeating, processing nothing.
 *   "abort" — ends its activation as violently as this host allows, WITHOUT
 *     emitting, once per process. PHP has no way for user code to kill a
 *     worker outright — the dispatcher catches per item and the throw rides
 *     the retry ladder — so what the scenario asserts is that the entity is
 *     served again and the aborting item does not produce twice.
 */
#[Proc(name: 'wedge_probe_procedure')]
final class WedgeProbe extends Procedure
{
    private static bool $aborted = false;

    public function run(mixed $input, Ctx $ctx): mixed
    {
        if ($input === 'wedge') {
            while (true) {
                sleep(3600);
            }
        }
        if ($input === 'abort' && !self::$aborted) {
            self::$aborted = true;

            throw new \RuntimeException('wedge_probe: killing the worker holding this activation');
        }

        return "ok:$input";
    }
}

#[Operative(factoryId: 'wedge_probe')]
final class WedgeProbeOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(WedgeProbe::class)->named('out')
            ->sink('wedge_sink', Wire::bytes(...));
    }
}

// -- session_timeout ---------------------------------------------------------
//
// The worked session-timeout shape: each input pushes the idle deadline out,
// and the firing closes the session. Single-entity so the e2e's crash-resume
// half exercises a durable Entity. The timeout is read from
// `TIMER_TEST_TIMEOUT_MS` (the e2e sets it short).

#[Proc(name: 'session_timeout_procedure')]
final class SessionTimeout extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['touched' => [], 'closed_at' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        if (array_key_exists('touched', $change)) {
            $state['touched'][] = $change['touched'];
        } else {
            $state['closed_at'][] = $change['closed'];
        }
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['touched' => $input]);
        // A processing-time timer, because the answer to "nothing has arrived"
        // cannot be an event-time one: nothing would advance the clock.
        $ctx->timerAfter(Wire::int(getenv('TIMER_TEST_TIMEOUT_MS')) ?: 1000);

        return null;
    }

    public function onTimer(Timer $timer, Ctx $ctx): void
    {
        $ctx->change(['closed' => $timer->scheduledAt]);
        $ctx->emit("closed@{$timer->scheduledAt}");
    }
}

#[Operative(factoryId: 'session_timeout')]
final class SessionTimeoutOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))
            ->into(SessionTimeout::class)->named('kafka')
            ->sink('test_kafka', Wire::bytes(...));
    }
}

// -- bounded_mailbox_{drop_nothing,drop_newest,drop_oldest} ------------------

/** The capacity and the fold reach each subclass by inheritance. */
#[Proc(mailboxCapacity: 4)]
class BoundedMailbox extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['processed' => 0];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['processed']++;
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['processed' => true]);

        return $input;
    }
}

#[Proc(name: 'drop_nothing_test_procedure')]
final class DropNothingMailbox extends BoundedMailbox
{
}

#[Proc(name: 'drop_newest_test_procedure')]
final class DropNewestMailbox extends BoundedMailbox
{
}

#[Proc(name: 'drop_oldest_test_procedure')]
final class DropOldestMailbox extends BoundedMailbox
{
}

#[Operative(factoryId: 'bounded_mailbox_drop_nothing')]
final class BoundedMailboxDropNothingOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('drop_nothing_input', Wire::text(...))->onFull(Overflow::DropNothing)
            ->into(DropNothingMailbox::class)->named('drop_nothing_echo')
            ->sink('bounded_mailbox_echo', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'bounded_mailbox_drop_newest')]
final class BoundedMailboxDropNewestOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('drop_newest_input', Wire::text(...))->onFull(Overflow::DropNewest)
            ->into(DropNewestMailbox::class)->named('drop_newest_echo')
            ->sink('bounded_mailbox_echo', Wire::bytes(...));
    }
}

#[Operative(factoryId: 'bounded_mailbox_drop_oldest')]
final class BoundedMailboxDropOldestOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('drop_oldest_input', Wire::text(...))->onFull(Overflow::DropOldest)
            ->into(DropOldestMailbox::class)->named('drop_oldest_echo')
            ->sink('bounded_mailbox_echo', Wire::bytes(...));
    }
}

// -- partition_relay ---------------------------------------------------------
//
// The partitioned-operatives upstream: keyed on the wiring so it can be
// partitioned at all, and emitting keyed so a partitioned forward target can
// resolve the owning member. The plugin channel is named `input` because a
// forwarded frame keeps its producing channel's name, and that is what selects
// the receiving operative's entry channel — so the entry here is `in`.

#[Proc(name: 'partition_relay_procedure', dynamicEntities: true)]
final class PartitionRelay extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['seen' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['seen'][] = $change['push'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push' => $input]);
        $ctx->emitKeyed(Wire::prefix($input), $input);

        return null;
    }
}

#[Operative(factoryId: 'partition_relay')]
final class PartitionRelayOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', Wire::text(...))->keyBy(Wire::prefix(...))
            ->into(PartitionRelay::class)->named('input')->keyed()
            ->sink('relay_out', Wire::bytes(...));
    }
}

// -- dynamic_entity ----------------------------------------------------------

// Rust reads the eviction TTL from `DYN_TEST_TTL_MS`; an attribute argument
// must be a constant expression, so this fixture declares none. Only the
// reference-runtime scenario arms a TTL, so nothing here reads one.
#[Proc(name: 'dynamic_entity_procedure', dynamicEntities: true)]
final class DynamicEntity extends Procedure
{
    public function state(?string $entityKey): mixed
    {
        return ['seen' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['seen'][] = $change['push'];
    }

    public function run(mixed $input, Ctx $ctx): mixed
    {
        $ctx->change(['push' => $input]);

        return $input;
    }
}

#[Operative(factoryId: 'dynamic_entity')]
final class DynamicEntityOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input', Wire::text(...))->onFull(Overflow::DropNothing)
            // Filtered before keying: a dropped value answers no key, so the
            // core never mints an entity for it.
            ->filter(static fn (mixed $input): bool => $input !== 'drop')
            ->keyBy(Wire::prefix(...))
            ->into(DynamicEntity::class)->named('kafka')
            ->sink('test_kafka', Wire::bytes(...));
    }
}

/** Every operative this app registers, in declaration order. */
final class Fixtures
{
    /** @return list<class-string<\Flezha\Operative>> */
    public static function all(): array
    {
        return [
            DoNothingOperative::class,
            BenchEchoOperative::class,
            CompareFilterOperative::class,
            StaticEntityOperative::class,
            SugarStaticEntityOperative::class,
            MixedOperative::class,
            SugarDoNothingOperative::class,
            SugarStoreInputOperative::class,
            HybridChannelOperative::class,
            EntityStressOperative::class,
            BenchKeyedNoPersistOperative::class,
            StoreInputOperative::class,
            StatefulCounterOperative::class,
            PersistenceOffCounterOperative::class,
            CoalescingCounterOperative::class,
            ResumeProbeOperative::class,
            BenchKeyedOperative::class,
            SchemaEvolutionV1Operative::class,
            SchemaEvolutionV2Operative::class,
            TierStoreOperative::class,
            CheckpointStoreOperative::class,
            CheckpointBatchedOperative::class,
            BenchCheckpointOperative::class,
            PoisonProbeOperative::class,
            WedgeProbeOperative::class,
            SessionTimeoutOperative::class,
            BoundedMailboxDropNothingOperative::class,
            BoundedMailboxDropNewestOperative::class,
            BoundedMailboxDropOldestOperative::class,
            PartitionRelayOperative::class,
            DynamicEntityOperative::class,
        ];
    }
}
