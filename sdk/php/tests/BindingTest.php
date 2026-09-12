<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Binding;
use Flezha\Ffi\Core;
use Flezha\Graph\Builder;
use Flezha\Graph\HostClassDecl;
use Flezha\Procedure;
use Flezha\StreamOperative;
use PHPUnit\Framework\TestCase;

/** The FFI binding, against a live core. */
final class BindingTest extends TestCase
{
    public function testTheLoadedCoreSpeaksTheSchemaThisPackageWasBuiltAgainst(): void
    {
        $core = Core::create();

        try {
            self::assertSame(Core::SCHEMA_VERSION, $core->coreSchemaVersion());
        } finally {
            $core->free();
        }
    }

    /**
     * Negotiated at context creation, so a shim paired with a mismatched core
     * is refused before any controller connection is attempted.
     */
    public function testAMismatchedSchemaIsRefusedNamingBothVersions(): void
    {
        $ffi = Binding::current();

        // A refused creation answers the null address, which is why the shim
        // carries the context as an integer at all.
        self::assertSame(0, (int) $ffi->flz_core_context_new(Core::SCHEMA_VERSION + 99));
        self::assertStringContainsString(
            'activation schema version mismatch',
            (string) $ffi->flz_core_init_error(),
        );
    }

    /**
     * The address is what crosses a thread boundary: `Swoole\Thread` takes
     * scalars, never objects, so a worker attaches by integer and re-casts.
     */
    public function testAContextCrossesAsAnIntegerAndAttachesBack(): void
    {
        $core = Core::create();

        try {
            $attached = Core::attach($core->address());

            self::assertNotSame(0, $core->address());
            self::assertSame($core->address(), $attached->address());
            self::assertTrue($core->ownsContext());
            self::assertFalse($attached->ownsContext());

            // A call that DEREFERENCES the context, not one that ignores it.
            // `schema_version` takes no context, so it passes over a handle
            // whose pointer is garbage — which is exactly the shape the first
            // implementation had, and it panicked inside the core's DashMap
            // the first time a worker thread polled.
            try {
                $attached->declareClass('{"persistent":true}');
                self::fail('the core accepted a declaration with no factory id');
            } catch (FlezhaException $e) {
                self::assertStringContainsString('factory_id', $e->getMessage());
            }

            // The asymmetry that makes the ownership rule structural rather
            // than a caller's discipline: freeing an attached handle is a
            // no-op, so a worker cannot double-free what it borrowed.
            $attached->free();
            self::assertSame(Core::SCHEMA_VERSION, $core->coreSchemaVersion());
        } finally {
            $core->free();
        }
    }

    public function testACallOnAFreedContextIsRefusedRatherThanSegfaulting(): void
    {
        $core = Core::create();
        $core->free();

        $this->expectException(FlezhaException::class);
        $this->expectExceptionMessage('has been freed');

        $core->declareClass('{}');
    }

    public function testACoreRefusalCarriesTheReasonTheCoreRecorded(): void
    {
        $core = Core::create();

        try {
            $this->expectException(FlezhaException::class);
            $this->expectExceptionMessage('factory_id');

            $core->declareClass('{"persistent":true}');
        } finally {
            $core->free();
        }
    }

    /**
     * The checkpoint options the core will accept together.
     *
     * Asserted against the core rather than against the builder's fields
     * because the core is what refuses: a declaration holding outputs at a
     * checkpoint it never acknowledges at is rejected at registration, so a
     * builder that can express it hands its author a class that cannot start.
     */
    public function testHoldingOutputsAtTheCheckpointAcknowledgesAtTheCheckpoint(): void
    {
        $decl = HostClassDecl::of(CheckpointOutputsOnly::spec());
        // The raw flag, and only it: the builder never asks for the
        // granularity, and the class registers because the core implies it
        // from this key.
        self::assertFalse($decl['checkpoint_ack']);
        self::assertSame('at_checkpoint', $decl['checkpoint_outputs']);

        $core = Core::create();

        try {
            $json = (string) json_encode($decl);
            $core->declareClass($json);
            self::assertSame('checkpoint', self::granularity($core, $json));
        } finally {
            $core->free();
        }
    }

    public function testTheBareCheckpointOptInRegistersWithNoCadence(): void
    {
        $decl = HostClassDecl::of(CheckpointAckOnly::spec());
        self::assertTrue($decl['checkpoint_ack']);
        self::assertArrayNotHasKey('checkpoint_every', $decl);

        $core = Core::create();

        try {
            $json = (string) json_encode($decl);
            $core->declareClass($json);
            self::assertSame('checkpoint', self::granularity($core, $json));
        } finally {
            $core->free();
        }
    }

    /**
     * The read-only half of the declaration: a record the core cannot honour
     * is refused with the vocabulary every SDK renders, and nothing is
     * registered.
     */
    public function testARefusedRecordAnswersTheVerdictAndRegistersNothing(): void
    {
        $core = Core::create();
        $verdict = null;

        try {
            $core->checkClass('{"factory_id":"x","channels":[],"procedures":[]}', $verdict);
            self::fail('the core refuses a record it cannot honour');
        } catch (FlezhaException $e) {
            self::assertStringContainsString('topology rejected [', $e->getMessage());
            // The same verdict as DATA: the ABI fills the out-buffer on the
            // refusal status too, so this veneer takes it — and frees it —
            // there as well, and a reader never parses the rendered prose.
            /** @var array{issues: list<array{name: string, text: string}>} $decoded */
            $decoded = json_decode((string) $verdict, true, 512, JSON_THROW_ON_ERROR);
            self::assertNotSame([], $decoded['issues']);
            self::assertStringContainsString($decoded['issues'][0]['name'], $e->getMessage());
        } finally {
            $core->free();
        }
    }

    /** The acknowledgement granularity the core derived from a record. */
    private static function granularity(Core $core, string $json): string
    {
        /** @var array{ack_granularity: string} $derived */
        $derived = json_decode($core->checkClass($json), true, 512, JSON_THROW_ON_ERROR);

        return $derived['ack_granularity'];
    }

    public function testTheMonotonicClockAdvances(): void
    {
        $core = Core::create();

        try {
            $first = $core->monotonicNanos();
            // The clock under test is the signal; PHP's own bounds the wait,
            // so a stopped core clock fails the assertion rather than hanging
            // the suite.
            $startedAt = hrtime(true);
            while ($core->monotonicNanos() <= $first && hrtime(true) - $startedAt < 1_000_000_000) {
                // spin
            }

            self::assertGreaterThan($first, $core->monotonicNanos());
        } finally {
            $core->free();
        }
    }

    public function testThePlatformIsOneTheDistributionCarries(): void
    {
        self::assertMatchesRegularExpression('/^(linux|macos|windows)-(x64|arm64)$/', Binding::platform());
        self::assertFileExists(Binding::libraryPath());
    }

    public function testAnAvailabilityCheckPassesOnABuildThatCanCall(): void
    {
        Binding::assertAvailable();

        self::assertTrue(class_exists(\FFI::class));
    }
}

#[Proc(name: 'checkpoint_procedure')]
final class CheckpointProcedure extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Operative(factoryId: 'checkpoint_outputs_only')]
final class CheckpointOutputsOnly extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->outputsAtCheckpoint(1500);
        $c->input('input')->into(CheckpointProcedure::class)->named('sink')->keyed()->sink('tier_sink');
    }
}

#[Operative(factoryId: 'checkpoint_ack_only')]
final class CheckpointAckOnly extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->effectivelyOnce();
        $c->checkpointAck();
        $c->input('input')->into(CheckpointProcedure::class)->named('sink')->keyed()->sink('tier_sink');
    }
}
