<?php

declare(strict_types=1);

namespace Flezha\Tests;

use Flezha\Exception\FlezhaException;
use Flezha\Ffi\Binding;
use Flezha\Ffi\Core;
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

    public function testAnEntryChannelNoOperativeHasNeverReceivedAFrame(): void
    {
        $core = Core::create();

        try {
            self::assertSame(0, $core->entryLastDeliveryNanos('no_such_channel'));
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
