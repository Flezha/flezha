<?php

declare(strict_types=1);

/**
 * The probe family — `poison_probe`, `wedge_probe` and `entry_idle_probe` —
 * the same partition as the reference's probe.rs. Loaded after fixtures.php,
 * whose `Wire` codecs these share.
 */

namespace Flezha\TestApp;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\IdleObserver;
use Flezha\Procedure;
use Flezha\Runner;
use Flezha\StreamOperative;

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

// -- entry_idle_probe --------------------------------------------------------
//
// An IdleObserver has no ctx and cannot emit, so the observer counts episodes
// and a procedure reports them as `<absent_at_boot>:<episodes>:<resumes>`. Two
// entry channels fan into ONE procedure: the core interns one observation per
// channel NAME, so a frame on `idle_report` leaves `idle_input`'s stamp
// untouched and the e2e can poll without ending the silence it measures.
//
// Under SwooleExecutor the observer runs on the thread that started the
// runner and the procedure in a worker interpreter that shares no memory with
// it — a static there is one value per interpreter. The counters therefore
// live in a file keyed by this process's id: written whole by the observer's
// thread, read whole by the procedure; a torn read fails the scenario's parse,
// which polls again.

final class EntryIdleProbeCounters
{
    public const WATCHED = 'idle_input';
    private const THRESHOLD_MS = 1_000;

    private static string $absentAtBoot = 'false';
    private static int $episodes = 0;
    private static int $resumes = 0;

    /**
     * Read the accessor before `run()`, then watch the channel. The stamp is
     * taken ahead of anything that can decline a frame, so "absent before the
     * first frame" is observable here and nowhere inside a procedure. The file
     * is seeded only when absent, so a worker interpreter re-running this
     * script cannot clobber the counts the starting thread keeps.
     */
    public static function arm(Runner $runner): void
    {
        self::$absentAtBoot = $runner->lastEntryDelivery(self::WATCHED) === null ? 'true' : 'false';
        if (!file_exists(self::path())) {
            self::publish();
            register_shutdown_function(static fn (): bool => @unlink(self::path()));
        }
        $runner->onEntryIdle(self::WATCHED, self::THRESHOLD_MS, new EntryIdleCounter());
    }

    public static function episode(): void
    {
        self::$episodes++;
        self::publish();
    }

    public static function resume(): void
    {
        self::$resumes++;
        self::publish();
    }

    public static function report(): string
    {
        return (string) @file_get_contents(self::path());
    }

    private static function publish(): void
    {
        file_put_contents(
            self::path(),
            self::$absentAtBoot . ':' . self::$episodes . ':' . self::$resumes,
            LOCK_EX,
        );
    }

    private static function path(): string
    {
        return sys_get_temp_dir() . '/flezha_entry_idle_probe_' . getmypid();
    }
}

final class EntryIdleCounter implements IdleObserver
{
    public function idle(float $gapMs): void
    {
        EntryIdleProbeCounters::episode();
    }

    public function resumed(float $gapMs): void
    {
        EntryIdleProbeCounters::resume();
    }
}

#[Proc(name: 'entry_idle_probe_procedure')]
final class EntryIdleProbe extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return EntryIdleProbeCounters::report();
    }
}

#[Operative(factoryId: 'entry_idle_probe')]
final class EntryIdleProbeOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        // Both channels wire the same class: one node, fan-in.
        $c->input(EntryIdleProbeCounters::WATCHED, Wire::text(...))->into(EntryIdleProbe::class);
        $c->input('idle_report', Wire::text(...))->into(EntryIdleProbe::class)->named('report')
            ->sink('entry_idle_report', Wire::bytes(...));
    }
}
