<?php

declare(strict_types=1);

/**
 * The probe family — `poison_probe` and `wedge_probe` — the same partition as
 * the reference's probe.rs. Loaded after fixtures.php, whose `Wire` codecs
 * these share.
 */

namespace Flezha\TestApp;

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\ChannelCodec;
use Flezha\Ctx;
use Flezha\Graph\Builder;
use Flezha\Procedure;
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
        $c->input('input', codec: ChannelCodec::Text)->retryBackoffMs(25)
            ->into(PoisonProbe::class)
            ->sink('out');
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
        $c->input('input', codec: ChannelCodec::Text)
            ->into(WedgeProbe::class)
            ->sink('out');
    }
}
