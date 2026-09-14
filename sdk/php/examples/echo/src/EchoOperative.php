<?php

declare(strict_types=1);

namespace Flezha\Examples\Echo;

use Flezha\Attribute\Operative;
use Flezha\Graph\Builder;
use Flezha\StreamOperative;

/**
 * The deployable unit. An operative declares the graph a message travels: the
 * entry channel it arrives on, the steps it passes through, and the channel its
 * result leaves on.
 *
 * `factoryId` is the operative's stable identity, and the name
 * `flz operative start` takes. The graph is checked as a whole when the runner
 * declares it, so one that cannot work is refused at startup.
 *
 * Values cross the wire as JSON unless a channel says otherwise. This one
 * carries plain text both ways, so `flz operative send --text` reads naturally
 * and the output is the text itself.
 */
#[Operative(factoryId: 'echo')]
final class EchoOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', static fn (string $bytes): string => $bytes)
            ->into(EchoProcedure::class)
            ->sink('out', static fn (mixed $value): string => (string) $value);
    }
}
