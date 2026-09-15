<?php

declare(strict_types=1);

namespace Flezha\Examples\Echo;

use Flezha\Attribute\Operative;
use Flezha\ChannelCodec;
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
 * The entry names the text codec, so what `flz operative send --text` sends
 * arrives as that string. The sink names nothing: a string is sent as it
 * stands, so the output is the text itself, and any other value would be JSON.
 */
#[Operative(factoryId: 'echo')]
final class EchoOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('in', codec: ChannelCodec::Text)
            ->into(EchoProcedure::class)
            ->sink('out');
    }
}
