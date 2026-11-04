<?php

declare(strict_types=1);

namespace Flezha\Examples\Echo;

use Flezha\Attribute\Relay;
use Flezha\ChannelBuilder;
use Flezha\ChannelCodec;

/**
 * The deployable unit. A relay declares the graph a message travels: the
 * entry channel it arrives on, the steps it passes through, and the channel its
 * result leaves on.
 *
 * `factoryId` is the relay's stable identity, and the name
 * `flz relay start` takes. The graph is checked as a whole when the runner
 * declares it, so one that cannot work is refused at startup.
 *
 * The entry names the text codec, so what `flz relay send --text` sends
 * arrives as that string. The output names nothing: a string is sent as it
 * stands, so the output is the text itself, and any other value would be JSON.
 */
#[Relay(factoryId: 'echo')] // the name `flz relay start` takes
final class EchoRelay extends \Flezha\Relay
{
    public static function channels(ChannelBuilder $c): void
    {
        $c->input('in', codec: ChannelCodec::Text) // messages arrive on the entry channel "in"
            ->into(EchoTasklet::class)             // each one runs through the echo tasklet
            ->output('out');                       // the result leaves on "out", for a plugin to pick up
    }
}
