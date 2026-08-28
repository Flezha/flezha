<?php

declare(strict_types=1);

namespace Flezha;

/**
 * What a channel does when its consumer's mailbox is full. `DropNothing` is
 * the default in every SDK: nothing is discarded without the author saying so.
 */
enum Overflow: string
{
    case DropNothing = 'drop_nothing';
    case DropNewest = 'drop_newest';
    case DropOldest = 'drop_oldest';
}
