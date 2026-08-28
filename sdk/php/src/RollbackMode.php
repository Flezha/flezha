<?php

declare(strict_types=1);

namespace Flezha;

/** What an aborted workflow chain does with the stages that committed. */
enum RollbackMode: string
{
    case AllOrNothing = 'all_or_nothing';
    case KeepProgress = 'keep_progress';
}
