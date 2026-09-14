<?php

declare(strict_types=1);

namespace Flezha\Examples\Echo;

use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Procedure;

/**
 * A processing stage of the operative's graph, run once per message. This one
 * prefixes the input, and what `run` returns is emitted.
 *
 * `name` is the procedure's stable identity. State is filed under it, so
 * renaming it later declares a different procedure rather than renaming this
 * one. With no `state` and no `applyChange` overridden, nothing is kept between
 * messages and nothing is journaled.
 *
 * Where the result goes is not decided here. The operative decides that, so the
 * graph can be rewired without touching this class.
 */
#[Proc(name: 'echo')]
final class EchoProcedure extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return 'echo: ' . $input;
    }
}
