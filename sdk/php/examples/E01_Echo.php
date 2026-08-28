<?php

declare(strict_types=1);

/**
 * The smallest operative: one entry channel, one stateless procedure, one
 * plugin sink. What a handler returns is emitted, so a pass-through is a
 * `return`.
 *
 *     php examples/E01_Echo.php default
 */

namespace Flezha\Examples\E01;

require_once __DIR__ . '/../vendor/autoload.php';

use Flezha\Attribute\Operative;
use Flezha\Attribute\Proc;
use Flezha\Ctx;
use Flezha\Flezha;
use Flezha\Graph\Builder;
use Flezha\Procedure;
use Flezha\StreamOperative;

#[Proc]
final class Echoer extends Procedure
{
    public function run(mixed $input, Ctx $ctx): mixed
    {
        return $input;
    }
}

#[Operative(factoryId: 'echo')]
final class EchoOperative extends StreamOperative
{
    public static function channels(Builder $c): void
    {
        $c->input('input')->into(Echoer::class)->named('out')->sink('echo_sink');
    }
}

if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    Flezha::createRunner($argv[1] ?? 'default')->defineStream(EchoOperative::class)->run();
}
