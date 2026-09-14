<?php

declare(strict_types=1);

/**
 * Starts a runner: a process that connects to the controller and serves your
 * operatives.
 *
 * Declare every operative you want served before `start()`. One runner can
 * serve several, and `defineWorkflow()` is the same call for the workflow kind.
 */

require __DIR__ . '/vendor/autoload.php';

use Flezha\Examples\Echo\EchoOperative;
use Flezha\Flezha;

// `run()` is `start()` followed by `awaitTermination()`. Splitting them leaves
// room for the admin calls below, and still blocks until shutdown.
$runner = Flezha::createRunner($argv[1] ?? 'default')
    ->defineStream(EchoOperative::class)
    ->start();

// Everything `flz operative` and `flz plugin` do is also a method here.
// Uncomment to drive this namespace from the application instead of from a
// terminal:
//
// $admin = $runner->admin();
// $admin->startOperative('my_echo', 'echo');
// $admin->enablePlugin('writer', [
//     'id' => 'writer',
//     'provider' => 'operative_file_sink',
//     'operative_id' => 'my_echo',
//     'opts' => ['path' => 'echo-out.txt'],
//     'hooks' => [['on_output', new stdClass()]],
// ]);
// $admin->send('my_echo', 'hello world', 'in');
//
// PHP has no threads: the message is processed once `awaitTermination()` below
// starts serving.
//
// docs/sdk/admin.md has the whole surface. An enforced controller refuses all
// of it until `flz role bind runner@default app default`.

$runner->awaitTermination();
