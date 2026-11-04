<?php

declare(strict_types=1);

/**
 * Starts a runner: a process that connects to the controller and serves your
 * relays.
 *
 * Declare every relay you want served before `start()`. One runner can
 * serve several, and the same call takes a steps relay.
 */

require __DIR__ . '/vendor/autoload.php';

use Flezha\Examples\Echo\EchoRelay;
use Flezha\Flezha;

// `run()` is `start()` followed by `awaitTermination()`. Splitting them leaves
// room for the calls below, and still blocks until shutdown.
$runner = Flezha::createRunner($argv[1] ?? 'default')
    ->registerFactory(EchoRelay::class) // the relays this runner serves
    ->start();                             // connects to the controller

// Everything `flz relay` and `flz plugin` do is also a method of
// `control()`, and `send()` hands a message to a relay. Uncomment to drive
// this namespace from the application instead of from a terminal:
//
// $control = $runner->control();
// $control->startRelay('echo', id: 'my_echo');
// $control->enablePlugin('writer', 'relay_file_output', ['path' => 'echo-out.txt']);
// $control->bindPlugin('writer', 'my_echo', hooks: [['on_output', new stdClass()]]);
// $runner->send('my_echo', 'hello world', 'in', msgId: 'hello-1');
//
// PHP has no threads: the message is processed once `awaitTermination()` below
// starts serving.
//
// docs/public/topics/core/sending_and_calling.md covers sending. An enforced
// controller refuses all of it until `flz role bind runner@default app default`.

$runner->awaitTermination();
