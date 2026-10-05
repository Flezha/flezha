# The php SDK's echo example

Every file of `sdk/php/examples/echo`, the runner the echo tutorial runs. Its dependency file names the SDK package and the version this release pins.

## examples/echo/README.md

````markdown
# Echo: the PHP hello world

The PHP hello world, as a standalone project. Nothing here depends on the Flezha
source tree: `composer.json` names one released version of `flezha/sdk`, the
way your own project would. It needs PHP 8.4 or later with `ext-ffi`.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `src/EchoTasklet.php` | the tasklet | the processing stage: prefixes each message |
| `src/EchoRelay.php` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.php` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
composer install
php runner.php
```

`composer install` also downloads the native library the SDK loads, for your
platform, from the release matching that version. The `allow-plugins` entry in
`composer.json` is what lets it.

Then, from another terminal:

```sh
flz relay start echo --id my_echo
flz relay send my_echo --text 'hello world' --await 2000
```

```
Delivered to 'my_echo'.
out	echo: hello world
```

It keeps running until a SIGTERM, Ctrl-C, or its standard input closing, so do
not start it with standard input closed. The namespace is the first argument
and defaults to `default`: `php runner.php my_app`.

## Building from a checkout

The package is not on Packagist yet. From the repository root, stage the
native library the package loads:

```sh
scripts/build-php-sdk.sh --release
```

It needs a Rust toolchain. Then tell Composer where the package is, once, naming
your checkout's absolute path:

```sh
composer config --global repositories.flezha-sdk \
  '{"type": "path", "url": "/path/to/flezha/sdk/php", "options": {"versions": {"flezha/sdk": "0.1.0"}}}'
```

After that, `composer install` in this folder links the checkout's package. The
entry lives in Composer's global configuration rather than in `composer.json`,
so this folder stays as it would be once the package is published; remove the
entry then.

## Where to go next

[`docs/guides/runner_deployment.md`](../../../../docs/guides/runner_deployment.md)
walks through what each of the three parts is and why the split is the way it
is. [`docs/sdk/php_sdk.md`](../../../../docs/sdk/php_sdk.md) covers the rest of the
SDK, including the threaded executor.
````

## examples/echo/composer.json

````json
{
    "name": "flezha/echo-example",
    "description": "One message in, one message out: a tasklet, a relay and a runner.",
    "type": "project",
    "license": "Apache-2.0",
    "require": {
        "php": ">=8.4",
        "flezha/sdk": "0.1.0"
    },
    "autoload": {
        "psr-4": {
            "Flezha\\Examples\\Echo\\": "src/"
        }
    },
    "config": {
        "allow-plugins": {
            "flezha/sdk": true
        }
    }
}
````

## examples/echo/runner.php

````php
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
// docs/topics/advanced/sending_from_runner_code.md covers sending. An enforced
// controller refuses all of it until `flz role bind runner@default app default`.

$runner->awaitTermination();
````

## examples/echo/src/EchoRelay.php

````php
<?php

declare(strict_types=1);

namespace Flezha\Examples\Echo;

use Flezha\Attribute\Relay;
use Flezha\ChannelCodec;
use Flezha\Graph\Builder;

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
    public static function channels(Builder $c): void
    {
        $c->input('in', codec: ChannelCodec::Text) // messages arrive on the entry channel "in"
            ->into(EchoTasklet::class)             // each one runs through the echo tasklet
            ->output('out');                       // the result leaves on "out", for a plugin to pick up
    }
}
````

## examples/echo/src/EchoTasklet.php

````php
<?php

declare(strict_types=1);

namespace Flezha\Examples\Echo;

use Flezha\Attribute\Task;
use Flezha\Ctx;
use Flezha\Tasklet;

/**
 * A processing stage of the relay's graph, run once per message. This one
 * prefixes the input and emits it through the context: `run` returns nothing.
 *
 * `name` is the tasklet's stable identity, and it is required. State is filed
 * under it, so renaming the class keeps it, and renaming `name` declares a
 * different tasklet. With no `initState` and no `applyChange` overridden,
 * nothing is kept between messages and nothing is journaled.
 *
 * Where the result goes is not decided here. The relay decides that, so the
 * graph can be rewired without touching this class.
 */
#[Task(name: 'echo')] // state is saved under this name, so choose it once
final class EchoTasklet extends Tasklet
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit('echo: ' . $input); // where it goes is the relay's business
    }
}
````
