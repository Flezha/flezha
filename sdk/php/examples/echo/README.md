# Echo: the PHP hello world

The PHP hello world, as a standalone project. Nothing here depends on the Flezha
source tree: `composer.json` names one released version of `flezha/sdk`, the
way your own project would. It needs PHP 8.4 or later with `ext-ffi`.

Three files, one for each part the guides describe:

| File | Part | What it does |
|---|---|---|
| `src/EchoOperative.php` | the operative | the graph: `in` feeds the procedure, output lands on `out` |
| `src/EchoProcedure.php` | the procedure | the processing stage: prefixes each message |
| `runner.php` | the runner | this process: offers the operative and waits |

## Run it

You need a controller running and reachable first
([`docs/guides/controller_deployment.md`](../../../../docs/guides/controller_deployment.md)).
From this folder:

```sh
composer install
php runner.php
```

Then, from another terminal:

```sh
flz operative start my_echo echo
flz operative send my_echo --text 'hello world' --await 2000
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
