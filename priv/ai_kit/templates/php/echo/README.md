# Echo: the PHP hello world

The PHP hello world, as a standalone project. Nothing here depends on the Flezha
source tree: `composer.json` names one released version of `flezha/sdk`, the
way your own project would. It needs PHP 8.4 or later with `ext-ffi`.

Three files, one for each of the [building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md):

| File | Part | What it does |
|---|---|---|
| `src/EchoTasklet.php` | the tasklet | the processing stage: prefixes each message |
| `src/EchoRelay.php` | the relay | the graph: `in` feeds the tasklet, output lands on `out` |
| `runner.php` | the runner | this process: registers the echo factory and waits |

## Run it

You need a controller running and reachable first
([Up and running](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md)).
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

To run the example against a Flezha checkout instead of the published package,
stage the native library the package loads, from the repository root:

```sh
scripts/build-php-sdk.sh --release
```

It needs a Rust toolchain. Then tell Composer where the package is, once, naming
your checkout's absolute path:

```sh
composer config --global repositories.flezha-sdk \
  '{"type": "path", "url": "/path/to/flezha/sdk/php", "options": {"versions": {"flezha/sdk": "0.1.1"}}}'
```

After that, `composer install` in this folder links the checkout's package. The
entry lives in Composer's global configuration rather than in `composer.json`,
so this folder stays as it is; remove the entry to use the published package
again.

## Where to go next

[Building blocks](https://github.com/flezha/flezha/blob/main/docs/public/topics/core/building_blocks.md)
walks through what each of the three parts is and why the split is the way it
is. The [PHP SDK guide](https://github.com/flezha/flezha/blob/main/docs/internal/sdk/php_sdk.md) covers the rest of the
SDK, including the threaded executor.
