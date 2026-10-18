# Starter: a relay that counts visits per page

A small, complete Flezha project in PHP, to grow into your own: one relay, one
tasklet that keeps state, and a test. `composer.json` names the SDK by its
released version, the way your own project does. It needs PHP 8.4 or later with
`ext-ffi`.

| File | What it is |
|---|---|
| `src/CountHits.php` | the tasklet `count`: keeps a page's total in state, changes it only through the change `run` records, and answers the new count |
| `src/StarterRelay.php` | the relay `starter`: the entry channel `hit`, described and keyed by page, into `count`, out on `counts` |
| `runner.php` | the runner: serves the relay until shutdown |
| `tests/CountHitsTest.php` | the test: four hits with no controller, and the replay check |

## Run it

You need a controller running and reachable, as
[Start the controller](https://github.com/flezha/flezha/blob/main/docs/public/getting_started/up_and_running.md#start-the-controller)
starts one. From this folder, install the SDK, run the test and start the
runner:

```sh
composer install
vendor/bin/phpunit tests
php runner.php
```

Then, from another terminal, start the relay and call it:

```sh
flz relay start starter --id starter
flz relay call starter --channel hit --arg page=home
```

```
{"page":"home","total":1}
```

Each call for the same page answers the next total. `flz` is the release's
`bin/flz`. The runner keeps running until a SIGTERM, Ctrl-C or its standard
input closing. The namespace is its first argument and defaults to `default`.

## Make it yours

Nothing inside the files is renamed for you. Change the `name` and the
namespaces in `composer.json`, and the namespace in each file. `starter` is the
name `flz relay start` takes. `count` is the name the tasklet's state is saved
under, so choose it before the first run.

## Before the SDK is published

The package is not on Packagist yet. From the root of a Flezha checkout, stage
the native library it loads (it needs a Rust toolchain):

```sh
scripts/build-php-sdk.sh --release
```

Then tell Composer where the package is, once, with the checkout's absolute
path. The entry lives in Composer's global configuration, so `composer.json`
stays as it is:

```sh
composer config --global repositories.flezha-sdk \
  '{"type": "path", "url": "/path/to/flezha/sdk/php", "options": {"versions": {"flezha/sdk": "0.1.0"}}}'
```
