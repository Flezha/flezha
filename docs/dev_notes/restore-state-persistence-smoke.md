# restore-state-persistence-smoke — implementation notes

The change was scoped as "repair one removed CLI option". The option had
never existed as a CLI option, and the three failures hiding below it were
each a different kind of lie the script had been telling.

## `--Xoperative_id` / `--Xposition` were never provider options

`--X…` flags are PROVIDER options: `CliManager.provider_opts/1` drops only
`extensions` and `trigger` from them and hands the rest to
`init_provider/1`, where a provider with a `@known_opts` allow-list refuses
every key it does not own. Attachment and subscription are plain CLI
options that `CliManager.enable_plugin/4` consumes first:

```elixir
operative_id = Map.get(plugin_flags, "operative_id") || Map.get(opts, "operative_id")
```

So `--Xoperative_id` was read for the `%Plugin{}` struct AND left in the
provider map, which is why it produced `unknown option(s) for websocket`
while still doing its job. `--Xtrigger` survived only because `trigger` is
one of the two keys `provider_opts/1` drops.

`position` is not a provider option either — it is a subscription
sub-option, valid only on the ordered kinds (`:fold`, `:transform`).
`on_interval` is a `:timer`, so `Extension.Table` answers
`{:error, :position_not_applicable}` and the script's `--Xposition=0` was
doubly wrong. The correct invocation carries no position at all.

`docker/dev/README.md` and `README.md` carried the same dead invocation
(the README's `--settings='{…}'` form never existed at all — `handle_enable`
has no such flag and requires a positional `<plugin_id>`); both were
rewritten against the vocabulary the script now proves.

## `operative_class` on disk is the operative ID, not the factory id

```elixir
def operative_class(%__MODULE__{} = state),
  do: Keyword.get(state.builder.opts, :operative_class, state.id)
```

So the class level of `<state_root>/<namespace>/<class>/<procedure>/<key>.wal`
is `sp_test_operative`, never the `store_input` factory — and
`flz state list store_input` reports nothing. The script had ONE variable,
`OPERATIVE_CLASS`, feeding both `flz operative start` (which wants the
factory) and `flz state list` (which wants the operative id). They are now
`FACTORY_ID` and `OPERATIVE_ID`. This was invisible for as long as the
plugin attach was broken, because nothing below it ran.

## The `--extension` vocabulary cannot express a `:timer` subscription

Filed as `fix-extension-subopt-typing`. `cli/src/utils.rs::serialize_flags`
inlines a value as raw JSON only when it parses as an object or array, so
`--on_interval.interval_ms=5000` ships as the STRING `"5000"`;
`atomize_keyword/1` atomises keys only; `validate_interval/1` accepts
integers only. Proven against a live stack:

```
Error enabling plugin: {:invalid_subscription_opts, {:interval_ms, "5000"}}.
```

`interval_ms` is required for every `:timer` subscription, so no
`on_interval` can be declared through `--extension` — which is why this
script drives `--trigger=proactive`, whose `trigger_to_subscription/1` path
hard-codes a real integer. `position` has the same shape and the same
problem. `--extension=on_output` (no sub-options) works fine.

## REFUTED, do not re-investigate

* **`pipefail` + `grep -q` reaping the writer.** The natural explanation for
  "the poll said the plugin was missing and the failure dump printed it" is
  a short-circuiting `grep -q` sending SIGPIPE to `docker compose exec` and
  `pipefail` reporting the writer's status. Tested against a live stack:
  pipeline exit 0 with pipefail on AND off, `PIPESTATUS` `0 0`, and the
  original assertion re-run verbatim 30 times missed 0 times. Not the cause.
  The real defect is that the assertion could not TELL: it discarded stderr,
  threw away the text it matched against, and re-ran the command to build
  the message — so its evidence and its verdict came from different reads.
  `await_flz_output` matches and dumps the same captured text and reports
  `flz`'s exit code, which is what separates "the CLI said no" from "the CLI
  failed".
* **`flz state at` / `flz state rotate` crash the BEAM.** The script had
  carried that claim in its header for a long time. All three `state` verbs
  were driven against a live stack; the node stayed up every time. The
  `state at` failure was a path bug in the script's own dead helper:
  `discover_entity_path` derived the entity key from the on-disk FILENAME,
  and `_` is `Registry.encode_entity_key/1`'s rendering of the empty key,
  not the key. With the decoded key the verb returns a state. Both dead
  helpers were deleted.

## Operational traps

* **Every run is a cold build.** `cleanup` runs `down -v`, which removes the
  `_build`, `deps` and `target` named volumes, so `cargo build -p flz` and
  `mix release` run from scratch each time. Four consecutive green runs
  measured 2m20s, 3m36s, 3m37s and 4m08s.
  This is why the script is recorded as manual rather than wired into CI:
  runtime is not the blocker, the Docker daemon + Kafka broker +
  in-container toolchain are.
* **Never edit the script while a run is executing it.** Bash reads a script
  incrementally, so an in-place rewrite mid-run corrupts execution. Stage
  the edit and apply it after the run exits.
* **`docker compose down --remove-orphans` is refused in this repo** (a
  sibling project shares the compose project name `dev`). The script's own
  teardown is project-scoped and safe; a manual teardown must drop the flag.

## Deliberately not done

* **`test/scripts/plugin_persistence.sh` carries the identical rotted
  invocation** — `--Xoperative_id`, `--Xtrigger`, `--Xposition` — and will
  fail the same way. It was left alone because it cannot be verified from
  here: it drives `docker-compose.healing.yml` and expects a RELEASE binary
  at `/flezha/target/release/flz`, neither of which this change builds. The
  fix content is proven; the run is not.
* **The four compose-smoke scripts duplicate a whole harness.**
  `fetch_active_ce_id` is byte-identical in `connection_healing.sh`,
  `plugin_persistence.sh`, `state_freshness.sh` and this one; so are the
  `COMPOSE` array, the `FLEZHA_KEEP_STACK` cleanup trap, `note`, and the
  runner-id poll (three names, one body). Extracting a sourced harness is
  the right move and is explicitly a non-goal here — it needs the three
  siblings runnable first, and `connection_healing.sh` restarts an `agent`
  service it never starts while `state_freshness.sh` self-documents as
  no-op'ing its assertion. Extraction without being able to run the callers
  is how a shared core acquires a dead branch.
