# Plugin packages

For users who install plugin packages on a controller and for authors who build them.
A plugin package (`.fpz`) carries compiled code: a plugin project's own application and
every library it needs that the controller release does not ship. The controller loads
the packages in its plugin root once, when it starts, and never changes a running
system's code. Installing, upgrading or removing a package takes effect at the next
restart. The provider contract a package implements is `Flezha.Plugin.ProviderBehaviour`'s;
the guide page [`custom_plugins.md`](../guides/advanced/custom_plugins.md) walks through
writing one.

## What a package is

A `.fpz` is a gzipped tar, laid out like a release's `lib/`:

```text
flezha_example_plugin-0.1.0.fpz
├── manifest.exs
├── CHECKSUMS.sha256
├── signature.sig                          # optional
└── lib/
    ├── flezha_example_plugin-0.1.0/ebin/  # the plugin application
    └── stamp_lib-0.1.0/
        ├── ebin/
        └── priv/                          # optional
```

`manifest.exs` is a literal Elixir term, read without evaluating anything:

```elixir
%{
  id: "flezha_example_plugin",
  version: "0.1.0",
  built_with: %{flezha: "0.1.0", otp: "28", elixir: "1.20.2"},
  apps: [flezha_example_plugin: "0.1.0", stamp_lib: "0.1.0"],
  providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}
}
```

The first application in `apps` is the plugin application: `id` is its name and `version`
its version. `CHECKSUMS.sha256` holds one `sha256sum` line for `manifest.exs` and for every
file under `lib/`. `signature.sig`, when present, is a 64-byte Ed25519 signature over
`CHECKSUMS.sha256`.

A package holds no source (`.ex` or `.erl`), no native code (a `priv` file ending in `.so`,
`.dll` or `.dylib`, or a module with an `on_load` function), no symbolic link and nothing
outside that layout. Each `lib/<app>-<vsn>/ebin/` holds exactly the beams its `.app` lists.
The checks run at install and again at every boot.

## Building a package

A plugin is an ordinary Mix project that depends on `flezha` and on the libraries it needs,
and names its providers in its application environment:

```elixir
def application do
  [env: [flezha_providers: %{"example_stamp" => FlezhaExamplePlugin.Stamp}]]
end
```

`mix flezha.plugin.package`, which every project depending on `flezha` has, builds the
archive:

```bash
MIX_ENV=prod mix flezha.plugin.package [--output DIR] [--sign key.pem]
```

It compiles the project, bundles every application of the plugin's runtime closure that
the release does not ship, records the Flezha, OTP and Elixir versions it was built with,
and writes `<id>-<version>.fpz` to `--output` (`_build/` by default). It refuses, writing
nothing:

- a library the release ships, resolved at another version than Flezha's own `mix.lock`,
  naming both versions and how to pin it;
- native code;
- an application that depends on `flezha` and declares an application callback (`mod`):
  a plugin's processes are its providers' declared workers;
- a `:flezha_providers` entry that is not a provider module of the plugin application.

A controller loads a package only on the runtime line it was built on:

- **OTP:** the release the package was built on or one of the two after it;
- **Elixir:** the same major, at the package's minor or a later one. A package built on an
  older minor loads with one warning naming both versions, logged at boot and printed by
  `flz plugin install`: code compiled there can call an Elixir internal a later minor
  changed, which fails only when that line runs;
- **Flezha:** the version the package was built for or a later one, of the same minor
  before 1.0 and of the same major from 1.0.

Build it with the controller's toolchain. For the Docker image, that is the
`plugin-builder` target of the controller's Dockerfile:

```bash
docker build --target plugin-builder -t flezha/plugin-builder:0.1.0 \
  -f docker/prod/Dockerfile.controller .
docker run --rm -v "$PWD/my_plugin:/src" flezha/plugin-builder:0.1.0 \
  sh -c 'mix deps.get && mix flezha.plugin.package'
```

## Listening to an operator from a plugin

A plugin that streams an operator's output to a client of its own (an HTTP plugin's
server-sent events, for one) reads it through `Flezha.Service.Listener.listen/4`, the
listener `flz operator listen` uses. It runs in the calling process, hands the handler one
event at a time, and detaches on every way out, a raise in the handler included. It does not
authorize: a plugin decides for itself who may read.

```elixir
Flezha.Service.Listener.listen(namespace, operator_id, opts, handler)
```

`handler` receives an event and answers `:continue` or `:done`:

| Event | What it is |
|---|---|
| `{:output, payload, metadata}` | an output on the operator's output chain |
| `{:part, payload, metadata}` | a part: a preview a tasklet sent while it ran, `flz.part` its number within its attempt, `flz.part_attempt` the attempt |
| `{:dead_lettered, %Flezha.Error{}, metadata}` | an input the operator dead-lettered, with the code `input.dead_lettered`, once its entry is durable |
| `{:input, payload, metadata}` | with `direction: :input`, an input the operator ingests |
| `:idle` | with `idle_ms`, no event for that long |

| Option | Default | Meaning |
|---|---|---|
| `direction` | `:output` | `:output` (outputs, parts and notices) or `:input` |
| `window_ms` | `:infinity` | a positive integer: how long to listen |
| `idle_ms` | `:infinity` | a positive integer: call the handler with `:idle` after that long with no event |
| `before` | none | a function run once after the attach, before the first event |
| `watch` | none | a process whose exit ends the listener |

It answers `{:ok, :window_closed | :done | :caller_down | :operator_ended}`, or
`{:error, %Flezha.Error{}}`: the enable's own refusal (`operator.not_found`, for one),
`listener.overrun` when the handler falls behind by more than `listener_pending_bytes` or
`listener_pending_frames` of outputs (held parts are dropped first, oldest first, and a
reader sees the gap as a jump in `flz.part`), or `listener.detached` when someone else
disabled its plugin. The listener goes on across the operator sleeping and waking, being
rebound, its Overwatch restarting, moving to another controller and a group rescale, and
ends with `:operator_ended` when the operator ends.

A server-sent events stream, with a comment every 15 seconds so a proxy keeps the
connection and a client that left is noticed:

```elixir
Flezha.Service.Listener.listen(namespace, session_id, [idle_ms: 15_000], fn
  {:part, payload, _metadata} -> write_event("part", payload)
  {:output, payload, _metadata} -> write_event("output", payload)
  {:dead_lettered, error, _metadata} -> write_event("dead_lettered", Flezha.Error.render(error, :json))
  :idle -> write_comment()
end)
```

`write_event/2` and `write_comment/0` write through the request's connection, which the
plugin keeps in its own process, and answer `:continue`, or `:done` once the client has
gone.

## Installing, uninstalling and syncing

```bash
flz plugin install ./flezha_example_plugin-0.1.0.fpz
flz plugin install https://example.com/flezha_example_plugin-0.1.0.fpz
flz plugin uninstall flezha_example_plugin
flz plugin sync
```

The three change the plugin root and nothing else: they run in a VM of their own and never
reach a running controller, so they work before the first start and they carry no token.
Write access to the plugin root is their gate. Each prints the absolute plugin root it
acted on; restart the controller to apply the change.

- `install` takes a local path or an `https://` URL (any other scheme is refused). A URL
  is fetched with peer verification against the operating system's CA store, following at
  most five redirects, each to `https://`, and abandoned past
  `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES`. The archive is verified in
  `<plugin_root>/.staging/` and renamed to `<plugin_root>/<id>.fpz`, replacing every
  archive that declares the same id. A refused install leaves the root as it was.
- An upgrade is an install of a newer archive of the same id.
- `uninstall` removes every archive declaring the id. The running controller keeps the
  package until it restarts.
- `sync` makes the root hold exactly the packages `FLEZHA_PLUGINS` declares: entries
  separated by whitespace or commas, each a path or an `https://` URL, optionally pinned
  with `#sha256=<hex>`. A pinned entry already present is kept without a fetch, every
  other entry is fetched and verified, and every archive the list did not produce is
  removed. A failed entry fails the sync and changes nothing.

While `FLEZHA_PLUGINS` is set, even empty, `install` and `uninstall` are refused with
`plugin.list_declared`: the next start's sync would undo them.

## What the controller holds

`flz plugin list --installed` shows one row per package id:

| Status | Meaning |
|---|---|
| `loaded` | loaded at this boot, with its version and providers |
| `refused` | present at this boot and not loaded, with its error's code, message and hint |
| `pending` | placed or replaced since this boot; loads at the next restart, and names the version running now if one is |
| `removed` | loaded at this boot, its archive since removed; unloads at the next restart |

`--json` prints the rows as an array of objects with `id`, `version`, `status`,
`loaded_version`, `providers` and `error` (`null`, or `{code, class, message, details,
hint}`). `flz plugin list --providers` lists the providers that can be enabled and ends
with a line naming how many packages were refused.

A package's providers are enabled like any provider (`flz plugin enable <name>
<provider_id> ...`). Enabling a provider of a refused package answers
`plugin.provider_unavailable`, naming the package and its refusal. When another controller
holding the namespace's hub lacks the provider, the enable warns naming it: the plugin
will not run there.

An operator placed on a controller that did not load a bound plugin's provider runs
without that plugin. It logs one warning naming the plugin and why: the package that
declared the provider and whether it was refused or is pending, or that no installed
package provides it. Its health reports `plugin_unhealthy` with
`plugin.provider_not_loaded`, a guaranteed output to the missing sink waits, and a plain
one is dropped and counted. The plugin comes up with the operator's next start on a
controller that has the package.

## Errors

| Code | Class | When |
|---|---|---|
| `plugin.package_invalid` | invalid | the layout, the manifest or the checksums are wrong; `details.problem` names which |
| `plugin.native_code` | invalid | a shared library under `priv/`, or a module with `on_load` |
| `plugin.app_callback` | invalid | an application depending on `flezha` declares `mod` |
| `plugin.signature_invalid` | denied | a signature no trusted key verifies |
| `plugin.signature_required` | denied | no signature, with `FLEZHA_PLUGIN_REQUIRE_SIGNATURE=true` |
| `plugin.trust_store_empty` | invalid | signatures required and no trusted key configured |
| `plugin.runtime_mismatch` | conflict | built outside the controller's OTP, Elixir or Flezha line; `details.rule` names which: `otp_newer`, `otp_too_old`, `elixir_newer`, `elixir_major` or `flezha_line` |
| `plugin.library_conflict` | conflict | a bundled library at another version than the one the release or an earlier package holds |
| `plugin.module_conflict` | conflict | a module the release or an earlier package already holds |
| `plugin.provider_conflict` | conflict | a provider id that is already a row |
| `plugin.duplicate_id` | conflict | two archives in the root declare one id; both are refused |
| `plugin.load_failed` | internal | loading an application or its modules, or starting a library, failed |
| `plugin.protocol_consolidation_failed` | internal | a protocol could not be consolidated with the package's implementations |
| `plugin.fetch_failed` | unavailable | a source could not be fetched |
| `plugin.digest_mismatch` | invalid | a pinned entry's archive has another digest |
| `plugin.source_unsupported` | invalid | a source is neither a path nor an `https://` URL |
| `plugin.not_installed` | not_found | uninstall of an id with no archive |
| `plugin.list_declared` | conflict | install or uninstall while `FLEZHA_PLUGINS` is set |
| `plugin.list_not_declared` | invalid | sync with `FLEZHA_PLUGINS` unset |
| `plugin.provider_unavailable` | unavailable | a provider of a refused or pending package |
| `plugin.provider_refused` | invalid | a package provider refused its options with a reason the catalogue does not know |
| `plugin.provider_failed` | internal | a package provider's worker failed to start with such a reason |

A package cannot add codes of its own. A provider refusal whose reason the catalogue
already maps keeps that code, and so does the reason a provider's worker stopped with at
start; any other is wrapped as `plugin.provider_refused` or
`plugin.provider_failed`, with the package, the provider and the package's own text in
`details`. The full catalogue is [`errors.md`](../errors.md).

## Deployment shapes

- **A release on a host**: `flz plugin install <path-or-url>`, then restart the
  controller however it runs (systemd, a shell). The plugin root is
  `FLEZHA_PLUGIN_ROOT`, `app_data/plugins` relative to the working directory by default.
- **Docker**: the image's plugin root is `/flezha/app_data/plugins`. Install into a named
  volume there with `docker exec <c> /flezha/bin/flz plugin install ...` and `docker
  restart <c>`; or set `FLEZHA_PLUGINS`, which the entrypoint syncs before it starts the
  controller, exiting non-zero without starting it when the sync fails; or build a derived
  image that copies the archives into that directory, with no volume over it.
  [`docker/prod/README.md`](../../docker/prod/README.md) has the commands.
- **Kubernetes**: the reference ConfigMap puts `FLEZHA_PLUGIN_ROOT` on the recovery volume
  and declares `FLEZHA_PLUGINS`; a change is a `kubectl rollout restart`.
  [`kubernetes/README.md`](../../kubernetes/README.md) has the steps.

The plugin root must be writable when the controller starts: every boot expands the
accepted archives under `<plugin_root>/.loaded/`, which nothing else touches, so an
install on a running node never changes a file the running system uses.

## Signing

Signing is optional. `mix flezha.plugin.package --sign key.pem` writes `signature.sig` with
an Ed25519 private key in PEM form (`openssl genpkey -algorithm ed25519 -out key.pem`).
`FLEZHA_PLUGIN_TRUSTED_KEYS` names the PEM public keys a controller verifies with (`openssl
pkey -in key.pem -pubout`), comma separated.

- A present signature is verified whatever the setting, and one no trusted key verifies
  is refused.
- A missing signature is accepted with a warning, unless
  `FLEZHA_PLUGIN_REQUIRE_SIGNATURE=true`.
- With signatures required and no trusted key, every package is refused and the
  controller still boots.

Verification runs at install and at every boot, so removing a key from the trust store
refuses the packages it signed at the next start. What each boot loaded and refused is
recorded as the telemetry events `[:flezha, :plugin_package, :loaded]` and
`[:flezha, :plugin_package, :refused]`, which reach the audit log.

## Configuration

`FLEZHA_PLUGIN_ROOT`, `FLEZHA_PLUGIN_TRUSTED_KEYS`, `FLEZHA_PLUGIN_REQUIRE_SIGNATURE`,
`FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` and `FLEZHA_PLUGINS` are rows of
[`configuration.md`](../configuration.md). No setting gates loading: an archive in the
plugin root is the opt-in.

## Limits

- No native code: NIFs and port programs are refused.
- An application that depends on `flezha` cannot have an application callback; long-lived
  processes are a provider's declared workers, which the controller supervises.
- A plugin's `config/*.exs` does not travel. A provider is configured by its `--X` options
  at enable, and a library reads only the defaults in its `.app`.
- One version of each library per controller: two packages bundling one library must
  agree on its version.
- A protocol the release consolidated is consolidated again with a package's
  implementations; a protocol a package defines itself stays unconsolidated.
- A package's own telemetry events reach handlers attached to them but are not exported by
  the observability bridge.

A plugin root left by the earlier source installer (an `installed/` tree, an
`installed_plugins.dets` file) is ignored: only `*.fpz` files are read. Those leftovers may
be deleted.
