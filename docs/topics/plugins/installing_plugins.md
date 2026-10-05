# Installing plugins

A plugin that does not ship with the controller comes as a plugin package: a `.fpz` file
carrying compiled code, the plugin's own application and every library it needs that the
controller release does not ship. The HTTP server, Kafka, RabbitMQ and Postgres change
capture plugins are packages, and so is a plugin of your own.

A controller loads the packages in its plugin root once, when it starts, and never changes
a running system's code. Installing, upgrading or removing a package takes effect at the
next restart. Packages are installed per controller: give every controller of a cluster
the same list, because a controller that lacks a package does not run that plugin, and a
relay bound to it there runs without it ([`where_plugins_run.md`](where_plugins_run.md#a-controller-without-the-provider)).

## How it works

The plugin root is a folder: `FLEZHA_PLUGIN_ROOT`, `app_data/plugins` relative to the
working directory by default, `/flezha/app_data/plugins` in the Docker image. It holds one
`<id>.fpz` archive per package. No setting gates loading: an archive in the plugin root is
the opt-in.

At every boot the controller takes the archives in id order, verifies each one, expands it
under `<plugin_root>/.loaded/<id>/`, which nothing else touches, and loads its code before
any relay's plugins come up, so a binding to a package's provider subscribes the plugin
like a built-in one. Install, uninstall and sync only change the archives: an install on a
running node never changes a file the running system uses. The plugin root must therefore
be writable when the controller starts. A package that fails a step is refused, with a
code that says why; one package's refusal does not affect another's, and the controller
finishes booting. A runner-only boot loads no package.

### What a package is

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
The checks run at install and again at every boot, so an archive changed on disk after its
install is refused at the next boot.

### Which packages a controller loads

A controller loads a package only on the runtime line it was built on:

- **OTP:** the release the package was built on or one of the two after it;
- **Elixir:** the same major, at the package's minor or a later one. A package built on an
  older minor loads with one warning naming both versions, logged at boot and printed by
  `flz plugin install`: code compiled there can call an Elixir internal a later minor
  changed, which fails only when that line runs;
- **Flezha:** the version the package was built for or a later one, of the same minor
  before 1.0 and of the same major from 1.0.

A package never replaces code the controller already holds: a module the release or an
earlier package holds is refused (`plugin.module_conflict`), and so is a bundled library at
another version than the one already loaded (`plugin.library_conflict`). One version of
each library runs per controller, so two packages bundling one library must agree on its
version. A protocol the release consolidated is consolidated again with a package's
implementations.

## How to use it

### Install, uninstall and sync

```sh
$ flz plugin install ./flezha_example_plugin-0.1.0.fpz
$ flz plugin install https://example.com/flezha_example_plugin-0.1.0.fpz
$ flz plugin uninstall flezha_example_plugin
$ flz plugin sync
```

The three change the plugin root and nothing else: they run in a VM of their own and never
reach a running controller, so they work before the first start and carry no token. Write
access to the plugin root is their gate. Each prints the absolute plugin root it acted on;
restart the controller to apply the change.

- `install` takes a local path or an `https://` URL; any other scheme is refused
  (`plugin.source_unsupported`). A URL is fetched with peer verification against the
  operating system's CA store, following at most five redirects, each to `https://`, and
  abandoned past `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES`. The archive is verified in
  `<plugin_root>/.staging/` and renamed to `<plugin_root>/<id>.fpz`, replacing every
  archive that declares the same id. A refused install leaves the root as it was.
- An upgrade is an install of a newer archive of the same id.
- `uninstall` removes every archive declaring the id. The running controller keeps the
  package, and its providers can still be enabled, until it restarts.
- `sync` makes the root hold exactly the packages `FLEZHA_PLUGINS` declares: entries
  separated by whitespace or commas, each a path or an `https://` URL, optionally pinned
  with `#sha256=<hex>`. A pinned entry already present is kept without a fetch, every
  other entry is fetched and verified, and every archive the list did not produce is
  removed. A failed entry fails the sync and changes nothing.

While `FLEZHA_PLUGINS` is set, even empty, `install` and `uninstall` are refused with
`plugin.list_declared`: the next start's sync would undo them. The declared list is then
the node's only truth.

```sh
$ flz plugin install ./flezha_example_plugin-0.1.0.fpz
```

```output
Installed flezha_example_plugin 0.1.0 (unsigned) into /opt/flezha/app_data/plugins.
Restart the controller to load it.
```

### What the controller holds

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
<provider_id> ...`). Enabling a provider of a refused or pending package answers
`plugin.provider_unavailable`, naming the package and its status. When another controller
holding the namespace lacks the provider, the enable warns, naming it.

A relay placed on a controller that did not load a bound plugin's provider runs
without that plugin. It logs one warning naming the plugin and why: the package that
declared the provider and whether it was refused or is pending, or that no installed
package provides it. Its health reports `plugin_unhealthy` with
`plugin.provider_not_loaded`, a guaranteed output to the missing sink waits, and a plain
one is dropped and counted. The plugin comes up with the relay's next start on a
controller that has the package.

### Where the packages come from

- **A release on a host:** `flz plugin install <path-or-url>`, then restart the controller
  however it runs (systemd, a shell).
- **Docker:** the image's plugin root is `/flezha/app_data/plugins`. Install into a named
  volume there with `docker exec <c> /flezha/bin/flz plugin install ...` and
  `docker restart <c>`; or set `FLEZHA_PLUGINS`, which the entrypoint syncs before it
  starts the controller, exiting non-zero without starting it when the sync fails; or build
  a derived image that copies the archives into its plugin root, with no volume over it.
  [`docker/prod/README.md`](https://github.com/flezha/flezha/blob/main/docker/prod/README.md) has the commands.
- **Kubernetes:** the reference ConfigMap puts `FLEZHA_PLUGIN_ROOT` on the recovery volume
  and declares `FLEZHA_PLUGINS`; a change is a `kubectl rollout restart`.
  [`kubernetes/README.md`](https://github.com/flezha/flezha/blob/main/kubernetes/README.md) has the steps, and
  [`kubernetes.md`](../deploying/kubernetes.md) the deployment.

### Signed packages

Signing is optional. A package author signs with `mix flezha.plugin.package --sign key.pem`
([`writing_a_plugin.md`](writing_a_plugin.md#sign-it)). `FLEZHA_PLUGIN_TRUSTED_KEYS` names
the PEM public keys a controller verifies with, comma separated.

- A present signature is verified whatever the setting, and one no trusted key verifies
  is refused (`plugin.signature_invalid`).
- A missing signature is accepted with a warning, unless
  `FLEZHA_PLUGIN_REQUIRE_SIGNATURE=true` (`plugin.signature_required`).
- With signatures required and no trusted key, every package is refused
  (`plugin.trust_store_empty`) and the controller still boots.

Verification runs at install and at every boot, so removing a key from the trust store
refuses the packages it signed at the next start. What each boot loaded and refused is
recorded as the telemetry events `[:flezha, :plugin_package, :loaded]` and
`[:flezha, :plugin_package, :refused]`, which reach the audit log.

### Settings

| Setting | Default | What it does |
| --- | --- | --- |
| `FLEZHA_PLUGIN_ROOT` | `app_data/plugins` | the folder of installed `*.fpz` packages, loaded at every boot; must be writable |
| `FLEZHA_PLUGINS` | unset | the packages this node declares, which `flz plugin sync` places |
| `FLEZHA_PLUGIN_TRUSTED_KEYS` | `""` | comma-separated PEM public key paths, read at boot |
| `FLEZHA_PLUGIN_REQUIRE_SIGNATURE` | `false` | refuse unsigned packages |
| `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` | `50000000` | a package's unpacked size and a download alike |

[Controller environment variables](../../configuration/environment_variables.md#plugins-dlq-and-schedules) has each row.

## Example

In the agentdesk example, the controller gets the HTTP server plugin, a package built in
its own repository and carried as the submodule `plugins/flezha_http_plugin`. A package
records the Erlang and Elixir versions it was built with, so it is built in the controller
image's builder stage, which has the same toolchain. From a checkout of the Flezha
repository with its submodules:

```sh
$ git clone --recurse-submodules https://github.com/flezha/flezha
$ cd flezha
$ docker build --target plugin-builder -t flezha/plugin-builder:0.1.0 -f docker/prod/Dockerfile.controller .
$ docker run --rm -v "$PWD:/src" -w /src/plugins/flezha_http_plugin flezha/plugin-builder:0.1.0 sh -c 'mix deps.get && mix flezha.plugin.package'
```

```output
...
==> flezha_http
Compiling 6 files (.ex)
Generated flezha_http app
Wrote /src/plugins/flezha_http_plugin/_build/flezha_http-0.1.0.fpz
```

The whole checkout is mounted, because the plugin builds against the Flezha checkout it is
a submodule of. The package holds the plugin and the libraries it needs that the
controller does not ship, Bandit and Plug among them.

agentdesk's controller image is a derived image that copies the project's
`controller/plugins/` folder into its plugin root:

```dockerfile
# The controller's image: Flezha's own, with the plugin packages in plugins/.
FROM flezha/flezha:0.1.0
COPY --chown=flezha:flezha plugins/ /flezha/plugins/
ENV FLEZHA_PLUGIN_ROOT=/flezha/plugins
```

[Full file](https://github.com/flezha/flezha_java_sdk/blob/main/examples/agentdesk/controller/Dockerfile)

The package is copied into that folder, and the image rebuilt and its container replaced,
from agentdesk's folder:

```sh
$ cp <the plugin's folder>/_build/flezha_http-0.1.0.fpz controller/plugins/
$ docker compose up -d --build --wait controller
$ flz plugin list --installed
```

```output
flezha_http 0.1.0  loaded  providers: http_server
```

The new controller starts the relays again once the runner has connected to it, within
about a minute; a call before that answers `relay.not_found`. It found them on the
`app_data` volume. To upgrade the plugin, put the newer package in its place and rebuild;
to remove it, delete it and rebuild.

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
start; any other is wrapped as `plugin.provider_refused` or `plugin.provider_failed`, with
the package, the provider and the package's own text in `details`. The full catalogue is
[`errors.md`](../../errors.md).

## Limits

- **Loading happens at boot only.** No package is loaded, replaced or unloaded while a
  controller runs.
- **No native code:** NIFs and port programs are refused.
- **A bundled library's settings are the package's.** A plugin's `config/*.exs` reaches
  only the libraries the package bundles, and a library the release ships cannot be
  reconfigured by a package. A provider is configured by its `--X` options at enable.
- **One version of each library per controller.**
- **Leftovers of the earlier source installer** (an `installed/` tree, an
  `installed_plugins.dets` file) are ignored: only `*.fpz` files are read, and those
  leftovers may be deleted.

## See also

- [`plugins.md`](plugins.md): enabling and binding a plugin once its package is loaded
- [`writing_a_plugin.md`](writing_a_plugin.md): building a package of your own
- [`kubernetes.md`](../deploying/kubernetes.md) and [`clustering.md`](../deploying/clustering.md): several controllers
- [`../../configuration.md`](../../configuration.md) and [`../../errors.md`](../../errors.md)
- Tutorial: [`add_your_own_plugin.md`](../../tutorials/agentdesk/add_your_own_plugin.md)
