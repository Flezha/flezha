# Installable Plugins

Flezha supports a Jenkins-style plugin lifecycle: executors install a
plugin package from a local archive, the controller validates, compiles, and
hot-loads the modules, and the plugin persists across restarts. No
rebuild of the release is required.

This document covers:

- [Package format](#package-format)
- [Authoring a plugin](#authoring-a-plugin)
- [Manifest schema](#manifest-schema)
- [Generating checksums](#generating-checksums)
- [Signing packages](#signing-packages)
- [CLI reference](#cli-reference)
- [Lifecycle semantics](#lifecycle-semantics)
- [Dependencies between plugins](#dependencies-between-plugins)
- [Per-node semantics (no cluster fanout)](#per-node-semantics)
- [Troubleshooting](#troubleshooting)

## Package format

A Flezha plugin package (`.fpz`) is a gzipped tarball containing exactly:

```
plugin_id-version.fpz
├── manifest.exs
├── src/
│   └── any_tree_of_ex_or_erl_sources
├── CHECKSUMS.sha256
└── signature.sig        # optional
```

Rules:

- No `.beam` files. Compilation happens on the controller at install time.
- No symbolic links, no path traversal (`..`), no absolute paths.
- Only `.ex` and `.erl` files under `src/`.
- `CHECKSUMS.sha256` MUST list every file under `src/` and `manifest.exs`.
- `signature.sig` is 64 raw bytes of Ed25519 signature over the UTF-8
  bytes of `CHECKSUMS.sha256`.

## Authoring a plugin

Minimal example — a single-file plugin:

```
my_plugin/
├── manifest.exs
├── src/
│   └── my_plugin.ex
└── CHECKSUMS.sha256
```

`src/my_plugin.ex`:

```elixir
defmodule MyOrg.MyPlugin do
  use Flezha.Plugin.ProviderBehaviour

  @enforce_keys []
  defstruct []

  @impl true
  def plugin_type, do: :io_binding

  def on_input(_ctx, _opts), do: :ok
end
```

`manifest.exs`:

```elixir
%{
  id: "my_plugin",
  version: "1.0.0",
  description: "Example plugin",
  author: "Ops Team",
  modules: [MyOrg.MyPlugin],
  provides: [
    %{
      id: "default",
      provider: MyOrg.MyPlugin,
      subscriptions: [{:on_input, []}],
      default_opts: %{},
      scope: :relay
    }
  ],
  dependencies: []
}
```

Build the package:

```bash
cd my_plugin
sha256sum manifest.exs src/*.ex > CHECKSUMS.sha256
tar czf ../my_plugin-1.0.0.fpz manifest.exs src/ CHECKSUMS.sha256
```

## Manifest schema

`manifest.exs` is an Elixir term (map). It is evaluated in a restricted
binding — only literal values are allowed. Function calls, variables,
and special forms (including sigils, `import`, `require`, `use`) are
rejected.

| Field | Type | Required | Notes |
| --- | --- | --- | --- |
| `:id` | `binary` | yes | `~r/^[a-z0-9][a-z0-9_]*$/`, max 64 chars |
| `:version` | `binary` | yes | SemVer; must parse via `Version.parse/1` |
| `:description` | `binary` | no | Human-readable |
| `:author` | `binary` | no | Human-readable |
| `:modules` | `[atom]` | yes | Non-empty; every module the package defines |
| `:provides` | `[map]` | yes | One entry per provision (blueprint for enable) |
| `:dependencies` | `[{binary, binary}]` | no | `{plugin_id, Version-requirement}` |

Each `:provides` entry:

```elixir
%{
  id: "default",                           # unique within the manifest
  provider: MyOrg.MyPlugin,                # MUST appear in :modules
  subscriptions: [{:on_input, []}],        # list of {extension, opts}
  default_opts: %{},                       # overridable at enable time
  scope: :relay                        # only :relay in this release
}
```

## Generating checksums

GNU `sha256sum` output is the canonical format:

```bash
cd my_plugin
sha256sum manifest.exs $(find src -type f | sort) > CHECKSUMS.sha256
```

Comments (lines starting with `#`) and blank lines are allowed and
ignored by the installer's parser.

## Signing packages

Signing is optional unless the controller is configured with
`require_signature: true`. Generate an Ed25519 keypair:

```bash
openssl genpkey -algorithm ED25519 -out plugin_key.pem
openssl pkey -in plugin_key.pem -pubout -out plugin_key.pub.pem
```

Sign `CHECKSUMS.sha256` and store the raw 64-byte signature:

```bash
openssl pkeyutl -sign -inkey plugin_key.pem -rawin \
  -in CHECKSUMS.sha256 -out signature.sig
```

Add `signature.sig` to the archive and deliver the public key to the
controller executor, who installs it in `config.exs`:

```elixir
config :flezha, :plugin_installer,
  trusted_keys: [File.read!("plugin_key.pub.pem")],
  require_signature: true,
  allow_unsigned: false
```

Both PEM-encoded keys and raw 32-byte binary keys are accepted.

## CLI reference

```
flz plugin install <path-to-.fpz>
flz plugin upgrade <plugin_id> <path-to-.fpz>
flz plugin uninstall <plugin_id>
flz plugin enable <plugin_id>             # installed plugin shortcut
flz plugin disable <plugin_id>
flz plugin list [--installed] [--json]
flz plugin info <plugin_id>
flz plugin help [<provider_id>]
```

The `list` command accepts both `--installed` (show catalog rows) and no
flag (show legacy, compile-time provider registry). `--json` produces
machine-readable output.

## Lifecycle semantics

### Install

1. Extract archive into staging.
2. Validate layout, size, and file types.
3. Verify every checksum in `CHECKSUMS.sha256`.
4. Verify signature (if present or required).
5. Parse and validate `manifest.exs`.
6. Resolve dependencies against the live catalog.
7. Compile every source (warnings treated as errors).
8. Cross-check produced modules against `:modules`.
9. Move staging → `app_data/plugins/installed/<id>/<version>/`.
10. Load every `.beam` into the VM via `:code.load_binary/3`.
11. Write catalog row as `:disabled`.
12. Clean up staging.

Any failure rolls back **all** side effects — no orphaned directories,
no dangling modules, no catalog rows.

### Enable

Calls `Flezha.Service.PluginManager.enable_plugin/2` once per
`:provides` entry. Subscription ids are `"<plugin_id>:<provision_id>"`
so multiple provisions coexist per package. A partial-enable failure
rolls back subscriptions taken earlier in the same call.

### Disable

Unsubscribes via `PluginManager.disable_plugin/1` per provision.
Modules stay loaded in the VM — re-enable is instant, no disk I/O.

### Upgrade

Installs the new package to a sibling `<new_version>/` directory. If
the old plugin was `:enabled`: disable subscriptions, wait up to 5s for
in-flight tasks to drain, purge old modules, load new modules,
re-subscribe against the new manifest. On any failure after old
modules are purged, the installer re-loads the old `.beam` files from
`<old_version>/ebin/` and re-subscribes, then deletes the new
directory and returns the error.

### Uninstall

Refuses while the plugin is `:enabled` (executor must disable first).
Refuses while any other installed plugin declares a dep on this one.
Purges every owned module, removes the `<plugin_id>/` directory, and
deletes the catalog row.

## Dependencies between plugins

Declare deps in the manifest:

```elixir
dependencies: [
  {"common", ">= 1.0.0"},
  {"kafka_base", "~> 2.3"}
]
```

Constraints use a subset of SemVer comparators: `>=`, `>`, `<`, `<=`,
`==`, and `~>`. No combinators.

- Missing dep: install is rejected with
  `{:error, {:dependency_missing, id, constraint}}`.
- Constraint failure: `{:error, {:dependency_constraint, id, c, installed}}`.
- Circular dep: `{:error, {:dependency_cycle, path}}`.
- Upgrade that would break a dependent: `{:error, {:dependents_block, [...]}}`.
- Uninstall while dependents exist: same shape.

No transitive install — the executor installs deps in order.

## Staged rollout / environment overrides

The installer subsystem is feature-flagged so it can be landed dark and
enabled per environment without a code change. Every knob is reachable
via env vars in `config/runtime.exs`:

| Env var | Maps to | Default |
| --- | --- | --- |
| `FLEZHA_PLUGIN_INSTALLER_ENABLED` | `:plugin_installer_enabled` | `false` |
| `FLEZHA_PLUGIN_ROOT` | `:plugin_installer[:plugin_root]` | `app_data/plugins` |
| `FLEZHA_PLUGIN_REQUIRE_SIGNATURE` | `:plugin_installer[:require_signature]` | `false` |
| `FLEZHA_PLUGIN_ALLOW_UNSIGNED` | `:plugin_installer[:allow_unsigned]` | `true` |
| `FLEZHA_PLUGIN_TRUSTED_KEYS` | `:plugin_installer[:trusted_keys]` | `[]` (comma-separated PEM paths) |
| `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` | `:plugin_installer[:max_package_bytes]` | `50_000_000` |
| `FLEZHA_PLUGIN_MAX_SOURCE_FILES` | `:plugin_installer[:max_source_files]` | `200` |

Staging rollout template:

```bash
export FLEZHA_PLUGIN_INSTALLER_ENABLED=true
export FLEZHA_PLUGIN_ROOT=/var/lib/flezha/plugins
# optional — enforce signing
export FLEZHA_PLUGIN_REQUIRE_SIGNATURE=true
export FLEZHA_PLUGIN_ALLOW_UNSIGNED=false
export FLEZHA_PLUGIN_TRUSTED_KEYS=/etc/flezha/plugin_keys/team_a.pub.pem,/etc/flezha/plugin_keys/team_b.pub.pem
```

Production rollout is the same but typically pinned through the
deployment manifest. Flipping the compiled-in default in
`config/config.exs` follows once production has stabilised.

## Per-node semantics

**Installation does NOT fan out across the cluster in this release.**
`flz plugin install` affects only the node the CLI is attached to. To
install on every node of a Flezha cluster, repeat the install on each
node. Cluster-wide semantics are tracked as a future enhancement.

Plugin state is persisted in `app_data/recovery/installed_plugins.dets`
per node. If a persistent volume is shared across nodes (e.g., via
NFS), simultaneous installs from multiple nodes will corrupt the
catalog; this configuration is unsupported.

## Troubleshooting

### `{:error, :load_failed}`

A plugin row is marked `:load_failed` at boot when the Loader cannot
`:code.load_binary/3` one of the row's modules. Common causes:

- The `ebin/<module>.beam` file was deleted out-of-band.
- Disk corruption; try `sha256sum` against `CHECKSUMS.sha256` under
  `installed/<id>/<version>/`.
- OTP/Elixir version drift between install time and the current controller.

Fix: re-install the package. The original `.fpz` can be rebuilt from
the `installed/<id>/<version>/src/` tree if the executor has disk
access.

### `{:error, {:compile_failed, file, reason}}`

The source failed to compile. `reason` contains the compiler's actual
output — syntax error, unused variable (warnings are errors), missing
module reference, etc. Fix the source, regenerate checksums, rebuild
the archive.

### `{:error, {:module_collision, mod}}`

`mod` is already loaded in the VM and not owned by any installed
plugin. It's part of the compiled release or a stdlib module. Pick a
different module name, or rename your package's module to live under
a project prefix (`MyOrg.*`).

### `{:error, {:dependency_cycle, path}}`

Two or more plugins form a circular dependency. `path` shows the
cycle. Break it by removing one of the edges in a manifest.

### `{:error, :signature_required}` or `:unsigned_not_allowed`

The controller config requires a signed package. Either sign the package
(see [Signing packages](#signing-packages)) or flip the policy in
`config.exs` during development.

### Recovery telemetry: `[:flezha, :recovery, :orphaned_plugin]`

Emitted when `Recovery.Coordinator` sees a persisted plugin row whose
installer catalog entry has disappeared (e.g., the `installed/<id>/`
directory was removed out-of-band). The row is silently dropped and
recovery continues.
