# Installable plugins

For operators who install plugin packages on a controller and for authors who build them.
A plugin package (`.fpz`) is a source archive: the controller validates it, compiles it,
hot-loads the modules and records the install so it survives a restart — no release
rebuild. This page covers the package format, the manifest, checksums and Ed25519 signing,
the `flz plugin` verbs, install/enable/upgrade/uninstall semantics, dependencies, the
configuration knobs, per-node scope and troubleshooting. What a provider does once it is
enabled is in [lifecycle.md](lifecycle.md).

The subsystem is off by default: with `plugin_installer_enabled: false` the
`Flezha.Plugin.Installer.Supervisor` subtree is not started and every installer verb fails.
Compile-time providers (`flz plugin enable <id> <provider_id> --X...`) do not need it.

## Package format

A `.fpz` is a gzipped tarball whose root holds exactly:

```text
plugin_id-version.fpz
├── manifest.exs
├── src/                 # any tree of .ex / .erl sources
├── CHECKSUMS.sha256
└── signature.sig        # optional
```

Rules, enforced at extraction:

- No `.beam` files — compilation happens on the controller at install time.
- No symbolic links, device files, `..` components or absolute paths; nothing else at the root.
- Only `.ex` and `.erl` under a non-empty `src/`.
- Uncompressed size ≤ `max_package_bytes`; files under `src/` ≤ `max_source_files`.
- `CHECKSUMS.sha256` lists `manifest.exs` and every file under `src/`.
- `signature.sig` is the raw 64-byte Ed25519 signature over the bytes of `CHECKSUMS.sha256`.

## Authoring a plugin

The in-tree example is `examples/plugins/hello_world/`:

```elixir
# src/hello_world.ex
defmodule Flezha.Examples.HelloWorld do
  def on_input(ctx, opts) do
    _ = {ctx, opts}
    :ok
  end
end
```

```elixir
# manifest.exs
%{
  id: "hello_world",
  version: "1.0.0",
  description: "Minimal installable-plugin example for Flezha.",
  author: "Flezha Examples",
  modules: [Flezha.Examples.HelloWorld],
  provides: [
    %{
      id: "default",
      provider: Flezha.Examples.HelloWorld,
      hooks: [{:on_input, []}],
      default_opts: %{greeting: "hello"},
      scope: :operative
    }
  ],
  dependencies: []
}
```

```bash
cd examples/plugins/hello_world
sha256sum manifest.exs $(find src -type f | sort) > CHECKSUMS.sha256
tar czf ../hello_world-1.0.0.fpz manifest.exs src/ CHECKSUMS.sha256
```

A provider is an ordinary module: every hook needs the matching `<hook>/2`
export (`{:missing_callback, name}` otherwise). The optional lifecycle callbacks —
`init_provider/1`, `activate/1`, `health/1`, `terminate_provider/1` — are in
[lifecycle.md](lifecycle.md). `use Flezha.Plugin.ProviderBehaviour` matters only for a
module that also serves as a compile-time provider with `--X` flags.

## Manifest schema

`manifest.exs` is one Elixir map evaluated literal-only: maps, lists, tuples, atoms,
module aliases, binaries and numbers. Any function call, variable, sigil or special form
is rejected with `{:error, :non_literal_ast}`.

| Field | Type | Required | Rule |
| --- | --- | --- | --- |
| `:id` | `binary` | yes | `~r/^[a-z0-9][a-z0-9_]*$/`, at most 64 bytes |
| `:version` | `binary` | yes | must parse with `Version.parse/1` |
| `:modules` | `[atom]` | yes | non-empty; must equal the set the sources produce |
| `:provides` | `[map]` | yes | non-empty; provision ids unique |
| `:dependencies` | `[{binary, binary}]` | no | `{plugin_id, requirement}`; default `[]` |
| `:description`, `:author` | `binary` | no | shown by `flz plugin info` |

Each `:provides` entry: `:id` (binary), `:provider` (a module listed in `:modules`),
`:hooks` (non-empty list of `{hook :: atom, opts :: keyword}`, type-checked against the
declaration for that hook's dispatch kind when the plugin is enabled), `:scope`
(only `:operative`), and optional `:default_opts` (a map, the provider opts at enable).
Violations return `{:error, {:manifest_invalid, reason}}` naming the first one.

## Checksums

`CHECKSUMS.sha256` is GNU `sha256sum` output — `<hex>  <relative path>` per line; blank
lines and `#` comments are ignored. Coverage is exact: `{:checksum_file_missing, path}`,
`{:checksum_mismatch, path}`, or `{:checksum_line_missing, path}` for a file under `src/`
(or `manifest.exs`) that is not listed.

## Signing

```bash
openssl genpkey -algorithm ED25519 -out plugin_key.pem
openssl pkey -in plugin_key.pem -pubout -out plugin_key.pub.pem
openssl pkeyutl -sign -inkey plugin_key.pem -rawin -in CHECKSUMS.sha256 -out signature.sig
```

Add `signature.sig` to the archive; the controller operator installs the public key:

```elixir
config :flezha, :plugin_installer,
  trusted_keys: [File.read!("plugin_key.pub.pem")],
  require_signature: true,
  allow_unsigned: false
```

Keys are PEM (`SubjectPublicKeyInfo`) or raw 32-byte binaries; a signature passes if any
trusted key verifies it (`:signature_invalid` / `:no_trusted_keys` otherwise). An unsigned
package is refused with `:signature_required` under `require_signature: true`, with
`:unsigned_not_allowed` under `allow_unsigned: false`, and otherwise accepted with a
`[:flezha, :plugin_installer, :unsigned_install]` event. `require_signature: true` with
empty `trusted_keys` refuses to boot the subtree (`{:config_invalid, :trusted_keys_empty}`).

## CLI reference

```text
flz plugin install <path-to-.fpz>
flz plugin upgrade <plugin_id> <path-to-.fpz>
flz plugin uninstall <plugin_id>
flz plugin enable <plugin_id>             # installed plugin — no provider id, no --X flags
flz plugin disable <plugin_id>
flz plugin list [--installed] [--json | --output json]
flz plugin info <plugin_id>
flz plugin help [<provider_id>]
```

- The `.fpz` path is read by the controller node the CLI is attached to.
- `enable` / `disable` route through the installer when the id is in the catalog, and
  through the compile-time provider path otherwise.
- `list` shows the compile-time provider registry; `--installed` selects the catalog
  instead. The output format never selects the collection: the registry renders as text
  only, so `flz plugin list --json` without `--installed` is refused rather than answered
  with the other list.
- Installer verbs authorize on the `_system` namespace with actions `plugin.install`,
  `plugin.upgrade`, `plugin.uninstall`, `plugin.enable`, `plugin.disable`; pass
  `--token <token>` when enforcement is on.

## Lifecycle semantics

### Install

1. Extract into `<plugin_root>/staging/<random>/`, enforcing the size limits and the
   package rules; validate the root layout.
2. Parse and validate `manifest.exs`; verify checksums and coverage.
3. Apply the signature policy; verify `signature.sig` when present.
4. Refuse an id already in the catalog (`{:already_installed, id}`); resolve dependencies;
   refuse a module already loaded from the release or owned by another plugin
   (`{:module_collision, mod}`).
5. Compile every source — warnings are errors — and require the produced module set to
   equal `:modules`.
6. Copy the package to `<plugin_root>/installed/<id>/<version>/`, write
   `ebin/<module>.beam`, load every beam with `:code.load_binary/3`.
7. Write the catalog row with status `:disabled`.

Any failure rolls back what was produced — directories, loaded modules, the row.

### Enable

`Flezha.Plugin.Installer.enable/2` calls `Flezha.Service.PluginManager.enable_plugin/2`
once per provision with the live id `"<plugin_id>:<provision_id>"` and the provision's
`:provider`, `:hooks` and `:default_opts` (merged with `opts[:overrides][provision_id]`).
The target operative is `opts[:namespace]` / `opts[:operative_id]`, falling back to the
same keys inside the merged opts. `flz plugin enable <id>` passes no opts, so a provision
enabled from the CLI must carry `:namespace` and `:operative_id` in its `default_opts` or
the enable is refused ("plugin missing namespace or operative_id"). A provision that fails
disables the ones enabled earlier in the same call. Refusals: `:not_found`,
`:already_enabled`, `:load_failed`.

### Disable

Disables every provision and marks the row `:disabled`. Modules stay loaded, so
re-enabling touches no disk. Disabling a `:disabled` or `:load_failed` row is a no-op.

### Upgrade

The new package runs the install pipeline — its `:id` must match
(`{:id_mismatch, new, existing}`) and dependents' constraints must still hold
(`{:dependents_block, [...]}`) — into a sibling `<new_version>/` directory, with the row
marked `:pending_upgrade`. If the old version was `:enabled`, its provisions are disabled
and the installer waits up to 5 s for in-flight `:request` tasks under
`Flezha.Operative.MessageHandlerTaskSup`. Old modules are purged, new ones loaded, and the
new manifest's provisions re-enabled. On success the old directory is deleted and the
status returns to what it was; on failure the new directory is removed, the old row and
beams restored from `<old_version>/ebin/`, and the old provisions re-enabled.

### Uninstall

Refused while `:enabled` (`:plugin_enabled`) or while another installed plugin depends on
it (`{:dependents_block, ids}`). Otherwise purges the modules, removes
`<plugin_root>/installed/<plugin_id>/` and deletes the row.

## Dependencies

```elixir
dependencies: [{"common", ">= 1.0.0"}, {"kafka_base", "~> 2.3"}]
```

A constraint is any requirement string `Version.parse_requirement/1` accepts, checked with
`Version.match?/2` against the installed dependency's version. No transitive install —
install dependencies first, in order.

| Condition | Error |
| --- | --- |
| dependency not installed | `{:dependency_missing, id, constraint}` |
| installed version fails the constraint | `{:dependency_constraint, id, constraint, installed}` |
| the new manifest closes a cycle | `{:dependency_cycle, path}` |
| upgrade would break a dependent | `{:dependents_block, [{dependent, constraint}]}` |
| uninstall while dependents exist | `{:dependents_block, [dependent]}` |

## Configuration

Defaults live under `config :flezha` in `config/config.exs`; each has an environment
override in `config/runtime.exs`. Booleans accept `true`, `1`, `yes` (case-insensitive).

| Env var | Key | Default |
| --- | --- | --- |
| `FLEZHA_PLUGIN_INSTALLER_ENABLED` | `:plugin_installer_enabled` | `false` |
| `FLEZHA_PLUGIN_ROOT` | `:plugin_installer[:plugin_root]` | `app_data/plugins` |
| `FLEZHA_PLUGIN_REQUIRE_SIGNATURE` | `:plugin_installer[:require_signature]` | `false` |
| `FLEZHA_PLUGIN_ALLOW_UNSIGNED` | `:plugin_installer[:allow_unsigned]` | `true` |
| `FLEZHA_PLUGIN_TRUSTED_KEYS` | `:plugin_installer[:trusted_keys]` | `[]`; comma-separated PEM file paths, read at boot |
| `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES` | `:plugin_installer[:max_package_bytes]` | `50_000_000` |
| `FLEZHA_PLUGIN_MAX_SOURCE_FILES` | `:plugin_installer[:max_source_files]` | `200` |

```bash
export FLEZHA_PLUGIN_INSTALLER_ENABLED=true
export FLEZHA_PLUGIN_ROOT=/var/lib/flezha/plugins
export FLEZHA_PLUGIN_REQUIRE_SIGNATURE=true
export FLEZHA_PLUGIN_ALLOW_UNSIGNED=false
export FLEZHA_PLUGIN_TRUSTED_KEYS=/etc/flezha/plugin_keys/team_a.pub.pem,/etc/flezha/plugin_keys/team_b.pub.pem
```

## Per-node semantics

`flz plugin install` affects only the node the CLI is attached to; there is no cluster
fan-out. Install on every node that should carry the plugin.

The catalog is a DETS file at `<plugin_root>/installed_plugins.dets`, synced after every
mutation. A file that fails to open is renamed `installed_plugins.dets.corrupt-<monotonic>`
and a fresh catalog started; the `installed/<id>/<version>/` trees stay so the packages can
be re-installed. A `plugin_root` shared between nodes (NFS and the like) is unsupported.

At boot the Loader reloads every row from its `ebin/`; a row whose beams cannot be loaded
is flipped to `:load_failed` and skipped.

## Telemetry

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:flezha, :plugin_installer, :store_opened]` | `%{rows}` | `%{path}` |
| `[:flezha, :plugin_installer, :unsigned_install]` | `%{count: 1}` | `%{plugin_id}` |
| `[:flezha, :plugin_installer, :load_failed]` | `%{count: 1}` | `%{plugin_id, module, reason}` |

## Troubleshooting

| Error | Meaning / fix |
| --- | --- |
| `:load_failed` | a beam under `installed/<id>/<version>/ebin/` could not load at boot (deleted, corrupt, or built under another OTP/Elixir); uninstall and re-install — the archive can be rebuilt from the `src/` tree on disk |
| `{:compile_failed, file, reason}` | compiler error or warning; fix, regenerate checksums, rebuild |
| `{:manifest_invalid, {:module_mismatch, ...}}` | the sources define a different module set than `:modules` |
| `{:module_collision, mod}` | `mod` belongs to the release or another plugin; use a project prefix |
| `:signature_required`, `:unsigned_not_allowed`, `:signature_invalid`, `:no_trusted_keys` | sign the package, or relax the policy in development |
| `{:invalid_layout, _}`, `{:unsafe_entry, name, kind}`, `{:package_too_large, _, _}`, `{:too_many_source_files, _, _}` | the archive breaks a package rule; rebuild it |

## See also

- [lifecycle.md](lifecycle.md) — what a provider does in `init_provider/1` and `activate/1`
- [../operations/authn_authz.md](../operations/authn_authz.md) — the `plugin.*` actions and tokens
- [../configuration.md](../configuration.md) — every application-env key in one place
