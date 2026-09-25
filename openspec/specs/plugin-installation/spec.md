# plugin-installation Specification

## Purpose

The plugin-installation capability gives runners a Jenkins-style runtime plugin lifecycle for Flezha: install from a signed/checksummed archive, compile and hot-load on the running agent, enable, disable, upgrade, and uninstall — all without rebuilding the release. A plugin package (`*.fpz`) ships `manifest.exs`, `src/*.ex` or `src/*.erl`, a `CHECKSUMS.sha256` file, and an optional detached Ed25519 signature; the agent compiles and loads modules exactly once at install time, persists the installed catalog in DETS (`installed_plugins.dets` in the layout's plugin subtree, `app_data/plugins/` by default), and re-hydrates every `:enabled`/`:disabled` package's modules into the VM before `Flezha.Service.PluginManager` starts, so no operator bind can replay a persisted subscription against a module that is not loaded. The capability defines the package format, manifest schema, checksum and signature verification rules, install/upgrade/uninstall/enable/disable semantics, dependency resolution, module-collision guards, on-disk layout, catalog persistence, boot-time hydration ordering, the `Flezha.Plugin.Installer` public service API shared by CLI and the planned Phoenix UI, recovery interaction, supervision tree ordering, and the configuration surface (`:flezha, :plugin_installer`).
## Requirements
### Requirement: Plugin package format

A plugin package SHALL be a gzip-compressed tar archive with the conventional extension `.fpz`. The archive MUST contain, at its root:

- `manifest.exs` — an Elixir term (map) describing the plugin (see "Manifest schema")
- `src/` — a directory containing one or more `.ex` and/or `.erl` source files, in any sub-directory layout the author chooses
- `CHECKSUMS.sha256` — a plain-text file with one `<64-hex-char>  <relative-path>` line per file under `src/` AND a line for `manifest.exs`, matching the GNU `sha256sum` output format

The archive MAY contain, at its root:

- `signature.sig` — a raw 64-byte Ed25519 signature over the UTF-8 bytes of `CHECKSUMS.sha256`

The archive MUST NOT contain any other top-level files or directories. The archive MUST NOT contain pre-compiled `.beam` files, symbolic links, device files, or files with non-portable permissions (executable bits other than `0o644` for files and `0o755` for directories).

#### Scenario: Well-formed package is accepted
- **WHEN** `Flezha.Plugin.Installer.install/2` is called with a `.fpz` path whose archive contains `manifest.exs`, a non-empty `src/` tree of `.ex` files, and a valid `CHECKSUMS.sha256`
- **THEN** the installer MUST proceed past layout validation

#### Scenario: Missing manifest rejected
- **WHEN** the archive lacks `manifest.exs` at its root
- **THEN** the installer MUST return `{:error, {:invalid_layout, :manifest_missing}}` and MUST NOT write to `app_data/plugins/installed/`

#### Scenario: Missing checksums file rejected
- **WHEN** the archive lacks `CHECKSUMS.sha256` at its root
- **THEN** the installer MUST return `{:error, {:invalid_layout, :checksums_missing}}`

#### Scenario: Pre-compiled beams rejected
- **WHEN** the archive contains a file whose name ends in `.beam`
- **THEN** the installer MUST return `{:error, {:invalid_layout, :beam_forbidden}}`

#### Scenario: Unknown top-level entries rejected
- **WHEN** the archive contains a top-level file or directory other than `manifest.exs`, `src/`, `CHECKSUMS.sha256`, or `signature.sig`
- **THEN** the installer MUST return `{:error, {:invalid_layout, {:unexpected_entry, path}}}`

#### Scenario: Oversized package rejected
- **WHEN** the archive's uncompressed size exceeds `:flezha, :plugin_installer, max_package_bytes` (default 50_000_000)
- **THEN** the installer MUST abort extraction, remove any partially-extracted files from staging, and return `{:error, {:package_too_large, size, limit}}`

### Requirement: Manifest schema

`manifest.exs` SHALL be an Elixir term of shape `%{id: binary, version: binary, description: binary, author: binary, modules: [module], provides: [map], dependencies: [{binary, binary}]}` and MAY include `:description` and `:author`. Every field carries these constraints:

- `id`: binary matching `~r/^[a-z0-9][a-z0-9_]*$/` and no longer than 64 characters
- `version`: binary parseable by `Version.parse/1` (SemVer)
- `modules`: non-empty list of atoms — every module the package defines
- `provides`: list of maps, each with `id` (binary, unique within the manifest), `provider` (atom, present in `modules`), `hooks` (list of `{atom, keyword}`), `default_opts` (map), and `scope` (atom, `:operator` only in this release)
- `dependencies`: list of `{plugin_id, constraint}` where `constraint` is a Version-compatible requirement string (e.g. `">= 1.0.0"`, `"~> 2.3"`); MAY be `[]`

The manifest MUST be evaluated inside a restricted binding that disables `import`, `require`, and `use`, and its abstract syntax tree MUST contain only literal nodes (maps, lists, tuples, atoms, binaries, integers, module aliases). The installer MUST reject any manifest whose AST contains function calls or variable references.

#### Scenario: Valid manifest parses
- **WHEN** `manifest.exs` evaluates to a map with a valid `:id`, a `:version` of `"1.2.0"`, non-empty `:modules`, at least one well-formed entry in `:provides`, and empty `:dependencies`
- **THEN** the installer MUST accept the manifest

#### Scenario: Missing required field rejected
- **WHEN** `manifest.exs` is missing the `:version` key
- **THEN** the installer MUST return `{:error, {:manifest_invalid, {:missing_field, :version}}}`

#### Scenario: Invalid id rejected
- **WHEN** `manifest.exs` declares `id: "My-Plugin"` (uppercase + hyphen)
- **THEN** the installer MUST return `{:error, {:manifest_invalid, {:invalid_id, "My-Plugin"}}}`

#### Scenario: Non-semver version rejected
- **WHEN** `manifest.exs` declares `version: "beta-2"` which `Version.parse/1` cannot parse
- **THEN** the installer MUST return `{:error, {:manifest_invalid, {:invalid_version, "beta-2"}}}`

#### Scenario: Provider not in modules list rejected
- **WHEN** a `provides` entry names `provider: Something.NotInModules` and that atom is absent from `manifest.modules`
- **THEN** the installer MUST return `{:error, {:manifest_invalid, {:provider_not_declared, Something.NotInModules}}}`

#### Scenario: Manifest containing code rejected
- **WHEN** `manifest.exs` contains a call expression such as `System.get_env("FOO")`
- **THEN** the installer MUST return `{:error, {:manifest_invalid, :non_literal_ast}}` WITHOUT evaluating the call

### Requirement: SHA-256 checksum verification

The installer SHALL recompute the SHA-256 digest of `manifest.exs` and of every file under `src/` and compare each digest byte-for-byte against the value declared in `CHECKSUMS.sha256`. Any mismatch, any missing line, and any line that refers to a path not present in the archive MUST cause the install to abort.

#### Scenario: All checksums match
- **WHEN** every file's recomputed SHA-256 matches its declared value in `CHECKSUMS.sha256`
- **THEN** the installer MUST proceed to the next step

#### Scenario: Tampered source rejected
- **WHEN** `src/my_plugin.ex` has been modified after `CHECKSUMS.sha256` was written and its recomputed digest no longer matches
- **THEN** the installer MUST return `{:error, {:checksum_mismatch, "src/my_plugin.ex"}}` and MUST NOT compile or load any module from the package

#### Scenario: Checksum line for missing file rejected
- **WHEN** `CHECKSUMS.sha256` lists a file path that does not exist in the archive
- **THEN** the installer MUST return `{:error, {:checksum_file_missing, path}}`

#### Scenario: Source file not covered by checksums rejected
- **WHEN** `src/extra.ex` is present in the archive but has no corresponding line in `CHECKSUMS.sha256`
- **THEN** the installer MUST return `{:error, {:checksum_line_missing, "src/extra.ex"}}`

### Requirement: Ed25519 signature verification

When `signature.sig` is present in the archive OR when `:flezha, :plugin_installer, require_signature` is `true`, the installer SHALL verify a signature over the UTF-8 bytes of `CHECKSUMS.sha256` using Erlang's `:crypto.verify/5` with `{:eddsa, :ed25519}`. A signature MUST be considered valid if and only if at least one public key in `:flezha, :plugin_installer, trusted_keys` verifies it. Missing signatures MUST be rejected when `require_signature: true` or `allow_unsigned: false`.

#### Scenario: Valid signature accepted
- **WHEN** the archive contains a `signature.sig` that verifies against one of the trusted keys
- **THEN** the installer MUST proceed past signature verification

#### Scenario: Invalid signature rejected
- **WHEN** the archive contains a `signature.sig` that does not verify against any trusted key
- **THEN** the installer MUST return `{:error, :signature_invalid}` and MUST NOT continue

#### Scenario: Unsigned package accepted in permissive mode
- **WHEN** the archive has no `signature.sig`, `require_signature: false`, and `allow_unsigned: true`
- **THEN** the installer MUST proceed past signature verification without raising an error, but MUST emit a `[:flezha, :plugin_installer, :unsigned_install]` telemetry event with the plugin id

#### Scenario: Unsigned package rejected in required mode
- **WHEN** the archive has no `signature.sig` and `require_signature: true`
- **THEN** the installer MUST return `{:error, :signature_required}`

#### Scenario: Unsigned package rejected when unsigned disallowed
- **WHEN** the archive has no `signature.sig` and `allow_unsigned: false`
- **THEN** the installer MUST return `{:error, :unsigned_not_allowed}`

#### Scenario: Trusted keys empty with require_signature rejects boot
- **WHEN** the agent starts with `require_signature: true` and `trusted_keys: []`
- **THEN** `Flezha.Plugin.Installer` MUST refuse to start, emitting a `:config_invalid` error and preventing the supervisor from completing `init/1`

### Requirement: Source compilation at install time

The installer SHALL compile every `.ex` file under `src/` via `Code.compile_file/2` and every `.erl` file via `:compile.file/2`, collecting the resulting `.beam` binaries in memory. Compilation MUST treat warnings as errors (`--warnings-as-errors`). The set of modules produced by compilation MUST exactly match the `manifest.modules` list; any mismatch aborts the install.

#### Scenario: Clean compile produces declared modules
- **WHEN** the source compiles cleanly and the produced module list equals `manifest.modules`
- **THEN** the installer MUST proceed to module loading

#### Scenario: Compilation error aborts install
- **WHEN** any source file fails to compile
- **THEN** the installer MUST return `{:error, {:compile_failed, file, reason}}`, MUST NOT load any module from the package, and MUST NOT write any `.beam` to `app_data/plugins/installed/`

#### Scenario: Warning treated as error
- **WHEN** a source file compiles but emits a warning (e.g. unused variable)
- **THEN** the installer MUST return `{:error, {:compile_failed, file, {:warning, message}}}`

#### Scenario: Module list mismatch rejected
- **WHEN** compilation produces modules `[A, B]` but `manifest.modules` declares `[A, B, C]`
- **THEN** the installer MUST return `{:error, {:manifest_invalid, {:module_mismatch, expected: [A, B, C], produced: [A, B]}}}`

#### Scenario: Extra module produced rejected
- **WHEN** compilation produces modules `[A, B, C]` but `manifest.modules` declares `[A, B]`
- **THEN** the installer MUST return `{:error, {:manifest_invalid, {:module_mismatch, expected: [A, B], produced: [A, B, C]}}}`

### Requirement: Module collision with compiled-in code

The installer SHALL refuse to install a plugin whose `manifest.modules` contains a module atom already loaded in the VM and not owned by any installed plugin in the catalog. This prevents an installed plugin from shadowing a built-in provider or Flezha core module.

#### Scenario: Installed module shadows built-in rejected
- **WHEN** a manifest declares `modules: [Flezha.Plugin.Provider.Kafka]` and that module is already loaded from the compiled release
- **THEN** the installer MUST return `{:error, {:module_collision, Flezha.Plugin.Provider.Kafka}}`

#### Scenario: Installed module shadows another plugin's module rejected
- **WHEN** a manifest declares `modules: [MyOrg.Shared]` and another installed plugin's catalog row lists `MyOrg.Shared` among its modules
- **THEN** the installer MUST return `{:error, {:module_collision, MyOrg.Shared}}`

### Requirement: Dependency resolution at install and upgrade

The installer SHALL evaluate `manifest.dependencies` against the live catalog before writing to disk. Every dependency tuple `{dep_id, constraint}` MUST map to a catalog row whose `:version` satisfies `constraint` (via `Version.match?/2`). Dependencies MUST NOT form a cycle considering the new manifest together with the existing catalog. Upgrades MUST additionally ensure that after the upgrade, every installed plugin's dependencies on the upgraded plugin remain satisfied.

#### Scenario: All dependencies satisfied
- **WHEN** `manifest.dependencies` is `[{"common", ">= 1.0.0"}]` and the catalog contains `common` at version `1.2.3`
- **THEN** the installer MUST proceed

#### Scenario: Missing dependency rejected
- **WHEN** `manifest.dependencies` names `common` but the catalog has no row keyed `"common"`
- **THEN** the installer MUST return `{:error, {:dependency_missing, "common", ">= 1.0.0"}}`

#### Scenario: Version constraint failure rejected
- **WHEN** `manifest.dependencies` names `{"common", ">= 2.0.0"}` and the catalog has `common` at `1.2.3`
- **THEN** the installer MUST return `{:error, {:dependency_constraint, "common", ">= 2.0.0", "1.2.3"}}`

#### Scenario: Dependency cycle rejected
- **WHEN** installing plugin `a` whose manifest lists dep `b`, and the existing `b` in the catalog lists dep `a`
- **THEN** the installer MUST return `{:error, {:dependency_cycle, ["a", "b", "a"]}}` and MUST NOT install

#### Scenario: Upgrade preserves dependents' constraints
- **WHEN** upgrading `common` from `1.2.3` to `2.0.0` and an installed plugin `x` declares `dependencies: [{"common", "~> 1.2"}]`
- **THEN** the upgrade MUST be rejected with `{:error, {:dependents_block, [{"x", "~> 1.2"}]}}` and the catalog MUST remain unchanged

### Requirement: Install transactionality

Installing a plugin SHALL be all-or-nothing with respect to on-disk state, in-VM module state, and the catalog. On any failure during the install pipeline, the installer MUST leave the system in its pre-install state: no `app_data/plugins/installed/<id>/<version>/` directory, no modules loaded into the VM, no row in the installed-plugins catalog, and no content in staging.

#### Scenario: Failure after module load rolls back
- **WHEN** the installer has loaded two modules into the VM and the third fails to load
- **THEN** the installer MUST call `:code.purge/1` and `:code.delete/1` for the first two modules, MUST remove the target `installed/<id>/<version>/` directory if it was written, MUST NOT write a catalog row, and MUST return an error with the root cause

#### Scenario: Failure during DETS write rolls back modules
- **WHEN** modules have loaded successfully but the catalog DETS write fails
- **THEN** the installer MUST unload every newly-loaded module and remove the `installed/<id>/<version>/` directory before returning the error

#### Scenario: Successful install leaves clean staging
- **WHEN** an install completes successfully
- **THEN** `app_data/plugins/staging/` MUST NOT contain any files related to that install

### Requirement: On-disk installed-plugin layout

The installer SHALL write installed plugin files to `app_data/plugins/installed/<plugin_id>/<version>/` with this exact structure: `manifest.exs`, `src/<same-tree-as-package>`, `ebin/*.beam` (one `.beam` per module produced by compilation), `CHECKSUMS.sha256`, `signature.sig` (only if the package carried one), and `installed_at` containing the integer monotonic time of the install. Staging work SHALL be performed under `app_data/plugins/staging/<uuid>/` and removed after the install concludes (success or failure).

#### Scenario: Canonical layout after install
- **WHEN** a plugin `kafka_ingest` version `1.2.0` is installed successfully
- **THEN** `app_data/plugins/installed/kafka_ingest/1.2.0/` MUST contain `manifest.exs`, `src/`, `ebin/` with every module's `.beam` file, `CHECKSUMS.sha256`, and `installed_at`

#### Scenario: Signature file preserved when present
- **WHEN** the source package carried `signature.sig`
- **THEN** that file MUST be copied to `app_data/plugins/installed/<id>/<version>/signature.sig` with identical bytes

#### Scenario: Staging removed on failure
- **WHEN** any step of the install fails
- **THEN** `app_data/plugins/staging/<uuid>/` MUST be removed before the installer returns the error

### Requirement: Installed-plugin catalog persistence

The system SHALL maintain a DETS-backed catalog named `installed_plugins.dets` in the layout's plugin subtree, reached through `Flezha.Plugin.Installer.Store`. Rows MUST be keyed by `plugin_id` (binary) and MUST contain at least: `version` (binary), `status` (`:enabled | :disabled | :load_failed`), `path` (binary — the `installed/<id>/<version>` absolute path), `manifest_digest` (SHA-256 of `manifest.exs` bytes), `modules` (list of atoms), `installed_at` (integer monotonic), `source_checksums` (map of relative-path → hex digest). Writes MUST fsync to disk at the end of each install, upgrade, enable, disable, and uninstall tasklet before the tasklet returns.

The catalog SHALL be one declared durable store: the store names its layout subtree and its filename, and the opening, the corrupt-file rotation, the per-mutation sync and the close on shutdown SHALL be performed by the shared store implementation. `Flezha.Plugin.Installer.Store` SHALL NOT open, rotate, sync or close a DETS table itself, and SHALL NOT create its directory itself.

#### Scenario: Install writes a :disabled row
- **WHEN** an install completes successfully
- **THEN** the catalog MUST contain a row for the plugin's id with `status: :disabled`, the correct `version`, `path`, `manifest_digest`, and `modules`

#### Scenario: Enable flips status to :enabled
- **WHEN** `Flezha.Plugin.Installer.enable/2` succeeds for a `:disabled` plugin
- **THEN** the catalog row for that plugin id MUST now have `status: :enabled`, and the change MUST be persisted to disk before the call returns

#### Scenario: Uninstall deletes the row
- **WHEN** `Flezha.Plugin.Installer.uninstall/2` succeeds
- **THEN** no row keyed by that `plugin_id` MUST exist in the catalog after the call returns

#### Scenario: Catalog survives agent restart
- **WHEN** the agent is stopped after an install and then restarted
- **THEN** `Flezha.Plugin.Installer.Store` MUST open the existing DETS file and expose every previously-written row

#### Scenario: Corrupt catalog rotates and starts fresh
- **WHEN** the DETS file fails to open because it is corrupt
- **THEN** the Store MUST rename it to `installed_plugins.dets.corrupt-<timestamp>`, open a fresh empty DETS file at the original path, log an error with both paths, and continue starting

#### Scenario: The catalog follows the configured plugin root
- **WHEN** the plugin root is configured to a directory other than the default
- **THEN** the catalog file MUST be created under that directory, and no component MUST spell a plugin root of its own

### Requirement: Boot-time module loading

On agent startup `Flezha.Plugin.Installer.Loader.init/1` SHALL iterate every row in
`Flezha.Plugin.Installer.Store` and load every `.beam` file in each row's `ebin/`
directory via `:code.load_binary/3`. Hydration runs synchronously inside `init/1`, so
it MUST complete before `Flezha.Plugin.Installer` starts and therefore before
`Flezha.Service.PluginManager` — the first process through which a persisted
subscription can be replayed — is started by `Flezha.Application.controller_children/0`.
A plugin whose `ebin/` is missing or whose `.beam` fails to load MUST have its catalog
row's `status` updated to `:load_failed` but MUST NOT prevent other plugins from loading.

#### Scenario: Enabled plugin's modules loaded before any subscription replay
- **WHEN** the agent starts with one `:enabled` plugin in the catalog and a persisted subscription for it in the owning operator's ProcessHub child spec
- **THEN** the plugin's modules MUST be present in the VM before `Flezha.Operator.Plugins.prepare/2` re-subscribes that entry at the operator's bind

#### Scenario: Disabled plugin's modules loaded at boot
- **WHEN** the agent starts with one `:disabled` plugin in the catalog
- **THEN** the plugin's modules MUST still be loaded into the VM at boot, so that a subsequent `flz plugin enable` performs no disk I/O

#### Scenario: Corrupt ebin marks load_failed
- **WHEN** a plugin's `ebin/<module>.beam` cannot be loaded via `:code.load_binary/3`
- **THEN** the catalog row MUST be updated to `status: :load_failed`, an `[:flezha, :plugin_installer, :load_failed]` telemetry event MUST be emitted, and the Loader MUST continue with the next plugin

#### Scenario: load_failed plugin cannot be enabled
- **WHEN** `Flezha.Plugin.Installer.enable/2` is called for a plugin whose catalog row has `status: :load_failed`
- **THEN** the installer MUST return `{:error, :load_failed}` and MUST NOT attempt to subscribe

### Requirement: Enable lifecycle

`Flezha.Plugin.Installer.enable(plugin_id, opts)` SHALL, for each entry in the plugin's manifest `provides` list, construct a `%Flezha.Plugin{}` with that entry's `provider`, `hooks`, and `default_opts` merged with caller-supplied overrides, and invoke `Flezha.Service.PluginManager.enable_plugin/2`. Enable MUST fail if ANY of the `provides` entries fails to subscribe, MUST roll back any subscriptions that succeeded earlier in the call, and MUST NOT mutate the catalog row's status on failure.

#### Scenario: Enable subscribes every provision
- **WHEN** a plugin's manifest declares two entries in `provides` and `enable/2` is called
- **THEN** `PluginManager.enable_plugin/2` MUST be invoked twice with the corresponding `%Flezha.Plugin{}` structs, and the catalog row MUST flip to `:enabled` only after both subscriptions succeed

#### Scenario: Partial enable rolls back
- **WHEN** the first `provides` entry enables successfully but the second fails with `{:error, reason}`
- **THEN** `Flezha.Plugin.Installer.enable/2` MUST invoke `PluginManager.disable_plugin/1` for the first subscription, MUST leave the catalog row as `:disabled`, and MUST return `{:error, reason}`

#### Scenario: Double enable rejected
- **WHEN** `enable/2` is called for a plugin whose catalog row is already `:enabled`
- **THEN** the installer MUST return `{:error, :already_enabled}` and MUST NOT re-subscribe

### Requirement: Disable lifecycle

`Flezha.Plugin.Installer.disable(plugin_id, opts)` SHALL invoke `Flezha.Service.PluginManager.disable_plugin/1` for every `%Flezha.Plugin{}` id the package currently has subscribed, flip the catalog row's `status` to `:disabled`, and return `:ok`. The Loader MUST NOT purge or delete the plugin's modules from the VM on disable.

#### Scenario: Disable unsubscribes without purging modules
- **WHEN** `disable/2` is called for an `:enabled` plugin with two active subscriptions
- **THEN** both subscriptions MUST be unsubscribed via `PluginManager.disable_plugin/1`, the catalog row MUST flip to `:disabled`, and the plugin's modules MUST remain loaded in the VM (verifiable via `:code.is_loaded/1`)

#### Scenario: Disable on already-disabled is idempotent
- **WHEN** `disable/2` is called for a plugin whose catalog row is already `:disabled`
- **THEN** the installer MUST return `:ok` without calling `PluginManager.disable_plugin/1`

#### Scenario: Disable then enable is fast
- **WHEN** a plugin is disabled then re-enabled in the same session
- **THEN** the re-enable MUST NOT re-read any `.beam` file from disk and MUST NOT re-compile any source

### Requirement: Upgrade lifecycle

`Flezha.Plugin.Installer.upgrade(plugin_id, new_path, opts)` SHALL install the new package to `installed/<id>/<new_version>/` alongside the existing `installed/<id>/<old_version>/` directory, then — if the catalog row was `:enabled` — atomically stop every live subscription for the id, purge and delete every old module via `:code.purge/1` + `:code.delete/1`, load every module from the new package via `:code.load_binary/3`, and re-subscribe every entry in the new manifest's `provides` list under the same plugin ids as before. On any failure after the old modules are purged, the installer MUST re-load the old modules from the preserved `installed/<id>/<old_version>/ebin/` and re-subscribe the previous provisions, then remove the new directory and return an error; the plugin MUST end in its pre-upgrade state.

#### Scenario: Enabled upgrade hot-swaps modules
- **WHEN** `upgrade/3` is called for an `:enabled` plugin from version `1.0.0` to `1.1.0`
- **THEN** the new `.beam` files MUST be loaded into the VM replacing the old ones, the subscriptions MUST be re-established, the catalog row MUST reflect `version: "1.1.0"` and `status: :enabled`, and `installed/<id>/1.0.0/` MUST be deleted after success

#### Scenario: Disabled upgrade does not subscribe
- **WHEN** `upgrade/3` is called for a `:disabled` plugin
- **THEN** the new modules MUST be loaded, the catalog row MUST reflect the new version with `status: :disabled`, and `PluginManager.enable_plugin/2` MUST NOT be called

#### Scenario: Failed upgrade rolls back to old version
- **WHEN** during upgrade the new module fails to load after the old modules have been purged
- **THEN** the installer MUST reload the old modules from `installed/<id>/<old_version>/ebin/`, re-subscribe the previous provisions via `PluginManager.enable_plugin/2`, delete `installed/<id>/<new_version>/`, leave the catalog row at the old version with `status: :enabled`, and return the upgrade error

#### Scenario: In-flight request quiesce window
- **WHEN** an upgrade begins while a `:request`-kind callback is still executing in a Task
- **THEN** the installer MUST wait up to 5000ms after unsubscribe for in-flight Tasks to terminate before calling `:code.purge/1`; after the window elapses the installer MUST purge anyway and MUST log a warning with the plugin id

### Requirement: Uninstall lifecycle

`Flezha.Plugin.Installer.uninstall(plugin_id, opts)` SHALL refuse when the plugin's catalog row is `:enabled`, returning `{:error, :plugin_enabled}` and making no state changes. When the row is `:disabled` or `:load_failed`, the installer SHALL purge and delete every module listed in the catalog row, remove the `installed/<plugin_id>/` directory in its entirety, and delete the catalog row. Uninstall MUST additionally refuse if any other installed plugin declares this plugin as a dependency.

#### Scenario: Uninstall refuses while enabled
- **WHEN** `uninstall/2` is called for an `:enabled` plugin
- **THEN** the installer MUST return `{:error, :plugin_enabled}`, the catalog row MUST remain `:enabled`, and the modules MUST remain loaded

#### Scenario: Uninstall purges modules and clears directory
- **WHEN** `uninstall/2` is called for a `:disabled` plugin whose modules are loaded
- **THEN** after the call, `:code.is_loaded/1` MUST return `false` for every module listed in the catalog row, `app_data/plugins/installed/<plugin_id>/` MUST NOT exist, and the catalog MUST NOT contain a row keyed by that plugin id

#### Scenario: Uninstall refuses while a dependent is installed
- **WHEN** plugin `b` declares `dependencies: [{"a", ">= 1.0.0"}]` and `uninstall/2` is called on `a`
- **THEN** the installer MUST return `{:error, {:dependents_block, ["b"]}}` and MUST NOT purge any module or delete any directory

#### Scenario: Uninstall of :load_failed succeeds
- **WHEN** a plugin is marked `:load_failed` at boot (its ebin was corrupt) and `uninstall/2` is called
- **THEN** the installer MUST remove the catalog row and the `installed/<plugin_id>/` directory without requiring the modules to be loaded in the VM

### Requirement: Public service API

`Flezha.Plugin.Installer` SHALL expose exactly these public functions, each reachable from in-node callers (CLI via `Flezha.Service.CliManager`, future Phoenix UI, and test code): `install/2`, `upgrade/3`, `uninstall/2`, `enable/2`, `disable/2`, `list/0`, and `info/1`. All calls MUST be serialized through a single GenServer; concurrent invocations of mutating tasklets (`install`, `upgrade`, `uninstall`, `enable`, `disable`) MUST queue rather than race. Read-only tasklets (`list`, `info`) MAY be served without queuing but MUST observe the most recent successful write.

#### Scenario: Mutating tasklets are serialized
- **WHEN** two callers invoke `install/2` concurrently
- **THEN** exactly one install MUST be in flight at a time and the second MUST wait for the first to complete before starting

#### Scenario: list returns every catalog row
- **WHEN** `list/0` is called
- **THEN** the installer MUST return one element per catalog row with at least `id`, `version`, `status`, `description`, and `provides`

#### Scenario: info for unknown plugin
- **WHEN** `info/1` is called with a plugin id not in the catalog
- **THEN** the installer MUST return `{:error, :not_found}`

### Requirement: CLI integration

The `flz plugin` CLI SHALL expose the subcommands `install`, `upgrade`, `uninstall`, `list`, `info`, `enable`, and `disable`. `install`, `upgrade`, `uninstall`, `list`, and `info` SHALL invoke `Flezha.Service.CliManager` methods that wrap the corresponding `Flezha.Plugin.Installer` functions. `enable` and `disable` SHALL dispatch to the installer when the given plugin id matches an installed package and otherwise SHALL fall through to the pre-existing legacy code path. `flz plugin list` SHALL support a `--json` flag that prints the list as a JSON array.

#### Scenario: flz plugin install delegates to installer
- **WHEN** `flz plugin install /path/to/foo.fpz` is executed
- **THEN** the CLI MUST invoke `Flezha.Service.CliManager.install_plugin("/path/to/foo.fpz", _)` which calls `Flezha.Plugin.Installer.install/2` and returns the result to stdout

#### Scenario: flz plugin enable dispatches to installer for installed ids
- **WHEN** `flz plugin enable kafka_ingest` is executed and `kafka_ingest` is an installed plugin
- **THEN** the CLI MUST invoke `Flezha.Plugin.Installer.enable/2` rather than the legacy enable path

#### Scenario: flz plugin enable falls through for legacy ids
- **WHEN** `flz plugin enable some_legacy_id some_provider` is executed and `some_legacy_id` is NOT in the installed catalog
- **THEN** the CLI MUST invoke the pre-existing `Flezha.Service.CliManager.enable_plugin/4` behaviour

#### Scenario: flz plugin list --json produces machine-readable output
- **WHEN** `flz plugin list --json` is executed
- **THEN** stdout MUST contain a JSON array, each element a JSON object with at least `id`, `version`, `status`, and `description`

### Requirement: Configuration surface

The system SHALL expose a single config key `:flezha, :plugin_installer` as a keyword list with at least these entries and defaults: `plugin_root: "app_data/plugins"`, `trusted_keys: []`, `require_signature: false`, `allow_unsigned: true`, `max_package_bytes: 50_000_000`, `max_source_files: 200`. The installer SHALL validate this configuration at startup and SHALL refuse to start when validation fails.

#### Scenario: Default configuration starts successfully
- **WHEN** the agent starts with no `:plugin_installer` config overrides
- **THEN** `Flezha.Plugin.Installer` MUST start with the default values and MUST accept unsigned packages

#### Scenario: Inconsistent signature config rejected at startup
- **WHEN** config declares `require_signature: true` and `trusted_keys: []`
- **THEN** the Installer supervisor MUST fail to start with `{:error, {:config_invalid, :trusted_keys_empty}}` and the node MUST NOT complete startup

#### Scenario: Configurable package size limit enforced
- **WHEN** config declares `max_package_bytes: 1_000_000` and a 2 MB package is installed
- **THEN** the installer MUST abort extraction and return `{:error, {:package_too_large, _size, 1_000_000}}`

### Requirement: Supervision tree ordering

The Installer subsystem SHALL be started as a single supervisor subtree
(`Flezha.Plugin.Installer.Supervisor`) under `Flezha.Supervisor`, placed ahead of
`Flezha.Service.PluginManager` in `Flezha.Application.controller_children/0`. The
subtree SHALL start three children in strict `:rest_for_one` order:
`Flezha.Plugin.Installer.Store` (DETS opener), `Flezha.Plugin.Installer.Loader` (in-VM
registry, depends on Store), and `Flezha.Plugin.Installer` (service GenServer, depends
on Loader). The subtree is gated by `:flezha, :plugin_installer_enabled`, which
defaults to `false`; with the flag off no installer child is started and only
compiled-in plugins exist.

#### Scenario: Installer ready before any subscription replay
- **WHEN** the agent is starting with `plugin_installer_enabled: true`
- **THEN** every `:enabled` and `:disabled` plugin's modules MUST be loaded into the VM (or marked `:load_failed`) BEFORE `Flezha.Service.PluginManager` starts, so no operator bind can replay a subscription against a module that has not been loaded yet

#### Scenario: Loader init failure prevents Installer start
- **WHEN** `Loader.init/1` fails (e.g. because the Store is in a bad state)
- **THEN** `Flezha.Plugin.Installer` MUST NOT start and the supervisor MUST restart the subtree according to its strategy

### Requirement: Recovery interaction with installed plugins

The Installer SHALL publish the set of known installed plugin ids via a queryable
function (`Flezha.Plugin.Installer.installed_ids/0`). A plugin enable is persisted by
`Flezha.Service.PluginManager.enable_plugin/2` into the owning operator's ProcessHub
child spec (`opts[:plugins]`) and replayed when that operator binds:
`Flezha.Operator.Overwatch.init/1` → `handle_continue({:bind_runner, …})` →
`Flezha.Operator.Lifecycle.complete_init/4` → `Flezha.Operator.Plugins.prepare/2` →
`Flezha.Plugin.Extension.Table.bulk_subscribe/2`. `Flezha.Plugin.Installer.enable/2`
drives its own enables through `enable_provisions/3`, one `%Flezha.Plugin{}` per
manifest `provides` entry, so an installed package's subscriptions reach that same
persistence path as a compiled-in plugin's.

A persisted subscription whose provider module is not in the VM — the package was
uninstalled while the controller was down, or its beams are `:load_failed` — MUST NOT
be subscribed. `Flezha.Plugin.Extension.Table.run_init_provider/1` answers
`{:error, {:module_not_loaded, provider}}`; `bulk_subscribe/2` halts at that entry, so
entries after it in the same operator's list are not attempted; `Plugins.prepare/2`
fails the bind, and `Flezha.Operator.Lifecycle.park/2` records the reason in the
Overwatch's `init_error`, which `Flezha.Convergence.Probe` publishes as `parked?: true`.
The operator stays alive and parked, holding no upstream resources, until the
divergence is repaired. What the user sees is that boolean: neither the plugin id
nor the provider module reaches a log line or a telemetry event.

#### Scenario: Orphaned persisted subscription parks the operator
- **WHEN** an operator's persisted child spec carries a subscription for plugin id `x` whose `%Flezha.Plugin{}.provider` is `MyOrg.X`, and `MyOrg.X` is not loaded (`Installer.installed_ids/0` does not contain `x`)
- **THEN** `Table.bulk_subscribe/2` MUST answer `{:error, {"x", {:module_not_loaded, MyOrg.X}}}`, the entry MUST NOT be subscribed, entries after it MUST NOT be attempted, and the Overwatch MUST park with that reason in `init_error` (`Convergence.Probe` reports `parked?: true`)

#### Scenario: Installed plugin's persisted subscription re-subscribes at bind
- **WHEN** an operator's persisted child spec carries a subscription for plugin id `k` whose provider is loaded (the package is `:enabled` in the installer catalog)
- **THEN** `Flezha.Operator.Plugins.prepare/2` MUST subscribe that entry at bind exactly as it does a compiled-in plugin's, and `Plugins.activate/1` MUST then transition it to `:running`

### Requirement: Settings payload schema uses canonical names

The plugin settings payload (the JSON serialized at `cli_manager.ex` and on disk) SHALL use field names: `operator_id`, `runner_id`, `controller_id`. Fields previously named `processor_id`, `compute_engine_id`, or `agent_id` SHALL be renamed in the schema; reading a settings file with the legacy field names SHALL return an error.

#### Scenario: Modern settings round-trip
- **WHEN** a plugin is installed with payload referencing `operator_id` and `runner_id`
- **THEN** the on-disk settings file SHALL contain those exact field names; reloading the Controller MUST produce the same in-memory plugin state

#### Scenario: Legacy field names rejected
- **WHEN** an on-disk settings file contains a field named `processor_id` or `compute_engine_id`
- **THEN** the loader MUST return an error identifying the offending field; the user is expected to re-author the settings

### Requirement: Disabling an operator's plugins SHALL bound every call it makes and SHALL NOT exit

`PluginManager` SHALL treat a call into an operator's process as bounded and
failable: a timeout, a dead target or any other exit SHALL be a value the handler
acts on, and SHALL NOT terminate `PluginManager` itself. A teardown walking many
operators SHALL continue past one that cannot be reached, and SHALL report which
ones it could not disable.

The bound SHALL be applied through the one bounded-call seam rather than a
`catch` clause written at the site, and the inner bound SHALL stay strictly
smaller than the bound the caller of `PluginManager` applies, so the two do not
give up at the same instant.

A teardown that walks many operators SHALL fan out under one bound rather than
call them one at a time, so that the walk's cost is one bound and not one bound
per unreachable operator. The fan-out SHALL NOT relocate work that owns
caller-scoped state: a step that creates an ETS table, unlinks a process or
seeds a route naming the caller SHALL run in the calling process.

#### Scenario: An operator is too busy to answer its unsubscribe

- **WHEN** an operator's process does not answer `plugin_unsubscribe` inside the bound
- **THEN** `PluginManager` SHALL stay alive, SHALL record that operator as not
  disabled, and SHALL continue with the rest

#### Scenario: A teardown of many operators finishes its walk

- **WHEN** `stop_all_operators/1` disables plugins for every operator in a namespace
  and one of them cannot be reached
- **THEN** every other operator SHALL still be stopped and its state directory
  handled, and the walk SHALL NOT abandon the remainder

#### Scenario: The durable row and the in-memory table do not disagree on a failure

- **WHEN** disabling a plugin fails after the durable disable has been written
- **THEN** the two SHALL be reconciled rather than left in disagreement, so a
  restart does not read a plugin as both disabled and installed

#### Scenario: Many unreachable operators cost one bound, not one each

- **GIVEN** a namespace whose operators include several whose processes do not answer
- **WHEN** the namespace is torn down
- **THEN** the plugin-disable walk SHALL complete within one bound plus slack,
  SHALL name every operator it could not disable, and `PluginManager` SHALL be
  the same running process afterwards

### Requirement: An installed package's provisions SHALL be rows of the provider table

An installed package's `provides` entries SHALL be registered as rows of the
one provider table the enabled providers are read from, through the table's
own registration function, and SHALL be resolved by `flz plugin enable`,
`--plugins` decoding and `flz plugin list` exactly as a first-party provider is.
The installer SHALL NOT write the provider list into application configuration
or hold a second list of its own.

#### Scenario: An installed provider resolves like a first-party one

- **WHEN** a package whose manifest provides `acme_source` is installed and
  enabled, and `flz plugin enable my_acme acme_source …` is issued
- **THEN** the provider SHALL be resolved from the provider table, and the same
  row SHALL answer `flz plugin list` and a `--plugins` spec naming it

#### Scenario: Disable does not remove the row

- **WHEN** an installed package is disabled
- **THEN** its provider rows SHALL remain in the table and its modules loaded,
  and only its subscriptions SHALL be dropped

### Requirement: Hook names use canonical Operator vocabulary

Plugin installation SHALL accept hook specs that reference hooks by their canonical names. The init-state hook is `:on_operator_init` (formerly `:on_processor_init`); other hook names that previously contained "processor" SHALL likewise use "operator". A manifest provision SHALL declare its hooks under the `hooks` key. Settings serialization SHALL persist the new names; reading a settings file that contains `:on_processor_init` SHALL produce an explicit error suggesting the user re-author the settings (no silent translation).

#### Scenario: Plugin hooks on_operator_init
- **WHEN** a plugin provision is installed with `hooks: [{:on_operator_init, [position: 0]}]`
- **THEN** the installation MUST succeed and the persisted settings file SHALL serialize the hook name as `on_operator_init`

#### Scenario: A provision without a hooks key is refused
- **WHEN** a manifest provision declares its hooks under any key other than `hooks`, or declares none
- **THEN** installation MUST be refused as a manifest missing a required key, and nothing SHALL be enabled

#### Scenario: Settings file with legacy hook name fails loud
- **WHEN** the settings file contains a hook on `:on_processor_init`
- **THEN** plugin loading MUST return `{:error, {:unknown_hook, :on_processor_init}}` and SHALL NOT fall back to `:on_operator_init` silently

