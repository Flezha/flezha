# plugin-installation Specification

## Purpose

The plugin-installation capability gives Flezha plugins the lifecycle of Jenkins plugins: the release is immutable, a plugin is a prebuilt package kept as data beside it, and a change to the plugin set takes effect when the controller restarts. A plugin is an ordinary Mix project depending on `flezha`; `mix flezha.plugin.package` builds its package (`*.fpz`): the compiled `.beam` and `.app` files of the plugin and of every library the release does not ship, laid out like a release's `lib/`, with `manifest.exs` recording the Flezha, OTP and Elixir versions it was built with and the providers it declares, a `CHECKSUMS.sha256` file and an optional detached Ed25519 signature. `flz plugin install`, `uninstall` and `sync` run in a VM of their own and only place or remove verified archives in the plugin root (`app_data/plugins/` by default); `FLEZHA_PLUGINS` declares a node's list, which is synced before the controller starts. Once, at boot, before the supervision tree is built, `Flezha.Plugin.Installer.Loader` verifies every archive, checks its libraries and modules against the VM, loads its code all or nothing, consolidates again the protocols it implements, starts its library applications and registers its providers as rows of the provider table; a refused package is recorded with its error code and never stops the controller. The capability defines the package format, manifest schema, checksum and signature verification, the install, uninstall and sync verbs, the boot-time loading order and its refusals, the list of what each boot loaded, recovery interaction, and the configuration surface (the `:plugins` rows).
## Requirements
### Requirement: Plugin package format

A plugin package SHALL be a gzip-compressed tar archive with the conventional extension
`.fpz`. The archive MUST contain, at its root:

- `manifest.exs`: an Elixir term describing the package (see "Manifest schema")
- `CHECKSUMS.sha256`: one `<64-hex-char>  <relative-path>` line per file under `lib/` and a
  line for `manifest.exs`, in the GNU `sha256sum` output format
- `lib/`: one directory `<app>-<vsn>` per bundled application, each holding `ebin/` with the
  application's `<app>.app` and one `.beam` per module its `modules` list names, and
  optionally `priv/`

The archive MAY contain, at its root, `signature.sig`: a raw 64-byte Ed25519 signature over
the bytes of `CHECKSUMS.sha256`.

The archive MUST NOT contain any other top-level entry, any `.ex` or `.erl` source, a
symbolic link, a device file, an absolute path or a path containing `..`. It MUST NOT carry
native code: a file under any `priv/` ending in `.so`, `.dll` or `.dylib`, or a module whose
beam declares an `on_load` function. An `<app>-<vsn>` directory whose `.app` names another
application or version, or whose `ebin/` holds a `.beam` its `modules` list does not name or
lacks one it names, MUST be refused. An archive whose uncompressed size exceeds
`plugin_max_package_bytes` (default 50_000_000) MUST be refused before anything is written
outside `<plugin_root>/.staging/`. These checks SHALL run at install and each time a
controller boots with the archive.

#### Scenario: Well-formed package is accepted

- **WHEN** an archive holding `manifest.exs`, `CHECKSUMS.sha256` and a `lib/` tree of
  `<app>-<vsn>/ebin` directories whose beams match their `.app` files is installed
- **THEN** it SHALL pass the format checks

#### Scenario: A source package is refused

- **WHEN** the archive holds a `src/` directory or any `.ex` file
- **THEN** it SHALL be refused with `plugin.package_invalid` naming the entry, and nothing
  SHALL be placed in the plugin root

#### Scenario: Missing manifest rejected

- **WHEN** the archive lacks `manifest.exs` at its root
- **THEN** it SHALL be refused with `plugin.package_invalid` naming the missing manifest,
  and nothing SHALL be placed in the plugin root

#### Scenario: Missing checksums file rejected

- **WHEN** the archive lacks `CHECKSUMS.sha256` at its root
- **THEN** it SHALL be refused with `plugin.package_invalid` naming the missing checksum file

#### Scenario: Unknown top-level entries rejected

- **WHEN** the archive holds a top-level entry other than `manifest.exs`, `CHECKSUMS.sha256`,
  `signature.sig` or `lib/`
- **THEN** it SHALL be refused with `plugin.package_invalid` naming the entry

#### Scenario: Native code is refused

- **WHEN** the archive holds `lib/foo-1.0.0/priv/foo.so`, or a module with an `on_load`
  function
- **THEN** it SHALL be refused with `plugin.native_code` naming the file or module

#### Scenario: Beams and modules list disagree

- **WHEN** an `ebin/` holds a `.beam` its `.app` does not list, or its `.app` lists a module
  with no `.beam`
- **THEN** it SHALL be refused with `plugin.package_invalid` naming the application and the
  module

#### Scenario: Oversized package rejected

- **WHEN** the archive's uncompressed size exceeds `plugin_max_package_bytes`
- **THEN** extraction SHALL stop, `.staging/` SHALL hold nothing of it, and it SHALL be
  refused with `plugin.package_invalid` carrying the size and the limit

### Requirement: Manifest schema

`manifest.exs` SHALL be an Elixir term of shape `%{id: binary, version: binary, built_with:
%{flezha: binary, otp: binary, elixir: binary}, apps: [{atom, binary}], providers: %{binary
=> module}}` and MAY include `:description`. Every field carries these constraints:

- `id`: valid as a plugin id (`~r/^[a-z0-9][a-z0-9_]*$/`, at most 64 characters) and equal
  to the name of the first application in `apps`, the plugin application
- `version`: parseable by `Version.parse/1` and equal to the plugin application's `vsn`
- `built_with`: `flezha` and `elixir` parseable by `Version.parse/1`, `otp` a string of
  digits
- `apps`: non-empty; each `{app, vsn}` has exactly one `lib/<app>-<vsn>` directory, and no
  other directory exists under `lib/`
- `providers`: a non-empty map; each key valid as a plugin id, each value a module the
  plugin application's `.app` lists

The manifest MUST be parsed without evaluating code: its abstract syntax tree MUST contain
only literal nodes (maps, lists, tuples, atoms, binaries, numbers, module aliases), and a
manifest whose tree contains a function call or a variable MUST be refused without being
evaluated.

#### Scenario: Valid manifest parses

- **WHEN** `manifest.exs` is the map the package task writes for the example plugin
- **THEN** it SHALL be accepted

#### Scenario: Missing required field rejected

- **WHEN** `manifest.exs` lacks `:built_with`
- **THEN** the package SHALL be refused with `plugin.package_invalid` whose details name the
  missing field

#### Scenario: Invalid id rejected

- **WHEN** `manifest.exs` declares `id: "My-Plugin"` (uppercase and a hyphen)
- **THEN** the package SHALL be refused with `plugin.package_invalid` naming the id

#### Scenario: Non-semver version rejected

- **WHEN** `manifest.exs` declares `version: "beta-2"`, which `Version.parse/1` cannot parse
- **THEN** the package SHALL be refused with `plugin.package_invalid` naming the version

#### Scenario: Provider not in modules list rejected

- **WHEN** a `providers` value names a module the plugin application's `.app` does not list
- **THEN** the package SHALL be refused with `plugin.package_invalid` naming the provider id
  and the module

#### Scenario: Manifest containing code rejected

- **WHEN** `manifest.exs` contains a call expression such as `System.get_env("FOO")`
- **THEN** the package SHALL be refused with `plugin.package_invalid` WITHOUT evaluating the
  call

### Requirement: SHA-256 checksum verification

The SHA-256 digest of `manifest.exs` and of every file under `lib/` SHALL be recomputed and
compared byte-for-byte with the value `CHECKSUMS.sha256` declares, at install and each time a
controller boots with the archive. A mismatch, a missing line, and a line naming a path the
archive lacks MUST each refuse the package.

#### Scenario: All checksums match

- **WHEN** every file's recomputed SHA-256 matches its declared value
- **THEN** verification SHALL proceed to the next step

#### Scenario: Tampered beam rejected

- **WHEN** `lib/stamp_lib-0.1.0/ebin/Elixir.StampLib.beam` was changed after
  `CHECKSUMS.sha256` was written
- **THEN** the package SHALL be refused with `plugin.package_invalid` whose details name the
  checksum mismatch and the path, and no module of it SHALL be loaded

#### Scenario: Checksum line for missing file rejected

- **WHEN** `CHECKSUMS.sha256` lists a path the archive does not hold
- **THEN** the package SHALL be refused with `plugin.package_invalid` naming that path

#### Scenario: An archive changed on disk after install is refused at boot

- **GIVEN** an installed archive whose bytes are altered in the plugin root
- **WHEN** the controller boots
- **THEN** the package SHALL be refused, and the controller SHALL finish booting

#### Scenario: File not covered by checksums rejected

- **WHEN** a file under `lib/` has no line in `CHECKSUMS.sha256`
- **THEN** the package SHALL be refused naming that path

### Requirement: Ed25519 signature verification

When `signature.sig` is present, or `plugin_require_signature` is `true`, the signature SHALL
be verified over the bytes of `CHECKSUMS.sha256` with `:crypto.verify/5` and
`{:eddsa, :ed25519}`, and it SHALL be valid if and only if at least one key in
`plugin_trusted_keys` verifies it. A present signature that no trusted key verifies MUST be
refused whatever the setting. A missing signature MUST be refused when
`plugin_require_signature` is `true`, and SHALL otherwise be accepted with a warning log and
`signed: false` in the package's load outcome. With `plugin_require_signature` `true` and no
trusted key, every package MUST be refused with `plugin.trust_store_empty`, and the
controller SHALL still boot. Verification SHALL run at install and at every boot, so a key
removed from the trust store refuses the packages it signed at the next boot.

#### Scenario: Valid signature accepted

- **WHEN** the archive's `signature.sig` verifies against one of the trusted keys
- **THEN** verification SHALL proceed

#### Scenario: Invalid signature rejected

- **WHEN** the archive's `signature.sig` verifies against no trusted key
- **THEN** the package SHALL be refused with `plugin.signature_invalid`

#### Scenario: Unsigned package accepted in permissive mode

- **WHEN** the archive has no `signature.sig` and `plugin_require_signature` is `false`
- **THEN** the package SHALL be accepted, a warning SHALL be logged, and its load outcome
  SHALL carry `signed: false`

#### Scenario: Unsigned package rejected in required mode

- **WHEN** the archive has no `signature.sig` and `plugin_require_signature` is `true`
- **THEN** the package SHALL be refused with `plugin.signature_required`

#### Scenario: Required signatures with an empty trust store

- **WHEN** the controller boots with `plugin_require_signature: true` and
  `plugin_trusted_keys: []`
- **THEN** every package SHALL be refused with `plugin.trust_store_empty` and the
  controller SHALL finish booting

### Requirement: Module collision with compiled-in code

The loader SHALL refuse a package any of whose modules to load is already loaded in the VM or
found on its code path (`:code.which/1` is not `:non_existing`), whether the module belongs
to the release, to OTP or Elixir, or to a package loaded before it at this boot. The modules
of an application skipped because the VM already holds it at the same version are not
modules to load. A package SHALL never replace a module of the release.

#### Scenario: Installed module shadows built-in rejected

- **WHEN** a package's plugin application lists `Flezha.Plugin.Provider.Kafka`
- **THEN** the package SHALL be refused with `plugin.module_conflict` naming the module and
  the release as its holder, and the release's module SHALL be the one loaded

#### Scenario: Installed module shadows another plugin's module rejected

- **WHEN** packages `a` and `b` both list module `MyOrg.Shared` in applications of different
  names
- **THEN** `a` SHALL load and `b` SHALL be refused with `plugin.module_conflict` naming
  `MyOrg.Shared` and `a`

### Requirement: On-disk installed-plugin layout

The plugin root (`plugin_root`, `app_data/plugins` by default) SHALL hold the installed
packages as archives: every `*.fpz` file in it is one, whatever its name, and install and
sync SHALL name the archives they place `<id>.fpz`, where `<id>` is the manifest's `id`. An
install SHALL write under `<plugin_root>/.staging/` and rename into place, and SHALL remove
what it wrote there however it ends. At each boot the loader SHALL remove
`<plugin_root>/.loaded/` and expand every archive it accepts into
`<plugin_root>/.loaded/<id>/lib/`, and the VM SHALL read package code and `priv` files only
from there. Install, uninstall and sync SHALL NOT write under `.loaded/`. Entries of the
plugin root other than `*.fpz` files, `.loaded/` and `.staging/` SHALL be ignored.

#### Scenario: Canonical layout after install

- **WHEN** the example package is installed
- **THEN** `<plugin_root>/flezha_example_plugin.fpz` SHALL exist and `.staging/` SHALL hold
  nothing of the install

#### Scenario: Staging removed on failure

- **WHEN** any step of an install fails
- **THEN** `<plugin_root>/.staging/` SHALL hold nothing of that install when the command
  returns, and the plugin root SHALL be as it was

#### Scenario: An install on a running node leaves the running code's files alone

- **GIVEN** a running controller that loaded a package from `.loaded/`
- **WHEN** a new version of it is installed
- **THEN** no file under `.loaded/` SHALL change until the next boot

#### Scenario: Leftovers of the old installer are ignored

- **GIVEN** a plugin root holding an `installed/` tree and `installed_plugins.dets`
- **WHEN** the controller boots
- **THEN** it SHALL read neither and SHALL load only the `*.fpz` archives beside them

### Requirement: Boot-time module loading

The controller SHALL load packages once per boot, inside `Flezha.Application`'s controller
start, after the state root is claimed and before the supervision tree's children are
computed, as plain function calls owning no process. It SHALL take the archives in id order
and put each through, in this order: verification (format, checksums, signature, manifest,
runtime line, and an id no other archive in the root declares); expansion into `.loaded/`;
the library and module checks; appending its `ebin` directories to the code path with
`:code.add_pathsz/1`; `:application.load/1` of each application to load; `:code.atomic_load/1`
of all their modules; protocol consolidation; the provider check (every provider module
answers a valid `__provider__/0` and its id is no row yet); starting its library
applications; registering its providers.

A package that fails a step SHALL be refused: its code path entries SHALL be removed, its
application specifications unloaded, the applications it started stopped, and no provider
row of it registered. Its modules loaded before the failing step SHALL stay loaded and
unreferenced until the next restart; nothing SHALL purge or replace a loaded module. One
package's refusal SHALL NOT affect another's, and the controller SHALL finish booting. A
runner-only boot (`controller_enabled: false`) SHALL load no package.

#### Scenario: A package's modules are loaded before any subscription replay

- **GIVEN** an operator whose persisted child spec enables the example package's provider
- **WHEN** the controller restarts
- **THEN** the provider module SHALL be loaded and its row registered before
  `Flezha.Service.PluginManager` starts, and the operator's bind SHALL subscribe the plugin

#### Scenario: Loading is all or nothing

- **WHEN** `:code.atomic_load/1` refuses any module of a package
- **THEN** no module of that package SHALL be loaded, its code path entries SHALL be gone,
  it SHALL be refused with `plugin.load_failed`, and the other packages SHALL load

#### Scenario: Two archives declaring one id

- **WHEN** the plugin root holds two archives whose manifests declare the same `id`
- **THEN** both SHALL be refused with `plugin.duplicate_id` naming the two files

### Requirement: Uninstall lifecycle

`flz plugin uninstall <id>` SHALL remove every archive in the plugin root whose manifest
declares `<id>` and print that the package unloads at the next restart of the controller. It
SHALL NOT touch `.loaded/` or the running VM. An id with no archive SHALL be refused with `plugin.not_installed`, and a node whose
list is declared SHALL refuse it with `plugin.list_declared`. After the next restart,
operators bound to plugins of the package's providers run without them, each binding
reported not started (see "Recovery interaction with installed plugins").

#### Scenario: Uninstall removes the archive, the running controller keeps the package

- **GIVEN** a running controller with the example package loaded
- **WHEN** `flz plugin uninstall flezha_example_plugin` runs
- **THEN** the archive SHALL be gone, `flz plugin list --installed` SHALL list the package
  `removed`, its provider SHALL still be enable-able until the restart, and after the
  restart no row of it SHALL exist

#### Scenario: Uninstall of an unknown id

- **WHEN** `flz plugin uninstall ghost` runs and no `ghost.fpz` exists
- **THEN** it SHALL be refused with `plugin.not_installed`

### Requirement: Public service API

`Flezha.Plugin.Installer` SHALL expose `install/2`, `uninstall/2`, `sync/2` and `list/0` as
plain functions owning no process. `install/2`, `uninstall/2` and `sync/2` act on the plugin
root alone and SHALL have the same effect from any VM running the release: the CLI's own VM,
the controller, or a later user interface running in the controller. `list/0` SHALL answer
the rows of "A refused package SHALL NOT stop the controller, and SHALL say why" from the
running controller's load outcomes and its plugin root. `Flezha.Service.Fleet.installed_plugins/1`
SHALL answer the same rows.

#### Scenario: The same install from two VMs

- **WHEN** the example archive is installed once through `flz plugin install` and once by
  calling `Flezha.Plugin.Installer.install/2` inside a running controller
- **THEN** both SHALL leave the same `<plugin_root>/flezha_example_plugin.fpz` and the same
  answer

#### Scenario: list answers loaded and pending rows

- **GIVEN** a controller that loaded package `a`, and package `b` installed after it booted
- **WHEN** `list/0` is called
- **THEN** it SHALL answer `a` as `loaded` and `b` as `pending`

### Requirement: CLI integration

The `flz plugin` CLI SHALL expose the subcommands `install <path-or-https-url>`,
`uninstall <id>`, `sync`, `list [--installed] [--json]`, `enable`, `disable` and `help`.
`install`, `uninstall` and `sync` SHALL run `flzvm eval` on the release and SHALL NOT reach a
running controller; they SHALL carry no token, print the absolute plugin root they acted on,
and exit with the exit code of their answer's class. `list`, `enable`, `disable` and `help`
SHALL remain requests to the running controller. `enable` and `disable` SHALL go to the
plugin manager directly, with no installed-package probe. There SHALL be no `upgrade` and no
`info` subcommand: an upgrade is an install of a newer archive of the same id.

#### Scenario: flz plugin install runs without a controller

- **WHEN** `flz plugin install /path/to/p.fpz` runs and no controller is running
- **THEN** the archive SHALL be verified and placed and the command SHALL exit 0

#### Scenario: flz plugin list --json produces machine-readable output

- **WHEN** `flz plugin list --installed --json` is executed
- **THEN** stdout SHALL contain a JSON array, each element an object with at least `id`,
  `version`, `status`, `providers` and `error`

#### Scenario: The removed subcommands are unknown

- **WHEN** `flz plugin upgrade p ./p.fpz` or `flz plugin info p` is executed
- **THEN** the CLI SHALL refuse it as an unknown subcommand and SHALL start no `flzvm`

### Requirement: Configuration surface

The system SHALL read the plugin settings from the `:flezha, :plugins` keyword list and from
the environment: `plugin_root` (`FLEZHA_PLUGIN_ROOT`, default `"app_data/plugins"`),
`plugin_trusted_keys` (`FLEZHA_PLUGIN_TRUSTED_KEYS`, comma-separated PEM file paths, default
`[]`), `plugin_require_signature` (`FLEZHA_PLUGIN_REQUIRE_SIGNATURE`, default `false`),
`plugin_max_package_bytes` (`FLEZHA_PLUGIN_MAX_PACKAGE_BYTES`, default `50_000_000`) and
`plugin_list` (`FLEZHA_PLUGINS`, unset by default). No setting SHALL gate loading: an archive
in the plugin root is the opt-in. There SHALL be no `plugin_installer_enabled`,
`plugin_allow_unsigned`, `plugin_max_source_files` or `plugin_installer_data_dir` setting and
no `installer_auto_save_ms` duration.

#### Scenario: Default configuration starts successfully

- **WHEN** a controller starts with no plugin setting and one valid unsigned archive in
  `app_data/plugins`
- **THEN** the package SHALL load and a warning SHALL name it unsigned

#### Scenario: Configurable package size limit enforced

- **WHEN** `plugin_max_package_bytes` is `1_000_000` and a 2 MB package is installed
- **THEN** it SHALL be refused with `plugin.package_invalid` carrying the size and the limit

### Requirement: Recovery interaction with installed plugins

A package provider's binding SHALL reach the same bring-up path as a first-party one's. A
binding is written by `Flezha.Service.PluginManager.bind/3` into the owning operator's
declared entry (`opts[:bindings]`) and brought up when that operator binds:
`Flezha.Operator.Overwatch.init/1` → `handle_continue({:bind_runner, …})` →
`Flezha.Operator.Lifecycle.complete_init/4` → `Flezha.Operator.Plugins.prepare/2` →
`Flezha.Plugin.Extension.Table.bulk_subscribe/2`.

A binding whose provider module is not loaded (its package was refused at this boot, was
uninstalled, or was never installed on this node) MUST NOT be subscribed and MUST NOT park
the operator. `Plugins.prepare/2` SHALL record the binding as not started in the operator's
extension table with the reason `plugin.provider_not_loaded`, naming the binding, the
provider module and this controller and, when an archive in this node's plugin root declares
that module, the package id and its status (refused with its code, or pending); the bind SHALL
go on, and one warning SHALL be logged with that reason. The operator runs without the plugin,
holding no upstream resources for it, and its health reports `plugin_unhealthy` for the
binding until a later bring-up on a controller that loaded the package subscribes it as for a
first-party provider.

#### Scenario: An orphaned binding leaves the operator running

- **GIVEN** an operator whose declared entry carries a binding to plugin `s` whose provider
  is `FlezhaExamplePlugin.Stamp`, and the example package was uninstalled before the restart
- **WHEN** the controller restarts
- **THEN** `s` SHALL NOT be subscribed, the operator SHALL bind and process its inputs, the
  warning naming `s`, `FlezhaExamplePlugin.Stamp` and that no installed package provides it
  SHALL be logged once, and `Convergence.Probe` SHALL report the operator `plugin_unhealthy`
  for `s` with `plugin.provider_not_loaded`, not parked

#### Scenario: Installed plugin's binding subscribes at bind

- **GIVEN** the same operator and the example package loaded at this boot
- **WHEN** the operator binds
- **THEN** `Flezha.Operator.Plugins.prepare/2` SHALL subscribe `s` exactly as a first-party
  plugin, and `Plugins.bring_up/1` SHALL then transition it to `:running`

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

The bound SHALL be applied through the one steps-call seam rather than a
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

A loaded package's providers SHALL be registered as rows of the one provider table, through
`Flezha.Plugin.Providers.add/2`, only at boot and before the supervision tree is built, and
SHALL be resolved by `flz plugin enable`, `--plugins` decoding, `flz plugin list` and
`flz plugin help` exactly as a first-party provider is. `add/2` SHALL refuse an id that is
already a row, which refuses the package with `plugin.provider_conflict`. The table's worker
families SHALL be derived from every row, so a package provider that declares a worker is
supervised through its own `Flezha.Plugin.Provider.WorkerSup` family as a first-party
provider is. The installer SHALL NOT write the provider list into application configuration
or hold a second list of its own.

#### Scenario: An installed provider resolves like a first-party one

- **WHEN** the example package is loaded and `flz plugin enable s example_stamp --operator_id
  op` is issued
- **THEN** the provider SHALL be resolved from the provider table, and the same row SHALL
  answer `flz plugin list`, `flz plugin help example_stamp` and a `--plugins` spec naming it

#### Scenario: A package provider id that is already a row is refused

- **WHEN** a package declares provider id `operator_stdout`
- **THEN** the package SHALL be refused with `plugin.provider_conflict`, and
  `operator_stdout` SHALL still resolve to the first-party module

#### Scenario: A package provider's worker is supervised

- **WHEN** the example package is loaded and its provider is enabled on an operator
- **THEN** its worker SHALL run under the provider's `WorkerSup` family in the controller's
  supervision tree, and a killed worker SHALL be handled by its declared restart value

### Requirement: Hook names use canonical Operator vocabulary

A plugin enable SHALL accept hook specs that reference hooks by their canonical names. The
init-state hook is `:on_operator_init` (formerly `:on_processor_init`); other hook names that
previously contained "processor" SHALL likewise use "operator". Settings serialization SHALL
persist the new names; reading a settings file that contains `:on_processor_init` SHALL
produce an explicit error suggesting the user re-author the settings (no silent translation).

#### Scenario: Plugin hooks on_operator_init

- **WHEN** a plugin is enabled with the hook `on_operator_init`
- **THEN** the enable MUST succeed and the persisted settings SHALL serialize the hook name as
  `on_operator_init`

#### Scenario: Settings file with legacy hook name fails loud

- **WHEN** the settings file contains a hook on `:on_processor_init`
- **THEN** plugin loading MUST return `{:error, {:unknown_hook, :on_processor_init}}` and
  SHALL NOT fall back to `:on_operator_init` silently

### Requirement: A plugin package SHALL be built by `mix flezha.plugin.package`

Flezha SHALL provide the Mix task `mix flezha.plugin.package`, available to every project
that depends on `flezha`. Run in a plugin project, it SHALL compile the project and write
`<id>-<version>.fpz` (into `--output`, `_build/` by default) holding the plugin
application and every application of its runtime closure that the target release does not
ship, where the target release's set is the runtime closure of `:flezha` plus `sasl` and
`iex`. It SHALL record in the manifest the Flezha version the project compiled against,
`System.otp_release()` and `System.version()`.

The task SHALL refuse, naming each offender and writing no archive:

- an application the release ships that the project resolved at a version other than the one
  in Flezha's own `mix.lock`;
- native code in any bundled application (see "Plugin package format");
- a bundled application that depends on `:flezha`, directly or transitively, and declares an
  application callback (`mod`);
- a plugin application whose `:flezha_providers` environment entry is missing, is not a map
  of provider id to module, names a module the plugin application does not list, or names a
  module whose `__provider__/0` does not answer a valid provider declaration.

With `--sign <path>` naming an Ed25519 private key in PEM form, the task SHALL write
`signature.sig` over `CHECKSUMS.sha256`. The controller SHALL NOT compile any plugin code.

#### Scenario: The example plugin is packaged

- **WHEN** `mix flezha.plugin.package` runs in `examples/elixir/plugin/` with `MIX_ENV=prod`
- **THEN** an archive SHALL be written whose `lib/` holds `flezha_example_plugin-<vsn>` and
  `stamp_lib-<vsn>` and no application of Flezha's runtime closure, and whose manifest
  records the task's own Flezha, OTP and Elixir versions and the plugin's providers

#### Scenario: A library pinned away from Flezha's lock is refused at package time

- **GIVEN** a plugin project whose lock resolves `jason` to a version other than Flezha's
  `mix.lock`
- **WHEN** the task runs
- **THEN** it SHALL exit non-zero naming `jason`, both versions and how to pin it, and SHALL
  write no archive

#### Scenario: A Flezha-dependent application with a callback is refused

- **GIVEN** a plugin application that declares `mod: {MyPlugin.Application, []}`
- **WHEN** the task runs
- **THEN** it SHALL exit non-zero naming the application and stating that a plugin's
  processes are its providers' declared workers

#### Scenario: A signed package

- **WHEN** the task runs with `--sign key.pem`
- **THEN** the archive SHALL hold a 64-byte `signature.sig` that the matching public key
  verifies over the archive's `CHECKSUMS.sha256`

### Requirement: A package SHALL load only on the runtime line it was built with

A package SHALL be accepted, at install and at boot, only when all three hold against the
running release:

- **OTP:** the running OTP release is the manifest's `built_with.otp` or one of the two
  releases after it;
- **Elixir:** the running Elixir has the same major as `built_with.elixir` and a
  `major.minor` not older than its;
- **Flezha:** the running Flezha is not older than `built_with.flezha` and, when that
  version's major is `0`, has the same minor, and otherwise has the same major.

A package accepted on an Elixir whose minor differs from `built_with.elixir`'s SHALL be loaded
and SHALL produce one warning naming the package and both Elixir versions, logged at boot and
printed by `flz plugin install`. A refusal SHALL be `plugin.runtime_mismatch` with a `rule`
detail naming the rule that failed. The comparison SHALL be made against the running release,
never against a constant. The operating system and the processor architecture SHALL NOT be
recorded or compared.

#### Scenario: A package built on another OTP is refused

- **GIVEN** a package whose `built_with.otp` is `"29"` and a controller running on OTP 28
- **WHEN** it is installed, or the controller boots with it in the plugin root
- **THEN** it SHALL be refused with `plugin.runtime_mismatch`, the details naming both
  runtimes and the rule `otp_newer`, and the hint naming the builder that matches the
  controller

#### Scenario: A package built two OTP releases back loads

- **GIVEN** a package whose `built_with.otp` is `"27"` and a controller running on OTP 29
- **WHEN** the controller boots
- **THEN** the package SHALL load

#### Scenario: A package built three OTP releases back is refused

- **GIVEN** a package whose `built_with.otp` is `"26"` and a controller running on OTP 29
- **WHEN** it is installed
- **THEN** it SHALL be refused with `plugin.runtime_mismatch` and the rule `otp_too_old`

#### Scenario: A package built on an older Elixir minor loads with a warning

- **GIVEN** a package built on Elixir `1.19.4` and a controller running Elixir `1.20.2`
- **WHEN** the controller boots
- **THEN** the package SHALL load and the log SHALL hold one warning naming the package,
  `1.19.4` and `1.20.2`

#### Scenario: A package built on a newer Elixir is refused

- **GIVEN** a package built on Elixir `1.21.0` and a controller running Elixir `1.20.2`
- **WHEN** it is installed
- **THEN** it SHALL be refused with `plugin.runtime_mismatch` and the rule `elixir_newer`

#### Scenario: A package built for an older patch of the same Flezha line loads

- **GIVEN** a package built with Flezha `0.1.0` and a controller running Flezha `0.1.3` on
  the same OTP release and Elixir minor
- **WHEN** the controller boots
- **THEN** the package SHALL load

#### Scenario: Before 1.0 a Flezha minor needs a rebuild

- **GIVEN** a package built with Flezha `0.1.0` and a controller running Flezha `0.2.0`
- **WHEN** it is installed
- **THEN** it SHALL be refused with `plugin.runtime_mismatch` and the rule `flezha_line`

#### Scenario: From 1.0 a Flezha major holds

- **GIVEN** a package built with Flezha `1.2.0`
- **WHEN** it is checked against Flezha `1.5.1`, and then against `2.0.0`
- **THEN** the first SHALL be accepted and the second refused with the rule `flezha_line`

### Requirement: A VM SHALL hold one version of each library

For each application a package bundles, the loader SHALL compare it with any application of
the same name already loaded in the VM, by the release or by a package loaded before it at
this boot. The same `vsn` SHALL mean the package's copy is skipped and the loaded one used;
a different `vsn` SHALL refuse the package with `plugin.library_conflict`, naming the
application, both versions and whether the release or which package holds the loaded one.
Install SHALL apply the same comparison against the release and against the manifests of
the archives already installed.

#### Scenario: A package bundling another version of a release library is refused

- **GIVEN** a package that bundles `hpax` at a version other than the release's
- **WHEN** the controller boots
- **THEN** the package SHALL be refused with `plugin.library_conflict` naming `hpax`, both
  versions and the release as the holder, and no module of the package SHALL be loaded

#### Scenario: Two packages sharing a library at one version both load

- **GIVEN** two packages that each bundle `stamp_lib` at the same version
- **WHEN** the controller boots
- **THEN** both SHALL load, `stamp_lib`'s modules SHALL be loaded once, from the package
  first in id order, and the second package's copy SHALL be skipped

### Requirement: A package's library applications SHALL start at boot, and an application that depends on Flezha SHALL NOT

The loader SHALL start every loaded application of a package that does not depend on
`:flezha` with `Application.ensure_all_started/2` and restart type `:temporary`, in
dependency order, before the package's providers are registered and before the supervision
tree is built. An application that depends on `:flezha`, the plugin application included,
SHALL be loaded and SHALL NOT be started; a package in which such an application declares
`mod` SHALL be refused with `plugin.app_callback`. A package whose library application fails
to start SHALL be refused, and the applications it started SHALL be stopped.

#### Scenario: A library application is running before operators bind

- **WHEN** the controller boots with the example package
- **THEN** `stamp_lib` SHALL be in `Application.started_applications/0` before
  `Flezha.Service.PluginManager` starts, and `flezha_example_plugin` SHALL be loaded and
  not started

#### Scenario: A library that fails to start refuses its package

- **GIVEN** a package whose library application's `start/2` returns an error
- **WHEN** the controller boots
- **THEN** the package SHALL be refused with `plugin.load_failed` naming the application and
  its reason, no provider row of it SHALL exist, and the controller SHALL finish booting

### Requirement: Protocols the release consolidated SHALL dispatch a package's implementations

When a package implements a protocol it does not define, the loader SHALL consolidate that
protocol again after the package's modules are loaded: the types SHALL be the protocol's
current consolidated types plus the package's, the result SHALL be loaded in place of the
protocol module, and every implementation the protocol dispatched before SHALL still
dispatch. The implementations a package brings SHALL be read from its beams' `__impl__`
attributes. Protocols a package defines itself SHALL stay unconsolidated. The release SHALL
ship its consolidated protocol beams with their debug info so that this consolidation can
read them (see `controller-packaging`). A consolidation that fails SHALL refuse the package
with `plugin.protocol_consolidation_failed`.

#### Scenario: A release protocol dispatches a package's struct

- **GIVEN** the released controller booted with the example package, whose `stamp_lib`
  implements `String.Chars` for its struct
- **WHEN** `to_string/1` is called on that struct inside the controller
- **THEN** the package's implementation SHALL answer, and `to_string/1` of an atom and of an
  integer SHALL answer as before

#### Scenario: Two packages implement the same release protocol

- **GIVEN** two packages that each implement `String.Chars` for a struct of their own
- **WHEN** the controller boots
- **THEN** both structs SHALL dispatch to their own implementation

### Requirement: A refused package SHALL NOT stop the controller, and SHALL say why

The controller SHALL finish booting whatever the packages in its plugin root do. Every
refusal SHALL carry an error with a `code`, a `class`, a plain-language `message`,
structured `details` and, where one exists, a `hint` naming the next step. The codes SHALL
be `plugin.package_invalid`, `plugin.native_code`, `plugin.app_callback`,
`plugin.signature_invalid`, `plugin.signature_required`, `plugin.trust_store_empty`,
`plugin.runtime_mismatch`, `plugin.library_conflict`, `plugin.module_conflict`,
`plugin.provider_conflict`, `plugin.duplicate_id`, `plugin.load_failed` and
`plugin.protocol_consolidation_failed`.

`flz plugin list --installed` SHALL show one row per package id with a status:

- `loaded`: loaded at this boot, with its version and providers;
- `refused`: present at this boot and not loaded, with its version, code, message and hint;
- `pending`: an archive placed or replaced since this boot, with "loads at next restart"
  and the loaded version when one is loaded;
- `removed`: loaded at this boot and its archive since removed, with "unloads at next
  restart".

`--json` SHALL print the rows as a JSON array whose elements carry `id`, `version`,
`status`, `providers` and `error` (`null`, or the object `{code, class, message, details,
hint}`). `flz plugin list` without `--installed` SHALL list only providers that can be
enabled and, when any package was refused, SHALL end with one line naming how many and
pointing at `flz plugin list --installed`. `flz plugin enable` naming a provider id that a
refused package declares SHALL answer `plugin.provider_unavailable`, naming the package and
its refusal code, and SHALL NOT answer that the provider is unknown.

A loaded package's provider refusal whose reason is a tuple the error catalogue maps by its
tag SHALL keep the code the catalogue gives it (a bare atom such as `:closed` SHALL be wrapped
as below), on the enable path and when the refusal is nested in an
operator's park reason. A refusal whose reason the catalogue does not map SHALL be wrapped: a
provider that refuses its options in `init_provider/1` SHALL answer `plugin.provider_refused`
(invalid), and one whose declared worker fails to start SHALL answer
`plugin.provider_failed` (internal), each with the package id, the provider id and the
package's own text in `details`, including when the refusal is nested in an operator's park
reason. A package SHALL NOT add codes to the error catalogue at boot.

#### Scenario: A refused package is listed with its reason

- **GIVEN** a controller that booted with one loadable package and one bundling a
  conflicting library
- **WHEN** `flz plugin list --installed --json` runs
- **THEN** the array SHALL hold the first with `status: "loaded"` and `error: null`, and the
  second with `status: "refused"` and `error.code: "plugin.library_conflict"`, and the
  controller SHALL report ready

#### Scenario: Enabling a refused package's provider names the package

- **GIVEN** package `acme` declaring provider `acme_source` was refused at boot
- **WHEN** `flz plugin enable src acme_source --operator_id op` runs
- **THEN** it SHALL fail with `plugin.provider_unavailable`, naming `acme` and its refusal
  code

#### Scenario: A package provider's own refusal carries a core code

- **GIVEN** the example package loaded, whose provider refuses an empty `path` option
- **WHEN** its provider is enabled on an operator with `--Xpath=` (empty)
- **THEN** it SHALL fail with `plugin.provider_refused`, and `details` SHALL name the package,
  the provider and the provider's own message

#### Scenario: A package provider's catalogued refusal keeps its code

- **GIVEN** a loaded package whose provider's `init_provider/1` refuses with a tagged reason
  that the error catalogue maps by its tag, as
  `{:partitioned_host, %{provider: name, operator: member}}` maps to
  `plugin.partitioned_host`
- **WHEN** its provider is enabled on an operator, and separately declared by an operator
  that is then started
- **THEN** the enable SHALL fail with the catalogue's code for that tag, not
  `plugin.provider_refused`, and the started operator's park reason SHALL carry the same code

#### Scenario: A package installed after boot is pending

- **GIVEN** a running controller
- **WHEN** a package is installed and `flz plugin list --installed` runs before a restart
- **THEN** the package SHALL be listed `pending` with "loads at next restart", and its
  providers SHALL NOT be enable-able until the controller restarts

### Requirement: Install SHALL place a verified archive and take effect at the next restart

`flz plugin install <source>` SHALL accept a local path or an `https://` URL and SHALL
refuse any other scheme. A URL SHALL be fetched with peer verification against the
operating system's CA store, following at most five redirects, each to `https://`, and
SHALL be abandoned once it exceeds `plugin_max_package_bytes`. The archive SHALL be written
under `<plugin_root>/.staging/`, verified with every check the boot applies that does not
need the package loaded (format, checksums, signature, manifest, runtime line, library
versions against the release and the installed manifests, provider ids against the
release's and the installed manifests'), and renamed to `<plugin_root>/<id>.fpz`, after
which every other archive whose manifest declares the same id SHALL be removed. It SHALL NOT change the running VM, and SHALL print the id, the
version, the absolute plugin root and that a restart of the controller loads it. A refused
install SHALL leave the plugin root as it was.

#### Scenario: Install with no controller running

- **GIVEN** a release that has never been started
- **WHEN** `flz plugin install ./flezha_example_plugin-0.1.0.fpz` runs
- **THEN** `<plugin_root>/flezha_example_plugin.fpz` SHALL exist, the command SHALL exit 0
  printing the absolute plugin root, and the next start SHALL load the package

#### Scenario: Upgrade is an install of the same id

- **GIVEN** `flezha_example_plugin` 0.1.0 is installed and loaded
- **WHEN** its 0.2.0 archive is installed
- **THEN** `<plugin_root>/flezha_example_plugin.fpz` SHALL be the 0.2.0 archive, the running
  controller SHALL still run 0.1.0 and list the package `pending` with the loaded version
  0.1.0, and after a restart 0.2.0 SHALL be loaded

#### Scenario: A plain HTTP source is refused

- **WHEN** `flz plugin install http://host/p.fpz` runs
- **THEN** it SHALL be refused naming the scheme, and nothing SHALL be fetched or written

#### Scenario: A download larger than the limit is abandoned

- **GIVEN** `FLEZHA_PLUGIN_MAX_PACKAGE_BYTES=1000000` and an HTTPS origin serving 2 MB
- **WHEN** the URL is installed
- **THEN** the fetch SHALL stop past the limit, `.staging/` SHALL hold nothing of it, and
  the install SHALL be refused with `plugin.package_invalid`

### Requirement: A declared plugin list SHALL be synced before the controller starts

When `FLEZHA_PLUGINS` is set, it SHALL declare the node's packages: entries separated by
whitespace or commas, each a local path or an `https://` URL, optionally ending with
`#sha256=<64 hex>`, the digest of the archive. `flz plugin sync` SHALL make the plugin root
hold exactly the declared packages: an entry whose pinned archive is already present SHALL
be kept without a fetch, every other entry SHALL be fetched, verified as an install is and
placed, a pinned entry whose fetched archive has another digest SHALL be refused, and every
archive no entry produced SHALL be removed. A sync that fails for any entry SHALL exit
non-zero and SHALL leave the plugin root as it was. While `FLEZHA_PLUGINS` is set, even to
an empty value, `flz plugin install` and `flz plugin uninstall` SHALL be refused with
`plugin.list_declared` naming the variable. `flz plugin sync` with the variable unset SHALL
be refused naming it.

#### Scenario: Sync converges the plugin root

- **GIVEN** a plugin root holding archives `a` and `b`, and `FLEZHA_PLUGINS` naming `b`
  (pinned, present) and `c`
- **WHEN** `flz plugin sync` runs
- **THEN** the plugin root SHALL hold `b` (not fetched again) and `c`, and not `a`

#### Scenario: A failed entry changes nothing

- **GIVEN** `FLEZHA_PLUGINS` naming a present archive and an unreachable URL
- **WHEN** `flz plugin sync` runs
- **THEN** it SHALL exit non-zero with `plugin.fetch_failed` naming the URL, and the plugin
  root SHALL be unchanged

#### Scenario: Install is refused on a node whose list is declared

- **GIVEN** `FLEZHA_PLUGINS` is set
- **WHEN** `flz plugin install ./p.fpz` runs
- **THEN** it SHALL be refused with `plugin.list_declared`, and the plugin root SHALL be
  unchanged

### Requirement: Every boot SHALL record what it loaded

After the supervision tree has started, the controller SHALL execute
`[:flezha, :plugin_package, :loaded]` for every package it loaded, with metadata `id`,
`version`, `digest` (the archive's SHA-256), `signed` and `providers`, and
`[:flezha, :plugin_package, :refused]` for every package it refused, with `id`, `version`
when known, `digest` and the error's `code` and `message`. Both SHALL be bridged to the
observability bus under the source `:flezha_plugin_packages` and SHALL reach the audit log.
Each package SHALL also be logged when it is decided, at `info` when loaded and at `error`
when refused.

#### Scenario: A boot's packages reach the audit log

- **GIVEN** the audit log enabled and a plugin root with one loadable and one refused
  package
- **WHEN** the controller boots
- **THEN** `audit-current.jsonl` SHALL hold one `flezha_plugin_packages` record named
  `loaded` and one named `refused` carrying the refusal code

### Requirement: An enable SHALL name the controllers that lack the provider

When `flz plugin enable` resolves a provider, it SHALL ask every other controller that holds
a hub for the target namespace whether its provider table has that id, bounded in time, and
SHALL print a warning naming each controller that does not, or that did not answer, and
stating that the plugin will not run there and that an operator bound to it there runs
without it. The enable SHALL proceed. No placement constraint SHALL be derived from it.

#### Scenario: One controller of the namespace lacks the package

- **GIVEN** two controllers holding hubs for namespace `n`, only the first with the example
  package loaded
- **WHEN** `flz plugin enable s example_stamp --namespace n` runs against the first
- **THEN** the enable SHALL succeed and SHALL print a warning naming the second controller
  and saying the plugin will not run there

### Requirement: A package SHALL carry the settings of the libraries it bundles

`mix flezha.plugin.package` SHALL write the plugin project's configuration of every library
application it bundles, as the `MIX_ENV` the task runs in loads it, into the `env` of that
application's `.app` in the archive, over the library's own defaults, written as Mix writes
an `.app` so that a value beyond ASCII reads back unchanged, before the checksums are
computed. A setting an `.app` file cannot hold (a function other than `&Mod.fun/arity`, a pid
or a reference) SHALL fail the task naming the application and the key. The plugin application's own configuration and the configuration of any
application the controller ships SHALL NOT be written. At load, a bundled library SHALL read
these settings as its defaults.

#### Scenario: A library configured by the plugin project

- **GIVEN** a plugin project whose `config/config.exs` sets `config :stamp_lib, prefix: ">>"`
  and which bundles `stamp_lib`
- **WHEN** it is packaged, installed and the controller boots
- **THEN** `Application.get_env(:stamp_lib, :prefix)` on the controller SHALL answer `">>"`

#### Scenario: A function in a library setting

- **WHEN** the plugin project configures a bundled library with an anonymous function
- **THEN** the package task SHALL fail naming the library and the key

#### Scenario: A shipped library is not reconfigured

- **WHEN** the plugin project configures `:jason`, which the controller ships
- **THEN** the archive SHALL hold no `.app` for `jason`, and the controller's `jason` settings
  SHALL be unchanged after boot

