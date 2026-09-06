# cli-runtime-data-reset Specification

## Purpose

The cli-runtime-data-reset capability defines a top-level `flz reset` CLI subcommand that clears the Controller's durable runtime state on disk (per-Entity WAL/snapshot tree, ProcessHub durable registry; optionally certs and sockets) so an operator can deterministically start fresh from a clean slate. The capability owns the filesystem walk (clear *contents* of well-known `app_data/` subdirectories, never the parent itself), a hard live-node guard that refuses to run while the Controller is up, idempotent semantics over missing directories, scriptable flags (`--yes`, `--dry-run`, `--include-certs`, `--include-sockets`, `--all`, `--app-data-root`, `--json`), the audit-trail output enumerating cleared paths and byte counts, and the stable exit-code contract that lets scripts distinguish "refused" from "bad usage" from "I/O error".
## Requirements
### Requirement: `flz reset` clears Controller durable runtime data on disk

The CLI SHALL provide a top-level `flz reset` subcommand that clears the Controller's durable runtime state so an operator can deterministically start fresh. By default the command SHALL clear the contents of:

- `app_data/state/` — per-Entity WAL/snapshot/outbox files written by `operative-state-persistence`.
- `app_data/process_hub/` — ProcessHub durable registries written by `auto-restore-on-boot`.

The command SHALL leave `app_data/certs/` and the socket directory intact unless explicitly requested via `--include-certs`, `--include-sockets`, or `--all`. The socket directory is resolved through the shared ladder and is frequently not under `app_data/`.

The command SHALL NEVER delete `app_data/` itself, nor the socket directory itself; it only clears the contents of the listed roots. A path-equality assertion SHALL prevent unlinking `app_data/` or any of the cleared roots themselves.

The command SHALL be idempotent: a missing root directory is success, not error.

#### Scenario: Default reset clears state and process_hub but keeps certs and sockets

- **GIVEN** the Controller is not running and `app_data/` contains `state/`, `process_hub/` and `certs/` with files, and the resolved socket directory holds socket files
- **WHEN** the operator runs `flz reset --yes`
- **THEN** `app_data/state/` is empty, `app_data/process_hub/` is empty, `app_data/certs/` is unchanged, and the socket directory is unchanged
- **AND** `app_data/` itself still exists with the same inode

#### Scenario: `--all` clears every documented root

- **GIVEN** the Controller is not running, `app_data/` contains `state/`, `process_hub/` and `certs/`, and the resolved socket directory holds Flezha sockets
- **WHEN** the operator runs `flz reset --yes --all`
- **THEN** the three `app_data/` subdirectories exist and are empty after the command returns
- **AND** the Flezha sockets in the resolved socket directory are unlinked

#### Scenario: Idempotent over a missing root

- **GIVEN** `app_data/state/` does not exist (e.g. fresh checkout)
- **WHEN** the operator runs `flz reset --yes`
- **THEN** the command exits 0 and reports `cleared app_data/state/ (0 files, 0 B)` (or omits the line)
- **AND** `app_data/process_hub/` is still cleared if it exists

#### Scenario: Refuse to delete the configured root itself

- **GIVEN** a malformed configuration sets `--app-data-root` to a path whose canonicalisation would target a single cleared root identical to the configured root
- **WHEN** the operator runs `flz reset --yes`
- **THEN** the command exits with code 1 and an explicit "refusing to delete configured `app_data` root itself" error before any unlink runs

### Requirement: `flz reset` refuses to run while the Controller is up

`flz reset` SHALL detect a running Controller and refuse to act, exiting with a dedicated non-zero exit code and a message directing the operator to `flz controller stop`.

Detection SHALL use `flzvm rpc` against `Flezha.Service.CliManager.controller_status/1` (the same transport used by `flz controller stop`/`status`). The probe is bounded by `--probe-timeout-ms` (default `1000`).

The probe SHALL be considered "Controller is up" when `flzvm rpc` exits 0 AND the RPC returns a parseable status reply. Any non-zero exit from `flzvm rpc` (no node, distribution refused, RPC timeout) SHALL be treated as "Controller is not up" and the reset SHALL proceed.

This guard answers about **this** Controller's node, and SHALL NOT be widened to cover a Controller colocated on the same machine: a peer is a different node with its own durable state, and its state is not this reset's to protect. What the peer shares is the socket directory, and that is guarded where it is touched.

The command SHALL NOT provide a `--force-while-live` (or equivalent) escape hatch that bypasses this guard.

#### Scenario: Refuse when Controller is up

- **GIVEN** the Controller is running and addressable via `flzvm rpc`
- **WHEN** the operator runs `flz reset --yes`
- **THEN** the command exits with code 4 (refused)
- **AND** stderr contains `flz reset refuses to run while the controller is up. Stop it first with: flz controller stop`
- **AND** no file under `app_data/` is touched

#### Scenario: Proceed when Controller is down

- **GIVEN** no Controller node is registered with `epmd` and `flzvm rpc` exits non-zero
- **WHEN** the operator runs `flz reset --yes`
- **THEN** the command proceeds with the reset and exits 0 on success

#### Scenario: Proceed when only stale sockets exist

- **GIVEN** the Controller crashed leaving a control socket in the resolved socket directory but no live BEAM node
- **WHEN** the operator runs `flz reset --yes`
- **THEN** the `flzvm rpc` probe exits non-zero (no live node)
- **AND** the reset proceeds; the stale socket is cleared if `--include-sockets` or `--all` is also passed

### Requirement: `flz reset` requires explicit `--yes` for any destructive action

`flz reset` SHALL NOT modify the filesystem unless the operator passes `--yes`. The command SHALL NOT prompt interactively under any circumstance.

Without `--yes` and without `--dry-run`, the command SHALL print the would-be plan with the verb `would clear` and exit with code 1, signalling that the caller forgot the flag.

With `--dry-run`, the command SHALL print the would-be plan with the verb `would clear` and exit 0 regardless of `--yes`. `--dry-run` and `--yes` are mutually exclusive — passing both SHALL exit 1 with a usage error.

#### Scenario: Bare `flz reset` is a non-zero preview

- **GIVEN** the Controller is not running
- **WHEN** the operator runs `flz reset` with no flags
- **THEN** stdout shows `would clear app_data/state/ ...` lines, no file is touched, and the exit code is 1

#### Scenario: `--dry-run` is a zero-exit preview

- **GIVEN** the Controller is not running
- **WHEN** the operator runs `flz reset --dry-run`
- **THEN** stdout shows `would clear ...` lines, no file is touched, and the exit code is 0

#### Scenario: `--yes` and `--dry-run` together is a usage error

- **WHEN** the operator runs `flz reset --yes --dry-run`
- **THEN** stderr explains the flags conflict and the exit code is 1, with no filesystem changes

### Requirement: `flz reset` prints an audit-trail summary of cleared paths and byte counts

`flz reset` SHALL print one human-readable line per inspected root, distinguishing `cleared` from `kept`, with file count and byte size, plus a final `reset complete in <N> ms` line. With `--json`, the same data SHALL be emitted as a single JSON object containing `cleared`, `kept`, `duration_ms`, and `dry_run` fields.

The socket root SHALL be reported with the rung that produced it alongside its path, in the same words the connect diagnosis and `flz runner check` use for that rung. An operator who expected `app_data/sockets/` and sees a runtime directory needs to read the ladder answering, not the command going wrong.

`--dry-run` (or absent `--yes`) SHALL substitute the verb `would clear` / `would keep` and the JSON `dry_run` flag SHALL be `true`.

The walk that computes file counts and bytes SHALL be the same in dry-run and apply modes so the preview matches the action.

#### Scenario: Human output enumerates roots

- **WHEN** the operator runs `flz reset --yes` against a populated `app_data/`
- **THEN** stdout contains lines such as `cleared app_data/state/ (12 files, 4.3 MiB)` for each cleared root, `kept app_data/certs/ (use --include-certs to clear)` for each kept root, and a final `reset complete in <N> ms` line

#### Scenario: The socket root names its rung

- **GIVEN** the per-user runtime directory is what the ladder resolved
- **WHEN** the operator runs `flz reset --dry-run --include-sockets`
- **THEN** the socket root's line carries both the absolute path and the rung that produced it

#### Scenario: `--json` output is a single object

- **WHEN** the operator runs `flz reset --yes --json`
- **THEN** stdout contains exactly one JSON object with `cleared` (array of `{root, files, bytes}`), `kept` (array of `{root, reason}`), `duration_ms` (integer), and `dry_run: false`

#### Scenario: `--dry-run` matches `--yes` plan exactly

- **GIVEN** a populated `app_data/`
- **WHEN** the operator runs `flz reset --dry-run --json`, captures the output, then runs `flz reset --yes --json` immediately after
- **THEN** the per-root `files` and `bytes` values in the second invocation match the first (modulo any change between the two runs, which there should not be)

### Requirement: `flz reset` resolves `app_data/` root from the same source as the Controller

`flz reset` SHALL resolve the `app_data/` root using, in order: the explicit `--app-data-root <PATH>` flag if given; the `FLEZHA_APP_DATA_ROOT` environment variable if set; otherwise the default `./app_data` relative to the current working directory.

This resolution SHALL live in one shared helper — `cli/src/args.rs`, `app_data_root/1` over `resolve_app_data_root` — so every CLI subcommand that touches `app_data/` agrees on the location, and SHALL NOT be re-derived at a call site.

#### Scenario: Explicit flag wins over env var

- **GIVEN** `FLEZHA_APP_DATA_ROOT=/var/lib/flezha-prod`
- **WHEN** the operator runs `flz reset --yes --app-data-root /tmp/test-root`
- **THEN** only `/tmp/test-root/state/`, `/tmp/test-root/process_hub/`, etc. are touched; `/var/lib/flezha-prod` is not read

#### Scenario: Env var wins over the default

- **GIVEN** `FLEZHA_APP_DATA_ROOT=/var/lib/flezha-prod` and the current directory is `/home/proc` containing `app_data/`
- **WHEN** the operator runs `flz reset --yes`
- **THEN** `/var/lib/flezha-prod/state/`, `/var/lib/flezha-prod/process_hub/` are cleared; `./app_data/` under the cwd is not touched

### Requirement: `flz reset` exit codes are stable and documented

`flz reset` SHALL use the following exit codes:

- `0` — success: all requested roots cleared (or `--dry-run` preview).
- `1` — invalid usage: missing `--yes` (and no `--dry-run`), conflicting flags, malformed `--app-data-root`, refusal to delete the root itself.
- `2` — partial success: some roots cleared, then a filesystem error stopped the walk; the affected paths are reported.
- `3` — I/O error before any action (e.g. `app_data/` exists but is unreadable).
- `4` — refused: Controller is up.

The codes SHALL be documented in `flz reset --help` output.

#### Scenario: Partial failure during walk

- **GIVEN** `app_data/state/` is writable but `app_data/process_hub/` contains a file the user cannot remove (e.g. permission denied)
- **WHEN** the operator runs `flz reset --yes`
- **THEN** `app_data/state/` is cleared, the walk stops on the permission-denied entry, the offending path is printed to stderr, and the exit code is 2

#### Scenario: Refused exit code is dedicated

- **GIVEN** the Controller is running
- **WHEN** the operator runs `flz reset --yes`
- **THEN** the exit code is exactly 4 (not 1, not 3) so scripts can distinguish "controller still up" from "bad usage"

### Requirement: `flz reset` resolves the socket directory from the same source as the Controller and its runners

`flz reset` SHALL resolve the socket directory through the shared socket-directory ladder — the one implementation the Controller, every runner and `flz runner check` reach — rather than assuming a fixed subdirectory of the `app_data/` root. The ladder's rungs are, in order: the explicit socket-directory environment variable used outright if set; else the per-user runtime directory; else the system-wide runtime directory; else the repository fallback under `app_data/`.

The resolved directory is therefore frequently NOT under the `app_data/` root, and `--app-data-root` SHALL NOT move it: the two are resolved from different sources because the Controller resolves them from different sources.

`flz reset` SHALL NOT create a directory while resolving one. It asks which directory holds this fleet's sockets; it does not establish one, and a run that has nothing to clear leaves the filesystem as it found it. This holds for every invocation, `--dry-run` included: a preview that writes to disk is not a preview.

`flz reset` SHALL NOT resolve past a rung a Controller is listening in. The rung that answers is the one the command names and reports on; where more than one rung is live and nothing says which is meant, resolution SHALL refuse naming every live directory and the environment variable that settles it — the same refusal a runner is given, because it is the same question. Stepping over a serving Controller and answering with a rung nothing has ever used would report on a directory that is not this fleet's.

When resolution refuses, `flz reset` SHALL report the socket root as kept with the refusal's reason and SHALL continue with the remaining roots. Not knowing which directory holds the sockets is not an I/O error on a path the command was asked to clear, and SHALL NOT change the exit code.

#### Scenario: The per-user runtime directory is cleared, not `app_data/sockets/`

- **GIVEN** the per-user runtime directory environment variable is set, and both that directory's `flezha/` subdirectory and `app_data/sockets/` contain socket files
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** the socket files under the per-user runtime directory are unlinked
- **AND** `app_data/sockets/` is not reported as the cleared socket root

#### Scenario: The explicit variable wins over every later rung

- **GIVEN** the explicit socket-directory environment variable names a directory holding a stale control socket, and the per-user runtime directory also holds one
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** only the directory the explicit variable names is cleared

#### Scenario: `--app-data-root` does not move the socket directory

- **GIVEN** the per-user runtime directory is set and the operator passes `--app-data-root /tmp/test-root`
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** `/tmp/test-root/state/` and `/tmp/test-root/process_hub/` are cleared
- **AND** the socket root cleared is the one the ladder resolved, not `/tmp/test-root/sockets/`

#### Scenario: A resolution refusal keeps the socket root and clears the rest

- **WHEN** the ladder cannot resolve a socket directory
- **THEN** the socket root is reported as kept with the reason, `app_data/state/` and `app_data/process_hub/` are still cleared, and the exit code is unchanged

#### Scenario: No directory is established by a run that clears one

- **GIVEN** no rung of the ladder exists on this machine
- **WHEN** the operator runs `flz reset --dry-run --include-sockets`
- **THEN** no directory is created at any rung, the working directory holds no new `app_data/sockets/`, and the exit code is 0

#### Scenario: A rung a peer is listening in is what the run names

- **GIVEN** a peer Controller is serving in the first rung the ladder offers, and this Controller's node is down
- **WHEN** the operator runs `flz reset --dry-run --include-sockets`
- **THEN** the socket root reported is the rung the peer holds, named with its path
- **AND** no later rung is created

### Requirement: `flz reset` never unlinks a live controller's socket

Because the socket directory may be shared, `flz reset` SHALL probe the resolved directory for a control socket before unlinking anything in it, using the same probe the connect diagnosis and `flz runner check` use.

When the probe finds a control socket that answers, `flz reset` SHALL keep the socket root — reporting it kept, naming the directory and the rung that produced it — and SHALL proceed with the remaining roots. It SHALL NOT refuse the whole run: several Controllers may share a machine and therefore a runtime directory, a peer's durable state is not under this reset's `app_data/` root, and refusing would make the command unusable wherever colocation is in use.

This probe is IN ADDITION TO the live-node guard, never a replacement for it. The live-node guard asks whether **this** Controller's node is up; the probe asks whether **any** Controller is serving in the directory about to be cleared. A peer Controller is a different node, so the live-node guard truthfully answers "not up" while the peer serves.

A control socket that is present but does not answer — a Controller that died hard — SHALL NOT keep the socket root: that is the stale socket the command exists to clear. A directory this user may not enter, or a socket that refuses for another reason, SHALL be reported and skipped rather than forced.

#### Scenario: A live peer Controller's socket directory is kept, and the rest is cleared

- **GIVEN** two Controllers share a runtime directory, the peer is serving, and this Controller's node is down
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** the live-node guard passes, because the peer is a different node
- **AND** the socket root is reported kept, naming the directory and its rung
- **AND** `app_data/state/` and `app_data/process_hub/` are still cleared, and the exit code is 0

#### Scenario: A stale socket is cleared, not kept

- **GIVEN** a Controller died hard leaving a control socket nothing is listening on
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** the probe reports the socket stale, and the socket files are unlinked

#### Scenario: A directory this user cannot enter is reported, not forced

- **GIVEN** the resolved socket directory belongs to another user and is not enterable
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** the socket root is reported kept with the reason, no unlink is attempted there, and the remaining roots are cleared

### Requirement: `flz reset` unlinks only the socket files Flezha creates

Within the resolved socket directory, `flz reset` SHALL unlink only the socket files Flezha itself creates — the control socket and the per-runner stream sockets — identified by the same naming the transport uses to create them. Every other entry SHALL be left in place.

The other clearable roots are subdirectories of an `app_data/` root the command owns, so clearing their whole contents is safe. The socket directory may be one an operator named through the explicit environment variable, and a directory an operator chose is not one `flz reset` may empty.

#### Scenario: Unrelated files in the socket directory survive

- **GIVEN** the resolved socket directory holds a stale Flezha control socket, a Flezha stream socket, and a file Flezha did not create
- **WHEN** the operator runs `flz reset --yes --include-sockets`
- **THEN** both Flezha sockets are unlinked
- **AND** the unrelated file is still present, and the directory itself still exists

### Requirement: The `app_data/` layout SHALL be one declaration both runtimes read

The set of subtrees Flezha writes under the data root SHALL be declared once, and both
the Controller and `flz` SHALL read that declaration rather than spelling a subtree's
name. Each row SHALL carry the subtree's name, the configuration key that may move it,
whether a directory claim is owed before first write, and how `flz reset` treats it —
cleared by default, cleared only when a flag names it, or never cleared.

No component SHALL derive a root of its own. A module that writes under the data root
SHALL name its subtree by its declared key and SHALL obtain the directory and any file
inside it from the declaration, so moving the data root moves every subtree with it.

The socket directory SHALL remain a row whose location is resolved by the socket-directory
ladder and not by the data root, because the two are resolved from different sources and
`--app-data-root` SHALL NOT move it. The declaration owns the names inside that directory;
it does not own the directory.

`flz reset`'s roots SHALL be exactly the rows declaring that they are cleared by default
or on a flag. Adding a subtree to the declaration SHALL NOT silently enlarge what `flz reset`
destroys: a new row is `never` unless it says otherwise.

#### Scenario: Moving the data root moves every subtree

- **GIVEN** the Controller is configured with a data root other than `./app_data`
- **WHEN** it writes durable state, auth state, the plugin catalog, the hub registry, the
  audit log, the DLQ and the schedule store
- **THEN** every one of those files SHALL be under the configured root, and no component
  SHALL write under `./app_data`

#### Scenario: `flz reset` and the Controller name one tree

- **GIVEN** a data root the Controller has written under
- **WHEN** `flz reset --dry-run --json` reports the roots it would clear
- **THEN** each reported path SHALL be the path the Controller resolves for that
  subtree's declared key

#### Scenario: A subtree the declaration marks `never` is not cleared

- **GIVEN** the Controller is not running and the data root holds every declared subtree
- **WHEN** the operator runs `flz reset --yes --all`
- **THEN** the subtrees declared `never` — among them the auth store — SHALL be
  untouched, and only the rows declaring `default` or a flag SHALL be cleared

#### Scenario: The socket row is not under the data root

- **GIVEN** the socket-directory ladder resolves to a per-user runtime directory
- **WHEN** the layout is asked for the socket row's directory
- **THEN** it SHALL answer the ladder's directory, unchanged by `--app-data-root` and
  by the data-root configuration key

