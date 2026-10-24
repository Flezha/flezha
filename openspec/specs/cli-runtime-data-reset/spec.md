# cli-runtime-data-reset Specification

## Purpose

The cli-runtime-data-reset capability defines a top-level `flz reset` CLI subcommand that clears the Controller's durable runtime state on disk (per-Cell WAL/snapshot tree, ProcessHub durable registry; optionally certs, parked DLQ records, schedules, the auth store, the audit trail, installed plugins and sockets) so a user can deterministically start fresh from a clean slate. The capability owns the filesystem walk (clear *contents* of well-known `storage/app/` subdirectories, never the parent itself), a hard live-node guard that refuses to run while the Controller is up, idempotent semantics over missing directories, scriptable flags (`--yes`, `--dry-run`, `--include-certs`, `--include-dlq`, `--include-schedules`, `--include-auth`, `--include-audit`, `--include-plugins`, `--include-sockets`, `--all`, `--storage-root`, `--json`), the audit-trail output enumerating cleared paths and byte counts and every root it kept, and the stable exit-code contract that lets scripts distinguish "refused" from "bad usage" from "I/O error".
## Requirements
### Requirement: `flz reset` clears Controller durable runtime data on disk

The CLI SHALL provide a top-level `flz reset` subcommand that clears the Controller's durable runtime state so a user can deterministically start fresh. By default the command SHALL clear the contents of:

- `storage/app/state/` — per-Cell WAL/snapshot/outbox files written by `relay-state-persistence`.
- `storage/app/process_hub/` — ProcessHub durable registries written by `auto-restore-on-boot`.

The command SHALL leave `storage/app/certs/`, `storage/app/dlq/`, `storage/app/schedules/`, `storage/app/auth/`, `storage/app/audit/`, `storage/app/plugins/` and the socket directory intact unless explicitly requested via `--include-certs`, `--include-dlq`, `--include-schedules`, `--include-auth`, `--include-audit`, `--include-plugins`, `--include-sockets`, or `--all`, which requests all seven. The socket directory is resolved through the shared ladder and is frequently not under `storage/app/`.

The command SHALL NOT clear the log directory under any flag, `--all` included: its default is relative to the directory the Controller ran in, and the command cannot tell which directory that was.

The command SHALL NEVER delete `storage/app/` itself, nor the socket directory itself; it only clears the contents of the listed roots. A path-equality assertion SHALL prevent unlinking `storage/app/` or any of the cleared roots themselves.

The command SHALL be idempotent: a missing root directory is success, not error.

#### Scenario: Default reset clears state and process_hub but keeps certs and sockets

- **GIVEN** the Controller is not running and `storage/app/` contains `state/`, `process_hub/`, `certs/`, `dlq/`, `schedules/`, `auth/`, `audit/` and `plugins/` with files, and the resolved socket directory holds socket files
- **WHEN** the user runs `flz reset --yes`
- **THEN** `storage/app/state/` is empty, `storage/app/process_hub/` is empty, and `storage/app/certs/`, `storage/app/dlq/`, `storage/app/schedules/`, `storage/app/auth/`, `storage/app/audit/`, `storage/app/plugins/` and the socket directory are unchanged
- **AND** `storage/app/` itself still exists with the same inode

#### Scenario: Each store flag clears its own store and no other

- **GIVEN** the Controller is not running and `storage/app/` contains `certs/`, `dlq/`, `schedules/`, `auth/`, `audit/` and `plugins/` with files
- **WHEN** the user runs `flz reset --yes --include-dlq`
- **THEN** `storage/app/dlq/` is empty, and `storage/app/certs/`, `storage/app/schedules/`, `storage/app/auth/`, `storage/app/audit/` and `storage/app/plugins/` are unchanged
- **AND** each of `--include-certs`, `--include-schedules`, `--include-auth`, `--include-audit` and `--include-plugins`, on the same starting tree, empties its own store and leaves the other five unchanged

#### Scenario: `--all` clears every documented root

- **GIVEN** the Controller is not running, `storage/app/` contains `state/`, `process_hub/`, `certs/`, `dlq/`, `schedules/`, `auth/`, `audit/` and `plugins/`, the resolved socket directory holds Flezha sockets, and the log directory holds a log file
- **WHEN** the user runs `flz reset --yes --all`
- **THEN** `storage/app/state/`, `storage/app/process_hub/`, `storage/app/certs/`, `storage/app/dlq/`, `storage/app/schedules/`, `storage/app/auth/`, `storage/app/audit/` and `storage/app/plugins/` exist and are empty after the command returns
- **AND** the Flezha sockets in the resolved socket directory are unlinked
- **AND** the log file is unchanged, and the log directory is reported kept with `not reset's to clear`

#### Scenario: Idempotent over a missing root

- **GIVEN** `storage/app/state/` does not exist (e.g. fresh checkout)
- **WHEN** the user runs `flz reset --yes`
- **THEN** the command exits 0 and reports `cleared storage/app/state/ (0 files, 0 B)` (or omits the line)
- **AND** `storage/app/process_hub/` is still cleared if it exists

#### Scenario: Refuse to delete the configured root itself

- **GIVEN** a malformed configuration sets `--storage-root` to a path whose canonicalisation would target a single cleared root identical to the configured root
- **WHEN** the user runs `flz reset --yes`
- **THEN** the command exits with code 2 and the error `reset.path_refused`, naming the storage app directory, before any unlink runs

#### Scenario: Refuse to clear a root that overlaps a kept one

- **GIVEN** the Controller is not running and `storage/app/dlq` is a symlink to `storage/app/audit` or to a directory inside it
- **WHEN** the user runs `flz reset --yes --include-dlq`
- **THEN** the command exits with code 2 and the error `reset.path_refused`, naming the path and the kept root it overlaps, before any unlink runs, and `storage/app/audit/` is unchanged
- **AND** the same refusal holds when a kept root such as `storage/app/audit` resolves into a root the run clears, such as `storage/app/state`

### Requirement: `flz reset` refuses to run while the Controller is up

`flz reset` SHALL detect a running Controller and refuse to act, exiting with the error `reset.controller_running` (class `conflict`, exit 6) whose hint is `flz controller stop`.

Detection SHALL send `Controller.boot_status` as a request over the control socket (the same path used by `flz controller stop`/`status`). The probe is bounded by `--probe-timeout-ms` (default `1000`).

The probe SHALL be considered "Controller is up" when the request is accepted and answers exit code 0 with a non-empty status. Any other outcome (no socket, a refused connection, no acceptance, a non-zero code, the timeout) SHALL be treated as "Controller is not up" and the reset SHALL proceed.

This guard answers about **this** Controller's node, and SHALL NOT be widened to cover a Controller colocated on the same machine: a peer is a different node with its own durable state, and its state is not this reset's to protect. What the peer shares is the socket directory, and that is guarded where it is touched.

The command SHALL NOT provide a `--force-while-live` (or equivalent) escape hatch that bypasses this guard.

A `--dry-run` changes nothing, so it SHALL NOT run the probe and previews whether or not the Controller is up; it is no escape hatch, because it never applies and `--yes --dry-run` is refused as a usage error.

#### Scenario: Refuse when Controller is up

- **GIVEN** the Controller is running and answers on its control socket
- **WHEN** the user runs `flz reset --yes`
- **THEN** the command exits with code 6 (conflict)
- **AND** stderr carries an `error:` line ending `(reset.controller_running)` and the line `hint: flz controller stop`
- **AND** no file under `storage/app/` is touched

#### Scenario: Proceed when Controller is down

- **GIVEN** no Controller is running and the control socket is absent
- **WHEN** the user runs `flz reset --yes`
- **THEN** the command proceeds with the reset and exits 0 on success

#### Scenario: Proceed when only stale sockets exist

- **GIVEN** the Controller crashed leaving a control socket in the resolved socket directory but no live BEAM node
- **WHEN** the user runs `flz reset --yes`
- **THEN** the probe's connection is refused (no listener)
- **AND** the reset proceeds; the stale socket is cleared if `--include-sockets` or `--all` is also passed

#### Scenario: A dry run previews while the Controller is up

- **GIVEN** the Controller is running and answers on its control socket
- **WHEN** the user runs `flz reset --dry-run --json`
- **THEN** the command prints the plan with `dry_run` set to `true` and exits 0
- **AND** no file under `storage/app/` is touched

### Requirement: `flz reset` requires explicit `--yes` for any destructive action

`flz reset` SHALL NOT modify the filesystem unless the user passes `--yes`. The command SHALL NOT prompt interactively under any circumstance.

Without `--yes` and without `--dry-run`, the command SHALL print the would-be plan with the verb `would clear` and exit with the error `reset.confirmation_required` (class `invalid`, exit 2), signalling that the caller forgot the flag.

With `--dry-run`, the command SHALL print the would-be plan with the verb `would clear` and exit 0 regardless of `--yes`. `--dry-run` and `--yes` are mutually exclusive — passing both SHALL exit 2 with a `usage.*` error.

#### Scenario: Bare `flz reset` is a non-zero preview

- **GIVEN** the Controller is not running
- **WHEN** the user runs `flz reset` with no flags
- **THEN** stdout shows `would clear storage/app/state/ ...` lines, no file is touched, and the exit code is 2

#### Scenario: `--dry-run` is a zero-exit preview

- **GIVEN** the Controller is not running
- **WHEN** the user runs `flz reset --dry-run`
- **THEN** stdout shows `would clear ...` lines, no file is touched, and the exit code is 0

#### Scenario: `--yes` and `--dry-run` together is a usage error

- **WHEN** the user runs `flz reset --yes --dry-run`
- **THEN** stderr explains the flags conflict and the exit code is 2, with no filesystem changes

### Requirement: `flz reset` prints an audit-trail summary of cleared paths and byte counts

`flz reset` SHALL print one human-readable line per inspected root, distinguishing `cleared` from `kept`, with file count and byte size, plus a final `reset complete in <N> ms` line. With `--json`, the same data SHALL be emitted as a single JSON object containing `cleared`, `kept`, `duration_ms`, and `dry_run` fields.

Every root the layout declares SHALL be inspected and reported on every run, so the summary names everything a reset left behind. A root a flag would clear SHALL be reported kept with the flag that clears it; a root no flag clears SHALL be reported kept with the reason `not reset's to clear`.

The socket root SHALL be reported with the rung that produced it alongside its path, in the same words the connect diagnosis and `flz runner check` use for that rung. A user who expected `storage/app/sockets/` and sees a runtime directory needs to read the ladder answering, not the command going wrong.

`--dry-run` (or absent `--yes`) SHALL substitute the verb `would clear` / `would keep` and the JSON `dry_run` flag SHALL be `true`.

The walk that computes file counts and bytes SHALL be the same in dry-run and apply modes so the preview matches the action.

#### Scenario: Human output enumerates roots

- **WHEN** the user runs `flz reset --yes` against a populated `storage/app/`
- **THEN** stdout contains lines such as `cleared storage/app/state/ (12 files, 4.3 MiB)` for each cleared root, `kept storage/app/certs/ (use --include-certs to clear)` for each root a flag would clear, `kept storage/logs/ (not reset's to clear)` for the root no flag clears, and a final `reset complete in <N> ms` line

#### Scenario: A plain reset names every store it keeps

- **GIVEN** the Controller is not running and `storage/app/` holds every subtree the layout declares
- **WHEN** the user runs `flz reset --yes`, or `flz reset --dry-run --json`
- **THEN** `storage/app/dlq/` is reported kept with `use --include-dlq to clear`, `storage/app/schedules/` with `use --include-schedules to clear`, `storage/app/auth/` with `use --include-auth to clear`, `storage/app/audit/` with `use --include-audit to clear` and `storage/app/plugins/` with `use --include-plugins to clear`
- **AND** the log directory is reported kept with `not reset's to clear`

#### Scenario: The socket root names its rung

- **GIVEN** the per-user runtime directory is what the ladder resolved
- **WHEN** the user runs `flz reset --dry-run --include-sockets`
- **THEN** the socket root's line carries both the absolute path and the rung that produced it

#### Scenario: `--json` output is a single object

- **WHEN** the user runs `flz reset --yes --json`
- **THEN** stdout contains exactly one JSON object with `cleared` (array of `{root, files, bytes}`), `kept` (array of `{root, reason}`), `duration_ms` (integer), and `dry_run: false`

#### Scenario: `--dry-run` matches `--yes` plan exactly

- **GIVEN** a populated `storage/app/`
- **WHEN** the user runs `flz reset --dry-run --json`, captures the output, then runs `flz reset --yes --json` immediately after
- **THEN** the per-root `files` and `bytes` values in the second invocation match the first (modulo any change between the two runs, which there should not be)

### Requirement: `flz reset` resolves the `storage/` root from the same source as the Controller

`flz reset` SHALL resolve the `storage/` root using, in order: the explicit `--storage-root <PATH>` flag if given; the `FLEZHA_STORAGE_ROOT` environment variable if set; otherwise the default `./storage` relative to the current working directory. Every store is under `<storage root>/app/`; the log directory stays `./storage/logs/`, which the flag does not move.

This resolution SHALL live in one shared helper — `crates/cli/src/args.rs`, `storage_root` and `app_dir` — so every CLI subcommand that touches `storage/` agrees on the location, and SHALL NOT be re-derived at a call site.

#### Scenario: Explicit flag wins over env var

- **GIVEN** `FLEZHA_STORAGE_ROOT=/var/lib/flezha-prod`
- **WHEN** the user runs `flz reset --yes --storage-root /tmp/test-root`
- **THEN** only `/tmp/test-root/app/state/`, `/tmp/test-root/app/process_hub/`, etc. are touched; `/var/lib/flezha-prod` is not read

#### Scenario: Env var wins over the default

- **GIVEN** `FLEZHA_STORAGE_ROOT=/var/lib/flezha-prod` and the current directory is `/home/task` containing `storage/`
- **WHEN** the user runs `flz reset --yes`
- **THEN** `/var/lib/flezha-prod/app/state/`, `/var/lib/flezha-prod/app/process_hub/` are cleared; `./storage/` under the cwd is not touched

### Requirement: `flz reset` exit codes are stable and documented

`flz reset` SHALL use the shared exit classes of the error catalogue:

- `0` — success: all requested roots cleared (or `--dry-run` preview).
- `2` (invalid) — missing `--yes` (and no `--dry-run`), conflicting flags, malformed `--storage-root`, refusal to delete the root itself or an overlapping path.
- `1` (internal) — a filesystem error, before any action (e.g. `storage/app/` exists but is unreadable, `reset.path_unreadable`) or during the walk after some roots were cleared (`reset.partial`, the cleared roots and the offending path in details).
- `6` (conflict) — the Controller is up (`reset.controller_running`).

The codes SHALL be documented in `flz reset --help` output, pointing at `docs/public/errors.md`.

#### Scenario: Partial failure during walk

- **GIVEN** `storage/app/state/` is writable but `storage/app/process_hub/` contains a file the user cannot remove (e.g. permission denied)
- **WHEN** the user runs `flz reset --yes`
- **THEN** `storage/app/state/` is cleared, the walk stops on the permission-denied entry, the offending path is printed to stderr in the `reset.partial` error, and the exit code is 1

#### Scenario: Refused exit code is dedicated

- **GIVEN** the Controller is running
- **WHEN** the user runs `flz reset --yes`
- **THEN** the exit code is exactly 6 (not 1, not 2) so scripts can distinguish "controller still up" from "bad usage"

### Requirement: `flz reset` resolves the socket directory from the same source as the Controller and its runners

`flz reset` SHALL resolve the socket directory through the shared socket-directory ladder — the one implementation the Controller, every runner and `flz runner check` reach — rather than assuming a fixed subdirectory of the `storage/app/` root. The ladder's rungs are, in order: the explicit socket-directory environment variable used outright if set; else the per-user runtime directory; else the system-wide runtime directory; else the repository fallback under `storage/app/`.

The resolved directory is therefore frequently NOT under the `storage/app/` root, and `--storage-root` SHALL NOT move it: the two are resolved from different sources because the Controller resolves them from different sources.

`flz reset` SHALL NOT create a directory while resolving one. It asks which directory holds this fleet's sockets; it does not establish one, and a run that has nothing to clear leaves the filesystem as it found it. This holds for every invocation, `--dry-run` included: a preview that writes to disk is not a preview.

`flz reset` SHALL NOT resolve past a rung a Controller is listening in. The rung that answers is the one the command names and reports on; where more than one rung is live and nothing says which is meant, resolution SHALL refuse naming every live directory and the environment variable that settles it — the same refusal a runner is given, because it is the same question. Stepping over a serving Controller and answering with a rung nothing has ever used would report on a directory that is not this fleet's.

When resolution refuses, `flz reset` SHALL report the socket root as kept with the refusal's reason and SHALL continue with the remaining roots. Not knowing which directory holds the sockets is not an I/O error on a path the command was asked to clear, and SHALL NOT change the exit code.

#### Scenario: The per-user runtime directory is cleared, not `storage/app/sockets/`

- **GIVEN** the per-user runtime directory environment variable is set, and both that directory's `flezha/` subdirectory and `storage/app/sockets/` contain socket files
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** the socket files under the per-user runtime directory are unlinked
- **AND** `storage/app/sockets/` is not reported as the cleared socket root

#### Scenario: The explicit variable wins over every later rung

- **GIVEN** the explicit socket-directory environment variable names a directory holding a stale control socket, and the per-user runtime directory also holds one
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** only the directory the explicit variable names is cleared

#### Scenario: `--storage-root` does not move the socket directory

- **GIVEN** the per-user runtime directory is set and the user passes `--storage-root /tmp/test-root`
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** `/tmp/test-root/state/` and `/tmp/test-root/process_hub/` are cleared
- **AND** the socket root cleared is the one the ladder resolved, not `/tmp/test-root/sockets/`

#### Scenario: A resolution refusal keeps the socket root and clears the rest

- **WHEN** the ladder cannot resolve a socket directory
- **THEN** the socket root is reported as kept with the reason, `storage/app/state/` and `storage/app/process_hub/` are still cleared, and the exit code is unchanged

#### Scenario: No directory is established by a run that clears one

- **GIVEN** no rung of the ladder exists on this machine
- **WHEN** the user runs `flz reset --dry-run --include-sockets`
- **THEN** no directory is created at any rung, the working directory holds no new `storage/app/sockets/`, and the exit code is 0

#### Scenario: A rung a peer is listening in is what the run names

- **GIVEN** a peer Controller is serving in the first rung the ladder offers, and this Controller's node is down
- **WHEN** the user runs `flz reset --dry-run --include-sockets`
- **THEN** the socket root reported is the rung the peer holds, named with its path
- **AND** no later rung is created

### Requirement: `flz reset` never unlinks a live controller's socket

Because the socket directory may be shared, `flz reset` SHALL probe the resolved directory for a control socket before unlinking anything in it, using the same probe the connect diagnosis and `flz runner check` use.

When the probe finds a control socket that answers, `flz reset` SHALL keep the socket root — reporting it kept, naming the directory and the rung that produced it — and SHALL proceed with the remaining roots. It SHALL NOT refuse the whole run: several Controllers may share a machine and therefore a runtime directory, a peer's durable state is not under this reset's `storage/app/` root, and refusing would make the command unusable wherever colocation is in use.

This probe is IN ADDITION TO the live-node guard, never a replacement for it. The live-node guard asks whether **this** Controller's node is up; the probe asks whether **any** Controller is serving in the directory about to be cleared. A peer Controller is a different node, so the live-node guard truthfully answers "not up" while the peer serves.

A control socket that is present but does not answer — a Controller that died hard — SHALL NOT keep the socket root: that is the stale socket the command exists to clear. A directory this user may not enter, or a socket that refuses for another reason, SHALL be reported and skipped rather than forced.

#### Scenario: A live peer Controller's socket directory is kept, and the rest is cleared

- **GIVEN** two Controllers share a runtime directory, the peer is serving, and this Controller's node is down
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** the live-node guard passes, because the peer is a different node
- **AND** the socket root is reported kept, naming the directory and its rung
- **AND** `storage/app/state/` and `storage/app/process_hub/` are still cleared, and the exit code is 0

#### Scenario: A stale socket is cleared, not kept

- **GIVEN** a Controller died hard leaving a control socket nothing is listening on
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** the probe reports the socket stale, and the socket files are unlinked

#### Scenario: A directory this user cannot enter is reported, not forced

- **GIVEN** the resolved socket directory belongs to another user and is not enterable
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** the socket root is reported kept with the reason, no unlink is attempted there, and the remaining roots are cleared

### Requirement: `flz reset` unlinks only the socket files Flezha creates

Within the resolved socket directory, `flz reset` SHALL unlink only the socket files Flezha itself creates — the control socket and the per-runner stream sockets — identified by the same naming the transport uses to create them. Every other entry SHALL be left in place.

The other clearable roots are subdirectories of an `storage/app/` root the command owns, so clearing their whole contents is safe. The socket directory may be one a user named through the explicit environment variable, and a directory a user chose is not one `flz reset` may empty.

#### Scenario: Unrelated files in the socket directory survive

- **GIVEN** the resolved socket directory holds a stale Flezha control socket, a Flezha stream socket, and a file Flezha did not create
- **WHEN** the user runs `flz reset --yes --include-sockets`
- **THEN** both Flezha sockets are unlinked
- **AND** the unrelated file is still present, and the directory itself still exists

### Requirement: The `storage/app/` layout SHALL be one declaration both runtimes read

The set of subtrees Flezha writes SHALL be declared once, and both
the Controller and `flz` SHALL read that declaration rather than spelling a subtree's
name. Each row SHALL carry the subtree's name, the configuration key that may move it,
whether a directory claim is owed before first write, and how `flz reset` treats it —
cleared by default, cleared only when a flag names it, or never cleared.

No component SHALL derive a root of its own. A module that writes under the data root
SHALL name its subtree by its declared key and SHALL obtain the directory and any file
inside it from the declaration, so moving the data root moves every subtree with it.

The socket directory SHALL remain a row whose location is resolved by the socket-directory
ladder and not by the data root, because the two are resolved from different sources and
`--storage-root` SHALL NOT move it. The declaration owns the names inside that directory;
it does not own the directory.

The log directory is not under the data root either: its default is relative to the
working directory, and neither the data root nor `--storage-root` SHALL move it.
`flz reset` SHALL report it where that default puts it.

`flz reset` SHALL report every row of the declaration, and SHALL clear exactly the rows
declaring that they are cleared by default or on a flag the user gave; a `never` row
is reported as kept. Adding a subtree to the declaration SHALL NOT silently enlarge what
`flz reset` destroys: a new row is `never` unless it says otherwise.

#### Scenario: Moving the data root moves every subtree

- **GIVEN** the Controller is configured with a data root other than `./storage/app`
- **WHEN** it writes durable state, auth state, the plugin catalog, the hub registry, the
  audit log, the DLQ and the schedule store
- **THEN** every one of those files SHALL be under the configured root, and no component
  SHALL write under `./storage/app`

#### Scenario: `flz reset` and the Controller name one tree

- **GIVEN** a data root the Controller has written under
- **WHEN** `flz reset --dry-run --json` reports every root, cleared or kept
- **THEN** each reported path SHALL be the path the Controller resolves for that
  subtree's declared key, reported with the subtree's declared reset mode

#### Scenario: A subtree the declaration marks `never` is not cleared

- **GIVEN** the Controller is not running, the data root holds every declared subtree, and
  the log directory holds a log file
- **WHEN** the user runs `flz reset --yes --all`
- **THEN** the row declared `never`, the log directory, SHALL be untouched and reported as
  kept, and only the rows declaring `default` or a flag SHALL be cleared

#### Scenario: The socket row is not under the data root

- **GIVEN** the socket-directory ladder resolves to a per-user runtime directory
- **WHEN** the layout is asked for the socket row's directory
- **THEN** it SHALL answer the ladder's directory, unchanged by `--storage-root` and
  by the data-root configuration key

