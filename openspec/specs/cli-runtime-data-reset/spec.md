# cli-runtime-data-reset Specification

## Purpose

The cli-runtime-data-reset capability defines a top-level `flz reset` CLI subcommand that clears the Controller's durable runtime state on disk (per-Entity WAL/snapshot tree, ProcessHub durable registry; optionally certs and sockets) so an operator can deterministically start fresh from a clean slate. The capability owns the filesystem walk (clear *contents* of well-known `app_data/` subdirectories, never the parent itself), a hard live-node guard that refuses to run while the Controller is up, idempotent semantics over missing directories, scriptable flags (`--yes`, `--dry-run`, `--include-certs`, `--include-sockets`, `--all`, `--app-data-root`, `--json`), the audit-trail output enumerating cleared paths and byte counts, and the stable exit-code contract that lets scripts distinguish "refused" from "bad usage" from "I/O error".

## Requirements

### Requirement: `flz reset` clears Controller durable runtime data on disk

The CLI SHALL provide a top-level `flz reset` subcommand that clears the Controller's durable runtime state under `app_data/` so an operator can deterministically start fresh. By default the command SHALL clear the contents of:

- `app_data/state/` — per-Entity WAL/snapshot/outbox files written by `operative-state-persistence`.
- `app_data/process_hub/` — ProcessHub durable registries written by `auto-restore-on-boot`.

The command SHALL leave `app_data/certs/` and `app_data/sockets/` intact unless explicitly requested via `--include-certs`, `--include-sockets`, or `--all`.

The command SHALL NEVER delete `app_data/` itself; it only clears the contents of the listed subdirectories. A path-equality assertion SHALL prevent unlinking `app_data/` or any of the cleared roots themselves.

The command SHALL be idempotent: a missing root directory is success, not error.

#### Scenario: Default reset clears state and process_hub but keeps certs and sockets

- **GIVEN** the Controller is not running and `app_data/` contains `state/`, `process_hub/`, `certs/`, and `sockets/` subdirectories with files
- **WHEN** the operator runs `flz reset --yes`
- **THEN** `app_data/state/` is empty, `app_data/process_hub/` is empty, `app_data/certs/` is unchanged, `app_data/sockets/` is unchanged
- **AND** `app_data/` itself still exists with the same inode

#### Scenario: `--all` clears every documented subdirectory

- **GIVEN** the Controller is not running and `app_data/` contains all four subdirectories
- **WHEN** the operator runs `flz reset --yes --all`
- **THEN** all four subdirectories exist and are empty after the command returns

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

- **GIVEN** the Controller crashed leaving `app_data/sockets/runner_control_channel.sock` on disk but no live BEAM node
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

`--dry-run` (or absent `--yes`) SHALL substitute the verb `would clear` / `would keep` and the JSON `dry_run` flag SHALL be `true`.

The walk that computes file counts and bytes SHALL be the same in dry-run and apply modes so the preview matches the action.

#### Scenario: Human output enumerates roots

- **WHEN** the operator runs `flz reset --yes` against a populated `app_data/`
- **THEN** stdout contains lines such as `cleared app_data/state/ (12 files, 4.3 MiB)` for each cleared root, `kept app_data/certs/ (use --include-certs to clear)` for each kept root, and a final `reset complete in <N> ms` line

#### Scenario: `--json` output is a single object

- **WHEN** the operator runs `flz reset --yes --json`
- **THEN** stdout contains exactly one JSON object with `cleared` (array of `{root, files, bytes}`), `kept` (array of `{root, reason}`), `duration_ms` (integer), and `dry_run: false`

#### Scenario: `--dry-run` matches `--yes` plan exactly

- **GIVEN** a populated `app_data/`
- **WHEN** the operator runs `flz reset --dry-run --json`, captures the output, then runs `flz reset --yes --json` immediately after
- **THEN** the per-root `files` and `bytes` values in the second invocation match the first (modulo any change between the two runs, which there should not be)

### Requirement: `flz reset` resolves `app_data/` root from the same source as the Controller

`flz reset` SHALL resolve the `app_data/` root using, in order: the explicit `--app-data-root <PATH>` flag if given; the `FLEZHA_APP_DATA_ROOT` environment variable if set; otherwise the default `./app_data` relative to the current working directory.

This resolution SHALL be implemented in a shared helper in `cli/src/utils.rs` (or equivalent) so other CLI subcommands that touch `app_data/` agree on the location.

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
