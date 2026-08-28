# `flz reset` resolves the socket directory

Implementation notes for `fix-reset-socket-dir-resolution`. Gotchas only; the
normative text is `openspec/specs/cli-runtime-data-reset`, and the
operator-facing material is `docs/operations/reset.md` § The socket directory.

## `Role::Controller` is the resolution reset needs, and the reason is the skip

The design says `Role::Controller`, and the argument it gives ("a controller
answers with one directory rather than refusing on ambiguity") is the weaker
half. The load-bearing half is `first_creatable`'s skip: the controller walk
**steps over a candidate whose control socket answers** and takes the next
rung. That is exactly the colocation arrangement — controller A holds
`$XDG_RUNTIME_DIR/flezha`, controller B walks past it to `/run/flezha` or
`app_data/sockets` — so asking as the Controller answers with *this*
controller's directory, which is what a reset acts on.

`Role::Runner` would have been wrong for the same reason it is right for a
runner: `first_live` **prefers** the directory that answers, so on a colocated
box it would have named the peer's directory and reset would have kept it and
never cleared this controller's stale sockets.

A consequence worth knowing: with `Role::Controller`, `Reach::Reachable` on the
resolved directory is reachable almost exclusively through the **explicit**
rung, because that rung is exempt from the skip. That is the shared-directory
case `support-multi-controller-colocation` refuses at boot — and the one case
where reset could otherwise unlink a live peer's socket.

## Resolving establishes a directory, so a run that was not asked does not ask

`resolve_socket_dir(Role::Controller, …)` calls `create_dir` on the candidate
it takes — that is what "creatable" means. So a plain `flz reset --yes`, which
does not touch sockets, must not consult the ladder at all, or it would
`mkdir -p $XDG_RUNTIME_DIR/flezha` on a box where no controller had ever run.
`build_plan` therefore resolves only when `--include-sockets` (or `--all`) is
in effect; a socket root that was not asked about has **no path**
(`Root.path: Option<PathBuf>`, reported as `socket directory`). The same slot
carries a resolution refusal, which is why `D5`'s "report and continue" needed
no second representation.

## A regular file named `runner_control_channel.sock` is not a stale socket

`probe` connects, and connecting to a regular file fails with `ENOTSOCK` →
`Reach::Unreachable`, not `StaleSocket`. So every fixture that means "a
controller died hard" has to `bind` a **real** socket and drop the listener;
`fs::write` produces a file the command correctly refuses to unlink. The same
trap is recorded in `docs/dev_notes/support-multi-controller-colocation.md`.

The second fixture trap is `SUN_LEN`: 108 bytes including the file name. The
first live drive of this change ran under the session scratchpad
(84 characters) and every probe came back
`Unreachable("path must be shorter than SUN_LEN")` — correct behaviour,
useless drive. Socket fixtures need a short base.

## The drive found a defect no test had: sockets counted as zero

`walk_for_stats` counted an entry when it was `is_symlink() || is_file()`, and
recursed when `is_dir()`. **A socket inode is none of those**, so the first
`--include-sockets` run unlinked two sockets and printed `(0 files, 0 B)` —
the same class of failure as the one this change exists to fix, in the audit
trail rather than the path. The count is now "everything that is not a
directory", which also picks up fifos and device nodes an operator may have put
in a shared directory. Only a live drive showed it: every unit fixture up to
that point used `fs::write`.

## One entry list, not two equal ones

The dry-run/apply equivalence used to rest on `walk_for_stats` and `clear_root`
each doing their own `read_dir` and happening to filter identically. They now
share `entries_to_clear`, and `measure_and_clear` owns the whole per-root
action — measure the list, unlink the same list when applying. The measurement
is returned even when the unlink fails partway, because the exit-code
heuristic reads it to tell "partial" (2) from "before any action" (3).

## What the refine passes changed

- **`diagnose` takes the role.** It was `resolve_socket_dir(Role::Runner, …)`
  followed by `probe`; reset needed the same pairing for the Controller and had
  spelled the two calls itself. One `diagnose(role, explicit)` now serves the
  runner's pre-retry diagnosis, `flz runner check` and reset, so the probe can
  never be run against a directory other than the one just resolved.
- **`Scope` collapsed into `Source`.** Two enums encoded one distinction, and
  the correlation is not accidental: the socket root is name-scoped *because*
  the ladder produced it and the ladder may answer with a directory an operator
  named. `Source::covers/1` is the whole of it.
- **`PlannedRoot` + `RootOutcome` collapsed into `Root`**, mutated in place.
  The loop had been hand-copying six fields from one to the other.
- **The operator wording left the planner.** `label` was a `String` field
  written at three construction sites; it is `Root::label/0` beside
  `Root::name/0` (the `--json` spelling), so the printer owns how a root reads
  and the planner owns only what happens to it.
- **`ClearMode::OptIn` carries the field, not just the flag name**
  (`fn(&Flags) -> bool` in the const table). The old shape matched the flag
  string in a second place with a `_ => false` arm, where a typo would have
  meant "never in scope", silently.

## The `flzvm rpc` guard is deliberately not widened

It asks whether *this* controller's BEAM node is up. A colocated peer is a
different node, so it answers "not up" truthfully while the peer serves — and
that is correct, because the peer's durable state is not under this reset's
`app_data/` root and is not this reset's to protect. What a peer shares is the
socket directory, and that is guarded in `plan_socket_root`, where it is
touched. Widening the node guard would refuse the whole run and make
`flz reset` unusable wherever colocation is in use.
