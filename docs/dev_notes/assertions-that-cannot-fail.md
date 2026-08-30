# `assertions-that-cannot-fail` — dev notes

## The defect is not the path; it is that nobody ever saw the assertion fail

`Flezha.State.Registry.class_dir/1` is three segments —
`<state_root>/<namespace>/<operative_class>`. Six sites built two:

```elixir
Path.join(Registry.state_root(), operative_class)
```

Five were `refute File.exists?` on that path. Since the system never writes
there, they passed whether or not the deletion they guard worked, and would have
kept passing after it broke. The sixth was `File.rm_rf` on the same path: it
reclaimed nothing and leaked the tree.

**Correcting the path is half the fix.** A corrected assertion that has still
never been seen fail carries exactly as much information as the one it replaced,
so each of the five was proved twice — vacuous before, red-able after.

## The proof protocol, and what each site answered

For each `refute` site: put the subject *genuinely at the real path*
(`Path.join([Registry.state_root(), namespace, class])`, or `class_dir/1` where
no namespace is in scope), run, and record. Then correct the assertion, run
again on the same subject, then remove the subject and run once more.

| site | subject present, OLD assertion | subject present, NEW assertion | subject removed |
|---|---|---|---|
| `class_registry_test.exs:99` | **34 passed** | **red** | green |
| `partition_group_rescale_test.exs:169` | (same run) | **red** | green |
| `partitioned_operatives_e2e_test.exs:185` | **2 passed** | **red** | green |
| `remote_durability_e2e_test.exs:110` | **3 passed** | **red** | green |
| `remote_durability_e2e_test.exs:205` | (same run) | **red** | green |

`remote_durability_e2e_test.exs` is `@moduletag :postgres_integration` and
self-skips: it needs `POSTGRES_INTEGRATION=1`, `--include postgres_integration`,
and `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink`. Its
two sites are therefore *not* covered by a plain `--include system` run and were
proved separately. **Never `docker compose down` here** — a sibling project
shares the compose project name; stop `flezha_postgres_sink` by name.

**Every one is green against real behaviour**, so nothing had to be escalated:
`ClassRegistry.put_effectively_once(class, false)` really writes no layout,
`PartitionGroup.rescale/3` really removes the vacated member's tree,
`TierE2EHarness.destroy_local_state/1` really destroys it. The change makes five
tests able to see a break that has not happened.

## The `rm_rf` is proved by a count, never by an assertion

An `rm_rf` on the wrong path fails silently. Its proof is the disk: snapshot
`app_data/state`, run one scenario, snapshot again.

```
$ mix test --include system test/system/workflow_crash_resume_e2e_test.exs:692
1 passed
+ app_data/state/workflow_loop_boot_2082
+ app_data/state/workflow_loop_boot_2082/chat_loop_workflow_2114   (3 sidecars)
```

after its own `on_exit` cleanup ran. With the fix the same run diffs to nothing.
That module's private `cleanup/3` was called from **seven** sites, the last
being `drive_crash_resume/5`, so it leaked one class tree per scenario per run —
and a leaked tree is not merely untidy: `namespace_of_class/1` falls back to
`disk_namespace/1`, which scans the root for any namespace directory holding a
directory of that class name, so a leftover captures the *next* run whose
`unique/1` integer collides. That is the mechanism that failed
`effectively_once_checkpoint_e2e_test.exs:37` 11 times in 40 before
`fix-first-input-drop-on-bind` cured the same mistake in
`TierE2EHarness.cleanup/3`.

The private `cleanup/3` is gone entirely rather than repaired: the module now
imports `TierE2EHarness.cleanup/3`, which is where the one teardown lives. A
private copy of a shared helper is *how* this defect survived the file's whole
lifetime.

## The sweep: 46 sites, exactly five vacuous

Every `refute File.exists?` / `refute File.dir?` in `test/` was read against the
layout it asserts on. **41 are sound**, for one of two reasons — they
derive the path from the module that owns it (`Registry.wal_path/1`,
`journal_path/1`, `inbox_path/1`, `input_log_path/1`, `outbox_dedup_path/1`,
`Bootstrap.recovery_marker_path/0`), or the subject is provably created earlier
in the same test (`audit_log_test.exs:476`, `controller_colocation_test.exs:72`
— which asserts `File.exists?(stale)` first, the best form in the tree —
`durability_tier_test.exs:607`, `registry_namespace_test.exs:80`/`:104`).

Two build a path by hand and were checked against the function that writes it
rather than waved through: `dlq/store_test.exs:110` matches
`Dlq.Store.blob_path/3` (`Path.join(proc_dir(rc, sh), id <> ".bin")`) and its
blob is written by the `Store.append` two lines up; `schedule/hub_test.exs:106`
matches `HubManager.registry_path/1`'s directory (`Path.join(root,
Atom.to_string(hub_id))`, which that function `mkdir_p!`s), so the schedule hub
writing anything durable would make it exist.

Three that look wrong and are **correct — do not "fix" them**:

- `state_path_migrator_test.exs:42`–`:43` join the flat `<root>/<class>`
  deliberately: that *is* the pre-migration layout whose absence they assert.
- `registry_namespace_test.exs:80`/`:104` hand-build `<root>/<ns>/<class>`
  because that module's subject **is** the layout; deriving them from
  `class_dir/1` would make them follow the resolution they exist to pin.
- `quic_test.exs:27` and `tier_e2e_harness.ex:211` name the **namespace**, not
  the class.

Two more were re-derived on the same rule though they were not vacuous:
`retention_test.exs:116` (`Path.join([root, "_system", "bnd_old"])` →
`Registry.class_dir("bnd_old")`, and proved red-able the same way) and
`bootstrap_test.exs:30`, which rebuilt `Path.join(auth_dir, "bootstrap.token")`
two lines above a sibling reading the `:bootstrap_token_path` config key that
`AuthTestHelper` defines as exactly that join.

## The seed is seven moduledocs, not two

The proposal named `registry.ex:6` and `:474`. Reading every documented layout in
`lib/` against the code that builds it found **seven**, across five modules, all
dropping the `<namespace>` segment their own code writes through `class_dir/1`:

| module | said | builds via |
|---|---|---|
| `state/registry.ex` (moduledoc, `all_entities/0`) | `<root>/<class>/<proc>/` | `class_dir/1` |
| `operative/class_registry.ex` | `<root>/<class>/` | `sidecar_path/2` → `class_dir/1` |
| `state/completion.ex` | `<root>/<class>/completion.record` | `Completion.path/1` → `class_dir/1` |
| `state/input_log.ex` | `<root>/<class>/input.log` | `Registry.input_log_path/1` |
| `state/proc_inbox.ex` | `<root>/<inst>/<inst>/inbox.wal` | `Registry.inbox_path/1` |
| `state/proc_journal.ex` | `<root>/<inst>/<inst>/proc.journal` | `Registry.journal_path/1` |

plus four in `docs/` (`glossary.md`, `architecture/workflow_profile.md`,
`resilience/workflow_operative.md` twice).
`docs/internals/state_persistence.md` was already right — it is the one place to
trust if the others disagree again. **Check a layout comment against the code,
never against the other comment**: in `registry.ex` two of three descriptions
disagreed with the module, and the majority was wrong.

## `namespace_dir/1` — the level above `class_dir/1` had no owner

The refine pass found the same defect shape one level up: four sites outside the
registry rebuilt `<state_root>/<namespace>` by hand, two of them production
(`AuthZ.Quotas.compute_disk_bytes/1`, `Convergence.Loop.namespace_dir_mtimes/0`)
and two test (`TierE2EHarness.cleanup/3` — the teardown seven crash-resume
scenarios now route through — and `quic_test.exs:27`).
`Registry.namespace_dir/1` now owns it and `class_dir/1` composes on it, so the
whole durable layout has exactly two definitions and neither is at a call site.
`disk_namespace/1` collapsed onto `namespaces/0` in the same pass — both were
"list the root, keep the directories".

## Traps

- **A `mix test` run must never be piped through `| tail`.** The failure block
  prints long before the summary. Redirect to a file and grep it.
- The Bash tool's own default timeout is **120 s** regardless of a `timeout 580`
  prefix; `--include system` needs ~3 min and is killed at 2 without an explicit
  tool timeout. The kill leaves no orphan `beam.smp`, but check.
- `scripts/format-check.sh` exits 1 at its **first** step on this box — `missing
  rubocop` — before it reaches any Elixir. `mix format --check-formatted` is the
  Elixir half and is clean. Unrelated to any change that touches no Ruby.
- A test file this session edits is not recompiled by `mix compile`; only the
  test run type-checks it. Run the touched files before concluding anything from
  a clean compile.
