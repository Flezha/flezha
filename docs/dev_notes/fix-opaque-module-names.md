# Seven modules renamed, and the durable-name question settled first

**Provenance.** Reconstructed after the fact from the four landed commits
(`0cb58166`, `055054f2`, `1848cb80`, `14c7cfc5`) and the archived change
directory, which is untracked and exists on one machine. Nothing here is a
measurement this note took.

## An Elixir module name IS durable state, and thirteen modules are pinned by it

This was settled **before anything moved**, from the write path and the read
path rather than by inference. The chain, link by link:

1. The cspec's `start` MFA carries a module atom —
   `start: {Flezha.Operative.Overwatch, :start_link, [{namespace, operative}]}`.
2. `process_hub`'s distributed supervisor re-`apply`s it on restore.
3. A plugin's `provider` is a **module atom, not a string id**: the wire string
   `"operative_kafka_producer"` is resolved through `resolve_provider/2` and the
   **string is discarded**.
4. Nothing strips it before the write — `Plugin.strip_transient/1` is the
   identity.
5. Two `__struct__` tags ride along (`%Flezha.Operative.Builder{}`,
   `%Flezha.Plugin{}`).
6. It reaches disk at `<cspec_root>/<hub_id>/registry.dets`.
7. **The code knows it holds module atoms**: `declared_manifest.ex` decodes with a
   bare `:erlang.binary_to_term/1` while `manifest.ex`, which holds no module
   names, uses `[:safe]`. The asymmetry is deliberate — `[:safe]` refuses atoms
   not already loaded.
8. **The failure is silent.** `plugin/extension/table.ex` answers a provider that
   will not load with a `Logger.warning` and then reports the phase as
   `:running`. A renamed provider does not crash: restore "succeeds", the plugin
   never starts, and the operator sees a healthy row.

The thirteen pinned modules — do not rename any of them without the migration:
`Flezha.Operative.Overwatch`, `Flezha.Operative.Builder`, `Flezha.Plugin`, and
the ten providers named in `config/config.exs` (`…Kafka.Producer`, `…Postgres`,
`…Websocket`, `…HttpPoll`, `…BeamMsgForward`, `…StdOut`, `…Filter`,
`…MonitoredIOForward`, `…OperativeForward`, `…MessageForward`). The precedent for
the cost, if one ever must move, is
`rename-operative-kinds-to-stream-and-workflow`: **`flz controller stop && flz
reset --yes`**, controller and every runner redeployed together.

**`Flezha.Schedule.Store` is the counter-example worth naming**: it stores the
*string* provider id and re-resolves per firing. That is the pattern the cspec
does not follow.

Everything else was opened rather than assumed and is clean: the WAL record
(fixed 16-byte header plus opaque SDK bytes), the tier manifest, the input log,
the proc inbox and journal, the completion record, the audit log's JSONL and
INDEX, the DLQ, all four authz DETS stores plus `bootstrap.token`, the `.layout`
marker, the `.instance.lock`, and the ClassRegistry sidecar (a kind atom, never a
module). Telemetry event names are literal atom lists in `@prefix` attributes, so
folding a `Telemetry` sibling cannot move an event name; metric names come from a
signal's `source`/`name`, never from `__MODULE__`; and the seven
`{:via, Registry, …}` sites key on ids.

**Honesty note carried across**: the local `app_data/*.dets` files held **no**
`Elixir.Flezha` atoms when this was checked — the registries had no operative
rows on that box. The verdict is proven from code, not from a sample of bytes.

## The seven, and why each name was false

Three renamed, four folded into their parent — each had named a namespace it did
not earn:

| retired | today |
|---|---|
| <!-- module-naming-record:begin -->`Flezha.State.Registry`<!-- module-naming-record:end --> | `Flezha.State.Paths` (pure path resolution; not a registry) |
| <!-- module-naming-record:begin -->`Flezha.AuthZ.Store`<!-- module-naming-record:end --> | `Flezha.AuthZ.DetsStore` |
| <!-- module-naming-record:begin -->`Flezha.SDK.Backoff`<!-- module-naming-record:end --> | `Flezha.SDK.Jitter` |
| <!-- module-naming-record:begin -->`Flezha.Service.Parser`<!-- module-naming-record:end --> | folded into `Flezha.Service.Identificator` |
| <!-- module-naming-record:begin -->`Flezha.Plugin.Provider.Metrics`<!-- module-naming-record:end --> | folded into `Flezha.State.Metrics` |
| <!-- module-naming-record:begin -->`Flezha.Plugin.Provider.Kafka.Telemetry`<!-- module-naming-record:end --> | folded into `Flezha.Plugin.Provider.Kafka` |
| <!-- module-naming-record:begin -->`Flezha.Plugin.Provider.OperativeForward.Telemetry`<!-- module-naming-record:end --> | folded into `Flezha.Plugin.Provider.OperativeForward` |

Every moved region is **byte-identical**, verified by diffing each against its
pre-move copy; both `@prefix` literals and all eleven `:telemetry.execute` tuples
are unchanged. The one edit inside a moved body is the one the fold forces.

## The trap: a module reference in VALUE position

`wal_transfer.ex` passed the module as a value —
`:erpc.call(source_node, Registry, :delete_files, …)` with **no dot after it** —
so the alias move silently repointed it at the stdlib `Elixir.Registry`. Compiler
silent. Found by a bare-token sweep, and it is the whole risk class of an alias
pass: `:erpc.call(node, Mod, …)`, `apply(Mod, …)`, `{:via, _, {Mod, …}}`. It
shipped once in segment A and was then swept twice by two different methods — per
file over the 374 introduced bindings, and globally over every introduced last
segment. The global sweep surfaced seven value-position sites; all seven were
already bound at HEAD and the pass adds an alias for none of them.

## The gate is the only layer that sees a stale name in prose

Three layers, in order of what each can and cannot see:
`mix compile --warnings-as-errors` catches every **code** reference and nothing in
a string, a doc, a spec or a runtime-built atom; `mix test` catches a reference
that compiles but resolves wrong; and **`scripts/check-module-naming.sh` catches
the rest, and it is the only thing that does.** It extends
`scripts/naming_gate.py` per charter §4 rather than adding a second gate, fails
on any surviving occurrence of the seven retired paths in any tracked
human-readable file, and was proved **red first**. Each pattern is
`re.escape(path) + \b`, which is what keeps `Flezha.AuthZ.StoresTest` out by
construction rather than by exemption.

**Its `record_tag` exemption had to cover six historical citations across three
places** — three dev notes, `docs/known_issues.md` and the refactor board.
Scoping it to `docs/dev_notes/`, as the task list said, would have made the gate
**red on the day it landed**.

## The alias rule, and the credo ratchet

One rule applied to all 689 `AliasUsage` sites (lib 220, test 469): **a bare
`alias` when the last segment is unique across `lib/`, and
`alias …, as: <Qualifier><Segment>` when it is not.** `AliasOrder` stayed at 66
across ~650 insertions, because every insertion is alphabetical. Nine `as:`
spellings were needed; two were not in the design and are the ambiguity the rule
exists to stop importing — `ProcessHub.Service.Recovery` collides with
`Flezha.State.Recovery`, and `KafkaEx.Auth.Config` with `Flezha.AuthZ.Config`.
Both are non-Flezha modules whose last segment names a Flezha one.

Only **7 of the design's 24 candidates** were reachable by an `AliasUsage` finding
at all: the other 17 are reached solely through `@behaviour`, `use`, a struct
literal or an existing alias, and **credo ignores `@`-rooted and non-call
references**.

The diff's shape was proved rather than reviewed: for each of 189 files, a shadow
tree of "HEAD plus only alias insertions and prefix removals" compares equal as a
**metadata-stripped AST** — 0 of 54 differ in `lib`, 0 of 135 in `test`.

Credo total **1 463 → 378**, with `ParenthesesOnZeroArityDefs` (431) **rejected
and disabled** rather than satisfied. 378 is a measured number that happens to
equal `1498 − 431 − 689`. `credo_warnings` becomes an integer in all 36 areas and
at the repo level, replacing `null`, so **that metric ratchets for the first
time**.

## `.credo.exs`'s `~r"^test/apps/"` exclusion was inert and always had been

`include/2` builds the file set from `Path.expand`'ed patterns, so it holds
**absolute** paths, while `convert_globs_to_local_paths/2` expands only
`is_binary` patterns and passes a `%Regex{}` through untouched; `exclude/2` is a
bare `String.match?`. A `^`-anchored *relative* regex can never match
`/home/…/test/apps/…` — the three siblings work only because they are
unanchored, which is now stated in a comment beside them. It was fixed **with the
baseline** rather than later, because a full run reports zero findings under
`test/apps` today, so honouring the exclusion moves the number by 0 now and would
otherwise be a silent ratchet shift the first time anyone writes a flaggable
construct in a test app. Proved two-sided: byte-identical probe modules inside
and outside `test/apps` in one run, the outside copy reported by two checks and
the inside copy by neither.

## Recorded, not resolved

- Net **+459 lines across all four segments**, and **no charter rule is cited for
  it** — charter §1 admits only §2's reuse order or a §7 ceiling, and this is
  neither. The lines exist because the user ruled on KI-10. Recording that is the
  point; a §1 exception that names no rule is one that was not earned.
- `Flezha.AuthZ.DetsStore`'s **destination namespace** is still wrong:
  `Flezha.Schedule.Store` depends on it, so it is shared code sitting in a
  sibling's namespace (charter §3). The rename makes the name true and leaves the
  layering defect visible.
- <!-- module-naming-record:begin -->`Flezha.Plugin.Provider.Metrics`<!-- module-naming-record:end --> was **dead** — zero callers for either public
  function. It was folded rather than deleted, because deleting production code
  needs a coverage argument this change did not make.
- <!-- module-naming-record:begin -->`Flezha.Observability.OtpBackend.Source`<!-- module-naming-record:end --> was classified KEEP here, with the note
  that a name like `PolledSource` would read better at the `use` site. That was
  later carried much further by `collapse-observability-source-behaviours`, which
  renamed it `…OtpBackend.Poller` and found the real defect underneath.
- Filed on the way: KI-61 (two `RecordLog` `@spec`s naming a type `Elixir.Registry`
  does not export), KI-62 (`convert_value/2` not running on the struct plugin
  path — ruled **not a bug**, the two paths have two written contracts), KI-63
  (the inert credo exclusion, fixed in segment D), KI-64 (`config/config.exs` and
  `mix.exs` sit outside every area in the scan table, so their findings are
  ratcheted by nothing) and KI-65 (`check-kind-naming.sh` getting redder every
  time the board describes it — the fix is the `record_tag` mechanism, not a
  sixth allowlist entry, and that is what `close-kind-naming-gate-red` did).
