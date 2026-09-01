# collapse-observability-source-behaviours — implementation notes

## The behaviour was undeclarable, which is why it had no implementers

[[KI-100]] recorded the symptom — `grep -rn "@behaviour
Flezha.Observability.Source" lib/ test/` returns nothing — and read the cause as
an omission: *"a macro that never declares the behaviour"*. Measured at the tree,
the cause is stronger than that. Declaring it was **refused by the compiler**:

```
$ mix compile --warnings-as-errors
    warning: conflicting behaviours found. Callback function init/1 is defined by
    both GenServer and Flezha.Observability.Source (in module
    Flezha.Observability.OtpBackend.BeamVmSource)
    … five, one per source …
Compilation failed due to warnings while using the --warnings-as-errors option
```

`Flezha.Observability.Source` declared `init/1`; every process-backed source is a
GenServer, which declares `init/1` too. So the behaviour could not be declared by
the one flavour whose paragraph in the spec describes it, and the only states
available were the one the tree was in — never declared — and a build carrying
five warnings.

**`init/1` is therefore deleted from the behaviour**, with the
`@type config :: map()` that only its signature used. Not a tidy: it is what
makes the collapse buildable. It is also the right shape for the *other*
flavour — a declarative source is application code calling `Bus.emit/1`, owns no
process and has no `init/1` to offer — so `init/1` was wrong for **both**
flavours and belonged to neither. What survives is `config_schema/0`: the
registration-time schema, the same callback `Flezha.Observability.Sink` declares.

`observability-foundation`'s own scenario *"A declared contract has no
implementation behind it"* asks that a callback nothing invokes be removed
"because … every implementer writes a stub for it". The overridable default in
the template answers the second half — **no implementer writes a stub**.
`config_schema/0` is kept rather than removed because removing it leaves the
behaviour with zero callbacks, which is deletion under another name, refused on
the measurement below.

## Deletion was refused on three registrations, not on taste

Three live registrations are `flavour: :declarative` —
`telemetry_bridge.ex:171`, `registry.ex:252` and `:259` — and the poller template
cannot describe them, because it is `use GenServer` and they own no process.
Deleting `Flezha.Observability.Source` removes the only written contract for the
half that is actually in use.

## The five `*_source.ex` siblings keep their names, and that is the thesis

The template is renamed; the modules it builds are not. Each of the five
registers through `Registry.register_source/1`, appears in `list_sources/0` and
in `flz observe sources`, is named by `OtpBackend.Supervisor`'s `@sources`, and
now declares `@behaviour Flezha.Observability.Source`. The suffix names what the
module **is**; the collapse makes `*Source` more accurate, not less. Renaming
them to `*_poller.ex` recreates the collision in the other direction — five
modules that are sources, none of them called one.

Priced: 5 file moves, 5 `defmodule` lines, the 5-name `alias` block and 5-name
`@sources` list in `otp_backend/supervisor.ex`, the `source_modules/0` assertion
(5 more names), the alias block and `@lifecycle` table in `otp_backend_test.exs`,
`docs/operations/observability.md` and two `docs/refactor/` recordings — for
**zero** movement in any ratcheted metric, inside a **blocking** area.
`bounded_mailbox_source.ex` additionally carries the capacity sense of "bounded"
and is already allowlisted in the kind-naming gate; not disturbed.

## `+3` in a blocking area with zero slack, funded twice by cause

`lib/flezha/observability` is blocking at `prod_loc` **3933** and the tree
measures **3933 exactly** — no slack, and `--check` fails on `c > b`. The
collapse adds three production lines to the template (`@behaviour`, its `@impl`,
`def config_schema(), do: %{}`).

| site | Δ | cause |
|---|---|---|
| `otp_backend/poller.ex` — behaviour, `@impl`, default | **+3** | the collapse |
| `source.ex` — `@callback init/1`, `@type config` | **−2** | forced by the conflict above |
| `poller.ex` — `start_link/1` in the file's own one-line form | **−2** | charter §5 layout |

`start_link/1` was the only single-expression function in the file written
`do … end`; `handle_info/2`'s catch-all, `init_state/1`, `poll/1` and now
`config_schema/0` are all `, do:`. The line is 94 characters, inside the
formatter's 98, so `mix format` leaves it. Result **3932**.

**The instrument flatters until `tracked_files()` is patched.** The renamed file
is untracked until the supervisor stages it, and `refactor_metrics.py` reads
`git ls-files`, so an unpatched run reads the area **3876** — 57 lines low, the
whole file — and reports a green it did not earn. Quote nothing from an unpatched
run while a rename is unstaged.

## The eighth `RETIRED` entry, red first

`scripts/check-module-naming.sh` exists because `mix compile` catches a stale
**code** reference and nothing catches one in a moduledoc, a spec, a runbook or a
config comment. The entry went in **before** the rename: **23 occurrences across
9 files**, then `module naming OK`.

Three lines in `docs/known_issues.md` cite the retired name as history and take
the **inline** `module-naming-record` form — both markers on the one line they
exempt. That form cannot be left open, which the gate now fails, and it exempts
exactly one line rather than a block.

`docs/refactor/STATE.md` spells it <!-- module-naming-record:begin -->`OtpBackend.Source`<!-- module-naming-record:end --> — a partial path the patterns do
not match by construction, since each pattern is the **full** retired path with a
trailing word boundary.

## No delta reaches a Purpose paragraph

`openspec/specs/observability-otp-backend/spec.md:5` said the built-in sources are
*"registered through the foundation's `Flezha.Observability.Source` behaviour"*.
Both halves were wrong: a behaviour registers nothing —
`Registry.register_source/1` does — and the sources did not implement it at all.
A `## MODIFIED Requirements` block reaches requirements only, and `openspec
archive` merges requirements into the published file without touching Purpose, so
the correction is made **directly in the published spec** and survives the
archive.

## The proof is the runtime, and the control is one assertion

```
BeamVmSource behaviours=[GenServer, …OtpBackend.Poller, …Observability.Source]
  config_schema=%{} exported=true          … five, one per source …
Source callbacks: [config_schema: 0]
Poller callbacks: [poll: 1, init_state: 1]
```

Negative control: with the `@behaviour` line and its `@impl` removed, the
lifecycle test fails on exactly one assertion —
`right: [GenServer, Flezha.Observability.OtpBackend.Poller]` — 12 of 13 pass.
Restored, 13 of 13. The assertion engages rather than silently matching nothing.

The test extends the existing `@lifecycle` table rather than opening a file: a
new `async: false` module moves the blocking `test` area's ratcheted
`async_false_share`, which has no headroom.

## One full run cascaded and did not reproduce

A full `mix test` at seed **727597** reported `1416/1917 passed, 6 invalid,
501 failed`, the tail of them exiting on `GenServer.call(Flezha.Dlq.Store, …)`
*no process* — an application-wide cascade, not 501 independent failures. The
**same seed** on the **same tree** then ran `1923 passed`, as did seed 693042.
Same seed, opposite result: environment, not the change. No output from the bad
run survives, so it is a suspicion and not an entry.
