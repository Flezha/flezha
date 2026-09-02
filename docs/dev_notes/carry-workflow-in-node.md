# Node carries the workflow kind (`carry-workflow-in-node`)

`test/apps/node_runner/fixtures_workflow.mjs` holds nineteen twins of the
reference's durable structured workflow fixtures
(`test/apps/rust_runner/src/workflow.rs`, `workflow_loop.rs`) under the same
factory ids, procedure names, channel names and output bytes. The manifest
defers nothing, `index.mjs` declares the module, and `bench.fleet` accepts node
on the workflow scenario. No `test/system/` file is touched: flipping `deferred`
to `[]` is what un-skips the scenarios that already exist.

## The model the fixture module rests on

A procedure node owns **exactly one output channel**, not one per outgoing
edge. So the whole topology vocabulary is handle reuse:

- a **fan-out** is one held handle wired twice — `branch_workflow`'s `mid`
  reaches `BranchB` and `BranchC`, `diamond_workflow`'s `dfan` reaches `DiamB1`
  and `DiamC`. Both consumers read exactly what the producer released on that
  one channel;
- a **fan-in** is two arms naming the same procedure with no node name — two
  unnamed `.into(DiamJoin)` mentions are one node, dispatched once after both
  arms commit;
- `.named()` names the channel the producer releases on, and a `sink()` on it
  is a hybrid channel: the same release reaches a plugin and the next
  procedure.

## The three factories

`durableStep(name, {change, emit, compensate, gate})` is the reference's
`durable_step!` macro with four axes instead of two. The macro parameterises
`emit` and `compensate`; `change` (which value is recorded) and `gate` (whether
the step waits for the release gate **before** recording it) are what the
reference varies by hand-writing the procedure instead. Carrying all four here
is what lets every durable procedure in the module be a single call.

`statelessStep(name, handle)` is the reference's `stateless_step!` — no state,
no change, the handler is the whole behaviour. `failingStep` is the terminal
one-liner that makes a saga unwind.

`benchChain(factoryId, steps, persistent)` builds the four bench chains over
sixteen generated `bench_s<n>` procedures. Each chain position needs its own
procedure, so they are generated rather than written out, and the edge names
follow the reference, which spells them from the step **type**
(`edge_BenchS<n>`) rather than from the procedure name.

## The release gate never settles, and never uses a timer

`blockUntilReleased()` returns `new Promise(() => {})` when
`FLEZHA_WORKFLOW_E2E_RELEASE` is absent. The harness kills the first runner and
launches the replacement with the variable set, so the gate is a boot-time
decision: never settling on the first runner, resolving at once on the second.

Two independent reasons forbid a timer, and either alone is sufficient.

1. **The contract.** On an event-loop host the parked activation must suspend
   and *yield the loop*. A synchronous hold freezes every other activation the
   loop owns — which is precisely the wedge signature `pool_wedge`'s fixtures
   exist to produce, so a workflow fixture that held the loop would make the
   wedge drill unfalsifiable. A promise that never settles suspends the one
   activation and leaves the loop free to answer control pings and dispatch a
   sibling procedure.
2. **The ratchet.** `.mjs` classifies as `ts` in `scripts/refactor_metrics.py`,
   whose `SLEEP["ts"]` is `setTimeout\(\s*(?:resolve|r)\b` and whose `sleeps`
   row is **blocking** on `test/apps`. The obvious
   `await new Promise(r => setTimeout(r, …))` spelling of the same wait is a
   gate failure, not a style preference.

`grep -n "setTimeout\|Atomics.wait\|sleep"` over the file returns nothing.

## `items` is a sorted array because a `Set` has no durable form

The reference's `SeenState` is a `BTreeSet<String>`, chosen so snapshot bytes
are deterministic. `sdk/node/src/codec.ts:109` refuses a `Set` at definition —
*"a Set decodes as a plain object or array"* — so the twin is the array it would
decode to, kept sorted by `insertSorted` for the same determinism and idempotent
on insert for the same re-run-after-resume.

## Every sink declares its encoder

The reference's bare `.sink(name)` serialises a `String` as UTF-8; this SDK's
plugin default is MessagePack. The e2e assertions compare **raw binaries**
(`test/system/workflow_crash_resume_e2e_test.exs:201`), so an unencoded sink
would deliver MessagePack bytes against a UTF-8 expectation. Every
`sink(name, encode)` in the module therefore passes `bytes` or a
`bytes`-wrapping lambda.

The **edge** encoding is untouched. An inter-procedure hop between two node
procedures carries MessagePack, and the core derives that channel's journal
codec from the channel's kind (`flz_core/src/host/register.rs`), so no fixture
declares one — and a fixture that reached for the edge encoding would break the
invariant that a sink encoder is the sink's alone.

## KI-141 is answered on the wire, and the control proves the test measures it

The diamond is the fan-out's proof. Comment out the `fan.into(DiamC)…` arm and
re-run only the no-crash diamond and it goes red on **both** transports:

```
1) test transport=quic a fan-in diamond joins once with no crash at all
   test/system/workflow_crash_resume_e2e_test.exs:180
   the fan-out multiplied the released batch: ["a:{\"seed\":\"go\"}", "b:{\"seed\":\"go\"}", "j:b:{\"seed\":\"go\"}"]
   code: assert Enum.sort(delivered) ==
   stacktrace:
     test/system/workflow_crash_resume_e2e_test.exs:201: (test)

Result: 0/2 passed, 11 excluded
```

Three payloads where five are asserted; `c:` and `j:c:` absent. With the arm
restored the same two pass. A control that stayed green would mean the
assertion is not measuring the fan-out at all; this one measures it, so the
one-channel-per-procedure model is proven on the wire and not only in a
declaration golden.

## KI-147: one declaration builder for two kinds

Each veneer had a single declaration builder emitting `effectively_once` for
stream and workflow alike. `flz_core::host::register::refuse_workflow_facts`
(`flz_core/src/host/register.rs:105`) rejects that key on a workflow class, so
the node runner died at boot the moment it declared its first workflow fixture:

```
Error: a workflow class cannot declare effectively_once
    at Runner.declareAll
```

Seven keys moved behind the kind — `effectively_once`, `correction_window_ms`
and the checkpoint family that rests on the first — while `sync_durability`,
`persistent` and `compact_after_bytes` stay on both kinds. The flag itself stays
on the spec: `Ctx.stampIdentity` and its siblings read it to decide whether a
keyed plugin frame carries its own identity. Only the **wire declaration** is
gated. The keys moved into a kind-gated helper rather than into a
`forStream`/`forWorkflow` pair four times, because the veneers already carry
`kind` on one ClassSpec.

The half worth keeping: **four SDK suites were green over a defect that kills
the runner, because four tests pinned it.** `sdk/node/test/authoring.test.mjs`,
`sdk/ruby/test/authoring_test.rb`, `sdk/python/tests/test_authoring.py` and
`sdk/php/tests/AuthoringTest.php` each asserted
`to_host_class_decl(workflow)["effectively_once"] == true`, each citing the
reference in a comment — but the reference declares the flag in **source** only,
and Java never puts it on the wire. All four now assert the key is absent while
the spec flag stays true. A test that pins the defect is worse than no test,
and only a live runner boot could tell them apart.

## What runs on node, and what still spawns rust

`FlezhaTest.SystemCase` stamps `@moduletag skip: deferral(capability)`, so the
manifest flip un-skips every `capability: :workflow` module. What then spawns a
**node** runner is decided by `reference_runtime: true` in the describe title.
Sampling `ps` through each run under `FLEZHA_E2E_RUNTIME=node`:

| module | result | namespaces spawned |
|---|---|---|
| `workflow_crash_resume_e2e_test.exs` | 13 passed | **8 node** (6 crash, 2 no-crash) + 5 rust |
| `workflow_kind_e2e_test.exs` | 1 passed | **1 node**, driving `echo_workflow` |
| `workflow_control_flow_e2e_test.exs` | 8 passed | 8 rust, **0 node** |
| `sdk_smoke_e2e_test.exs` | 25 passed | node, incl. the two workflow declaration goldens |

`remote_durability_e2e_test.exs` carries a per-test `capability: :workflow` tag
but stays skipped behind `:postgres_integration`, which needs a container.

A namespace is not a cell: `workflow_control_flow`'s eight rust namespaces come
from its **four** pinned describes. The eight `(reference/uds)` cells that still
spawn rust are those four — the loop, the rewind, the retry ladder and the
branch rollback — plus four in `workflow_crash_resume`: the tier, the opt-out
and the two outbox drills. Each is pinned in its own describe title because it
proves **controller** mechanics and merely needs a runner to drive them.
Unpinning them changes what runs for java too, since java is the only other
carrier, so it is one cross-runtime widening with a multi-SDK proof obligation
and belongs to the programme's close-out, where a single unpin serves five
runtimes.

Reading a `ps` sampler over any run: every `mix test` also boots **two**
namespace-less reference runners from `config/runtime.exs:12-13`
(`FLEZHA_EXTERNAL_PROCESSES`). They belong to no module's fleet, appear in every
run whatever `FLEZHA_E2E_RUNTIME` says, and are not evidence that a scenario
spawned rust.

## The `self_terminating` gap stays latent

Both references refuse a workflow that is neither self-terminating nor
duration-capped — Rust `BuildWorkflowError::ExternalStopOnly`
(`sdk/rust/src/workflow/spec.rs:25`), Java
`WorkflowSpec.java:199` (*"is not terminable: its only possible exit is an
external stop"*). **No veneer carries either the declaration or the refusal**:
`grep -rn "self_terminating\|selfTerminating" sdk/{node,ruby,python,php}`
returns nothing. All four loop fixtures here declare `maxDurationMs`, so nothing
in this set trips it. It is a missing *refusal* in four languages, which is its
own change.
