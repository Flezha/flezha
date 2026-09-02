# Ruby carries the workflow kind (`carry-workflow-in-ruby`)

`test/apps/ruby_runner/fixtures_workflow.rb` holds nineteen twins of the
reference's durable structured workflow fixtures
(`test/apps/rust_runner/src/workflow.rs`, `workflow_loop.rs`) under the same
factory ids, procedure names, channel names and output bytes. The manifest
defers nothing, `runner.rb` registers them through `define_workflow`, and no
file under `test/system/` is touched: flipping `deferred` to `[]` is what
un-skips the scenarios that already exist.

The model — one output channel per procedure, a fan-out as one held handle
wired twice, a fan-in as two arms naming one procedure with no node name — is
change 1's and is written up in `unify-veneer-procedure-output-channel.md` and
`carry-workflow-in-node.md`. What follows is what Ruby decides for itself.

## The release gate is a never-fed queue, and three things rest on that

```ruby
RELEASE_GATE = Thread::Queue.new          # nothing ever pushes

def self.block_until_released
  RELEASE_GATE.pop unless ENV.key?(RELEASE_ENV)
end
```

The harness kills the first runner and launches the replacement with the
variable set, so the gate is a **boot-time decision**: never answering on the
first runner, returning at once on the second.

1. **One spelling satisfies the contract under both executors.**
   `Thread::Queue#pop` is scheduler-aware, which is the property this SDK's own
   fiber executor is built on — `executor.rb:227` calls the queue between
   fetchers and fibers *"scheduler-aware, so a handler's I/O yields instead of
   blocking a thread"*, and `:273` relies on *"a scheduler-aware pop"*. Under
   the thread executor the pop parks one pool worker with the GVL released and
   the starvation supervisor grows the pool; under
   `FLEZHA_RUBY_EXECUTOR=fibers` it yields the fiber and the dispatcher keeps
   scheduling. **The executor is an axis of the run here** (the manifest says
   so in `dispatch.env`), so a gate that worked under one and not the other
   would prove nothing about the run the harness chose. That is the change's
   spec delta.
2. **It holds no timer, and a timer would break a blocking gate.**
   `SLEEP["ruby"]` in `scripts/refactor_metrics.py` is `(?<![\w.])sleep[ (]`
   and `test/apps` is blocking at `sleeps 8` — one of which is already this
   app's `wedge_probe`. A transliteration of the reference's
   `thread::sleep(Duration::from_millis(20))` poll adds a ninth. Running that
   regex over `fixtures_workflow.rb` returns nothing.
3. **The shutdown is bounded by machinery that already exists.** Both executors
   drain against `DRAIN_TIMEOUT_S` and report what they abandoned, which is the
   same treatment `wedge_probe`'s forever-parked handler already gets. A parked
   gate does not hold a runner's exit open.

## Two base classes instead of two macros

The reference's `durable_step!` and `stateless_step!` are macros because Rust
has no shorter way to write a trait impl. Ruby's `class X < Flezha::Procedure`
already is that factory, and `Flezha::Declarative#inherited` makes a subclass
carry every declaration its parent made — so `DurableStep` is one class with
four overridable methods and nine of its subclasses are one line of body each:

| method | what it is |
|---|---|
| `recorded(input)` | the value journaled; the reference keys it by the procedure name |
| `gated?(input)` | whether the step waits for the gate **before** journaling |
| `released(input, ctx)` | what leaves. A value is emitted by the dispatcher (`dispatch.rb:185` emits a handler's non-nil return); a step needing a key emits through the ctx and answers `nil` |
| `compensate(outputs, ctx)` | present or absent — see below |

The macro parameterises the emit and the compensation; `recorded` and `gated?`
are the two the reference varies by hand-writing the procedure instead. Only
`UndoingStep`, which adds the one-record undo four fixtures share, needed a
second base class. **No `stateless_step` twin was written**: seven stateless
procedures are three-line classes and a wrapper over them would carry nothing.

**Compensability is declared by defining the method.**
`Definition#compensate?` is `declares?(:compensate)`, so `CommitA < DurableStep`
is non-compensable and `SagaA < DurableStep` becomes compensable by writing
one. That is what the two committed goldens pin —
`commit_block_workflow.json` `"compensable_procs": ""`,
`saga_block_workflow.json` `"0,1"` — and both pass unchanged.

## `items` is a sorted Array because a Set is refused at registration

The reference's `SeenState` is a `BTreeSet<String>`, chosen because *"`BTreeSet`
keeps snapshot bytes deterministic"* (`workflow.rs:50`).
`Codec.unrepresentable_reason` (`sdk/ruby/lib/flezha/codec.rb:99`) answers *"a
Set decodes as a Hash, losing its identity; declare an Array, or supply a
hand-written codec"*, and `Procedure.build_definition` walks
`initial_state(nil)` through `assert_representable` — so the refusal fires when
the class is defined, not at the first restore. The twin is the array a Set
would decode to, kept sorted by `Fixtures.insert_sorted` for the same
determinism and idempotent on insert for the same re-run after a resume.

## Every sink declares its encoder

`Ctx#plugin_output` (`ctx.rb:214`) falls back to `Codec.encode` — MessagePack —
when a channel carries no `sink_encode`, while the reference's bare
`.sink(name)` is `default_plugin_serializer`, raw UTF-8 for a `String`. So the
four channels the reference spells bare — `branch_workflow`'s `d_out` and
`c_out`, `diamond_workflow`'s `j_out`, `retry_workflow`'s `out` — take
`encode: BYTES` here. It is load-bearing for `j_out`:
`workflow_crash_resume_e2e_test.exs:180` compares forwarded frames as **raw
binaries**, so a MessagePack length prefix would fail the fan-out proof for a
codec reason and read as a fan-out defect.

The **edge** encoding is never touched: `Ctx#workflow_output` encodes with the
class codec, and KI-144 renamed the field `sink_encode` so the edge path cannot
reach it.

## `proc_idx` is not the declaration index

`test/system/workflow_crash_resume_e2e_test.exs` asserts
`ClassRegistry.leaf_procs(operative_id) == [4]` for the diamond, and the
declaration order here registers `diam_join` third and `diam_c` fourth. Both
are true: the core re-numbers the nodes in Kahn topological order
(`flz_core/src/workflow/spec.rs`, *"FIFO — for a tree this is exactly BFS from
the root, so workflow `proc_idx` numbering stays stable"*), so the join is
index 4 however the arms are written. What the declaration **does** decide is
the order of a channel's consumer list, which is why both fan-out arms are
written in the reference's order.

## KI-141 on the wire, with the control that proves the test measures it

`dfan` declares one channel with **two** consumers and a sink; the runtime-
agnostic no-crash diamond then asserts five payloads. The control removes the
`DiamC` arm and moves the join's `j_out` sink onto the surviving arm — the join
must keep an output channel, or it fails for a declaration reason instead of
for the fan-out. Both transports go red with exactly three payloads:

```
1) test transport=quic a fan-in diamond joins once with no crash at all
   test/system/workflow_crash_resume_e2e_test.exs:180
   the fan-out multiplied the released batch: ["a:{\"seed\":\"go\"}", "b:{\"seed\":\"go\"}", "j:b:{\"seed\":\"go\"}"]

Result: 0/2 passed, 11 excluded
```

`c:` and `j:c:` absent — the shape KI-141 produced silently under the per-edge
model. With the arm restored the same two pass. The per-edge spelling is not
reproducible (change 1 removed it from the builder), so this is the honest
control rather than a revert.

## What runs on ruby, and what still spawns rust

The manifest flip un-skips every `capability: :workflow` module;
`reference_runtime: true` in a describe title is what still pins one to rust.

| module | result under `FLEZHA_E2E_RUNTIME=ruby` |
|---|---|
| `workflow_crash_resume_e2e_test.exs` | 13 passed — 8 ruby runs (4 tests × 2 transports) + 5 rust |
| `sdk_smoke_e2e_test.exs` | 25 passed, including the live 19-id factory set and the two workflow declaration goldens |
| `workflow_kind_e2e_test.exs` | 1 passed, driven by a **rust** runner |
| `workflow_control_flow_e2e_test.exs` | 8 passed, all rust |

The eight `(reference/uds)` cells — the tier, the opt-out, the two outbox
drills, the loop, the rewind, the retry ladder and the branch rollback — prove
**controller** mechanics and merely need a runner to drive them. Unpinning them
changes what runs for java too, so it is one cross-runtime widening with a
multi-SDK proof obligation and belongs to the programme's close-out, where a
single unpin serves five runtimes.

## The `self_terminating` gap stays latent

Both references refuse a workflow that is neither self-terminating nor
duration-capped — Rust `BuildWorkflowError::ExternalStopOnly`, Java
`WorkflowSpec.java`'s *"is not terminable"*. **No veneer carries either the
declaration or the refusal**, Ruby included: `WorkflowOperative`'s settings list
has no `self_terminating`. All four loop fixtures here declare
`max_duration_ms`, so nothing in this set trips it. It is a missing *refusal*
in four languages, which is its own change.
