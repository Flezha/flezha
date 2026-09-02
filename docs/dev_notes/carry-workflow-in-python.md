# Python carries the workflow kind (`carry-workflow-in-python`)

`test/apps/python_runner/fixtures_workflow.py` holds nineteen twins of the
reference's durable structured workflow fixtures
(`test/apps/rust_runner/src/workflow.rs`, `workflow_loop.rs`) under the same
factory ids, procedure names, channel names and output bytes. The manifest
defers nothing, `runner.py` registers the module with `define_workflow`, and no
`test/system/` file is touched: flipping `deferred` to `[]` is what un-skips the
scenarios that already exist.

## The transcription is verified, not eyeballed

Every one of the nineteen `to_host_class_decl` outputs is **byte-identical** to
the node twin's, `sdk_lang` excluded. That comparison is the strongest single
check in the change — it collapses nineteen topologies, four sink encoders,
sixteen generated bench steps and every workflow knob into one equality — and it
is cheaper to re-run than to re-read:

```python
# node:   toHostClassDecl over every export of fixtures_workflow.mjs
# python: graph.to_host_class_decl over fixtures_workflow.ALL
# diff every key but sdk_lang  ->  no differences across all 19
```

Two things it does **not** cover, and both are proved on the wire instead: the
sink encoders (a declaration carries the sink's *name*, never its closure) and
the release gate.

## The release gate is an awaited future, and it serves two executors

```python
async def block_until_released() -> None:
    if RELEASE_ENV in os.environ:
        return
    await asyncio.get_running_loop().create_future()
```

A future nobody resolves. The harness kills the first runner and launches the
replacement with `FLEZHA_WORKFLOW_E2E_RELEASE` set, so the gate is a boot-time
decision: never settling on the first runner, returning at once on the second.

This host is the only one with **two** dispatch members, and the manifest
promises the fixtures are identical under both. `dispatch.resolve` picks the
right behaviour by asking where it is running rather than by being told:

* thread executor — no running loop, so `_thread_loop().run_until_complete(...)`
  drives the coroutine and blocks. That parks **one pool worker**. The core's
  driver thread is untouched, control pings are answered by the core's own I/O
  threads, and `executor._Supervisor` grows the pool on the starvation
  observation, so a sibling procedure stays dispatchable;
* `FLEZHA_PYTHON_EXECUTOR=asyncio` — a running loop, so the `await` suspends the
  one activation and yields the loop.

`threading.Event().wait()` would satisfy the first and freeze the second, which
is the wedge signature `pool_wedge`'s fixture exists to produce. A timed wait
would also be a **gate failure**, not a style preference:
`scripts/refactor_metrics.py:364`'s `SLEEP["python"]` is
`\btime\.sleep\(|\basyncio\.sleep\(` against a blocking `test/apps sleeps 8`, one
of which is `fixtures.py`'s own `time.sleep(3600)` in `wedge_probe`. Running the
gate's own regex over the file returns `[]`.

## Only a gated step is a coroutine

`durable_step` emits an `async def process` **when and only when a `gate` is
declared** — two of nineteen procedures, `diam_b2` and `chat_turn`. Every other
durable step stays synchronous, and `resolve` answers a non-awaitable with no
event loop at all (`dispatch.py:236`). That matters because `bench_chain_16`
runs this factory sixteen deep under `mix bench.workflow --scenario chain`: an
unconditionally `async` handler would put a `run_until_complete` on every step of
the hot path to buy nothing for the seventeen procedures that never wait.

## A stateful procedure is a class, so the factory builds one

`flezha.procedure` reads `initial_state`, `apply_change` and `compensate` off an
*instance* of what it is handed (`sdk/python/src/flezha/procedure.py:178`), so
node's options-object spelling has no analogue: `durable_step` closes over its
four axes and defines a class. `compensate` is attached as a `staticmethod`
taking `(outputs, ctx)`, which is the shape `dispatch._compensate` calls
(`dispatch.py:568`) — a bound method would arrive with `self` in the outputs
slot.

## `items` is a sorted list because MessagePack has no set

The reference's `SeenState` is a `BTreeSet<String>`, chosen so snapshot bytes are
deterministic. `msgpack.packb` cannot represent a `set`, and
`Procedure._validate` walks the declared starting state through
`codec.assert_representable` at **import** — so a `set` here fails at
declaration, not at the first snapshot. The twin is the sorted, de-duplicated
list it would decode to, held by `bisect.bisect_left` insertion for the same
determinism and idempotent for the same re-run after a resume.

## KI-141 on the wire, and the control proves the test measures it

The diamond is the fan-out's proof: `dfan` carries a sink and **two** consumers,
and under the per-edge model the second was silently dropped. Comment out the
`fan.into(diam_c)…` arm (moving `.named("j_out").sink(...)` onto the surviving
arm so the join still reaches the sink) and re-run only the no-crash diamond:

```
1) test transport=quic a fan-in diamond joins once with no crash at all
   test/system/workflow_crash_resume_e2e_test.exs:180
   the fan-out multiplied the released batch: ["a:{\"seed\":\"go\"}", "b:{\"seed\":\"go\"}", "j:b:{\"seed\":\"go\"}"]
   code: assert Enum.sort(delivered) ==
   stacktrace:
     test/system/workflow_crash_resume_e2e_test.exs:201: (test)

Result: 0/2 passed, 11 excluded
```

Three payloads where five are asserted; `c:` and `j:c:` absent, on **both**
transports. With the arm restored the same two pass. The red is byte-identical to
the node change's, which is what "one contract, every SDK" looks like when it is
measured rather than asserted.

## Every sink declares its encoder; no fixture touches the edge codec

The reference's bare `.sink(name)` serialises a `String` as UTF-8; this SDK's
plugin default is MessagePack, and the e2e compares forwarded frames as **raw
binaries** (`workflow_crash_resume_e2e_test.exs:201`). Every `sink(name,
encode=...)` here passes `fixture_support.raw` or a `raw`-wrapping lambda. The
**edge** encoding is untouched: an inter-procedure hop between two Python
procedures carries MessagePack and the core derives that channel's journal codec
from the channel's kind, so no fixture declares one — and a fixture that reached
for the edge encoding would break the invariant that a sink encoder is the
sink's alone.

## `leaf_procs` is a DAG index, not a procedure index

`workflow_crash_resume_e2e_test.exs` asserts `ClassRegistry.leaf_procs(id) ==
[4]` for the diamond, while `diam_join` is procedure **3** in every SDK's
declaration (node's and Python's agree). The metadata is built from
`spec.dag.order[node]` (`flz_core/src/command/operative_start.rs:209`), so the
number is the core's own topological index and no SDK can move it by reordering
its declaration. A port that "fixes" the ordering to make that assertion read is
chasing a number that was never the SDK's.

## The retry counter is `itertools.count`

A retry re-runs the procedure of the same instance in place and dispatch is
entity-affine, so one process-wide counter is the whole ladder's view. `next()`
on a `count` is one C-level call; `x += 1` on a module global under a worker
pool is a read, an add and a store.

## The `self_terminating` gap stays latent

Both references refuse a workflow that is neither self-terminating nor
duration-capped — Rust `BuildWorkflowError::ExternalStopOnly`
(`sdk/rust/src/workflow/spec.rs:25`), Java `WorkflowSpec.java:199`. No veneer
carries either the declaration or the refusal:
`grep -rn "self_terminating\|selfTerminating" sdk/{node,ruby,python,php}`
returns nothing. All four loop fixtures here declare `max_duration_ms`, so
nothing in this set trips it. It is a missing *refusal* in four languages, which
is its own change.
