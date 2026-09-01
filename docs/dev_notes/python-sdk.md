# Python SDK — implementation notes

Gotchas and load-bearing decisions from building `sdk/python`, the PyO3 veneer
(`flz_core_ffi/src/python_glue.rs`) and `test/apps/python_runner`. What the docs
already say is not repeated here; what is here is what the next person would
otherwise rediscover.

## The veneer

**`unsendable` on the context pyclass is a wedge, and only the e2e found it.**
PyO3's `#[pyclass(unsendable)]` pins an object to its creating thread and panics
on a call from any other — which is precisely what a worker pool is. Every unit
test drove a Python `FakeCore`, so the pool tests proved the pool and not the
crossing; the first live poll from a worker thread panicked with
`Core is unsendable, but sent to another thread`. The class is plain
`#[pyclass(frozen)]` with a manual `unsafe impl Send + Sync`, which is sound
because `FlzContext` is the ABI's own thread-safe handle and
`flz_core_complete` is documented as callable from a thread that never polled.
`test_native.py` now calls `poll` and `wait_starvation` from a spawned thread so
the attribute cannot come back.

**The context is an object, not a `static`.** Ruby and JNI keep the pointer in a
process-wide static because their runtime is one interpreter per process.
CPython is not — sub-interpreters are a supported model — so the handle lives in
a `#[pyclass]` reached only through the interpreter holding it. A Rust-side test
walks the module source and fails on any `static` declaration, because a
`static CONTEXT` is the shortest path to "one context per process" and is what
the two neighbouring veneers do.

**PyO3 0.29 renamed half the API this veneer touches.** `downcast` → `cast`,
`FromPyObject` grew a second lifetime and its own error type (so `?` needs a
`for<'a> …::Error: Into<PyErr>` bound — the `Extractable` trait exists to write
that once), and `allow_threads` → `detach`. A veneer written against 0.24
examples does not compile.

**`Py_GIL_DISABLED` needs `pyo3_build_config::use_pyo3_cfgs()` in the build
script**, or `cfg!(Py_GIL_DISABLED)` — which is how the runner reports its wheel
tier — warns on its own answer.

**The payload view is a `#[pyclass]` with `__getbuffer__`**, which is what makes
"the common decode path allocates no intermediate `bytes`" true rather than
aspirational: `msgpack.unpackb` reads it directly. It is constructible from
Python on purpose — a buffer type Python cannot build is a contract only Rust
can test. `abi3-py311` accepts the buffer slots (they entered the limited API in
3.11), so the compatibility wheel keeps the zero-copy path.

## The Python side

**`__slots__` may not name `__doc__` on a class that has a docstring.** Both
`Procedure` and `Operative` carry the wrapped callable's documentation on a
plain `doc` attribute instead; the obvious `"__doc__"` slot entry raises
`ValueError` at class-creation time.

**`typing.Any` is a class from 3.11 on**, so the obvious
`isinstance(annotation, type)` test accepts it and the codec then refuses a type
nobody declared. `_declared_type` treats `Any` and `object` as "undeclared" and
hands the body to the MessagePack default. The worked example found this: a
`change: Any` annotation failed registration.

**A union change type is refused, by name.** A durable body carries the codec's
bytes and nothing that says which arm of a union wrote them, so a union would
decode to whichever arm was tried first — a defect that surfaces only on a
restore. Every SDK answers this the same way: one Change type per procedure
(Rust's is an enum). The refusal names the two Python spellings — one class
whose fields cover the variants, or `Any` plus plain dicts.

**The state guard delegates rather than using `object.__setattr__`.** A Pydantic
model's `__setattr__` maintains `__pydantic_fields_set__`; bypassing it would
leave the model inconsistent. The guard calls the class's *original*
`__setattr__` and arms itself at the end of `__init__`, because a state object
has to be built before it can be guarded.

**Ambient-clock detection patches `datetime.datetime` with a metaclass.**
`datetime.now` is a C-level classmethod that cannot be reassigned, so the only
way to observe it is a subclass in the module's place — and a bare subclass
makes `isinstance(a_real_datetime, datetime)` answer `False` for every value
built before the swap, which is a worse defect than the one being diagnosed.
`_DatetimeMeta` delegates both `__instancecheck__` and `__subclasscheck__` back
to the real type. `time.time` / `uuid.uuid4` are plain functions and need none
of this.

**The module `state.py` was renamed `entity_state.py`.** `flezha.state` is the
decorator, and a submodule of the same name is shadowed by the package's
re-export — `from flezha import state as state_module` silently binds the
function.

## Concurrency

**One async dispatcher, driven two ways, with no event loop on the fast path.**
The dispatch path is `async def` because the asyncio executor must dispatch *on*
the loop (the ABI documents completion from a thread other than the poller's,
and that is what "completes on the loop thread" needs). The thread executor
would then pay `run_until_complete` per activation — so `resolve()` asks where
it is running: on a loop it awaits, off one it drives the awaitable to
completion on the calling thread's own loop. Off a loop the dispatch therefore
never suspends, and `run_now()` finishes it with a single `send`. A synchronous
handler costs one coroutine step and no loop at all.

**The starvation supervisor answers with a THREAD under both executors.** The
asyncio executor's failure mode is a completion that parks the loop; growing the
loop's concurrency would rescue nothing, because the loop is what is stuck. So
`_Supervisor.grow` spawns a fetch-and-run thread either way, and the whole
worker — poll, dispatch, complete, retire on an idle streak — is one function
shared with the thread executor's base pool.

**A controller-initiated close must reach the runner's own wait.** `poll`
answering `shutting_down` ends the workers, but `await_termination` waits on the
runner's `stop()`; without the `on_core_shutdown` callback a controller close
left the process alive with no workers. Found by inspection while writing the
shutdown test, not by the suite.

**Retired threads are pruned at spawn**, not at the drain: a long-lived runner
that grows and retires through many starvation episodes would otherwise
accumulate a dead `Thread` per episode for the life of the process.

## Identity stamps — the gap `keyed_channels` was hiding

A non-tier keyed plugin emit carries `flz.entity` / `flz.seq` / `flz.key`, and
**the SDK mints them**: the core's tier-off path ships plugin frames with only
`channel`, and `Flezha.State.Wire.output_stamps` runs on the durable outbox
(tier and workflow) path alone. Java does this in `ActivationDispatcher`;
**Node, Ruby and PHP do not do it at all**, which is why all three defer
`keyed_channels`. Python mints them in `Ctx._identity_stamps`, with Java's
byte-shape (`flz.seq` is `<change_seq>.<emit_ordinal>`) and Java's condition
(keyed **and** not effectively-once), because a sink deduplicates on them and
two SDKs disagreeing about the shape is a sink that silently stops
deduplicating. The emit ordinal is separate from `stable_id`'s: one counts
frames produced, the other identifiers asked for, and a handler that does one
and not the other must not shift the sequence a sink orders by.

This was found by the worked example asserting its own output was stamped, then
isolated by driving the *reference* `partition_relay` fixture under Python and
Java in turn.

## `sink(name)` names a different thing in Rust, and it selects the forward

The Rust builder's `sink(plugin_name)` **attaches a plugin sink to the channel
already under construction** — the channel keeps whatever `named(...)` called
it, and the argument names the *plugin*. Every veneer SDK's builder (Node, Ruby,
PHP, Python) instead *creates* a plugin channel named by the `sink` argument and
discards a preceding `named(...)`. The Elixir app records the same divergence in
a comment on its own `partition_relay`.

That is invisible until a **forward**: a forwarded frame keeps its producing
channel's name, and that name is what selects the receiving operative's entry
channel. The reference `partition_relay` reads
`.named("input").keyed().sink("relay_out")`, so its channel is `input` and the
downstream `dynamic_entity`'s entry channel — also `input` — receives it. The
Python twin copied the spelling verbatim and produced a channel named
`relay_out`, which selects nothing.

It surfaced as "SOL-USD was not forwarded", failing 2 of 2 whole-file runs and
passing 3 of 3 in isolation — which looked exactly like a load-sensitive race
and was not: a re-send probe left it failing, and the forward provider logged
nothing at all, meaning no frame ever reached it. The fix is one name:
`.keyed().sink("input")`. Ruby, Node and PHP carry the same fixture bug and do
not see it, because all three defer `keyed_channels` and the forwarding
scenario is tagged with it.

## Packaging

**The free-threaded tier starts at `cp314t`, not `cp313t`.** PyO3 refuses the
free-threaded build of CPython below 3.14 (`error: PyO3 does not support the
free-threaded build of CPython versions below 3.14`), so the design's `cp313t`
is not a target this SDK has. Found by building against a real 3.13t
interpreter; a CI matrix pinned to 3.13t would have found it only after merge.

**maturin builds the veneer from `flz_core_ffi` through `manifest-path`**, so
there is no packaging crate: `sdk-core-abi` requires the veneer to be a feature
of the ABI crate, and a thin wrapper crate would have been one anyway. The sdist
carries the whole Rust workspace slice it needs and builds from source with a
toolchain (verified).

**The checkout-local `.venv` gets a `.pth` file, not `pip install -e .`.** An
editable install runs maturin and builds the extension a second time, which is
the one thing `scripts/build-python-sdk.sh` has already done.

## The test app and the conformance manifest

**Python defers two capabilities where every other veneer SDK defers nine.**
Node, Ruby, PHP and Elixir all defer the persistence family, the tier, timers,
correction windows, keyed channels, workflow, dynamic-entity eviction, entry
idle and schema evolution. Python carries all of them but `workflow` and
`entry_idle`, and each of the seven it carries is proved by the live
suite:

| capability | what it took |
| --- | --- |
| `keyed_channels` | the identity stamps above |
| `state_persistence` | six fixtures; `coalescing_counter`'s coalesce key has no SDK seam (Rust's is a `Change` trait method), and the fold is idempotent per `(interval, ts)` either way, so the change ships uncoalesced — a throughput property, not a semantic one |
| `effectively_once` | four fixtures, plus `Channels.checkpoint_ack()`, which Rust and Java have and no veneer SDK had |
| `correction_window` | the `candle` fixture, on an event-time clock |
| `schema_evolution` | two fixtures declaring one procedure name either side of a version bump |
| `entity_timers`, `dynamic_entity` | fixtures the app already had |

**`entry_idle` is deferred too, and the reason is the opposite of `workflow`'s.**
The capability is two layers: the delivery stamp lives in `flz_core`
(`entry_input.rs`, written on the shared dispatch path), so Python has it for
free like every FFI veneer, and `Core.entry_last_delivery_nanos` plumbs the ABI
read. The policy half — `last_entry_delivery` and `on_entry_idle` on the runner
handle, the `IdleObserver`, the episode latch and the tick — is per-SDK host
code and exists in Rust and Java only. A manifest entry with an empty fixture
list is not evidence of a capability: this one claimed a surface the SDK does
not expose, and the claim is what made a zero-fixture capability look free.
Owed, with the four veneers' remaining host-layer work, to the follow-up that
carries it.

**`workflow` stays deferred, by name**, to `sdk-durable-fixture-parity`. Its 19
reference fixtures need two authoring-surface additions Python does not have —
an inter-procedure `codec()` declaration, and a hybrid channel spelled as
`sink(...).into(...)` rather than as a `branch` — and the structured-workflow
semantics (loop modes, rewind, branch and diamond trees, staged outbox undo) are
the largest single block in the reference app. Run-sugar workflows *do* work:
`examples/python/durable_agent` declares one and it binds against a live
controller.

**The gate is not clause-free for a new SDK.** Two per-SDK tables take one row
each (`@connect_sources` and the `sdk_lang` source-of-truth map), the nightly
workflow's `workflow_dispatch` options take one, and the manifest's `docs` page
must exist and name the manifest back. Nothing else changed in the harness.

**`test/fixtures/host_class_decl_keys.txt` has a sixth reader.** Its header
names them, and `test_decl_keys.py` asserts it does — a detector nobody knows
about is a detector that lapses.

## Measurement

Three interleaved rounds, `--engine flezha --workload identity -p 2 -m 2000`,
GIL tier, durability off, on a 4-core laptop. Medians, end-to-end (generate +
drain):

| runtime | rec/s | of Rust |
| --- | --- | --- |
| Rust | 21,675 | — |
| Java | 16,804 | 78% |
| Python (asyncio) | 14,121 | 65% |
| Python (thread) | 10,863 | 50% |

**These are this box's numbers for these runtimes and nothing else.** Python's
gap to Rust is the poll-host shape's gap, in the same band Java's already
documented 27–55% sits in.

**The per-activation crossing does not dominate, and that is measured rather
than assumed**: the *executor* moves Python's number by 30%, which a
crossing-bound workload could not allow. So batched dispatch per mailbox drain
(design D13's identified fallback) is not indicated by this measurement and is
not implemented; `sdk-declarative-keys` owns the next lever, and it is the right
one — it removes the crossing entirely for keys and simple transforms.

The asyncio executor being *faster* on a trivial handler is not a
recommendation: with almost no work per activation, GIL handoff among eight
pool threads costs more than it buys, and the thread executor's advantage
appears when handler bodies block. The default stays `thread` for the reason
the docs give — one blocking call stalls the asyncio loop.

## Known-flaky, and settled by A/B

A full `--include system` run on this box fails about seven tests whichever
runtime it is under, and never the same seven: `python` failed dlq, correction,
partitioned forwarding, effectively-once, schema evolution and compaction;
`rust`, on the next run, failed checkpoint, both entity-timer cases, credit
flow, remote-durability-off and both dynamic-entity cases. **Every Python
failure passes in isolation** — partitioned 2/2, correction 2/2, timers 2/2 —
and the reference runtime's count is the same. It is the load-sensitive
population the repository already records, not this change's.

`dlq_e2e_test` in particular was settled by interleaving rather than by
assertion: the tier-on case failed **2 of 3 runs under `rust`** while passing 3
of 3 under `python`, and the tier-off case fails roughly 1 in 3 under either —
the population `docs/dev_notes/rename-block-to-drop-nothing.md` names.
