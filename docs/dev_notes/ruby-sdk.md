# Ruby SDK — implementation notes

Per-change gotchas for `ruby-sdk`. Read before touching `sdk/ruby/`,
`flz_core_ffi/src/ruby_glue.rs` or `test/apps/ruby_runner/`.

## The environment this needs, and why it is not obvious

- **bindgen needs clang's *builtin* headers, not just libclang.** `rb-sys` runs
  bindgen over `ruby.h`; with `libclang.so` present but
  `libclang-common-N-dev` absent, the build dies inside cargo with
  `'stdarg.h' file not found`, which reads like a broken Ruby install and is
  not one. The CI workflow installs `libclang-dev` for exactly this and says so
  at the step.
- **Ubuntu's `ruby3.2` package has no YJIT and ships no headers.** It is built
  without YJIT (`ruby --yjit` warns and `RubyVM::YJIT` is undefined) and the
  headers live in a separate `-dev` package. A source-built Ruby is the
  reliable answer for development.
- **The floor is 3.3, not the 3.2 the design leaned to.** `Process.warmup` is
  3.3+ and the startup-order requirement calls it unconditionally, and 3.2 is
  past end of life. This resolves that open question.

## The veneer

- **`magnus::embed::init()` records the caller's stack frame as the GC's scan
  root**, and `setup()` is `#[inline(always)]` for that reason. Booting the VM
  inside a lazily-called helper — a `OnceLock` initialiser, say — records a
  frame that has already returned by the time the GC scans, and the process
  segfaults at `0x30` with no usable backtrace. It has to be at the top of the
  test function, held for the whole test.
- **Two of `abi_scenario`'s phases call in from a spawned thread**, where an
  embedded VM has no handle at all. That is why `through_veneers` exists
  separately from `abi_complete`: the ABI accepts a completion from any thread,
  so the *mapping* stays on the test's own thread and only the ABI call
  crosses. The shutdown phase polls off-thread too, but answers
  `SHUTTING_DOWN` and never reaches the hook — the guard states that rather
  than relying on it.
- **The log callback is deliberately NOT veneered**, exactly as it is not in
  `jni_glue.rs`. `rb_thread_call_with_gvl` is documented as usable only from
  inside an `rb_thread_call_without_gvl` callback — "you cannot convert a
  non-Ruby thread into a Ruby thread using this API" — and the diagnostics a
  host would route through it come from threads the core owns. The Node veneer
  needs the bridge because its fetchers are native; this host's threads are
  Ruby's own and log in Ruby. Building a queue for a callback the ABI's own
  entry point discards would be a mechanism with no producer.
- **`magnus 0.8`'s `function!` does not take `&Ruby` as a first argument** (that
  is an `init`-only convenience via `RubyInit`), so every entry point resolves
  the handle itself through one `ruby()` helper over `Ruby::get_unchecked` —
  the same shortcut magnus takes on the return path of every bound function,
  on the same thread and the same precondition.
- **`ruby-embed` is a feature, not a dev-dependency.** A dev-dependency on
  `magnus` would make `cargo test --workspace` fail to build on any machine
  without Ruby, and this crate's whole point is that it builds without any of
  the three interpreters.
- The feature adds exactly `Init_flz_core_ffi` and `ruby_abi_version` and
  removes nothing; CI asserts that rather than reviewing it.

## The gem

- **`require "flezha/flz_core_ffi"`, not `require "flz_core_ffi"`.** The
  library is installed under `lib/flezha/`, and Ruby derives `Init_<basename>`
  from the basename either way — but only the first spelling is on the load
  path.
- **`Marshal` appears nowhere**, and `no_marshal_test.rb` scans the shipped
  sources so a later change cannot reintroduce it. The scan also asserts it
  scanned something: a guard over an empty file list passes forever.
- **MessagePack extension types, not markers.** Encoding a Symbol as
  `{"$sym" => …}` collides with `symbolize_keys: true`, which the codec needs
  so `{count: 1}` folds back as `{count: 1}` rather than `{"count" => 1}` —
  and an `apply_change` that reads a different shape on replay than it did live
  is the exact drift the codec exists to prevent. A `MessagePack::Factory` with
  Symbol and Timestamp extensions has neither problem.
- **`apply_change` mutates in place and its return value is ignored** — the
  same contract as Rust's `&mut S`. An earlier draft wrote
  `@state = fold_change(...) || @state`, which replaces the state with whatever
  the fold happened to return: `state[:seen] << x` answers the Array.
- **There is no state-mutation guard.** Node's is a `Proxy`; Ruby has no
  counterpart that does not cost an allocation per field read. The rule is
  documented, and `Testing::Harness#replay_equals!` is what makes it safe —
  and in this SDK it catches *both* an unrecorded mutation and an impure fold,
  where the guarded SDKs can only catch the second.
- **The unreachable-procedure refusal cannot fire through the fluent DSL**,
  because every node the DSL creates is created BY an `into` and is therefore
  fed. Its test builds the node through the builder directly — a check with no
  way to reach it is a check nobody has run.

## Concurrency

- **The GVL seam is one function.** `without_gvl` is where the rule lives, and
  it captures panics rather than letting one unwind across the C frame that
  called it.
- **No unblocking function is registered**, on purpose: the ABI has no
  per-thread wake, so the poll timeout (250 ms) IS the interrupt granularity.
- **The fiber executor must not close its own scheduler.** Ruby closes a
  thread's scheduler when the thread ends, and closing it runs every
  outstanding fiber to completion first — which is exactly the drain semantics
  wanted. An explicit `Fiber.scheduler&.close` in an `ensure` is a *second*
  close, and `Async::Scheduler` raises `ClosedError` on it.
- **A read error on stdin is not EOF.** Only `EOFError` / a gone descriptor
  ends the watch; `Errno::EINTR` and `IO::WaitReadable` retry. Conflating them
  is how a runner shuts itself down moments after connecting (the Node SDK hit
  the same trap from the other direction).
- `Runner#boot` is split from `Runner#run` so the startup ORDER — declare, warm
  up, then serve — is something a test observes rather than a comment.

## The test app and the gate

- **The gate is not clause-free for a new SDK.** It keeps two per-SDK tables —
  where an SDK spells its language byte, and which of its sources may name a
  connect variable — and admission is one row in each. `node-sdk` edited the
  same file for the same reason; the change's spec delta was corrected to say
  so rather than claiming the gate is untouched.
- **The manifest defers `workflow` alone.** This is a deferral of fixtures,
  not mechanism: the dispatcher answers every activation kind, and
  `dispatch_test.rb` sweeps all of them. The entry-idle observer is the gem's
  own host layer (`lib/flezha/entry_idle.rb`) over the latch the core keeps by
  watch id, not a mechanism the core carries on the gem's behalf.

## What the refine pass found

- **A subclass silently lost every declaration its parent made.** Class-level
  declarations are class instance variables, and Ruby does not inherit those:
  `class Sub < Base; end` came out with `entity_mode :single`, the default
  mailbox capacity and schema version 1, whatever `Base` declared. The test app
  had already grown four re-declarations working around it. `Declarative`
  now carries an `inherited` hook, and it is total by construction — every
  declaration registers its own name, so one added later is covered without
  anyone remembering to extend a list.
- **Identity is deliberately NOT inherited.** `procedure_name` and
  `factory_id` are per class: a subclass taking its parent's node identity
  makes two procedures claim one node, and its parent's factory id is a
  duplicate the core refuses. `procedure_name` also stopped memoizing its
  default — with the hook in place, `Base.definition` before `class Sub < Base`
  would otherwise have copied the parent's resolved name into the subclass.
- The guarded `Core` wrapper is generated from one `ENTRY_POINTS` list, and
  `native_test.rb` asserts that list is exactly what the veneer exports — so
  the fork guard cannot be forgotten at a call site, and an entry point added
  on one side and not the other is a red build rather than a `NoMethodError`.
- One `Executor.fetch` holds what the three answers to `poll` mean; the thread
  and fiber executors both drive it, and the deferred Ractor one will.
- `Testing::Harness` drives `Dispatch.invoke` over a real `RegisteredClass`
  rather than building its own `Ctx` and holding its own state, so what it
  proves is what the runner does.
- The veneer's three copies of the out-buffer ownership protocol collapsed into
  `without_gvl_taking_buffer`, which is where the "no native buffer reaches the
  Ruby heap" promise is now kept.

## Both executors are proved end to end

`FLEZHA_E2E_RUNTIME=ruby mix test --include system` is green under the thread
executor and, separately, under `FLEZHA_RUBY_EXECUTOR=fibers` — 1849 passed,
50 skipped, on both. The executor is therefore an axis of the run the same way
the runtime is, and the manifest declares it in `dispatch.env` rather than the
test app pinning one.

## The drain had to be bounded, and only the full suite found it

`ThreadExecutor#stop` joined **every** worker. The `wedge_probe` fixture's
handler sleeps forever by design, so on stdin EOF the drain never finished and
the runner never exited — the harness stops runners by closing their pipes, so
each wedge drill leaked a live process holding its namespace. Both executors
are now bounded by `DRAIN_TIMEOUT_S`, report what they abandoned, and a runner
that gave up on part of its drain does **not** call `context_free`: the worker
it gave up on is inside user code and may yet call in.

The orphan is also why an earlier reading of two full-run failures as "the known
load-sensitive flake population" was **half wrong**, and the correction matters
more than the original claim. `dlq_e2e_test` tier-off is that population — it
passes alone, and the **rust** arm fails the same file's tier-on case on the
same box, which `ruby` skips. `control_plane_wedge_e2e_test` was not: it was
this defect, accumulating orphans across the run. A test that passes alone and
fails in a suite is evidence of a leak before it is evidence of load.

## Owed

- The `workflow` fixture set, shared with the Node, PHP and Python apps, owed
  to the follow-up that adds the two builder surfaces it needs.
- `ruby-sdk-ractor-executor` — the Ractor executor, deferred by name. The
  state discipline that keeps it reachable landed here.
