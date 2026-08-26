# Node SDK — implementation notes

Gotchas and load-bearing decisions found while building `@flezha/sdk`, the
`napi` veneer on `flz_core_ffi`, and `test/apps/node_runner`. What is in
`docs/sdk/node_sdk.md` is not repeated here; this is what the next person
changing this code needs and cannot read off the source.

## The spike (task 1.1) held, and what it actually proved

`cargo build -p flz_core_ffi --features napi` produces a `cdylib` Node loads as
a `.node` addon with **the crate's existing layout** — `crate-type =
["cdylib", "staticlib", "rlib"]`, no napi-rs-specific arrangement. The feature
is strictly additive, and that is asserted rather than reviewed: it adds
exactly one exported symbol (`napi_register_module_v1`), removes none, and the
sixteen `Java_com_flezha_sdk_native_1_FlzCore_*` exports are untouched under
`--features jni,napi`. The `binaries`/`veneer` jobs of
`.github/workflows/node-sdk.yml` re-prove it on every change, because the Java
SDK ships a binary built from this same crate.

`napi_build::setup()` in `build.rs` is a no-op on Linux and load-bearing
elsewhere (macOS `-undefined dynamic_lookup`, the Windows import library), and
`build.rs` is gated on `#[cfg(feature = "napi")]` so the crate's other
artifacts link exactly as before.

## Task 1.2's answer: napi-rs's async support is the WRONG tool, measured

The design deferred "does napi-rs's asynchronous work run on libuv's
threadpool?" to a spike. It does, and the difference is not marginal. With
`UV_THREADPOOL_SIZE` at its default 4 and sixteen parked tasks outstanding:

| Mechanism | `crypto.pbkdf2` latency |
| --- | --- |
| `AsyncTask` / `Task` (`napi_create_async_work`) | **3101 ms** |
| `Env::create_deferred()` resolved from our own thread | **1.9 ms** |

So the veneer owns its executor. Every blocking entry point creates a deferred
promise and resolves it from the host's own `BlockingPool`. Do not "simplify"
this to `#[napi] async fn` or `AsyncTask` — both starve `fs`, `dns` and
`crypto` for the whole process, and the negative control above is the proof.

Related: **`napi`'s `async` feature is tokio** (`async = ["tokio_rt"]`). The
crate deliberately does not enable it. What IS enabled is `dyn-symbols`, and
that is not cosmetic: without it the N-API symbols are resolved at link time,
so a plain Rust test binary containing this crate fails to link and task 1.10's
scenario is impossible. With it they resolve from the host at load time, the
addon works unchanged inside Node, and `abi_scenario.rs` links.

## Task 1.10 rides the EXISTING scenario rather than a second one

`flz_core_ffi/tests/abi_scenario.rs` already stands up a fake controller, an
engine and every activation kind. Under `--features napi` its two helpers now
route through the veneer: `abi_poll` asserts `activation_to_js` maps the
decoded activation field by field, and `abi_complete` builds the result in the
shape JavaScript answers with, maps it back through `result_from_js`, asserts
equality, and completes with the mapped value. So the whole scenario is a
veneer scenario, with no Node process and no duplicated harness.

The activation direction is checked field by field rather than by a round trip
**on purpose**: there is no inverse in production — nothing ever maps
JavaScript back to an `Activation` — so an inverse written for the test could
agree with a wrong forward mapping.

## The veneer decodes the schema, so the SDK never re-implements postcard

`napi_glue.rs` decodes the polled activation with `schema::decode_activation`
and hands JavaScript a flat object with a `kind` discriminator; `complete`
takes the mirror shape and encodes it. The alternative — passing bytes — would
put a second implementation of the wire contract in TypeScript, which is the
drift `test/fixtures/host_class_decl_keys.txt` exists to prevent one level up.
Payload `Vec`s move into `Buffer`s (napi adopts the allocation, no copy), and
the core's buffer is freed through `flz_core_buf_free` inside the same fetcher
turn that decoded it.

`#[napi(object)]` has no sum type, hence the flat-with-discriminator shape
rather than a union. `u64` fields are `BigInt` on both sides — that is the
mapping the durable format needs anyway.

## `pick_loop` is entity AFFINITY, and the design said otherwise

D4 of the proposal says "Per-entity affinity SHALL NOT be implemented: the core
never hands out two activations for the same entity concurrently, so any loop
may take any activation." That argument is sound about **concurrency** and
silent about **state locality**, and state locality is what decides it:
`schema.rs` says of `RestoreFold` that "the host holds the materialized state",
and a dispatch loop is a separate V8 isolate. Round-robin would send an
entity's second activation to a loop that never folded its state.

So `pick_loop` hashes the entity identity, and round-robins **only** the
prepare crossing — the one activation that names no entity, because the key is
what it is computing. This is still one selection function and one dispatch
path: with one target both arms answer 0, which is the single-loop
configuration. The spec delta was corrected to match.

**An absent key is not an absent entity**, and the first cut of this got it
wrong: it round-robined whenever `entity_key` was `None`, which is also how a
**single-entity** procedure's activations arrive. That splits a single-entity
procedure's state across loops. Nothing caught it until the second e2e arm ran
— `dlq_e2e_test`'s poison probe failed its replay, because the fixture's
antidote flag and the poison input had landed on different workers. Discriminate
on the activation KIND, never on the presence of a key. With the fix both arms
are 1802/1802; the failure was not the flake it resembled.

**Rebuild the addon with `scripts/build-node-sdk.sh`, not by hand.** The first
attempt at that fix appeared not to work, because `cargo test -p flz_core_ffi`
builds the lib for the test harness and does NOT rebuild the `cdylib` the
`.node` file is copied from. An e2e run against a stale addon is
indistinguishable from a fix that did not work.

## The event-loop rule needed a THIRD mechanism, not two

The proposal names two: no blocking call on the loop (D2), and a growable
completion pool (D3). Implementing them surfaced a third gap the conformance
suite found immediately.

`wedge_probe`'s `wedge` input holds its dispatch loop forever — that is the
whole fixture. Under the single-loop configuration the harness's teardown
(closing the runner's pipes, never a signal) is observed **on that same loop**,
so a wedged runner cannot be stopped at all and orphans a process the harness
must eventually kill. The reference Rust runner does not have this problem: it
wedges one worker thread of several.

The fix is `watchStdinEof(graceMs)` in `napi_host.rs` — a native thread reading
fd 0. On end of file it wakes every blocked poller, gives the loops the grace
period to drain and exit on their own, and terminates the process if they have
not. A healthy runner always exits first; a wedged one is terminable, which it
was not. `Runner.observeShutdown` uses it instead of `process.stdin`, which
cannot fire on a held loop.

## A departing loop's activations are abandoned NATIVELY

"A terminating dispatch loop abandons what it held" cannot be a message
protocol between isolates: a worker that dies mid-activation reports nothing.
So `napi_host.rs` keeps `HELD: token → target id`, written at hand-off (before
the threadsafe call, because the target may answer on another thread before
`call` returns), cleared by `complete` and `abandon`, and drained by
`abandon_held_by` when a registration is released or the host stops. That is
the only place that still knows what a dead worker had.

## Two defects in shared code, found by this change

1. **The language gate ran AFTER the format gate** in
   `flz_core::header::resolve_record` / `resolve_state`. Every SDK before Node
   wrote postcard, so the format byte always matched and the language check ran
   next; Node is the first with a different `format_id`, and a foreign record
   was refused as `HeaderInvalid` ("malformed header") instead of
   `LangMismatch` naming both languages — which the persistence spec requires.
   Fixed by hoisting `refuse_foreign_language` to the head of both resolvers
   (`resolve_version` disappeared into them). The existing format-gate test had
   to hold the language constant or it would have started proving the language
   gate instead.
2. **`decodeValue` returned `Buffer` sub-views** when handed a `Buffer`, because
   MessagePack hands out `bin` values as subarrays of its input and a subarray
   of a `Buffer` is a `Buffer`. `Buffer` carries a `toJSON` that rewrites it as
   `{type:'Buffer',data:[…]}`, which silently corrupted the live-state read and
   the migration handover. `decodeValue` now decodes through a plain
   `Uint8Array` view. This is what "byte arrays decode to one consistent
   representation" actually costs.

## `state` without `applyChange` is legal, and the reference needs it

The first cut refused a procedure declaring `initialState` and no
`applyChange`. The reference `compare_filter` fixture is exactly that shape —
`type State = CompareFilterState; type Change = NoChange` in Rust, seeded by
the controller's init values and then only read. The refusal moved to the point
of use: `ctx.change` throws naming the missing fold. The reverse (a fold with
no state) is still refused at declaration.

Init values reach a host class as an `ApplyState` activation
(`entity_state.rs`'s `apply_one` routes host-held state through the ABI), so
the JSON marker scheme in `dispatch.ts` is on that path too.

## `filter` is a sentinel, and it answers no key

`Stream.filter` yields a `DROPPED` symbol rather than `undefined` or `null`,
both of which a handler may legitimately carry. `applyTransforms` stops at it,
and the three places a chain runs share that one function. At the prepare
crossing a dropped payload answers `keys: []`, which for a **dynamic** consumer
is what `routing.rs`'s `let Some(key) = key else { return Ok(false) }` reads as
"do not route" — so the drop happens before an entity is minted. For a static
fan-out an empty key set means *every* declared entity, so do not reuse the
same answer there.

## The conformance gate takes one row, and that is the documented seam

The testing spec says admitting a runtime needs no change to the conformance
gate. One row is needed: `gate_test.exs`'s `stamped` table, which maps a
runtime to where its SDK spells its language byte, and whose own error message
instructs you to add it. It is data, not a clause — nothing else about the
harness changed — and `docs/sdk/conformance.md` now names it as step 9 of
adding an SDK.

## `replayEquals` can only catch an impure fold here

In Rust and Java a handler can mutate state outside a recorded change, and the
replay gate is what finds it. In Node the state guard makes that impossible —
`ctx.state` refuses assignment — so the only divergence left is an
`applyChange` (or `initialState`) that is not a pure function of its arguments.
That is what the harness test exercises; a test written against "handler
mutates state directly" passes trivially and proves nothing.

## Scope carried, and what the deferral costs

The manifest carries the six required capabilities and defers the other nine.
The SDK **implements** state persistence, timers, keyed channels, dynamic
entities, the effectively-once tier and workflows — the deferral is about
fixtures, not mechanism: the gate requires a carried capability to list *every*
reference fixture id, so a capability is all-or-nothing and the remaining
thirty-odd fixture twins (nineteen of them workflow) are the work. `pool_wedge`
is **claimed**, per the drill's result: `control_plane_wedge_e2e_test.exs`
produces the `runner_pool_wedged` verdict against the Node runner and the
runner is evicted.

Both dispatch configurations are an axis of the run
(`FLEZHA_NODE_WORKERS`), matching the shape `FLEZHA_E2E_RUNTIME` already has.

## The three seams to reach for

The refine pass collapsed the parallel copies these replaced; adding a
fifth blocking entry point or a third activation kind that runs user code
means calling one of them, not writing a fourth or third copy.

| Seam | Where | What it owns |
| --- | --- | --- |
| `off_loop(env, ctx, work)` | `napi_glue.rs` | The whole event-loop rule: create the deferred promise, run the blocking ABI call on the growable pool, resolve or reject. Every entry point that can park goes through it, so "no blocking core call on a JavaScript thread" is a property of one function rather than of four correct copies. |
| `invoke(...)` | `dispatch.ts` | Build the entity's context, run user code inside the invocation's async context, and write the state back in a `finally` — so a change recorded before a throw stays in the live entity as well as in its log. |
| `foldRestore(...)` | `state.ts` | Rebuild an entity: declared start, snapshot baseline, changes folded on top. The dispatcher and the test harness are both real callers, which is why the harness proves what a warm restart does. |

## Small things that will bite

- **A threadsafe function built over a tuple delivers no arguments.** The log
  callback was `ThreadsafeFunction<(i32, String), …>` and the host logger saw
  `undefined` for both, so every native diagnostic was silently lost — which is
  how the multi-loop bug below stayed invisible. Use an `#[napi(object)]`
  struct; that is what the dispatch hand-off already used, and it works.
- **An error from `read` on standard input is not end of file.** The runtime
  makes fd 0 non-blocking the moment a worker thread exists, so a read with
  nothing buffered answers `WouldBlock`. Treating it as end of file shut the
  runner down a few hundred milliseconds after it connected, every time, and
  only in the multi-loop configuration.
- **A verdict message travels to a dead-letter entry**, so it carries
  `name: message` (in family with the other SDKs) and not a stack. The stack is
  logged host-side at debug, where it is a diagnostic rather than a record.

- `named()` names the **channel**; the consuming node's identity is
  `into(procedure, nodeName?)`. Conflating them makes `a → b → a` build a node
  called `back` instead of a cycle.
- A `#[napi]` item looks dead under `cargo test --lib`, because napi-derive
  emits its registration constructor only outside `cfg(test)`. Hence
  `#[cfg_attr(test, allow(dead_code))]` on the two napi modules in `lib.rs` —
  scoped to that build so genuine dead code elsewhere still reports.
- Edition 2021 captures `ctx.0` **by field**, so a `move` closure over a
  `Send` newtype around a raw pointer captures the raw pointer and is not
  `Send`. `CtxPtr::get(self)` forces the whole-struct capture.
- The default logger swallows write failures. A harness stops a runner by
  closing its pipes, and an unhandled EPIPE on `console.log` turns a clean
  drain into a crash the run reports as a runner failure.
- `runtime-shim.js` is the one file that cannot be written once: `__dirname`
  and `import.meta.url` have no common spelling, so there is a hand-written
  copy per module system that the build copies into each output directory.

## Publishing is the one step this change does not take

`.github/workflows/node-sdk.yml`'s `binaries` job builds each per-platform
package with `sdk/node/scripts/platform-package.mjs` — the artifact **is** the
publishable directory, so publishing is `npm publish` over five of them and
nothing else. The script refuses a version that disagrees with the
`optionalDependencies` entry `@flezha/sdk` declares, because a mismatch there
is an install that silently falls through to a source build.

What is proven without publishing: exactly one per-platform package is
consulted on a supported platform, none on an unsupported one, and the failure
names what a source build needs (`sdk/node/test/native.test.mjs`). Nothing in
this repository is published to npm yet, and the `npm publish` is live
deployment.

## Where the tests are

| What | Where |
| --- | --- |
| The veneer, end to end, no Node | `flz_core_ffi/tests/abi_scenario.rs` under `--features napi` |
| The blocking pool and `pick_loop` | `flz_core_ffi/src/napi_host.rs` `mod tests` |
| The package | `sdk/node/test/*.test.mjs` (`npm test`) |
| Compile-time refusals | `sdk/node/types-tests/`, `npm run typecheck` + `typecheck:expected-failures` |
| The conformance suite | `FLEZHA_E2E_RUNTIME=node mix test --include system`, and again with `FLEZHA_NODE_WORKERS=4` — the second arm is not a formality: three defects were only ever visible there |
| The worked example | `examples/node`, `npm test` |
