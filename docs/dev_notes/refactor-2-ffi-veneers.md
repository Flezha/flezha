# FFI veneers refactor (`refactor-2-ffi-veneers`) — implementation notes

Area 2 of the refactor programme: `flz_core_ffi` and the Node, Python, Ruby
and PHP SDKs, zero behaviour change, run as five worker segments (batches
0–2, 3–4, 5–6, 7–8, 9) with a supervisor commit at each checkpoint. What
`docs/refactor/charter.md` says is not repeated, and the A/B tables live in
the change's `design.md` (`## Hot paths and the A/B` — box, noise floors,
checkpoints 1 and 2, and the final pass). What is here is what a later
session would otherwise rediscover, plus the narrative the charter's §5
moved out of the four bindings' module docs.

## The veneer core

**Every typed call goes through the `extern "C"` entry point, never the Rust
behind it.** `veneer::poll` calls `flz_core_poll`, not the function that
implements it. This is not ceremony: the last-error slot is written by the
`extern "C"` shell, and PHP takes the C ABI *as a C ABI* — it has no Rust in
between and reads `flz_core_last_error` as a C string. A veneer that
shortcut to the inner function would leave PHP reading a stale slot while
every other language still worked, which is the exact failure a shared core
is supposed to make impossible.

**`Sink` and `Source` are stateless handles**: `&self`, `Result<_,
Self::Error>`, and `Source::Error: From<Refusal>`, so each language keeps its
own exception class (pyo3 `TypeError`, magnus `TypeError`, napi `InvalidArg`)
while the rule that produced the refusal lives once. `const CAMEL: bool` on
both traits plus `Key::spell(camel)` is what gives napi its spellings from
the same key table. `Source::map(v, what)` / `bytes(v, what)` / `u64(v,
what)` carry `what` for one reason only: the old per-language texts ("an
output must be a dict", "an output must be a Hash", "payload must be bytes",
napi's "does not fit an unsigned 64-bit integer") survive byte for byte.
`Sink::json` is a provided method defaulting to `bytes`; Python overrides it
to `PyBytes` because `json.loads` takes bytes. The timer item's `now` stays
spelled `scheduled_at`.

**One shared reader has one leniency rule, and that moved two napi fields.**
`eventTime` on an effect and `payload` on an output were required in napi's
generated `FromNapiValue` and optional-with-default in Python and Ruby;
through the shared `Source` they are optional-with-default everywhere. Two
JNI refusal texts moved the same way — "namespace contains NUL" → "namespace
contains a NUL byte", the same for the instance id — because JNI now reads
`veneer::c_string`; nothing in `sdk/java` asserts them. Two `napi_host`
diagnostic log lines lost a redundant prefix. These are the change's only
text deltas; every `flz_core_last_error` string is pinned by `error.rs`'s
render-every-variant test and every graph refusal was checked literal by
literal against the area tag.

## The napi cost that a shared reader introduces, and what it is

`Object::get` costs **four** N-API calls per field:
`napi_create_string_utf8` mints a fresh JavaScript string for the key on
*every* read, `napi_get_property` reads through it, `napi_typeof`
classifies the result — and because `get` folds only `undefined` into
`None`, the caller then needs its own `napi_typeof` to tell an absent key
from an explicit `null`. napi-derive's generated `FromNapiValue` for a
`#[napi(object)]` struct pays that once per object; a shared per-field
reader pays it once per field.

`napi_glue.rs`'s `named` collapses it to two: `get_named_property_unchecked`
takes the key as a C string, which V8 interns rather than allocating a JS
string per read, and one `get_type()` maps `Undefined | Null` → `None`. The
absent-versus-explicit-`null` semantics are unchanged — the same two types
fold to `None`, in one place instead of two.

This was found by measurement, not by reading: row 7node read −4.25 % paired
median with B slower in **5 of 5 pairs, order-independent**, inside the row's
own 11.89 % single-run floor. A floor alone would have passed it. What
resolved it was a third arm (B2 against B4) isolating the two batches, and
after the fix the same decisive pass read −0.11 % paired median with the
direction gone. The cost was entirely on the `Source` side; the design's
fallback (keep the outbound structs, take the shared reader for results
only) would not have helped.

**Both hot-path rows need a third arm to say anything, for opposite
reasons.** Row 7node's within-arm spread is 46–54 k rec/s, so one five-round
pass reads a direction that the next pass does not reproduce; only comparing
adjacent arms (B4 against B9) isolates a batch's own cost. Row 5's trap is
different and worth remembering: **arm A's `host_loop` was built in a
`git worktree` under the scratchpad**, and a Rust binary is not
byte-reproducible across build paths — the final pass put B9 3.5 % behind
arm A over ten pairs, while B2 against B9, both built in the real tree, put
B9 *ahead* in 5 of 5. A cross-tree binary comparison of a tight 60-second
loop measures code layout as much as code. Build every arm from the same
path, or compare only arms that share one.

## Traps in the crate

**No `Env` exists in a Rust test binary**, so the napi `Sink`/`Source` cannot
be driven from `flz_core_ffi/tests/`. They are proved by a compile spike plus
the Node suite, and that is the ceiling of what a Rust test can say about
napi. The spike's typing matters if anyone rewrites it: napi 3.12.2's
`build_callback` wants `CallJsBackArgs: 'static + JsValuesTupleIntoVec`;
`Object::new(&Env)`, `BufferSlice::from_data(&Env, _)` and
`Unknown::from_napi_value` leave `'env` free and `Env` is `Copy`, so a sink
holding `Env` **by value** is `JsSink<'static>` returning `Unknown<'static>`.
The borrow form `Object::new(&ctx.env)` is what the compiler refuses.

**A threadsafe function delivering a tuple delivers no arguments** — that is
`node-sdk.md`'s trap and it still holds: the activation crosses as one
`schema::Activation`, and `HELD` (token → target) is written *before* the
threadsafe call because a dead worker reports nothing.

**`magnus::embed::init()` runs once per process.** The Ruby primitive
round-trips are therefore a function called from inside
`c_abi_full_scenario` after the VM boot, never a second `#[test]`; Python's
can be its own `#[test]`. The VM must also be booted from the test function's
own frame — `embed::init()` records the caller's stack frame as the GC scan
root.

**`BLOCKING_ENTRY_POINTS` is the one list.** All three veneer surface tables
(`hostSurface` in napi, its Python and Ruby twins) derive `blocking` from it
rather than repeating a per-language list; napi's `setLogCallback` row is the
one entry with `abi: None`, because it is the only veneered call that is not
a C entry point.

## The tests

`tests/abi_scenario.rs` (1580 lines, the area's last file over the ceiling)
became a 35-line module root over `tests/abi/{harness, lifecycle, shape,
veneer_napi, veneer_python, veneer_ruby, python_binding}.rs`. The design
named six files; a seventh holds the shape round-trips, because folding them
into `harness` would have made it ~720 lines and mixed two owners. The
lifecycle's fourteen phases are functions called in order from **one**
sequential `#[test]` — they share ordered state, and splitting the `#[test]`
would have made the ordering a hope rather than a fact.

**The charter's 50-line function ceiling is production-only** (amended at
`7f665dba`, which is what `refactor_metrics.py` had measured all along), so
`tests/abi/shape.rs` keeps two flat assertion tables of 75 and 72 lines. The
**file** ceiling still binds tests, and that is what forced this split.

`await_frame` waits on a condvar the fake controller notifies on every
captured frame; no `thread::sleep` remains anywhere under
`flz_core_ffi/tests/` or in `napi_host`'s test module, and `BlockingPool`
gained a `changed: Condvar` notified at every bookkeeping transition so
`drain` no longer polls at 10 ms.

**A coverage diff decided one design point, not just the deletions.** Node's
handout-contract tests read the core's diagnostics off `console` rather than
through `setLogger`; the first draft used `setLogger` and the coverage diff
showed `logger.js` losing the console write path — which is exactly what a
host with no logger of its own exercises. Reading the diff is what caught it.

## The veneers

- The four graph validators split by rule family in each language's casing
  (channel names, channel wiring, overflow, clocks, entities, schema, tier,
  window, dag). Proof of zero behaviour change is a literal-by-literal
  comparison of every string over twelve characters against
  `refactor-baseline-2` — against the **area tag**, not merely against the
  previous commit, so it covers every segment at once.
- Ruby's three separate fakes (`RecordingCore` twice, `FakeCore` once) are one
  `TestSupport::FakeCore` with an events log, `Thread::Queue#pop(timeout:)`
  for `poll` and a `ConditionVariable` for `wait_starvation` — no clock
  anywhere. The handout-contract tests drive a real `Native.context_new` and
  assert the core's own refusal; they were proved non-vacuous by breaking the
  expected token and reading the core's text back.
- PHP's `StreamOperative` and `WorkflowOperative` fold their duplicated
  `ClassSpec` construction onto `Operative::assemble`.
- `diagnostics.py` → `logger.py`, public names unchanged.
- **The comment-deletion estimates in the plan were false and were not met.**
  A scan of every `@param`/`@return` in `sdk/php/src` found *one* redundant
  line, and it carries prose; the rest are generics no signature can express.
  The plan's ≈ −500/−300/−100/−150 comment lines rest on a premise this tree
  does not satisfy. The rule was applied, the narrative marker is gone and no
  refusal string moved; the numbers were not manufactured.

## Build, packaging and the box

- **The committed native binaries and both jars are compiled `flz_core_ffi`.**
  Five `libflz_core_ffi.{so,dylib,dll}` under
  `sdk/java/flezha-sdk-native/src/main/resources/native/`, plus
  `sdk/java/flezha-sdk/dist/flezha-sdk.jar` and
  `test/apps/java_runner/dist/java_runner.jar`, all change whenever this crate
  does. `scripts/build-dist.sh --host` refreshes them, and the Java suite that
  follows is `mvn … clean verify` — a bare `verify` straight after
  `build-dist.sh` fails at the shade step on a stale `target/`.
- **`scripts/build-{node,python,ruby,php}-sdk.sh` install a DEBUG library by
  default.** That is right for the e2e (it builds in seconds and the runner is
  not what a conformance run measures) and wrong for anything measured:
  `FLEZHA_BENCH_SKIP_BUILD=1` will happily measure a debug veneer and say
  nothing. Every A/B arm is built `--release` into the scratchpad and swapped
  in with a sha256 check before and after each run.
- `build-ruby-sdk.sh` exports
  `BINDGEN_EXTRA_CLANG_ARGS=-I$(gcc -print-file-name=include)` when the box has
  no clang builtin headers; without it rb-sys's bindgen dies inside cargo with
  `'stdarg.h' file not found`, which reads like a broken Ruby. A raw
  `cargo build -p flz_core_ffi --features ruby` outside that script needs the
  same export **and** rbenv's Ruby on `PATH`, or it dies on `'ruby.h' file not
  found`.
- `build-php-sdk.sh` runs `composer install` inside the `flezha-php-zts` image
  when the host has no composer, as the invoking user (or `vendor/` comes out
  root-owned on the host).
- Three committed `.pyc` files are gone and `__pycache__/` is ignored at the
  root, so a Python runner launch no longer dirties the tree.
- `build-dist.sh --host` cross-compiles to `x86_64-pc-windows-gnu`, which is
  the only build that surfaces `flz_core/src/control_unit.rs:19`'s
  `unused_imports` warning: `socket_dir` is used only inside `#[cfg(unix)]`
  code. Pre-existing at `refactor-baseline-2` and outside this area.

## Running the conformance suite per runtime

`FLEZHA_E2E_RUNTIME` is an axis of the run. Two of the five arms need box
setup that nothing in the tree provides:

- **Ruby** — the manifest's launch line is a bare `ruby` and the system Ruby
  is below the SDK's 3.3 floor, so the run needs
  `PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH"`.
- **PHP** — there is no host PHP at all. The arm runs through a `php` shim on
  `PATH` that execs `docker run --rm -i --init --network host …
  flezha-php-zts php "$@"` with the repo, `/tmp/flezha_sockets` and
  `XDG_RUNTIME_DIR` bind-mounted at the **same absolute paths** inside the
  container, because a UDS path has to mean one thing on both sides of the
  boundary. Two of those flags are load-bearing and both were learned from a
  red run:
  - `--network host`. Without it the container has its own loopback, the UDS
    half of the suite passes and **every `transport=quic` test fails** with
    "temporary test runner not connected" — the runner dials `127.0.0.1:4433`
    and reaches itself. Seven failures, all QUIC, is the signature.
  - `-i`. The harness stops runners by closing the port's stdin; with `-i` the
    attach stream drops, the container's PHP sees EOF and drains. The same
    run also went from 506 s to 89 s once QUIC stopped burning connect
    timeouts.

  Every `FLEZHA_*` variable is forwarded by name: the runner's transport
  environment comes from `PortPilot.open_port`'s `:env`, never from a file, so
  a shim that drops the environment produces a runner that cannot find the
  control socket.

  **The shim leaks one container per wedge drill.** A host runner is killed
  when the harness closes its port; here the port kills only the `docker run`
  client, and the `wedge_probe` fixture's handler never returns, so the
  container survives the run. Give the containers a common `--name` prefix and
  sweep them (`docker rm -f`) after the arm — one was left by each of the two
  PHP runs here.

**A full `--include system` run on this box reds 2–3 tests, and the set
rotates.** Interleaved full runs of the reference (rust) arm at HEAD and at
`refactor-baseline-2` — two each, alternating — produced 3, 3, 3 and 2
failures with **no two runs sharing a set**: `dlq_e2e` tier-on (the recorded
F1 product defect) in three of five, and single sightings of
`effectively_once_checkpoint_e2e`, `effectively_once_e2e`,
`workflow_crash_resume_e2e`, `credit_flow_e2e`, `control_plane_wedge_e2e`
and `connect_discovery_e2e`. All four of the modules that failed in the
runtime arms pass 3/3 at HEAD *and* 3/3 at the baseline when run alone. Read
one red full run as the box, not as a regression — and note that a baseline
worktree under a deep scratchpad path fails `connect_discovery_e2e`'s socket
ladder with `{:error, :einval}` for its own reason (`SUN_LEN`), so that arm's
own path length is a confound to allow for.

The board's "`mix test --include system` always exits 2" no longer holds: the
two `:postgres_integration` modules now self-skip through
`FlezhaTest.SystemCase.skip_unless(System.get_env("POSTGRES_INTEGRATION") ==
"1", …)` as a `@moduletag skip:`, so the exit code tracks real failures again
— four of the seven arms exited 0.

## Narrative moved out of the module docs

Most of what the four bindings' `//!` blocks carried already lives in
`docs/dev_notes/{node,python,ruby,php}-sdk.md` — the libuv-threadpool
measurement, the `#[pyclass]`-not-`static` reasoning, Ruby's absent log
callback. Two pieces did not.

### The poll timeout is the stated interrupt bound (Python as well as Ruby)

No unblocking function is passed to any blocking entry point, in any
language. The ABI has no per-thread wake — `flz_core_shutdown` releases
*every* poller — so the timeout the caller passes to `poll` is what bounds
how quickly `KeyboardInterrupt` / `Thread#kill`, a termination signal and a
runner close are observed. That is a stated bound, not an accident, and each
package documents the value it uses. `complete` is inside the released-lock
window for the same reason `poll` is: it can park behind an entity gate or a
`DropNothing` producer, and the thread that drains that consumer is another
thread of the same interpreter.

### `jni_glue.rs` owns the byte-level crossing

Java decodes postcard itself, so this binding is the one that does **not**
use `veneer::shape`. Every `byte[]` returned to Java is copied out of the
native buffer into a fresh Java array and the native buffer is freed through
`flz_core_buf_free` before the call returns; inbound `byte[]` arrays are
short-lived copies and the JVM keeps ownership. A refusal maps once, in
`throw`, onto `com.flezha.sdk.native_.FlezhaNativeException`, falling back to
`java.lang.RuntimeException` off the SDK classpath.
