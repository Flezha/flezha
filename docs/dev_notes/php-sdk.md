# PHP SDK — implementation notes

Per-change notes for `add-php-sdk`. The design and the requirements are in the
archived change; what follows is the set of things that were only learnable by
building it.

## The toolchain, and why there is a Dockerfile

`docker/dev/Dockerfile.php` has two targets because the SDK has two postures
and neither is what a distribution ships:

- `nts` — stock non-thread-safe PHP. `ext-ffi` is **bundled with PHP's source
  but not built** by the official images, and it links against libffi, so the
  target needs `libffi-dev` + `docker-php-ext-install ffi`. Composer also needs
  `git` and `unzip` or every install fails in `ZipDownloader`.
- `zts` — the thread-safe build plus Swoole. The tag is `php:<v>-zts`, **not**
  `php:<v>-zts-cli`; that tag does not exist. Swoole's `./configure` needs
  `libssl-dev` (its openssl support is on by default and the build dies at
  "Package requirements (libssl) were not met" without it), and
  `--enable-swoole-thread` is what provides `Swoole\Thread` at all — the pecl
  default build has no thread API.

## `ffi.enable=preload` is refused inside a `Swoole\Thread` — measured

The single most load-bearing finding, and it is not in any design document.
PHP's shipped default `ffi.enable=preload` permits FFI **from the CLI SAPI**,
and a `Swoole\Thread` is a fresh interpreter that is not it. The first
`FFI::cdef` in a worker throws
`FFI API is restricted by "ffi.enable" configuration directive` — in a thread,
where it reads as a broken install rather than as a configuration.

So `SwooleExecutor` has **three** requirements, not two, and it checks all
three before the runner connects: a ZTS build, `Swoole\Thread`, and
`ffi.enable=true`. The check is `requirementRefusal(bool, bool, string)` — a
pure function of what it reads — because `ffi.enable` is `PHP_INI_SYSTEM` and a
test cannot make a satisfying build unsatisfying at runtime; every arm is
provable on one machine only in that shape. The `zts` image sets `ffi.enable=1`
for the same reason.

## PHP's FFI has two shapes the C declarations must be written around

Both were found by a failing test, and both would have been a runtime failure
on the first activation rather than a load error:

1. **A `char *` return arrives as a PHP string**, not `CData`. `FFI::isNull()`
   on it throws a `TypeError`, which turned every core refusal into a
   `TypeError` that hid the refusal. `Core::cstring()` reads all three shapes
   (string, null, `CData`) in one place.
2. **`FFI::string()` takes the pointer itself.** `$out->cdata` on a `char *`
   CData throws "Attempt to read field 'cdata' of non C struct/union" — only a
   *scalar* CData has `->cdata`. `$len` (a `size_t`) does; `$out` (a `char *`)
   does not.

Every byte buffer in the `cdef` is declared `char *` where Rust says
`uint8_t *`. They are the same parameter at the ABI, and `char *` is the one
PHP converts a string to and from without a per-call copy. `namespace` is a PHP
keyword, so that parameter is `ns`.

## PHP does not inherit attributes, and the fix is field-wise

`ReflectionClass::getAttributes()` reads only the class's own attributes. Taken
literally, `class Sub extends Base {}` comes out single-entity with the default
mailbox capacity and schema version 1 — the exact defect the Ruby SDK found in
its own class-instance-variable declarations.

Nearest-attribute-wins is **not enough**, and the test app is what proved it:
the common case is a subclass that renames the node and changes nothing else
(`#[Proc(name: 'stress_b')]` on a child of a static-keyed parent), and
whole-attribute inheritance silently dropped the key set exactly there —
`stress_b/c/d` came out `single`, `macro_static_entry_procedure` came out
`single`, and the bounded-mailbox procedures came out capacity 1024 instead of
4. So `Definition::declarationOf` merges **field by field** up the hierarchy,
reading `ReflectionAttribute::getArguments()` (which returns only what was
written) and mapping positional arguments through the constructor's parameter
names. `name` is the one field never inherited, for the reason Ruby's
`procedure_name` is not: a node identity is per class, and an inherited one
would make two procedures claim one node.

## The fixtures are read out of the Rust source, not copied

`SchemaFixtureTest` parses `flz_core/src/schema.rs` for `SCHEMA_VERSION` and
for the pinned hex, so a schema change is a red PHP build in the commit that
makes it. A copy would only fail once someone remembered to update it.

Only the two directions a host uses exist — activations decode, results encode
— and each is pinned **against the bytes** rather than against the other. A
decode-then-encode round trip passes a mistake that is symmetric, and every
mistake in a hand-written codec is.

## `Swoole\Thread` cannot carry a closure, so the seam carries multiplicity

A thread is constructed from a *script path* plus scalar arguments and
bootstraps a fresh interpreter. `Swoole\Thread\Atomic` is one of the few values
that crosses. That is why:

- the `Executor` seam is `run(Runner)` / `stop()`, never `submit(callable)`;
- the shared budget and metrics live in `SwooleLoopState`'s atomics rather than
  per loop — one runner has one budget, and N threads each spending their own
  would recycle N times too late;
- the worker bootstrap is a **script file** (`src/Executor/swoole_worker.php`),
  which is why the Swoole containment rule is "the adapter's own files" rather
  than "the adapter file". `PackagingTest` enforces it against the *tokenised*
  source, so a doc comment explaining the seam is not an offence.

The supervisor is the **main thread**, not a fourth mechanism: `run()` blocks
until the pool is done anyway, the main thread is otherwise idle, and it is the
thread that owns the pool it grows. Its time-limited `wait_starvation` is also where
the runner observes a stop request.

## Shutdown is polled, not asynchronous

A non-thread-safe PHP has no second thread to put a watcher on, so `SIGINT` /
`SIGTERM` are dispatched with `pcntl_signal_dispatch()` **between polls**
(`pcntl_async_signals(false)`), and standard-input EOF is a zero-timeout
`stream_select` in the same place. The poll timeout (250 ms) is therefore the
stated upper bound on how quickly either is observed.

`ext-pcntl` is **not** in the official Docker images (`docker-php-ext-install
pcntl`). The runner says so at startup rather than degrading silently: without
it SIGTERM ends the process immediately, so in-flight activations are abandoned
rather than drained. Nothing is lost — the core re-hands them on the next bind
— but the shutdown is abrupt where it did not need to be.

## What is deliberately absent

- **No state-mutation guard.** `$ctx->state` is writable. Node's guard is a
  `Proxy`; PHP has no counterpart that does not cost an allocation per field
  read. `Harness::assertReplayEquals()` is what makes the rule safe, and here
  it catches BOTH an unrecorded mutation and an impure fold.
- **No `encodeActivation` / `decodeResult`.** They would have no caller: the
  fixtures are pinned against the bytes in the direction each is used.
- **No launcher, no fork.** `flz_core` spawns ~20 OS threads that do not
  survive `fork(2)`, and the process/supervision layer is the orchestrator's.

## Deferred, by name

The manifest defers `workflow` alone — on **fixtures, not mechanism**: the
workflow kind is implemented and reached through the same core the Rust and
Java arms prove it on, and its fixtures wait on two builder surfaces every
veneer lacks. The entry-idle observer is this SDK's own host layer
(`src/EntryIdle.php`) over the latch the core keeps by watch id; it is not
something the core proves on this SDK's behalf.
