# PHP carries the workflow kind (`carry-workflow-in-php`)

`test/apps/php_runner/fixtures_workflow.php` and `fixtures_workflow_loop.php`
hold nineteen twins of the reference's durable structured workflow fixtures
(`test/apps/rust_runner/src/workflow.rs`, `workflow_loop.rs`) under the same
factory ids, procedure names, channel names and output bytes. The manifest
defers nothing, `runner.php` registers them through `defineWorkflow`, and no
file under `test/system/` is touched: flipping `deferred` to `[]` is what
un-skips the scenarios that already exist.

## A fixture is a class, so the reference's macros are base classes

Node's port could write `durableStep(name, {change, emit, compensate, gate})`
because a Node fixture is a value a factory returns. A PHP fixture is a
**class**: `Definition::declarationOf` reads `#[Proc]` off the class and
`Builder::nodeFor` takes a `class-string`, so a factory function has nothing to
hand back. The transliteration is inheritance — which is not a workaround but
the mechanism this SDK already leans on, since `Definition::declarationOf`
merges the attribute **field by field** up the hierarchy precisely so a
subclass that renames the node keeps everything else.

So the four axes become four protected hooks on `DurableStep`
(`gated`, `changeValue`, `release`, and `UndoingStep`'s pair
`compensationGated` / `undoRecord`), and every durable procedure in the module
is a class body of three lines or fewer.

**`UndoingStep` exists because `Definition::overrides` reads which class
declared a hook** (`getDeclaringClass() !== Procedure::class`). A `compensate`
on `DurableStep` would make *every* durable step compensable and contradict
`test/fixtures/declarations/commit_block_workflow.json`, whose
`compensable_procs` is empty. The saga axis therefore lives one class below the
durable one, and `saga_b` — whose hook does nothing — is a two-line subclass
that declares it anyway, because declaring it is what makes the stage
compensable.

Node's third factory has **no PHP counterpart and needs none**: `Procedure`
*is* the stateless step — no `state()`, no `applyChange()`, the whole behaviour
in `run()`. A `StatelessStep extends Procedure {}` would be an empty layer.

## The gate parks a pool worker, and there is no timer anywhere

```php
$pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
while (!self::released()) {
    $read = [$pair[0]];
    stream_select($read, $write, $except, null);
}
```

Three facts pick this spelling, and each is checkable.

1. **The manifest declares `"model": "thread-pool"`**, so the contract's pool
   clause applies literally: park a pool worker, never the driver thread. There
   is no PHP counterpart to node's never-settling promise — the dispatcher calls
   `run()` synchronously and the SDK has no fiber seam — so the activation's
   own thread is what parks. `flz_core` owns the driver, the heartbeat and the
   control channel on threads of its own, so the runner keeps answering control
   pings; `SwooleExecutor` grows the pool on `flz_core_wait_starvation`, which
   is what keeps a sibling dispatchable. Under the default one-loop
   `SyncExecutor` the sibling waits, and that is the executor's own stated
   in-flight ceiling.
2. **A timer is a gate failure, not a style choice.** `SLEEP["php"]` is
   `\b(?:u)?sleep\(` (`scripts/refactor_metrics.py`) and `test/apps sleeps` is
   **blocking at 8** — of which `rust_runner/src/workflow.rs`,
   `java_runner/…/WorkflowE2eGate.java` and `php_runner/fixtures_probe.php`
   already hold three. A `usleep(20_000)` poll, which is the direct
   transliteration of both references, makes it 9. The socket park matches no
   regex and costs no processor time. Running the ratchet's own code over the
   two new files reports `sleeps 0` and `narrative_markers 0`.
3. **The gate is a boot-time decision.** `WorkflowResumeHarness.boot_runner/4`
   puts `FLEZHA_WORKFLOW_E2E_RELEASE` in the *replacement* runner's
   environment and ends the first runner with `kill -KILL`, so a park that
   never ends is torn down with the process it parks. The loop's condition is
   the same `while (!released())` the reference writes; only the body differs.

The one thing an indefinite `stream_select` costs is a warning: a signal
delivered to a parked runner ends the wait with `EINTR`, and PHP reports
*"Unable to select [4]: Interrupted system call"* on the runner's standard
error even though the loop simply parks again. That warning is the noise and
not the event, so the call — and only that call — is `@`-suppressed.

## `wake_after_ms` cannot be an attribute argument

A PHP attribute takes constant expressions, and `bench_loop_workflow`'s wake
deadline comes from `FLEZHA_BENCH_WAKE_AFTER_MS`. `#[Workflow(...)]` therefore
cannot express it. The seam that already exists is
`Operative::workflowDeclaration()`, a `protected static` hook returning the
attribute value, so that one class overrides it and builds its `#[Workflow]` at
registration:

```
FLEZHA_BENCH_WAKE_AFTER_MS unset → {"loop_mode":"per_input", … ,"max_duration_ms":86400000}
FLEZHA_BENCH_WAKE_AFTER_MS=250  → …,"max_duration_ms":86400000,"wake_after_ms":250}
```

This is the only fixture in the set whose declaration is not a literal, and it
needed no SDK change.

## `items` is a sorted list because a PHP array keeps insertion order

The reference's `SeenState` is a `BTreeSet<String>`, chosen so snapshot bytes
are deterministic. A PHP array preserves insertion order, so `DurableStep`'s
fold sorts after an insert and skips a value it already holds — the same
determinism, and the same idempotent re-run after a resume.

## Every sink declares its encoder

The reference's bare `.sink(name)` is `default_plugin_serializer`, which is raw
UTF-8 for a `String`. A bare sink here falls back to the class codec, which is
**JSON**, so a bare `j_out` would deliver `"j:b:{\"seed\":\"go\"}"` — quoted and
escaped — against a raw-binary expectation. `workflow_crash_resume_e2e_test.exs`
compares the forwarded frames as raw binaries, so that failure would read as a
fan-out defect and be diagnosed as one. Every `sink(...)` in the two modules
passes `Wire::bytes(...)` or a `"prefix:$v"` lambda.

The **edge** encoding is untouched: an inter-procedure hop carries JSON and the
core derives that channel's journal codec from the channel's kind, so no
fixture declares one. That is KI-144's `sinkEncode` invariant, and a fixture
that reached for the edge encoding would break it.

## KI-141 is answered on the wire, and the control proves the test measures it

`diamond_workflow`'s `dfan` is one channel carrying a sink and **two**
consumers; the declaration probe reports `consumers=[1,4] sink=diamond_sink`,
where the per-edge model would have shown one. Commenting out the
`$fan->into(DiamC::class)…` arm and re-running only the no-crash diamond turns
it red on both transports:

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
assertion is not measuring the fan-out at all.

## KI-147 holds on PHP, and PHP was the veneer carrying the most

`HostClassDecl::streamTier` returns `[]` for a workflow class, so
`effectively_once`, `ack_granularity`, the checkpoint family and
`correction_window_ms` never reach the wire from one. The half specific to this
SDK: `Operative::assemble` copies **every** tier field from the builder for both
kinds, where the other three veneers hardcode workflow defaults — so before the
gate at the decl seam, PHP carried more of the refused keys than any other
veneer. It is correct that it still copies them: `Ctx::stampIdentity` reads
`$spec->effectivelyOnce` to decide whether a keyed plugin frame carries its own
identity, and `tier_commit_block_workflow` calls `$c->effectivelyOnce()` for
exactly that reason. Only the declaration is gated. The probe over all nineteen
reports no refused key on any wire declaration.

## The single-loop executor is why a `private static` is safe here

`RetryFlaky`'s attempt counter is a `private static int`, as
`PoisonProbe::$antidoteTaken` and `WedgeProbe::$aborted` already are. Under
`SwooleExecutor` each worker interpreter would hold its own copy — the standing
KI-133/KI-132 shape — and this box cannot run that executor at all: `PHP_ZTS`
is 0, `Swoole\Thread` is absent and `ffi.enable` is `preload`, so all three of
`SwooleExecutor::requirementRefusal`'s arms fire and the PHP suite's five skips
are all `SwooleExecutorTest`. The static is safe **for that reason and only that
reason**, and `retry_workflow` is in a `reference_runtime: true` describe, so
nothing here drives it on PHP anyway.

## What runs on PHP, and what still spawns rust

`FlezhaTest.SystemCase` stamps `@moduletag skip: deferral(capability)`, so the
manifest flip un-skips every `capability: :workflow` module; `reference_runtime:
true` in a describe title is what still pins a cell to rust. The eight
`(reference/uds)` cells — the tier, the opt-out, the two outbox drills and all
four of `workflow_control_flow_e2e_test.exs` — prove **controller** mechanics
and merely need a runner to drive them. Unpinning them widens what runs for
every carrier at once, so it belongs to R7's close-out rather than to one
SDK's port.

Every `mix test` also boots two namespace-less reference runners from
`config/runtime.exs` (`FLEZHA_EXTERNAL_PROCESSES`); they belong to no module's
fleet and are not evidence that a scenario spawned rust.
