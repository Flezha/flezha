# PHP SDK

A poll host over `flz_core`, through `ext-ffi`. The SDK contains no kernel
code: transport, framing, ordering, per-cell exclusivity, durability,
retries, the retry→park ladder, timers, credit, routing, bounded runs
and reconnect all live in the core. This package brings the authoring surface,
the codec for user types, the concurrency host and idiomatic errors.

Everything here is reached through the C ABI (`flz_core_ffi`), the same one the
Java SDK's JNI bridge consumes — so a capability the PHP SDK cannot reach is a
defect of the ABI rather than of this package.

- The boundary in mechanism detail → [`poll_core.md`](poll_core.md)
- The language-agnostic authoring contract → [`authoring_contract.md`](authoring_contract.md)
- What proves this SDK still works → [`conformance.md`](conformance.md), and
  the manifest at `test/apps/php_runner/conformance.json`

## Supported PHP versions

`php >= 8.4` and `ext-ffi`, and nothing else. 8.4 is the supported line, not
the minimum the code could parse: the authoring surface itself needs only 8.1 —
enums, `never`, readonly properties, first-class callable syntax — and `ext-ffi`
is stable from 8.0, but 8.4 is what this repository builds, tests and benchmarks
the SDK on. CI runs the package's suite on the declared floor, which is also the
newest release; the matrix regains a second entry when a newer line ships.

`SwooleExecutor` declares its own, higher requirements, checked when it is
selected rather than at install: see [Choosing an executor](#choosing-an-executor).

## Installing

```
composer require flezha/sdk
```

Installation pulls no further packages. The package is also a Composer plugin:
as Composer installs it, it downloads the one `libflz_core_ffi` binary the host
needs from the release matching the package's version, checks it against that
release's SHA-256 manifest, and places it in the package's `lib/<platform>/`.
Releases publish it for `linux-x64`, `linux-arm64`, `macos-x64`, `macos-arm64`
and `windows-x64`. A checksum mismatch or a failed download refuses the install
rather than leave a package that cannot load.

Composer asks once whether to trust the plugin. A non-interactive install needs
it allowed in the project's `composer.json`:

```json
"config": { "allow-plugins": { "flezha/sdk": true } }
```

`FLEZHA_NATIVE_LIB_DIR` names a directory to load the binary from in preference
to the package's own, on any platform. When it already holds the binary at
install, nothing is downloaded, which is how an offline install works. This
repository's own build script and test app use it too.

An install that skipped the plugin, through `--no-plugins` or by not allowing
it, has no binary, and the first call into the core refuses naming how to
finish: allow the plugin and run `composer reinstall flezha/sdk`, or download
the asset and manifest the message names, check the checksum, and put the
binary where it says.

The binaries are build output, not source: `scripts/build-dist.sh` cross-builds
all five triples into `sdk/php/lib/<platform>/` from the same libraries the
Java distribution embeds — the *same* library, which is the point of using
`ext-ffi` rather than a compiled extension — and `scripts/build-php-sdk.sh`
stages just the host's for a checkout. They are not committed, and a checkout
that has them downloads nothing.

## Authoring

```php
use Flezha\Attribute\Operator;
use Flezha\Attribute\Task;
use Flezha\Ctx;
use Flezha\Flezha;
use Flezha\Graph\Builder;
use Flezha\Tasklet;
use Flezha\UnboundedOperator;
use Flezha\Timer;

#[Task(name: 'session', idleTtlSeconds: 3_600)]
final class Session extends Tasklet
{
    private const QUIET_MS = 30_000;

    public function initState(?string $cellKey): mixed
    {
        return ['events' => 0, 'deadline' => null];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        if ($change['kind'] === 'event') {
            $state['events']++;
            $state['deadline'] = $change['deadline'];

            return;
        }
        $state['events'] = 0;
        $state['deadline'] = null;
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        // Cancel the timer this cell is holding before arming the next one,
        // by the timestamp its registration answered.
        if ($ctx->state['deadline'] !== null) {
            $ctx->cancelTimer('processing_time', (int) $ctx->state['deadline']);
        }
        $ctx->change(['kind' => 'event', 'deadline' => $ctx->timerAfter(self::QUIET_MS)]);
    }

    public function onTimer(Timer $timer, Ctx $ctx): void
    {
        $events = $ctx->state['events'];
        $ctx->change(['kind' => 'closed', 'deadline' => null]);
        $ctx->emit(['session' => $ctx->cellKey, 'events' => $events, 'closed_at' => $ctx->now()]);
    }
}

#[Operator(factoryId: 'sessions')]
final class SessionOperator extends UnboundedOperator
{
    public static function channels(Builder $c): void
    {
        $c->input('events')
            ->keyBy(static fn (array $e): string => (string) $e['session'])
            ->into(Session::class)->keyed()
            ->sink('closed');
    }
}

Flezha::createRunner('default')->registerFactory(SessionOperator::class)->run();
```

That is `examples/E04_SessionTimeout.php`. A tasklet is a class extending
`Tasklet` with a `#[Task]` attribute. The attribute carries the scalar
declarations; the methods carry the behaviour. Only `run()` is required. Which
optional hooks a tasklet declares is read from the methods it **overrides**:
`initState` overridden means it keeps state, `applyChange` that it folds
changes, `onTimer` that it may register timers. A declaration and its
implementation therefore cannot disagree.

- **`name` is required.** It is part of every cell's durable path, so renaming
  the class keeps the state and renaming `name` declares a different tasklet.
  A class with no `name`, or an empty one, is refused at registration naming
  the class, and a subclass writes its own: every other `#[Task]` argument is
  merged down the class hierarchy, `name` never is.
- **`run` returns `void`.** A value leaves a tasklet only through
  `$ctx->emit()` or `$ctx->emitKeyed()`, and an input that should produce
  nothing simply emits nothing. PHP itself refuses a `run` that returns a value.
- **`$ctx->part($value)` previews the output while `run` is still working.**
  It sends `$value` on the tasklet's own output channel, encoded as
  `$ctx->emit($value)` would encode it, and returns without waiting. It works
  in `run` and `onTimer`, in an unbounded operator and in a bounded operator's
  stage. The operator's listeners receive the parts before the output; a sink
  never does, and a part is never durable or replayed. Parts are numbered from
  0 within each attempt, so a retry starts its preview again. A part that
  cannot leave at once, such as in a local run, is dropped. A channel that
  does not end in a sink (it only feeds another tasklet, or ends in
  `response()`) throws a `FlezhaException` coded `part.no_sink`, and a context
  kept after its invocation returned throws `part.outside_invocation`. The
  testing harness sends nothing.
- **`initState(?string $cellKey)`** is a cell's state before anything has
  happened to it; `$cellKey` is null for a single-cell tasklet.
- **A change is plain data.** `$ctx->change(['kind' => 'event', ...])` records
  any value JSON carries, and `applyChange(mixed &$state, mixed $change)` folds
  it, live and again on restore. No change class is needed; the optional
  `stateType` and `changeType` below are for an author who wants typed values.
- **The cell mode comes from the wiring.** A tasklet fed through `keyBy` gets a
  cell per key, one declaring `cellKeys` a cell per listed key, and any other
  one cell. `keyBy` into a tasklet declaring `cellKeys` is refused as
  `topology.key_by_on_unkeyable_target`.
- **`idleTtlSeconds`** evicts a dynamic cell idle that long, in whole seconds;
  unset never evicts. `maxCells` caps how many are live.
- **The clock follows from what the class writes.** Overriding
  `eventTime(mixed $input): int` makes it event time,
  `#[Task(logicalClock: true)]` makes it the cell's change sequence, and
  otherwise it is the wall clock; `$ctx->now()` serves whichever it is. Doing both is refused naming
  `eventTime` and `logicalClock`, and a logical clock on a tasklet with no
  `initState` is refused as `topology.unservable_clock`.

Everything a type system would have caught is refused at registration, naming
the class, the handler and the defect: the same set, in the same vocabulary, as
the Rust builder's compile-time refusals.

### What each kind takes

A class extending `BoundedOperator` carries its driver settings in
`#[Bounded operator]`: the loop mode, the rollback mode, a maximum duration, a wake
deadline and `syncDurability: true`, the one spelling of synchronous
durability. Retry is the edge's for both kinds: `retryMax` and
`retryBackoffMs` on the channel that feeds a tasklet, three attempts from a
50 ms base when neither is written, and `retryMax(1)` fails fast.

A setting of one kind written on the other is refused by name, never dropped.
The builder's `effectivelyOnce()`, checkpoint calls and `correctionWindow()`
reach the core from a bounded operator too, which refuses them as
`topology.effectively_once_on_bounded`, `topology.checkpoint_on_bounded` and
`topology.correction_window_on_bounded` when the runner starts. `#[Bounded operator]` on a class
extending `UnboundedOperator` is refused at `registerFactory`, naming the
attribute and the class, and as `topology.sync_durability_on_unbounded` when it sets
`syncDurability: true`.

`#[Operator]` carries the two settings of both kinds: `persistent` (default
`true`) and `sleepWhenIdle`, which defaults to `persistent` on a bounded operator and
to `false` on an unbounded operator. `sleepWhenIdle: true` with `persistent: false` is
refused as `topology.sleep_without_persistence` when the runner starts.

A bounded tasklet registers timers as an unbounded tasklet does, and a firing
runs as a pass of its own ([`cell_timers.md`](cell_timers.md#timers-in-a-bounded-operator));
only inside a `compensate` hook do `timerAfter`, `timerAtEventTime` and
`cancelTimer` throw naming `runtime.timers_unavailable`.

### The short form

An operator class that defines its one tasklet's `run` instead of
`channels()` is a whole operator: one class, no tasklet class beside it. The
kind is the class it extends, and `#[Operator]` names the channels around that
tasklet: `sink` is the plugin sink its output reaches, `input` the entry
channel, and `codec` the entry codec the builder's `input` takes.

```php
#[Operator(factoryId: 'upcase', sink: 'out', codec: ChannelCodec::Text)]
final class Upcase extends UnboundedOperator
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit(strtoupper($input));
    }
}

#[Operator(factoryId: 'place', sink: 'placed', input: 'order', codec: ChannelCodec::Text)]
final class Place extends BoundedOperator
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit($input);
    }
}

Flezha::createRunner('default')
    ->registerFactory(Upcase::class)
    ->registerFactory(Place::class)
    ->run();
```

The generated operator is the one you would have written by hand with that
single tasklet: an entry channel named `input` (or the `input` argument's
value) decoded by `codec`, the tasklet, a plugin sink named by `sink`, a
factory id equal to `factoryId`, and every class-level setting at its default.
The tasklet is named `factoryId` verbatim, so the full form that wires a
tasklet of that name keeps every cell where it was. It runs the class's
`run` and takes whichever hooks the class defines (`initState`, `applyChange`, ...),
and a `#[Task]` on the class carries its settings, all but `name`. A bounded operator
short form is a `once` bounded operator, run once over its first message:
`flz operator start --text '<payload>'`, or a start followed by
`flz operator send`; a second message to a `once` instance is accepted and
never read. `#[Bounded(loopMode: ...)]` beside `#[Operator]` sets its loop
mode, the one class-level setting a short form takes:

```php
#[Operator(factoryId: 'chat', sink: 'replies', codec: ChannelCodec::Text)]
#[Bounded(loopMode: LoopMode::PerInput)]
final class Chat extends BoundedOperator
{
    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->emit($input);
    }
}
```

`sink` is optional. Without one, the operator declares its entry channel into
the tasklet and no plugin channel, which is all an operator that only keeps
state needs:

```php
#[Operator(factoryId: 'counter', input: 'ticks', codec: ChannelCodec::Text)]
final class Counter extends UnboundedOperator
{
    public function initState(?string $cellKey): mixed
    {
        return ['seen' => []];
    }

    public function applyChange(mixed &$state, mixed $change): void
    {
        $state['seen'][] = $change;
    }

    public function run(mixed $input, Ctx $ctx): void
    {
        $ctx->change($input);
    }
}
```

Anything more is the full form: a second tasklet, a fan-out, a merge, a keyed
edge, a retry ladder, the effectively-once tier, a correction window, and every
class-level setting but a bounded operator's loop mode. A short form writing another
one, `persistent: false`, `sleepWhenIdle` or a `#[Bounded operator]` argument other
than `loopMode`, is refused at `registerFactory` naming the setting and the
full form.

The factory id is always written: it is an address a user types, and a
class rename must not move it, so a class with no `#[Operator]` is refused as
any operator is. `registerFactory`, where this SDK places every refusal,
refuses a class defining both `run` and `channels()` naming
`topology.short_form_with_topology`, and one defining neither naming both. A second
operator of either kind under a factory id the runner holds is refused naming
`topology.duplicate_factory_id`. `#[Task]` takes no `kind`, `sink`, `input` or `codec`,
and a tasklet carrying one is refused naming `#[Operator]`. `#[Task]` on a
class defining `channels()` is refused too: a full form's tasklet settings go
on its tasklet classes. A hook a short form defines is held to the signature
`Tasklet` gives it, and one PHP would refuse on a tasklet is refused naming
the hook. There is no
`topology.sink_without_output`: `run` is declared `: void` on both `Tasklet` and
`Operator`, a value leaves only through `$ctx->emit()`, and no body declares
an output type, so nothing in a class says whether it emits. The manifest
leaves `shorthand_typed_output` off and the conformance row skips by name.

### Channel payloads

A channel names the format of its payload with `codec:` on `input` and on
`sink`, a case of the `Flezha\ChannelCodec` enum:

| Codec | An entry hands the tasklet | A sink sends |
| --- | --- | --- |
| `ChannelCodec::Text` | the string, if it is valid UTF-8; otherwise the decode fails | a valid UTF-8 string as it is; any other value fails the emit |
| `ChannelCodec::Bytes` | the string as it arrived | any string as it is; any other value fails the emit |
| `ChannelCodec::Json` | the decoded JSON, an object as an array | any value as JSON, a string quoted |

```php
$c->input('in', codec: ChannelCodec::Text)->into(EchoTasklet::class)->sink('out');
```

An entry with no codec and no decoder reads JSON: `{"a":1}` arrives as
`['a' => 1]`, and plain text fails its decode and rides the retry and
dead-letter ladder. A sink with no codec and no encoder follows the value: a
string is sent as it is and anything else as JSON. So a sink needs no codec for
a string or a JSON value, and names one only to quote a string, with
`ChannelCodec::Json`. A decoder or encoder callable takes the place of a codec
for anything else, and a codec given beside a callable on one channel is refused
when the operator is declared, naming the operator and the channel. PHP ships
no MessagePack library, so the enum has no MessagePack case.

`->response()` ends a channel on the answer to an outside call instead of a
sink, and takes the sink's arguments, `?callable $encode` and
`?ChannelCodec $codec`: [`calls.md`](calls.md). `->call(target: 'pricing', deadlineMs: 500)`
or `->call(factory: 'agent', deadlineMs: ..., create: false, codec: ...)` ends a chain on a
call to another operator and answers an edge of readonly `Flezha\Response` values:
[`calls.md`](calls.md#operator-calls-call).

A JSON integer outside PHP's 64-bit range decodes as a `float`. `NAN`, `INF`, a
string that is not valid UTF-8 or an object that is not `Codable` has no JSON
form, and sending one to a sink that names no codec fails the emit naming the
channel.

A value passed between two tasklets of one operator never crosses a channel
codec: it stays JSON. An Elixir entry declared with no decoder hands its
tasklet the binary instead, so the same bytes on an undeclared entry are an
array here and a string there.

### State and Change

Durable `State` and `Change` values serialize as **JSON**, produced by an
author-declared `toArray()` and rebuilt by `fromArray()`:

```php
final class Totals implements \Flezha\Codable
{
    public function __construct(public int $count = 0) {}

    public function toArray(): array { return ['count' => $this->count]; }

    public static function fromArray(array $data): static { return new self((int) $data['count']); }
}

#[Task(name: 'counting', stateType: Totals::class)]
final class Counting extends Tasklet { /* ... */ }
```

**Never PHP's `serialize()`.** It embeds fully-qualified class names and
mangles private property names (`\0Class\0prop`), so renaming or moving a class
breaks the restore of every record already on disk — a data-loss-shaped failure
with nothing to warn about it. The durable bytes here carry the array and
nothing else.

Without a declared type, plain arrays and scalars are carried directly. What
JSON cannot represent — an object that is not `Codable`, a byte string, `NAN` —
is refused at **registration**, against what `initState()` returns, so an author
learns at startup rather than at the first restore.

The same encoding answers `flz state get` and a migration handover, because it
already is JSON. There is no second form to convert between.

### Testing a tasklet

`Flezha\Testing\Harness` drives one tasklet without a controller, through the
same dispatch and restore functions a runner uses:

```php
$harness = new Harness(Session::class, cellKey: 'alice');
$harness->feed(['session' => 'alice']);
$harness->assertReplayEquals();
```

`assertReplayEquals()` folds the recorded change log into a fresh initial state
and requires the answer to equal the live one. In this SDK that catches **both**
failures at once: a handler that mutated the state without recording a change,
and an `applyChange` that is not a pure fold. PHP has no cheap deep read-only
view of a value — the Node SDK's guard is a `Proxy`, which has no counterpart
here that does not cost an allocation per field read — so this assertion is what
makes the mutate-only-through-changes rule safe. Put it in your suite.

## One runner per process

The SDK starts exactly one runner per process and never forks, spawns,
supervises or recycles. **N processes are N runners**, which is what the
controller's placement already spreads operators across, and it keeps this
layer agnostic between Kubernetes, systemd and a supervisor's own process
management.

## Local mode: run a graph with no controller

`LocalOperator::start` takes the operator class you deploy and runs its
declared graph in this process — no controller, no socket, no local-mode member
on the class:

```php
$run = LocalOperator::start(WordCountOperator::class);

try {
    $run->onSink('totals', static fn (SinkOutput $out) => print($out->payload . PHP_EOL));
    $run->feed('lines', 'the quick brown fox');
    $run->runUntilIdle(10_000);
} finally {
    $run->close();
}
```

`onSink` registers against the sink's **channel** name, which is the producing
tasklet's name unless the declaration named it.

**This host has one thread, so `runUntilIdle` drives the loop rather than
parking in the core.** Under the default `sync` executor the caller *is* the
poll loop: one turn takes an activation if the core has one, answers it, and
hands on whatever the sinks released; a turn that hands nothing back asks the
core whether the graph has settled. A thread parked inside the core's own idle
wait would be waiting on work it had stopped doing. Worked example:
[`../../sdk/php/examples/E07_LocalMode.php`](../../sdk/php/examples/E07_LocalMode.php).

**State is held in memory and nothing is durable** — no write-ahead log, no
snapshots, no dead-letter queue. Durability, effectively-once delivery, plugins,
placement and migration, the dead-letter queue, observability export, authz and
multi-runner scale need a controller; a declaration asking for one of those is
refused at startup, naming it. Walk-through:
[`../quickstart.md`](../quickstart.md).

## Sizing, as arithmetic

One loop processes one activation at a time, so a deployment is described by
two numbers under either executor — only the unit of concurrency changes:

- **throughput per loop ≈ 1 ÷ tasklet-body latency.** A 1 ms CPU-bound body
  gives about 1000/s; a 20 ms query gives about 50/s.
- **in-flight concurrency = loop count.** Processes under `SyncExecutor`;
  threads × processes under `SwooleExecutor`. A body that waits 2 s on a remote
  API occupies its loop for 2 s, so 100 simultaneous such calls need 100 loops.

Worked: 5,000 orders/s through a 4 ms body needs 20 loops. Under `SyncExecutor`
that is 20 processes — 20 Kubernetes replicas, or a systemd template unit with
20 instances. Under `SwooleExecutor` on an 8-core host it is 3 processes of 8
threads.

The runner **measures** this rather than leaving it to be assumed: at shutdown
it logs the activations it served and the mean tasklet-body time, with the
per-loop throughput that implies.

The ceiling differs, and that is the whole reason both executors exist. A
process carries a PHP interpreter plus `flz_core`'s ~20 OS threads, so a few
dozen to low hundreds per host is comfortable and thousands is not. A Swoole
thread carries an interpreter and no second core runtime, so one process
reaches its core count without multiplying either.

### Supervisor recipes

The SDK spawns nothing, so process count is the orchestrator's. All three of
these are equivalent to it:

**systemd template unit** — `flezha-runner@.service`:

```ini
[Service]
ExecStart=/usr/bin/php /srv/app/runner.php default
Restart=always
Environment=FLEZHA_MAX_ACTIVATIONS=100000
```

`systemctl start flezha-runner@{1..20}`.

**Kubernetes** — one container, `replicas: 20`. The runner exits zero on
SIGTERM after draining, so a rolling update loses nothing.

**RoadRunner's `service` plugin** — the one PHP-native supervisor worth naming,
*as a supervisor*:

```yaml
service:
  flezha:
    command: "php runner.php default"
    process_num: 20
    remain_after_exit: false
```

RoadRunner is **not** a transport here: routing activations through Goridge is
the sidecar hop this design exists to remove. FrankenPHP fits as a container
when an app wants HTTP and a runner in one deployment, but not as a threading
host — its threads are driven by `frankenphp_handle_request()`, so a
never-returning poll loop pins one and fights its request-driven scheduler.

### Recycling

`FLEZHA_MAX_ACTIVATIONS` (or `->maxActivations(N)`) shuts the runner down
cleanly after N activations and exits zero, so a supervisor can recycle a
long-lived process. Unlimited by default. A clean shutdown costs no data:
unpolled work stays journaled controller-side and is re-forwarded on the next
bind, and the activation the budget refused is handed back untouched.

## The executor seam

```php
interface Executor
{
    public function name(): string;
    public function run(Runner $runner): void;   // MAY drive the loop on N threads
    public function stop(): void;                // every loop leaves user code first
    public function state(): ?LoopState;
}
```

The seam carries **loop multiplicity, not work distribution**. The obvious
shape — `submit(callable $activation)` — is unimplementable for Swoole: a
`Swoole\Thread` is constructed from a script path plus scalar arguments and
bootstraps a fresh interpreter, so closures, objects and resources do not cross
the boundary, and anything shaped as "hand this callable to a thread" has
exactly one possible implementation, the inline one. Multiplicity is
expressible under every PHP threading model.

**Safety comes from the core, not the adapter.** `PollCore` holds per-cell
exclusivity — it will never hand out two activations for the same cell
concurrently, however many threads poll it — so no executor can produce a race
in user state. An executor can affect throughput and latency; it cannot affect
correctness. That is what makes it safe to ship two and to let you write a
third.

Each thread polls, dispatches and completes within itself, so no activation
crosses a thread boundary: there is no queue, no saturation and no backpressure
question — a busy thread simply does not poll.

`FLEZHA_PHP_EXECUTOR` (`sync` | `swoole`, default `sync`) selects it for a run.
It is an axis of the run rather than a property of a file, so no test, fixture,
benchmark or manifest entry exists in a per-executor variant — and the launch
line names it nowhere: a runner inherits the environment of whatever started
it, and the entry script reads the variable itself.

### Choosing an executor

**`SyncExecutor`** — the default. One loop on the calling thread, zero Composer
dependencies, no extension beyond `ext-ffi`, and it runs on the
non-thread-safe (NTS) build every distribution ships. A tasklet body that
performs blocking I/O is *correct*, not degraded: the body runs, the activation
completes, the next one is polled. Scale by replicas, and get per-process
failure isolation — a dead process drops one runner and the controller
re-places only its operators.

**`SwooleExecutor`** — opt in when a *single* operator must exceed one core.
An operator binds to exactly one runner, and an NTS PHP process runs one
thread of PHP, so a few large CPU-bound operators cap at one core each; this
is what lifts that. It needs:

- a thread-safe (**ZTS**) PHP build,
- a Swoole built against it with `--enable-swoole-thread` (the pecl default has
  no thread API at all),
- **`ffi.enable=true`**, not the shipped `preload`. `preload` permits FFI from
  the CLI SAPI, and a worker thread is a fresh interpreter that is not it, so
  every thread would be refused its first core call.

All three are checked by `Executors::swoole()` before the runner connects, and
the failure names what is missing and how to get it.
`docker/dev/Dockerfile.php --target zts` builds an image with all three.

The pool starts at one thread and grows on the core's own signal rather than a
guess: a supervisor blocks in `flz_core_wait_starvation` — which answers when
work is queued and no thread is inside `poll`, precisely a PHP thread sitting
inside a user's PDO call — and adds a thread per observation up to
`swoole_cpu_num()`.

**That growth rule is why the threaded arm is not a free win, and the numbers
say so.** Measured with `mix bench --runtime php`, arms interleaved on one
8-core host (PHP 8.4.25, `do_nothing`-class bodies of about 1 ms):

| shape | `SyncExecutor` | `SwooleExecutor` | ratio |
| --- | ---: | ---: | ---: |
| 20 operators × 200 (`regression`) | 97,752 msg/s | 83,815 / 70,921 msg/s | **0.86 / 0.73** |
| 1 operator × 6,000 (`dynamic`, 8 keys) | 32,026 msg/s | 35,143 msg/s | **1.10** |

The many-operator shape is *slower* under threads, and correctly so: those
operators already spread across runner processes, one loop keeps up, the pool
never starves and so never grows past one thread — leaving the supervisor's
wait as pure overhead. The single-operator shape is where the pool has
something to do, and it is the only one of the two that a `SyncExecutor` cannot
scale by adding processes.

Both numbers are bounded by a fixture whose body does almost no work, so they
measure the *seam*, not the ceiling: the wider the tasklet body, the more of
the host's cores a single operator can reach, which is the claim the executor
exists for. Take your own measurement with your own body before sizing on
either row.

### The entry script is re-entered

A `SwooleExecutor` worker bootstraps a **fresh interpreter from the runner's
own entry script**, rebuilds the PHP-side tasklet registry and attaches to
the existing context by its address — `flz_core_declare_class` is per context,
not per thread. Two consequences for the script you write:

- It must be safe to `require`. Anything with a side effect outside declaring
  and starting the runner happens once per thread.
- **It must carry no `#!` line.** A shebang is stripped only from the script
  PHP was invoked with; a worker `require`s the file, where the line becomes
  raw output before `<?php` — and `declare(strict_types=1)` is then no longer
  the first statement, which is a fatal error in every worker before it polls
  once. The pool reports a worker that ends before shutdown for exactly this
  reason, and replaces it.

`Runner::run()` takes the attachment the bootstrap left and serves against it;
nothing in your script changes between the two paths.

### Writing your own

Implement `Executor`. That is the whole contract, and it is the same seam an
event-loop adapter over amphp or ReactPHP would use — the SDK ships none,
because an event loop buys I/O density inside one process and no cores, which
more processes and now threads already buy.

### Swoole is optional, and named in one place

The package requires only `php` and `ext-ffi`; `ext-swoole` is a `suggest`. No
Swoole type is named outside the adapter's own files (`src/Executor/Swoole*.php`
and `src/Executor/swoole_worker.php`), and no shared code names
`SwooleExecutor` — `Executors::swoole()` is the one door. PHP autoloads per
class, so the adapter is inert on disk until someone calls it: no parse, no
memory, no error on an NTS build with no extension. `PackagingTest` enforces
both rules against the parsed source rather than the prose.

## Accepted limits

- **Under `SyncExecutor` one runner uses one core.** An NTS PHP process runs
  one thread of PHP code, so cores come from running more processes. That is
  fine at the thousands-of-small-operators scale this targets, and it is
  exactly the ceiling `SwooleExecutor` removes for a few large CPU-bound ones.
- **No event-loop executor**, in this change or a planned next one. Threads
  supersede one for the case that matters, and the seam takes a user-written
  adapter without an SDK change.
- **`SwooleExecutor` needs a ZTS build** most distributions do not ship. It is
  opt-in for that reason and never the default — and every suite still runs
  under it.
- **PHP decodes postcard in interpreted code**, where Python and Node bind
  natively and decode in Rust. If that measures as the dominant per-activation
  cost, the identified lever is a JSON activation-format flag on the ABI.
- **`PHP_INT_MAX` is 2^63−1 and the schema is full of `u64`.** A field whose
  value needs the 64th bit raises a decode error naming it, rather than
  silently degrading to a float: real offsets, millisecond timestamps and
  change seqs do not approach 2^63, and a value that does is corruption.
- **Crash-sharing.** `flz_core`'s threads live in this process's address space,
  so an FFI misuse takes the runner down with them. The SDK owns every FFI
  call; no raw pointer, `FFI\CData` or handle is exposed on the authoring
  surface. The blast radius is one runner.
- **Installing needs the release's binary.** An offline or plugin-less install
  supplies it by hand, as above.

## Fixtures

The manifest at `test/apps/php_runner/conformance.json` defers nothing: every
capability is carried with the reference's fixtures, the persistence, schema
evolution and effectively-once set in `test/apps/php_runner/fixtures_state.php`,
the bounded operator set in `fixtures_bounded.php` and `fixtures_bounded_loop.php`, and
the rest in `fixtures.php` and `fixtures_probe.php`.

## Diagnostics

`FLEZHA_LOG_LEVEL` (`debug` | `info` | `warn` | `error`, default `warn`)
selects what the default logger writes to standard error. A host that wants
PSR-3, spans or structured fields passes its own `Flezha\Logger` subclass to
`->logger(...)`; there is no logging dependency.

The runner observes shutdown three ways, because a harness, a user and the
controller each have their own: end of file on standard input, `SIGINT` /
`SIGTERM`, and the core's own stop — which every loop sees as "shutting down"
from its next poll. All three drain in-flight activations before the process
exits.

Signals need **`ext-pcntl`**, which ships with PHP and is enabled on most
distribution CLI builds but not in the official Docker images
(`docker-php-ext-install pcntl`). Without it the runner says so at startup and
SIGTERM ends the process immediately: nothing is lost — the core re-hands
unanswered work on the next bind — but in-flight activations are abandoned
rather than drained. Closing standard input drains either way.
