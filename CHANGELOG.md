# Changelog

## Unreleased

### Fixed — five development instruments that misreported or collided (`close-five-small-tooling-defects`)

Five small defects in the tooling, each reproduced before it was touched. The
Python coverage report had been measuring almost nothing of the package: the
`filterwarnings` entry in `pyproject.toml` imported `flezha` before coverage
started tracing, and moving the filter into `tests/conftest.py` takes the
package total from 61 % to 93 % — the filing had counted two modules, the
measurement says the whole package. Node's coverage table lists only `dist/`
now instead of the fixtures the suite writes under `/tmp`. One reader,
`scripts/manifest_pin.sh`, replaces the five inline patterns CI used to pull a
pinned version out of a manifest, and refuses a range. The test node is named
the way the release names its node — `FLEZHA_NODE_NAME@FLEZHA_NODE_HOST` —
with a base derived per run, so two suites on one box are both distributed.
The Python state guard's delegation inside the runtime window has a test.

### Fixed — `flz schedule run` is an authorized operative start, and the schedule flags travel as one options document (`gate-schedule-run-and-fold-its-flags`)

`flz schedule run <name>` fired an instance with no authorization under any
posture, and its request had nowhere to carry a token. It now takes `--token`
(or `FLEZHA_TOKEN`) and is judged as `operative.start` in the schedule's own
namespace before anything spawns. That makes it the one schedule verb not
judged in `_system`: an `operator@_system` token can add, list and remove a
schedule but cannot run one; a token bound to the schedule's namespace with
`operative.start`, or `admin@_system`, can.

- **One options document.** `flz schedule add`'s optional flags — `--namespace`,
  `--init_values`, `--kind`, `--plugins` — and the credential reach the
  controller as one keyword list instead of positional arguments with defaults,
  and an absent flag is absent from the request. The operator-facing flags do
  not change; `--plugins` arrives as the text the operator wrote and is stored
  verbatim.

### Fixed — a runner's self-report is counted once per runner (`count-a-runner-self-report-once`)

**The value of eight `:flezha_transport` metrics changes on any runner with more
than one secondary stream**: `credit_parked`, `mailbox_blocked`,
`mailbox_drops_newest`, `mailbox_drops_oldest`, `mailbox_pending_pushes`,
`restore_discarded_inputs`, `timers_armed` and `timers_fired`. A runner sends the
same heartbeat on every secondary stream it holds and the controller mirrors it
into every channel row, so each of these read the runner's value multiplied by
its stream-pool size and moved when the pool was resized. They now read the
runner's value once per runner, summed across runners; a per-channel counter
(`credit_active_sessions`, `credit_overrun_total`, `send_gate_held`, the
`channel_*` io counters) is unchanged.

- **One collapse, owned by the slot layout.** `Flezha.Transport.LivenessCounters.by_runner/1`
  collapses a runner's channel rows to one; the transport source and the
  convergence stall classifier both read it, and which slots are runner-global
  is derived from the layout rather than listed.

### Fixed — an Elixir Entity holds a change record the wire refused (`hold-a-failed-record-in-the-elixir-sdk`)

The Elixir SDK already held a plugin-bound output no stream would take and
replayed it on the next bound one. The Entity's change records were still lost
outright, on both arms — the write refused, and no stream bound at all — so
after a reconnect the controller could receive an output with no record of the
change that produced it, and nothing counted the loss.

- **A refused record is held and replayed** on the stream the rebind wires, in
  the order the records were written, so a `change_seq` is never inverted.
- **The hold reads `FLEZHA_RECONNECT_RING_CAPACITY`** (default 1024), drops the
  oldest past it, and counts the shed as `drops_oldest` — the same terms and the
  same knob the output hold uses. That counter now moves for records too.
- **A dispatch's records still reach the wire before its output frame**, on a
  replay as on a live stream: the Operative rebinds every Entity and waits before
  replaying its own outputs.
- **The credit window is unchanged**: a frame is debited when it is written, not
  when it is held, so a held record does not consume the window and a replay
  debits the session the reconnect armed rather than the one that failed.

### Changed — the Kafka producer's queue-overflow option joins the policy vocabulary (`rename-kafka-queue-overflow`)

**BREAKING for a producer plugin that configured `:queue_overflow` explicitly.**
`:block` is now `:drop_nothing`; `:drop_oldest` and `:drop_newest` are unchanged.

- **Rename, no behaviour change.** The option's three values answer the same
  question as `OverflowPolicy` — what does a full queue discard? — and now spell
  it the same way. `:drop_nothing` is already true of the mechanism: a full
  queue holds the record and makes room only from a drained batch.
- **No compatibility alias.** `--Xqueue_overflow=block` is refused with
  `Invalid value for queue_overflow: block`. A stored plugin row carrying the
  retired atom must be re-enabled with the new spelling; because `:block` was
  the default, a row carries it only if an operator set the value already in
  force.
- **The parking mechanism keeps its own names.** `:queue_block_timeout_ms`
  still bounds a synchronous `enqueue/4` caller's wait, and the worker's
  `blocked` queue and `:block_timeout` message are unchanged — they name the
  park, not the policy.
- **`scripts/check-overflow-naming.sh`** now catches this surface: it matched
  the policy list only when the retired name sat next to a sibling, which is
<!-- overflow-naming-record:begin -->
  why it passed over `queue_overflow: :block` for the whole of its life.
<!-- overflow-naming-record:end -->

### Added — Python SDK (`python-sdk`)

A sixth host for `flz_core`, over PyO3: a `python` feature on `flz_core_ffi`
beside `jni`, `napi` and `ruby`, not a crate of its own. `pip install flezha`
needs no Rust toolchain — an `abi3` wheel per platform targeting the 3.11 floor,
and per-version free-threaded wheels from `cp314t` (PyO3 refuses the
free-threaded build below 3.14). Both tiers build from one source with one API,
and the runner names the tier it is running in its startup line.

The authoring surface is decorators — `@flezha.procedure`, `@flezha.state`,
`@flezha.stream_operative`, `@flezha.workflow_operative` — over a handler that
is a plain function when it is a transform and a class when it has hooks. **Type
hints are the declaration**: the input, output, state and change types all come
from the annotations, and the package ships `py.typed` with generic `Edge[T]` /
`Procedure[I, O]`, so a wrong edge is a pyright error before it is a
registration refusal. That pairing is what Python has in place of Rust's
`tests/ui` compile-fail pins, and both halves are pinned — by a fixture that
must fail type-checking and by a refusal that names the handler and the defect.

Durable `State` and `Change` serialize as MessagePack from author-declared
dataclasses, Pydantic models or attrs classes — **never `pickle`**, whose
loading executes code and whose payloads embed class paths; its absence is
asserted over the loaded source. A class declared `@flezha.state` rejects direct
attribute assignment at runtime, naming the field and `ctx.change`, which is the
enforcement level the authoring contract now records for Python.

Concurrency is the Rust host's model, because CPython's situation is the Rust
host's: a base pool of threads each looping `poll → dispatch → complete` on
itself, plus a supervisor parked in the starvation wait, reusing
`FLEZHA_WORKER_POOL_SIZE` / `FLEZHA_WORKER_POOL_MAX`. **Every blocking core call
releases the GIL**, so the lock bounds only handler bodies — and the
free-threaded tier lifts even that. An opt-in asyncio executor
(`FLEZHA_PYTHON_EXECUTOR=asyncio`) hands each activation to one shared loop,
fetching on real threads and completing on the loop thread; the suite runs under
both and the ordering scenarios are identical. Under either executor the
supervisor's answer to starvation is one more fetch-and-run **thread**, because
a stalled loop cannot rescue itself. The SDK spawns no processes and refuses any
core call after a fork, naming the rule.

`test/apps/python_runner/` claims eight of the nine capability families every
other veneer SDK defers — the persistence family, the effectively-once tier,
timers, correction windows, keyed channels, dynamic-entity eviction, entry idle
and schema evolution — each proved by the live suite over UDS and QUIC.
Claiming `keyed_channels` required minting the `flz.entity` / `flz.seq` /
`flz.key` identity stamps in the SDK, which Java does and Node, Ruby and PHP do
not. `workflow` is deferred by name to `sdk-durable-fixture-parity`.
`mix bench.roundtrip --runtime python` lands with the change.

`examples/python/durable_agent/` is the worked example: a durable agent loop
with a model call, a retry ladder, a compensation and a suspending workflow
session.

### Added — PHP SDK (`add-php-sdk`)

A fifth host for `flz_core`, and the first to consume the C ABI **as a C ABI**:
`ext-ffi`, no compiled veneer. PHP has no stable ABI across minor versions, so a
native extension would be a PHP-version × thread-safety × platform matrix that
grows with every release; `ext-ffi` needs one library per platform — the five
that already ship for Java.

`composer require flezha/sdk` pulls nothing: `php >= 8.4` and `ext-ffi`, and
that is the whole requirement. The authoring surface is a `#[Proc]` attribute
for the scalar declarations and a fluent `channels()` builder for the graph,
with the optional hooks read from the methods a procedure **overrides** so a
declaration and its implementation cannot disagree. Durable `State` and
`Change` serialize as JSON through author-declared `toArray()` / `fromArray()`
— never PHP's `serialize()`, whose embedded class names and mangled property
names turn a class rename into a broken restore.

Two executors ship behind one seam that carries **loop multiplicity, not work
distribution** (a `Swoole\Thread` cannot receive a closure, so `submit(callable)`
has exactly one possible implementation). `SyncExecutor` is the default: one
loop on the calling thread, on the non-thread-safe build every distribution
ships. `SwooleExecutor` runs the same loop on a pool of threads over one shared
context, growing on the core's own `flz_core_wait_starvation` signal, so a
single operative can exceed one core. Which one is an axis of the *run*
(`FLEZHA_PHP_EXECUTOR`), never a property of a file: no test, fixture or
manifest entry exists in a per-executor variant.

`SDK_LANG_PHP = 6` and a JSON change-format id join the allocation table in
`flz_core/src/header.rs`; `FORMAT_ID_KRYO = 4` is recorded there too, so the
table of record is what makes a new id provably free. The one Rust behaviour
change is none — this is a constant, a test and a consumer.

Measured, not assumed: `ffi.enable=preload` (PHP's default) permits FFI from the
CLI SAPI, and a `Swoole\Thread` is a fresh interpreter that is not it, so the
threaded executor requires `ffi.enable=true` and says so before the runner
connects.

### Changed — `DropNothing` withholds work instead of holding a host thread (`nonblocking-drop-nothing-backpressure`)

Under `DropNothing`, a push into a full consumer mailbox parked the **calling
host thread** in a bounded retry loop. Room only appears when somebody runs the
consumer's round, so if the parked thread was the only one that could have run
it, the process stopped. The typed Rust host escaped through
`routing::install_inline_serve` — a Rust `fn` pointer, so it could not cross
the C ABI and no FFI host ever had it. Measured with the hook claimed by a
no-op, exactly the state a C-ABI host leaves it in: **0 of 20 payloads
delivered, wedged 15 s** on a one-worker host.

A push a producing Entity cannot deliver is now appended to that Entity's
**push holdback**, its handout gates stay held until the holdback drains, and
the producing thread returns immediately — the state
`PollCore::retry_after_failure` already put an entity in during a backoff
window, re-armed by the consumer's own wait list. Ordering is preserved by
construction: a gated Entity is handed no further activation, and a push made
while its holdback is non-empty appends rather than attempting. While an Entity
owes a push the flusher holds its durable records, so a consumed offset can
never become durable ahead of an edge output derived from it.

`INLINE_SERVE`, `install_inline_serve`, `serve_inline`, `ACTIVATION_DEPTH`,
`in_activation` and `poll_pool::serve_slot` are deleted, and with them three
latent defects the hook carried: an inline serve called `try_assemble` directly
and so bypassed `FLEZHA_MAX_INFLIGHT_ACTIVATIONS`; it re-entered an activation
on the producing thread with no depth bound, so a chain of full mailboxes
nested; and it ran a consumer's completion, durable record included, inside the
producer's still-outstanding activation.

Reaches Rust and Java below the seam (both complete through `flz_core`). The
Elixir SDK needs no mechanism change and that is a decision, not a gap — it
already defers the caller's reply into a `waiters` FIFO — but it takes the
reporting half, because heartbeat keys are a cross-SDK contract. The data
heartbeat gains `pending_pushes` and `pending_push_oldest_ms` on all three
SDKs; a gated producer holds no handout, so `inflight` is blind to it.

**Behind `FLEZHA_DROP_NOTHING_NONPARKING`, default off** — a rollout switch,
not a fourth policy: nothing an author declared changes either way. With the
flag off the producer parks as before, minus the inline serve, and the
starvation supervisor is the rescue. The entry-path transport reader keeps
parking in both: it has no producing Entity, and its stalling is what carries
ingress backpressure to the controller.

**In plain terms, for whoever meets this at 3am.** A `DropNothing` channel
discards nothing, so a push into a full consumer mailbox has to wait — this
change moves *what* waits. Flag off (the default): the pushing thread parks
until room appears, and if that thread was the only one that could have run the
consumer, nothing ever frees the slot. Flag on: the value goes onto a
per-producer holdback queue, the thread returns immediately, and the producing
Entity is handed no further work until the queue drains. Nothing is discarded
and order is preserved either way. **The default is off; nothing changes until
someone sets it.** Turn it on where a thread cannot be spared — a PHP
`SyncExecutor`, a Node host on the event loop, where the park is not a risk but
a guaranteed freeze and no supervisor can rescue a runtime with no spare thread
to buy. Do not turn it on for speed.

**The mechanism buys the freed thread and costs throughput under saturation.**
Measured (`hybrid_channel_throughput`, 5 000 messages per iteration): at the
default mailbox capacity the flag costs nothing (7.19 ms with it on against
7.36 ms off, p = 0.46), but at `FLEZHA_MAILBOX_CAPACITY=4` — a mailbox
saturated continuously — it is **30–36% slower than the park** (25.78 ms
against 18.92 ms for one consumer, 59.75 ms against 46.10 ms for three). The
proposal predicted the opposite; its figure was measured single-threaded and
excluded what dominates here, that the re-arm is a scheduling round-trip where
the park was a futex wake straight back into the emit. This is why the flag
ships **off** and why flipping its default is gated on three things, all named
recorded separately: a ≥ 2 h soak with
it on against a workload that actually saturates a consumer, the Java
round-trip bench run under both settings (**owed** — not run in this change, so
the numbers above prove one runtime only), and that 30–36% cost either closed
or accepted in writing.

Two windows this does **not** close, both named in
`docs/resilience/data_durability.md`: the consumer's volatile mailbox between a
landed push and the consumer's own durable record, and the controller's send
buffer, which still discards the newest frame when a runner stops reading.

### Fixed — a due wake no longer wedges a poll host (`close-overflow-silent-failures`)

`PollCore::promote_due` runs *inside* `poll` and pushed a due timer or an
interval close under `DropNothing`. With a full mailbox and no inline-serve
hook — the situation of **every** C-ABI host — the poller parked there. Because
`pollers_inside` is held across the whole call and `starving()` answers `false`
while it is non-zero, that poller read to the starvation wait as a thread about
to take work: the runner stopped and hid that it had. Measured at 1 of 3
firings delivered, wedged 15 s, with the pool ceiling free to grow.

A wake that cannot be admitted is now re-recorded at a later due time and the
pass continues — the wake list itself is the holdback — so the condition is
both impossible and *reportable*, since a due entry is starvation with no
dwell. A deferred firing keeps the timestamp it was armed with. No mechanism
was added and `routing::in_activation` leaves `poll_core.rs`.

### Added — the Java SDK brings a starvation supervisor (`close-overflow-silent-failures`)

`flz_core_wait_starvation` existed in the ABI and nothing in `sdk/java`
referenced it, so a Java runner whose fetchers were all held processed nothing,
recovered from nothing and reported nothing — measured at 0 of 20 payloads,
forever. `FlzCore.waitStarvation` is now bound and `PollHost` parks one
platform supervisor thread in it, starting one more fetcher per observation up
to `FLEZHA_WORKER_POOL_MAX` (default 10× `FLEZHA_WORKER_POOL_SIZE`, never below
it — the same shape and spelling the Rust pool uses). Every growth logs at
`INFO`; the first refusal at the ceiling logs at `WARN` and latches.

This does **not** cover the conformance `pool_wedge` condition: no ceiling
outruns user code that never returns.

### Fixed — an edge output's offset can no longer become durable ahead of the output (`close-overflow-silent-failures`)

**FFI hosts only** (Java and every proposed FFI SDK); the typed Rust path emits
during `run()` and was already correct. `resolve_item` folded the invocation's
changes, plugin outputs *and consumed offset* into the effect record and only
then routed the edge outputs. `OutputTarget::Edge` is excluded from the
transactional outbox, so a crash inside the flusher's 5 ms linger left the
offset durable, the input skipped on replay, and the consumer never fed — a
permanent loss while the tier reported effectively-once. The two blocks are now
in the other order: an item with an undelivered edge output is unresolved, and
an unresolved item contributes nothing durable. A fan-out holds its record
until every consumer's push has landed.

### Added — every overflow outcome is observable (`close-overflow-silent-failures`)

`drops_newest_total`, `drops_oldest_total` and `pushes_blocked_total` were
written by the kernel and read by nothing: `EntitySlot::counters()` had zero
production callers, so no heartbeat, export or log ever carried a drop.

- **Three counters ride the data heartbeat.** Process-wide aggregates beside
  `dispatches` and `credit_parked`, mirrored by the controller into
  `hb_blocked_parks` / `hb_drops_newest` / `hb_drops_oldest`. The Elixir SDK
  reports the same keys from its own counters.
- **`blocked_parks` now counts wait iterations, not episodes.** A runner held
  in one long block episode showed a frozen dispatch count beside a frozen
  block count — byte for byte the picture a wedged runner shows.
  `runner_pool_wedged` reads the pair: dispatches frozen with blocks climbing
  is backpressure, dispatches frozen with blocks flat is a wedge.
- **`DropOldest` counts its admission.** A force-push that admits now bumps
  `pushes_total` (it is a successful enqueue by that counter's own definition;
  `processed` used to exceed `admitted` for this policy) and the evicted item's
  identity is named in a `flezha::mailbox` debug line instead of discarded.

### Changed — a dropping policy is refused where its loss is unaccountable (`close-overflow-silent-failures`)

**BREAKING for a class that declared both `effectively_once` and a dropping
overflow policy** — a combination that was silently lossy and is now a
declaration error naming the channel and both declarations. Refused in the Rust
and Java builders and again in `flz_core`'s declaration validation, so a
hand-written host declaration cannot slip past. A correction window requires
the tier, so it is covered by the same rule. Shedding stays fully available to
a class that promises neither.

Independently of the declared policy, a **replay-slice member always takes the
`DropNothing` path**: a dropped member left the slice open and short, the slice
still closed on the end-of-slice broadcast, and the correction folded whatever
arrived — a wrong answer with nothing reported.

### Changed — the overflow policy names its guarantee (`rename-block-to-drop-nothing`)

**BREAKING for every operative that declares an overflow policy.** The policy
formerly spelled `Block` is now `DropNothing` — searching for `Block`,
`BLOCK`, `"block"` or `:block` on the overflow surface lands here.

- **Rename, no behaviour change.** `OverflowPolicy` now answers one question
  three times — *what does a full mailbox discard?* — instead of answering it
  twice and describing the producer's thread once. `DropNothing` is already
  true of the shipped mechanism, which retries until the push lands and
  discards nothing.
- **Every surface moves together**: Rust `OverflowPolicy::DropNothing`, Java
  `OverflowPolicy.DROP_NOTHING`, Elixir `:drop_nothing`, the `HostClassDecl`
  wire literal `"drop_nothing"`, and `on_full = "drop_nothing"` /
  `onFull = "drop_nothing"` in the procedure attribute. An unrecognised
  spelling is refused at declaration; there is no compatibility alias.
- **The parking mechanism keeps its own names.** `block_parker`,
  `BLOCK_PARK_TIMEOUT` and `pushes_blocked_total` name the entry-side park,
  which still exists and is honestly named. So do the Kafka producer's
  `:queue_overflow`, the observability bus's per-sink `overflow:` and the
  transport write buffer — separate mechanisms, unchanged.
- **`scripts/check-overflow-naming.sh`** fails a build that reintroduces the
  retired spelling on the policy surface, and runs in the Naming CI job.

### Changed — nothing drops by default (`close-silent-data-loss-defaults`)

**BREAKING for operatives and producer chains that ran above capacity.** Two
defaults invert from drop to block, so a saturated pipeline now back-pressures
its source (visibly slower ingest) instead of silently discarding records with
every health surface green.

- **Entry channels default to `DropNothing`** in both SDKs (`OverflowPolicy`). An
  operative that wants to shed under saturation declares it —
  `.on_full(OverflowPolicy::DropNewest)` / `.onFull(OverflowPolicy.DROP_NEWEST)`
  — and the loss is a stated intent at the declaration site. The cost: a
  blocked entry parks the shared wire reader, so siblings on the same
  connection wait with it. Inter-procedure channels already blocked.
- **The Kafka producer defaults to `:queue_overflow = :drop_nothing`** for every
  record, not only delivery-guaranteed ones. `:drop_oldest` / `:drop_newest`
  remain available and must be chosen. `Producer.stats/1` now reports
  `dropped_total`, so a chosen drop is readable synchronously, not only via
  telemetry.
- **A cold-takeover baseline pull retries** on the resync interval
  (`:wal_resync_retry_interval_ms`, default 1 s) instead of latching: a pull
  that failed to dispatch no longer strands the Entity in permanent shed.
- **Removed** the producer's inert `:partitioner` option and the documented
  but unimplemented `kafka_msg_partition` metadata override. One plugin
  instance serves one `(topic, partition)`.

### Changed — a production controller must choose a durability posture (`durability-tier-default-on`)

**BREAKING for operators.** `FLEZHA_DURABILITY_BACKEND` no longer defaults to
`none` in production. A prod release booting with it unset refuses to start and
prints the two valid moves; dev and test are unchanged.

- **Set one line before rolling the new image.**
  `FLEZHA_DURABILITY_BACKEND=postgres` (or `s3`, or `local_path`) turns the
  remote durability tier on; `FLEZHA_DURABILITY_BACKEND=none` is the explicit
  local-only opt-out and reproduces the previous behaviour exactly — no tier
  processes, no backend connection, no change on any ack path. Rollback stays
  lossless: local state was authoritative throughout.
- **Local-only is visible.** `/healthz` carries `"durability":"local_only"` on
  every response and `flz controller status` prints
  `durability_posture: local_only`, so a fleet audit can find the controllers
  whose durable state has exactly one copy. Neither is an alarm.
- **The tier now ships the platform stores too.** A controller that dies with
  its disk used to lose its parked failures and its security state even with the
  tier on. Dead-letter parked records ship as one claim-fenced artifact per
  procedure — on every park, drop, replay-mark and retention drop, so a resolved
  entry disappears from the backend rather than being resurrected — and hydrate
  with the operative that owns them. The control-plane stores (authz roles,
  tokens, namespaces, quota counters and the durable enabled flag; the schedule
  definitions) ship on the backstop sweep as per-writer replicas and hydrate
  **before** any hub starts or any connection is accepted, so a resurrected
  controller enforces from its first verb and the one-shot bootstrap claim reads
  already-claimed.
- **`bootstrap.token` never leaves the node.** The shipped file list is explicit
  rather than a directory scan: the one-shot plaintext secret beside the authz
  DETS is excluded by construction, along with the break-glass `REBOOTSTRAP`
  marker and the TLS material.
- **Every durable store is now accounted for.** `docs/operations/remote_durability_tier.md`
  carries the shipped set and the accepted-exclusions table (the audit log,
  delegated to external log shipping, plus the four non-stores above). A store
  on neither list is a defect.

### Changed — declared intent lives in the hub's declared set (`durable-operator-intent`)

- **Every operative start is declared, every stop is remembered for good.**
  Starts pass `durable: true` into ProcessHub's declared-children set; stops
  and workflow completions ride the hub stop path, which removes the entry
  before the process terminates. A whole-cluster cold start restores the
  declared set (spawned workflow instances included), and a deliberately
  stopped operative can no longer be resurrected by a node returning after an
  arbitrary absence.
- **Removed operator knob:** `FLEZHA_PROCESS_HUB_STOPPED_ROW_TTL_MS` (and
  `:process_hub_stopped_row_ttl_ms`). Stop knowledge is declared-set absence
  and never expires, so there is no 30-day tombstone window left to outwait.
  A deployment still setting the variable keeps booting; the value is
  simply no longer read.
- **Off-cluster intent copy.** With the durability tier enabled, the declared
  set ships through it (`remote_manifest` over `Flezha.State.DurabilityTier`),
  so intent survives the loss of every cluster disk. New tier backend
  `:local_path` (file storage, e.g. an NFS mount) joins `:postgres` and
  `:s3`; any `{module, opts}` implementing the backend behaviour remains the
  extension point.
- **Reroute is placement-consistent and atomic.** A disconnected runner's
  operatives are rerouted per-operative through the factory-filtered HRW
  placement, and the registry binding switches only on a confirmed migrate —
  never to a random runner, never on a discarded result.
- **Cross-node re-placement.** An operative on a node with no runner
  advertising its factory is moved — state intact, consent-gated — onto a
  node whose fleet has one, instead of retrying a doomed local rebind forever.

### Changed — authentication and authorization finished end to end, enforced by default (`finish-platform-authn-authz`)

**BREAKING for operators.** The trusting-LAN default is gone; every transport
surface authenticates its peer and the policy point enforces by default.

- **Transport authentication.** The QUIC server verifies a client certificate
  against a configured trust anchor (`FLEZHA_QUIC_CLIENT_CA`, default the
  generated `app_data/certs/ca.pem`; the cert generator now also mints a
  `client.pem`/`client.key` identity so single-host deployments work out of
  the box). The UDS control socket and its directory are owner-only. The
  release no longer ships a constant `RELEASE_COOKIE` — a node without a
  provisioned cookie refuses to start distribution — and TLS distribution is
  supported via `FLEZHA_DIST_TLS_OPTFILE`. `FLEZHA_TRANSPORT_AUTH=off` is the
  explicit wide-open opt-out, warned on every boot.
- **Enforcement by default.** `authorize/3` has three postures: `:open` (the
  explicit `FLEZHA_AUTHZ_ENABLED=false` opt-out, warned every boot),
  `:bootstrap_pending` (configured on, unclaimed — operator verbs refuse
  naming `flz auth bootstrap` while dataflow and runner reconnects continue),
  and `:enforced`. Destructive verbs (`state delete/rotate`, `dlq
  replay/drop`, `operative stop`, `controller stop`) and every plugin
  mutation are gated and audited.
- **Principals from the transport only.** CLI identity is a verified token
  (`flz --token` / `FLEZHA_TOKEN` → `caller_context.principal`); runner
  identity is `runner@<namespace>` minted from the connect handshake;
  caller-supplied principals are ignored.
- **Namespace isolation through the State subsystem.** Durable state lives
  under `app_data/state/<namespace>/…`, resolved via a class→namespace claim
  recorded at bind (conflicts refused; `_system`-migrated classes adopted).
  `Flezha.Boot.StatePathMigrator` lifts the flat layout under `_system/` once,
  stamped by a `.layout` marker. Listings and state verbs authorize against
  the resolved target namespace.
- **Remaining quota points.** `max_runners` enforced at runner connect
  (refusal observable), `max_state_bytes` (renamed from
  `max_disk_bytes_total`) accounted from the state tree, rebuilt on boot,
  enforced at operative allocation, with a latched Watchdog alarm — never a
  write-path kill — on breach of a running namespace; per-namespace
  `max_mailbox_bytes_total` caps the credit window.
- **CLI.** New `flz auth` (`bootstrap`, `whoami`, `rebootstrap`), `flz token`
  (`issue`, `revoke`, `list`), `flz role` (`bind`, `unbind`, `list`) families;
  `flz namespace` gains `create`, `delete`, `show`, `set-quota`. Break-glass:
  offline `flz auth rebootstrap` invalidates a lost `admin@_system` and
  re-enters the bootstrap flow on the next boot, audited.
- **Credential hygiene.** Kafka SASL credentials ride a closure into the
  client start (Postgres pattern): the returned/persisted opts are redacted,
  `sasl_password_env` resolves the secret from the environment at client
  start.

Operator runbook (cookie, certs, k8s manifests, break-glass):
`docs/operations/authn_authz.md`. Migration: provision the cookie + certs,
boot, claim the bootstrap, issue tokens; rollback is
`FLEZHA_AUTHZ_ENABLED=false` + the previous release.

### Added — tiered procedure authoring: defaults you can always override but never must (`add-tiered-procedure-authoring`)

Procedure authoring had two tiers with a gap between them. The sugar
(`#[flezha::procedure]` / `@Procedure`) inferred everything from the `run`
signature but could not express multi-emit, so the most common real procedures —
an OHLC aggregator closing one candle and opening the next, an SMA emitting per
window — fell off the sugar cliff straight into full ceremony. Three changes
close it, each tier a strict superset of the one above and explicit always
winning:

**Sugar covers multi-emit.** A sugared `run` may take a trailing procedure
context and return unit; the body then performs any number of emits, and
`emits = Type` (Rust) / `emits = Type.class` (Java) declares the output type the
return value no longer can. The expansion stays exactly the code a hand-written
implementation would contain. A value return alongside the context parameter, a
`key = …` alongside it (the body keys its own emits), and an `emits` without it
are each refused by name rather than silently accepted.

**Java stops charging for erasure.** `inputType()`, `outputType()`,
`changeType()`, `clockType()` and `configType()` now default to the type
arguments read off the implementing class's generic supertype chain — a direct
`implements Procedure<…>`, or a concrete subclass of a generic intermediate
class. `kryoTypes()` defaults to the resolved Change and State. That is ~24
lines of pure type-erasure tax removed from every hand-written Java procedure.
An argument still generic at the concrete class is **never guessed**:
`defineStream` / `defineWorkflow` fails naming the class and the parameter, and
nothing is registered — a startup error, not a dispatch error far from its cause.

**Rust gets a simplified tier.** `SimpleProcedure` declares only `Input`,
`Output`, `State` plus `name` / `init_state` / `run`; a blanket impl lifts it to
`Procedure` with `Clock = WallClock`, `Config = ()` and the state's derived
change type. Implementing `Procedure` directly stays fully supported and is the
way to declare anything non-default; a type cannot sit in both tiers, because
the impls overlap and the compiler refuses it.

Authoring surface only — no runtime behaviour, wire format or journaling change,
and every existing declaration keeps its exact meaning.

### Added — `route_latest`, the latest-document router building block (`add-route-latest-building-block`)

`c.route_latest(documents, traffic, key_fn)` (Rust) /
`c.routeLatest(Doc.class, documents, traffic, keyFn)` (Java) turns an
operative-wide document stream into per-key deliveries: every key seen on the
traffic stream receives each distinct document exactly once, on that key's own
next traffic item. Rust returns a stream of `(key, document)` pairs, Java a
stream of `RoutedDocument<D>`; the consumer keys by that key.

It exists because a keyed procedure cannot receive a broadcast — every keying
declaration is total, and inside a partitioned member the computed key is
admitted against that member's key-group range — so this shape was being
re-derived by every application that hit the wall. The subtle part is the one
now encoded in the SDK: the router delivers on the key's **own traffic**, never
by fanning out over a remembered key set, because a remembered set outlives
key-group ownership (a rescale, or one out-of-range item seen during a reconcile
window, would make later deliveries park against the partition guard).
Deliver-on-own-traffic makes over-delivery impossible by construction.

The price, documented and deliberate: **one tick of latency per key** — a key
learns of a new document on its next traffic item, and a key whose traffic has
stopped is never delivered to. A full broadcast primitive (retained documents,
delivery to late-born Entities, rescale-time replay) was considered and
rejected: it would add machinery to exactly the recovery paths with the worst
incident history, and the deliver-on-own-traffic construction covers those cases
without it.

Sugar, not a runtime feature: the block expands to a merge into one packaged
single-entity procedure whose `{document, version, delivered}` state rides the
standard change journal and passes the replay-determinism harness. No new
runtime semantics, no wire change, no new replay path. A document equal to the
retained one is inert (so a document source's re-poll after a reconnect is
silent downstream); a differing one bumps the version and every key is owed one
delivery again. The document is never parsed — parsing stays with the consumer.

Java-side notes: the document `Class` token is required because the router
retains the document in journaled state and only the wiring knows its type; the
Kryo state codec is now built from the procedure *descriptor*'s type list (which
starts as a copy of `kryoTypes()`, so nothing else changes) rather than from a
freshly instantiated Procedure, which is what lets the wiring register that
type.

### BREAKING — one plugin-sink method: `sink` + `sealed` (`unify-plugin-sink-api`)

`also_output(name)` / `also_output_with(name, f)` and the terminal
`output(name)` collapse into ONE method: `sink(name)` / `sink_with(name, f)`,
always returning the stream handle. A channel is the plugin-bound terminal by
having no consumers wired after it, not by which method declared it — the
runtime derives that from the wiring, which is what the two spellings were
encoding by hand. `.sealed()` is the optional terminator, returning the
non-chainable `Channel<T>` the old `output` returned; it is purely a
compile-time statement that nothing follows and changes no topology.

No shim, no coexistence. Migration is mechanical: `also_output_with` →
`sink_with`, `also_output` → `sink`, `output(name)` → `sink(name)` (append
`.sealed()` where the non-chainable return type was relied on). Wire frames,
descriptor field names, plugin bindings and error identifiers are unchanged; a
terminal declaration's frames are byte-identical to what `output` wrote, and a
unit fixture holds both shapes against those literals.

Consequences worth knowing: `ChannelDescriptor` lost `plugin_name` (dead) and
`plugin_serializer` — `plugin_sink` is now the one representation for both
shapes. A sink declared on a channel that a *later* statement also wires a
consumer into is now correctly hybrid; before, the terminal `output` had
already frozen the kind and those consumers silently received nothing.
`.on_full(...)` after `sink(...)` compiles now (it did not after `output`) and
is still never consulted on a channel with no mailbox — `.sealed()` refuses it
as before.

### Fixed

- **The remote durability tier is now correct with more than one controller**
  (`fix-durability-gc-multi-writer`). Two controllers sharing one backend is the
  configuration the tier exists to serve, and four defects made it unsafe there.
  All were latent — `FLEZHA_DURABILITY_BACKEND` defaults to `none` — but every
  one becomes a live data-loss path the moment the backend is enabled.

  - **Orphan collection deleted a peer's in-flight upload.** Every node ran its
    own hourly pass that listed the whole store, and the tier is manifest-last
    by design, so "uploaded but not yet referenced" — a state every ship passes
    through — was indistinguishable from an orphan. The victim then wrote a
    manifest naming a deleted blob, never re-uploaded it (`from =
    shipped_through + 1`), and at recovery the Entity came back **empty** while
    hydration returned `:ok`. Collection is now scoped to the references a node
    holds local state for AND refuses anything younger than `gc_grace_ms`
    (default 1 h); artifacts of a reference no node holds any more are still
    reclaimed past `gc_abandoned_after_ms` (default 7 d), so scoping cannot
    leak. Neither property alone is the safety argument.
  - **Blob names were position-addressed, so two writers overwrote each other's
    bytes.** An ordinary migration leaves the source node holding a full copy
    (`pull_operative/3` copies; only `move_entity/3` deletes), so two nodes
    legitimately held one reference at the same WAL epoch and wrote
    `r-<epoch>-<from>-<to>` with different records. Names now carry the content
    hash, as the registry cspec already did.
  - **The WAL epoch could never fence two writers.** It lives in the
    `.outbox.dedup` side-file, which `WalTransfer` copies and `Hydrator`
    restores verbatim, so both nodes held the same value and equal epochs
    overwrite. Manifest writes are now conditional on a **claim counter** that
    exists only in the remote store; bidding for a lineage requires the node to
    have actually adopted the operative, and losing one is reported as a routine
    handover rather than a permanent fence — the old latch would have left an
    RPO breach and a `degraded` `/healthz` after every migration.
  - **The registry cspec was a cluster-shared key at epoch 0.** `hub_id` is
    namespace-derived, so every controller shipped its own replica to one key
    with no fencing and last writer won — a node with a stale registry could
    erase the record of which operatives exist. The key now carries the writer;
    hydration picks the newest replica.
  - **`commit_sync/1` conflated operative id and class**, so a class declaring
    RPO 0 could have its barrier answer `:ok` having shipped neither the Entity
    WAL nor the input log, and `sync_class?/1` never armed the gate at all.

  Also: a manifest naming an artifact the backend does not hold is now an
  alarm-grade `[:flezha, :state, :artifact_missing]` rather than a silent cold
  start, and the durability telemetry family reaches the observability bus for
  the first time — including `rpo_exceeded`, which the docs already called
  alarm-grade.

  **Breaking for an existing store**: blob key layout and the `put_if/4` backend
  contract both change. Free today because the tier is enabled nowhere; if you
  had enabled it, wipe the bucket or table — local state is authoritative, so
  nothing is lost. New knobs: `FLEZHA_DURABILITY_GC_GRACE_MS`,
  `FLEZHA_DURABILITY_GC_ABANDONED_AFTER_MS`, `FLEZHA_DURABILITY_WRITER_ID` (set
  this under a Deployment, where the pod name changes on every reschedule).

### Breaking

<!-- kind-naming-record:begin -->
- **The two operative kinds are now `stream` and `workflow`**
  (`rename-operative-kinds-to-stream-and-workflow`). They were `unbounded` and
  `bounded` — borrowed from stream-processing vocabulary for a property of the *data*, applied to what is
  actually an *execution model* distinction, and outright false for a durable
  `PerInput` operative, which consumes an unbounded input stream forever and
  bounds only its iteration. Nothing about execution, durability or the wire
  tag numbers moves; only names.

  What changes for a caller: the kind atoms (`:unbounded`/`:bounded` →
  `:stream`/`:workflow`) and their wire literals — `""` still means the
  `stream` default, and `"unbounded"`/`"bounded"` are now rejected as unknown
  with **no compatibility mapping**; the SDK author surfaces
  (`UnboundedOperative`/`BoundedOperative` → `StreamOperative`/
  `WorkflowOperative` in both SDKs, with the whole surrounding family, and
  `define_unbounded`/`define_bounded` → `define_stream`/`define_workflow`,
  `spawn_bounded` → `spawn_workflow`); the protobuf messages `BoundedCompleted`,
  `BoundedStop`, `SpawnBoundedRequest`, `SpawnBoundedResponse` and their oneof
  fields; `flz operative start --kind <stream|workflow>` (`--kind bounded` is
  refused); and the env vars `FLEZHA_BOUNDED_{DRIVER_STACK_KIB,PARK_IDLE_MS,
  RETENTION_HOURS,RETRY_FAILS}` → `FLEZHA_WORKFLOW_*`.

  **Upgrading requires `flz controller stop && flz reset --yes`** — the kind
  atom is persisted in the ProcessHub DETS cspec registry and in the class
  registry's on-disk sidecar, and no code reads the retired atoms. Controller
  and every runner redeploy together; there is no forward-compatible
  intermediate state, so rollback is symmetric and equally requires a reset.
  Deployment config referencing `FLEZHA_BOUNDED_*` — including the
  `flezha_market` ConfigMaps, which live in a separate repo — must be renamed
  before its next deploy.

  The words survive where they are true. `bounded` and `unbounded` remain
  correct English for describing a *run* (a workflow operative runs a bounded
  iteration; a stream operative folds an unbounded input) and for unrelated
  capacity concepts such as the bounded mailbox subsystem, which is untouched.
  `scripts/kind-naming-allowlist.txt` enumerates every preserved occurrence and
  `scripts/check-kind-naming.sh` — wired into CI as the Naming gate — fails a
  kind-sense reintroduction.
<!-- kind-naming-record:end -->

### Measured

- **How many workflow instances a runner holds, answered with a number**
  (`workflow-driver-as-state`). One OS driver thread per actively-running
  workflow instance was the last thread-per-instance cost in the SDKs, and the
  proposal to delete it — turning the chain's position from a call stack into
  stored state — was gated on measuring it first. It was measured, and the
  verdict is **no-go**: the thread is ~1.4 KiB of a workflow instance's ~73 KiB
  resident cost (the rest is heap), 10 000 blocked instances cost ~734–884 MiB
  and start in 0.65 s, and a driver blocked at a stage barrier — the agent-turn
  shape the proposal was named for — costs 0.5 % of one core at that
  population. Cutting the driver stack from 2 MiB to 256 KiB moves no resident
  byte, because a blocked driver's stack was never resident, so that stopgap is
  not adopted either. What the measurement *did* find is now documented in
  `docs/resilience/workflow_operative.md`: interference tracks total OS thread
  count rather than instance count (an agent turn holds two threads in Rust,
  one in Java, three if the class declares `max_duration`), and the CPU a
  blocked population burns comes entirely from three waits that re-check every
  200 ms — the inbox wait, the commit-ack wait and the duration watcher — not
  from the thread model. Kept: `sdk/rust/tests/workflow_scaling.rs`, an
  `#[ignore]`d instance-count scaling probe, and
  `FLEZHA_WORKFLOW_DRIVER_STACK_KIB`, unset by default and an address-space
  lever only. No behaviour change.

### Changed

- **A keyed channel's plain emit carries the Entity's own key**
  (`default-emit-to-entity-key`). In a keyed Entity — the OHLC entity for
  `BTC/USD`, the SMA entity for `BTC/USD` — the natural key of essentially
  every output *is* the key the input routed by, and the SDK already holds
  it. `ctx.emit(value)` / `emit_with_meta` on a `.keyed()` channel (Java:
  `emit` / `emitWithMeta`) now stamp `flz.key` with that key instead of
  failing with `KeyRequiredOnKeyedChannel`, so the stamped key equals the
  routing key by construction and cannot drift from it. A drifted key was
  never a local error: it ships as the record key and, inside a partitioned
  group, hashes into an arbitrary key-group and parks at the downstream
  partition guard. `emit_keyed(key, value)` is unchanged and remains the way
  to emit under a key that differs from the Entity's; both spellings share
  one stamping path. Both SDKs also expose the key directly
  (`ctx.entity_key()` / `ctx.entityKey()`). Unchanged refusals: a
  single-entity Procedure's key-less emit on a keyed channel (there is no
  Entity key to default to) and `emit_keyed` on a channel not declared
  `.keyed()`. Backwards compatible — every currently-legal call keeps its
  behaviour; one error case became the safe default.

- **Operator start and enable verbs are idempotent**
  (`idempotent-start-and-enable`). `flz operative start` against a live
  instance now exits 0 as an explicit no-op (it previously exited non-zero
  with "operative id already in use"); a claim refused with no live winner —
  a stale registry row — still errors. `flz operative start --partitions N`
  over an existing group converges: only the missing members start, iff N
  matches the identity every survivor declares (else it refuses naming
  `flz operative scale`); a fresh group start and its all-or-nothing rollback
  are unchanged. `flz plugin enable` on a plugin id already registered for the
  same operative exits 0 untouched — no cspec write, no re-subscribe; the same
  id held by a different target still errors, and a group enable counts
  already-enabled members as success. Two id collisions previously accepted
  are now refused: a plain start of a live group's base id (it minted a rogue
  singleton beside the group) and a partitioned start over a live
  unpartitioned operative. Together these make re-running a cold-start script
  the supported heal after a warm restart. Scripts keying on the old non-zero
  "already exists" exit must use `flz operative info` instead.

- **The control plane has one actor, and every failure signal has one consumer**
  (`close-control-plane-silent-wedges`). Three silent-wedge classes closed
  subtractively, under one rule: sensors sense, exactly one actor acts,
  `/healthz` reads the actor's snapshot, the orchestrator restarts on
  `/healthz`.

  `Flezha.State.Watchdog` is a pure sensor again. Its direct
  `State.Recovery.resync/2` on an output stall, its `last_resync` dedup map and
  `recently_resynced?/4` are **deleted** — they were a second, uncoordinated
  remediation path racing the convergence Loop's own rebind with no shared
  backoff. The remedy is not lost: the Loop's output-stall response is now a
  ladder, `resync_runner` (a new `Heal` primitive re-running the
  `RestoreInitState` handshake against the runner the operative is already
  bound to — no migration) escalating to `rebind_operative` when the stall
  survives it. Both rungs share ONE cooldown key so escalating walks a single
  5 s → 30 s → 60 s → 300 s ladder, and the rung is read off that entry's
  attempt count rather than a second counter. Measured end to end: 0.84 ms from
  sweep to remedy decision, because the Loop ticks on the stall event rather
  than waiting out `tick_ms`.

  The data-plane stall classifier's verdict — computed every tick and, until
  now, read by nothing — is passed into `Diff.compute/3` in-process (no
  telemetry subscription, no side channel) and mapped by remedy: wedged runner
  → `evict_runner`, wedged connection → `resync_runner` for that runner's
  operatives, wedged operative → `rebind_operative`. Ops ride the standard
  per-target backoff, and a target the classifier *proved* wedged is no longer
  protected by the guards that exist to spare a healthy one — neither
  `skip_rebind?`'s active-runner check nor `FleetManager.evict_runner/2`'s
  "never evict a live runner" rule, which had made a pool-wedged (still
  connected) runner unevictable.

  Controller-fabric verdicts (`driver_loop_frozen`, `nif_queue_backing_up`)
  reach `/healthz` as `503 data_plane_fabric_wedged` once sustained for
  `FLEZHA_FABRIC_503_POLLS` consecutive polls (default 3; `0` disables the
  fabric 503 and is the rollback switch). No BEAM-level heal reaches a wedged
  NIF driver thread — every heal this controller could issue travels through
  it — so the orchestrator's restart is the remedy. Verdicts the pipeline owns
  stay `200 degraded`.

  Sensors are now watched with timestamps rather than another process: a
  periodic sensor stamps its own last completed pass into
  `Convergence.Snapshot`, and `/healthz` reports one older than 3× its cadence
  as `200 degraded, reason: sensor_stale`. A Watchdog that wedges without
  crashing is visible within one poll instead of silently turning detection
  off.

  Start authority is one claim: `OperativeManager.start_operative/3` now mints
  the `:duplicate` verdict itself (the CLI, schedules, procedure-requested
  spawns and the ProcessHub orphan reconcile all resolve through the same
  registry check), and a start that loses it verifies the winner is live and
  does nothing to it. Previously the loser took the generic failure path, which
  called `stop_child_quietly/2` on the id — **stopping the winner**. A reconcile
  round completing with `started > 0` now triggers one adopt-and-converge tick,
  and `reconcile_duplicate` is alarm-grade with its kept/stopped nodes.

  Also fixed: a restarted `Convergence.Loop` left the previous incarnation's
  telemetry handler registered, so `attach_many` refused as `:already_exists`
  and the new Loop never heard another sensor event — a silently deaf actor, of
  exactly the class this change closes.

  One config key added (`FLEZHA_FABRIC_503_POLLS`); no new module, loop,
  GenServer or telemetry subscription; no wire, storage or SDK change.

- **Workflow stage work runs on the SDK's workers, in both languages**
  (`workflow-steps-through-sdk`). The workflow driver used to fan a stage out
  onto a second, core-owned thread pool, and a Java step cost TWO threads: one
  of those pool threads submitted the crossing and then blocked on the reply
  while a Java thread ran the code. Both are gone. The driver now hands each
  entity partition out as a **step activation** on the poll seam's direct
  queue — a typed class runs it on its poll workers, a C-ABI host receives the
  same ordinary `Work` (or `CompensateRun`) activation it already knew — and
  collects the answers at the stage barrier it already owned. Every step
  therefore costs exactly one thread, no core-owned thread runs or waits on
  user code, and workflow work inherits the adaptive pool / virtual threads
  from `sdk-owned-concurrency`. The entity-retry ladder moves with it: a step
  runs ONE attempt and releases its worker, and the driver times the
  re-submission at its barrier, so a backoff window no longer occupies a
  worker (policy, classification, exhaustion, telemetry and stop
  responsiveness are unchanged). `WorkStealingPool` and every handle threaded
  through the tree are deleted — the change is a net deletion. No wire,
  journal, persistence or authoring-API change; Java bytecode is untouched.

- **Blocking user code no longer halts the runner** (`sdk-owned-concurrency`).
  The worker count used to be the limit on how many jobs could be underway:
  with the pool sized to CPU count, CPU-count slow calls (an LLM turn, a slow
  HTTP fetch) stopped everything else — not slowed, stopped. That limit now
  belongs to the core and is decoupled from any thread count: one process-wide
  cap on work activations underway (`FLEZHA_MAX_INFLIGHT_ACTIVATIONS`, default
  1024), enforced at handout and released on completion. Direct activations
  (restore folds, snapshot requests, workflow steps) stay exempt — they are
  barrier-bounded by their own drivers. Each SDK then services that
  with whatever its runtime does best: **Rust** parks one supervisor thread in
  the core's new starvation wait (work queued, nobody inside `poll`) and grows
  its pool one thread at a time up to `FLEZHA_WORKER_POOL_MAX` (default 10×
  cores), with the grown threads retiring after an idle streak; **Java**'s
  poller threads become two platform *fetchers* that hand every activation to
  a **virtual thread**, which dispatches and completes it, so waiting costs a
  stack and the starvation condition cannot arise. No wire, controller,
  persistence or authoring-API change; a workload that never starves observes
  exactly the pool it does today.

- **Neither a retry backoff nor a processing-time timer holds a thread**
  (`sdk-deferred-wake`). One deferred-wake list inside the poll core now
  answers "do something with this entity at time T", serviced by the threads
  already waiting in `poll` — no thread of its own. A retryable failure
  records its backoff and returns the completing worker immediately (it used
  to sleep there, so a dependency outage that failed every entity could put
  every worker to sleep at once, and the sleep also delayed shutdown); the
  entity's exclusivity gates stay held across the window, so ordering and the
  head-of-line holdback are unchanged. The per-Operative timer wheel thread is
  deleted: an Operative registering timers across any number of Entities now
  costs no thread. Firing semantics are unchanged — onto the Entity's mailbox,
  serialized against `run`, ascending scheduled order, `fired_at` still the
  scheduled timestamp — except that a deadline passing while every worker is
  inside user code is promoted when the first one re-polls. Both SDKs get this
  from the shared core; no wire, controller or API change.

### Added

- **An application can now see that its input went quiet**
  (`expose-entry-input-idle-signal`). The SDK owns the IO threads, so when a
  feed goes silent every procedure simply stops being called: output freezes,
  the process stays alive, and the only component that knows a frame arrived is
  the SDK. Both SDKs now track, per entry channel, the instant the runner last
  delivered a frame there — one relaxed atomic store on the delivery path — and
  expose it plus an idle-observer registration on the runner handle:
  `last_entry_delivery` / `on_entry_idle(channel, threshold, observer)` (Rust),
  `lastEntryDelivery` / `onEntryIdle` (Java). The observer fires **once per idle
  episode**, never before the channel's first frame (a cold start is not an
  incident), and re-arms on a resume notification; it runs on an SDK background
  tick, never on the delivery path, and a panic/throw is contained and logged.

  The SDK ships **no default threshold and no default action** — the signal
  (when a frame last arrived) is SDK knowledge, the policy (what gap is
  anomalous — a quiet overnight market is not a dead feed) is the
  application's. The stamp itself lives in the shared core, written on the one
  entry-delivery seam, so the Java SDK — a poll host over that core — reads the
  same fact through the C ABI rather than keeping a second copy in step.
  `docs/sdk/entry_input_idle.md`.

- **The authoring sugar can now feed a `.keyed()` channel.** A keyed output
  channel accepts only `emit_keyed`, but `#[flezha::procedure]` / `@Procedure`
  always generated a plain `emit` — so every procedure with a keyed sink had to
  be hand-written as a full `Procedure` impl, and a partitioned operative wants
  keyed sinks on *all* of its outputs. Both SDKs gain a `key` opt-in naming a
  function from the output to its natural key: Rust
  `#[flezha::procedure(key = my_fn)]` with `fn(&Output) -> String`, Java
  `@Procedure(key = "myMethod")` with `static String myMethod(Output)`. The
  generated emit then carries the key and the runtime stamps `flz.key`.
  Declaring `key` on a procedure with no output is a compile-time /
  annotation-processing error. Nothing changes for a procedure that does not
  declare it.

- **The testnet WS server speaks Kraken-v2 subscriptions.** It understood only
  its own `{"event_type":"subscribe",…}` payload, whose generators were global
  to the server — so a partitioned operative could not be exercised against it:
  every member would have received every symbol and parked the out-of-range
  half. It now also accepts
  `{"method":"subscribe"|"unsubscribe","params":{"channel":…,"symbol":[…]},"req_id":N}`,
  acks once per symbol echoing the `req_id` the subscription source correlates
  on, and runs generators **per connection per symbol**. Generator tuning moves
  server-side (`FLEZHA_TESTNET_CONFIG`: per-channel generator + cadence,
  per-symbol overrides layered on shared defaults), since a v2 subscribe
  carries no room for it. The legacy payload is unchanged, and a disconnect now
  stops that connection's generators instead of leaking them.

- **One stream operative can now run across N machines**
  (`add-partitioned-operatives`). A stream operative bound to exactly one
  runner, so a single logical stream could never use more than one machine —
  the only way past that box was hand-sharding into N unrelated operatives with
  hand-split source configs, no routing-correctness check, no unified view, and
  no way to change N later without orphaning entity state.

  `flz operative start ohlc candles --partitions 3` materializes one logical
  operative as three **ordinary** instances (`ohlc#p0` … `ohlc#p2`), each with
  its own Overwatch, ProcessHub child, registry row, runner binding, input log,
  state tree, plugins and DLQ identity. Placement, convergence, rebind,
  migration and recovery act per member, unchanged. There is no group record:
  each member's cspec opts carry `group`/`partition`/`partitions` and its owned
  key-group range, and every group view is recomputed from the member rows.

  Keys hash into 256 key groups via the FNV-1a-128 already frozen by
  `StableId`; each partition owns a contiguous range, shipped explicitly rather
  than derived from `(index, count)`. Three implementations (`Flezha.Partition`,
  `flz_core::util::key_group`, Java's `StableId`) are pinned against one shared
  fixture-vector set.

  - **Sources filter the ask, not the data.** The websocket subscription
    source filters its desired set to the partition's range before reconciling
    (new static `subscription_items` opt supplies a literal list through the
    same machinery, mutually exclusive with `subscription_url`); `http_poll`
    gains `{{partition_index}}` / `{{partition_count}}` URL placeholders
    rendered once at init, with URL validation running on the rendered value.
  - **`operative_forward` routes per message** by `hash(flz.key)` to the owning
    member, one worker per target member. A message with no `flz.key` is parked
    with `partitioned target requires keyed emit` — never round-robined.
  - **The SDK refuses a mis-routed key on its first frame**, naming the key,
    its key group, the owning partition and the contract, before any state for
    that key exists. Additive `AddOperative` fields (`partition_index`,
    `partition_count`, `key_group_from`, `key_group_to`) carry the range; a
    class with no keyed entry-fed procedure is refused with
    `PartitionsWithoutKeyedProcedure`.
  - **`flz operative scale <group> M`** rescales under an explicit quiesce
    (sources off, input logs consumed, DLQs empty), moves every Entity whose
    key group changes owner (rename on one controller, `WalTransfer` pull
    across controllers), then starts M members with the recomputed ranges. A
    refusal changes nothing; a crash mid-move is re-runnable.
  - **CLI**: `flz operative list` shows one row per group with per-member
    placement, `info` validates that all 256 key groups are owned exactly once
    and renders gaps/overlaps as warnings, `stop <group>` fans out, and
    `flz state get <group>/<procedure>/<key>` resolves the owning member by
    hash. `flz plugin enable/disable <group>` fans out with partition opts
    injected and `#pK`-suffixed plugin ids.

  `--partitions 1` (the default) is byte-identical to before; all proto fields
  are additive with zero values meaning "unpartitioned". See
  `docs/sdk/partitioned_operatives.md`.

- **A controller can now survive dying with its disk**
  (`add-remote-durability-tier`). Every durable byte flezha owned lived on the
  owning controller's local disk — Entity WALs and snapshots, workflow
  proc-journals, durable inboxes, the effectively-once input log, and the
  ProcessHub registry cspec that records which operatives exist at all — and
  `WalTransfer` moves a subset only at migration time. Losing the disk lost all
  of it. The new `Flezha.State.DurabilityTier` ships those as immutable
  artifacts to a remote backend and hydrates them back when a controller binds
  an operative whose local state is absent.

  **Off by default.** With `FLEZHA_DURABILITY_BACKEND=none` (the default) no
  process starts, no backend is contacted, and every acknowledgement path gates
  on local fsync exactly as before. Backends are `postgres` (over the
  already-present `postgrex`) and `s3` (hand-rolled sigv4 over `:mint`); a
  backend implements only `put`/`get`/`list`/`delete`/`put_if` and never learns
  WAL semantics.

  Shipping reuses the boundaries that already existed — compaction completion,
  checkpoint-interval close, workflow `ProcCommit`/`IterCommit` — plus one
  backstop sweep (`ship_interval_ms`, default 30 s) for entities that reach
  none of them. A shipped WAL artifact is a **seq-addressed record bundle** read
  at ship time, never a file byte range, because compaction and correction
  rewrite the live WAL in place. The per-reference manifest is written **last**
  through an epoch-conditional write, so a crash mid-upload leaves the previous
  entry authoritative and the orphan blobs invisible to recovery.

  A workflow class may declare `SYNC_DURABILITY` (Rust) / `syncDurability()`
  (Java): the controller then completes the remote ship of a commit barrier
  before acking it, so the ack the driver blocks on implies the commit survives
  losing the disk (RPO 0). Stream classes are refused it at registration.
  Everything else ships asynchronously and never delays an append, ack or
  watermark advance; a stalled shipper raises the alarm-grade
  `[:flezha, :state, :rpo_exceeded]`, surfaced in `flz controller status` and
  `/healthz`. A stale-epoch manifest refusal fences that reference and alarms
  rather than overwriting a claimant that superseded it.

  Rollback is `backend: none` — local state is authoritative throughout, so
  disabling the tier alters nothing on disk. See
  [`docs/operations/remote_durability_tier.md`](docs/operations/remote_durability_tier.md).

- **Every controller node is a valid entry point for every namespace command**
  (`schedule-spawn-without-runner`). A new cluster directory
  (`Flezha.Cluster.Directory`, one `:pg` scope — no new dependency) lets each
  node advertise which factories its runners serve and which namespace hubs it
  hosts; reads are local on every node. Workflow starts (`flz operative add`,
  schedule fires, `ctx.spawn_workflow`) submitted on a node that cannot satisfy
  them locally are forwarded once to a capable node and re-validated there, so
  a schedule no longer fails forever when a rolling update moves its runner to
  another controller node. The command plane routes the same way: `flz
  operative send`/`stop`/`info`, `flz state get` and `flz dlq replay` resolve
  their target cluster-wide. A failed schedule slot is retried with capped
  exponential backoff under the same deterministic instance id until the next
  slot supersedes it, and `flz schedule list` gained `LAST FIRE (UTC)` /
  `LAST RESULT` columns. `factory_unknown` and "no runners" refusals are now
  honest cluster-wide verdicts. The dead `Flezha.Service.RunnerManager` was
  deleted (`Flezha.list_runners/1` now reads through the directory).

- **The Java SDK is rehosted on the poll core**
  (`rehost-java-sdk-on-poll-core`). The Java runtime's kernel mirror —
  command handlers, framing, reconnect, flusher, credit, DLQ park, timer
  wheel, the workflow driver — is deleted; JVM poller threads drive
  `flz_core` through a JNI binding over `flz_core_ffi` (`FlzCore`,
  schema-negotiated at context creation). The core's host layer grew from
  v1 to full capability for this: the five command activation kinds
  (restore fold, snapshot, live read, apply-state, migration serialize),
  the prepare crossing for keys/event times, static/dynamic entity modes,
  producer-evaluated inter-procedure edges, the effectively-once and
  checkpoint ack-granularity tiers, late-input correction, per-entity
  timers, and workflow lifecycles (run-sugar sessions + structured chains)
  — all reachable through the C ABI and pinned by the ABI scenario suite.
  Activation schema bumped to v2 (prepare answers, `change_seq`,
  `factory`, `Output.event_time`, workflow kinds); both SDKs' fixture
  suites pin the same bytes.

- **The Rust SDK's kernel is now a language-agnostic poll core**
  (`extract-sdk-poll-core`). A new `flz_core` crate owns transport,
  ordering, durability, retries, timers, credit, routing and the workflow
  driver; host SDKs drive it through one surface — `poll() -> Activation` /
  `complete(ActivationResult)` — and the core never calls host code. The
  Rust SDK is rebuilt as a typed layer over the crate (public API
  unchanged); `flz_core_ffi` exposes the same engine over a versioned C ABI
  (creation-time schema negotiation, any-thread poll/complete, shutdown
  wake), with a CI scenario driving connect → inputs → timer → shutdown
  entirely through the ABI. No wire, controller, or WAL format change.
  See `docs/sdk/poll_core.md`.

- **A live websocket's subscription set can change without reconnecting**
  (`add-websocket-subscription-source`). `Plugin.Provider.Websocket` accepts an
  optional `subscription_url` publishing the desired item set
  (`{"version": <integer>, "items": [<string>, …]}`). The provider polls it every
  `subscription_interval_ms`, diffs it against what it has applied, and sends
  operator-supplied `subscribe_template` / `unsubscribe_template` frames — with
  `{{items}}` and `{{req_id}}` substituted — for the difference, on the
  connection it already holds. Previously the only way to change a subscription
  was `plugin disable` then `plugin enable`: a full teardown that dropped every
  other stream on that socket to move one item, and spent the upstream's
  reconnect budget on a change needing no reconnect.

  Item strings are opaque — never parsed, validated or interpreted — so all
  exchange vocabulary lives in the templates and in the source's response, not in
  the framework. A delta commits into the applied set only when an inbound frame
  bearing its `req_id` reports success; failed, malformed and unanswered
  requests are simply left uncommitted, so the next cycle re-derives an
  identical delta. Convergence is the retry mechanism, and there is no bespoke
  retry state. Protocols echoing no correlation id use
  `subscription_ack_mode: "none"`, committing on send.

  The applied set is persisted to the plugin cspec after every commit, so a
  plugin restart, an operative handover and a controller restart all resume with
  what the socket actually holds. That also fixes a correctness edge predating
  the feature: the data-liveness watchdog resent `post_connect`, the *boot-time*
  payload, so any runtime change would have been silently reverted by the very
  mechanism meant to heal a stalled subscription. Connect and both watchdog
  tiers now assert the current applied set.

  A failed poll is *no information*, never an empty desired set: it can never
  cause an unsubscribe. An explicitly empty `items` array is a real instruction
  and does. `flz operative info` reports the applied set, last reconcile outcome
  and last error; each reconcile that sent a frame emits
  `[:flezha, :ws, :reconcile]`. Fully backwards compatible — with no
  `subscription_url` there is no poll timer, no applied set, and no subscription
  state on the per-frame path. See `docs/plugins/websocket_subscription.md`.

- **An Entity's current value is readable without a serving tier**
  (`entity-state-live-query`). `flz state get <operative/procedure[/entity_key]>`
  returns one live Entity's in-memory state as JSON together with the
  `change_seq` it was serialized at. Until now the only read was
  `flz state at`, which serves the controller's WAL at a point in the *past*, so
  anything needing the current value had to wait for it to reach a sink and read
  that copy — a second source of truth, always behind, free to diverge.

  The value is the freshest one there is: the SDK serializes under the Entity's
  state lock and does **not** drain pending changes, so the reply includes
  changes applied but not yet flushed, and is therefore at or ahead of what is
  durable. The seq is how a caller tells versions apart and sees how far ahead of
  durability it is. A read triggers no compaction, snapshot or WAL truncation.

  An Entity that is not held in memory is **refused naming why** — idle-evicted,
  instance suspended, no Entity by that key, or Operative unbound — and points at
  `flz state at` as the durable path. There is deliberately no durable fallback:
  dynamic-mode eviction is destructive (the recreate cold-starts), the controller
  cannot decode the SDK's WAL bytes, and a fallback would mean binding a runner
  and restoring an Entity as a side effect of a read. `entity_evicted` and
  `entity_not_found` are distinguished by a bounded per-SDK trace of what the
  reaper evicted, since eviction otherwise leaves nothing behind.

  The reply is bounded by the existing `:wal_high_watermark_bytes`; over it the
  read is refused with the byte count and the selector that would narrow it. No
  new configuration key, no new control command, no new payload-id: `EntityState`
  gained two optional selector fields, and an unset selector encodes
  byte-identically, so the migration/handover caller is untouched. Dynamic-mode
  Entities are covered on both runtimes, closing a gap in the serialization
  requirement. Authorized as `:"state.get"` (viewer). Docs:
  `docs/internals/state_persistence.md`; examples:
  `sdk/rust/examples/live_state_read.rs`, Java `E11_LiveStateRead`.

- **An operative can be fed from an HTTP resource**
  (`add-http-poll-source-provider`). The built-in `operative_http_poll` source
  provider (`Flezha.Plugin.Provider.HttpPoll`) polls a URL on an interval and
  injects the response body as an ordinary journaled entry frame — but only when
  the body's SHA-256 changes, so a steady endpoint costs one request per interval
  and zero frames. The first successful poll after the plugin starts always
  injects, which converges an operative that has just been started, recovered, or
  handed over onto current content within one interval; consumers of a polled
  channel must therefore treat its frames idempotently. Until now runtime
  configuration could reach a running operative only by restarting it with new
  `init_values` or by an operator hand-pushing `flz operative send`.

  Delivery uses the operative's confirming ingest and the hash advances only once
  the operative has taken the payload, so a frame dropped by a parked operative
  or an unbound stream is re-offered on the next poll instead of being lost until
  the endpoint next changes. Timeouts, connection errors, statuses outside 2xx
  (redirects included — they are not followed, since `:httpc` would replay the
  configured headers to an unvalidated host and downgrade TLS), an oversized
  body, a non-UTF-8 body, and a refused ingest all inject nothing, retain the
  last delivered hash and ETag, and count toward the standard provider health
  contract; the next success clears the run and delivers the change the outage
  hid. `ETag`/`If-None-Match` is passed through for bandwidth, with the local hash
  still the injection gate. The size cap aborts the transfer as the body arrives,
  so it bounds controller memory and not merely what the operative receives.
  HTTPS is verified against the system CA store. Attach refuses a non-http
  scheme, a hostless URL, credentials embedded in the URL, a sub-second interval,
  a timeout that could outlast its own interval, a non-positive tunable, a body
  cap above the ceiling, and any unknown option — the last on `init_provider/1`,
  the path a `--plugins` spec actually takes.

  **Controller-only**: no SDK, wire, or journal change. See
  `docs/plugins/http_poll.md`.

- **A procedure can consume more than one channel**
  (`add-multi-input-procedures`). `c.merge::<I>()` (Rust) / `c.merge(I.class)`
  (Java) subscribes two or more source channels to one procedure whose `Input`
  is an author-declared sum type, each subscription supplying a pure mapping
  from its payload into that type. Entry channels and inter-procedure channels
  are both valid sources on a stream operative, freely mixed, so a detector
  can take its data feed and
  a live config channel without the operative being restarted with new
  environment. Per-source FIFO is preserved and nothing is ordered across
  sources; `entities_for` sees the mapped value; the mappings run identically on
  live delivery and WAL replay, so a warm restart restores the same state.
  Fewer than two sources, the same channel subscribed twice, or a source that
  would re-wire an already-declared edge all fail operative definition with a
  named topology error. A **workflow** operative merges its procedures' output
  streams — its entry channel seeds the root from the spawn args and names no
  channel, so an entry channel as a workflow merge source is refused by name.
  Single-input wiring is unchanged.

  **No wire change and no controller change**: the merge is a subscription-time
  construct in the SDK, frames are still journaled per source channel, and the
  controller has no model of a channel's consumers. See
  `docs/sdk/multi_input_procedures.md`.

- **A procedure can ask for a workflow operative**
  (`add-procedure-requested-spawn`). `ctx.spawn_workflow` (Rust) /
  `ctx.spawnWorkflow` (Java) spawns-or-delivers a workflow instance on one
  `(namespace, instance_id)` from inside any procedure: a free id starts the
  factory with the payload as its first input, a taken id receives the payload
  as one more input (`if_running: deliver`) or refuses (`error`). Concurrent
  requests race-resolve at the operative registry to exactly one instance.
  Failures — quota, unknown factory, unreachable controller — resolve the
  caller's async handle and never fault the requesting procedure; an
  uninspected handle logs the failure once. Spawning is a live-processing side
  effect: a replayed invocation issues nothing. Until now a workflow operative
  could only be started from outside the cluster, so every pipeline that needed
  one grew its own external dispatcher. See
  `docs/sdk/procedure_requested_spawn.md`.

  **Wire**: two additive `CommandRequest` variants, `SpawnWorkflowRequest`
  (tag 34, SDK → controller) and `SpawnWorkflowResponse` (tag 35,
  controller → SDK). Additive-tag compatibility as with every prior wire
  change: an older controller ignores the unknown variant (the requester times
  out), so the controller image and both SDKs are released together.

- `flz schedule add --plugins <json>` — a schedule entry can now carry the
  same plugin-spec array `flz operative start --plugins` accepts
  (`schedule-entries-carry-plugins`). The spec is validated against the
  enabled providers at `add` time, stored verbatim, and decoded at every
  firing; each firing stamps its namespace and instance id on every plugin
  and prefixes the plugin id with the instance id so overlapping slots never
  collide. Without this a cron-fired workflow job had no output channel at all
  — a workflow instance finishes in milliseconds, so plugins must be attached
  at spawn. A spec that fails to decode at fire time starts nothing and is
  reported on `[:flezha, :schedule, :spawn_failed]` rather than firing a run
  whose output is dropped. The schedule row gained a field, so the
  delete-`schedules.dets`-and-re-add note below applies to this change too.

### Changed (BREAKING)

- Scheduled workflow spawns are now driven by **one schedule process per
  schedule, hosted in a dedicated `:flz_schedules` ProcessHub**, replacing the
  per-controller polling evaluator (`single-owner-schedule-processes`). The
  hub's `Singularity` redundancy makes exactly one process exist cluster-wide
  per schedule name, so a slot fires once without leader election and without
  cross-node state sync: every controller asserts the definitions its own
  node-local store holds and duplicate assertions no-op. This closes the
  documented multi-controller double-fire hole. `Flezha.Schedule.Evaluator`
  is removed; the schedule entry's `last_slot` watermark is gone (a process
  holding an armed timer *is* the watermark) and with it
  `FLEZHA_SCHEDULE_TICK_MS`. **A `schedules.dets` written by an earlier build
  must be deleted (`rm app_data/schedules/schedules.dets`) and its schedules
  re-added** — the row shape changed and no migration is provided.
  `flz schedule list` gains a `NODE` column. On start a schedule process
  fires the single most recent slot inside `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS`
  to cover restart and migration gaps; older slots are still never
  back-filled.

- New guarded SDK entry point in both SDKs (`runner-lifecycle-singleton-api`,
  superseded before release by `sdk-runner-entrypoint-redesign` — the shipping
  surface is the one described here). `Flezha.newInstance(namespace)` (Java) /
  `flezha_runner::new(namespace)` (Rust) and the
  `Flezha.registerStream`/`registerWorkflow` /
  `register_stream::<R>()`/`register_workflow::<T>()` registration surface
  are removed with no back-compat shim. The replacement is a runner handle —
  no builder, no freeze step:
  `Flezha.createRunner(ns).defineStream(A.class).run()` (Java) /
  `Flezha::create_runner(ns)?.define_stream::<A>()?.run()?` (Rust).
  `RunnerBuilder` and both Java registry facades
  (`StreamOperativeRegistry`/`WorkflowOperativeRegistry`) are gone.
  The declared set is per-runner and starts empty each generation;
  `define_*`/`remove_*` (`removeStream`/`removeWorkflow` /
  `remove_stream::<R>()`/`remove_workflow::<T>()`) are legal before and
  after `start()`, a post-start change shipping the full factory set as a
  fire-and-forget `UpdateFactories`. A removal refuses while live instances
  of the factory exist (`OperativeTypeInUseException` /
  `FlezhaError::OperativeTypeInUse`). Starting with zero declared classes is
  legal.
  The singleton slot is claimed at `start()`, not at create: any number of
  unstarted runners may exist, and a second `start()`/`run()` fails loudly
  (`IllegalStateException` / `FlezhaError::AlreadyRunning`) naming the running
  namespace instead of silently corrupting the process-global
  identity/sink/credit state; after `close()`/`Drop` a new runner may start.
  `create_runner` is what returns the `FlezhaRunner` handle — `start()`
  returns `Result<&Self, FlezhaError>` (bind it: a chained
  `…create_runner(ns)?.start()?` drops the temporary at statement end),
  `run()` = `start()` + `awaitTermination()` (standalone binaries — the
  `Thread.currentThread().join()` / stdin-loop keep-alive hacks are gone),
  and `Flezha.currentRunner()` / `Flezha::current_runner()` exposes the
  active runner. Rust declaration, connect, and lifecycle failures are now
  ONE typed `Result` error, `FlezhaError` (wrapping `TopologyBuildError` /
  `BuildWorkflowError`), instead of panics and the removed
  `RunnerStartError`/`RunnerError`; the per-issue topology enum is
  `TopologyIssue`. Java's annotation processor generates
  `GeneratedStreamOperatives.defineAll(FlezhaRunner)`.
  Naming rules for future SDK surface: `docs/sdk/naming_conventions.md`.

### Fixed

- **A rescale left everything forwarding into the group routing by the old
  member set.** `operative_forward` resolves its target's members once, at
  `init_provider/1`, and nothing re-ran it: the workers stayed alive, so the
  plugin reported healthy while the worker for a removed member retried
  forever against an operative that no longer existed — until its queue
  saturated and back-pressured the **source** operative's output chain. A
  rescale now nudges every forward that targets the group to re-read the member
  set, and reports how many it re-pointed. Surviving members keep their workers
  and their queued payloads; a removed member hands its queue back before its
  worker stops and each payload is re-routed to whoever owns its key now, in
  order and without loss. A restarted provider that exports no `activate/1`
  also returns to `:running` rather than sitting `:prepared` — where
  `plugin_health/3` would have reported it healthy without probing until the
  next bind.

- **Two concurrent `flz operative scale` calls on one group could leave it half
  started** — each read the member set the other was rewriting, so the loser's
  rollback stopped the members the winner had just started. A rescale now takes
  a cluster-wide lock on `{namespace, group}` and a second one is refused by
  name. The lock is released if the holder dies, which is the crash case the
  re-runnable move sequence already assumes.

- **A rescale could restart a member before its id was released.** The stop
  replies before ProcessHub finishes unregistering the child, so reusing the id
  — every rescale reuses `#p0` — could lose the claim to the member it had just
  stopped. The stop now waits for the same uniqueness predicate the restart
  checks, and gives up before anything durable has moved.

- **A dynamic Entity never restored its durable state on a rebind**
  (found implementing `add-partitioned-operatives`). The SDK's restore handler
  resolved the named Entity by walking `instance.entities` — the static and
  single slots — while a dynamic Entity lives in its Procedure's registry and
  has no slot until its first input. Every `RestoreInitState` for one was
  therefore acked with its terminal seq and discarded, and the next input
  cold-started the Entity on top of its own WAL. The handler now materializes
  the named dynamic Entity and folds the state into it. Eviction is unchanged
  and does not conflict: it deletes the files, so no restore is ever shipped for
  an evicted Entity. This is also what carries a key across a partition
  rescale, where its state moved into another member's tree before that member
  ever saw the key.

- A controller that lost **every runner** for a namespace could neither keep its
  operatives nor move them, and the healing machinery hid the damage
  (`fix-last-runner-migration-state-loss`). Three defects compounded:
  `hub_migrate` had been reduced to a vacancy check that no-ops exactly when the
  hub still holds operatives — i.e. whenever there is something to migrate;
  `:runner_migrate` committed the binding *before* the runner answered, so a
  failed heal left the operative naming a **live but unreachable** runner, which
  made `Convergence.Diff` stop classifying it misbound and emptied the delta
  permanently; and because a handover serializes its state from the **runner**,
  a migration caused by that runner's death shipped an empty payload while still
  marking `arrived_via_hotswap?`, which suppressed the WAL restore *and* the
  explicit repair path. The binding is now committed only on success, the flag
  only when the handover carried state, and `hub_migrate` drains the hub
  (`HubManager.drain_hub/2`, the seam graceful shutdown already used) so
  operatives leave through a live HotSwap handover. New
  `Flezha.State.WalTransfer` moves what the handover cannot: a
  destination-pulled, chunked `:erpc` copy of the Entity's durable files,
  written to a `.transferring` sibling that is fsynced and atomically renamed,
  with names resolved through the Entity's own file set so a peer cannot read
  arbitrary paths. `state_root` stays node-local and is never assumed shared.
  One-shot at migration time, not replication — a controller that dies together
  with its disk still takes its state with it.

- A durable workflow operative whose procedures hold **no per-Entity state**
  went silent after its first turn. The proc-journal is instance-scoped but rode
  `RestoreInitState.proc_journal`, a per-Entity frame — and such a class has no
  Entity WAL, so the controller shipped no restore at all. Every wake resumed
  at iteration 0, and each turn's commit collided with iteration 0's
  `outbox_flushed` marker (the flush marker is keyed `{type, proc_idx, iter}`),
  so its staged output was journaled and never delivered. Only the first turn
  of such an operative was ever emitted; it bit any instance that parked
  between turns, on both SDKs.
  `ProcJournalState` moves from `RestoreInitState` (field 6, removed) to
  `AddOperative` (field 6, added) — instance-scoped state on the bind, beside
  `resume_entity_count`, delivered once per bind instead of once per persisted
  Entity and before the driver spawns. No compatibility shim: an SDK and a
  controller must be upgraded together.

### Changed

- An operative may now declare **more than one entry channel** (both SDKs). The
  `sdk-graph-authoring` graph-shape rule is relaxed from "exactly one entry" to
  "one or more entries, every node reachable from some entry"; the
  `MultipleEntryChannels` topology error is removed. The runtime already routed
  each inbound frame to its entry by `metadata["channel"]` (falling back to the
  first-declared entry), so this only lifts the build-time refusal — enabling a
  single operative to ingest several external sources (e.g. a market pipeline
  reading a trade feed and an order-book feed on distinct channels). Workflow
  operatives keep their single-entry linear-chain rule.

- Plugin subscriptions added via `flz plugin enable` now survive any
  Overwatch (re)start automatically. The ProcessHub child-spec is the
  source of truth for an operative's enabled plugins; the runtime
  `Overwatch.state.extensions` table is rebuilt from it on every
  `init/1`. Previously, plugins enabled after `flz operative start`
  were dropped on runner-pod restart, agent restart with intact
  ProcessHub state, or any in-place restart that ran through
  `HubManager.alter_child_data/2` (e.g. `kubectl rollout restart
  daemonset/<sdk-daemonset>`), forcing runners to re-run the enable
  commands manually before the data plane recovered. See
  `openspec/specs/plugin-subscription-persistence/spec.md`.

  **Upgrade note for clusters with already-running operatives.**
  Operatives whose plugins were enabled dynamically before this
  upgrade have an empty `opts[:plugins]` in the cspec. They keep
  working as long as no Overwatch (re)start happens before the next
  `flz plugin enable` / `flz plugin disable` proc, which canonicalises
  the cspec. Runners who want to canonicalise immediately can run
  `flz plugin disable <id> && flz plugin enable <id>` for each
  plugin. No automatic migration job is shipped.

### Added

- Controller-native cron scheduling of workflow operatives
  (`add-scheduled-operative-spawns`). `flz schedule add <name> --cron
  "<expr>" --tz <iana> --factory <factory_id> [--namespace ns]
  [--init_values json]` registers a durable schedule (node-local DETS at
  `app_data/schedules/schedules.dets`; names are capped at 11 bytes so the
  composed instance id fits the 32-byte operative id limit); `list` /
  `remove` / `run <name>` complete the family. A per-controller evaluator
  (`Flezha.Schedule.Evaluator`) fires each matching slot through the
  standard workflow start path with deterministic instance id
  `<name>@<slot-iso>` — a duplicate fire attempt no-ops on the id, which is
  the cluster-wide exactly-once guard. Slots are evaluated in the entry's
  IANA time zone (new `tz` dependency): a spring-forward gap slot skips, a
  fall-back ambiguous slot fires once at the first occurrence. Slots older
  than `FLEZHA_SCHEDULE_MISFIRE_GRACE_MS` (default 120 s) at evaluation —
  e.g. missed while the controller was down — are skipped and logged with
  their timestamps, never back-filled; a still-running previous slot never
  blocks the next one (ids differ; bound runtime via `max_duration`). Only
  workflow operatives are schedulable. Telemetry: `[:flezha, :schedule,
  :fired | :skipped | :spawn_failed]`. Operator guide in
  [`docs/operations/schedules.md`](docs/operations/schedules.md).

- Hardened first-party Kafka producer plugin
  (`Flezha.Plugin.Provider.Kafka.Producer`). The previous thin
  `KafkaEx.produce`-per-call wrapper is replaced with a per-`(plugin_id,
  topic, partition)` `ProducerWorker` GenServer that runs linger-based
  batching, bounded outbound queueing with configurable overflow strategy
  (`:drop_oldest` / `:drop_newest` / `:block`), automatic retries with
  exponential backoff for every transient broker error class, active
  metadata refresh on `:not_leader_for_partition` / `:no_broker` /
  `:unknown_topic_or_partition`, idempotent producer semantics via Produce
  v5+ (`InitProducerId` + per-partition sequence numbers), and auto-create
  on first publish for unknown topics. Underlying `KafkaEx.New.Client`
  workers are deduplicated across plugin instances by connection identity
  via the new `Flezha.Plugin.Provider.Kafka.WorkerRegistry`. New plugin
  parameters: `:retries`, `:retry_backoff_ms`, `:retry_max_backoff_ms`,
  `:delivery_timeout_ms`, `:linger_ms`, `:max_batch_bytes`, `:queue_max`,
  `:queue_overflow`, `:queue_block_timeout_ms`, `:idempotent`,
  `:auto_create_topic`, `:auto_create_partitions`,
  `:auto_create_replication_factor`, `:topic_configs`,
  `:metadata_refresh_timeout_ms`. Six new telemetry events under the
  `[:flezha, :plugin, :kafka, _, _]` prefix surface every state
  transition. Synchronous counters available via
  `Flezha.Plugin.Provider.Kafka.Producer.stats/1`. Full runner-facing
  contract in [`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md).

### Changed (BREAKING)

- `:required_acks` default for the Kafka producer plugin is now `-1`
  (acks=all). Was `1`. Combined with the new default `:idempotent =
  true`, this is the durability-correct default — retries cannot create
  duplicates on the broker, and acks=all stays equivalent to acks=1 in
  steady state on clusters with `min.insync.replicas = 1`. Callers that
  depended on the old fast-but-lossy single-broker-ack latency profile
  should explicitly set `required_acks: 1, idempotent: false` per the
  migration table in [`docs/plugins/kafka_producer.md`](docs/plugins/kafka_producer.md).
- `Flezha.Plugin.Provider.Kafka.Producer.init_provider/1` now returns
  `{:ok, opts, producer_pid}` where `producer_pid` is the
  `ProducerWorker` GenServer (was the kafka_ex worker pid). The
  underlying kafka_ex worker is shared across plugin instances and is no
  longer directly addressable from plugin opts. Plugin host code that
  relied on holding a direct kafka_ex worker pid should use
  `Producer.stats/1` or attach a `:telemetry` handler instead.

- SDKs ship a final state snapshot to the agent on graceful shutdown so
  rolling deploys preserve the latest state. Rust and Java SDKs now emit
  one `RunnerShutdownNotification{operative_id, serialized_state}`
  per running operative on the control channel when the SDK is dropped,
  receives SIGTERM (Kubernetes pre-stop), or is closed explicitly via
  `Flezha.close()` (Java) / handle drop (Rust); the agent's
  `Flezha.Runner.ControlChannel` overwrites
  `Flezha.Operative.Overwatch.state.snapshot` with the supplied binary
  and emits an `RunnerShutdownAck`. Per-operative ack wait is bounded
  at 2 s, parallelised across operatives. SIGKILL bypasses as expected;
  agent falls back to the last periodic snapshot. Disabled-snapshot
  deployments (`overwatch_state_snapshot_interval_ms == 0`, e.g.
  `flezha_market`) get a one-shot shutdown handoff that survives until
  the next `AddOperative` reissue. New protobuf messages
  `RunnerShutdownNotification` (tag 12) and `RunnerShutdownAck`
  (tag 13) are additive — older agents/SDKs interoperate by falling
  back to the periodic-snapshot freshness shipped by
  `heal-connections-on-restart`.
- Runner ↔ agent connections now heal automatically across either-side
  restarts; operatives resume traffic within 5 s of reconnect without
  runner intervention. Stream rebind is enforced as a hard contract on
  `Flezha.Runner.StreamChannel`, the UDS/QUIC acceptors atomically
  bind incoming streams only to nil-ref channels, and
  `Flezha.Runner.FleetManager.runner_connect/3` re-issues
  `AddOperative` for operatives ProcessHub still has tagged to a dead
  runner_id matching the connecting runner's profile. Rust and
  Java SDK `AddOperative` handlers are now idempotent on duplicate
  `operative_id` (log + skip).
- `mix test.connection_healing` opt-in alias that runs
  `test/scripts/connection_healing.sh` against the dev compose stack to
  exercise both restart paths end-to-end. Requires Docker on the host;
  not part of the default `mix test` budget.
- `test/flezha/deployment/k8s_manifest_test.exs` k8s manifest sanity test
  that asserts the daemonset / configmap fields the connection-healing
  flow relies on (`flezha-sockets` volume, `FLEZHA_SOCKET_FILES_PATH`,
  `FLEZHA_RUNNER_KEEPALIVE`) stay well-formed.

## 0.1.0 — Cluster snapshot & restore (replaces DETS recovery)

Restart-survival is now runner-driven via `flz cluster snapshot/restore`.
The previous DETS-based automatic recovery subsystem has been removed
along with the SYSMODE coordinator GenServer that drove it.

### Added

- New top-level CLI commands `flz cluster snapshot <file>` and
  `flz cluster restore <file>` for explicit, runner-driven cluster-wide
  state capture and restore. Both commands accept `--namespace NS` for
  per-namespace procedures; default behaviour without the flag is
  cluster-wide. (Both verbs were later removed; `flz boot
  import-legacy-snapshot` reads the format they produced.)
- `Flezha.Cluster.Snapshot` and `Flezha.Cluster.Snapshot.Coordinator`
  Elixir modules implementing the gather/apply RPC fan-out and the
  atomic ETF file write.
- `Flezha.Cluster.Snapshot.PluginRecoverability` predicate that decides
  which plugins survive a snapshot/restore cycle.
- `Flezha.Operative.Builder` now recognises `:initial_state_binary` in
  opts; `Flezha.Operative.Overwatch` sends a new `EntityApplyState`
  control command to the compute engine before the first input is
  forwarded so a re-spawned operative begins with primed state.
- New protobuf message `EntityApplyState` and corresponding handler
  in the Rust and Java SDKs. Default impl returns the canonical
  `apply_state_not_supported` error so older SDK builds are detectable;
  the agent logs once per operative on apply failure.
- `:flezha, :snapshot_state_cap_bytes` configuration key (default 8 MB),
  with `FLEZHA_SNAPSHOT_STATE_CAP_BYTES` env-var override.

### Removed (BREAKING)

- The entire DETS-based recovery subsystem:
  `Flezha.Recovery.Store`, `Flezha.Recovery.PluginStore`,
  `Flezha.Recovery.Coordinator`, `Flezha.Recovery.SysMode`. The agent no
  longer writes child specs to a DETS file on operative start, no longer
  reads DETS at boot, no longer maintains a SYSMODE state machine, and
  no longer exchanges SYSMODE with peers. Recovery from an unattended
  crash now requires the runner to run `flz cluster restore` against
  a previously-captured snapshot file.
- `Flezha.Operative.InitState.Providers.Sibling` along with its
  registration in `:enabled_plugins`, its capability spec
  (`operative-init-state-sibling`), and the `lib/flezha/operative/init_state/`
  tree. Restart-survival init-state seeding is now handled by
  `flz cluster restore` priming `:initial_state_binary` directly. The
  generic `operative-init-state` provider chain remains for non-snapshot
  sources (HTTP, S3, computed defaults, custom modules implementing
  `on_operative_init/2`).
- Application env keys: `:flezha, :recovery_window_ms`,
  `:flezha, :recovery_data_dir`, `:flezha, :recovery_enabled`,
  `:flezha, :recovery_autostart`, `:flezha, :prefer_explicit_restore?`.
  Env-var overrides removed: `FLEZHA_RECOVERY_*`, `FLEZHA_DATA_DIR`,
  `FLEZHA_PREFER_EXPLICIT_RESTORE`.
- ProcessHub `post_children_start_hook` and `child_unregistered_hook`
  callbacks into the recovery coordinator.
- `failure-recovery` capability and its spec.
- `scripts/recovery_e2e.sh` shell-driven recovery smoke test.
- `docker/prod/docker-compose.local-test.yml` overlay.

### Plugin installer

- `Flezha.Plugin.Installer.Store` no longer co-locates its DETS file
  under the (now-removed) `recovery_data_dir`. New config knob:
  `:flezha, :plugin_installer_data_dir` (defaults to the installer's
  `:plugin_root`, which itself defaults to `app_data/plugins`).
