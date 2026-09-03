# Flezha repo notes

Detailed per-change implementation notes (gotchas, load-bearing decisions, test
maps) live in `docs/dev_notes/<change-id>.md` — one file per OpenSpec change.
Before touching an area listed below, READ its notes file first; the gotchas
there are deliberately not repeated in `docs/`.

Maintenance rule: when a change lands, write its notes to
`docs/dev_notes/<change-id>.md`, add its full abstract to
`docs/dev_notes/INDEX.md` (newest first), and add ONE SHORT hook line to the
index below — one line, one screen width, no wrapping. This file is loaded whole
every session, so the long abstract belongs in INDEX.md, never here.

Language: all files, documents, commit messages and code comments in this
repository are English.

Cross-SDK rule: there is no such thing as a Rust-only SDK change. Any surface
every SDK carries — the authoring/declaration surface, the `HostClassDecl`
keys, the wire protocol, a default (overflow, retry ladder, mailbox, heartbeat
keys), a bench or e2e fixture — changes in Rust, Java AND Elixir in the same
change, or the change names the SDK it defers and the follow-up that owes it.
`flz_core` is shared by Rust and Java (JNI) only; the Elixir SDK is its own
core and inherits NOTHING from it. A perf number measured on one runtime is
proven on one runtime. Normative text and the per-SDK matrix:
`docs/architecture/runner.md` → "One change, every SDK".

## Refactor programme (2026-08-28 →)

Rules: `docs/refactor/charter.md`. Live board: `docs/refactor/STATE.md`.
Resume the supervisor with `/loop /refactor`. Areas run as OpenSpec changes
`refactor-<n>-<area>`, one fresh worker at a time, zero behaviour change.

## Machine hygiene — check every 30–60 minutes

Any session that runs builds, suites or benchmarks **checks the machine at least
every 30–60 minutes** and cleans up what it started. This is a laptop; a leaked
process is not free.

```
ps -eo stat,pid,ppid,etime,cmd | awk '$1 ~ /^Z/'        # zombies
ps -eo etime,pid,cmd | grep -E 'cargo|rustc|criterion|beam.smp|mix |mvn|phpunit|rake|pytest'
ps -eo etime,pid,cmd | grep -E 'until \[|sleep [0-9]'   # polling loops
docker ps                                               # containers left up
```

Kill every background job, polling loop and container **you** started once the
work that needed it is done. If you cannot tell whether a process is yours, ask
before killing it.

**A snapshot lies about benchmark load.** Criterion and the round-trip benches
run in 60-second windows separated by builds and process startup, so `top` at a
random moment often shows an idle box while the fan runs hard. Judge by the
number of runs over the window (`find <bench-log-dir> -mmin -60 | wc -l`) and by
the 15-minute load average, not by one sample.

**Never `docker compose down` here, and never `--remove-orphans`** — the sibling
project `flezha_market` shares the compose project name and its containers would
be destroyed. Stop containers by name.

## Writing an OpenSpec spec delta

`openspec validate --strict` reads a requirement's **first body line** for
`SHALL`/`MUST`. Line-wrapping that pushes it to the second line fails with
"must contain SHALL or MUST" even though the paragraph has it. Put the modal on
the first line, then wrap. This has cost four round trips.

A `## MODIFIED Requirements` block **replaces the whole requirement**: carry
every surviving scenario across, or they are deleted silently. A requirement
TITLE changes only via `## RENAMED Requirements`. Every change needs at least
one delta — a change that genuinely alters no requirement usually means a rule
exists in a script or workflow and is stated in no spec, which is worth writing
down rather than working around.

## Per-change notes index

One hook per change, newest first. Full abstracts:
`docs/dev_notes/INDEX.md`. Read the linked note before touching its area.

- [Python, Ruby and PHP carry the workflow kind — every FFI SDK now does](docs/dev_notes/carry-workflow-in-php.md) — three ports in parallel, three different release gates, and every one checked by RUNNING the ratchet regex rather than by eye
- [Ruby carries the workflow kind](docs/dev_notes/carry-workflow-in-ruby.md) — the release gate is a never-fed `Thread::Queue#pop`, the one park that is scheduler-aware under BOTH executors and holds no timer
- [Python carries the workflow kind](docs/dev_notes/carry-workflow-in-python.md) — all nineteen declarations are BYTE-IDENTICAL to node's, and the gate is an awaited future because this host has TWO executors
- [Node carries the workflow kind](docs/dev_notes/carry-workflow-in-node.md) — four SDK suites were GREEN over a defect that kills the runner at boot, because four tests pinned it (KI-147)
- [Veneers: one output channel per procedure, not per edge](docs/dev_notes/unify-veneer-procedure-output-channel.md) — the merge made `ChannelSpec.encode` AMBIGUOUS and every veneer's edge path then shipped the sink's bytes (KI-144) with every gate green
- [`effectively_once` in the Elixir SDK](docs/dev_notes/carry-effectively-once-in-elixir.md) — one record, a held record, and TWO mechanisms that were off at the same time
- [Late-input correction in the Elixir SDK](docs/dev_notes/carry-correction-window-in-elixir.md) — the marker no test can prove, and the split taken one line early
- [`entity_timers` in the Elixir SDK](docs/dev_notes/carry-entity-timers-in-elixir.md) — the BEAM's timer service IS the wake list, and the wire order is records, arm, outputs
- [Keyed sinks, the idle reaper and schema callbacks in Elixir](docs/dev_notes/carry-keyed-dynamic-and-schema-in-elixir.md) — three re-expressions of the kernel's rules, and the sweep's cap is KI-135
- [`state_persistence` in the Elixir SDK](docs/dev_notes/carry-state-persistence-in-elixir.md) — the kernel's rules re-expressed in a core that inherits NOTHING from `flz_core`
- [`entry_idle` in every host](docs/dev_notes/carry-entry-idle-in-every-host.md) — one latch moved DOWN into `flz_core` so no host re-derives it; the first capability made universal
- [`entry_idle` was two layers and the matrix priced it as one](docs/dev_notes/make-entry-idle-claims-honest.md) — the only capability no `test/system/` module was tagged for, and the gate said so on every PR and passed anyway
- [The required set stops being a chore](docs/dev_notes/require-universal-capabilities.md) — it held six entries while SEVEN capabilities were deferred by no manifest at all
- [The veneer fixture port — R5 S2–S5](docs/dev_notes/port-durable-fixtures-to-veneers.md) — 42 fixture implementations under one change, for Node, Ruby and PHP
- [Identity stamps in the erased veneers](docs/dev_notes/stamp-identity-in-erased-veneers.md) — `flz.entity`/`flz.seq`/`flz.key` on a non-tier keyed plugin emit, then the `keyed_channels` flip
- [`.named()` reached every terminator but the one that ships the frame](docs/dev_notes/honour-named-on-plugin-channels.md) — KI-120, one expression per veneer, and the file says it was an omission TWICE before the bug
- [Three flags where Rust and Java have one seam](docs/dev_notes/imply-checkpoint-granularity-in-veneers.md) — KI-122; three flags could not be right, so one private helper per veneer
- [The partition fan-out reaches every provider, so the allow-list must too](docs/dev_notes/allow-partition-opts-on-every-provider.md) — KI-119; the invariant was already written down, for the OTHER half of the same list
- [The provider table refuses, and a crash IS a refusal](docs/dev_notes/close-provider-value-validation.md) — KI-116 fixed as two mechanisms rather than as its eight sites, `prod_loc` exactly flat
- [The Elixir runner's stdin watcher hangs when its stdout breaks](docs/dev_notes/end-an-orphaned-elixir-runner.md) — KI-134; two lines in `application.ex`, one before the loop and one condition inside it
- [A wedged PHP runner could not die, and won the rebind under a fresh id](docs/dev_notes/end-a-wedged-runner-on-terminal-disconnect.md) — KI-130, the wedge ladder and the graceful ship
- [A Python runner exits 0 after SIGTERM, and the watcher holds no lock](docs/dev_notes/exit-cleanly-after-sigterm-in-python.md) — KI-125; one call in `_watch_stdin_eof`
- [The credit verdict is read from the row the test settled on](docs/dev_notes/harden-credit-flow-verdict-read.md) — KI-128; test-only, and it stops searching the liveness registry afresh after the burst
- [`test/` and `test/apps/` — the async budget governed everything, and it was spent on nothing](docs/dev_notes/refactor-7-tests.md) — the async budget started with ZERO headroom, so the batch order is load-bearing: FOLD FIRST, SPLIT SECOND, at 2 016 test macros before and after
- [Every SDK's test toolchain is pinned, and CI reads the pin](docs/dev_notes/pin-sdk-test-toolchains.md) — **a pin nothing reads stops nothing** — the half every one of the four SDKs failed, and the rule was stated in no spec
- [The Ruby suite's test toolchain is declared, and the pin is the half that matters](docs/dev_notes/pin-ruby-test-dependency.md) — **the pin is what makes the suite run**; the `require` only makes a failure name a library instead of a missing method
- [The poller template stops calling itself a Source](docs/dev_notes/collapse-observability-source-behaviours.md) — <!-- module-naming-record:begin -->`Flezha.Observability.OtpBackend.Source`<!-- module-naming-record:end --> is a POLLER, not a source, and the real behaviour it shadowed was UNDECLARABLE by the compiler
- [A record block exempts its own lines and no others](docs/dev_notes/fix-record-block-latch.md) — `naming_gate.walk()` tested the OPENING marker first and `continue`d, so one inline record pair LATCHED the exemption to end of file
<!-- overflow-naming-record:begin -->
- [The Kafka queue-overflow option joins the policy vocabulary](docs/dev_notes/rename-kafka-queue-overflow.md) — `:queue_overflow: :block` → `:drop_nothing`, a PLAIN rename with no accept-both
<!-- overflow-naming-record:end -->
<!-- kind-naming-record:begin -->
- [The kind gate stops failing on correct prose, and the allowlist stops hiding a real one](docs/dev_notes/close-kind-naming-gate-red.md) — the gate had been RED for the whole programme on correct prose, so three sessions reworded prose to satisfy a regex; the kind sense is now UNMASKABLE
<!-- kind-naming-record:end -->
- [`refactor-5e-b-platform` — fifteen trees, and a struct is an edge](docs/dev_notes/refactor-5e-b-platform.md) — charter **§11h**: A STRUCT PATTERN IS A DEPENDENCY EDGE, and no call-graph tool written this week could see one
- [`refactor-5e-a-runtime` — ten edges cut, and the eleventh refuted by the graph](docs/dev_notes/refactor-5e-a-runtime.md) — charter **§11i**: A MODULE MOVE RE-KEYS ANYTHING SPELLED `__MODULE__`, and the graph is the instrument where a grep is only a hint
- [`refactor-5d-observability` — the instrument tree, measured rather than planned](docs/dev_notes/refactor-5d-observability.md) — the area's whole `Registry → Bus` direction was **one call**, and the metric was the smaller half of removing it
- [`service` — the seam is what cannot move, and five designed splits said otherwise](docs/dev_notes/refactor-5c-service.md) — `lib/flezha/service` went 15 files → 33 and three files over 800 → none for **+25** production lines
- [`lib/flezha/plugin` — four cycles cut by six named edges, and the one ceiling that stays](docs/dev_notes/refactor-5b-plugin.md) — `cycles` **4 → 0**, and the metric alone does not show how: the 55-file cycle fell to `extension/table.ex → service/plugin_manager.ex`
- [`state` — ten modules out of four, and fourteen edges named](docs/dev_notes/refactor-5a-state.md) — a BLOCKING ratchet from this commit, and the headline is the EDGE LEDGER: fourteen cut by name, seven left with reasons
- [`cli` — three parallel mechanisms become one each, and the instrument is measured too](docs/dev_notes/refactor-6-cli.md) — three parallel mechanisms become one each; the honest saving is **−438**, not the raw −513, because 75 lines are a classifier artefact
- [`sdk/java` — every ceiling at zero, and the area costs 310 lines](docs/dev_notes/refactor-4-java-sdk.md) — the four files over 800 went to zero by splitting on the state they mutate, and the area costs **+372** production lines
- [`InputLog`'s fsync companion dies loudly now](docs/dev_notes/close-input-log-syncer-blindness.md) — KI-53's silent fsync, plus the half nobody filed: a bare `receive` with no `after` that parks the log for ever
- [The transport stops discarding, and the sender raises its own gate](docs/dev_notes/close-transport-send-buffer-loss.md) — the gate that removes KI-3's silent drop is written by the DRIVER thread behind an unbounded command queue
- [Tests that lie or leak, and a regression test that lied](docs/dev_notes/green-the-test-hygiene-tail.md) — TWO of the four filed entries did not survive verification, and the row's own runs found two more (KI-66, KI-67)
<!-- module-naming-record:begin -->
- [Seven modules renamed, and the durable-name question settled first](docs/dev_notes/fix-opaque-module-names.md) — **an Elixir module name IS durable state and THIRTEEN modules are pinned by it**
<!-- module-naming-record:end -->
- [The Prometheus label rule](docs/dev_notes/close-prometheus-label-coercion.md) — KI-45 was filed *Not driven* and both halves of that were wrong; the blast radius is FIVE ETS tables
- [The Java diamond delivered 9 payloads for 5](docs/dev_notes/java-diamond-resume.md) — KI-33's diagnosis was refuted on BOTH halves; the mechanism is `EmitContext.collectEdgeOutputs` adding one output PER CONSUMER
- [The suite's log gate finishes, and finds two more](docs/dev_notes/finish-the-suite-log-gate.md) — KI-48 took Shape B in `ControlChannel.send_control/3`, because reordering the child list trades an `[error]` line for a silent drop
- [Four small fixes that share nothing](docs/dev_notes/small-mechanical-fixes.md) — `duplicate_pairs` is a PAIR count over whatever the run scanned, so a recording NAMES its scan and `--check` refuses a foreign baseline
- [The format gate tells the truth](docs/dev_notes/format-gate-tells-the-truth.md) — `format-apply.sh`'s `need` preflight tested EVERY formatter before running ANY
- [Every second Java build in a checkout](docs/dev_notes/java-build-runs-twice.md) — shade REPLACES the jar plugin's output file in place
- [Assertions that cannot fail — `<state_root>/<class>`](docs/dev_notes/assertions-that-cannot-fail.md) — six sites joined `state_root` to a class where `Registry.class_dir/1` is THREE segments (`<root>/<namespace>/<class>`)
- [`process_hub` moves to the hex release](docs/dev_notes/adopt-process-hub-hex-release.md) — 0.7.0 IS the pinned commit's code, but the package's own `mix.exs` raises `elector`, so the lock diff is TWO lines
- [The suite's uncaptured-log gate](docs/dev_notes/green-the-suite-log-gate.md) — the merge gate could never report anything because it was red on EVERY run, and the third line is a production defect (KI-48)
- [The OTLP codec's attribute rule](docs/dev_notes/close-otlp-export-crash.md) — `to_string/1` on every attribute meant OTLP LOG EXPORT HAD NEVER WORKED, and separately shipped `%{l: [1,2,3]}` as raw bytes
- [Mimic's global mode is released by the test that took it](docs/dev_notes/close-mimic-global-leak.md) — 28 modules set `:global` raw against 10 already using `MimicGlobal.set_mimic_global_until_exit/1`
- [Build artefacts leave git](docs/dev_notes/unbundle-committed-build-artifacts.md) — 947 MB off the history (961 MB → 15 MB fresh clone, 163 blobs over 5 MB → 0) and every one of those blobs was in the four purged path groups
- [First input dropped at bind](docs/dev_notes/fix-first-input-drop-on-bind.md) — the defect was a COMMENT: `verdict/2` answered `:ok` on `{:error, :no_stream}` because "the bind delivers it", and nothing ran at bind

- [Watermark overwritten after an effectively-once park](docs/dev_notes/fix-outbox-watermark-overwrite.md) — an entry with NO OUTPUTS completes inside `await_entry/4` (tally `pending: 0`)
- [Rust SDK refactor — `sdk/rust` + `flezha-macros`](docs/dev_notes/refactor-3-rust-sdk.md) — `sdk/rust/tests/ui/*.rs` must NEVER be rustfmt-formatted — their line numbers are pinned by the `.stderr` snapshots
- [FFI veneers refactor — `flz_core_ffi` + Node/Python/Ruby/PHP](docs/dev_notes/refactor-2-ffi-veneers.md) — `veneer/mod.rs` calls the `extern "C"` entry point and never the Rust behind it
- [Kernel refactor — `flz_core`, `flz_transport`, `flz_nif`](docs/dev_notes/refactor-1-kernel.md) — one `Error` per crate whose `Display` IS the old string, because the controller matches token prefixes and the veneers assert substrings
- [Dispatcher-seam A/B spike](docs/dev_notes/refactor-1s-dispatcher-spike.md) — verdict CONVERGE (rows 1–5 inside their floors, allocs/elem flat), nothing merged: the seam lives on branch `spike/1s-dispatcher` (`f480f89e`) and area 1 re-implements it
- [Test-suite refactor](docs/dev_notes/refactor-0-tests.md) — the e2e flake population after the sleeps were gone was ONE race: the primary data stream binds after the start ack
- [Refactor baseline, gates and formatters](docs/dev_notes/refactor-0-baseline.md) — `scripts/refactor_metrics.py` counts a Rust file as test from the `#[cfg(test)]` that opens a `mod` to EOF
- [Python SDK](docs/dev_notes/python-sdk.md) — `#[pyclass(unsendable)]` pins the context to its creating thread and panics on a call from any other
- [PHP SDK](docs/dev_notes/php-sdk.md) — `ffi.enable=preload` is REFUSED inside a `Swoole\Thread` (it permits FFI from the CLI SAPI and a thread is a fresh interpreter that is not it)
- [`flz reset` resolves the socket directory](docs/dev_notes/fix-reset-socket-dir-resolution.md) — `Role::Controller` is right not for the design's stated reason (no ambiguity refusal) but because `first_creatable` STEPS OVER a candidate a peer is listening in
- [Ruby SDK](docs/dev_notes/ruby-sdk.md) — bindgen needs clang's BUILTIN headers (`libclang-common-N-dev`)
- [Several controllers on one machine](docs/dev_notes/support-multi-controller-colocation.md) — the socket probe is REUSED from `improve-runner-controller-discovery` (one NIF over `path_helper::probe`) and `Reach` stays untouched
- [Runner ↔ controller discovery](docs/dev_notes/improve-runner-controller-discovery.md) — `System.put_env/2` NEVER reaches the NIF, so `resolve_connect/3` takes the CALLER's reading of both variables
- [Bench report + signal honesty](docs/dev_notes/improve-bench-report-and-signals.md) — the two driver-global stall checks had NO fed precondition, so a healthy fleet satisfied both clauses VACUOUSLY
- [Node SDK](docs/dev_notes/node-sdk.md) — the spike held, but napi-rs's own async support is the WRONG tool and that is MEASURED — the veneer owns its executor
- [Non-blocking DropNothing backpressure](docs/dev_notes/nonblocking-drop-nothing-backpressure.md) — a `DropNothing` push into a full consumer parked the CALLING HOST THREAD and `install_inline_serve` was a Rust `fn` pointer that could not cross the C ABI
- [Overflow silent failures](docs/dev_notes/close-overflow-silent-failures.md) — `promote_due` runs INSIDE `poll` and pushed under the no-drop policy, so a full mailbox parked the poller and HID that it had
<!-- overflow-naming-record:begin -->
- [The overflow policy names its guarantee](docs/dev_notes/rename-block-to-drop-nothing.md) — `OverflowPolicy::Block` → `DropNothing` on every SDK
<!-- overflow-naming-record:end -->
- [Subscription sub-option types](docs/dev_notes/fix-extension-subopt-typing.md) — the proposal put the pre-write check in `enable_claimed/2`, one step TOO LATE — canonicalisation runs at the head of `enable_plugin/2`
- [Cross-SDK placement guard](docs/dev_notes/guard-cross-sdk-placement.md) — the proposal was STALE; the real hole was `HostClassDecl.sdk_lang` defaulting a veneer to RUST's byte
- [Persistence smoke check runs again](docs/dev_notes/restore-state-persistence-smoke.md) — `--Xoperative_id`/`--Xposition` were NEVER provider options: `--X…` reaches `init_provider/1` and a `@known_opts` provider refuses what it does not own
- [Silent plugin wedges](docs/dev_notes/close-silent-plugin-wedges.md) — a failed `activate/1` was invisible forever: `:prepared` reads healthy
- [SignalHandler owns SIGTERM](docs/dev_notes/fix-signal-handler-boot-race.md) — the "boot-order race" did not exist: `sigint` is NOT a settable signal (ERTS reserves it) and the original probe only ever tested sigterm
- [Runner dispatch wedges](docs/dev_notes/close-runner-dispatch-wedges.md) — three silent wedges closed; `rehand_unprocessed` RENAMED to `abandon`, and the core reports rather than reclaiming on a timer

- [Schema evolution](docs/dev_notes/state-evolution.md) — a bumped `SCHEMA_VERSION` protected NOTHING on the snapshot — v1 bytes restored transposed and silent
- [Unprivileged, signal-correct harness boot](docs/dev_notes/improve-controller-startup-time.md) — `exec su -m` reaps its child ~2s after SIGTERM
- [Cross-SDK conformance suite](docs/dev_notes/sdk-conformance-suite.md) — the runtime is an axis of the RUN (`FLEZHA_E2E_RUNTIME`), never of a file
- [Cross-SDK perf parity](docs/dev_notes/cross-sdk-perf-parity.md) — the campaign was controller + shared-kernel work, so nothing needed porting
- [Open-source licensing](docs/dev_notes/open-source-licensing.md) — MPL-2.0 controller + Apache-2.0 SDK side
- [Workflow durable-path throughput](docs/dev_notes/workflow-durable-path-throughput.md) — a batched append is a deferred reply and the token is the `send_request` id
- [Workflow performance campaign](docs/dev_notes/workflow-perf-campaign.md) — `mix bench.workflow`; a ping TIMEOUT never resets the live control channel (last attempt or a dead-transport error only)
- [Operative startup latency](docs/dev_notes/operative-startup-latency.md) — `-p 4` bring-up 365→30 ms; 1000 operatives 9.1 s→1.2 s and their 2000 plugins 57 s→0.24 s
- [Elixir SDK + native BEAM transport](docs/dev_notes/elixir-sdk-native-transport.md) — ONE Elixir SDK core, TWO deployment modes (in-node `:native` message-passing + standalone UDS/QUIC binary protocol, zero controller changes)
- [A window of Produce requests per partition](docs/dev_notes/kafka-producer-in-flight-window.md) — a broker answers ONE connection in order and rejects a sequence gap: those two facts are the whole design
- [Amortised effectively-once output confirmation](docs/dev_notes/amortise-effectively-once-output-confirmation.md) — the reports are batched, never the facts — one cast, one settle, one watermark advance per window
- [Nothing drops by default](docs/dev_notes/close-silent-data-loss-defaults.md) — `Block` is the default for every channel kind in BOTH SDKs and the FFI host, keep them in lockstep
- [Async effectively-once ingest](docs/dev_notes/async-effectively-once-ingest.md) — the host never waits on the input log
- [Durable-path throughput](docs/dev_notes/durable-path-throughput.md) — a confirmation refills the outbox window
- [Delivery confirmation gates the outbox cursor](docs/dev_notes/sink-acknowledged-delivery.md) — a cast is not delivery; the cursor moves on the sink's confirm/park
- [Durability posture is a decision](docs/dev_notes/durability-tier-default-on.md) — prod refuses to boot with no `FLEZHA_DURABILITY_BACKEND`
- [Durable operator intent](docs/dev_notes/durable-operator-intent.md) — intent = the hub declared set (pairs with process_hub `declared-children-and-remote-manifest`)
- [Wire-frame limits](docs/dev_notes/wire-frame-limits.md) — reject a declared length BEFORE allocating, pre-auth
- [Authn/authz finished end to end](docs/dev_notes/finish-platform-authn-authz.md) — three-way enforcement posture; identity ONLY from the transport
- [Tiered procedure authoring](docs/dev_notes/add-tiered-procedure-authoring.md) — three tiers, explicit wins; `emits` is the ONLY source of the ctx-shape output type
- [`route_latest` building block](docs/dev_notes/add-route-latest-building-block.md) — a document reaches each keyed Entity on that key's OWN traffic, never a remembered-set fan-out
- [One plugin-sink method: `sink` + `sealed`](docs/dev_notes/unify-plugin-sink-api.md) — terminality is DERIVED from the wiring
- [Operative kinds are `stream` and `workflow`](docs/dev_notes/rename-operative-kinds-to-stream-and-workflow.md) — three senses of the old kind word and only one renamed
- [Websocket subscription source](docs/dev_notes/add-websocket-subscription-source.md) — delta frames on the already-open socket
- [Per-Entity timers](docs/dev_notes/entity-timers.md) — timers are FOLDED STATE (Change→WAL→replay)
- [Live Entity read](docs/dev_notes/entity-state-live-query.md) — `flz state get`; a read is NOT a compaction (no drain, seq stamped in metadata)
- [Stream transforms + edge keying](docs/dev_notes/stream-dsl-keyby-and-transforms.md) — transforms fuse into ONE closure per edge at the routing seam
- [Multi-input procedures](docs/dev_notes/add-multi-input-procedures.md) — merge maps at the ROUTING seam so replay determinism is free
- [Procedure-requested spawn](docs/dev_notes/add-procedure-requested-spawn.md) — `ctx.spawn_workflow`; namespace comes from the ControlChannel, never the wire
- [Operative-to-operative messaging](docs/dev_notes/operative-to-operative-messaging.md) — the route existed, only a VERDICT was missing
- [Runner entry point + dynamic type set](docs/dev_notes/sdk-runner-entrypoint-redesign.md) — runner-as-handle, slot claimed at `start()`
- [Java SDK parity](docs/dev_notes/java-sdk-parity.md) — the unblocker was `AddOperativeResponse.metadata`
- [Checkpoint ack-granularity](docs/dev_notes/effectively-once-checkpoint-granularity.md) — producer-side only, interval replay IS crash-resume replay
- [Stream late-input correction](docs/dev_notes/stream-late-input-correction.md) — THERE IS NO SPLICE: rewound baseline + event-time-sorted slice on the existing replay
- [Workflow loop rewind](docs/dev_notes/workflow-loop-rewind.md) — `rewound` marker, not truncation
- [Workflow entity retry](docs/dev_notes/workflow-entity-retry.md) — strictly pre-commit retry ladder reusing the stream backoff
- [Workflow durable loop + suspension](docs/dev_notes/workflow-durable-loop-and-suspension.md) — loop modes Once/PerInput/Chained
- [Workflow staged commit + outbox](docs/dev_notes/workflow-staged-commit-and-outbox.md) — `ctx.emit` never hits the wire inside a structured proc
- [Workflow proc-journal](docs/dev_notes/workflow-proc-journal.md) — per-instance `proc.journal`, journal-then-ack `ProcCommit`
- [Dead-letter queue](docs/dev_notes/platform-dead-letter-queue.md) — retry-then-park; tier-on parks ride the effect record atomically
- [Postgres sink plugin](docs/dev_notes/effectively-once-output-postgres-sink.md) — one transaction per batch, `(epoch, seq)` guard makes re-drives no-ops
- [Deterministic effects + output identity](docs/dev_notes/deterministic-effects-and-output-identity.md) — `ctx.now()`/`ctx.stable_id()` contract
- [Snapshot compaction is now real](docs/dev_notes/entity-compaction-is-a-stub.md) — both SDKs answered `CompactEntity` with a stub
- [Per-Entity state persistence](docs/dev_notes/operative-state-persistence.md) — WAL layout, wire payload-ids, dynamic entity mode + destructive eviction
- [Last-runner migration keeps its state](docs/dev_notes/fix-last-runner-migration-state-loss.md) — runner ownership is NODE-LOCAL
- [Auto-restore on boot](docs/dev_notes/auto-restore-on-boot.md) — recovery is an ORPHAN RECONCILE, no marker
- [Convergence self-heal loop](docs/dev_notes/consume-recovery-mode-and-self-heal.md) — now `Convergence.Loop` (Intent→Probe→Diff→Planner→Heal primitives)
- [Data-plane liveness instrumentation](docs/dev_notes/data-plane-liveness-instrumentation.md) — NIF driver self-report + BEAM counters + stall classifier (deepest-wedged-layer-wins)
- [Graceful shutdown](docs/dev_notes/controller-graceful-shutdown.md) — quiesce → drain → finalise; orchestrator grace period MUST be `timeout + 15s`
- [Observability foundation](docs/dev_notes/platform-observability-foundation.md) — Signal envelope + bus + sinks + bridges
- [OTP backend sources](docs/dev_notes/platform-observability-otp-backend.md) — beam_vm/host/processhub/transport pollers
- [Observability export](docs/dev_notes/platform-observability-export.md) — hand-rolled Prometheus listener + OTLP/HTTP via `:mint`
- [Durable audit log](docs/dev_notes/platform-audit-log.md) — JSONL + INDEX, fsync per record by default
- [Platform authn/authz foundation](docs/dev_notes/platform-authn-authz.md) — roles/tokens/quotas/bootstrap; policy point `AuthZ.authorize/3`
