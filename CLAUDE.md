# Flezha repo notes

## `route_latest` building block (`add-route-latest-building-block`)

- **`c.route_latest(documents, traffic, key_fn)` / `route_latest_named(.., name)`
  (Rust) — `c.routeLatest(Doc.class, documents, traffic, keyFn[, name])`
  (Java)** — an operative-wide document reaches every keyed Entity, one key at a
  time, on that key's OWN next traffic item. Returns `Stream<(String, D)>`
  (Rust) / `Stream<RoutedDocument<D>>` (Java — no tuples, and the runtime needs
  a `Class` token for the routed payload); the consumer keys by that key.
- **Why it exists at all**: a keyed procedure cannot receive a broadcast — every
  keying declaration is total, and in a partitioned member the computed key is
  admitted against the member's key-group range.
- **THE load-bearing rule — deliver on the key's own traffic, never a
  remembered-set fan-out.** A remembered key set outlives key-group ownership
  (a rescale, or ONE out-of-range item seen during a reconcile window), so
  fanning out over it parks later deliveries against the partition guard.
  Deliver-on-own-traffic makes over-delivery impossible by construction. Price:
  one tick of latency per key, and a key whose traffic stopped is never
  delivered to. That is also why there is no retained broadcast to late-born
  Entities and no rescale-time replay to specify.
- **Sugar, not a runtime feature**: a `merge` into ONE packaged single-entity
  procedure with `{document, version, delivered}` journaled state. No wire
  change, no new replay path. The Rust packaged types are `pub(crate)` — the two
  methods are the whole surface, which is what keeps the guarantee
  un-bypassable; Java has no crate privacy and the runtime instantiates a
  Procedure reflectively from a public class, so `LatestDocumentRouter` is
  public and says so in its javadoc.
- **GOTCHA (Java, the real blocker) — the state codec was built from a freshly
  instantiated Procedure, so the wiring could not register anything.**
  `ActivationDispatcher.JavaInstance` now builds `KryoCodec` from
  `desc.kryoTypes()` (the ProcedureDescriptor's list, which was DEAD — copied
  from `probe.kryoTypes()` at registration and never read) instead of
  `instance.kryoTypes()`. Identical for every existing procedure (both are a
  no-arg instance's constant list); it is what lets `routeLatest` append the
  document's own class, which only the wiring knows.
- **GOTCHA — `ProcedureTestHarness.replayEquals()` builds its codec from
  `procedure.kryoTypes()` alone** (it has no topology), so a Java harness test
  of the router must use a document type Kryo registers by default —
  `RouteLatestRouterTest` uses opaque `String` documents. A typed document is
  covered by `RouteLatestExpansionTest`, which asserts the registration.
- **LIMIT (Java only, documented, not fixed)**: a HOT-SWAP migration moves state
  as JSON, and the retained document is an `Object` field, so a typed document
  comes back as a generic map. Crash-resume, compaction and rebind all restore
  through Kryo and are unaffected.
- **GOTCHA — two routers over one traffic stream is refused, not silently
  merged.** Unnamed mentions of one class are ONE node, so the second
  declaration re-wires the traffic edge and trips `EdgeAlreadyWired` /
  `edgeAlreadyWired` naming `route_latest`. Name them. One channel used as BOTH
  streams trips `DuplicateMergeSource`.
- **Reuse**: `Stream::map` is now the owned-clone spelling of the new
  `map_ref(what, f)` (one closure-erasure site); `Merge::wire` /
  `MergeImpl.wire` became the shared named/unnamed merge-closing seam both
  `into` spellings and the block use. The key function is an EDGE transform, so
  the traffic payload is replaced by its key and never reaches the router.
- **Tests**: `sdk/rust/src/channel/route_latest/tests.rs` (unit, beside the
  private types: 5 spec scenarios + `replay_equals`/`replay_emits_equal` +
  expansion + both refusals), Java `RouteLatestRouterTest` (delivery contract)
  and `RouteLatestExpansionTest` (topology, kryo registration, refusals).
- **Docs**: `docs/sdk_authoring_contract.md` §4.1, both READMEs, root +
  `sdk/rust/CHANGELOG.md`, `sdk/rust/public-api.txt`.

## One plugin-sink method: `sink` + `sealed` (`unify-plugin-sink-api`)

- **`sink(name)` / `sink_with(name, f)` (Rust) — `sink(name[, serializer])`
  (Java)** is the ONE egress method, always returning the stream handle.
  `output` / `also_output` / `also_output_with` / `alsoOutput` are DELETED, no
  shim. `.sealed()` is the optional terminator returning the non-chainable
  `Channel<T>`.
- **Terminality is DERIVED, not declared.** `build_topology` (Rust) /
  `StreamOperativeTopologyBuilder.build` (Java) sets
  `kind = PluginBound` when a channel has a sink and NO consumers, else leaves
  it `InterProcedure`. That is what lets one method serve both shapes, and it
  fixed a latent hole: `s.output("x"); s.into::<B>();` froze the kind first, so
  B silently received nothing — the same pair is hybrid now.
- **`ChannelDescriptor` lost `plugin_name` (never read by anyone) and
  `plugin_serializer`.** `plugin_sink` is the ONE representation of egress in
  both SDKs; the terminal and hybrid emit paths are now literally one function
  (`EntityEmitSink::emit`; Java `doEmit`'s single non-ENTRY arm), so their
  frames cannot drift.
- **The `EmitSink` trait collapsed to ONE `emit(value, meta, key)`** —
  `emit_plugin` / `emit_inter_procedure{,_with_meta}` / `is_plugin_bound` are
  gone (5 impls updated). `has_plugin_sink()` survives only because `route`
  needs it for the metadata refusal, and `route` asks ONLY when metadata is
  present, so an ordinary emit pays no topology lookup.
  `WorkflowCollectAdapter` keeps a PRIVATE `is_plugin_bound` — the one place
  the shape still matters: a plugin-bound channel must not `collect` into the
  next stage's handoff batch.
- **GOTCHA — `sealed()` must NOT touch the builder.** An early version called
  `with_channel`, which runs `ensure_output_channel`, so sealing a bare leaf
  node CREATED the phantom output channel the lazy creation exists to avoid.
  `Channel<T>` is now a zero-state marker in both SDKs (Rust lost `pub id`,
  Java lost `id()` / `payloadType()`), so "sealing changes no topology" is true
  by construction. Pinned by `sealing_a_leaf_declares_no_channel` /
  `sealingALeafDeclaresNoChannel`.
- **GOTCHA — `.on_full(...)` after `sink(...)` now COMPILES** (it could not
  after `output`, which returned `Channel<T>`). It is still never consulted on
  a channel with no mailbox; `.sealed()` is what refuses it, and the trybuild
  fixture moved accordingly (`sealed_disallows_on_full.rs`).
- **GOTCHA — the trybuild wiring refusal is E0107, not "method not found".**
  `Channel<T>` picks up `Into::into` from the std blanket impl, so
  `.sealed().into::<Next>()` complains about the turbofish instead. Documented
  in `sealed_disallows_wiring.rs`; the clean E0599 shape is in the sibling.
- **GOTCHA — `tests/ui/*.rs` are NOT formatted by `cargo fmt`.** They are
  trybuild data files, unreachable from any target, so they keep the compact
  house style. Running rustfmt on them by hand renumbers every line and
  invalidates the `.stderr` snapshots.
- **KNOWN, NOT FIXED — the `sink(name)` argument is inert at runtime in BOTH
  SDKs.** The frame's routing key is the CHANNEL name (`channel: String`
  metadata, from `descriptor.name`); `PluginSink::name` / `pluginSink().name()`
  is never read on either emit path. `.named("out").sink("out")` is the common
  idiom for that reason. Pre-existing (the removed `output(name)` set the
  equally-unread `plugin_name`), and the spec for this change keeps the
  parameter — but it is the obvious next cleanup.
- **Tests**: `sdk/rust/tests/hybrid_channel.rs` gained
  `terminality_is_derived_from_the_wiring_and_sealed_changes_nothing`,
  `terminal_and_hybrid_sink_frames_match_the_pre_rename_fixture` (the
  wire fixture: body + `{"channel": name}` and nothing else) and
  `sealing_a_leaf_declares_no_channel`; Java mirrors the first and last in
  `StreamOperativeTopologyBuilderTest$GraphAuthoring`, and
  `OldApiNotImportableTest` gained in-process javac cases for the removed
  spellings — with a POSITIVE CONTROL, because a classpath that resolved
  nothing would make every refusal vacuously true.
- **Docs**: `docs/sdk_naming_conventions.md` §9 (rule + migration table),
  `docs/operative_architecture.md` "Plugin egress: `sink`, hybrid and terminal"
  (renamed section — `docs/sdk_protocol_spec.md` links it by name), both
  READMEs, `sdk/rust/public-api.txt`, root + `sdk/rust/CHANGELOG.md`.

## Operative kinds are `stream` and `workflow` (`rename-operative-kinds-to-stream-and-workflow`)

<!-- kind-naming-record:begin -->
- **`:unbounded` → `:stream`, `:bounded` → `:workflow`**, everywhere a developer
  reads: kind atoms and wire literals, SDK types in both SDKs, protobuf message
  names (tags 22/23/34/35 unchanged), CLI `--kind`, env vars, module/file/spec
  directory names, comments and docs. BREAKING, no shim. Behaviour, durability
  and every wire tag number are untouched — only names.
- **GOTCHA — three senses of the word, and only one renamed.** *Kind* (renamed),
  *capacity* (`BoundedMailboxSource`, `:flezha_bounded_mailbox`, "backpressure
  and bounded mailboxes" — untouched) and *plain English* ("bounded by",
  "a bounded (1024) FIFO", "unbounded growth" — untouched). A repo-wide
  `s/bounded/workflow/` corrupts ~65 English uses and the mailbox subsystem, and
  it still **compiles** — that is why `scripts/kind-naming-allowlist.txt` was
  built BEFORE any edit and is the input to both the sweep and the gate.
- **GOTCHA — the binding family shares no substring.** `bound`, `unbound`,
  `bind`, `rebind`, `misbound`, `inbound`, `bound_runner`,
  `require_bound_principal` mean *attached to a runner or namespace*;
  `unbound` ("not placed") is unrelated to `unbounded`. Widening any pattern
  from `bounded` to `bound` corrupts ~210 sites.
- **GOTCHA — `crossbeam::channel::unbounded` / `bounded` are third-party
  capacity APIs**, not kinds. They are allowlisted; renaming them is the one
  corruption that *does* fail the build (it did, once, in `flz_transport`).
- **GOTCHA — the English sense also appears as IDENTIFIERS, and those renames
  compile.** `Convergence.SafeCall.bounded/3` is a *time-limited* `GenServer.
  call`, not a kind; the sweep turned it into `SafeCall.workflow/3` across nine
  files and nothing failed. Same class of silent damage: "bounded-retry
  governor" (Overwatch's start ladder), "bounded ack-wait", "bounded outbound
  queue" (Kafka), "**Bounded capacity**" (`mailbox.rs`), "bounded park"
  (`routing.rs`), "bounded quota", "bounded margin", "bounded primitives". A
  phrase allowlist is NOT enough — after the sweep, audit by grouping every
  renamed occurrence by its neighbouring word and reading the groups whose
  neighbour is not an operative noun. That audit is what found all of them.
- **`scripts/check-kind-naming.sh` is the permanent gate** (CI workflow
  `naming.yml`, path-unfiltered, toolchain-free). It matches any identifier
  *containing* `bounded` — not just the bare word, so `BoundedOperative` and
  `spawn_bounded` are caught — masks each allowlisted phrase out of the line,
  and fails on the residue. Scope is `git ls-files`, so `openspec/changes/`
  (which holds the archive), `target/` and `_build/` are excluded by being
  ignored; `dist/` and binaries are excluded explicitly.
- **The old words are KEPT where they are true**, as adjectives for the run:
  a workflow operative runs a *bounded* iteration to a terminal state; a stream
  operative folds an *unbounded* input forever. `docs/glossary.md` carries that,
  the `PerInput` caveat (its input is unbounded; only its iteration is bounded —
  the fact that makes the old name false rather than merely unfamiliar) and the
  one-sentence rationale, so the question is settled rather than reopened.
- **GOTCHA — the archive keeps the old ids.** `openspec/changes/archive/**` is a
  historical record and is out of scope, so the change directories are still
  `bounded-loop-rewind`, `bounded-proc-journal`, `bounded-entity-retry`, … even
  though the section headings here and the live `openspec/specs/` capabilities
  now say `workflow-*`. Grep the archive with the old name.
- **UPGRADE — `flz controller stop && flz reset --yes` is mandatory.** The kind
  atom is persisted in the ProcessHub DETS cspec AND in `ClassRegistry`'s
  on-disk sidecar; nothing reads the retired atoms. Controller and every runner
  redeploy together. `flezha_market` ConfigMaps referencing `FLEZHA_BOUNDED_*`
  need the rename in that repo before its next deploy.
- **GOTCHA — trybuild `.stderr` fixtures moved for a non-obvious reason.** Five
  UI cases now print `flezha_runner::Stream` where they printed `Stream`;
  renaming the sibling module `bounded` → `workflow` changed which path rustc
  picks as shortest-visible. Semantically identical, blessed with
  `TRYBUILD=overwrite`.
- **GOTCHA — `EntityIdTest` pins byte OFFSETS, not just content.** Its fixture
  name shortened by 3 bytes (`MarketUnboundedOperative` → `MarketStreamOperative`)
  and every hard-coded index in the framing assertion had to move with it.
- **Rebuilt artifacts**: `scripts/build-dist.sh` after `mvn clean` in
  `sdk/java` — the committed `sdk/java/flezha-sdk/dist/`,
  `test/apps/java_runner/dist/` and the five `flezha-sdk-native` binaries embed
  the api classes, and the fat jar shades stale `target/classes` first.
<!-- kind-naming-record:end -->

## Websocket subscription source (`add-websocket-subscription-source`)

- **`subscription_url` + `subscribe_template` / `unsubscribe_template` on
  `Plugin.Provider.Websocket`** — poll a URL publishing
  `{"version": <int>, "items": [<string>]}`, diff it against the applied set,
  send delta frames on the socket ALREADY OPEN. Controller-only; no SDK change,
  no wire change, no new control message.
- **`{{items}}` / `{{req_id}}` are the whole protocol surface.** Item strings are
  opaque — never parsed. That is what keeps a market-data requirement from
  becoming market-data code in the framework.
- **Commit is ack-gated and retry is CONVERGENCE, not retry state.** A delta
  folds into `applied` only when an inbound JSON object echoes its `req_id` and
  does not carry `"error"` / `"success": false`. Failed/unanswered → left
  uncommitted → next cycle re-derives an identical delta under a FRESH req_id.
  `expire_stale_inflight` drops the previous cycle's ids so a very late ack
  cannot commit a set the source moved on from. `subscription_ack_mode: "none"`
  commits on send for protocols with no correlation id.
- **GOTCHA — a re-assertion carries the reserved `req_id` 0.** Connect and the
  data-liveness re-subscribe re-send what is ALREADY applied; the in-flight
  space starts at 1, so their acks can never be mistaken for a pending delta's.
- **GOTCHA — `handle_connect/3` fires on EVERY reconnect.**
  `schedule_subscription_poll/1` cancels the previous timer before arming, else
  each reconnect leaves another poll chain running and the source is polled
  twice, then four times. Pinned by "reconnecting does not leave a second poll
  chain armed". (`schedule_idle_check/1` has the same latent shape —
  pre-existing, NOT fixed here.)
- **A failed poll is NO INFORMATION, never an empty desired set** — it can never
  unsubscribe. An explicitly empty `items` array IS a real instruction and does.
- **FIXED IN PASSING (correctness edge predating the feature):** the
  data-liveness watchdog resent `post_connect`, the BOOT-TIME payload, so any
  runtime change would have been silently reverted by the mechanism meant to
  heal a stalled subscription. `subscription_frames/1` is now the ONE answer to
  "what is this socket subscribed to", read by connect and both watchdog tiers.
- **Durability rides the EXISTING cspec path.** `persist_applied` casts
  `{:plugin_opt_persist, plugin_id, key, value}` to the host Overwatch, which
  updates BOTH `Table.put_plugin_opt/4` (what `attempt_restart/3` re-runs
  `init_provider/1` from — plugin restart / handover) AND the cspec via
  `PluginManager.persist_plugin_opt/5` → `RegistryUpdate.put_plugin_opt/3`
  (controller restart). The cspec write runs in a `Task` — it is a cluster-wide
  registry round-trip with its own retry ladder and must not stall the operative.
- **GOTCHA — `Table.put_plugin_opt/4` must update `subs` entries too.**
  `build_entry/3` COPIES opts into `provider_opts`, so updating only
  `table.plugins` leaves state that survives a restart but not a dispatch.
- **`with_host_identity/2` now also merges `:source_plugin_id`** (the provider
  needs to name its own cspec row). Adding a host key BROKE `http_poll`'s
  `reject_unknown` — which is why the host list now lives once, in
  `ProviderBehaviour.host_opts/0`.
- **Strict options are GENERATED now.** A provider declaring `@known_opts` gets
  both halves from `use ProviderBehaviour`'s `__before_compile__`: the `build/1`
  refusal AND `validate_opts/1`. Both are needed for different reasons —
  `Plugin.SpecDecoder` hands a `--plugins` spec straight to `init_provider/1`
  without ever building the struct. Three providers (`http_poll`, `websocket`,
  `operative_forward`) had hand-rolled copies; `operative_forward` still has no
  `init_provider` check wired (latent, documented, NOT changed here).
- **`Flezha.Plugin.Provider.HttpFetch`** is the ONE HTTP GET (extracted from
  `PollWorker`): streamed body cancelled at a cap, conditional GET on the ETag,
  verified TLS, `autoredirect: false` (a bearer token must not be replayed to
  whatever a redirect names). Also owns `validate_url/4` / `validate_headers/3`.
- **`Websocket.Subscription`** is the policy half — struct, `from_opts/1` (every
  refusal), `render/3`, `acknowledged?/1`, `desired_set/1`, `fetch_request/2` —
  with NO knowledge of Fresh. `Websocket` owns the loop and its timers.
- **Observability rides a new optional `inspect_state/1` provider callback**, so
  `flz operative info` needs no per-provider knowledge. NOT `flz plugin info` —
  that is the installed-`.fpz` surface and has no operative to report against.
- **GOTCHA — a fake WS server for tests: Mint's GUID is
  `258EAFA5-E914-47DA-95CA-C5AB0DC85B11`.** Mistyping it fails the upgrade with
  `:invalid_nonce`, which reads like a framing bug and is not one.
- **Tests**: `websocket_subscription_source_test.exs` (50 cases: refusals,
  in-place deltas, ack gating, durability, connect/watchdog convergence — all
  driven at the `handle_info/2` settled-poll seam),
  `websocket_reconcile_live_test.exs` (a REAL `:gen_tcp` RFC 6455 server + HTTP
  origin; asserts exactly ONE TCP connection accepted across an add and a
  remove — that is what pins "no reconnect"). Six mutations were run against the
  guards to confirm the tests bite.
- **Docs**: `docs/websocket_subscription_source.md`, `docs/glossary.md`,
  `docs/plugin_lifecycle.md`, root `CHANGELOG.md`.

## Per-Entity timers (`entity-timers`)

- **`ctx.timer_at_event_time(ts)` / `ctx.timer_after(d)` + the two cancels
  (Rust) — `timerAtEventTime` / `timerAfter` / `cancelEventTimeTimer` /
  `cancelProcessingTimeTimer` (Java)**, handled by `Procedure::on_timer` /
  `Procedure.onTimer`. Registration returns the ABSOLUTE scheduled timestamp;
  that value IS the cancellation handle. Pending timers are a SET, so a
  duplicate registration is a no-op that produces no second durable op.
- **THREE REUSES carry the whole feature**, which is why it needs no new store,
  no new control message and (almost) no controller change:
  (1) **the Change → WAL → replay fold** makes the pending set durable —
  registration / cancellation / firing are records in the Entity's OWN change
  log, so crash-resume, replay and retention are free;
  (2) **the per-Entity event-time high-water** (from
  `stream-late-input-correction`) is the event-time trigger — no scheduler,
  no watermark propagation;
  (3) **the nil-offset effect record** (`effectively-once-checkpoint-granularity`
  made `consumed_offset: Option<u64>` first-class) carries a firing, because a
  firing consumes no input offset and every controller reader already tolerates
  `nil`.
- **Two record kinds, both stored VERBATIM by the controller.** Tier-off:
  `FORMAT_ID_TIMER_OPS = 3` under the EXISTING `entity_state_change`
  payload-id, body a postcard `Vec<TimerOp>` — the controller stores, truncates
  and re-ships it exactly like a change record and never decodes it. Tier-on:
  `EffectRecordBody.timers` (new trailing field), so a firing's Changes,
  outputs and the fired timer's removal are ONE record. That field is the ONLY
  Elixir change: `Wire.decode_effect_body/1` had a strict trailing-byte check,
  so it and `encode_effect_body/1` learned `timers` (+ the `wire_test.exs`
  fixture, shared with `flusher.rs` and `EffectRecordTest`).
- **`fired_at` is the SCHEDULED timestamp and `ctx.now()` EQUALS it inside
  `on_timer`.** A chained `timer_after` therefore schedules off the timer that
  woke it, and a replay observes the same value. `timer_after(d)` anchors on
  `ctx.now()`, not an ambient read — that is what makes "replay registers the
  same timers" true.
- **GOTCHA — a quiet Entity NEVER fires its event-time timers.** Nothing
  advances its clock. Inherent to event time (Flink behaves identically with an
  idle source); the processing-time kind is the answer to "nothing has
  arrived", and it is what a session timeout / idle flush / join expiry wants.
- **Event-time firing rides the dispatch path; processing-time rides the
  MAILBOX.** One `TimerWheel` per Operative (lazily created — an Operative that
  never registers one never pays for a thread) keyed
  `(procedure, entity_key, scheduled_at)`; on expiry it PUSHES onto the target
  Entity's existing mailbox rather than invoking `on_timer`, so the
  single-active-drainer gate that already serializes inputs serializes firings
  too. No second lock. Both SDKs.
- **GOTCHA — `admit_and_track` had to start advancing the high-water for EVERY
  event-time-clocked class**, not only a correcting one: it was gated on
  `correction.is_some()`, and event-time timers fire against that mark. It is
  now gated on `<S::Clock>::IS_EVENT_TIME` instead, which is also strictly more
  correct (a wall/logical value must never be folded into an event-time mark).
- **GOTCHA — compaction would have silently eaten the pending set.** A snapshot
  covers `S::State` alone, and `truncate_up_to(snapshot_seq)` deletes every op
  below it. `snapshot_frame` / `CompactEntityHandler` therefore re-state the
  FULL set as a fresh record AFTER reading the seq, so it lands at
  `snapshot_seq + 1` and survives; a crash between the two leaves the
  untruncated log, which still holds the originals. Ordering the other way
  round would delete the restatement.
- **GOTCHA — a `NoChange` Procedure is not registered with the flusher**, so
  its timer ops would never drain. Both SDKs drain them synchronously instead
  (`Self::flusher_registered()` in Rust; `slot.flusherEntry()` — always built,
  registered only when changeful — in Java) rather than registering every
  Entity and spawning the flusher thread for programs that will never produce a
  record.
- **A registration made during a REPLAY is registered** — the deliberate
  opposite of `ctx.spawn_workflow`'s `SkippedOnReplay`. A timer is internal
  state; skipping it would rebuild a pending set the live run never had. Both
  calls sit on the same `ProcedureCtx`, so it is a trap worth stating.
- **`TimerPolicy` is what a Procedure declares about its timers, resolved
  ONCE** (bound, procedure/clock names for refusals, whether the Clock is
  event-time, and the counter set) instead of four arguments threaded to every
  session. `TimerPolicy::session/firingSession` is the only way to open a
  `TimerSession`, in both SDKs. In Rust the arming seam (`apply_ops` /
  `rearm_restored`) lives beside the wheel in `entity/timer.rs`, reached from
  the slot's own `instance` back-reference; Java's `EntitySlot` has no such
  back-reference, so its `armTimerOps` stays on the dispatcher — a real
  asymmetry, not an oversight.
- **A failed firing parks like a poison input** (same retry ladder), and the
  removal is committed EITHER WAY — a poison timer parks once instead of firing
  forever, the same reason a park closes a checkpoint interval. `TimerSession`
  works on a COPY of the pending set so a retried attempt leaves no
  registration behind; `commit` is where the counters move.
- **FIXED IN PASSING (Java)** — `HeaderTemplate.decodeHeader` rejected any
  `format_id != KRYO`, so a Java Entity restoring a change log containing its
  OWN effect records (format 2) threw `formatNotSupported`. It now accepts 2
  and 3 and `EntityStateRestore.rebuild` branches, folding an effect record's
  enclosed changes. `EffectRecord.decode` + `DlqPark.decodeDlqEntry` are new
  for that.
- **Refusals**: `EventTimeTimerWithoutEventTimeClock` (an event-time timer
  under a non-event-time clock), `TimerCapExceeded`
  (`max_timers_per_entity()`, default 10 000), `TimersUnavailable` (a workflow
  stage or a `compensate` hook — structural: those contexts carry no session).
- **LIMITS (documented, not fixed)**: dynamic eviction discards an Entity's
  timers with its state (legal, D6); a HOT-SWAP migration does not carry them
  (the handover ships JSON `State` and suppresses the WAL restore — they are
  still on disk, so a later restore rebuilds them); workflow operatives have no
  timers (they have `wake_after`); resolution is milliseconds.
- **Observability**: per-Procedure `TimerCounters` (pending / registered /
  fired / cancelled / refused) plus `timers_armed` + `timers_fired` in every
  `data_heartbeat`, beside `dispatches` / `blocked_parks`.
- **Tests**: `sdk/rust/tests/entity_timers.rs` (own `[[test]]`,
  `bench-internals,testing`; cases that arm real deadlines take
  `timer_test_guard()` because the state sink is process-global),
  `EntityTimersTest.java`, the `TimersUnavailable` unit case in
  `dispatcher.rs`, and e2e `test/system/entity_timers_e2e_test.exs` — fires +
  re-seat-from-disk on BOTH runtimes (fixtures `session_timeout` /
  `java_session_timeout`, timeout from `TIMER_TEST_TIMEOUT_MS`). GOTCHA: the
  e2e's re-seat uses `TierE2EHarness.force_fresh_rebind` (kill the Overwatch),
  because `AddOperative` rebuilds the SDK-side instance — fresh slots, fresh
  wheel, no in-memory pending set — so ONLY the restore can still close the
  session. Killing the runner process instead does NOT rebind within the test's
  budget.
- **FIXED IN PASSING (controller, pre-existing from `entity-state-live-query`)**
  — `Overwatch.await_restore_init_state/2` still called
  `GenServer.call(control_unit, {:send_control, command}, timeout)` after that
  change reshaped `handle_call` to `{:send_control, command, want_metadata}`.
  EVERY awaited `RestoreInitState` therefore crashed the ControlUnit with a
  `FunctionClauseError`, taking the runner connection with it — invisible to
  `mix test` (the unit suite never drives a real restore) and fatal to
  `late_input_correction_e2e`, `effectively_once_checkpoint_e2e` and the
  workflow branching resume. It now goes through `ControlUnit.send_control_await/3`,
  which is the ONE awaited-round-trip seam and owns the reply-ref bookkeeping.
- **Docs/examples**: `docs/entity_timers.md`, plus sections in
  `docs/sdk_authoring_contract.md` (3.4), `docs/operative_architecture.md`,
  `docs/resilience/data_durability.md`, `docs/sdk_protocol_spec.md`,
  `docs/glossary.md`; `sdk/rust/examples/session_timeout.rs`, Java
  `E12_SessionTimeout`; `sdk/rust/public-api.txt` updated.

## Live Entity read (`entity-state-live-query`)

- **`flz state get <operative/procedure[/entity_key]>`** — the CURRENT
  in-memory state of one live Entity (or one Procedure's Entities) as JSON,
  plus the `change_seq` it was read at. `CliManager.state_get/3` →
  `OperativeManager.get_entity_state/3` → Overwatch → the EXISTING `EntityState`
  control round-trip.
- **`EntityState` was EXTENDED, not duplicated.** Two optional proto strings
  (`procedure = 2`, `entity_key = 3`; empty = absent, so a selector-less command
  encodes byte-identically and the migration/handover caller is untouched). No
  new command, no new payload-id, no second serializer. **Why the SDK produces
  the JSON at all: the controller CANNOT decode durable state** — WAL bytes are
  postcard (Rust) / Kryo (Java) and it stores them opaquely, the same property
  that makes `EffectRecordBody.changes` SDK-owned. `EntityState` is the
  SDK-produced readable answer that already existed.
- **The seq rides the FRAME METADATA (`flz.change_seq`), not the JSON body** —
  the body's shape is specified and already has a caller. Plumbing: a default
  `CommandAction::reply` (Rust) / `CommandHandler.reply` (Java) returning
  `(payload, metadata)` and forwarding to `handle` with no metadata, so ONE
  handler overrides it and the other ~19 are untouched; `ControlChannel` now
  passes the frame metadata to `ControlUnit`, and `send_control_await` takes
  `opts` (`:timeout`, `:metadata`) instead of a positional timeout.
- **A read is NOT a compaction.** It takes the state lock, serializes, releases —
  it does NOT drain pending changes, so the value can be ahead of the WAL (that
  is the point, and why the seq is stamped). Compaction must drain because its
  `snapshot_seq` becomes a truncation point. Pinned by
  `a_read_leaves_the_entity_undrained_and_its_seq_unchanged` (Rust) and its Java
  twin, both of which drop the flusher registration so "unflushed" is a fact of
  the test rather than a race against the 5 ms linger.
- **GOTCHA — Java's dynamic keyset is `entityKeyToIdx`, NOT the slot list.**
  `reapIdleDynamicEntities` removes the KEY and leaves the slot in the
  `CopyOnWriteArrayList`, so iterating slots reports Entities the reaper already
  evicted. Rust's registry removes the whole entry, so only Java had the trap.
- **`Keyset` (Rust enum / Java record) is the one place "what Entities does this
  Procedure have right now" is answered** — declared keys for static, the live
  registry/key-index for dynamic — and it answers BOTH a single-key lookup and a
  walk-all. That is load-bearing, not tidiness: materializing the keyset to find
  one Entity would make a narrowed read cost a whole walk, which is the entire
  point of the selector. Java's `deserialize` runs on the same seam.
- **Eviction is a distinct diagnosis, so it needs a trace.** Dynamic eviction is
  destructive and leaves nothing on disk, so "idle-evicted" vs "never existed"
  cannot be told apart after the fact — both SDKs keep a bounded (1024) FIFO of
  evicted keys purely to answer that refusal. Beyond the cap it degrades to
  `entity_not_found`, the safe direction.
- **Refusal tokens lead the error string** (`entity_key_without_procedure`,
  `procedure_not_found`, `entity_not_found`, `entity_evicted`) so the controller
  maps them without parsing prose; the controller adds `{:not_resident,
  :suspended | :unbound}` and the `{:oversized_state, bytes}` ceiling
  (`:wal_high_watermark_bytes`, no new config key). Every non-resident refusal
  names `flz state at` — there is deliberately NO durable fallback.
- **Deleted in passing**: `ControlUnit.handle_call({:get_entity_state, id})` —
  dead since the Overwatch started going through `send_control_await`, and a
  second, now-wrong way to ask the same question.
- **Tests**: `sdk/rust/tests/state_serialize.rs` (selector narrowing, dynamic
  coverage, refusals, unflushed-change seq, no-side-effects, the proto
  byte-identity pin, and the cross-language JSON fixture),
  `StreamOperativeStateSerializerTest` (same set + the identical fixture
  literal — both encoders sort object keys),
  `test/flezha/service/cli_manager_state_get_test.exs` (every verdict as the
  operator sees it), e2e `dynamic_entity_e2e_test.exs` "a live dynamic Entity is
  readable by name, narrowed to one key" on BOTH runtimes. GOTCHA: those two e2e
  cases take a namespace PER RUNTIME — placement is HRW over every runner in the
  namespace, so a peer case's runner (which lacks the factory) must not be a
  candidate.
- **Docs/examples**: `docs/entity_state_live_query.md` (the capability page —
  selector, freshness contract, refusal table, the programmatic seam a later
  HTTP surface consumes), `docs/sdk_protocol_spec.md` (`EntityState` selector +
  metadata + wire refusal tokens), `docs/resilience/state_persistence.md`
  "Live read", `docs/glossary.md` "Live read vs time-travel read", root +
  `sdk/rust/CHANGELOG.md`. Examples: `sdk/rust/examples/live_state_read.rs`,
  Java `E11_LiveStateRead` (both declare a `.key_by` Operative and show the
  exact `flz state get` invocations and refusals against it).

## Stream transforms + edge keying (`stream-dsl-keyby-and-transforms`)

- **`map` / `filter` / `flat_map` / `key_by` (Rust) — `map` / `filter` /
  `flatMap` / `keyBy` (Java)** on BOTH stream handles. Transforms are closures
  on a `ConsumerEdge`, not Procedure nodes: no Entity, no mailbox, no dispatch
  hop. **BREAKING — `Procedure::entity_key` + `ENTITY_KEY_OVERRIDDEN` /
  `entityKey` + `entityKeyOverridden` are DELETED, no shim.** Dynamic keying is
  `.key_by(f)` on the wiring, full stop.
- **Fusion is STRUCTURAL, not an optimizer pass.** Every transform declared
  before the closing `.into` is composed into ONE stored closure
  (`BuilderState::pending` → `extend_pending` → `compose`), so the runtime
  applies at most one transform + one key per edge and never learns a chain
  existed. That is why there is no `disableChaining()` to write: `.into` IS the
  boundary. The Rust test asserts it by COUNTING traversals (3 inputs → 3
  predicate calls), not by inspecting the topology.
- **GOTCHA — same seam as the merge mapping, and that is the whole design.**
  Closures are evaluated in `route_to_consumers` / `routeToConsumers`, the ONE
  path live delivery and the controller's re-forwarded replay slice both
  travel, so replay determinism cost zero code — exactly as
  `add-multi-input-procedures` found. A merge mapping is now literally one link
  of an edge transform chain (`erase_merge_map` builds an `EdgeYield::One`).
- **`EdgeTransformFn` takes `&Arc<dyn Any + Send + Sync>`, not `&dyn Any`** —
  that is what lets a surviving `filter` re-yield `One(Arc::clone(v))` (a
  refcount bump) instead of cloning the payload. `map`/`flat_map` still need
  `T: Clone` (the author writes `Fn(T) -> U`), matching the merge precedent.
- **Entity mode is read off the WIRING now.** `build_topology` collects
  `keyed_consumers` from the channel edges BEFORE the descriptor loop consumes
  them (that loop moves `entry.consumers`), then `mode = (entity_keys.is_empty(),
  keyed)`. Java mirrors it with a `dynamic` flag set during validation and
  carried on `ProcedureDescriptor` (replacing `entityKeyOverridden()`).
- **GOTCHA — a keyed edge into a procedure ALSO reached by an unkeyed edge is a
  hole the spec's own totality rule closes.** `UnkeyedEdgeIntoDynamicProcedure`
  refuses it; without it that input has no Entity and would silently vanish.
  Not in the proposal's refusal list — added because "the key function is total,
  so there is no keyless-drop path" cannot otherwise hold.
- **GOTCHA — a transform declared AFTER `key_by` is refused
  (`TransformAfterKeyBy`).** The key runs on the FINAL post-transform value
  (design D4), so a later transform would hand it a type it was never written
  against — a downcast panic at routing time instead of a build error.
- **`deliver_edge` / `deliverEdge` is the ONE place edge closures become
  mailbox pushes** — live routing AND the retry ladder's re-attempt both call
  it, so a re-attempt cannot deliver differently from the attempt it retries.
  A transform-free edge takes its `None` / `null` arm and allocates nothing
  (`transform_value` returns `Option`, `transformValue` returns `@Nullable`) —
  that is what keeps the "costs nothing when unused" promise true.
- **Containment: `ConsumerEdge::transform_value` / `key_of` catch_unwind ONCE**,
  and both seams read that one result. Stream → `spawn_edge_ladder` →
  `run_retry_park_ladder` (extracted from `run_entry_parse_ladder`; Java:
  `DlqPark.retryThenPark`, extracted from `PrimaryStreamReader`) → retry per the
  channel budget → DLQ park. Workflow → `extend_mapped` → terminal chain error the
  saga compensates. Both verified to FAIL with the guard removed.
- **An edge KEY never reaches the workflow driver** — a keyed edge makes its
  consumer dynamic and `build_dag` refuses a dynamic workflow procedure. A
  workflow ENTRY edge refuses ANY closure (`MergeFromEntryChannel`, message
  widened): the workflow seed bypasses the routing seam entirely.
- **D8 landed as ONE type, not two.** Rust: `Stream<'b, T, K = NodeTarget>` with
  `EntryStream<'b, T>` a type ALIAS (`K = EntryTarget`); the shared surface —
  `into`, the four transforms, `on_full`/`retry_*` — is one impl block, and
  `named`/`codec`/`keyed`/`sink`/`sealed`/`branch` stay on `NodeTarget`.
  Java: `EdgeStream<T>` interface, `EntryChannel extends` it (now an empty
  marker), `Stream implements` it; `StreamImpl` HOLDS an `EdgeStreamImpl` and
  forwards, because `Stream` must stay an abstract class for `@SafeVarargs
  branch` and Java has no second superclass. Cost: trybuild error text now says
  `Stream<'_, String, EntryTarget>`.
- **`Merge` keys ONCE, on the merge.** `Merge::key_by` extends each source's
  chain at wire time, so a key declared on a SOURCE handle trips
  `TransformAfterKeyBy` for free — that IS the "declared once" rule, not a
  separate check. `MergeSource::merge_channel_id` → `merge_site` (channel +
  pending chain); Java collapsed its two `from` overloads into one over
  `EdgeStream`.
- **Retired**: `inputs_dropped_no_key_total`, `TopologyIssue::StaticAndDynamic`
  (now `KeyByOnUnkeyableTarget`), Java's `@EntityKey` code generation (the
  annotation survives ONLY so declaring it errors with `.keyBy(f)` named).
- **FIXED IN PASSING (pre-existing Java bug the e2e needed):**
  `StreamOperativeStateSerializer` branched on `entityKeys().isEmpty()`, so a
  dynamic operative's `EntityState` reported only `entities.get(0)`. It branches
  on `dynamic()` now and emits the per-key map. **STILL OPEN (documented, not
  fixed):** Java never registers a lazily-created Entity with its flusher, so a
  dynamic Entity's state never reaches disk there — which is why the Java e2e
  arm asserts live state, not a WAL file.
- **Tests**: `sdk/rust/tests/stream_transforms.rs` (own `[[test]]` entry,
  `bench-internals`; two cases share `ChainOperative`'s statics and serialize on
  `CHAIN_LOCK`), `StreamTransformsTest.java`, workflow `PanickingTransformWorkflow`
  / the existing `PanickingMergeWorkflow` (message now `edge transform panicked`
  / `edge transform threw`), `tests/ui/procedure_entity_key_removed.rs` +
  `entry_stream_has_no_plugin_sink.rs`, e2e
  `test/system/dynamic_entity_e2e_test.exs` on BOTH runtimes.
- **Docs/examples**: `docs/stream_transforms.md` (the proposal named
  `docs/sdk_graph_authoring.md`, which does not exist — the graph surface is
  documented in `docs/operative_architecture.md`),
  `sdk/rust/examples/stream_transforms.rs`, Java `E10_StreamTransforms`.

## Multi-input procedures (`add-multi-input-procedures`)

- **`c.merge::<I>()` / `c.merge(I.class)`** — subscribe two or more source
  channels to ONE procedure whose `Input` is an author-declared sum type, each
  `.from(source, map)` supplying the mapping. Entry streams and inter-proc
  streams are both sources on a STREAM operative, freely mixed (workflow
  takes inter-proc only — see below). Single-input `.into` is unchanged.
- **The whole feature is ONE field on the consumer edge.**
  `ChannelDescriptor.consumers` went from `Vec<ProcedureId>` to
  `Vec<ConsumerEdge>` (`procedure` + `Option<MergeMapFn>`); `consumer_ids()` /
  `consumerIds()` is the id-only view the graph walks and the workflow DAG use.
  No wire change, no controller change, no journal-format change.
- **GOTCHA — map at the ROUTING seam, not the delivery seam.** The mapping is
  applied in `dispatcher::route_to_consumers` (Rust) /
  `ProcedureDispatcher.routeToConsumers` (Java — the wire reader and
  `fanoutInterProcedure` both go through it), which is the one path both live
  delivery and the controller's re-forwarded replay slice travel. That is why
  replay determinism cost zero code: frames are still journaled per source
  channel, and the slice re-maps identically. Mapping later (inside dispatch)
  would have forked live and replay.
- **`entities_for` / `entity_key` see the MAPPED value** — they run after the
  edge map, so per-source entity mode is encoded in the enum, not per edge.
- **Workflow works over INTER-PROC channels only**: `ChainCtx::edge_map` +
  `extend_mapped` apply the edge map where the driver accumulates a producer's
  batch into its child AND where `resumed_start` re-seeds from the journaled
  handoff. Java mirrors both in `WorkflowOperativeRunner`. Without this a merge
  in a workflow chain would downcast-panic.
- **GOTCHA — a merge mapping is the ONE author closure on a workflow chain that
  runs on the DRIVER thread**, not inside an entity invocation, so it is the one
  the existing `catch_unwind`s miss. `extend_mapped` contains it (Rust) /
  `WorkflowOperativeRunner.extendMapped` rethrows as `WorkflowOperativeException`
  (Java) into the chain's existing abort-and-compensate path; without that an
  unwind skips `emit_workflow_completed` and the instance is stranded with no
  terminal frame. No retry — a mapping is pure by contract, so a re-run panics
  identically. Stream is already covered on the inter-proc side (the producer's
  invocation guard); a stream ENTRY source is unguarded, exactly as the
  entry parser on that same wire-reader seam already is.
- **GOTCHA — the consumer-release block must sit AFTER the intent markers, the
  stage record and the proc-commit barrier.** That ordering is load-bearing now
  that a mapping can throw there: release-first leaves the producer whose edge
  blew up out of `completed` AND off disk (no intent markers), so it is never
  compensated and a crash mid-abort cannot recover it either. Java had it
  release-first and leaked exactly that (comp log `[undo-a]` instead of
  `[undo-b, undo-a]`); it now mirrors Rust's `run_iteration`. The abort scope is
  "stages recorded as completed" — a Rust sibling still in flight in the same
  level is NOT undone, which is the pre-existing mid-level abort shape (a failed
  `commit` at the same spot behaves identically), not a merge-specific hole.
- **The two workflow mapping sites do NOT share an undo scope.** Forward
  accumulate: parity, the producer whose edge blew up is always unwound on both
  SDKs (measured). Crash-resume re-seed: Rust's `drive_chain` seeds the resumed
  iteration's `completed` from `journaled_stages`' in-flight stages
  (`driver.rs` `resumed_stages` → `completed`), so the journaled producer IS
  unwound; Java's `runIteration` starts `completed` empty and `continue`s past
  journaled procs without recording them, so NOTHING is unwound. Do not write
  "both SDKs unwind the same set" without saying which site.
- **KNOWN LIMITATION (verified against HEAD, documented, NOT fixed) — a resumed
  Java workflow chain compensates only what re-ran.** `runIteration` never seeds
  `completed` from the proc-journal, so any abnormal exit from a resumed chain
  leaves the journaled-committed procedures un-undone; Rust unwinds them.
  Pre-existing and NOT merge-specific (it applies to a resumed Java chain
  failing anywhere forward) — a merge mapping throwing at the re-seed site is
  one way to reach it. Symptom: a Java workflow instance that crashed mid-chain,
  resumed, then failed reports `error` having compensated fewer procedures than
  its Rust twin. Closing it is a workflow-resume semantics change, out of scope
  here. Written up in `docs/multi_input_procedures.md` "Known limitation".
- **GOTCHA — a workflow merge over an ENTRY channel is refused, not supported.**
  The workflow seed (`driver.rs seed_bytes`, fed by the spawn args and by
  `PerInput` inbox items) is a bare payload naming no channel, so no mapping
  could be selected and every entry channel past the first is unreadable —
  `build_dag` would happily accept the shape (`entry.consumers.len() == 1`
  holds) and every invocation would then fail the input-type check. Hence
  `BuildWorkflowError::MergeFromEntryChannel` / the same name in Java's
  `WorkflowSpec`. Stream is unaffected: the controller stamps
  `metadata["channel"]` there, which is exactly what the workflow seed lacks.
- **Refusals**: `TopologyIssue::MergeTooFewSources` / `DuplicateMergeSource` /
  `EdgeAlreadyWired` (both SDKs, same names) + the workflow
  `MergeFromEntryChannel`. Mapped-type mismatch is a COMPILE error in Rust
  (`Procedure<Input = I>` bound on `.into::<Proc>()`) and a registration error in
  Java (`wireConsumer` compares the declared merge type, not the channel
  payload type) — the standard erasure asymmetry.
- **GOTCHA — `attach_consumer` must not silently drop a mapping.** It dedups by
  consumer id, so a second declaration on an existing edge used to keep the
  first mapping (or none) and deliver the wrong value forever, with nothing
  said. It now refuses whenever either side carries a mapping;
  mapping-free re-declaration stays the legal fan-in / repeated-`.into` no-op.
  A duplicated source reports ONLY `DuplicateMergeSource` — `Merge` counts
  `declared` (every `.from` written), not surviving sources, so the arity check
  no longer also blames the author for declaring too few.
- **GOTCHA — a Java test's instance id IS the 32-byte wire payload-id.**
  `factoryId + "-instance"` truncates and the frame finds no operative
  (`dispatch: operative instance ... not found`). Use the bare factory id.
- **GOTCHA — `mvn test -pl flezha-sdk-runtime` without `-am` resolves
  `flezha-sdk-api` from the stale installed jar**, so a new api class throws
  `NoClassDefFoundError`. Always `-am` after touching the api module, and
  rebuild the committed dist jars (`mvn clean` first — the fat jar shades stale
  `target/classes`).
- **Tests**: `sdk/rust/tests/merged_input.rs` (its own `[[test]]` entry,
  `required-features = ["bench-internals"]`; the three entry-merge cases share
  one static observer and serialize on `tuned_test_guard()`),
  `MergedInputTest.java`, `E09_MergedInputTest.java`. The workflow halves are the
  diamond fan-in re-authored as a merge — `MergeWorkflow` in
  `workflow_driver_scenarios` and `MergeWorkflowOperative` in `WorkflowKindTest` —
  each run forward AND resumed from a journal, which is what covers the
  accumulate and re-seed mapping sites; the entry-source refusal is pinned in
  `dag_topology_is_validated` and `WorkflowKindTest`. `PanickingMergeWorkflow` /
  `ThrowingMergeWorkflowOperative` pin the containment — both were confirmed to
  FAIL (driver thread gone, `drain` times out / the completion latch never
  counts down) with the guard removed, and both assert the EXACT undo list on the
  FORWARD site (`undo-b` then `undo-a` — Rust's literals carry the output too,
  a fixture-format difference) so a re-divergence in release ordering is caught
  rather than assumed away. Both fixtures are ALSO driven through the
  crash-resume re-seed site via the resume harness + a journal, asserting
  containment and the terminal report; Rust additionally asserts the journaled
  stages were undone in reverse, read off the compensation-DONE markers (the
  comp log is empty there — `diamond_op!` logs per completed output and a
  journal-rebuilt `EntityRun` has none, a trap worth remembering). Java asserts
  no undo list on that path — see the known limitation above.
- **`sdk/rust/public-api.txt` is part of "done" for any SDK-surface change**
  (`sdk/rust/README.md` "Public API workflow"). It is human-curated commentary,
  not `cargo public-api` output, so it is hand-edited — and a CHANGED signature
  (here `ChannelDescriptor::consumers`) must be recorded, not just additions.
- **Docs/examples**: `docs/multi_input_procedures.md`, `docs/glossary.md`
  ("Merge"), `sdk/rust/examples/merged_input.rs`, Java `E09_MergedInput`.

## Procedure-requested spawn (`add-procedure-requested-spawn`)

- **`ctx.spawn_workflow` / `ctx.spawnWorkflow`** — spawn-or-deliver a workflow
  instance from inside any procedure, in the requester's own namespace. Free id
  ⇒ spawn with the payload as the first input; taken id ⇒ deliver
  (`IfRunning::Deliver`) or `already_running` (`Error`).
- **The namespace is NOT on the wire.** `SpawnWorkflowRequest{instance_id,
  factory_id, payload, if_running}` (tag 34) + `SpawnWorkflowResponse{outcome,
  error}` (tag 35). The controller takes the namespace from the ControlChannel
  the frame arrived on — cross-namespace spawning is impossible by
  construction, not by validation.
- **GOTCHA — the first-frame guarantee is `:seed_input`, not a follow-up send.**
  The payload rides the Builder's `opts[:seed_input]` and is ingested by
  `Overwatch.seed_input/2` INSIDE the bind `handle_continue`, so no other
  mailbox message (including a concurrent requester's delivery) can be ingested
  ahead of it. A start-then-send would lose that race. `fresh_inbox?/1` skips
  the seed when a durable instance's inbox already holds entries — that is a
  restart, not a spawn, and the cspec (which carries the seed) is replayed on
  controller boot.
- **`OperativeManager.start_workflow/5`** is the ONE place the registry's
  duplicate-id refusal (`"operative id already in use"` → `:duplicate`) is
  recognised; `Schedule.Job` and `spawn_or_deliver/5` both go through it.
  `spawn_or_deliver/5` = existence check (`exist?/2`) → factory check
  (`FleetManager.runners_for_factory`) → `start_workflow` → race-loser falls
  back to `send_io_confirming` (the existing confirming ingest).
- **GOTCHA — resolution must NOT run on the ControlChannel process.** It starts
  an operative, which drives `AddOperative` and waits for the response on that
  same control stream. `handle_spawn_workflow/3` runs in a `Task` and sends
  `{:spawn_workflow_result, ...}` back so the reply frame is still written by the
  channel process (same reason as the workflow-completed teardown).
- **Failure isolation**: the SDK call returns a handle; refusals never reach
  `run`. Rust drops an unobserved handle into ONE process-wide reaper thread
  (`spawn.rs`); Java logs from the reply callback unless `await`/`tryResult`
  claimed the handle first. Java additionally always logs the pre-resolved
  `Unavailable` (no control plane) — documented asymmetry.
- **`shutdown::AckResult` is now `Result<String, String>`** (was
  `Result<(), String>`): the Ok payload carries the reply's outcome, empty for
  bare acks. Java's `AckRegistry` future is `CompletableFuture<String>` with
  `complete/2` + `fail/2`. One rendezvous, no second registry.
- **`runner::coordinator_try_send`** reports whether the message reached a
  coordinator; `coordinator_send` delegates to it. A stale trigger (closed
  runner) now resolves a spawn as `Unavailable` instead of waiting out 30 s.
- **Replay issues nothing** — `ProcedureCtx::during_replay` (Rust, set from
  `EntityPayload.replay`) / `ProcedureCtx.replaying()` (Java, threaded through
  `InputEnvelope`) ⇒ `SkippedOnReplay`.
- **Tests**: `test/flezha/service/operative_manager_spawn_test.exs` (ETS
  `insert_new` stands in for the registry's atomic claim, so the concurrent
  burst exercises the real race), `control_channel_spawn_workflow_test.exs`,
  Rust `procedure::tests::spawn`, Java `SpawnHandleTest` + `SpawnRequesterTest`.
  GOTCHA: the Rust coordinator trigger is process-global — every test that
  installs one takes `runner::env_test_guard()`.
- **Docs**: `docs/procedure_requested_spawn.md`, `docs/sdk_protocol_spec.md`,
  `docs/resilience/workflow_operative.md`. Examples:
  `sdk/rust/examples/spawn_workflow_worker.rs`, Java `E08_SpawnWorkflowWorker`.

## Operative-to-operative messaging (`operative-to-operative-messaging`)

- **The route already existed; only a VERDICT was missing.**
  `OperativeManager.send_io/4` → `HubManager.get_pid` → `cast {:io_in,..}` →
  `Overwatch.ingest/3` → `:on_input` chain → `journal_input/2` has been there
  for over a year with ZERO production callers. This change adds the plugin
  that exposes it as configuration plus a confirming entry point a retry loop
  can read. No new control message, no proto change, no new dedup store.
- **`Overwatch` `handle_call({:ingest, msg, md})`** — three clauses (suspended
  / `init_error` / normal) reusing `ingest/3` + `ingest_suspended/3` AS-IS.
  Verdicts: `:ok | {:ok, :absorbed} | {:ok, :suspended} | {:error, :not_ready
  | :not_found | :refused}`. `{:error, :not_found}` is minted by
  `OperativeManager.send_io_confirming/5`, not by the Overwatch.
- **`journal_input/2` now answers `:refused`** (was an indistinguishable `nil`)
  when the durable-inbox append FAILS — absorbed-duplicate stays `nil`. So
  `ingest/3` returns `nil | :refused | {msg, md}`, and both legacy `:io_in`
  clauses match on the 2-tuple instead of `nil` (behaviour-identical: neither
  was ever forwarded).
- **`forward_input/2` was EXTRACTED from the existing `{:io_in,..}` call
  clause** and is now shared with the confirming path — there is exactly ONE
  channel-forward + `[:flezha, :stream, :drop]` telemetry site. It also gained
  a `channel: nil` clause (the old code would have exited on a call to nil).
- **GOTCHA — an unbound stream is NOT uniformly an error.** `confirm_forward/2`
  maps `{:error, :no_stream}` to `:ok` for a durable target (effectively-once
  or durable workflow: it is already journaled, the bind delivers it) and to
  `{:error, :not_ready}` otherwise. Returning `not_ready` for a durable target
  would make the sender retry a payload the receiver already has, which for an
  effectively-once target appends at a NEW offset = a genuine duplicate.
- **The cast keeps its silent `init_error` drop** — deliberately. A sender that
  needs the payload held uses the confirming path.
- **Provider = `Flezha.Plugin.Provider.OperativeForward`** (`:io_binding`,
  `:on_output` + `:on_input`) + `ForwardSup` (THIRD `WorkerSup` family, beside
  `Kafka.ProducerSup` / `Postgres.SinkSup`; key = `{plugin_id, target}`) +
  `ForwardWorker`. Wired in `application.ex` next to the other two.
- **GOTCHA — the worker must NOT deliver from its own `handle_call`.** It
  spawns a monitored process per in-flight item (`make_ref()` tag + separate
  monitor ref; demonitor+flush on result). If it blocked in its own mailbox,
  A.Overwatch→workerA→B.Overwatch→workerB→A.Overwatch is a FOUR-WAY deadlock
  the moment both peers have a message in flight. `operative_forward_test`'s
  cycle case reproduces exactly that (each stand-in peer echoes through its own
  forward plugin from inside its ingest handler) rather than asserting around it.
- **One payload in flight at a time** — order-preserving; a retryable verdict
  re-arms the same queue head. `:transient` restart so `Table.do_unsubscribe`'s
  `safe_kill(owned_pid)` (exit `:shutdown`) STOPS the worker instead of having
  the DynamicSupervisor restart it. (Postgres/Kafka are `:permanent` and do
  restart — a known asymmetry, not fixed here.)
- **Source identity reaches `init_provider/1` via ONE generic seam**:
  `Table.with_host_identity/2` merges `%{source_namespace:,
  source_operative_id:}` LAST (same anti-spoof rule as `bulk_activate`'s
  `activation_env`). Needed for the self-target / cross-namespace refusals.
- **Refusals at enable time** (= inside `init_provider/1`, which runs in the
  `PluginManager.enable_plugin` call): missing `:target_operative_id`,
  target == self, target resolving in a DIFFERENT running namespace. A target
  resolving NOWHERE is ACCEPTED — it may not be started yet and the worker
  retries. Longer cycles (A→B→A) are deliberately allowed.
- **`build/1` is OVERRIDDEN to reject unknown string keys** — the generic
  `ProviderBehaviour` `build/1` silently drops any key that is not a struct
  field, which would let `delivery_mode: "exactly_once"` read as accepted. The
  guarantee is DERIVED, never configured (outbox ⇒ effectively-once; no outbox
  ⇒ at-least-once), so there must be no ack-mode knob.
- **`ProviderBehaviour.retry_backoff_ms/3`** — the Postgres sink's inline
  `min(base * 2^(n-1), max)` extracted; both sinks share it now.
- **RESTART LIMITATION (verified, documented, NOT fixed)**: in-flight queued
  items do NOT survive a controller restart. `ProcJournal.flush_entry/3` casts
  `{:io_out,..}` THEN appends `outbox_flushed`; `WalWriter.dispatch_outbox_
  entries/2` casts THEN advances+persists `outbox_flushed_seq`. Neither cursor
  is gated on a sink ack, so once the marker lands the item is no longer owed.
  Same posture as the Kafka/Postgres buffers. Already pinned by the existing
  `proc_journal_test` "pending_entries == [] after flush" assertion — no new test.
- **Telemetry** `[:flezha, :plugin, :operative_forward, :delivered|:retry|
  :queue_depth|:saturated]` (Kafka's `[:flezha, :plugin, <provider>, _]` shape)
  → `TelemetryBridge` source `:flezha_operative_forward`.
- **NOT this change**: synchronous request/reply — that is the shipped
  `operative-message-handlers` (`:on_message` + `MessageForward`), which reaches
  the SDK factory's `handle_message`. This reaches the INPUT STREAM. Both docs
  cross-link.
- **Tests**: `test/flezha/operative/overwatch_confirming_ingest_test.exs` (all
  six verdicts + the cast's unchanged silent drop; drives `handle_call/3` on a
  struct like `overwatch_suspension_test`, with a spawned stand-in StreamChannel
  because the test process cannot be its own channel),
  `test/flezha/plugin/provider/operative_forward_test.exs` (refusals, cycle,
  reuse, saturation, retry-until-delivered, identity stamps, disable-stops-worker,
  telemetry). `Mimic.copy(Flezha.State.ProcInbox)` added to `test_helper.exs`.
- **Docs**: `docs/operative_forward_plugin.md` (new),
  `docs/resilience/data_durability.md` "Operative to operative",
  `docs/glossary.md`, `docs/plugin_lifecycle.md`,
  `docs/operative_message_handlers.md`. `sdk_protocol_spec.md` needs NO change —
  both ends are controller-side and the payload reaches the target's runner on
  the ordinary `:io_in` frame.

## Runner entry point + dynamic type set (`sdk-runner-entrypoint-redesign`)

- **Runner-as-handle, no builder, mirrored across both SDKs.**
  `Flezha.createRunner(ns).defineStream(A.class).run()` (Java) /
  `Flezha::create_runner(ns)?.define_stream::<A>()?.run()?` (Rust) —
  Beam's `Pipeline.create()` shape: ONE concrete `FlezhaRunner` carries
  declaration and lifecycle. `RunnerBuilder`, `newRunner`/`new_runner`, both
  Java registry facades (`StreamOperativeRegistry`/`WorkflowOperativeRegistry`),
  freeze/unfreeze, and the zero-op refusal are GONE (no shim). Zero-operative
  start is LEGAL (the controller always accepted an empty factory list).
  `run()` = `start()` + `awaitTermination()` — never independent. Naming rules
  (incl. `create*` over `new*`, `remove*` as `define*`'s inverse, prefer
  Beam/Spark/Kafka-Streams vocabulary) live in `docs/sdk_naming_conventions.md`.
- **The declared factory set is PER-RUNNER and starts EMPTY each generation**
  (gen 2 must re-define; inventory harvest merges at every `start()`). The
  process-global class registries are overwrite-on-insert + `unregister_class`
  — duplicate detection lives in the runner's declared set. `define*`/`remove*`
  are legal pre- AND post-start: post-start changes ship `UpdateFactories`
  (proto tag 33, full set, fire-and-forget, NO ack) via ControlChannel →
  `FleetManager.handle_cast({:sync_factories,..})` (remove_ce_factories +
  store_factories composed); the reconnect init frame — serialized from the
  same per-runner set at send time — stays the resync of record.
- **Removal authority is SDK-side and race-free by construction.** Rust: a
  `RunnerMessage::RemoveFactory` handled on the coordinator loop (serialized
  with `AddOperative`; seam = `RunnerNode::remove_factory`, unit-tested in
  `fast.rs`); `WorkflowInstance` carries `factory_id` for the scan. Java (no
  single coordinator thread): `FlezhaImpl.removeType` = unregister-first →
  scan live instances → re-register + throw `OperativeTypeInUseException`
  (extends ISE) on a live hit — the transient window is benign (a failing
  `AddOperative` is re-placed by convergence). Live instances of the factory ⇒
  `FlezhaError::OperativeTypeInUse` / that exception; undefined ⇒
  `NotDefined` / IAE naming the id.
- **Rust: ONE public error.** `FlezhaError` (thiserror; `InvalidNamespace,
  AlreadyRunning, ConnectFailed, InvalidTopology(#[from] TopologyBuildError),
  InvalidWorkflow(#[from] BuildWorkflowError), DuplicateFactoryId, NotDefined,
  OperativeTypeInUse, NotStarted, RunnerUnavailable`) — the whole chain
  compiles under one `?`. `RunnerStartError`/`RunnerError` deleted. The
  per-issue topology enum was renamed `BuildTopologyError` → `TopologyIssue`
  (the per-kind `TopologyBuildError` keeps its documented name).
  `operatives()` returns `Result<HashMap<String, Arc<StreamInstance>>, _>`
  (the `Arc<RwLock<Arc<Mutex<..>>>>` view + `snapshot_as_operative_view` are
  gone); `id()`/`getId()` → `namespace()`/`getNamespace()`.
- **The slot is claimed at `start()`, not create.** Any number of unstarted
  runners may exist; CAS in `start()`; the loser never dials nor touches
  shared state; owner-never-started Drop/close is a no-op; double-`start()`
  on one runner errors. Rust `RunnerCore { namespace, declared, started,
  terminated+condvar }` behind `ACTIVE_RUNNER: Mutex<Option<ActiveRunner>>`;
  Java `FlezhaRunner` holds two instance-scoped `RegistryCore`s + an
  `AtomicReference<RunnerHandle>` (`internal/RunnerHandle` = the runtime SPI:
  runnerId/operatives/awaitTermination/close/removeType/syncFactories).
  `FlezhaImpl.start(String, Supplier<List<String>>, Runnable)` — the supplier
  feeds the initial handshake AND reconnect re-handshakes. `FactorySync`
  (runtime) ships `UpdateFactories` via the GracefulShutdown frame-write
  pattern; the processor aggregator generates `defineAll(FlezhaRunner)`.
- **GOTCHA — a chained `start()` temporary Drops at statement end** (Rust):
  always `let runner = ...start()?;`. `run()` is immune.
- **GOTCHA — the e2e harness kills runners by closing stdin.** `Port.close/1`
  is how `node_test_helper` tears down runner processes, so both test apps
  keep a stdin-EOF **watcher** thread (exit-on-EOF) even though keep-alive is
  `run()`'s job now. Deleting it orphans every e2e runner.
- **GOTCHA — the committed dist jars embed the api classes.** Any change to
  `flezha-sdk-api` needs `scripts/build-dist.sh` (or the jar steps manually)
  + committing `sdk/java/flezha-sdk/dist/` and `test/apps/java_runner/dist/`.
  The fat-jar module shades its own stale `target/classes` FIRST — after an
  api change, `mvn clean` in sdk/java or the shaded jar keeps old classes.
- **Known limitation (documented, not fixed)**: a parked/suspended durable
  workflow instance is absent from the SDK instance registry, so removing its
  type succeeds SDK-side and rehydration later fails placement. FleetManager's
  dropped-factory warning is the tripwire
  (`docs/resilience/workflow_operative.md`).
- **Lifecycle tests**: Rust `sdk/rust/tests/runner_lifecycle.rs` — still ONE
  sequential `#[test]`; the fake `UnixListener` controller now CAPTURES +
  DECODES frames (init JSON factory_ids, UpdateFactories protobuf);
  `FLEZHA_CONTROL_ACK_TIMEOUT_MS` raised so the frame-less fake never triggers
  reconnect churn. Java `RunnerLifecycleTest` in flezha-sdk-api with test
  doubles under the runtime FQNs (`com.flezha.sdk.runtime.FlezhaImpl`
  implements `RunnerHandle`, fakeable `LIVE_INSTANCES`, recorded
  `SYNCED_SETS`); controller side in
  `test/flezha/runner/fleet_manager_sync_factories_test.exs` +
  `control_channel_update_factories_test.exs`. The real runtime wiring is
  covered by the e2e matrix.

## Java SDK parity (`java-sdk-parity`)

- **The Java SDK is now at functional parity with Rust.** Every capability an end
  user can author in Rust — the effectively-once tier, checkpoint ack, late-input
  correction, the full workflow durable stack (proc-journal, staged outbox, durable
  loop + suspension, entity retry, loop rewind, branching), dynamic entity mode,
  and the determinism contract — is available in Java. The e2e matrix no longer
  branches per runtime; there is ONE contract.
- **The unblocker was `AddOperativeHandler` returning `new byte[0]`.**
  `AddOperativeResponse.metadata` is the ONE seam `ClassRegistry.decode_declarations/1`
  reads. With it empty the controller could not be TOLD a Java class is
  `effectively_once` / has a `correction_window` / an `ack_granularity` /
  `compensable_procs` / `leaf_procs` — so every declaration-gated capability was
  blocked, including ones not yet written. `Declarations.java` builds it now. A
  class declaring nothing still encodes to **zero bytes** (= "declared nothing"),
  which is what keeps existing classes byte-identical.
- **Two codecs coexist by design.** `EffectRecordBody` is postcard (the controller
  decodes it) but its `changes` elements are the SDK's own bytes — **Kryo** in
  Java, postcard in Rust — because the controller stores them opaquely and
  re-ships them on restore. `format_id` discriminates the FRAME kind (2 = effect
  record), not the change codec.
- **`Postcard.java`** is the ONE postcard implementation (extracted from
  `DlqPark`'s private encoder). **`AckRegistry.java`** is the ONE ack rendezvous
  (`RunnerShutdownAck`, `ParkPayloadAck`, `ProcCommitResponse`,
  `IterCommitResponse`). Callers MUST register before sending — that ordering is
  what makes it race-free.
- **`StableId`** is two longs, not `BigInteger`: the FNV prime is `2^88 + 0x13b`,
  so the 128-bit multiply is one shift + one `Math.unsignedMultiplyHigh` —
  allocation-free, which matters on a path called inside `run`. Pinned
  cross-language on BOTH sides (`determinism.rs` +
  `StableIdTest`).
- **Clocks are type-level markers.** `Clock.now()` was deleted — `WallClock.now()`
  returned ambient `Instant.now()`, an ambient-time trap inside the very type
  meant to prevent one. The runtime evaluates the declared clock ONCE per
  invocation via `clockType()` and serves it from `ctx.now()`.
- **Erasure ⇒ registration-time refusals.** What Rust rejects at compile time (a
  `TimeExtractor<Input>` bound, `UnservableClock`) Java rejects at registration,
  with the same error names. Permanent asymmetry, equal safety.
- **GOTCHA — the topology builder throws its accumulated `errors` BEFORE the
  descriptor-materialization loop.** A refusal added to that loop is added after
  the throw point and never fires. Put new refusals in the validation phase.
- **GOTCHA — never assert on `EntitySlot.effectBuffer()`.** The background
  flusher drains it within a ~5 ms linger tick, so such a test passes alone and
  flakes under load. Use `EffectRecordCapture` (test helper) which captures what
  actually SHIPPED. `stagedChanges()` IS safe — the flusher deliberately never
  drains it (an open interval is not flushable).
- **GOTCHA — a workflow unit test's `WireWriter` IS the controller.** It must (a)
  `AckRegistry.complete()` every `ProcCommit`/`IterCommit` (the driver BLOCKS at each
  barrier; a silent fake controller hangs it for 30 s), (b) FLUSH each commit's
  outbox onto the secondary stream (output is staged now — the SDK no longer ships
  it, so every emit assertion depends on this), and (c) count its completion latch
  only on `WORKFLOW_COMPLETED`.
- **GOTCHA — classify beyond-window at ADMISSION, not dispatch.** Dispatch is async
  on the pool, so folding the event-time high-water there means a burst classifies
  against a mark that has not moved yet.
- **A park is SUSPENSION, not completion**: `IterPark` exits WITHOUT
  `WorkflowCompleted`. Emitting one would tear the instance down and lose the
  iteration it parked at.
- **Branching needed NO planner change**: BFS numbering already makes completion
  order topological and `planCompensation` already reverses it. What it DID need is
  per-proc input routing — a fan-in must **accumulate** both producers' output, not
  overwrite.
- **`data_heartbeat` now carries `dispatches` + `blocked_parks`** (was
  `credit_parked` only). This was a LIVE operational gap: a wedged Java runner
  looked healthy to the controller's `StallClassifier`.
- **GOTCHA — `ProcCommit.iter` MUST be stamped; Java silently did not.**
  `commitProcBoundary` set only `operative_id`/`proc_idx`, so every `proc_committed`
  landed at `iter=0` while `iter_committed` advanced normally. The controller
  keys an entry's flush marker by `{type, proc_idx, iter}`
  (`ProcJournal.pending_entries/1`), so on a loop's SECOND and later iterations the
  commit collided with iteration 0's already-written `outbox_flushed` and its
  staged output was **journaled but never delivered** — silent, and invisible to
  every test that ran one iteration. Rust always stamped it (`driver.rs`, both the
  forward and `compensating` builds). Java now stamps both, `StageRecord` gained
  `iter` (Rust parity), and `resumeIter/1` is the single definition of "the
  iteration a resumed instance runs next". Pinned by
  `WorkflowKindTest.perInputLoopCommitsPerIterationThenParks`, which asserts
  `opCommitIters() == [0, 1]` — it fails with `[0, 0]` against the old code.
  Symptom to recognise: a `PER_INPUT`/`CHAINED` Java operative emits fine on its
  first turn and is silent forever after, with `flz state journal` showing an
  `proc_committed` carrying an outbox and no following `outbox_flushed`.

## Checkpoint ack-granularity (`effectively-once-checkpoint-granularity`)

- **The kernel was already checkpoint-capable.** `EffectRecordBody.consumed_offset`
  is `Option<u64>` and EVERY controller reader already tolerates `nil`:
  `dispatch_one_effect` advances the watermark only `if is_integer(...)`,
  `max_offset/2` skips nils rebuilding the durable watermark at open,
  `consumed_offset_of/1` folds nil→`-1` for the correction baseline. So the mode
  is a **producer-side** change: no new record type, no new control message, no
  proto change, no new retention rule.
- **Interval replay = the EXISTING crash-resume replay.**
  `Replay.restore_and_replay_journal/1` already re-forwards
  `InputLog.watermark + 1 .. last_offset` on every tier bind. Hold the watermark
  at the last checkpoint and that path IS the interval replay. Same for retention
  (truncates below the min durable consumed-offset = the last checkpoint) and for
  credit (drains on `:watermark_advance` → bursts at checkpoint cadence). All
  three requirements fell out with ZERO controller code.
- **SDK**: `c.checkpoint_ack()` (needs `effectively_once()`, else
  `CheckpointAckWithoutTier`) + `ctx.checkpoint()`. `TypedBuffer` gained a third
  vec `staged` — the open interval's changes. `drain_to_frames`/`is_empty`
  deliberately IGNORE it (an open interval is not flushable; draining it alone
  would put state ahead of its offset — the exact tear the mode prevents).
  Dispatcher emission: `closing = checkpointed || dlq.is_some()` → ship
  `{staged, consumed_offset}`; else ship `{[], outputs, None}` iff outputs exist.
- **Mid-interval emits flush immediately** (open question A/B → **A**). Option B
  (stage until the checkpoint) buys nothing: the interval's outputs are re-emitted
  on replay under either option, because the inputs are re-forwarded. Keyed sinks
  absorb it — the tier already requires them.
- **A park CLOSES the interval** (the DLQ open question). The park record already
  carries `consumed_offset`; letting it also carry the staged changes means the
  poison offset is acknowledged and the replay starts AFTER it. No offset-skip set,
  no DLQ lookup on replay. It is also the only correct choice — otherwise the
  poison is acked while the state before it is not durable.
- **A restore discards the open interval** (`FlushableBuffer::discard_staged`,
  called in `restore_into_slot` beside the mailbox/replay-buffer clears). Same
  rationale: it belongs to a past that no longer exists.
- **The controller needs the declaration for exactly ONE reason**:
  `Watchdog.sweep_catastrophic_dataflow` reads "produced output before, silent
  since" as a dead feed — a lie for a checkpoint class, whose silence is its steady
  state. Rides `AddOperativeResponse.metadata` (`ack_granularity=checkpoint`) →
  `decode_declarations/1` → `ClassRegistry.put_checkpoint_ack/2` (ETS + sidecar,
  same shape as the tier; `put_flag/4`+`flag?/3` now share that shape).
  `sweep_output_staleness` needs NO gate — it fires only when changes are still
  arriving but persistence froze, which is a genuine wedge in checkpoint mode too.
  `StallClassifier.watermark_frozen?` would misfire, but it is currently
  unreachable (`progress_markers/0` never populates the tier cursors).
- **Tests**: `operative_dispatch.rs` (interval holds / park closes / restore
  discards), `builder.rs` (`checkpoint_ack_without_the_tier_is_refused`),
  `wal_writer_outbox_test.exs` (nil-offset record flushes outputs, holds the
  watermark), `class_registry_test.exs`, `add_operative_response_test.exs`,
  `watchdog_test.exs` (with a positive control — the pre-existing workflow
  catastrophic refute was vacuous: `:catastrophic_dataflow_timeout_ms` defaults to
  60 s and no test armed it), e2e `effectively_once_checkpoint_e2e_test.exs`
  (fixture `checkpoint_store`, `commit:` closes the interval).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `EffectRecord` encoder + `c.checkpointAck()`
  + `ctx.checkpoint()` + the staged interval.

## Stream late-input correction (`stream-late-input-correction`)

- **THERE IS NO SPLICE.** `Ingest.journal_input/2` appends to the input log
  BEFORE forwarding (`overwatch.ex:634`); the SDK extracts event time later, in
  `dispatch`. So a late input is already durable in the log at its arrival offset
  by the time anything knows it is late. The slice is an offset range and already
  contains it. The correction = the EXISTING tier restore-and-replay path with
  (i) a rewound baseline and (ii) an event-time-sorted slice. **No new control
  message, no correction journal, no crash marker, no proto change.**
- **Crash-mid-correction needs no marker** — before the rewind commits, nothing
  durable changed (the late input is still unconsumed → re-classified → correction
  re-runs); after it commits, the on-disk state IS the baseline and the ordinary
  resume replay IS the correction. Hence: for a correction-declaring class, EVERY
  journaled replay is marked and sorted.
- **Baseline = scan FORWARD to the first record with `event_time >= T`, keep the
  one before it.** A backward scan for "first earlier event time" is WRONG — it
  stops at an out-of-order straggler (a beyond-window `process_as_arrival` input)
  and leaves the records that overtook the late input in place, correcting
  nothing. Pinned by `wal_rewind_test.exs` "folds an out-of-order straggler".
- **`WalWriter.rewind_to(seq, epoch)`** — keeps records ≤ seq, drops ONLY snapshots
  > seq, re-derives `pending_outbox` from the rewritten file, lowers the input-log
  watermark (`InputLog.rewind_consumed/3`). NOT `realign_epoch/4`, which truncates
  the WAL to zero and `File.rm`s every snapshot (a cold-takeover wipe). Shared tail
  `bump_epoch/2` (outbox cursors + `persist_dedup` = the COMMIT POINT). `epoch` is a
  PARAMETER, not `current+1` — that is what makes a crash-resumed correction
  idempotent instead of double-bumping.
- **`flz.replay_end` is a BROADCAST, not a mark on the last input.** An input routes
  to ONE entity by key, so a marker riding the last one strands the slice every
  OTHER entity buffered. It is a payload-less frame intercepted in `fast.rs` before
  channel lookup → `dispatcher::end_replay_slice`.
- **Drain ordering gotcha**: a closed slice must flush BEFORE any live input popped
  behind it (`maybe_dispatch_replay_slice` runs both mid-pop-loop and after it) —
  otherwise the live input jumps ahead of the very inputs being re-ordered. Caught by
  `a_replayed_slice_dispatches_in_event_time_order`.
- **`do_sweep` gates on the FLOOR, not the watermark** — under a hold window the
  floor advances with the clock while the watermark stands still, so the old
  watermark-advanced guard never truncated.
- **Effect record gained `event_time: u64`** (same `format_id = 2`; no legacy shim —
  the library is unpublished). Serves both the correction baseline AND
  `TimeTravel`'s `{:timestamp, ts}` (which used to approximate from the WAL file's
  mtime; `pick_snapshot_by_mtime/2` deleted). Cross-language fixture in
  `flusher.rs` + `wire_test.exs`.
- **No snapshot-header high-water needed**: `compact_after_age_ms >= correction_window`
  guarantees the retained WAL always covers the window, so max `event_time` over the
  restored change log IS the high-water (anything older is beyond-window anyway).
  `compact_after_age_ms` therefore moved onto the per-operative `resolve/5` cascade —
  it is a correctness constraint now, not a tuning knob.
- **Declaration**: `c.correction_window(ms)` + `c.beyond_window_policy(..)`; rides
  `AddOperativeResponse.metadata` → `ClassRegistry.decode_declarations/1`. Refusals: tier
  + `EventTimeFrom` + all-channels-keyed (SDK build); hold-window + snapshot age-floor
  ≥ window (controller, at start). Beyond-window default = `process_as_arrival` (it is
  today's behaviour — declaring a window must never silently start dropping data).
- **Limit worth knowing**: a correction overwrites the keys it re-emits; it does NOT
  retract a key the stale run emitted and the corrected run does not. Safe for
  event-time-bucketed keys (candles); documented in `data_durability.md`.
- **SCOPE DECISION — no `input.idx` side-file.** The hold window now actually bounds
  the log, so a header-only scan is O(retained). A durable index would add
  crash-consistency + truncation-trimming + rebuild for a speculative win.
- **NEVER reset the event-time high-water on a replay.** Every replay is preceded
  by a restore, which already rebuilt it from the change log. Zeroing it first
  (an early draft did) is lossy on a crash-resume whose unconsumed tail carries a
  LOWER max event time than the recovered state — the entity comes back with a
  high-water below the truth and silently reads a genuinely late input as on-time.
  Pinned by `a_replay_does_not_lower_the_event_time_high_water`.
- **`WalWriter.fold_records/3`** is now the ONE place the
  `decode_record → recurse → halt-on-torn-tail` walk lives. There were **11**
  copies across `wal_writer`/`input_log`/`time_travel`/`record_log`/`retention`;
  all but `scan_for_last_valid_seq` (its error branch truncates the torn tail —
  a side effect, not a fold) now go through it. `Wire.decode_effect_record/2` is
  the matching "header check + body decode" pair. `Retention.rewrite_offline/2`
  reuses `WalWriter.rewrite_keep_seq_gt/2` instead of its own byte-copy.
- **CLI**: `flz operative replay <operative/procedure/key> --from <seq|ts>`.
  Telemetry `[:flezha, :correction, :completed|:refused]`, `[:flezha, :state, :wal_rewind]`.
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `c.correctionWindow(ms)` + event-time slice.

## Workflow loop rewind (`workflow-loop-rewind`)

- **Marker, not truncation**: `rewound` = proc-journal entry type 6, target =
  the entry's own `iter` field (empty payload), appended with
  `current_epoch + 1` — the FIRST non-zero epoch anything writes. The journal
  stays append-only; `ProcJournal.live_entries/1` is the one fold that applies
  markers (**on `rewound(N)`, purge accumulated entries with `iter > N`; keep
  everything appended after it**). `restore_state/1`, `pending_entries/1`,
  `consumed_offset/1`, `committed_iterations/1` all run over it — so a rewound
  iteration's unflushed outbox **ceases to be owed**, not merely gets skipped.
- **Undo is derived, not marked**: `restore_state/1` also returns `rewind_undo`
  = proc_committed entries before the marker with `iter > N`, minus those with a
  `compensated` entry appended *after* their own commit (that "after its own
  commit" test is what keeps a re-run distinguishable from the original it
  replaced, and survives a second rewind). No done-marker, no completeness
  oracle → crash-mid-rewind resumes with exactly the outstanding work.
- **Wire**: ONE additive field, `ProcJournalState.rewind_undo = 8`
  (`repeated ProcCommittedProc`). No new control message — the controller writes the
  marker and rebinds; the folded journal is all the SDK sees.
- **SDK**: `drive_chain` gains one branch before the forward loop →
  `run_rewind_undo` reuses `plan_compensation(AllOrNothing, groups, &[])`
  verbatim (reverse iteration, reverse stage) — **forced** AllOrNothing whatever
  the class's `rollback_mode` says, since a rewind unwinds committed iterations
  by definition. `StageRecord` gained `iter` (the `compensated` entry's `iter`
  was hardcoded `0` before). Journals ONE `compensated` per proc — **even an empty
  one for a hookless proc**, which is what drains `rewind_undo`; and it does NOT
  consult the per-entity done markers (they are per *entity*, and an entity
  that ran in two rewound iterations must be undone once per iteration).
  The two undo passes deliberately stay separate — one is **marker**-backed
  (crash resume, per entity), the other **journal**-backed (rewind, per proc) —
  but share `plan_compensation` (the plan), `compensate_entity` (the effect) and
  `journal_undo` (the `compensated` record).
- **Resume = rebind, not park**: `Overwatch.handle_call({:rewind, N})` → guard →
  `append_rewound` → evict (`evict_runner/2`, extracted from `enter_suspended`)
  → `rehydrate/1`. No `parked` entry, no wake timer.
- **Coverage guard** (`rewind_guard/2`, controller-side, before anything durable
  changes): an unwound proc that staged plugin output MUST declare `compensate`.
  The keyed-channel half of the original spec is **unreachable for workflow** —
  `WorkflowEntitySink` stages `(channel, bytes)` and `outbox_items` drops the
  key, so a workflow emit carries no `flz.key`. Facts ride the EXISTING
  `AddOperativeResponse.metadata` map (`"compensable_procs" => "0,2"`, no proto
  change) → `ClassRegistry.put_compensable_ops/2` (same sidecar shape as the
  tier). The guard asks `ProcJournal.staged_channels_above/2` — the journal owns
  its fold, Overwatch owns the policy (`live_entries/1` is private).
- **`ClassRegistry.decode_declarations/1`** is the ONE place the `AddOperative`
  response is decoded → `%{effectively_once:, compensable_procs:}`. A new
  SDK-declared capability is added there and nowhere else. (`compensable_procs`
  `nil` = the class declared nothing — Java/stream/run-sugar — vs `[]` = a
  chain with nothing compensable; a rewind treats those differently.)
- **GOTCHA — a rewind does NOT replay inputs.** `ControlChannel` truncates the
  inbox to the consumed offset at every iteration commit
  (`control_channel.ex:629`), so a committed turn's input is GONE. Moving
  `consumed_offset` back cannot resurrect it. Rewind is **undo-and-continue**:
  the loop resumes waiting and the NEXT message becomes iteration N+1. (The e2e
  caught this — an earlier draft asserted re-delivery and failed.)
- **Entity State** is unwound by the `compensate` hooks (the saga contract), NOT
  by truncating the state WAL. A hookless proc leaves its State change in place.
- **CLI**: `flz operative rewind <instance> --to-iteration N`
  (`CliManager.operative_rewind/3`); `flz state journal` renders `rewound`.
  Telemetry `[:flezha, :workflow, :rewind]`.
- **Tests**: `proc_journal_test.exs` (epoch bump, fold purge, outbox no longer
  owed, `rewind_undo` drain, re-run + second rewind),
  `overwatch_suspension_test.exs` (rewind + both refusals),
  `cli_manager_rewind_test.exs`, `workflow_driver_scenarios` (reverse-order undo,
  hookless drain), e2e "loop rewind (rust/uds)" in
  `workflow_crash_resume_e2e_test.exs` (fixture `chat_rewind_workflow` —
  `ChatReplyUndoable` keeps its undo handle in State and retracts in reverse;
  asserts `flz.epoch` 0 before / 1 after).
- **Split**: stream late-input correction + `flz operative replay` were carved
  out into the `stream-late-input-correction` openspec — its design needs four
  primitives that do NOT exist (event-time→seq map, non-destructive WAL rebase,
  event-time-ordered replay splice, an actually-enforced input-log `hold_window`;
  `realign_epoch/4` wipes the WAL and every snapshot — it cannot be "scoped").

## Workflow entity retry (`workflow-entity-retry`)

- **Ladder (SDK, Rust)**: `run_entity_retrying` in `workflow/driver.rs` wraps the
  pool task — only the FAILING entity re-runs its batch (siblings stay buffered
  at the barrier), fresh sink per attempt (a failed attempt's staged output is
  dropped, never double-emitted). Backoff REUSES `dispatcher::retry_backoff`
  (the stream DLQ ladder's helper: `base × 2^(n-1)` ±50% jitter, capped
  `RETRY_BACKOFF_MAX_MS` = 5 s) and the wait is cancellation-aware
  (`sleep_cancellable`, ≤200 ms poll → a stop/`max_duration` ends it at once).
  Retries count into `entity::park::RETRIES_TOTAL` (same `data_heartbeat`
  metric as stream).
- **Policy**: `WorkflowOperative::retry_attempts()` (default 3 TOTAL attempts) +
  `retry_backoff()` (default 500 ms base) → resolved onto the SHARED
  `channel::RetryPolicy` in `WorkflowSpec.retry`; `0` collapses to one attempt
  (fail-fast); `attempts > 1` with a zero backoff is rejected at registration
  (`BuildWorkflowError::ZeroRetryBackoff`).
- **Classification**: `WorkflowError { message, non_retryable }` (was a tuple
  struct); `WorkflowError::non_retryable/1`, `From<String>/<&str>` stay
  retryable. A procedure body classifies with the EXISTING
  `ProcedureError::non_retryable` — `run_collecting` carries `is_non_retryable()`
  through to the ladder. A panic stays retryable (author never classified it).
- **Boundaries**: retry is strictly PRE-commit (a journaled proc is never
  retried); a crash mid-ladder resumes as a normal uncommitted-proc re-run
  (attempt counter resets). `run`-sugar has no entities → terminal on first Err.
  A stage error while the cancel token is set now reports `cancelled` (was
  `error`) — `run_stage_parallel`'s caller folds it through `abnormal_status`.
- **Tests**: `workflow_driver_scenarios` (recover / exhaust+compensate /
  non-retryable / attempts=0 / stop-interrupts-backoff / run-sugar counted
  once), `retry_policy_is_resolved_and_validated`, e2e "entity retry (rust/uds)"
  in `test/system/workflow_crash_resume_e2e_test.exs` (fixture `retry_workflow`,
  failures armed via `FLEZHA_WORKFLOW_RETRY_FAILS`). GOTCHA: an e2e/unit scenario
  must never busy-wait on the main test thread — it is the fake controller and
  must stay in `drain` to ack `ProcCommit`, else the driver's 30 s ack timeout
  fires (cancel from a helper thread instead).
- **Docs**: `docs/resilience/workflow_operative.md` "Entity retry: the rung
  before the abort" (incl. the workflow-vs-stream escape-ladder split),
  `docs/resilience/data_durability.md` DLQ "Workflow equivalence".
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `retryAttempts()` / `retryBackoff()` ladder.

## Workflow durable loop + suspension (`workflow-durable-loop-and-suspension`)

- **Loop (SDK)**: `WorkflowOperative::loop_mode()` → `Once` (default, the
  old single pass, byte-identical) | `PerInput` (one iteration per inbox
  item — chat turns) | `Chained` (seeded by the previous iteration's
  terminal handoff). `drive_chain` is now an iteration loop around the
  SAME stage machinery (`run_iteration` = the old for-loop body).
  Registration rejects a loop on run-sugar (`LoopWithoutChain`) and a
  durable `Chained` whose terminal proc has no codec'd output channel
  (`LoopTerminalWithoutCodec` — else a crash re-seeds empty and reads as
  "converged").
- **Iteration commit**: `IterCommit` (proto tag 30) / `IterCommitResponse`
  (31) at every iteration boundary → ONE `iter_committed(iter,
  consumed_offset)` journal entry (type 3, ACTIVE) carrying the terminal
  handoff; ack; THEN boundary `CompactEntity` per entity + outbox flush
  + inbox truncate (post-ack cleanup — a failure there costs work, not
  correctness). Every `proc_committed` is stamped with its enclosing `iter`.
- **KeepProgress is real**: `plan_compensation`'s `committed` is rebuilt
  from journaled `committed_procs` grouped by iter (`journaled_iterations`).
  Planner UNCHANGED. A journal-rebuilt iteration's `EntityRun.outputs` is
  empty (the at-least-once undo-handle-in-State contract) — the tests key
  on that (`undo-loop:1` in-flight vs `undo-loop:0` journaled).
- **Durable inbox**: `lib/flezha/state/proc_inbox.ex` — per-instance
  `inbox.wal` beside `proc.journal` on the SAME WalWriter substrate,
  identity `Registry.inbox_id/1` (`{inst, inst, "_proc_inbox"}`).
  **Offset = WAL seq + 1** (the substrate REQUIRES seq 0 first — an
  empty WAL receiving any other seq sheds the frame and pulls a baseline
  — while offset 0 must mean "nothing consumed"/"unjournaled").
  `Registry.@reserved_files` keeps `inbox.wal` out of `scan_dir` (it
  would otherwise decode as an Entity named "inbox").
- **Append-before-forward**: `Ingest.journal_input/2` — the ONE durable
  ingest seam (eff-once InputLog clause + durable-workflow ProcInbox clause).
  Stamps `flz.inbox_offset`; a duplicate (`flz.msg_id`, else payload hash,
  inside the undelivered window) returns nil = absorbed; a failed append
  is NOT forwarded.
- **Suspension**: SDK ships `IterPark` (tag 32) and exits WITHOUT
  `WorkflowCompleted` (`RunOutcome::Parked`). Controller journals
  `parked(N[, deadline])` (type 4, ACTIVE) THEN
  `Lifecycle.suspend/3` → `RemOperative` + `operative_set_runner(nil)`;
  Overwatch + registry entry STAY (zero *runner* resources).
  `Convergence.Diff`/`Loop` skip `suspended?` (else they'd rebind it and
  undo the eviction). `flz operative info` → phase `suspended`.
- **Rehydrate = bind**: `complete_init` always delivers the undelivered
  inbox tail (`deliver_inbox_tail`), so crash-resume / migrate / wake all
  use ONE path; `rehydrate/1` reuses the existing `{:unpark, ...}` migrate
  task. The wake deadline is an Overwatch `Process.send_after` re-armed
  from the journal at boot (NOT a Watchdog sweep — fires exactly, no
  scheduler); it wakes by appending a synthetic empty inbox item
  (`flz.wake=timer`), so racing wake sources collapse to one rehydration
  by construction. Boot: `suspended_on_disk?` (journal's last entry is
  `parked` AND nothing undelivered) → enter suspended, never bind.
- **Knobs**: `FLEZHA_WORKFLOW_PARK_IDLE_MS` (SDK-side, default 5 s
  hysteresis), `WorkflowOperative::wake_after()`.
- **CLI**: `flz state inbox <instance>`; `flz state journal` renders
  `iter_committed`/`parked`.
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `loopMode()` / `wakeAfter()` / `IterCommit` / `IterPark`.
- **Tests**: `workflow_driver_scenarios` (loop turns → iteration commits +
  park + resume-at-iteration; journal-backed KeepProgress/AllOrNothing),
  `loop_authoring_is_validated`, `test/flezha/state/proc_inbox_test.exs`,
  `proc_journal_test.exs` (iter/park entries),
  `control_channel_proc_commit_test.exs` (IterCommit journal→ack→truncate,
  boundary snapshot, IterPark→suspend),
  `test/flezha/operative/overwatch_suspension_test.exs` (park/rehydrate/
  timer/redeploy).
- **Docs**: `docs/resilience/workflow_operative.md` "Durable loop and
  suspension", `docs/resilience/data_durability.md` "the durable inbox",
  `docs/sdk_protocol_spec.md` `IterCommit`/`IterCommitResponse`/`IterPark`
  + `flz.inbox_offset`.
- **Unblocks**: `operative-rewind-and-correction` → `workflow-loop-rewind`.

## Workflow staged commit + transactional outbox (`workflow-staged-commit-and-outbox`)

- **Staging (SDK)**: inside a structured proc `ctx.emit` NEVER hits the wire —
  `WorkflowEntitySink` (per entity, per stage) buffers `(channel, bytes)`
  when `ChainCtx.journaling()` (durable class AND coordinator installed);
  otherwise (opted-out / transport-less) it forwards direct, as before.
  `WorkflowCollectSink::emit_plugin_bytes(channel, bytes)` carries the
  channel name (`descriptor.name`, same as the eff-once effect record).
  A failing stage drops its staged bytes with the `Err` — nothing leaks.
  `run`-sugar keeps the direct path (no barrier to stage against).
- **Transport**: extends the SAME `ProcCommit` handshake — `outbox`
  (`ProcOutboxItem{entity_id, seq_in_proc, channel, payload}`, tag 6) +
  `compensating` (tag 7). No second protocol. `ship_proc_commit/3` is shared
  by the forward barrier and the undo record; the undo wait ignores the
  cancel token (rollback IS the response to that cancel).
- **Journal**: `proc_committed` carries entities+handoff+outbox in ONE record;
  `outbox_flushed` (type 2, payload `[for_type u8]` — forward vs undo are
  gated independently) and `compensated` (type 5, outbox only) are now
  ACTIVE. Epoch = u32 in the WAL record **header** (bytes 0–3; byte 9 is the
  effect-record format-id, so no collision), 0 until rewind bumps it.
- **Flush**: `ProcJournal.flush_pending/2` is the ONE path — post-ack
  (`ControlChannel`, retry timer on `:no_overwatch`) and restore re-drive
  (`Overwatch.replay_persisted_entities`, gated `kind == :workflow`, passes
  its own pid). Journal state (not memory) decides what is owed →
  idempotent by construction. Delivery = `GenServer.cast(overwatch,
  {:io_out, payload, stamps})`, same fan-out as `WalWriter`'s eff-once
  outbox. Stamps: `flz.entity`, `flz.seq` = `<proc_idx>.<seq_in_proc>`
  (undo: trailing `.1` — numeric so the Postgres sink's
  `string_to_array(seq)::bigint[]` guard still orders it), `flz.epoch`,
  `channel`. Telemetry `[:flezha, :state, :proc_outbox_flush]`.
- **Ordering gotcha**: the undo record is journaled BEFORE the
  `compensation_done` marker — a crash between them re-runs the (idempotent)
  hook rather than losing its undo output.
- **Tests**: `workflow_driver_scenarios` (staging, leak-nothing, run-sugar
  direct, `UndoSaga` comp staging; the fake controller now FLUSHES each
  commit's outbox on ack, which is why the old output assertions still
  hold), `test/flezha/state/proc_journal_test.exs` (crash-before-flush
  re-drive with identical keys = truncate journal back to pre-flush bytes),
  `control_channel_proc_commit_test.exs` (sync on `:sys.get_state(channel)` —
  the marker append lands after the `:io_out` cast).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — staged commit + `ProcCommit` outbox.
- **Docs**: `docs/sdk_protocol_spec.md` `ProcCommit`/`ProcCommitResponse`,
  `docs/resilience/data_durability.md` "Workflow operatives: staged commit
  and the transactional outbox".

## Workflow proc-journal (`workflow-proc-journal`)

- **Store**: `lib/flezha/state/proc_journal.ex` — per-instance `proc.journal` at
  `<state_root>/<inst>/<inst>/proc.journal` on the WalWriter+FsyncBatcher
  substrate under reserved identity `Registry.journal_id/1`
  (`{inst, inst, "_proc_journal"}`); entry codec
  `[type u8][proc_idx u16][iter u32][payload]` inside normal WAL framing,
  type 1 = `proc_committed` (entities-ran + channel-tagged handoff blobs),
  2–5 reserved (tolerated on decode). Size bound =
  `:wal_high_watermark_bytes` → `{:error, {:oversized_handoff, ..}}`. The
  journal writer skips the outbox dedup side-file so it never registers a
  phantom Entity.
- **Handshake**: SDK→controller `ProcCommit` (proto tag 28) at every proc
  barrier of a durable structured workflow; ControlChannel appends +
  fsyncs, replies `ProcCommitResponse` (tag 29) under the SAME reference_id
  (journal-then-ack); driver blocks ≤30 s (`workflow/driver.rs
  commit_proc_boundary`), failure/timeout → terminal error + compensation.
  Telemetry `[:flezha, :state, :proc_commit]`.
- **Resume**: journal state rides `RestoreInitState.proc_journal`
  (`ProcJournalState`); `drive_chain` skips committed procs and seeds the next
  batch from the journaled handoff via the channel codec. Compensation
  precedence: a comp-**done** marker wins (intents alone don't — they also
  accumulate during forward progress). Skipped procs still roll back on a
  later abnormal exit (folded from intent markers).
- **Codec**: `.codec()` (postcard/serde) or `.codec_with(enc, dec)` on
  inter-proc channels; REQUIRED for durable structured workflow classes —
  registration fails `MissingInterProcCodec` naming the channel. Opted-out
  classes skip journal + handshake entirely (re-run-from-top preserved).
- **CLI**: `flz state journal <operative/instance>`
  (`CliManager.state_journal/2`).
- **Java SDK**: AT PARITY (`java-sdk-parity`) — `ProcCommit` handshake + journal-gated
  resume-skip. The e2e no longer branches per runtime.
- **Tests**: `test/flezha/state/proc_journal_test.exs`,
  `test/flezha/runner/control_channel_proc_commit_test.exs`, Rust
  `workflow_driver_scenarios` (gating + resume-skip + codec validation),
  e2e `test/system/workflow_crash_resume_e2e_test.exs` (proc A NOT re-run on
  rust; opted-out `optout_commit_block` re-runs from top).
- **Docs**: `docs/sdk_protocol_spec.md` `ProcCommit`/`ProcCommitResponse`,
  `docs/resilience/workflow_operative.md` "Durability" section.

## Dead-letter queue (`platform-dead-letter-queue`)

- **SDK retry-then-park**: both dispatchers re-run a failing invocation
  (returned error or panic/exception, one policy) up to the input
  channel's `retry_max` (default 3 total attempts, exponential backoff +
  jitter capped 5 s); `.retry_max(n)` / `.retry_backoff_ms(b)` on the
  channel builders, `ProcedureError::NonRetryable` /
  `ProcedureException.NonRetryable` parks immediately. Rust:
  `sdk/rust/src/operative/dispatcher.rs` retry loop +
  `sdk/rust/src/entity/park.rs` (blocking wire park, ack registry);
  Java: `ProcedureDispatcher.runOnce` + `runtime/state/DlqPark.java`
  (hand-rolled postcard encoder, fixture-pinned cross-language).
- **Protocol**: data-stream payload-id `park_payload` (postcard
  `ParkBody`, same outer frame as `entity_state_change`, zero header)
  acked by control command `ParkPayloadAck` (proto tag 27) AFTER the
  store fsync — park-then-advance, SDK re-sends every 2 s until acked.
  Tier-on parks skip the frame entirely: `EffectRecordBody` gained a
  trailing `dlq: Option<DlqEntryBody>` so the park and the
  consumed-offset advance are ONE atomic WAL record; the WalWriter
  materializes the store entry synchronously before
  `InputLog.record_consumed` (watermark un-freezes after the park).
- **Store**: `lib/flezha/dlq/store.ex` — one GenServer,
  `app_data/dlq/<class>/<procedure>/entries.jsonl` + `<id>.bin` blobs,
  fsync-before-ack, idempotent on origin identity
  (`entity@offset` tier-on, `channel:sha256(payload)` tier-off);
  re-park while `status=replaying` bumps `generation`. Retention age AND
  size (30 d / 4 GiB, more restrictive wins), loud
  `[:flezha, :dlq, :retention_dropped]` per deletion. Config `:dlq`
  block in runtime.exs (`FLEZHA_DLQ_DIR`, `FLEZHA_DLQ_RETENTION_DAYS`).
- **CLI**: `flz dlq list|show|replay|drop` (`cli/src/dlq.rs` →
  `CliManager.dlq_*`). Replay re-enters via
  `GenServer.cast(overwatch, {:io_in, payload, %{"channel" => ...}})` —
  fresh output identity by construction (new input → new WAL seqs, NO
  epoch bump); success detected by absence of a re-park within
  `:dlq[:replay_grace_ms]` (5 s) then entry removed; failing replay
  re-parks generation+1. `drop`/`replay` audited via `:flezha_dlq`
  events (TelemetryBridge mapping + audit-sink allow-list).
- **Observability**: `dlq_parked_total`, `dlq_dropped_by_retention_total`,
  `dlq_usage` metrics bridge to the bus (source `:flezha_dlq`).
- **Workflow profile**: out of scope — AbortAll + saga compensation is the
  equivalent escape; nothing is written to the DLQ store.
- **Tests**: `sdk/rust/tests/dlq_retry.rs`, Java `DlqRetryTest`,
  `test/flezha/dlq/store_test.exs`, WalWriter park cases in
  `wal_writer_outbox_test.exs`, CLI `cli_manager_dlq_test.exs`, e2e
  `test/system/dlq_e2e_test.exs` (Rust + Java + tier watermark;
  `--include system [--include java_sdk]`). Poison test procs:
  `poison_probe` (Rust) / `java_poison_probe` (rebuild the committed jar
  with `scripts/build-dist.sh` after SDK changes!) / `tier_store` value
  `"poison"`.
- **Docs**: `docs/resilience/data_durability.md` "Dead-letter queue",
  `docs/sdk_protocol_spec.md` `park_payload`/`ParkPayloadAck`, glossary.

## Postgres sink plugin (`effectively-once-output-postgres-sink`)

- **Modules**: `lib/flezha/plugin/provider/postgres.ex`
  (`Flezha.Plugin.Provider.Postgres`, `:io_binding`, registered as
  `operative_postgres`) + `postgres/{sink_sup,sink_worker}.ex`. `SinkSup`
  and Kafka's `ProducerSup` are thin wrappers over the shared
  `Flezha.Plugin.Provider.WorkerSup` core (one Registry + DynamicSupervisor
  instance per provider family; ensure/reap/stop semantics, reuse policy
  injectable — Kafka passes `ProducerWorker.reusable?/1`). A re-prepared
  plugin reuses its live worker so buffered records survive Overwatch
  restarts. New top-level dep `postgrex ~> 0.22` (active only when the
  plugin is configured, like `:kafka_ex`).
- **Semantics**: per flush batch ONE `Postgrex.transaction` — bulk upsert
  into `flezha_records` (key = `flz.key`, fallback `<entity>/<seq>` for
  keyless tier outputs, unique `_anon/N` for unstamped records) + one
  `flezha_sink_progress` watermark upsert per entity. Both guarded by
  `(epoch, string_to_array(seq)::bigint[])` row comparison so re-drives
  and stale epochs are byte-exact no-ops. Same-key records within a batch
  are collapsed to the newest identity first (`ON CONFLICT` double-touch).
- **Failure posture**: unreachable DB → retry with exponential backoff
  (`retry_backoff_ms` 250 → `retry_max_backoff_ms` 5000), records kept;
  enqueue callers park past `queue_max` (10_000) — delay, never loss.
  `auto_create` default false: missing tables fail `health/1` with
  `{:missing_tables, [...]}` and no DDL runs. `health/1` also reports
  `{:commit_stalled, reason}` when pending + failing past
  `unhealthy_after_ms` (30 s) — feeds the plugin-health → convergence path.
- **Credential hygiene**: password rides a closure into DBConnection's
  `:configure` (never in worker state/child args); `init_provider` returns
  opts with `:password` redacted; failure logs carry `host:port/db` only.
- **Tests**: `test/plugin/provider/postgres_sink_test.exs`
  (`POSTGRES_INTEGRATION=1 mix test --include postgres_integration`, needs
  `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink` —
  service is named `postgres-sink` on host port **5433** because the
  flezha_market dev stack shares compose project "dev" and owns `postgres`
  on 5432); e2e crash matrix `test/system/effectively_once_postgres_e2e_test.exs`
  (also needs `--include system`). Shared kill/rebind harness for both
  tier e2e suites: `test/helpers/tier_e2e_harness.ex`.
- **Docs**: `docs/resilience/data_durability.md` "Delivery semantics".

## Deterministic effects + output identity (`deterministic-effects-and-output-identity`)

- **Determinism contract** (documented on the `Procedure` rustdoc): `run`
  / `apply_change` / `compensate` obtain time via `ctx.now()` and
  identifiers via `ctx.stable_id()` — no ambient `SystemTime::now()` /
  `Uuid::new_v4()` / external reads. Verified by
  `ProcedureTestHarness::replay_emits_equal` (output-side sibling of
  `replay_equals`; injectable fixed/scripted wall clock).
- **Clocks** (`sdk/rust/src/procedure/clock.rs`): no static
  `Clock::now()` — the dispatcher evaluates the declared clock once per
  invocation via `ClockFor<Input>` (all `Time = u64`). `WallClock` =
  epoch ms at dispatch entry; `EventTimeFrom<F>` = `F:
  TimeExtractor<Input>` over the current input (compile-enforced);
  `LogicalClock` = the raw durable `change_seq` (re-armed `terminal+1` on
  restore). Build rejects `LogicalClock` + `Change = NoChange`
  (`BuildTopologyError::UnservableClock`).
- **`ctx.stable_id()`**: frozen FNV-1a-128 over
  `entity identity ‖ change_seq ‖ per-invocation ordinal`.
- **Keyed channels**: `.keyed()` (plugin wire required, else
  `KeyedChannelWithoutPluginWire`) + `ctx.emit_keyed(key, value)`; plugin
  frames stamped `flz.entity` (`instance/procedure/entity_key`),
  `flz.seq` (`<change_seq>.<emit_ordinal>`, read after flush-before-emit),
  `flz.key`. User `flz.*` metadata rejected (`ReservedMetadataKey`).
  Non-keyed channels byte-identical (guarded by test + criterion bench).
  `flz.seq` is NOT an exact append-dedup handle until
  `effectively-once-output` journals it; workflow operatives reject keyed
  emits (their plugin wire carries no metadata). Controller untouched —
  stamps pass through `StreamChannel` → `Overwatch` `:on_output`.
- **Downstream (not this change)**: flezha_market migration to the ctx
  APIs; Java SDK ctx parity; Kafka producer adopting `flz.key` as message
  key.
- **Docs**: `docs/sdk_protocol_spec.md` "Output identity stamps",
  `docs/resilience/data_durability.md` "Output identity".

## Snapshot compaction is now real (`entity-compaction-is-a-stub`)

- **The controller half was always built; both SDKs answered `CompactEntity`
  with a STUB** (`command/compact.rs` → `snapshot_seq: 0`, no frame; Java
  `CompactEntityHandler` → `new byte[0]`). Measured live before the fix: 9
  `compaction_trigger`, 8 `compaction_timeout`, 0 `compaction_completed`, 0
  snapshot files, flat WAL. So the earlier "Java at full parity / persistence
  green" claims did NOT hold for this path on EITHER SDK until this change.
- **SDK emit** = the producing half. Rust adds `ProcedureDispatcher::snapshot_frame`
  (the type-erased MIRROR of `restore_state`, NOT a third `build_state_fns` fn
  pointer — reuses the same postcard machinery restore decodes); Java's
  `CompactEntityHandler` shares the `EntityLocator.findStream` scan with
  `RestoreInitStateHandler` (cx-refine extraction) + Kryo. Both
  DRAIN pending changes first (`flush_one` / new `EntityFlusher.flushEntity/1`)
  so state == shipped WAL, then serialize under the state lock and ship
  `entity_state_snapshot` (new payload-id const in both sinks) stamped with
  `change_seq - 1` = the highest seq the snapshot folds. **GOTCHA — the codec
  is the trap**: postcard (Rust) / Kryo (Java), NOT the JSON migration
  serializer, or restore can't decode it (and compaction being unexercised, it
  would pass review). Java `NativeTransport.sendEntityStateSnapshot` DELETED
  (0 callers, javadoc lied about stamping the payload-id).
- **Seq semantics (load-bearing)**: controller `truncate_up_to(seq)` =
  `rewrite_keep_seq_gt` (keeps `seq > snapshot_seq`), and `establish_baseline`
  sets `last_persisted_seq = snapshot_seq`, so `snapshot_seq` MUST be the
  highest ALREADY-shipped seq (`change_seq - 1` after draining) — stamping
  `change_seq` (the next-to-assign) would dedup-drop the next real record.
- **Controller now inspects `CompactEntityResponse`** (`dispatch_compact_entity`
  → the shared `ControlUnit.send_control_await/3` request/reply off-process so a
  slow snapshot never blocks the Watchdog sweep / WalWriter pull; **falls back to
  a direct `send_control` when no ControlUnit exists** — isolated unit tests).
  cx-refine unified that request/reply seam: `send_control_await` now backs
  `send_operative_message`, the credit-window handshake, Overwatch's private
  `send_control`, and the compaction inspection. `{0,0}` against a
  non-empty WAL (`Restore.has_persisted_state?`) = failed handshake →
  `[:flezha, :state, :compaction_failed]` → `Watchdog.compaction_failed`.
- **The timeout handler was itself a silent no-op** (`Recovery.resync/2` with no
  `runner_id` → `{:error, :no_runner_id}` discarded with `_ =`). Now the
  `compactions` map carries `{started_at, runner_id}`; the Watchdog holds one
  `Convergence.Backoff` keyed `{:compaction, entity_id}` (added ONE tiny
  `attempt/2` accessor — no strike map). Below give-up: record + backoff-gated
  retry, NO disruptive resync of a live Entity. At give-up (attempt ≥ 4):
  `[:flezha, :state, :compaction_gave_up]` alarm + a real resync WITH the
  runner_id. Completion resets the strike.
- **`compact_after_bytes` was unreachable by ANY caller** (`_procedure_id`
  discarded). Now `PersistenceResolver.compact_after_bytes(operative_class,
  metadata)` routes through the `resolve/5` cascade → `ClassRegistry.compact_
  after_bytes` (SDK declaration) → global default; the SDK ships it in
  `AddOperative` metadata (smallest across procedures, only when ≠ 8 MiB
  default — byte-identical otherwise) on BOTH arms; decoded in
  `decode_declarations/1` + persisted via `put_compact_after_bytes`. Watchdog
  now passes the CLASS, not the procedure id.
- **e2e on both arms**: `test/system/compaction_e2e_test.exs` (Rust) +
  `test/java_sdk/compaction_e2e_test.exs` assert a snapshot FILE appears and the
  WAL strictly shrinks against a real runner — the `assert_receive
  compaction_completed` is what fails against a stub. Round-trip unit tests:
  Rust `snapshot_frame_round_trips_through_restore`, Java
  `compactEntityShipsASnapshotThatRoundTripsThroughRestore`. Controller units:
  `watchdog_test` (backoff/give-up), `persistence_resolver_test`,
  `class_registry_test`, `add_operative_response_test`.
- **NOTE**: `snapshot-baseline-durability`'s baseline-seed pull (a cold
  controller pulls a full-state snapshot via `CompactEntity`) is now genuinely
  reachable for the first time — its `dispatch_resync` empty-WAL branch was
  inert while the SDK stubbed. See that change's spec.

## Per-Entity state persistence (`operative-state-persistence`)

- **Modules**: `lib/flezha/state/` — `Registry`, `Wire`, `Metrics`,
  `Outbox`, `PersistenceResolver`, `Supervisor`, `WalWriter`,
  `FsyncBatcher`, `Snapshot`, `Retention`, `Restore`, `TimeTravel`,
  `TestHelpers`.
- **On-disk layout**: `app_data/state/{operative_class}/{procedure_id}/{entity_key}.{wal,snap.{seq},outbox.dedup}` (flat — `runner_id` was dropped by `auto-restore-on-boot`; `Flezha.Boot.StatePathMigrator` handles upgrade migration).
- **Wire payload-ids**: `entity_state_change`, `entity_state_snapshot`,
  `delete_entity_state` (data stream); `RestoreInitState`, `RestoreInitStateResponse`,
  `CompactEntity`, `CompactEntityResponse`, `EntityMailboxStatus`
  (control channel). See `docs/sdk_protocol_spec.md` "Per-Entity
  State Persistence".
- **Dynamic entity mode**: SDK-side
  `sdk/rust/src/entity/dynamic.rs` (`DynamicEntityRegistry` +
  `Reaper`). Dynamic-mode Procedures (`entity_key()` overridden, empty
  `entity_keys()`) create Entities lazily per key. Eviction is OFF by
  default (`entity_idle_ttl() -> None`); when a TTL is opted in, the
  per-Procedure reaper evicts idle Entities **destructively** — it ships
  `delete_entity_state` (SDK→controller) and the recreate cold-starts
  fresh (no restore-from-disk, by decision 2026-06-16). Controller handler:
  `Flezha.State.Supervisor.delete_entity/1` (stops WalWriter, drops
  FsyncBatcher mark, unlinks files via `Registry.delete_files/1`).
- **Runtime config knobs**: `:state_root` (default `app_data/state`),
  `:agent_fsync_interval_ms` (5 ms), `:wal_high_watermark_bytes`
  (16 MiB), `:wal_low_watermark_bytes` (`high / 2`),
  `:time_travel_retention_check_interval_ms` (60 s),
  `:time_travel_retention_hours` (per-Operative map), `:operative_persistence`
  (per-Operative map), `:default_compact_after_bytes` (8 MiB, env
  `FLEZHA_COMPACT_AFTER_BYTES`; SDK trait default in lockstep),
  `:compact_after_age_ms` (300 s, `0` disables — and is `0` in `:test`;
  env `FLEZHA_COMPACT_AFTER_AGE_MS`).
- **Snapshot baseline durability (`snapshot-baseline-durability`)**: the
  `Watchdog` sweep auto-triggers `CompactEntity` on WAL **size or age**
  (`[:flezha, :state, :compaction_trigger]`); a fresh controller with an
  empty WAL for a live runner's Entity **pulls** a baseline
  (WalWriter awaiting-baseline sheds + one `CompactEntity`,
  `[:flezha, :state, :baseline_seed]`) instead of erroring; the resync
  path never pushes an empty `RestoreInitState`
  (`Restore.has_persisted_state?/1`, zero-byte WAL ≡ no state).
- **Credit-grant flow control (`credit-grant-flow-control`)**:
  runner→controller state-stream back-pressure is credit-based — the
  `StreamChannel` offers `CreditWindowInit` on the first state frame
  (per-Operative `:credit_window_bytes`, default `wal_high_watermark_bytes`;
  kill switch `FLEZHA_CREDIT_FLOW_CONTROL`), debits each frame's wire size,
  and mints one batched `CreditGrant` per fsync-drain below
  `:credit_low_watermark_bytes` (default `window / 2`). SDK windows:
  `sdk/rust/src/entity/credit.rs`, `sdk/java .../state/CreditWindow.java`
  (park in the flusher under the buffer lock; `credit_parked` in
  `data_heartbeat` metadata). Credit sessions get no `EntityMailboxStatus`;
  shed/overrun on one emits `[:flezha, :state, :credit_anomaly]`. Soak:
  `test/load/credit_soak_test.exs` (`MIX_RUN_CREDIT_SOAK=1`). The
  source-ingestion credit loop is deferred until `effectively-once-output`
  lands (input-log + min-watermark do not exist yet).
- **CLI**: `flz state list <operative>`, `flz state at <operative/procedure/key> <seq|ts>`,
  `flz state rotate <operative/procedure/key>`, `flz state retention <operative> <hours>`,
  `flz state delete <operative/procedure/key>`. Implemented in `cli/src/state.rs`;
  RPCs into `Flezha.Service.CliManager.state_*/N`.
- **Detailed notes**: `docs/resilience/state_persistence.md`.

## Last-runner migration keeps its state (`fix-last-runner-migration-state-loss`)

- **The three defects were one silent failure.** A controller losing every
  runner could neither keep its operatives nor move them, and the healing
  machinery destroyed its own evidence.
- **GOTCHA — runner ownership is NODE-LOCAL.** `ControlUnit` and the
  `StreamChannel` registry live in the per-node `Identificator` Registry and
  `FleetManager` is a per-node GenServer, so an Overwatch can only bind to a
  runner connected to ITS OWN node. `Convergence.Heal.rebind_operative/3`
  handing a local target to a remote Overwatch fails by construction
  (`{:ping_failed, :no_control_unit}`).
- **GOTCHA — `runner_migrate` used to commit the binding BEFORE the ping.** A
  failed migrate then left the operative naming a LIVE runner it could not
  reach; `Probe` read `bound_runner ∈ active_runners`, `Diff` dropped it from
  `misbound_operatives`, and the delta went permanently empty. Symptom: an
  operative that `flz operative list` shows as placed and healthy while every
  frame drops and convergence reports nothing to do. The binding is now
  committed only after the runner answers.
- **GOTCHA — the handover's state source is the RUNNER.**
  `prepare_handover_state/1` calls `serialize_operative_sdk(state.runner_id, ..)`,
  so a migration caused by that runner's death ships `<<>>`. Setting
  `arrived_via_hotswap? = true` on that empty handover suppressed the WAL
  restore at bind time AND the `kick_restore` repair — fresh start, replay
  locked out, state readable on disk the whole time. The flag is now set only
  when the payload was non-empty.
- **`Flezha.State.WalTransfer`** — destination-pulled chunked `:erpc` copy of a
  Entity's durable files. Source answers reads only (`entities/1`,
  `manifest/1`, `read_chunk/3`); destination writes `.transferring` → fsync →
  atomic rename. `read_chunk/3` resolves the requested name against
  `Registry.entity_files/1` (extracted, also backs `delete_files/1`) rather
  than joining onto the state dir — a peer must not be able to read arbitrary
  paths. `prepare_handover_state/1` stamps `source_node`; an empty handover
  triggers `adopt_source_state/2` → pull → the existing `{:kick_restore, _}`.
- **`hub_migrate` drains instead of no-opping.** It had been reduced to
  `stop_hub_if_vacant/1`, which no-ops exactly when the hub holds operatives —
  i.e. whenever there is something to migrate.
  `HubManager.drain_hub/2` is the ONE drain seam (extracted from
  `Shutdown.Coordinator`, including the load-bearing `await_local_children_gone`
  — `Migration.drain/2` returns before the async handovers land). Runs off the
  FleetManager loop: it blocks to its deadline and a runner's disconnect call is
  waiting on that process.
- **NO `ProcessHub.leave/2` was written.** The spec asked for it "or whichever
  ProcessHub-provided leave function honors `HotSwap{handover: true}`" —
  `Migration.drain/2` already is exactly that, and flezha already consumed it.
- **GOTCHA — the migrate drain must be SHORT** (5 s vs graceful shutdown's 60 s).
  A drain holds the node out of the namespace's distribution for its whole
  duration, and `hub_migrate`'s is fire-and-forget: a 60 s one outlived the
  namespace it drained and broke whatever used that namespace next. Same hazard
  in production when a runner reconnects seconds later.
- **GOTCHA — restoring a stopped hub also needs its runner hooks.** A fresh hub
  has no per-runner `child_registered` hook, so `hook_insert_pid` never fires,
  the StreamChannels never learn the operative pids, and every later data-plane
  read (entity state, migration, drain handover) silently gets nothing back
  while everything reads healthy. Restore is `ensure_hub(ns)` PLUS
  `HubManager.register_handler(ns, runner)` per `FleetManager.active_runners(ns)`.
  This bit `hub_manager_registry_test.exs`, whose `durable_registry_pending?/0`
  setup stops every OTHER running hub — including the suite-shared `default`,
  whose absence surfaces as the misleading "no runners in namespace default"
  (`Validator.valid_namespace/1` reports a dead HUB with that string).
- **Pinned by** `test/flezha_test.exs:700` ("runner disconnect migrate hub") on
  its ORIGINAL assertion, `test/flezha/state/wal_transfer_test.exs` (incl. a
  real two-BEAM-node pull with distinct state roots, `:multi_node`), and the
  two new cases in `test/operative/overwatch_async_migrate_test.exs`.
- **Limit**: one-shot at migration time. A controller that dies WITH its disk
  loses that state; replication is still a non-goal.

## Auto-restore on boot (`auto-restore-on-boot` + `consume-recovery-mode-and-self-heal`)

- **ProcessHub dependency — TEMPORARILY back on the GitHub `dev` branch**
  (`alfetahe/process-hub`), pending the next hex release; revert
  `process_hub_dep/0` to a `"~> 0.6.x"` requirement when it ships.
  `mix.lock` pins the commit, so `mix deps.get` does NOT follow the
  branch: `make refresh-process-hub` takes the tip and rewrites the lock
  (commit it — CI and every other build path use the lock as-is), and
  `docker/prod/Dockerfile.controller` unlocks it so the image takes the
  tip on a cache-missed layer. `FLEZHA_PROCESS_HUB_PATH` still overrides
  with a local checkout.
- **Modules**: `lib/flezha/boot/` — `StatePathMigrator`, `RecoveryHooks`.
  The `StatePathMigrator` runs synchronously from
  `Flezha.Application.start/2` before `Flezha.State.Supervisor`.
  Cross-controller state movement lives in `Flezha.State.WalTransfer`
  (see `fix-last-runner-migration-state-loss`): a destination-pulled,
  one-shot copy at MIGRATION time. Ongoing cross-node WAL replication
  remains a non-goal — a controller that dies with its disk takes its
  operatives' state with it.
- **THE MARKER IS GONE — recovery is an ORPHAN RECONCILE.** There is no
  `cluster.healthy`, no boot mode, no arming step. Every node runs a
  round `reconcile_grace_ms` after coordinator start (then at most once
  per `reconcile_interval_ms`) that STARTS THE DIFFERENCE between its
  durable registry and what the cluster is observed to hold: a dead
  cluster's difference is the whole list (it restores itself), a live
  cluster's is empty (it starts nothing). **That first round is what
  reaches `:normal`, so the grace IS the boot gate** —
  `RecoveryGate`/`BootLoader.await_namespace_hubs` block on it, which is
  why it is 45 s (above the runner handshake window; an orphan start
  needs a runner to place against) and why every recovery test shrinks
  it to upstream's 1 s floor.
- **HubManager wiring**: `Flezha.Service.HubManager.process_hub/1`
  passes `registry_backend: {:durable_ets, [path: ...]}` and
  `auto_recovery: [reconcile_grace_ms:, reconcile_interval_ms:,
  stopped_row_ttl_ms:]` (from `auto_recovery_config/0`) to every
  `ProcessHub.child_spec/1`. After each `start_hub/1` it calls
  `Flezha.Boot.RecoveryHooks.register/1`, which forwards four hooks to
  telemetry: `pre_recovery_replay`, `post_recovery_replay`,
  `reconcile_round` and `reconcile_duplicate`.
- **GOTCHA — a deprecated `:auto_recovery` key is accepted with a WARN
  and silently ignored**, which is indistinguishable from working.
  `hub_manager_test`'s `auto_recovery_config/0` case round-trips the
  config through the real `Recovery.parse_config/1` for exactly that
  reason — it catches both an unread key and an out-of-range value.
- **GOTCHA — `ProcessRegistry.dump/1` INCLUDES stopped rows.** A
  deliberate stop now marks the row `:stopped` (surviving the stopping
  node's absence) instead of deleting it, and the row lives
  `stopped_row_ttl_ms` = **30 days**. Every reader meaning "what should
  be running" goes through `HubManager.live_registry_dump/1`
  (`dump_all(include_unbound: false, include_stopped: false)`) — `flz
  operative list`, convergence's intent-mismatch check, the durable
  counts. Using bare `dump/1` there resurrects an operative the
  operator stopped.
- **GOTCHA — two loops now start operatives.** The reconcile starts
  orphans AND `Convergence.Health` binds/rebinds on its own 5 s tick;
  neither is gated on the other. `reconcile_duplicate` telemetry is the
  tripwire for whether they actually contend in prod.
- **Recovery tracker**:
  `Flezha.Service.HubManager.RecoveryTracker` is a per-supervisor
  GenServer subscribed to `[:process_hub, :recovery, _]` telemetry.
  `HubManager.recovery_state/1` and `await_recovery_complete/1,2`
  expose the per-hub state machine; `Flezha.BootLoader.await_namespace_hubs/0`
  and `Flezha.Runner.FleetManager.init/1` gate registry reads on it.
- **Boot CLI**: `flz boot status`, `flz boot await`,
  `flz boot import-legacy-snapshot` — implemented in `cli/src/boot.rs`
  / `Flezha.Service.CliManager.boot_*`. `flz cluster status` surfaces
  `recovery_state` per node. **DELETED with the marker**: the
  `flz cluster prepare-recovery` verb, `CliManager.cluster_prepare_recovery/1`
  and its table printer, and the `recovery_armed` column.
- **Start-fresh workflow**: `flz controller stop && flz reset --yes`
  wipes durable runtime data (`app_data/state/*`, `app_data/process_hub/*`).
  Implemented in `cli/src/reset.rs`; refuses to run while the Controller
  is up. See `docs/operations/reset.md`.
- **Persistence layers**: ProcessHub durable registry at
  `app_data/process_hub/<hub_id>/registry.dets` (cspec + `opts[:plugins]`)
  + per-Entity WAL at `app_data/state/<operative_class>/<procedure_id>/...`
  (change-by-change progress).
- **Plugin enable/disable** uses cspec-write-first ordering via
  `ProcessRegistry.update/3`; `Flezha.Service.HubManager.update_operative_plugins/3`
  was deleted (the persistent registry covers durability).
- **Runtime config knobs**: `FLEZHA_PROCESS_HUB_RECONCILE_GRACE_MS`
  (45 s), `FLEZHA_PROCESS_HUB_RECONCILE_INTERVAL_MS` (30 s),
  `FLEZHA_PROCESS_HUB_STOPPED_ROW_TTL_MS` (30 d),
  `FLEZHA_AUTO_RESTORE_ENABLED` (default `true`; `false` disables the
  reconcile entirely and reverts to in-memory `:ets`).
- **Removed by `consume-recovery-mode-and-self-heal`**:
  `Flezha.Boot.WalTransfer`; `:pre_replay_fleet_wait_ms` config key;
  `Flezha.Reconciler.{Loop, Actions, Health, Probe, Activity, Leader}`
  (replaced by `Flezha.Convergence.Health`); `Flezha.State.Outbox`;
  `Flezha.Reconciler.Supervisor` (top-level); `FLEZHA_RECONCILER_ENABLED`,
  `FLEZHA_LEADERSHIP_ENABLED`, `FLEZHA_RECONCILE_*` env vars.
- **Detailed notes**: `docs/resilience/auto_restore_on_boot.md`.

## Convergence Health watcher (`consume-recovery-mode-and-self-heal`)

- **Modules**: `lib/flezha/convergence/health.ex` — the single
  controller-side GenServer driving bind/rebind/plugin-restart
  decisions. `lib/flezha/healthz/server.ex` — opt-in `:gen_tcp`
  HTTP/1.1 `/healthz` listener following the
  `Flezha.Observability.Export.PrometheusHttp` pattern.
- **Supervision tree**: `Flezha.Convergence.Health` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.State.Watchdog`;
  `Flezha.Healthz.Server` is inserted right after, gated on
  `:healthz_enabled` (default `false` — flezha_market opts in via
  its ConfigMap).
- **Behaviour**: one tick per `convergence_tick_ms` (default 5_000).
  On init: `HubManager.await_recovery_complete/1` blocks emissions
  until every per-namespace hub reaches `:normal | :skipped`. On
  every tick: walk `HubManager.get_all_operatives/1` per running
  namespace, classify each operative as `:healthy | :degraded | :failed
  | :unbound`, emit at most one action per operative (`:bind`,
  `:rebind`, `:restart_plugin`, `:noop`).
- **Output-stall → forced rebind**: subscribes to
  `[:flezha, :state, :output_stall]` (emitted by
  `Flezha.State.Watchdog`); on receipt, queues a `:rebind` with
  exponential back-off `5s/30s/60s/300s`. Single signal closes the
  silent-freeze failure class.
- **Plugin escalation**: `handle_plugin_failure/2` tracks strikes
  per operative-plugin and escalates to an operative restart at
  `convergence_plugin_strike_threshold` (default 2). Emits
  `[:flezha, :convergence, :plugin_restart]` and
  `[:flezha, :convergence, :plugin_escalated]` telemetry.
- **/healthz contract** (when `FLEZHA_HEALTHZ_ENABLED=true`):
  `GET /` → `pong\n`; `GET /healthz` → `200 healthy`,
  `200 recovering` (kubelet must not restart during planned
  recovery), or `503 unhealthy` with `reason ∈ tick_stale |
  output_stalled | recovery_failed`. Unknown paths → 404; non-GET
  → 405; malformed → 400. Bind defaults to `127.0.0.1`.
- **Runtime config knobs**: `FLEZHA_USE_CONVERGENCE_HEALTH`
  (default `true`), `FLEZHA_CONVERGENCE_TICK_MS` (default 5_000),
  `FLEZHA_CONVERGENCE_OUTPUT_STALL_GRACE_MS` (default 60_000),
  `FLEZHA_CONVERGENCE_PLUGIN_STRIKE_THRESHOLD` (default 2),
  `FLEZHA_HEALTHZ_ENABLED` (default `false`),
  `FLEZHA_HEALTHZ_PORT` (default 9569).
- **flezha_market deployment**: `kubernetes/flezha-daemonset.yaml`
  ships a `livenessProbe: httpGet /healthz:9569` block;
  ConfigMap sets `FLEZHA_HEALTHZ_ENABLED=true` and
  `FLEZHA_PROCESS_HUB_REGISTRY_ROOT`. There is no cold-boot operator
  workflow any more — the reconcile is the recovery.
- **Fresh-process runner takeover**: phase 5 of the change keeps
  the takeover on the same self-healing loop instead of bolting on
  a parallel path. `FleetManager.handle_call(:runner_connect, ...)`
  dispatches `:runner_connected_event` and returns; nothing else.
  `Flezha.Convergence.Health` subscribes to that event via
  `EventManager.add_handler/1` and runs an immediate tick. The
  existing `:rebind_if_runner_available` action body — already
  responsible for migrating operatives whose `bound_runner` is no
  longer active — flips the registry tag via
  `HubManager.operative_set_runner/3` and seeds the new runner's
  pre-spawned `StreamChannel.operative_mapping` via
  `HubManager.hook_insert_pid/2` (the same fan-out the
  `child_registered` hook uses), then emits
  `[:flezha, :runner, :reconnected_reassigned]` +
  `[:flezha, :runner, :reconnected]` (mode: `:reassigned`,
  reason: `:convergence_rebind`) per reseated operative. The existing
  back-off (5s/30s/60s/300s) carries the retry semantics — a
  failed Overwatch migrate leaves the registry untouched and the
  operative stays `:degraded` for the next tick.

## Data-plane liveness instrumentation (`data-plane-liveness-instrumentation`)

- **Purpose**: localize a silent `io_in`/output stall to one layer.
  PURE instrumentation — zero data-plane behaviour change, no
  remediation. Closes the 22:14 candle incident's "which layer
  wedged?" gap (BEAM channel vs NIF driver thread vs runner).
- **NIF driver self-report**: `flz_nif/src/driver_stats.rs` —
  `DriverStats{loop_iterations, last_loop_ms, cmd_queue_depth:
  AtomicU64; streams: Mutex<HashMap>}` shared `Arc` into the
  flz-uds-driver loop (`uds_driver.rs` bumps the heartbeat once per
  iteration, rebuilds the per-stream snapshot once per iteration —
  never per frame). `driver_stats/1` NIF (in `server.rs`, registered
  via auto-discovery `init!`) returns the map; `:undefined` for QUIC
  (`TransportServer.stats: Option<Arc<DriverStats>>`).
- **BEAM counters**: `lib/flezha/transport/liveness_counters.ex` —
  always-on `:counters`/`:atomics` + public ETS registry (mirrors
  `Observability.Counters`). `StreamChannel` bumps `io_in_received /
  send_frame_calls / nil_drops / flz_data_received / last_send_ms`;
  `Operative.Overwatch` bumps `io_in_received / forwarded / filtered_nil`.
  Dead-owner entries pruned on `snapshot_all/0`.
- **Metric emission**: `Transport.Server.get_stats/1` merges
  `Native.driver_stats`; `OtpBackend.TransportSource` (the
  `:flezha_transport` source) emits per-transport driver metrics +
  flat global BEAM aggregates (bounded cardinality — classifier does
  per-entity localization).
- **Stall classifier**: `lib/flezha/convergence/stall_classifier.ex`
  — pure `classify(current, prev, opts)` returning one of
  `driver_loop_frozen | nif_queue_backing_up | socket_write_blocked |
  runner_not_consuming | channel_not_sending | operative_not_forwarding
  | :ok` (deepest-wedged-layer-wins; each verdict has an "is it being
  fed?" advancing-delta precondition). `Convergence.Loop` builds the
  snapshot per tick and emits `[:flezha, :data_plane,
  :stall_localized]` (with `%{layer: …}`) on verdict change —
  detection only, feeds NO heal; bridged to the bus via
  `TelemetryBridge`.
- **Runtime config knob**: `:observability, :data_plane_liveness`
  (default `true`, env `FLEZHA_DATA_PLANE_LIVENESS`) gates the ~1 Hz
  poll + classifier; counters stay always-on.
- **Procs fallback** (docs, not built): sample
  `/proc/<beam_pid>/task/*/` for `flz-uds-driver` threads in `D`/frozen
  `utime` (`bin/flzvm pid`).
- **Detailed notes**: `docs/resilience/transport_liveness.md`.

## Graceful shutdown (`controller-graceful-shutdown`)

- **Modules**: `lib/flezha/shutdown/` — `Coordinator`, `SignalHandler`.
  Coordinator drives `quiesce → drain → finalise`; SignalHandler
  installs OS handlers via `:os.set_signal/2` so SIGTERM/SIGINT
  trigger graceful shutdown in every BEAM run mode.
- **Supervision tree**: SignalHandler is the **first** child in
  `Flezha.Application.start/2` (handlers installed before any
  service can observe a delivered signal); Coordinator is inserted
  after `Flezha.Service.HubManager` (drain queries the hub).
  `prep_stop/1` calls `Coordinator.shutdown(:prep_stop)`.
- **Hot path gating**: `Coordinator.shutting_down?/0` is an ETS-backed
  flag (no GenServer hop). Read by `OperativeManager.start_operative/2` and
  `FleetManager.connect_handler/4`.
- **Runtime config knobs**: `:graceful_shutdown_enabled` (default
  `true`, env `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED`),
  `:graceful_shutdown_timeout_ms` (default `60_000`, env
  `FLEZHA_GRACEFUL_SHUTDOWN_TIMEOUT_MS`). **Pairing rule**: orchestrator
  grace period (`terminationGracePeriodSeconds` /
  `stop_grace_period`) MUST be `(timeout_ms / 1000) + 15s`. Bump in
  lockstep.
- **CLI**: `flz controller stop` (drives the coordinator over the
  CLI socket — works in release/mix/iex), `flz controller status`
  (surfaces `shutdown_in_progress`, `shutdown_phase`,
  `signal_handler_status`). Implemented in `cli/src/controller.rs` /
  `Flezha.Service.CliManager.controller_{stop,status}`.
- **Telemetry**: `[:flezha, :shutdown, :requested|:phase_started|:phase_completed|:drain_progress|:timeout|:complete]`.
- **Test config**: `config/test.exs` sets
  `:graceful_shutdown_enabled` to `false` so `Application.stop/1`
  teardown remains fast.
- **Detailed notes**: `docs/resilience/graceful_shutdown.md`.

## Observability foundation (`platform-observability-foundation`)

- **Modules**: `lib/flezha/observability/` — `Signal`, `Source`,
  `Sink` (behaviours), `Registry`, `Bus`, `RingBufferSink`,
  `PubSubSink`, `LoggerHandler`, `TelemetryBridge`, `SinkSupervisor`,
  `SourceSupervisor`, `Supervisor`, `Counters`, `DefaultSinks`. The
  service surface lives at `lib/flezha/service/observability.ex`.
- **Supervision tree**: `Flezha.Observability.Supervisor` is inserted
  in `Flezha.Application.start/2` AFTER `Flezha.Service.HubManager`
  and BEFORE `Flezha.Shutdown.Coordinator`. Children, in start
  order: `Registry` → `Phoenix.PubSub` (server name
  `Flezha.Observability.PubSub`) → `SinkSupervisor` → `SourceSupervisor`
  → `LoggerHandler` → `TelemetryBridge` → `DefaultSinks` (one-shot
  worker that registers `:ring_buffer` and `:pubsub` sinks).
- **Signal envelope**: one struct, four kinds — `:metric`, `:event`,
  `:log`, `:trace_span`. Constructor `Flezha.Observability.Signal.new/1`
  validates kind / atoms / map shapes and auto-fills monotonic `ts`.
- **Bus**: `Flezha.Observability.Bus.emit/1` runs in the caller, never
  blocks under default `:drop` overflow policy, checks per-source
  cardinality on `:metric` signals only, and wraps the dispatch body
  in `try/rescue/catch` so a broken sink cannot crash a producer.
- **Default sinks**: `RingBufferSink` (per-source ETS `:ordered_set`
  with TTL'd retention; queries run in the caller process) and
  `PubSubSink` (per-source topic `flezha.observability.source.<src>`,
  disabled by default in test config).
- **Bridges**: `LoggerHandler` installs an OTP `:logger` handler that
  routes log records into the bus as `:log` signals from the `:logger`
  source. `TelemetryBridge` attaches a single `:telemetry.attach_many/4`
  handler that lifts the known `[:flezha, ...]` events onto the bus
  without modifying any producer call-site.
- **Service API**: `Flezha.Service.Observability` exposes `subscribe/2`,
  `query/2`, `list_sources/1`, `list_sinks/1`, `enable_source/2`,
  `disable_source/2`. Every call takes a `caller_context` (`:source`
  in `[:cli, :ui, :sdk, :internal]`, optional `:principal`, optional
  `:request_id`). Mutating calls emit audit `:event` signals on
  `:observability_service` — the seam `platform-audit-log` will
  consume. Thin CLI wrappers live in `Flezha.Service.CliManager`
  (`observability_list_sources/1`, `_enable_source/2`, etc.).
- **Runtime config knobs**: `:observability` map — `:enabled` (default
  `true`, env `FLEZHA_OBSERVABILITY_ENABLED`), `:ring_buffer_retention_ms`
  (`900_000`), `:ring_buffer_max_entries_per_source` (`10_000`),
  `:overflow_policy` (`:drop`), `:cardinality_budget_per_source`
  (`1_000`), `:pubsub_sink_enabled` (`true` in prod, `false` in test).
- **Counters**: `Flezha.Observability.Counters` holds atomic
  `:dispatcher_failure_count`, `:logger_bridge_failures`,
  `:telemetry_bridge_failures` plus a public ETS table used to
  de-dup "log once per unique error class".
- **Bench**: `mix flezha.bench.observability [iterations]` boots
  just the observability supervisor (no Controller / runners) and
  reports nanoseconds per emit against zero sinks.
- **Detailed notes**: `docs/observability_foundation.md`.

## OTP backend sources (`platform-observability-otp-backend`)

- **Modules**: `lib/flezha/observability/otp_backend/` —
  `Supervisor`, `BeamVmSource`, `HostSource`, `ProcParser`,
  `ProcessHubSource`, `TransportSource`, `BoundedMailboxSource`.
- **Supervision tree**: `Flezha.Observability.OtpBackend.Supervisor`
  is inserted in `Flezha.Observability.Supervisor` AFTER
  `DefaultSinks` and BEFORE `Export.Supervisor`. `:one_for_one` so a
  per-source crash leaves peers running.
- **Shipped sources** — `:beam_vm` (memory / schedulers / GC /
  process+port counts, 1s default), `:host` (Linux `/proc` RSS /
  CPU / FD / loadavg, 5s default; one-shot `:host_source_degraded`
  event on non-Linux), `:processhub` (per-hub registered processes +
  pending recoveries, 1s), `:flezha_transport` (per-transport
  pending-stream count via `Flezha.Transport.Server.get_stats/1`,
  1s), `:flezha_bounded_mailbox` (registered, one-shot
  `:bounded_mailbox_unavailable` event until the Rust→Elixir mailbox
  surface lands).
- **Runtime config knobs**: `:observability, :otp_backend_enabled`
  (default `true` in prod, `false` in test); `:observability,
  :otp_backend, :poll_intervals` map per-source override.
- **CLI**: `flz observe sources`, `flz observe sinks`,
  `flz observe tail <source>`, `flz observe enable <source>`,
  `flz observe disable <source>` — implemented in
  `cli/src/observe.rs` and
  `Flezha.Service.CliManager.observability_render_*`.
- **Detailed notes**: `docs/observability_otp_backend.md`.

## Observability export (`platform-observability-export`)

- **Modules**: `lib/flezha/observability/export/` — `Supervisor`,
  `PrometheusRegistry`, `PrometheusSink`, `PrometheusHttp`,
  `OtlpCodec`, `OtlpClient`, `OtlpSink`.
- **Supervision tree**: `Flezha.Observability.Export.Supervisor` is
  the last child of `Flezha.Observability.Supervisor`, started AFTER
  the OTP backend. Registers the declarative
  `:observability_export` source on init so the exporters'
  operational events participate in bus fan-out.
- **Prometheus**: hand-rolled `:gen_tcp` HTTP/1.1 listener (~250
  LOC, no Cowboy/Plug) at default `127.0.0.1:9568/metrics`. Accepts
  only `GET /metrics` and `GET /`; everything else returns 404 / 405
  / 400. Type inferred from `Signal.measurements` keys (`:count` →
  counter, `:value`/`:gauge` → gauge, `:duration_us`/`:bytes` →
  histogram); `metadata.prometheus_type` overrides. Cardinality
  enforcement aggregates over-budget label values into a synthetic
  `{__truncated__="true"}` series and emits
  `:prometheus_label_truncated` events.
- **OTLP**: OTLP/HTTP via `:mint` (top-level dep, was transitive
  via `:mint_web_socket`). Hand-rolled protobuf encoder in
  `OtlpCodec` matches OpenTelemetry v1 schema for metrics / logs /
  traces. Batched with size + time triggers (`:otlp_batch_max` 512,
  `:otlp_flush_interval_ms` 10s). Overflow policy `:block` —
  producer back-pressure during OTLP outages, bounded by
  `mailbox_watermark × sum-of-backoffs`. Backoff: 250/500/1000/2000/4000
  ms, max 5 attempts.
- **Runtime config knobs**: `:observability_export` map with
  `:prometheus_enabled` (default `false`), `:prometheus_port`
  (`9568`), `:prometheus_bind` (`"127.0.0.1"`), `:otlp_enabled`
  (`false`), `:otlp_endpoint`, `:otlp_headers` (map for bearer
  tokens), `:otlp_flush_interval_ms`, `:otlp_batch_max`. Env-var
  overrides: `FLEZHA_PROMETHEUS_ENABLED`, `FLEZHA_PROMETHEUS_PORT`,
  `FLEZHA_OTLP_ENABLED`, `FLEZHA_OTLP_ENDPOINT`.
- **Registry extension**: `Flezha.Observability.Registry.register_external_sink/1`
  lets exporters register a sink whose pid is owned by a different
  supervisor — required because `Export.Supervisor` owns the
  Prometheus and OTLP sinks, not the foundation `SinkSupervisor`.
- **Detailed notes**: `docs/observability_export.md`.

## Durable audit log (`platform-audit-log`)

- **Modules**: `lib/flezha/observability/audit_log/` — `Codec`,
  `Index`, `Reader`, `Writer`, `Rotation`, `Retention`, `Sink`,
  `Supervisor`. Service surface at
  `lib/flezha/service/audit_log.ex`.
- **Supervision tree**: `Flezha.Observability.AuditLog.Supervisor`
  is the LAST child of `Flezha.Observability.Supervisor` (gated on
  `:audit_log, :enabled`, default `false`). Children, in start
  order: `Writer` → `Rotation` → `Retention` → `Sink`. The Sink is
  last so subscriptions go live only after the Writer is ready to
  accept records.
- **Default allow-list**: `:observability_service`, `:flezha_shutdown`,
  `:flezha_plugin_installer`, `:audit_log` (audit-of-audit). Extend
  via `:audit_log, :extra_subscriptions`.
- **On-disk layout**: `app_data/audit/INDEX` (binary catalog),
  `app_data/audit/audit-current.jsonl` (active file),
  `app_data/audit/audit-YYYYMMDDTHHMMSSZ-N.jsonl[.gz]` (rotated).
  `INDEX` is rebuildable on boot if missing or corrupt.
- **JSON Lines record**: stable field order `ts_us, ts_iso, source,
  name, measurements, metadata, caller`; `caller` is denormalised
  from `metadata.caller_source / :principal / :request_id`.
- **Fsync**: `:per_record` (default, ~1-10 ms/record on local SSD)
  or `:batched` (every `:fsync_interval_ms`, bounded loss window).
  Overflow policy `:block` so audit emits are never silently
  dropped.
- **Rotation**: size (`:max_file_bytes`, 64 MiB) OR age
  (`:max_file_age_seconds`, 1 day), checked every
  `:rotation_check_interval_ms` (60 s) and on every write.
- **Retention**: age (`:retention_seconds`, 30 d) AND total size
  (`:max_total_bytes`, 4 GiB) — more restrictive wins. Active file
  is never deleted by retention.
- **Service API**: `Flezha.Service.AuditLog` —
  `list_files/1`, `read/2`, `tail/2`, `unsubscribe/2`, `stats/1`,
  `rotate/1`. Every public call emits an audit-of-audit event on
  `:audit_log` source. Master flag check returns
  `{:error, :observability_disabled}`; subsystem flag check returns
  `{:error, :audit_log_disabled}`.
- **CLI**: `flz audit list`, `flz audit read <file>`,
  `flz audit tail`, `flz audit stats`, `flz audit rotate` —
  implemented in `cli/src/audit.rs` and
  `Flezha.Service.CliManager.audit_log_render_*`.
- **Shutdown integration**: `Flezha.Shutdown.Coordinator.do_finalise`
  calls `Writer.close/0` BEFORE the `FsyncBatcher` flush, workflow
  by a 5 s timeout. Post-close signals are dropped with a per-sink
  counter increment.
- **Runtime config knobs**: `:audit_log` map (see
  `docs/audit_log.md` for the full list). Env-var overrides:
  `FLEZHA_AUDIT_ENABLED`, `FLEZHA_AUDIT_DIR`,
  `FLEZHA_AUDIT_RETENTION_DAYS`.
- **Detailed notes**: `docs/audit_log.md`.

## Platform authn/authz foundation (`platform-authn-authz`)

- **Modules**: `lib/flezha/authz/` — `Action`, `Role`, `Principal`,
  `Namespace`, `Scope`, `Policy`, `Config`, `Store`, `AuthState`,
  `RoleStore`, `NamespaceStore`, `TokenStore`, `QuotaCounters`,
  `Quotas`, `Bootstrap`, `Bootstrap.OnBoot`, `SourceRegistration`,
  `Supervisor`; top-level `Flezha.AuthZ` exposes `authorize/3`,
  `require_bound_principal/1`, `enabled?/0`. Cert side lives at
  `lib/flezha/authn/cert.ex` (`principal_from_peer/1` +
  `namespace_from_peer/1`, RFC 4514 normalised DN / SPIFFE URI SAN).
- **Supervision tree**: `Flezha.AuthZ.Supervisor` is inserted in
  `Flezha.Application.start/2` AFTER `Flezha.Observability.Supervisor`
  and BEFORE `Flezha.Shutdown.Coordinator`. Children, in order
  (`:rest_for_one`): `AuthState` → `RoleStore` → `NamespaceStore`
  → `TokenStore` → `QuotaCounters` → `SourceRegistration` (one-shot
  `:platform_authz` source registration) → `Bootstrap.OnBoot`
  (boot-time token writer + refuse-to-start gate). Gated by
  `:authn_authz, :application_managed` (default `true`; tests flip
  to `false` so they can bring up isolated stores against a tmp
  `auth_dir`).
- **On-disk layout**: `app_data/auth/` (mode `0700`) —
  `state.dets` (durable `:enabled` flag),
  `roles.dets` (principal → {namespace, role}),
  `namespaces.dets` (catalog + quotas),
  `tokens.dets` (SHA-256-hashed registry),
  `quota_counters.dets` (per-namespace counters),
  `bootstrap.token` (one-time, mode `0600`, unlinked on claim).
  Persistent-state tree under `app_data/state/_system/...` after the
  one-shot namespace migration in `Flezha.Boot.StatePathMigrator`.
- **Policy point**: `Flezha.AuthZ.authorize(caller_context, action,
  scope) :: :ok | {:error, {:forbidden, reason}}`. Four roles:
  `viewer`, `operator`, `admin`, `runner`. Role→action map is
  compile-time in `Flezha.AuthZ.Policy`. Auth-state mutations
  (`:authz.*`) are additionally gated to `admin` on the bound
  namespace; only `admin@_system` may target other namespaces.
- **Tokens**: wire form `flzt_<base32>` (256-bit); storage form
  SHA-256 hash. Raw value returned to caller exactly once at
  `TokenStore.issue/5`. Revocation is instant
  (`TokenStore.revoke/2` sets `revoked_at`; `verify/1` rejects).
  Mutations emit `:token_issued` / `:token_revoked` on
  `:platform_authz`.
- **Quota engine**: `Flezha.AuthZ.Quotas.check_allocation/3` —
  one ETS lookup per call (NamespaceStore cache + QuotaCounters
  cache). `record_allocation/3` / `record_deallocation/3` apply
  async. `OperativeManager.start_operative/2` enforces `max_operatives` /
  `max_entities_per_operative` at allocation time and records on
  success; `stop_operative/2` decrements on stop. Mailbox / disk /
  runner enforcement points land in the namespace-state
  follow-up.
- **Bootstrap**: on first boot when no `admin@_system` binding
  exists AND `:enabled` is `true`, `Bootstrap.OnBoot` writes
  `app_data/auth/bootstrap.token` (mode `0600`) after verifying the
  parent dir's mode (aborts with `error` on group/other-write
  bits). `flz auth bootstrap <token>` (Elixir wrapper:
  `Flezha.Service.CliManager.authz_bootstrap/2`) claims it,
  resolves the principal (cert SAN > cert DN > `--principal` >
  freshly-minted admin token), binds `admin@_system`, flips
  `AuthState.enabled` durably, unlinks the token file, emits
  `:bootstrap_completed`.
- **CLI surface (Elixir-side wrappers ready; Rust verbs deferred)**:
  `Flezha.Service.CliManager.authz_{bootstrap,issue,revoke,list,whoami}`,
  `namespace_{create,delete,list,show,set_quota}`,
  `role_{bind,unbind,list}`. Each wraps in `AuthZ.authorize/3` and
  emits a `:platform_authz` audit event on mutating verbs.
- **Audit log integration**: `:platform_authz` in the default
  subscription allow-list (`Flezha.Observability.AuditLog.Sink`);
  JSONL records gain a top-level `namespace` field (defaults to
  `"_system"` when absent on pre-upgrade rows); `caller.principal`
  is the verified principal when `:enabled` is `true`, else the
  caller-source atom (preserving pre-change behaviour).
- **Compatibility mode**: `:authn_authz, :enabled` defaults `false`
  in `config/runtime.exs` and `config/test.exs`. While `false`,
  `AuthZ.authorize/3` returns `:ok` unconditionally and
  `require_bound_principal/1` is a no-op — every existing service
  caller continues to work without producing a verified principal.
  Flips to `true` durably on `Bootstrap.claim/2` success;
  `FLEZHA_AUTHZ_ENABLED=false` is an explicit opt-out that emits a
  `warn` on every boot.
- **Runtime config knobs**: `:authn_authz` map —
  `:enabled` (default `false`, env `FLEZHA_AUTHZ_ENABLED`),
  `:auth_dir` (default `app_data/auth`, env `FLEZHA_AUTHZ_DIR`),
  `:bootstrap_token_path` (default `<auth_dir>/bootstrap.token`,
  env `FLEZHA_AUTHZ_BOOTSTRAP_TOKEN_PATH`),
  `:token_ttl_default_seconds` (`86_400`),
  `:sdk_token_ttl_seconds` (`2_592_000`),
  `:namespaces` (map of per-namespace quota overrides),
  `:application_managed` (default `true`; tests set `false`).
- **Deferred to follow-ups**: transport-layer cert wiring + CLI
  socket handshake + Rust CLI verbs
  (`platform-authn-authz-transport`); namespace prefix through
  State subsystem + ProcessHub registry + remaining service-module
  refactors + remaining quota points
  (`platform-authn-authz-namespace-state`); operator guide and
  multi-tenant integration suite
  (`platform-authn-authz-docs-and-e2e`).
