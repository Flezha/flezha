# Known issues

Defects found but not fixed where they were found, with enough context that a
session which has never seen this repository can pick one up and work it.

**Why this file exists.** The refactor programme's charter (`docs/refactor/
charter.md` §1) forbids fixing a defect inside a refactor commit: a refactor
changes structure and never behaviour, so anything found on the way is recorded
and fixed in its own change. That recording used to live in the programme's
board (`docs/refactor/STATE.md`), which is programme-scoped and retires when the
programme does. These outlive it, so they live here.

**What an entry must carry.** A command that reproduces the defect, and the
output that command actually gave. An entry without one records a suspicion, and
a suspicion cannot be verified, closed, or acted on by anyone but its author.
On 2026-08-30 all forty entries were triaged against the code: eleven were
already fixed or never true, five overstated their scope, four **understated**
it, and five could not be reproduced at all. Every one of them had been written
in passing, mid-other-work, without a reproduction — which is exactly what
charter §1's "record it, fix it in its own change" does not by itself require.

**How to use it.** Each entry states where the defect is, what it does, the
evidence it is real, why it was not fixed on the spot, and what a fix has to
account for. `P1` blocks correctness or hides data loss; `P2` costs real time or
lets a gate lie; `P3` is a wart. An entry marked **proposal owed** needs an
OpenSpec change written before code moves; one marked **investigation first**
cannot be specified yet.

**Adding to it.** New entries go in the right section with the same fields. Do
not delete a resolved entry — move it to *Resolved* with the commit that closed
it, so a future reader can tell "fixed" from "never recorded".

**Note on proposals.** `openspec/changes/` is gitignored, so an OpenSpec change
named below exists only on the machine that wrote it until it is archived. **This
file is the durable record**; a proposal is the working artifact. If a change
named here is missing from `openspec list`, it was never archived and the entry
below is all that survives — which is enough to write it again.

---

## P1 — correctness and silent data loss

### KI-121 · An UNNAMED plugin sink takes the sink's name in the veneers and the producer node's in Rust

- **Severity**: P2 · the second divergence on the same spec requirement as [[KI-120]], left
  open deliberately when that one was closed. · Found by
  `honour-named-on-plugin-channels` while writing its spec delta.
- **Where**: `sdk/node/src/authoring.ts`, `sdk/ruby/lib/flezha/builder.rb`,
  `sdk/php/src/Graph/Edge.php` — the same `sink()` methods KI-120 fixed, in the branch
  where no `.named()` was called.
- **The rule they miss**: `openspec/specs/sdk-graph-authoring/spec.md:59-62` says a sink with
  **no** `.named(...)` takes the **producer node's** identity. Rust does exactly that —
  `sdk/rust/src/channel/builder/state.rs:236`, `let name = self.procedures[node].name`. All
  three veneers use the **sink's** name instead.
- **Why it was NOT closed with KI-120, and this is the right call.** KI-120's fix is
  backward-compatible precisely because the no-`.named()` path keeps today's behaviour — and
  for this case today's behaviour is itself wrong. Closing it renames every unnamed plugin
  channel across three SDKs' fixtures at once: **16 sink calls in the node test app alone**.
  That is its own change with its own blast radius, not a rider.
- **The divergence is NOT recorded as settled by the suites.** The compatibility test added
  in each SDK is deliberately named *"keeps the name it has today"* and commented as
  **compatibility, not conformance**, citing the spec line — so a future reader cannot
  mistake a green suite for agreement with the spec. The proposal's Non-goals and the dev
  note both name it.
- **A fix must** decide whether the spec or the veneers are right before renaming anything,
  and must count the fixture churn first — the conformance apps' channel names appear in
  goldens and e2e assertions, so this is a rename with reach, not a one-expression change
  like KI-120.

### KI-120 · The veneers' `sink()` silently discards `.named()`, so a plugin channel takes the sink's name instead of the channel's

- **Severity**: P1 · a **cross-SDK authoring-surface divergence** that makes a forwarded
  frame unroutable. Found by `stamp-identity-in-erased-veneers` once KI-119 stopped masking it.
- **Where**: `sdk/node/src/authoring.ts:350`, `sdk/ruby/lib/flezha/builder.rb:216`,
  `sdk/php/src/Graph/Edge.php:150`.
- **Evidence** — `partitioned_operatives_e2e_test.exs:221` after the KI-119 fix:

  | runtime | result |
  |---|---|
  | rust | **1 passed** |
  | node / ruby / php | `0/1` — `[flezha] operative <id> has no channel 'relay_out'`, then `SOL-USD was not forwarded` |

- **Mechanism, named to the function.** Rust's `Stream::sink_with`
  (`sdk/rust/src/channel/builder/stream.rs:378-388`) **attaches** a plugin sink to the
  existing channel — `entry.plugin_sink = Some(...)` — so
  `.named("input").keyed().sink("relay_out")` leaves the CHANNEL named `input` and
  `relay_out` names only the plugin. The wire's `channel` metadata is therefore `input`,
  which selects the downstream fixture's `input` entry channel. All three veneers instead
  call `addChannel({name, kind: "plugin"})` — a **new** channel named after the sink — and
  **never read `pending.channelName`**, so `.named()` is silently discarded and the wire
  carries `relay_out`, which the receiving operative does not have.
- **This is a one-site omission, not a model difference, and the file proves it.**
  `.named(name)` stores `pending.channelName` (`authoring.ts:265`), and the **edge** paths
  already honour it at `:308` and `:334` — both `this.pending.channelName ?? <default>`.
  Only the plugin path forgot. The three fixtures' own comments assert Rust's model against
  their own SDKs: *"The plugin channel is named `input` because a forwarded frame keeps its
  producing channel's name."*
- **Not the stamps.** KI-119's fix and this row's stamps both work: the frame reached the
  forward, the forward resolved a target member from `flz.key`, and it was delivered. The
  failure is the RECEIVING operative rejecting the channel name.
- **A fix must** apply the file's existing pattern to the one call site that omitted it —
  the channel name becomes `pending.channelName ?? <sink name>`, the sink name stays the
  plugin's. **That is backward-compatible**: with no `.named()`, `?? name` reproduces
  today's behaviour exactly, so only the currently-broken combination changes.
  **Renaming the fixtures' sink to `input` is refused**: it would make the veneer fixtures
  stop being twins of Rust's and hide a divergence the conformance suite exists to find, and
  it would leave `.named()` silently discarded for every user.

### KI-119 · `operative_forward` refuses the partition options the platform always injects — a REGRESSION introduced by `close-provider-value-validation`

- **Severity**: P1 · every partitioned operative whose group fans out to an
  `operative_forward` plugin **fails to start**. · **Introduced 2026-09-01 by `eb938c8b`
  (R1), verified by the supervisor. The reporting worker dated it to `b9504074` (2026-08-16)
  and that dating is WRONG** — see below, because the correction is the whole point.
- **Where**: `lib/flezha/plugin/provider/operative_forward.ex:76` (`with :ok <-
  validate_opts(opts)`, added by `eb938c8b`) against
  `lib/flezha/plugin/spec_decoder.ex:22-26`.
- **Evidence** — `FLEZHA_E2E_RUNTIME=<rt> mix test --include system
  test/system/partitioned_operatives_e2e_test.exs:221`, run for **rust, node, ruby and php**;
  all four give `0/1 passed` with byte-identical text:

  ```
  {:error, "partition 0 of group pup323 failed to start (pup323#p0:
    %{reason: \"unknown option(s) for operative_forward: key_group_from,
                key_group_to, partition_count, partition_index\"})"}
  ```

  It fails at `PartitionGroup.start`, **before any emit**, so no SDK code is reached — which
  is why it is red under the Rust reference too and is not an SDK defect.
- **Mechanism, named to the function.** `Flezha.Plugin.SpecDecoder.decode/4` merges
  `partition_index`/`partition_count`/`key_group_from`/`key_group_to` into **every** plugin's
  opts for a partition member (its own `@doc` says so). The refusal text is emitted by the
  **generated `validate_opts/1`** (`provider_behaviour.ex:359`, allowed set built at
  `:613-616` from `@known_opts ++ @internal_opts ++ host_opts()`). `OperativeForward` lists
  the four keys in none of the three.
- **WHY THE DATING MATTERS, and why "it has never passed" is the wrong reading.**
  `@known_opts` strictness is indeed old — but **`init_provider/1` never CALLED
  `validate_opts/1` until `eb938c8b`**. Verified:
  `git show eb938c8b~1:lib/flezha/plugin/provider/operative_forward.ex` has no
  `validate_opts` reference at all; HEAD calls it at `:76`. That call was **KI-116's site 5**,
  fixed deliberately. So the injected keys flowed through silently before R1 and are refused
  after it. **This is four hours old, not two weeks.**
- **How it escaped R1's verification, which is the reusable lesson.** R1 ran the full
  `mix test` (1960 passed) and its area slice — and `partitioned_operatives_e2e_test.exs` is
  `@moduletag :system`, **excluded from `mix test` by default** and run only by
  `nightly-e2e.yml`. **A change to plugin option strictness cannot be verified by `mix test`
  alone**; the partition fan-out path is reachable only under `--include system`. R1's worker
  did reason about injected opts — it added `@injected_opts`/`@internal_opts` for
  `:worker_pid` and `:routes` precisely to avoid refusing plugin heals — but neither it nor
  the supervisor considered `SpecDecoder`'s four partition keys.
- **A fix must** declare the four keys on `operative_forward`. The two providers that can
  also be partition members already do, with an explanatory comment —
  `websocket.ex:86-91` and `http_poll.ex:68-72`, both reading *"Injected by a partition
  group's plugin fan-out, not written by hand."* Prefer `@internal_opts`/`@injected_opts`
  over `@known_opts` on semantic grounds (these are platform-injected, not author-written,
  which is exactly what that attribute was created for four hours earlier) — **but check the
  `defstruct` consequence at `:55`, which derives from `@injected_opts`**, and check whether
  anything reads the keys before adding struct fields nothing populates. Regression test must
  cover the partition path, and a `mix test`-only green does not prove it.

### KI-118 · The erased hosts accept a procedure topology the Rust SDK refuses as unroutable

- **Severity**: P2 · a **cross-SDK authoring-surface divergence**, which `CLAUDE.md`'s
  Cross-SDK rule names as the surface that must not differ. Found by R4's survey as an
  ordinal question; **the reproduction refuted that framing and produced this one.**
- **How it was found, and why the first diagnosis was wrong.** Reading three functions side
  by side shows Rust incrementing `emit_ordinal` **inside** `if self.keyed`
  (`sdk/rust/src/operative/dispatcher/emit_sink.rs:105` wrapping the `+= 1` at `:374`) while
  Java (`EmitContext.java:287`) and Python (`ctx.py:253-255`) increment on **every** plugin
  output. That reads as an ordinal divergence: dense versus gapped `flz.seq` values. **It is
  not, because the topology that would expose it cannot be built in Rust.**
- **Evidence** — a probe written to exhibit the ordinal gap, which instead refused to build:

  ```
  $ cargo test -p flezha_runner --features bench-internals,testing --test ki118_probe
  KI-118 probe: Rust REFUSED with: operative topology validation failed:
    [DuplicatePluginSink { channel: "keyed_out" }]
  ```

- **Mechanism, named to the function.** Two Rust rules compose: a channel takes one plugin
  sink (`sdk/rust/src/channel/builder/stream.rs:382`, *"the first call wins"*), and a
  procedure produces one output channel (`sdk/rust/src/workflow/spec.rs`
  `refuse_multiple_outputs`, whose own comment reads *"A procedure producing two channels is
  unroutable (the topology keeps one `output_channel_id`, so the second would be silently
  orphaned). Structurally impossible on the stream-handle builder; kept as
  defense-in-depth."*). Together they allow one plugin sink per procedure, so a keyed and a
  non-keyed plugin emit can never coexist in one Rust invocation — **Rust's `if self.keyed`
  guard is unobservable, and for every topology Rust can express, Rust, Java and Python
  agree.**
- **The actual divergence**: node, ruby and php (and Java via `emitTo`) **accept** one
  procedure declaring several plugin channels — their graph validators passed fixtures
  declaring two, and php three — while Rust refuses that shape as structurally unroutable.
  The ordinal gap is a symptom that exists only on the side permitting the topology.
- **Neither ordinal rule is wrong, and that is recorded so nobody "fixes" one.** The spec is
  silent: `docs/architecture/kernel.md:489` gives `<change_seq>.<emit ordinal>` without
  saying what advances it. `flz.seq` is consumed as a **dedup key**
  (`plugin/provider/postgres/sink_worker.ex`, the `(epoch, seq)` guard;
  `kafka/producer_worker/wire.ex`), which needs uniqueness, not density. Both schemes give
  uniqueness and both are replay-stable.
- **A fix must** answer the authoring question first — **may one procedure produce several
  plugin channels?** — because the ordinal rule follows from that and not the other way
  round. If yes, Rust's refusal is over-restrictive and the ordinal rule must be written
  down. If no, four SDKs accept a graph the reference calls unroutable and their validators
  owe the same refusal. **Do not change an ordinal increment before that is decided**:
  `stamp-identity-in-erased-veneers` deliberately matched Java/Python, the only defined
  answer on a topology Rust has no opinion about.

### KI-117 · A 1 ms wall-clock budget asserted on the Kafka enqueue path, 30 000x tighter than the property it defends

- **Severity**: P3 · test hygiene. **Reproduced by the supervisor 2026-09-01**, on a
  loaded box, at a checkpoint of `close-provider-value-validation`.
- **Where**: `test/flezha/plugin/kafka_test.exs:381` at `a305d1f3`
  (`:463` after that change adds tests above it) —
  `assert elapsed_us < 1_000, "expected <1ms, got #{elapsed_us}us"`.
- **Evidence**:

  ```
  $ mix test test/flezha/plugin/kafka_test.exs test/flezha/plugin/provider_behaviour_test.exs \
      test/flezha/plugin/extension_test.exs
  1) test on_input / on_output enqueue path on_output returns {:ok, ctx} immediately
     even when broker would block 30 s (FlezhaTest.Plugin.KafkaTest)
     expected <1ms, got 1364us
  Result: 96/97 passed
  ```

- **NOT caused by the change that found it**: the test is byte-identical at HEAD
  (`git show HEAD:test/flezha/plugin/kafka_test.exs`, `:356`/`:381`) and appears
  nowhere in that change's diff. The R1 worker saw it once in five runs and
  recorded it as a suspicion rather than filing; the supervisor then hit it on the
  first independent run, which is what moved it from suspicion to established.
- **Mechanism, named to the assertion**: the test's own title states the property —
  the enqueue returns *instead of* waiting on a broker that would park for 30 s.
  A 1 ms absolute budget is **30 000x tighter than that property needs**, so on a
  laptop running other suites concurrently, ordinary scheduler jitter fails a test
  whose subject never blocked. The failure carries no information about the
  behaviour it names.
- **A fix must** assert the property rather than a microsecond count — that the
  call returns while a broker stub is still parked — or, if an absolute bound is
  kept, size it against the 30 s it is distinguishing itself from, not against
  1 ms. Whichever is chosen, this is `test/` work and belongs to whoever next
  opens that file; it is **not** a product defect and it does not gate any change.

### KI-116 · Plugin providers coerce operator-supplied values without validating them, and a raise escapes the enable

Found by the sibling survey `validate-kafka-queue-overflow` was required to run, 2026-09-01.
**Reported, not fixed** — scope beyond Kafka's `:queue_overflow` was the supervisor's call.

**The shared mechanism, and why it is P1 rather than a wart.** Two facts compose:

1. `ProviderBehaviour.__before_compile__` injects `defp convert_value(_field, value), do:
   {:ok, value}` (`provider_behaviour.ex:529`) at the end of **every** provider, so a
   `convert_value/2` table that looks like a validator answers `{:ok, anything}` for any
   shape its explicit clauses do not match.
2. `Extension.Table.subscribe/3` (`extension/table.ex:93`–`:97`) catches **`:throw` only**:

       catch
         :throw, {:init_provider_failed, reason} -> {:error, reason}

   So a `FunctionClauseError` or `ArgumentError` raised inside `init_provider/1` **escapes
   the enable** rather than refusing it.

Together: a bad operator value is either accepted silently or blows past the one place
designed to turn it into a refusal.

**Sites, each verified at the code:**

- **`kafka.ex:183`–`:184` `auth_mechanism/1`** — byte-for-byte the `coerce_atom/1` just
  deleted, and partial. Reached from `init_provider/1:97` → `resolve_auth/1:164`, with a
  documented closed set (`kafka_producer.md:90`) delivered as a string. No
  `convert_value(:auth, …)` clause exists. Terminal check is in the dependency and
  **raises**. **Wider blast radius than the defect just fixed**: the same function is
  captured into `auth_fun:` (`kafka.ex:108`) and re-invoked by `WorkerRegistry` on every
  client restart, so the raise kills the **shared** registry.
- **`kafka.ex:180`–`:181` `auth_key_atom/1`** — same shape; unknown-but-existing keys are
  then dropped by `Map.take` in the dep, so `%{"user" => "svc"}` silently means *no
  username*.
- **`ProviderBehaviour.coerce_bool/1` (`:143`–`:145`) and `coerce_int/1` (`:140`–`:141`)
  validate nothing and are partial.** Their own docstring says *"for `init_provider/1`,
  where opts are pre-validated"* — **that premise is false**: the only pre-validation on
  that path is the generated `validate_opts/1`, which checks key NAMES and never a value.
  Live callers: `postgres.ex:59`/`:142` (`:auto_create`, `:ssl`), `kafka.ex:235`/`:256`.
  `ssl: "yes"` is a `FunctionClauseError` that escapes the enable. Total counterparts
  (`parse_bool/2`, `parse_int/2`) exist and are wired only into `build/1`, which KI-93
  records has no production caller.
- **`postgres.ex:112`** reads `:unhealthy_after_ms` with `coerce_int/1` on **every health
  probe**; the raise is swallowed by `worker_health/3`'s `safe_verdict/2` into
  `{:fail_open, …}`, so a bad value **silently disables that plugin's health verdict for
  ever**.
- **`operative_forward.ex:59`** declares `@known_opts` (`:54`) so `validate_opts/1` is
  generated — and `init_provider/1` **never calls it**, unlike `websocket.ex:166` and
  `http_poll.ex:84`. The key-level twin: an unknown option is silently ignored.
- **`websocket/subscription.ex:303`–`:312`** — `type = if pcp["type"] == "binary", do:
  :binary, else: :text`, a two-value enum with no check, so `"Binary"` silently sends a
  text frame.
- **`monitored_io_forward.ex:55`** reads `:heartbeat_interval` raw into
  `Process.send_after/3`; its `convert_value/2` clause is `build/1`-only.
- **`postgres.ex:12`** moduledoc claims *"the `@known_opts` table below"* — the attribute
  does not exist; the module is strict on neither path.

**Clean, recorded so nobody re-checks**: `Websocket.:subscription_ack_mode` (total
`Subscription.ack_mode/1` on the init path, downstream matcher has a catch-all) and
`HttpPoll` (every tunable bounded on the init path, `validate_opts/1` called first).

**A fix must** treat the two composing facts, not only the sites: make the provider
`convert_value/2` table **total** where it is used as a validator (an injected permissive
catch-all is what makes an apparent validator inert), and decide whether `subscribe/3`
should turn a raise into a refusal rather than letting it escape. Fixing sites one at a
time leaves the mechanism that generates them.



**Three corrections to this entry, verified 2026-09-01 by the R1 worker and
re-run by the supervisor before they were written here.**

1. **A bare throw is a third escape flavour this entry does not name.**
   `Kafka.parse_hosts/1` throws a plain string, and `subscribe/3`'s
   sentinel-only `catch` lets it past exactly as it lets a raise past:
   `PROBE subscribe kafka hosts=nocolon -> CAUGHT :throw: "Invalid host format: nocolon"`.
   Any fix that catches only `:error` leaves this one standing.
2. **Site 2 is half-wrong as filed.** A misspelled *required* auth key does not
   silently mean "no username" — the dependency's `require_keys!/2` raises
   first (`PROBE kafka auth %{"user" => "svc"} -> RAISED ArgumentError: Missing
   keys: [:username]`). The silent drop is real for *extra* keys, which
   `KafkaEx.Auth.Config.new/1` discards through `Map.take/2`
   (`PROBE kafka auth %{..., "timeout" => 1} -> accepted`). One closed key set
   fixes both halves.
3. **The mechanism-2 fix belongs in `run_init_provider/1`, not `subscribe/3`.**
   That is the single point invoking the callback, and it is called from BOTH
   `do_subscribe/4` (enable) and `attempt_restart/3` (plugin heal) — the second
   of which this entry does not mention. Both already handle its
   `{:error, reason}` arm, so the refusal reaches the operator on both paths
   rather than only on enable.


### KI-72 · The QUIC driver reports no driver stats at all, so nothing above it can see the transport

- **Severity**: P2 · **established**, and it is why KI-56's own reproduction
  could not be run as filed.
- **Where**: `flz_nif/src/server.rs:111` sets `stats: None` for
  `start_quic_server`; `driver_stats/1` (`:126`–`:133`) answers `:undefined`
  when it is `None`. `flz_nif/src/quic_driver.rs` mentions `DriverStats`
  nowhere.
- **What happens**: `write_buffer_len`, `frames_written`, `frames_read`,
  `last_write_ms`, `loop_iterations` — the whole self-report — exists for UDS
  and does not exist for QUIC. So
  `TransportSource.emit_driver_stats/2` emits nothing for a QUIC controller,
  and `Flezha.Convergence.StallClassifier`'s driver clauses
  (`lib/flezha/convergence/stall_classifier.ex:178`) can never fire on it. A
  QUIC deployment is running with the data-plane liveness instrumentation
  switched off, and nothing says so.
- **Evidence**:

  ```
  $ grep -n 'stats' flz_nif/src/quic_driver.rs
  (no output)
  $ sed -n '105,133p' flz_nif/src/server.rs
          stats: None,
  ...
  /// Self-reported driver liveness for this server. Returns a map for UDS
  /// servers, or `:undefined` for QUIC (no per-stream stats yet).
  ```

  The comment at `resources.rs` states it outright — "`None` for the QUIC
  driver, which does not yet surface per-stream stats (the silent-stall
  incident was UDS)" — so it is known and deliberate; what is new is that KI-56
  was filed with a reproduction that reads a value this driver never produces.
- **A fix must** either give the QUIC driver the same `DriverStats` the UDS
  driver has, or state in the liveness docs that QUIC is uninstrumented so a
  green `/healthz` on QUIC is not evidence about the data plane. Note for
  `close-transport-send-buffer-loss` task 4.1/4.2: a liveness verdict on the
  drop counter is UDS-only until this is answered.

- **Supervisor verdict 2026-08-31 — REAL; the deferral is NAMED and the row does not
  grow.** The QUIC driver produces no `DriverStats`, so `TransportSource` emits
  nothing and `StallClassifier`'s driver clauses cannot fire on a QUIC controller.
- **What this constrains, and it must be written into the change**: tasks 4.1/4.2
  make the drop counter reportable, and that reporting is **UDS-only** until this is
  answered. Row 6.5 must say so where the requirement is stated, rather than leaving
  a reader to infer that a QUIC controller is covered.
- **Why not fix it here**: row 6.5's purpose is to stop the transport losing data.
  Making QUIC observable is a separate and larger piece — it is why KI-56's own filed
  reproduction was unperformable — and folding it in would grow a P1 correctness
  change into an instrumentation change. It is queued with KI-68 and KI-69 as the
  runner/controller-seam group to be surveyed together.


### KI-57 · UDS socket paths overflow `sun_path` in a full run, and a test dies of it

- **Severity**: P1 · **investigation first**, then proposal. **Supersedes
  KI-17's triage**, which closed this as *"never true — measured worst case 107
  bytes, longest ever written 89"*. It is true, it is measured below, and 107
  was one byte from the wall.
- **Where**: `Flezha.Socket.Support.stream_socket_path/2`
  (`lib/flezha/socket/support.ex:77`) composes
  `socket_dir() <> "/#{type}_stream_#{runner_id}.sock"`. For `:secondary` that
  is **`socket_dir()` + 63 bytes** at a 40-character runner id. The NIF refuses
  at 108 (`SUN_LEN`, NUL included).
- **What happens**: the driver refuses, `Flezha.Runner.StreamAcceptor.init/1`
  exits `"driver init failed"`, its supervisor's `:secondary_acceptor` child
  fails to start, and `Flezha.Runner.FleetManager.runner_connect/3` then
  refuses the connect (`{:runner_supervisor_down, {:noproc, …}}`). The runner
  never gets a control channel, so any operative that needed it never runs.
- **Measured, in two full runs I made today** (`java-diamond-resume`, batch 6):

  ```
  $ FLEZHA_E2E_RUNTIME=java mix test --include system
  Result: 1918/1920 passed, 5 skipped, 35 excluded
  1) test v1 code meeting v2 state is refused, not decoded (SchemaEvolutionE2ETest)
     Assertion failed, no matching message after 30000ms
  2) test state written at v1 comes back through the migration a bumped v2 declares
     ** (MatchError) no match of right hand side value: :error   # State.Supervisor.lookup

  $ grep -c sun_path <the java log>     -> 1306
  $ grep -c sun_path <the reference log> -> 249      # mix test --include system: 1920 passed
  ```

  So the overflow fires **hundreds of times per run in both runtimes** and is
  usually survived — a refused connect is retried under a fresh session token,
  and a token one digit shorter fits. It becomes a test failure only where the
  scenario cannot recover.
- **Intermittent by exactly that much.** The identical command 12 minutes later,
  same tree, same jar: **1919/1920**, `1258` `sun_path` lines, and only the
  *second* of the two schema tests failed — its runner drew a 40-character id
  (`schema_evolution_e2e_123428_126547821466`) where the first drew 39
  (`…_22140820659`). One digit of a session token is the whole difference
  between a green suite and a red one.
- **The distinguishing measurement** is the runner-id length, and nothing else:

  ```
  reference run (passed):  38-39 chars  schema_evolution_e2e_30405_376271936606
  java run (failed):       40 chars     schema_evolution_e2e_419362_660179359383
  ```

  `unique/1` is `System.unique_integer([:positive])`, which grows through a
  BEAM's life — 5 digits early, 6-7 late — so **the same module passes or fails
  by where it lands in the run**. Same generator as **KI-23**, a different wall:
  that entry measured `digits=5` over a plain `mix test` and concluded the
  32-byte operative-id cap is not reached; a `--include system` run mints more
  and reaches **6**, which is what puts this path over 108. Alone it is never
  near the wall:

  ```
  $ FLEZHA_E2E_RUNTIME=java mix test test/system/schema_evolution_e2e_test.exs --include system
  Result: 2 passed        # 1.5 seconds
  ```
- **Which directory is long was not established.** The main node resolves
  `/run/user/1000/flezha` (21 bytes → 84 for a 40-char id, nowhere near the
  limit), while the peer directories `config/test.exs:147` mints are
  `/tmp/flezha_sockets/<node>@127.0.0.1` — up to **48** bytes for
  `native_reject_peer@127.0.0.1`, which is 48 + 63 = **111**. That is the only
  arithmetic in the tree that reaches the observed failure, but the failing
  acceptor was not attributed to a specific node before this was filed.
- **Why not fixed here**: found while closing KI-33, in a change that touches
  the Java emit funnel and one e2e file — nothing to do with socket naming, and
  the fix is a naming/length decision (shorten the socket file name, hash the
  runner id, or bound `unique/1`'s suffix) that belongs with whoever owns the
  discovery ladder.
- **A fix must**: make the path length independent of how late in a run a test
  is scheduled, and fail loudly at construction with the path and its length
  rather than as a driver init error 3 frames away. Note the count, not the
  failure, is the real signal: a run can be green with 249 of these in its log.

### KI-55 · A host SDK applies an inter-procedure edge transform twice

- **Severity**: P1 · **proposal owed**.
- **Where** (re-pointed 2026-08-31 by area 4 segment D, which split the file and
  did **not** touch either site): `EmitContext.collectEdgeOutputs`
  (`sdk/java/flezha-sdk-runtime/src/main/java/com/flezha/sdk/runtime/core/EmitContext.java`)
  applies the consumer edge's transform chain producer-side, and
  `EntityRouting.valuesForEntity`
  (`.../runtime/core/EntityRouting.java`) applies the *same* chain again when the
  consumer receives the value. Both sites run for one inter-procedure **stream**
  edge. Both were `ActivationDispatcher.java:841` and `:318` before the split.
  `valuesForEntity`'s javadoc — *"re-run symmetrically at delivery"* — was carried
  across **verbatim** rather than corrected: that sentence is this entry's to fix,
  not a refactor's.
- **What happens**: a `map` / `flatMap` / `filter` declared on an inter-proc
  edge composes with itself. A `.map(String.class, s -> "M:" + s)` between two
  procedures delivers `M:M:mid:v` where the contract says `M:mid:v`. Silent —
  no error, no counter, just a wrong value.
- **Impact**: `openspec/specs/sdk-poll-core/spec.md` → *"Edge transforms and
  keys evaluate producer-side"* says the transform is applied once, when the
  producing activation completes. Applying it a second time at delivery
  violates that, and a `filter` composed with itself is idempotent while a
  `map` is not — so the shape decides whether the defect shows. `key_by` is
  unaffected (selection, not reshaping).
- **Why it survived**: every transform in the tree is declared on an **entry**
  channel, where there is no producer-side application (the producer is the
  wire) — so `mergedInputAppliesEdgeMapping`, the Rust transform suite and the
  stream-transform e2e all pass and say nothing about this. Two comments assert
  the opposite as design: `valuesForEntity`'s javadoc ("re-run *symmetrically*
  at delivery because only bytes cross the core") and
  `docs/dev_notes/stream-dsl-keyby-and-transforms.md:20`. Re-running is
  symmetric for a key and for an entry channel; it is composition for a map on
  an inter-proc edge.
- **Reproduction** — `EntityRoutingTest.interProcEdgeMapAppliesTwice`
  (added by `java-diamond-resume`, and asserting the CURRENT wrong value so the
  suite is green and a fix is not silent). Invert its assertion to
  `["=M:mid:v"]` and it fails:

  ```
  $ mvn -B -f sdk/java/pom.xml -pl flezha-sdk-runtime \
      -Dtest=ActivationDispatcherTest#interProcEdgeMapAppliesTwice \
      -Dsurefire.failIfNoSpecifiedTests=false test
  Expecting actual:
    ["=M:M:mid:v"]
  to contain exactly (and in same order):
    ["=M:mid:v"]
  ```

  The fixture is `MappedPipelineOperative`: `c.input(String.class, "in")
  .into(MidProc.class).named("mapped_mid").map(String.class, s -> "M:" + s)
  .into(KeyRecorder.class)` — one producer, one mapped inter-proc edge, one
  consumer.
- **Blast radius is not Java only** — the same two-site shape is in every
  byte-level host, because `HostChannelDecl` carries no transform
  (`flz_core/src/host/register.rs:466` builds every host consumer edge with
  `transform: None`), so each host must apply its own and each applies it at
  both ends: **Node** `src/ctx.ts:157` + `src/dispatch.ts:283`/`:363`,
  **Python** `flezha/ctx.py:225` + `flezha/dispatch.py:276`, **PHP**
  `src/Ctx.php:231` + `src/Dispatch/Dispatcher.php:462`. Ruby declares no
  transform surface. **Reproduced on Java; the other three are read, not run.**
  Rust is unaffected: `flz_core::routing::route_to_consumers` evaluates in
  process, once.
- **Not fixed here on purpose**: found while closing KI-33 (the workflow half of
  the same seam, `docs/dev_notes/java-diamond-resume.md` D6). The workflow arm
  removed the double application for workflow classes by releasing the value
  raw; the **stream** path is a separate defect with a separate blast radius —
  it decides which of the two sites is authoritative for four SDKs at once, and
  the spec sentence says producer-side, so the consumer half is the one that
  looks wrong. That is a design decision, not a patch.
- **A fix must**: pick ONE seam per host SDK and prove it with a transform on an
  inter-proc edge (not an entry channel), in every host SDK that carries the
  surface; and correct the two comments that call the second application
  symmetric. Note that the workflow arm now depends on `valuesForEntity` being
  the consumer-side application — removing it would silently drop workflow edge
  transforms.

### KI-41 · A re-entrant outbox window advances the watermark before the park it is dispatching is durable

- **Severity**: P1 · **investigation first**.
- **Where**: `lib/flezha/state/wal_writer.ex` — `take_outbox_window/3` calls
  `await_entry/4` *before* `apply_effect/2` for the same entry.
- **What happens**: an entry with no outputs completes inside `await_entry/4`
  (tally `pending: 0`), so `complete_entry({:cursor, seq})` re-enters
  `try_dispatch_outbox/1` and a nested window dispatches the *next* record and
  reports its consumed-offset — all before the outer entry's own
  `apply_effect/2` runs. A zero-output entry is exactly what a dead-letter park
  produces, and `apply_effect/2`'s own comment states the required order: "the
  watermark move can truncate the poison input out of the input-log, so the DLQ
  store entry must be durable first". Under re-entry that order inverts.
  `drain_confirmed/1` likewise advances the in-memory `outbox_flushed_seq` past
  the park before the park is stored.
- **Evidence**: a probe on `dispatch_outbox_window/2`, `take_outbox_window/3`,
  `apply_effect/2` and `advance_consumed/2`, driven by
  `test/flezha/state/wal_writer_outbox_test.exs` "a nested window's advance
  survives the outer window closing", printed exactly this order:
  `take seq=0` → `complete_entry {:cursor, 0}` → nested `window-open` →
  `take seq=1` → `apply_effect seq=1 offset=2` → `record_consumed 2` →
  `apply_effect seq=0 dlq=true` (the park) → outer `record_consumed`.
- **Impact**: the exposure is the few microseconds between the nested window's
  `record_consumed/3` and the outer entry's park write, inside one `do_fsync/1`
  on one process. A retention sweep landing in that window could truncate the
  poison input while the DLQ entry is not yet stored. A crash in the window is
  safe: the boot re-drive re-reads the WAL and `apply_effect/2` is idempotent.
  `persist_dedup/1` runs at the end of the same `do_fsync/1`, after the park, so
  the *durable* cursor is never inverted.
- **Why not fixed here**: found while instrumenting
  `fix-outbox-watermark-overwrite` (its task 1.2, the design's Open Question).
  That change's own Non-Goals rule the re-entrancy out of scope — reshaping
  `complete_entry/2`'s control flow belongs to `refactor-5a-state` — and this is
  an ordering inversion, not the double-write that change fixes.
- **A fix must**: run an entry's `apply_effect/2` before anything can complete
  it, or stop `complete_entry/2` re-entering dispatch while a window is open.
  Note the two are the same seam `refactor-5a-state` will restructure.
- **Triage (2026-08-30)**: **REAL** — the ordering is unchanged at HEAD and
  reproduces deterministically — **but the impact claim is NOT confirmed, and no
  loss was constructible.** `fix-outbox-watermark-overwrite` landed as
  `9410eea7` ("fix(state): the consumed watermark stops moving backwards", an
  ancestor of HEAD). It clamped `InputLog.record_consumed/3` monotonically and
  did not touch `take_outbox_window/3`, which still calls `await_entry/4` at
  `lib/flezha/state/wal_writer.ex:1060` and `apply_effect/2` at `:1062` (the
  entry said `:1058`/`:1060`; two lines of drift, corrected 2026-08-31 against
  HEAD by area 5a's survey). The
  trigger is precisely a **zero-output** entry: `await_entry/4` →
  `register_entry/4` ends in `tally(key, & &1)`, and `complete?/1` holds on
  `pending: 0`, so `complete_entry({:cursor, seq})` fires inside the very call
  that registered the entry.
- **Reproduction** — a local trace over the functions the entry names, driven by
  the two-record batch it describes. Write the probe to the scratchpad and run
  it with `mix test` (it needs `test/helpers`); it is a probe, not a repo test.

  ```elixir
  # /tmp/ki41_probe_test.exs — setup copied from
  # test/flezha/state/wal_writer_outbox_test.exs (tmp_app_data_dir,
  # start_state_tree, a per-test dlq dir, effect_header/effect_body/output).
  :erlang.trace_pattern({Flezha.State.WalWriter, :take_outbox_window, 3}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :apply_effect, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :advance_consumed, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :complete_entry, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.WalWriter, :dispatch_outbox_window, 2}, true, [:local])
  :erlang.trace_pattern({Flezha.State.InputLog, :record_consumed, 3}, true, [:local])
  :erlang.trace_pattern({Flezha.Dlq.Store, :append, 1}, true, [:local])
  :erlang.trace(:all, true, [:call, {:tracer, collector}])

  in_one_window(fn ->
    # seq 0: a park — ZERO outputs, consumed_offset 1.
    :ok = WalWriter.append(pid, 0, effect_header(),
            effect_body([], 1, %{channel: "in", error: "poison", attempts: 3, payload: "kp:poison"}))
    # seq 1: a normal record, consumed_offset 2, durable in the same batch.
    :ok = WalWriter.append(pid, 1, effect_header(), effect_body([output("k2", "b")], 2))
  end)

  :ok = WalWriter.fsync(pid)
  ```

  ```
  $ MIX_ENV=test mix test /tmp/ki41_probe_test.exs
  ===== KI-41 OBSERVED ORDER =====
    dispatch_outbox_window
    take_outbox_window(chunks=0, consumed=-1)
    complete_entry({:cursor, 0})
    dispatch_outbox_window
    take_outbox_window(chunks=0, consumed=-1)
    apply_effect(seq=1 offset=2 dlq=false)
    take_outbox_window(chunks=1, consumed=2)
    advance_consumed(2)
    InputLog.record_consumed(2)
    apply_effect(seq=0 offset=1 dlq=true)
    Dlq.Store.append(origin_offset=1)  <-- THE PARK
    take_outbox_window(chunks=1, consumed=1)
    advance_consumed(1)
    InputLog.record_consumed(1)
  ===== watermark = 2 =====
  >>> KI-41 CONFIRMED: the watermark advanced BEFORE the park was stored
  ```

  This is the entry's trace, function for function. **Control**: give the park
  record ONE output and the same probe prints `apply_effect(seq=0 dlq=true)` →
  `Dlq.Store.append(origin_offset=1)` → … → `InputLog.record_consumed(2)`, i.e.
  `>>> KI-41 NOT OBSERVED — the park was stored first, as intended`. Zero
  outputs is the entire trigger, and the probe can report clean.
- **NOT established — no loss follows from the inversion.** Three facts close
  every path this triage could find. (1) `take_outbox_window/3` only admits
  entries at `seq <= outbox_durable_seq`, so the park's **effect record is
  already fsynced** before any of this runs, and the boot re-drive re-appends
  the DLQ entry (`test/flezha/state/wal_writer_outbox_test.exs` "re-drive of a
  park record is idempotent" pins it). (2) `persist_dedup/1` is never reachable
  inside the nesting — every call site (`:798`, `:862`, `:920`) runs it *after*
  `try_dispatch_outbox/1` returns, by which time the park is written — so the
  **durable** re-drive floor is never inverted. (3) The truncation the entry
  fears costs nothing: `Dlq.Store` carries the park's own `payload`
  (`lib/flezha/dlq/store.ex:340`), and a replay never reads the input log, so
  losing the poison input at that offset loses no recoverable data. The exposure
  is therefore a few microseconds of an **in-memory ETS watermark** being ahead
  of a park that is independently recoverable.
- **Worth fixing**: yes, but as an ordering invariant, **not** as silent data
  loss — this should drop out of the P1 band. It stays worth doing because the
  invariant `apply_effect/2` states in its own comment is one a future reader
  will rely on, and re-entrant `complete_entry/2` makes it locally unreadable.
  Fold it into `refactor-5a-state` as the entry already says; it does not
  justify a change of its own.

---


## P2 — gates that lie, and time sinks



### KI-122 · `checkpoint_ack()` exists in no veneer but Python, and `outputs_at_checkpoint()` does not imply it in any of the four

- **Severity**: P2 · a published spec SHALLs an authoring surface three SDKs cannot spell, and
  the workaround an author would reach for produces a class the core refuses. · Found by the
  R5 survey, 2026-09-01, checking the `sdk-durable-fixture-parity` audit's `effectively_once`
  verdict against the code.
- **Where**: `sdk/node/src/authoring.ts:177-181`, `sdk/ruby/lib/flezha/builder.rb:79-83`,
  `sdk/php/src/Graph/Builder.php:63-71` — `outputsAtCheckpoint` / `outputs_at_checkpoint`,
  and the `checkpointAck` / `checkpoint_ack` method that is absent beside them.
  `sdk/python/src/flezha/builder.py:164-168` has the method and the same missing implication.
- **The rule they miss**:
  `openspec/specs/effectively-once-checkpoint-granularity/spec.md:15-17` — *"An operative with
  the effectively-once tier enabled SHALL be able to opt into `checkpoint` ack-granularity
  (`ChannelBuilder::checkpoint_ack()`)"*. The requirement names no SDK, so the Cross-SDK rule
  applies it to all seven. `docs/configuration.md:522-524` documents `checkpoint_ack()`
  (`checkpointAck`) as a knob and states that `checkpoint_every(n)` **and**
  `outputs_at_checkpoint(max_delay_ms)` each *"Implies `checkpoint_ack()`"*.
- **What Rust and Java do, and the veneers do not.** Both route all three options through one
  seam that turns the granularity on first: `sdk/rust/src/channel/builder/mod.rs:195-205`
  `checkpoint_policy`, and Java `StreamOperativeTopologyBuilder.java:239-242`
  `amendCheckpoint`. The four veneers instead keep three independent fields, and only
  `checkpoint_every` writes `ack_granularity`.
- **Two halves, one seam.**
  - **A — no `checkpoint_ack()` at all** in node, ruby and php. A class that wants the
    checkpoint acknowledgement with **no** declared cadence — `checkpoint_store` and
    `checkpoint_batched_store` in the reference — is inexpressible.
  - **B — `outputs_at_checkpoint()` does not imply it**, in all four veneers including Python.
    Python's fixture masks this by calling `.checkpoint_ack().outputs_at_checkpoint(1_500)`.
- **Reproduced** (2026-09-01). Declaration side, one script per SDK against the checked-in
  builder, `effectively_once()` + `outputs_at_checkpoint(1500)` and nothing else:

      node: ack_granularity = "per_input"   checkpoint_outputs = "at_checkpoint"
            ChannelBuilder.checkpointAck === undefined
      ruby: ack_granularity = "per_input"   checkpoint_outputs = "at_checkpoint"
      php:  ackGranularity  = per_input     checkpointOutputs  = at_checkpoint
            method_exists($b, 'checkpointAck') = NO

  Registration side, the same Node declaration through `native/flz_core_ffi.node`:

      $ node ack2.mjs
      declareClass REFUSED: checkpoint_every / checkpoint_outputs /
      checkpoint_max_delay_ms require ack_granularity checkpoint

- **The mechanism, named to the function.** `flz_core/src/host/register.rs:160-170` refuses a
  declaration whose `checkpoint_outputs` is `AtCheckpoint` while `ack_granularity` is not
  `Checkpoint`. In Rust and Java that combination is unreachable because
  `checkpoint_policy` / `amendCheckpoint` sets `AckGranularity::Checkpoint` before amending
  the policy. In the veneers it is the **only** thing `outputs_at_checkpoint` can produce, and
  the refusal names a knob three of them have no method for. The failure is loud, not silent:
  the class does not register.
- **Blast radius**: no shipped fixture hits it — Python's calls both, and node/ruby/php carry
  no `effectively_once` fixture at all, which is why nothing has caught it. It blocks two of
  the four `effectively_once` fixtures R5 must port, and the
  `test/fixtures/declarations/checkpoint_batched_store.json` golden
  (`"ack_granularity": "checkpoint"` with no `checkpoint_every`) cannot be produced by any of
  the three veneers today.
- **This corrects three published claims.**
  `openspec/changes/sdk-durable-fixture-parity/design.md:80-87` rules `effectively_once` **F**
  ("missing fixture — the SDK can already do this") for node, ruby and php, citing the three
  methods that are present without noticing the fourth is absent. `docs/dev_notes/node-sdk.md:203-207`,
  `ruby-sdk.md:112-114` and `php-sdk.md:136-141` each say the deferral is *"fixtures, not
  mechanism"*. For `effectively_once` that is not true in any of the three.
- **A fix should** give each veneer the Rust/Java seam rather than a fourth flag: one private
  `amend_checkpoint` that sets the granularity and amends the policy, with `checkpointAck`,
  `checkpointEvery` and `outputsAtCheckpoint` all going through it. ~15 lines per SDK,
  deletion-funded against the three independent assignments each has today. Python takes the
  same seam for half B. Whether the *implication* is right, or the veneers should refuse the
  combination at build time the way `flz_core` refuses it at registration, is the one open
  question — `docs/configuration.md:523-524` says imply, and Rust and Java do.


### KI-115 · Two published specs mandate behaviour the code does not have

Found by area 8's docs sweep, 2026-09-01. **Both are spec-vs-code gaps, not doc drift**, so
area 8 correctly left them alone — a spec is normative, and correcting it is a requirement
change, not a documentation fix.

**A · `openspec/specs/controller-packaging/spec.md:43` SHALL-mandates a removed Make
target.** The requirement opens *"`make docker-build-multiarch` SHALL produce manifests for
exactly `linux/amd64` and `linux/arm64`"*. That target does not exist:

    $ grep -n 'docker-build-multiarch' Makefile
    (no output)

`463e9f40` *"build: publish multi-arch controller image via a native per-arch script"*
removed it deliberately; the manifest list is now `make docker-publish` →
`scripts/publish-image.sh:88`–`:106`, run once per architecture host. **This one is a stale
SPEC** — the mechanism was replaced and the requirement was not carried across. A fix must
be a `## MODIFIED Requirements` block naming the current mechanism, carrying every
surviving scenario verbatim.

**B · `openspec/specs/plugin-installation/spec.md:409` MUST-mandates a telemetry event that
is emitted nowhere.** The requirement says `Recovery.Coordinator` *"MUST emit a
`[:flezha, :recovery, :orphaned_plugin]` telemetry event with the plugin id"*:

    $ grep -rn 'orphaned_plugin' lib/ | wc -l
    0
    $ grep -rn ':flezha, :recovery' lib/ | wc -l
    0

**Which side is wrong is NOT established and must not be guessed.** `lib/flezha/boot/
recovery_hooks.ex` emits a different event about ProcessHub's orphan *reconcile*
(`candidates`/`orphans`/`started`/`duplicates`/`elapsed_ms`) — a different mechanism about
a different thing. So either the skip-and-report behaviour exists without its observability
(**a defect of the class this programme has closed repeatedly today: something silent that
a spec says should be loud**), or the requirement describes a superseded design.

**A fix must** first answer which: does `Recovery.Coordinator` skip an orphaned plugin row
at all today, and if so does anything report it? If the behaviour exists and only the event
is missing, that is a code defect and the spec is right. If the whole mechanism was
replaced, the spec is stale like A. **Do not amend the spec until that is measured** —
amending it to match the code would ratify a silence the spec deliberately forbade.


### KI-6 · `ruff check` is red, and is deliberately not gated

- **Where**: `sdk/python`; `scripts/format-apply.sh` runs `ruff format` only.
- **Status (2026-08-30)**: the *silence* this entry was really about is closed
  by `format-gate-tells-the-truth`. The decision is written where a reader
  looks for it — a comment beside the `ruff format` call in
  `scripts/format-apply.sh` says the linter is out of the gate, why, and that
  adopting it is its own change. What is **not** closed is the 77 errors, so
  the entry stays open rather than becoming folklore.
- **Why not gated**: clearing it is a Python SDK source change with judgement
  in it, not a formatting pass. `DTZ005`/`DTZ001` are the ambient-clock family
  the Python SDK deliberately guards against (`docs/dev_notes/python-sdk.md`)
  and are worth reading, not silencing; `RUF022`/`RUF023` reorder `__all__` and
  `__slots__`, and `__slots__` order is not free of meaning.
- **Reproduction** (unchanged, re-measured 2026-08-30):

  ```
  $ sdk/python/.venv/bin/ruff check --statistics sdk/python test/apps/python_runner
   17  I001    [*] unsorted-imports
   16  RUF022  [-] unsorted-dunder-all
    9  UP035   [*] deprecated-import
    7  RUF023  [*] unsorted-dunder-slots
    5  B017    [ ] assert-raises-exception
    4  BLE001  [ ] blind-except
    4  UP017   [*] datetime-timezone-utc
    3  DTZ005  [ ] call-datetime-now-without-tzinfo
  … 19 rules in all
  Found 77 errors.
  [*] 55 fixable with the `--fix` option
  $ echo $?
  1
  ```
- **What adopting looks like**: four rules (`I001`/`RUF022`/`UP035`/`RUF023`, 49
  findings) are mechanical and fixable in one pass; the remaining 28 need a
  site-by-site read. Then one line in `scripts/format-apply.sh` — a
  `gate_run "ruff check" …` beside the `ruff format` one — turns it on for the
  local gate and for CI at the same time, because CI runs that script.
- **Worth fixing**: yes, as its own change.

### KI-14 · `flezha-php-zts` has no coverage driver

- **Where**: the PHP image — neither xdebug nor pcov, so `phpunit --coverage-text`
  cannot run in the container. Blocks any PHP coverage diff.
- **Triage (2026-08-30)**: **REAL**, reproduced, and true of both images.
- **Reproduction**:

  ```
  $ docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work/sdk/php \
      flezha-php-zts php vendor/bin/phpunit --coverage-text
  PHPUnit 10.5.64 by Sebastian Bergmann and contributors.
  Runtime:       PHP 8.4.25
  ...............................................................  63 / 129 ( 48%)
  ..........S.................................................... 126 / 129 ( 97%)
  ...                                                             129 / 129 (100%)

  There was 1 PHPUnit test runner warning:

  1) No code coverage driver available

  OK, but there were issues!
  Tests: 129, Assertions: 1284, PHPUnit Warnings: 1, Skipped: 1.
  ```

  `php -m` lists neither `xdebug` nor `pcov` in `flezha-php-zts` **or**
  `flezha-php`. The tests themselves run fine; only coverage is impossible.
- **A dead end worth recording**: `phpdbg` *is* present at
  `/usr/local/bin/phpdbg` in the ZTS image, but it is not a way out —
  `phpunit/php-code-coverage` 10.1.16 (under PHPUnit 10.5.64) dropped phpdbg
  support, so only Xdebug or PCOV will do.
- **Worth fixing**: yes, and it is one line in the image — `pecl install pcov`
  plus `extension=pcov.so`, which costs far less runtime than Xdebug. It matters
  because a coverage diff is how this repo decides test deletions
  (`docs/dev_notes/refactor-1-kernel.md`), and PHP is the one SDK that cannot
  produce one. Small enough to ride along with any other PHP image change.

### KI-15 · The Ruby veneer needs an explicit bindgen include path here

- **Where**: this box lacks `libclang-common-*-dev`, so the Ruby build needs
  `BINDGEN_EXTRA_CLANG_ARGS=-I$(gcc -print-file-name=include)`. Handled in
  `scripts/build-ruby-sdk.sh`; recorded because the failure reads as a broken
  Ruby rather than a missing header.
- **Triage (2026-08-30)**: **REAL**, reproduced exactly — and the entry
  understates it: the guard covers the build script only, so every **direct**
  cargo invocation on a box like this one still dies.
- **Reproduction** — the box's premise first: no `libclang-common-*-dev` is
  installed and `/usr/lib/llvm-*/lib/clang/*/include/stdarg.h` does not exist,
  while `$(gcc -print-file-name=include)/stdarg.h` does. Then, with the SDK's
  own Ruby on `PATH` and `BINDGEN_EXTRA_CLANG_ARGS` unset:

  ```
  $ PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH" \
    env -u BINDGEN_EXTRA_CLANG_ARGS cargo build -p flz_core_ffi --features ruby
  error: failed to run custom build command for `rb-sys v0.9.130`
    /home/moon/.rbenv/versions/3.4.10/include/ruby-3.4.0/ruby/ruby.h:23:10:
        fatal error: 'stdarg.h' file not found
  exit 101
  ```

  `scripts/build-ruby-sdk.sh:23-33` exports the include path and the same build
  then succeeds — but nothing else does. `cargo test -p flz_core_ffi --features
  ruby-embed`, which `ruby-sdk.yml:121` runs and which the refactor programme
  runs directly, never passes through that script.
- **A trap the entry does not name**: with the *system* Ruby (3.2.3, no
  `ruby-dev`) the build fails earlier and differently — `'ruby.h' file not
  found` — which hides the stdarg.h symptom entirely and sends the reader after
  the wrong header.
- **The entry's own remedy was tried and cannot be built** (2026-08-30,
  `small-mechanical-fixes`). `rb-sys` is a *dependency* of `flz_core_ffi`, so
  cargo runs its build script — the one that runs bindgen — before
  `flz_core_ffi/build.rs` exists to set anything: the reproduction dies at
  `Compiling rb-sys v0.9.130` and this crate's build script never runs. There is
  no channel back either; `cargo::` metadata flows from a dependency to its
  dependents. The only mechanism that reaches a bare `cargo test -p flz_core_ffi
  --features ruby-embed` is `.cargo/config.toml`'s `[env]`, which takes a
  **literal** value — here `-I/usr/lib/gcc/x86_64-linux-gnu/13/include`, i.e.
  gcc 13, `x86_64`, Debian layout. Committing that would set the variable on
  every box, point most of them at a directory that does not exist, and leave
  this same failure behind a guard that looks general. Worse than none, so the
  wrapper stays.
- **The residue, and it stays open**: `cargo test -p flz_core_ffi --features
  ruby-embed` is **unprotected** on a box without clang's builtin headers.
  Re-proved after `cargo clean -p rb-sys`, rbenv 3.4.10 on `PATH`, the variable
  unset: exit 101, `'stdarg.h' file not found`. Export
  `BINDGEN_EXTRA_CLANG_ARGS=-I$(gcc -print-file-name=include)`, or install
  `libclang-dev` — which is what `ruby-sdk.yml` does, so **CI is not exposed**,
  and that workflow is the only caller of the ruby features besides the wrapper
  (`build-dist.sh` does not build them).
- **What did change**: the `'ruby.h'` trap above is now diagnosed rather than
  left to mislead. `scripts/build-ruby-sdk.sh` refuses *before* cargo when the
  Ruby on `PATH` has no development headers, names that as the cause and says it
  is **not** the include-path problem; and the reason no build-time home exists
  is written beside the bindgen guard, where the next reader looks.

### KI-58 · A metric NAME is never checked against the exposition grammar

- **Severity**: P2 · found 2026-08-30 while driving KI-45; **out of scope of
  `close-prometheus-label-coercion` on purpose** (its design D5).
- **Where**: `PrometheusRegistry.exposition_name/2`.
- **What happens**: the label-name rule that change adds does not cover the
  metric name itself. A source registered as `:"weird source.name"` with a
  metric `:"3bad-name"` renders `weird source.name_3bad-name` — a space, a dot
  and a leading digit, none legal in `[a-zA-Z_][a-zA-Z0-9_]*`, and Prometheus
  rejects the whole scrape.
- **Why it is not the same fix**: a label key arrives from third-party
  metadata at runtime and must be sanitized, because refusing it would silently
  drop a caller's dimension. A metric name is a **developer-chosen atom**, known
  at registration, and belongs refused at `Registry.register_source/1` where the
  developer sees the error — not quietly rewritten on every scrape.
- **A fix must** put the check in `observability-foundation`'s registration
  path, not in the exporter.


### KI-60 · Two different JSON *value* rules ship in one tree

- **Severity**: P3 · found 2026-08-31 while closing KI-59.
- **Where**: `AuditLog.Codec.stringify_value/1` and
  `CliManager.inspect_or_pass/1`.
- **What happens**: both answer "how does an arbitrary term become a JSON
  value?", and they disagree. `stringify_value/1` recurses into lists and maps
  and passes `nil` through as JSON `null`; `inspect_or_pass/1` inspects a list
  or map and renders `nil` as the string `"nil"`. Two answers to one question,
  both shipping, in outputs an operator may compare side by side.
- **Why it was not collapsed with KI-59**: each is individually defensible for
  its own consumer, and collapsing either changes an output shape that something
  may parse. It needs a decision about which shape is correct before a fix, not
  a mechanical merge.
- **Also here**: `PrometheusRegistry.truncation_count/1`'s doc says
  `flz observe sources --json` reads it. Nothing does.
- **A fix must** pick one rule and state why, the way `metadata_text/1` now
  does for the text case — the point of that extraction is that there is one of
  it.

### KI-76 · `composer.lock` pins a dependency above the PHP floor `composer.json` declares, and the format gate reports it as unformatted PHP

- **Severity**: P2 · no runtime effect (`sdk/php` ships no vendor tree); it makes
  `scripts/format-check.sh` red on any PHP 8.1–8.3 box and names the wrong cause.
- **Where**: `sdk/php/composer.json:` `"require": {"php": ">=8.1"}` with no
  `config.platform.php`, against `sdk/php/composer.lock`'s
  `symfony/console v8.1.5`, which requires `php >=8.4.1`. The offending syntax is
  `sdk/php/vendor/symfony/console/Command/Command.php:681` —
  `(new \ReflectionClass(static::class)->getAttributes(...)`, PHP 8.4's
  parenthesis-free `new`. `scripts/format-apply.sh:38` runs
  `${FLEZHA_PHP:-php}`, so an unset `FLEZHA_PHP` picks the host interpreter.
- **Reproduction** (this box: system `php` 8.3.6 NTS, vendor tree installed from
  the `flezha-php` image, which carries 8.4.25):

  ```
  $ php sdk/php/vendor/bin/php-cs-fixer fix --dry-run --config .php-cs-fixer.dist.php
  PHP Parse error:  syntax error, unexpected token "->" in
    /home/moon/Code/flezha/sdk/php/vendor/symfony/console/Command/Command.php on line 681
  $ echo $?
  255

  $ scripts/format-check.sh; echo $?
  ...
  UNFORMATTED — run scripts/format-apply.sh:
    php-cs-fixer
  FAILED — the output above says why:
    formatters (scripts/format-apply.sh --check)
  1
  ```

  With the interpreter the vendor tree was installed for, the same files are
  clean and the gate reports only the genuinely absent tool:

  ```
  $ FLEZHA_PHP="docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work flezha-php php" \
      scripts/format-check.sh; echo $?
  ...
  Found 0 of 89 files that can be fixed in 0.605 seconds
  NOT RUN — the tool is not installed here:
    rubocop — no rubocop: install the development dependency in sdk/ruby/flezha.gemspec
  2
  ```

- **Mechanism**: `composer.json` declares the package's PHP floor and nothing
  pins the *platform* composer resolves against, so a `composer update` run on a
  PHP 8.4 box locks dev dependencies that the declared floor cannot parse. The
  lock is tracked; the vendor tree is not, so the mismatch is invisible until an
  8.1–8.3 interpreter loads it. `gate_run` then sees php-cs-fixer exit non-zero
  and files it under `UNFORMATTED`, which is the one thing it is not: through
  8.4.25 the same 89 files come back **0 to fix**. This is KI-4's shape one layer
  down — a tool that *errored* being reported as a check that *failed*.
- **Not this area's**: found by `refactor-4-java-sdk` segment A task 2.1, which
  runs the format gate; it touches no `sdk/java` file and predates the area
  (nothing in the area's diff is PHP, and `composer.lock` is unmodified).
- **A fix must** make the two tracked files agree — either
  `config.platform.php` pinned at the declared floor so `composer update`
  resolves what `>=8.1` can run, or the floor raised to `>=8.4` and said so in
  `sdk/php`'s manifest and docs — and, separately, keep "the tool errored" out of
  the `UNFORMATTED` list, since a wrong cause costs more than a missing one.

- **Supervisor verdict 2026-08-31 — real, P2, and split.** The **gate half** —
  `format-check.sh` reporting `UNFORMATTED — php-cs-fixer` for a tool that *crashed* —
  is a defect on its own terms and fixable by whoever next owns the format gate. It is
  the same class as `docs/dev_notes/format-gate-tells-the-truth.md`: a gate that names
  the wrong cause sends the next person to fix something that is already correct, which
  is worse than a gate that is simply red.
- **The floor half is the repository owner's, not a supervisor's**: whether the PHP SDK
  supports **8.1** (add `config.platform.php` so composer resolves inside the declared
  floor) or **8.4+** (raise the declared floor to match the lock) is a support-matrix
  decision. Recorded under *Awaiting the user's decision* on the refactor board.
- **Not area 4's work** and deliberately not folded into it: it is `sdk/php` plus the
  format gate, and an area whose contract is zero behaviour change is the wrong place
  for it.
- **It explains two earlier dismissals.** Rows 5+6 and 6.5 both hit this parse error and
  recorded it as *"environment, pre-existing"* — accurate, and without a mechanism.
  This entry supplies the mechanism, which is why the dismissals were not wrong so much
  as unfinished.


---

### KI-79 · Five `flz` verbs exit 0 when the RPC they issued failed

- **Severity**: P2 · a script cannot tell "the CLI said no" from "the CLI said
  nothing", which is the exact distinction
  `docs/dev_notes/restore-state-persistence-smoke.md` had to add an exit-code
  check to recover.
- **Where**: `cli/src/operative.rs:217` (`handle_stop`), `:314` (`handle_rewind`),
  `:361` (`handle_replay`), `:388` (`handle_state`), and `cli/src/controller.rs:96`
  (`handle_start`). Each ends in a `match status { Ok(s) if s.success() => {},
  Ok(_) => eprintln!(...), Err(e) => eprintln!(...) }` with **no `exit()` in
  either failure arm**, so `main` returns normally and the process exits 0.
- **Evidence** — a copy of `target/debug/flz` in a temp dir beside a `flzvm`
  shim that prints to stderr and exits 7:

  ```
  exit=0   flz operative stop op1
  exit=0   flz operative rewind op1 --to-iteration 3
  exit=0   flz operative replay a/b/c --from 5
  exit=0   flz operative state op1
  exit=0   flz controller start
  exit=1   flz operative send op1 chan {}      <- the correct sibling
  exit=1   flz operative list
  ```

  Deterministic, 5/5 on repeat. `flz operative send`
  (`cli/src/operative.rs:263`) has the identical `match` **with**
  `Ok(_) => exit(1)` and a comment saying why — so this is drift between hand-copied
  blocks, not a decision anyone made.
- **Mechanism**: there is no shared "run one RPC" seam. The area has **34**
  `Command::new(...).arg("rpc")` sites in **five** exit-code dialects — 17 exit 1
  with `"Error: Command execution failed."`, 5 propagate `status.code()`, 2 exit 3
  silently, 3 diagnose the spawn and exit 3, and these 5 exit 0. Only four sites
  call a helper, and three of those four helpers are copies of each other.
- **Why it was not fixed on the spot**: found by the `refactor-6-cli` survey,
  which changes no code. It is **not** a refactor: it changes the exit code of
  five operator verbs, so charter §1b step 6 requires its own commit with a
  red-first test.
- **A fix must** give the five failure arms the `exit(1)` their sibling already
  has, and be proved by a test that is **shown red on unmodified HEAD** for all
  five verbs — a `TempDir` holding a copy of `CARGO_BIN_EXE_flz` beside a failing
  `flzvm` shim is enough and needs no controller. It must **not** also unify the
  five dialects: which of 1 / 3 / `code` each verb should return is an
  operator-surface decision, separate from "a failure is not a success".

- **Supervisor verdict 2026-08-31 — REAL, and it is area 6's batch 0.** It is drift,
  not a decision: `operative send` carries the identical `match status` **with**
  `exit(1)` and a comment, six lines away. Fix it **red-first and as its own commit,
  before the rpc-seam refactor touches those 34 sites** — the survey's own risk 3 is
  right that one commit would leave a bisect unable to separate an exit-code change
  from a structural regression.

## P2 — tests that lie or leak

### KI-114 · `release_cli_system_test` cannot pass from inside `mix test`, and a failed setup leaks a live daemon

- **Severity**: P2 · `mix test --include system` reports `1 invalid` on every run, and a
  failed run poisons later ones. Pre-existing — `FLEZHA_STATE_DIR` appears **0** times in
  that file at `refactor-baseline-7`, so it predates the test refactor; the state-root
  claim it trips over landed with `support-multi-controller-colocation`.
- **Where**: `test/flezha/plugin/installer/release_cli_system_test.exs:61`–`:73` (the `env`
  list) and `:74`–`:84` (the `on_exit` ordering).
- **Evidence** — the daemon's own log, reproduced by hand outside the test and again after
  `mix release --overwrite`, deterministic 3 of 3:

      ** (exit) {:bad_return, {{Flezha.Application, :start, ...},
        {:EXIT, {%RuntimeError{message: "state root .../app_data/state is already claimed
        (held by pid 3361080): two controllers writing one entity WAL tree corrupt it, so
        this one will not start. Set FLEZHA_STATE_DIR to a state root of this controller's
        own ..."},
        [{Flezha.Boot.StateRootLock, :claim!, 1, ...},
         {Flezha.Application, :start_controller, 0, ...}

      Result: 0 tests, 1 invalid
      daemon did not become ready within 60000ms

- **Mechanism, named to the function**: the `env` list sets `FLEZHA_DATA_DIR`,
  `FLEZHA_PLUGIN_ROOT` and `FLEZHA_SOCKET_FILES_PATH` but **not `FLEZHA_STATE_DIR`**, and
  `Paths.state_root/0` (`lib/flezha/state/paths.ex:90`) falls back to `app_data/state` —
  which the running `mix test` BEAM already holds. **The refusal message names the exact
  fix and the test does not take it.**
- **The second half is worse than the first.** The flunk happens at `:77` and the `on_exit`
  that runs `flzvm stop` is registered at `:79`, so a failed `setup_all` **leaks a live
  release daemon holding `flezha@127.0.0.1`**. Every later attempt in that release
  directory then dies with `Protocol 'inet_tcp': the name flezha@127.0.0.1 seems to be in
  use`, while `flzvm pid` still answers (it is node-name agnostic) so `ready?/1`'s first
  clause passes against the **wrong node**. That cascade was observed and the leaked daemon
  cleaned up by hand.
- **A fix must** add `{"FLEZHA_STATE_DIR", Path.join(tmp, "state")}` to the env list — what
  the refusal instructs — **and** register the `on_exit` **before** `wait_until_ready!/1`,
  so a failed setup cannot leak the daemon. Both halves, or the next failure poisons the
  next run.


- **Evidence against "every run" (supervisor, 2026-09-02)**: of three full `--include system` arms run one at a time at `0ba5c9fe`+S5 — node, ruby, php — only the ruby arm produced the `1 invalid` (`daemon did not become ready within 60000ms` at `release_cli_system_test.exs:77`, 183 s arm); node and php did not. With session 6's clean baseline arm that is 1 of 4 measured, so the entry's "fails on every run" is refuted; the failure is real and intermittent.

### KI-18 · Load-sensitive tests, each seen once and green alone

Recorded so a future red run is not mistaken for a regression. **Green in
isolation is not evidence** — reproduce under load before concluding anything
(see `docs/refactor/STATE.md` for the stress-regime method).

**Status 2026-08-31, `green-the-test-hygiene-tail`**: of the eight tests this
entry collected, **two are fixed, two are dropped as verified cured, and four
stay recorded with their mechanisms and no fix.** Every coordinate below has
been re-pointed and now carries the test's name — **six of the eight had
drifted** (`591→598`, `383→389`, `413→419`, `303→304`, `390→395`, `384→440`),
and a `file:line` that rots is how an entry stops being checkable.

**The tag paragraph this entry used to carry is deleted, because the work it
called "the most urgent thing in this entry" is done.** All seven
`refactor-baseline-*` tags resolve and all seven are ancestors of HEAD:

```
$ for t in refactor-baseline refactor-baseline-0a refactor-baseline-0b \
           refactor-baseline-1 refactor-baseline-1s refactor-baseline-2 refactor-baseline-3; do
    printf "%-22s " "$t"; git merge-base --is-ancestor "$t" HEAD && echo "$(git rev-parse -q --verify $t^{commit}) ancestor=YES"; done
refactor-baseline      2a12065d2b0de3740f2f387567dd56934e4d1421 ancestor=YES
refactor-baseline-0a   2a12065d2b0de3740f2f387567dd56934e4d1421 ancestor=YES
refactor-baseline-0b   a147111c2c32c59ec540fb9f78aebfebd0b7d141 ancestor=YES
refactor-baseline-1    df3fc0f4fd56c30f792641e73b2c1d30f4f34f82 ancestor=YES
refactor-baseline-1s   10939a9c71ac1c52123a6b5df82ce0260d1829ea ancestor=YES
refactor-baseline-2    b0697e49ae36f3caabcdcfd449c279e99e5ed1b1 ancestor=YES
refactor-baseline-3    61bbcb987df5e665d9196c73d43bb47d78dc3112 ancestor=YES
```

Each points at the `dev` twin the 2026-08-30 triage table named, so every
reproduction instruction that rests on one is valid again.

#### Fixed

- **`test/plugin/kafka/worker_registry_test.exs:304`** — *"three callers swap
  after worker death; entry refs is preserved (1.7 / path A)"*. **A missing
  barrier, and it had two faces.** `handle_call({:swap, {key, old_token}, caller}, …)` (`worker_registry.ex:185`)
  checks `state.entries[key]` first, so a monitor `:DOWN` still queued behind
  the three `send(pid, :swap)` leaves the dead pid in the entry: the starter is
  never called and `:385`'s `assert_receive {:starter_called, _new_worker_pid}, 100` fails —
  or, if the DOWN lands between the swaps, tokens are dropped and
  `:390`'s `assert entry.callers == 3` fails with `2`, which is the face this entry
  originally recorded. Fixed by waiting for the signal the registry already
  publishes: the three spawned callers forward
  `{:kafka_worker_dead, {key, token}, reason}` — `worker_registry.ex:378`–`:380`
  sends it, and this same module already waits on it at `:294`–`:295` — and the
  test asserts three arrivals before it issues any swap. The `eventually/2` +
  `inspect_state` poll its sibling uses at `:440` was deliberately **not**
  copied: it polls for the effect of an event the system announces, and needs a
  timeout to tune. **The window was forced, never sampled** — a scratch
  instrument blocked the registry inside `handle_call` through its
  test-supplied `starter`, queued the three swaps, killed the worker behind
  them and released; the unfixed test then failed at `:385` on every run. The
  instrument was deleted; the module is 5 for 5 across five seeds.
- **`test/flezha/state/watchdog_test.exs:395`** — *"a remote-durability RPO
  breach alarms once and clears when it catches up"*. **A leak, not load.**
  `DurabilityTier.breaches/0` folds `Shipper.status()`, which is node-global,
  and the closing `assert DurabilityTier.breaches() == []` asserted about every
  emitter on the node; the entry it found belonged to another module
  (`{:entity, {"TestOperative", "TestProcedure", "p1"}}`, the default id in
  `test/helpers/state_test_helpers.ex`). Eight lines above, the same test pins
  its `assert_receive` for exactly this reason. Fixed by carrying that pin to
  the closing assertion —
  `breached? = fn -> Enum.any?(DurabilityTier.breaches(), &(&1.ref == ref)) end`
  (`:434`), used both at `:456` and at the `wait_until` at `:437`, which
  acquires the identical exposure the moment a foreign entry exists.
  **Forced deterministically**: the test now records a *second, foreign* breach
  through the same production path and never clears it, so the ledger is
  deliberately not empty (`:460` asserts that it is not). The foreign breach is
  committed, not scratch — it is what keeps the pin load-bearing, and removing
  only the pin is the negative control, which was run and came back red.

#### Dropped — verified cured

Kept here rather than deleted, so "was never true" stays distinguishable from
"was fixed" (charter §1b.5).

- `workflow_crash_resume_e2e_test.exs:249` — **never a load-sensitive test.**
  It was a KI-42 site: the module carried its own private `cleanup/3` whose
  `File.rm_rf` named `<state_root>/<class>`, a path that cannot exist, so each
  of its seven call sites leaked one class tree per run and a later run whose
  `unique/1` id collided read the leftovers. `assertions-that-cannot-fail`
  deleted that private copy; the module now imports `TierE2EHarness.cleanup/3`.
- `effectively_once_e2e_test.exs:35` (filed `:34`) — cured by
  `fix-first-input-drop-on-bind` (`e16260a7`) in both halves: the module lost
  its `await_stream_bound` call, and its `on_exit` rides
  `TierE2EHarness.cleanup/3`, whose state path the same commit corrected. The
  deleted settle only ever covered Phase 0; the SIGKILL-and-rebind face it
  actually failed on is now gated in the product (`Lifecycle.complete_init/4`).

#### Recorded, not fixed — and why each has no designable red-first

- **`test/flezha_test.exs:598`** — *"runner disconnect preserve"* (the `@tag :runner_disconnect_preserve` is `:597`,
  so `mix test …:597` selects the *previous* test under ExUnit's at-or-before
  rule; this is the coordinate the entry filed as `:591`). **Demoted to what it
  is: one sighting, no mechanism.** 1 of 6 full runs, 0 of 10 alone, 0 of 3
  instrumented, and not seen since. Candidate mechanisms exist — a fixed
  `@temp_namespace` and fixed `operative_1..15` shared with three sibling
  tests, `reset_recovery_states/0` on a process-global cache, the singleton
  convergence `Loop` on a 1 s tick while `:663` also drives `Loop.tick_now/0` — and
  **none of them is *the* mechanism.** A fix with no mechanism is a guess.
- **MOVED 2026-09-01 by `refactor-7-tests` segment C**: this test now lives at
  **`test/flezha_handover_test.exs:81`** in `FlezhaTest.Handover`. A **fourth sighting**
  was observed there in a full `mix test` (`assert m.migrated >= 1` / `left: 0`), green
  alone (8 passed) and green in the `--include multi_node` superset (1932 passed) — the
  recorded load-sensitive signature exactly. The coordinates below are the pre-move ones
  and are kept because the evidence was gathered against them.
- **SIXTH SIGHTING 2026-09-01**, during `make-entry-idle-claims-honest`'s task-4
  checkpoint: `mix test --include sdk_smoke` gave **1982/1983**, the failure being this
  test with the same `assert m.migrated >= 1` / `left: 0`, green alone immediately after
  (8 passed). The run followed a Maven package and four e2e arms — the recorded load
  exposure exactly.
- **A NEW CONFOUND EXISTS FROM THAT DATE, and it is recorded rather than waved through.**
  `make-entry-idle-claims-honest` arms `on_entry_idle` **unconditionally** at
  `test/apps/rust_runner/src/main.rs:42` (and the Java twin in `Main.main`), with a 1 s
  threshold whose tick clamps to **250 ms**. So from that change on, **every reference
  runner process the suite launches carries a watcher thread waking 4x a second**, and
  `NodeTestHelper.start_runner/2` — which this test calls four times — launches a real
  reference runner. That is genuinely new load in a load-sensitive test.
  **Ruled unrelated, on three grounds**: this entry's recorded mechanism is controller-side
  ("the drain finding nothing to drain"), five sightings predate the thread entirely, and
  both new e2e arms that DO carry it passed. **But if a seventh sighting arrives, this is
  the first thing to eliminate** — re-run with the arming commented out before looking
  anywhere else. Noting the asymmetry that makes it worth remembering: defining an
  operative is passive until used, whereas arming a watcher spawns a live thread at once.
- **`test/flezha_test.exs:389`** — *"graceful shutdown drains an operative to a
  peer through live handover"*. Mechanism established and **the fix is not
  test-side.** `:408` asserts `ow_state.arrived_via_hotswap?`, which is
  `serialized_state != <<>>` (`handover.ex:99`), fed by a control-channel round
  trip (`handover.ex:184`) whose limit is `ControlUnit`'s
  `@default_reply_timeout_ms 5_000`. A peer slower than that falls back to
  `state.snapshot`, which this test never creates, and the failure reads
  verbatim "expected live HotSwap handover, not a WAL cold restore". The 5 s
  limit is unrelated to the drain's own 30 s, so widening the drain does
  nothing. The only test-side "fix" is to accept the cold restore the test
  exists to refuse; the only production-side one is widening a production
  timeout to make a test pass. **Its red-first cannot be designed without
  forcing a slow peer, which is load, not a window.**
  **Second sighting, 2026-08-31, and it is a different assertion in the same
  test**: under self-inflicted load (three full suites back to back, 88.7 s and
  88.4 s against 57 s) it failed **2 of 3** runs at `:403` on
  `assert m.migrated >= 1` with `left: 0` — the drain finding nothing to drain,
  which is `:419`'s mechanism, not the handover timeout described above. So the
  two `flezha_test.exs` sites share one exposure and the module has **two**
  independent ways to go red under load; a red here names the assertion before
  it names the mechanism.
  **Third sighting, 2026-08-31**, same assertion and same `left: 0`, in a single
  ordinary `mix test` (72.3 s, 1863 of 1864, nothing else running) and green
  `1 passed` alone immediately after — so the exposure is not confined to
  self-inflicted load.
  **Fourth sighting, 2026-08-31** (`refactor-4-java-sdk` segment A, arm-A
  build): same assertion, same `left: 0`, in the `mix test` run that followed a
  `mvn -P coverage,native-verify clean verify` — **74.5 s** — and the same suite
  re-run on a quiet box minutes later was **1872 passed, exit 0, 59.9 s**. The
  count did not move (1871 *of 1872*), so it is this entry, not a regression.
  **Fifth sighting, 2026-08-31** (`refactor-4-java-sdk` segment C), and it
  repeats the fourth exactly: same assertion, same `left: 0`, in the `mix test`
  that followed a `mvn -P coverage,native-verify clean verify`, a native
  rebuild, an `install`, a `package` and a full `--include sdk_smoke` suite —
  60.6 s, 1871 *of* 1872, 121 excluded, uncaptured-log gate **0**. A maven build
  immediately before the suite was the recorded exposure — four of the five
  sightings then followed one.
- **THE EXPOSURE IS WIDER THAN MAVEN (corrected 2026-08-31, sightings 6 and 7).**
  Area 5e-b hit it twice **having run no maven at all**; both of its sightings
  overlapped a *second lane's concurrent ExUnit suite* on the same box. So the
  common factor is **a box under a second heavy workload**, not maven specifically —
  and that is the §11c parallel-lane condition, which did not exist when this entry
  was written. A future reproducer should force **concurrent load**, of which a
  maven build is one instance.
- **And 5e-b refuted the obvious suspect rather than assuming**: its new test file
  mutates `:flezha` application env, so it ran an A/B — the slice **without** the file
  passed 208, **with** it passed 214 three times out of three, the test alone passed,
  its own file passed 14, and the full suite on a quiet box passed **1922**. The file
  is not the cause.
- **`test/flezha_test.exs:419`** — *"a deferring operative is force-migrated at
  the drain deadline without wedging"*, the third site in this module.
  `assert_receive :deferred, 1_000` times out; the mailbox holds the competing
  `[:flezha, :shutdown, :drain_progress]` with `migrated: 0, forced: 0,
  elapsed_ms: 5, remaining_hubs: 0`, so the drain found nothing to drain and
  the `migration_deferred` hook never had an operative to fire on — a
  start/registration race ahead of the drain, not a drain defect. **It failed
  twice in a row, which looked deterministic and was not**: interleaved A/B,
  alternating arms, went red, red, then 1836 green four times running, and 3 of
  3 green alone. `:472` carries the identical `arrived_via_hotswap?` exposure
  as `:389`.
  **Third sighting, 2026-08-31**, in the `FLEZHA_E2E_RUNTIME=java mix test
  --include sdk_smoke` run of the same session (100.8 s), mailbox
  `migrated: 0, forced: 0, elapsed_ms: 4, remaining_hubs: 0` — the recorded
  shape to the field. The two sites went red in **consecutive** suite runs on a
  box warm from a maven build, one each, which is the shared exposure this entry
  already names rather than two defects.
  **Fourth sighting, 2026-08-31** (`refactor-4-java-sdk` segment C): the same
  `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` run, 69.5 s, mailbox
  `migrated: 0, forced: 0, elapsed_ms: 4, remaining_hubs: 0` — the recorded
  shape to the field again, and **1894 of 1895 with 98 excluded**, the same
  total the arm-A and arm-B builds of this area measured. This segment changes
  only `sdk/java/flezha-sdk-processor`, which runs at compile time and is in no
  running system, so the count is what makes it this entry and not a regression.
  **Fifth sighting, 2026-08-31** (`refactor-4-java-sdk` segment D): this one is
  the plain `mix test`, not the java smoke — **1871 of 1872 passed, 121
  excluded**, mailbox `migrated: 0, forced: 0, elapsed_ms: 4,
  remaining_hubs: 0`, the recorded shape to the field a third time. The suite
  ran on a box warm from `mvn verify` plus the arm's `install` and `package`,
  which makes it **five of six sightings across the two sites following a maven
  build immediately before the suite**. `mix test test/flezha_test.exs:419`
  alone: **1 passed** in 4.7 s. The total did not move (1872), and the
  `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` run of the same
  segment was **1895 of 1895**, so the java arm is not what reddened it.
- **`test/system/sdk_smoke_e2e_test.exs:56`** — *"handshake + AssignRunnerId
  succeeds"* (the `@tag scenario: "handshake"` is `:55`, the coordinate filed).
  **Its evidence is a `java` runtime arm, and it reads as a boot budget rather
  than a test defect.** One red in three consecutive
  `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` runs against a
  `java_runner.jar` whose md5 never changed, so the artifact is not the
  variable. The red run took **133.9 s against 68.3 s and 69.3 s** and is the
  only one of the three carrying `[fleet_manager] proceeding past recovery
  timeout`; the failure itself is `command_assign_runner_id` timing out at its
  5 s budget, which then took the 20 s `{:runner_connect, …}` call and the
  `{Flezha.Transport.Server, :uds}` child down with it. The module alone: 23
  passed. **Not re-run here** — this row builds no `java_runner.jar`.

- **`test/system/schema_evolution_e2e_test.exs:147` and `:69`** — two
  *different* tests in the same module, each red once in two consecutive runs,
  seen 2026-08-31 while verifying `close-input-log-syncer-blindness`.
  `:147` (*"v1 code meeting v2 state is refused, not decoded"*) failed a full
  `mix test --include system` on `assert_receive {:refused, …}, 30_000` with an
  empty mailbox; the immediately following run of the module alone instead
  failed `:69` (*"state written at v1 comes back through the migration"*) on
  `wait_until(fn -> total(…) == 12 end)`. **Not this change**: an interleaved
  A/B of the module (HEAD's `input_log.ex` vs the fixed one, 3 rounds each)
  was **6 of 6 green on both arms**, and the new production path never fired
  once in the whole system run — `grep -c input_log_syncer_died` over the log
  is 0, so the only behaviour this change adds was never reached. The second
  full `--include system` on the fixed arm was **1953 passed, exit 0**. No
  `sun_path` line precedes either failure, so KI-57 does not explain them.
  `STATE.md:1427` already records `:65` in this module going red under the
  python arm behind a `sun_path` overflow — a different mechanism.

#### Method note for the next A/B here

A HEAD worktree cannot run this suite beside the main tree without `TMPDIR`
isolation: `config/test.exs:148` keys the socket directory on
`System.tmp_dir!()` and `node()` alone, so two checkouts land in the same
`/tmp/flezha_sockets/<node>` and the second refuses to boot — `is another
controller's socket directory … answered a connect`, out of
`Flezha.Socket.Support.refuse_live_peer!/1`, taking `Flezha.BootLoader` and the
whole run down before the first test. Run the baseline arm under
`TMPDIR=/tmp/<short>`; it must be short, because the resolved UDS path still
has to fit `SUN_LEN`'s 108 bytes.

#### What could NOT be established, and still cannot

Which assertion actually failed in any of the five original sightings. No
captured failure output survives anywhere in the repo — `docs/refactor/STATE.md`
and the archived `refactor-0-tests` design record only `file:line` and a count,
and for two of them that line is the *test* line, so it names a whole scenario
rather than an assertion. The two mechanisms now fixed were established by
reading the code, not by reproducing the original red; neither is *proven* to be
the one that fired, and both are real defects that would each produce a red
under load.

### KI-69 · A runner connect racing a hub teardown kills the whole UDS transport server

- **Severity**: P2 · found by `green-the-test-hygiene-tail` segment C's batch-7
  verification, in one of three full `mix test` runs. **Recorded, verdict owed.**
  It is a `lib/` defect and row 4 changes nothing in `lib/` by design.
- **Where**: `lib/flezha/service/hub_manager.ex:504` (`register_handler/2` →
  `ProcessHub.register_hook_handlers/3`), reached from
  `Flezha.Runner.FleetManager`'s `{:runner_connect, …}` handler, called by
  `FleetManager.connect_handler/4` (`fleet_manager.ex:64`) from
  `Flezha.Transport.Server.handle_new_runner/4` (`transport/server.ex:472`).
- **What happens**: a runner's `connect_control_channel` frame lands while its
  namespace's hub is going down. `register_hook_handlers/3` is a plain
  `GenServer.call(:default_hub, …, 5000)` with nothing between it and a hub
  that is terminating, so it exits `:shutdown`; that exit propagates out of
  `FleetManager`'s `handle_call` and out of `connect_handler/4`, and it takes
  `Flezha.Transport.Server.Uds` — the **acceptor for every runner on this
  node** — down with it, logging an uncaptured `[error]`:

  ```
  05:01:02.861 [warning] [shutdown] drain ns=default force-migrated 1 deferred operative(s)
  05:01:02.891 [error] GenServer Flezha.Transport.Server.Uds terminating
  ** (stop) exited in: GenServer.call(Flezha.Runner.FleetManager,
       {:runner_connect, #Reference<0.1054811107.79298560.47071>, "default", […], …}, 20000)
      ** (EXIT) exited in: GenServer.call(:default_hub,
           {:register_hook_handlers, :child_registered_hook,
            [%ProcessHub.Service.HookManager{id: "default_28669168419_hub_pid_insertion",
              m: Flezha.Service.HubManager, f: :hook_insert_pid,
              a: ["default_28669168419", :_], p: 0}]}, 5000)
          ** (EXIT) shutdown
      (elixir 1.20.2) lib/gen_server.ex:1183: GenServer.call/3
      (flezha 0.1.0) lib/flezha/runner/fleet_manager.ex:64: …connect_handler/4
      (flezha 0.1.0) lib/flezha/transport/server.ex:472: …handle_new_runner/4
  ```

  **30 ms after the drain line**, so the window is the teardown that drain
  starts.
- **The existing guard does not cover it.** `connect_handler/4` opens with
  `check_not_shutting_down/1` (`fleet_manager.ex:79`), which reads
  `ShutdownCoordinator.shutting_down?/0` — a **node-wide** flag. The hub here is
  one namespace's, torn down by a drain drill while the node itself is not
  shutting down, so the guard passes and the call goes into a dying hub anyway.
- **Reproduction — ONE sighting, NOT reproduced, and that is stated plainly.**
  Seen in `mix test --seed 303` (exit 0, `1863 passed`, one gate hit). Then, in a
  30-minute timebox: `mix test --seed 303` × 3 → **0 hits**, and
  `mix test test/flezha_test.exs --seed 303` × 5 → **0 hits**. Two of the three
  full re-runs failed instead on `flezha_test.exs:389` with `m.migrated == 0`,
  which is [[KI-18]]'s recorded family under load (88.7 s and 88.4 s against
  57 s), not this. So: a complete stack trace and a mechanism read from it, and
  **no reproduction**. Charter §1b calls that a recorded suspicion with a named
  mechanism, and the mechanism here is the stack, not an inference.
- **The owning test is not pinned.** The drain line is `ns=default` and the
  reroute lines around it name `test_temp_namespace_958502630122`, so it is the
  `test/flezha_test.exs` shutdown-drill family; naming the test needs a `--trace`
  run this segment did not spend.
- **This is the same population [[KI-21]] keeps re-teaching**: the set of
  uncaptured-`[error]` sources is whatever the last N runs sampled. Three quiet
  runs preceded this one.
- **A fix must** decide whether a hub that is terminating is a *refusal* for the
  connecting runner or a *retry*, and must not let either answer travel as an
  exit through `Transport.Server`. One connect arriving at the wrong instant
  taking the node's whole UDS acceptor down is the part that is wrong regardless
  of which answer is chosen. The node-wide `shutting_down?/0` guard is the wrong
  granularity for a per-namespace hub and cannot be widened into one — the hub's
  own liveness is what the caller needs to know.

- **Supervisor verdict 2026-08-31 — RECORDED AS A SUSPICION, not fixed, and queued
  with [[KI-68]] for a production-defect row.** Charter §1b: a defect that will not
  reproduce inside the 30-minute timebox is recorded with what was tried and does
  **not** halt the area. What was tried is here — 3 full re-runs at the same seed
  plus 5 module runs, 0 hits against 1 sighting.
- **Confidence and severity point opposite ways, and the entry must keep them
  apart.** The *mechanism* is strong: it was read off a real stack, not inferred —
  `register_handler/2`'s plain `GenServer.call` to a terminating hub exits
  `:shutdown`, and that exit travels out through `connect_handler/4` into
  `Transport.Server.Uds`. The *rate* is one sighting in an unmeasured population.
  So: a severe consequence — one badly-timed connect takes down the acceptor for
  **every** runner on the node — held at P2 because nobody has yet made it happen
  twice. Do not let a future reader round that to either "rare, ignore it" or
  "P1, drop everything".
- **Why not row 4**: it is a `lib/` defect and row 4 changes nothing in `lib/` by
  design. Same disposition as KI-68, and the two should be surveyed together — both
  are teardown-ordering defects on the runner/controller seam.


### KI-68 · An opted-out workflow's teardown burns the whole 60 s drain bound after a crash-resume

- **Severity**: P2 · found by `green-the-test-hygiene-tail` segment B, running
  the two system modules batch 5 touched. **Pre-existing, not this change** —
  proved below. **Recorded, verdict owed.**
- **Where**: `lib/flezha/runner/control_channel.ex:493` (`@drain_owed_ms 60_000`)
  and its caller at `:467`, against
  `test/system/workflow_crash_resume_e2e_test.exs:1173`, whose
  `wait_until/1` budget is **15 s**.
- **What happens**: `test "opted-out workflow writes no journal and re-runs from
  the top"` (`:296`) fails with `completed operative was never deregistered`.

  ```
  $ mix test test/system/workflow_crash_resume_e2e_test.exs:296 --include system --seed 13
  1) test opted-out class (reference/uds) opted-out workflow writes no journal and re-runs from the top
     test/system/workflow_crash_resume_e2e_test.exs:296
     completed operative was never deregistered
     stacktrace: test/system/workflow_crash_resume_e2e_test.exs:1173
  Result: 0/1 passed
  ```

  **Deterministic**: 1 of 1 in a whole-module run (43/44) and 3 of 3 alone, at
  three seeds.
- **The operative finished; only the teardown is late.** A probe at the moment
  the assertion gives up:

  ```
  [PROBE] optout_commit_block_1574 still registered pid=#PID<0.480.0> alive=true
          status={:ok, %{status: "ok", finished_at: 1788143389, …}}
  ```

  So `Completion.write/2` has already run and the terminal status is `ok`; the
  `Overwatch` is simply still up.
- **Mechanism, measured to the millisecond.** `ControlChannel` writes the
  completion and then tears down in a `Task`: `drain_owed_outputs/3` first, then
  `OperativeManager.stop_operative/2`. The drain waits for the Overwatch to have
  run the `direct_outputs` the completion counted, capped by
  `@drain_owed_ms 60_000`. On a crash-resume the resumed run's outputs are
  best-effort — the test's own comment says so — so the count is never reached
  and the bound is what ends the wait. Raising only the test's budget to 90 s:

  ```
  [PROBE] deregistered=true after 59994 ms
  Result: 1 passed
  ```

  59 994 ms of a 60 000 ms bound. Nothing is wedged; the teardown is 4× the
  budget the test allows it.
- **Not this change.** The same test with `workflow_crash_resume_e2e_test.exs`
  restored to its HEAD content (`git show HEAD:… > …`, then restored) fails
  identically at `:295`, and the id this scenario mints is byte-identical before
  and after batch 5 (`optout_commit_block` is 19 bytes, so neither the deleted
  `String.slice(…, 0, 24)` nor the new bound touches it). The
  `unique_id_path/1` shape was ruled out separately: reverting only it to the
  old flat path leaves the test red.
- **A fix must** decide which side is wrong, and the two answers differ in kind.
  Either a completed workflow whose outputs were dropped on a rebind should not
  hold its Overwatch for a full minute — in which case the drain needs a
  cheaper terminal condition than "the counted outputs arrived" — or the
  assertion is simply under-budgeted against a production bound it never names,
  in which case it must wait past `@drain_owed_ms` and say why. Raising the test
  budget without deciding that is the shape this file exists to refuse.

---
- **Supervisor verdict 2026-08-31 — REAL and PRE-EXISTING; filed, not fixed here,
  and queued as its own Phase 1 row.** `@drain_owed_ms 60_000`
  (`lib/flezha/runner/control_channel.ex:492`) was introduced by `a9787a0f`, the
  workflow perf campaign — confirmed with `git log -S`. It is not this row's doing.
- **Why it does not pause row 4**: charter §1b pauses an area for a defect *in the
  code that area is working on*. Row 4 changes **nothing** in `lib/` by design, and
  this is a production drain cap. Folding a 60-second behaviour decision into a
  test-hygiene row would be exactly the scope drift the programme exists to remove.
- **Why it is worth its own change**: a completed workflow's teardown burning the
  full 60 s waiting for direct outputs a resumed run will never deliver is a real
  cost on every crash-resume, and the reproduction is **deterministic** (1/1 in
  module, 3/3 alone) with the mechanism named to the millisecond — raising only the
  test budget to 90 s yields `deregistered=true after 59994 ms`. That is a better
  starting point than most entries in this file ever had.

### KI-109 · `FsyncBatcher`'s dirty set is node-global and seeded from every live writer; one test compares it with `==`

- **Severity**: P2 · a merge-gating suite that goes red once in two full runs on a
  test that passes alone, in a module nobody touched.
- **Where**: `test/flezha/state/fsync_batcher_test.exs:99` —
  `assert MapSet.new([a, b]) == FsyncBatcher.dirty_set()` — against
  `lib/flezha/state/fsync_batcher.ex:93`, whose `init/1` seeds
  `dirty_set: MapSet.new(WriterRegistry.registered_ids())`, i.e. **every writer
  registered in the node**, not this module's two.
- **Evidence**, a full `mix test` at seed 192799 on an otherwise green tree
  (the only working-tree change was a whitespace re-wrap in
  `lib/flezha/service/cli_manager.ex`):

  ```
  1) test mark_dirty + tick tick coalesces dirty set (Flezha.State.FsyncBatcherTest)
     test/flezha/state/fsync_batcher_test.exs:89
     code:  assert MapSet.new([a, b]) == FsyncBatcher.dirty_set()
     left:  MapSet.new([{"TestOperative", "TestProcedure", "a"},
                        {"TestOperative", "TestProcedure", "b"}])
     right: MapSet.new([{"cursors-quiet-24067", :input_log},
                        {"il_59202", :input_log},
                        {"TestOperative", "TestProcedure", "a"},
                        {"TestOperative", "TestProcedure", "b"}])

  Result: 1922/1923 passed, 121 excluded
  ```

  Both foreign members are **another module's**: `il_59202` is minted at
  `test/flezha/state/input_log_test.exs:25` and `cursors-quiet-24067` at
  `:442`, and `InputLog` registers as `{operative_class, :input_log}`
  (`lib/flezha/state/input_log.ex:624` marks that key dirty on every append).
- **Rate, and what it is not**: 1 of 2 full runs. The re-run at the **identical**
  `--seed 192799` passed 1923/1923, so this is not seed-deterministic — module
  order alone does not decide it. It does not reproduce narrowly either:
  `fsync_batcher_test.exs` alone is 5/5, `test/flezha/state/` at that seed is
  318/318, and the two files paired are 38/38 at seeds 192799, 1, 2 and 3.
- **Mechanism**: `WriterRegistry` is a `keys: :unique` `Elixir.Registry`, so a
  registered id implies a **live** process — the foreign members are leaked
  `InputLog` processes still alive when this module's `setup` calls
  `start_supervised(Supervisor)` and a fresh `FsyncBatcher` seeds itself from
  the whole node. The drain would clear them, but the tick interval is 5 ms
  (`@default_interval_ms`) and this module runs five tests in 0.08 s, so the
  assertion lands inside the first tick. Both halves are needed: a leak in
  `input_log_test`, and an equality assertion on a set the leaker can populate.
- **A fix must** stop asserting node-global state — assert that the two ids are
  **members** of the dirty set and that a flush removes them, or give the module
  its own `FsyncBatcher` under its own `WriterRegistry` name rather than the
  application's — and, separately, find why `input_log_test` leaves registered
  `InputLog` processes behind its module, because the leak is the half that will
  surface somewhere else next. A `refute` on the foreign keys is not a fix: it
  pins the leak instead of removing it.
- **Not fixed on the spot**: found by row 3.5's final full-suite run, which is a
  two-line formatter re-wrap in `lib/flezha/service/cli_manager.ex`. It touches
  no `lib/flezha/state` and no test; charter §1b files it here.

## P3 — warts, wording, and measurement notes

- **Evidence (R2b close, 2026-09-02)**: the same red in two smoke-tier arms of a change that touches nothing in `lib/flezha/state` — `fsync_batcher_test.exs:94` under `FLEZHA_E2E_RUNTIME=ruby` read `right: MapSet.new([])` (a concurrent flush emptied the singleton's set) and under `php` read the set plus a concurrent test's writer `{"rw_19170", "proc", "k1"}`; green in the full suite and in the node, python and elixir arms of the same tree. The two `right:` values name the mechanism the entry records: strict equality against a node-global singleton.

### KI-129 · On an erased host with more than one loop, a live Entity read can answer state whose changes are not yet journaled, and its seq stamp cannot say so

- **Severity**: P3 · a wart in what the live read promises, not a data-loss path: the WAL is
  what restores, and a tier-off class has no input log by design. It bites a TEST that uses
  the live read as its settle signal before a stop, a writer lookup or a snapshot. · Found
  2026-09-02 by the R5 S3 worker when `schema_evolution_e2e_test.exs` went red 4 of 6 under
  ruby the first day it ran there (the supervisor reproduced it 2 of 2).
- **Where**: `sdk/ruby/lib/flezha/dispatch.rb:158-160` — `invoke` writes the state back in
  its `ensure` (`registered.set_state`); `run_work` then builds the effects and only when it
  returns does the executor call `core.complete` with the change bodies, which the core's
  flusher frames and ships. A `live_state_read` served by another worker thread in that
  window answers a total whose changes are not yet framed. Python's thread pool, ruby's
  threads, node with `FLEZHA_NODE_WORKERS > 0` and php under Swoole all have the window;
  node's single loop closes it; the Rust typed path journals in `apply`.
- **Evidence**: `FLEZHA_E2E_RUNTIME=ruby mix test --include system test/system/schema_evolution_e2e_test.exs`
  → `1/2 passed` (twice, supervisor), failing at `:147`/`:170`; worker 4 red of 6 with the red
  site moving between `:103` (no WalWriter yet), `:139` (`total` never reaches 12 after a
  stop/restart) and `:170` (no `:restore_refused`) — every site a stop or a lookup taken on
  the strength of a live read. A probe copy that awaits the persisted seq before each of the
  three sites (`CreditTestHelper.wait_for_entity/3` + `await_seq(wait_for_writer(...), n)`,
  the helpers `credit_flow` already uses) is 3 of 3 green under ruby.
- **The wart proper**: the read's metadata `seq` is the handout `change_seq`, so a reader
  cannot tell that the state it holds is ahead of the journal; `stop_operative(preserve_state:
  true)` keeps what has landed. Nothing in the spec promises more, and the timers test already
  says so of `send_io`.
- **Verdict (supervisor, 2026-09-02)**: the test race is fixed under §9 in the S3 ruby
  commit (the three waits, red first by rate); the read semantics are recorded, not changed —
  a test settles on `await_seq`, never on a live read, before anything that depends on the
  journal.


### KI-132 · The PHP Swoole executor is an axis of the run that nothing runs end to end

- **Severity**: P3 · a test gap, the same class as a pin nothing reads: `test/apps/php_runner/conformance.json` declares `FLEZHA_PHP_EXECUTOR` as an axis of the run (`sync` or `swoole`), and no workflow, script or recorded arm sets it to `swoole`. · Found by the R2b survey, 2026-09-02.
- **Where**: `.github/workflows/php-sdk.yml:113-138` builds the ZTS + Swoole image and prints a version line; nothing runs a scenario in it. Every recorded php arm (R5's `2017/2019`, the KI-130 drill on seven runtimes) is the default `sync`.
- **Evidence**:

      $ grep -rn FLEZHA_PHP_EXECUTOR .github/workflows/        → exit 1
      $ grep -rn -i swoole .github/workflows/ | grep -v Dockerfile
      .github/workflows/php-sdk.yml:113: - name: Cache the ZTS PHP + Swoole image
      .github/workflows/php-sdk.yml:136:   assert(class_exists("Swoole\\Thread"), "no Swoole\\Thread");
      .github/workflows/php-sdk.yml:138:   printf("php %s zts, swoole %s, %d cpus\n", ...);
      $ grep -c -i "executor=swoole\|FLEZHA_PHP_EXECUTOR=swoole" docs/refactor/STATE.md docs/dev_notes/php-sdk.md → 0 / 0

- **A fix must** run at least the smoke tier under `FLEZHA_PHP_EXECUTOR=swoole` somewhere that reads the result — the nightly, or the php workflow inside the `flezha-php-zts` image with the controller toolchain — or the manifest stops calling the executor an axis of the run.
- **Verdict (supervisor, 2026-09-02)**: real, P3, owed; `carry-entry-idle-in-every-host` (R2b) runs its own scenario under swoole in the zts image if the box can, or reports by name — that run is not a substitute for the gap.


### KI-135 · The Elixir SDK declares `max_entities/0` and never enforces it

- **Severity**: P3 · a documented cap that binds nothing: `Procedure.max_entities/0` (default 100 000, `procedure.ex:8,24,30`) is read by `Procedure.max_entities/1` (`:58-62`), which has no caller; `Operative.ensure_entity/3` (`operative.ex:209-218`) mints a dynamic Entity for every new key. The reference caps best-effort (`flz_core/src/dynamic.rs:72-74`, `get_or_create`). · Found by the R6 survey, 2026-09-02.
- **Evidence** (worker, a scratch script declaring `max_entities/0 = 2` on a dynamic node and routing three keys):

      $ MIX_ENV=test FLEZHA_CONTROLLER_ENABLED=false mix run --no-start $SCRATCH/max_entities_probe.exs
      max_entities declared: 2  live entities: 3  keys: ["k1", "k2", "k3"]

- **Mechanism**: the cap is read nowhere on the routing path.
- **A fix must** read the cap in `ensure_entity/3` and refuse or evict the way the reference does, with a unit test that declares a cap of 2 and routes three keys — red today. `dynamic_entity`'s neighbourhood: R6's second change (`carry-keyed-dynamic-and-schema-in-elixir`) owns it by name.
- **Verdict (supervisor, 2026-09-02)**: real, P3, owed to R6's second change; does not pause the first.


### KI-134 · An `elixir_runner` outlived the harness closing its port in the elixir smoke tier — SUSPICION

- **Severity**: P3 · seen once (R2b's batch 8, 2026-09-02): after the `--include sdk_smoke` arm under `FLEZHA_E2E_RUNTIME=elixir`, a `beam.smp … mix run --no-halt … smoke_12324` was still alive 1 m 22 s later, killed by pid (SIGTERM, then SIGKILL). The test app's stdin watcher is what should have halted it when the port closed. Not reproduced; rate unknown (one arm observed).
- **Where**: `test/apps/elixir_runner/` — the entry point's end-of-file handling on standard input, and whatever `mix run --no-halt` does with a closed stdin.
- **A fix must** first reproduce (the elixir smoke tier, then `ps` for a surviving `mix run --no-halt` 30 s after) and read what the app does on EOF; the other six apps exit on it.
- **Verdict (supervisor, 2026-09-02)**: a suspicion, recorded; boxed by whoever next runs the elixir smoke tier.


- **Recurrence (R6 batch 1, 2026-09-02)**: after a `--include system` run of the five `state_persistence` modules under `FLEZHA_E2E_RUNTIME=elixir`, `ps` showed `… mix run --no-halt --no-deps-check -- smoke_4354` (etime 0:36) after `ensure_port_dead` had returned; killed by pid. Two of two elixir arms this session have left one behind, so the rate is not "once" — the box is owed the day the elixir smoke tier next runs with time to read the app's stdin handling.

- **Third sighting (R6 checkpoint 3, 2026-09-02)**: the supervisor's COMBINED run of the five `state_persistence` modules under `FLEZHA_E2E_RUNTIME=elixir` (`--include system --include sdk_smoke`, one BEAM) left one `mix run --no-halt` behind; the worker's five per-module runs of the same files left none. So the leak follows the multi-module run, not any one scenario — the app's exit on a port close is racing the next module's spawn, or the last module's teardown. Killed by pid.

- **Fourth sighting and a mechanism candidate (R6 close, 2026-09-02)**: the elixir smoke tier (`2030 passed`) left `mix run --no-halt … -- smoke_27429` alive 16 s after; a SIGTERM by pid took more than 2 s to end it. The R6 worker's reading: `RunnerServer.reconnect/2` is a synchronous retry loop with `Process.sleep` inside `handle_info`, so an ORPHANED runner (its controller gone) cannot see the supervisor's exit signal until the `shutdown:` budget expires and the kill lands — a budget `carry-state-persistence-in-elixir` raised from 5 s to 10 s for the graceful ship, so the leak past `ensure_port_dead` now lasts longer. Not proved by experiment; the separating one is a reconnect loop stubbed to yield, then the same tier.

### KI-133 · A PHP fixture's `static` is a per-interpreter value under Swoole, so the poison probe's antidote can land on a different worker than its poison — SUSPICION

- **Severity**: P3 · `test/apps/php_runner/fixtures.php:403` `PoisonProbe::$antidoteTaken` is `private static bool`; each `Swoole\Thread` is a fresh interpreter (`docs/dev_notes/php-sdk.md`), so the static is one value per worker and the antidote and the poison can land on different workers once the pool grows. · Found by the R2b survey, 2026-09-02; not reproduced — the arm that would show it is KI-132's.
- **A fix must** move the probe's memory to something one per process (a pid-keyed file, as R2b's `entry_idle` twin does) or prove the fixture is only ever driven with one worker.
- **Verdict (supervisor, 2026-09-02)**: a suspicion, recorded; reproduced or refuted the day KI-132's arm runs.


### KI-124 · Python's `persistence_off_counter` declares the reference's procedures under `stateful_counter`'s names

- **Severity**: P3 · a fixture-fidelity divergence in the porting template; nothing in
  `test/` or `lib/` reads either spelling, and a persistence-off class writes no WAL that
  would carry the name to disk. · Found by the R5 S2 worker, 2026-09-01, porting the six
  `state_persistence` fixtures from Python and checking each against the reference.
- **Where**: `test/apps/python_runner/fixtures_state.py:116-118` —
  `.into(counter_entry, "counter_entry_procedure").named("ticks").into(counter, "counter_procedure")`.
  The reference names them `persistence_off_entry_procedure` /
  `persistence_off_counter_procedure` (`test/apps/rust_runner/src/state.rs:533-534,557-558`);
  Java agrees (`PersistenceOffStreamOperative.java:36,44`). Rust and Java write the two
  classes twice; Python reuses `stateful_counter`'s classes and keeps their names.
- **Evidence**: `grep -rn 'persistence_off_.*_procedure' test/ lib/` → only the Rust and Java
  fixtures. The smoke golden pins controller-read declaration keys only
  (`test/fixtures/declarations/store_input.json` is `{"keyed": "false"}`), so no test can be
  red on a node name.
- **A fix must** pass the reference's names through Python's node-name override — the same
  `into(proc, name)` the node port uses — two string edits. Not done in R5 S2 because §1b's
  bar for a fix commit is a red-first test, and no test can see a node name; a golden that
  pinned `persistence_enabled: false` would pin something worth pinning and is the honest
  vehicle if one is wanted.
- **Verdict (supervisor, 2026-09-01)**: real, not worth a change of its own; the node, ruby
  and php ports follow the reference. Owed to whichever change next touches
  `fixtures_state.py`, or to R5's close-out if nothing does.


### KI-61 · `RecordLog`'s two `@spec`s name `Elixir.Registry.entity_id/0`, a type that does not exist

- **Severity**: P3 · specs and one moduledoc line, no runtime effect.
- **Where**: `lib/flezha/state/record_log.ex:36` and `:57` —
  `@spec append(Registry.entity_id(), …)` and the `Registry.entity_id()`
  argument of the read spec — plus `:17`, whose moduledoc says
  `Registry.scan_dir/1`. The file's only alias is
  `alias Flezha.State.{Supervisor, WalWriter}` (`:21`), so the bare `Registry`
  expands to `Elixir.Registry`, the stdlib process registry.
- **Reproduction**:

  ```
  $ grep -n '^\s*alias' lib/flezha/state/record_log.ex
  21:  alias Flezha.State.{Supervisor, WalWriter}
  $ MIX_ENV=test mix run --no-start -e '…Code.Typespec.fetch_types/1…'
  Elixir.Registry types: dispatch_opts/0, listener_message/0, start_option/0,
    spec/0, body/0, guards/0, guard/0, match_pattern/0, meta_value/0,
    meta_key/0, value/0, key/0, keys/0, registry/0
  Flezha.State.Paths has entity_id/0? true
  Elixir.Registry has entity_id/0? false
  Flezha.State.Paths exports scan_dir/1? true
  ```

- **Mechanism**: Elixir resolves a remote type's module through the file's alias
  table at expansion time and never checks that the type exists — only Dialyzer
  does. `Flezha.State.RecordLog` never aliased the path module, so both specs
  have always declared an argument type `Elixir.Registry` does not export, and
  the compiler is silent about it. Pre-existing and unchanged by
  `fix-opaque-module-names`: the same three lines resolved to the same stdlib
  module before the path module was renamed to `Flezha.State.Paths`.
  The rename only makes the prose misleading as well — a reader chasing
  `Registry.scan_dir/1` now finds no such module in the tree.
- **A fix must**: add the path module to `record_log.ex`'s alias group and spell
  the two specs and the moduledoc line against it, so the declared type is the
  one the callers actually pass (`Flezha.State.Paths.entity_id/0`). It is a
  behaviour-free correction, but it changes what a spec *means*, so it is not a
  rename and does not belong in a rename commit.
- **Supervisor verdict 2026-08-31 — real, deferred to `refactor-5a-state`.**
  Reproduced independently at the checkpoint: `Code.Typespec.fetch_types(Registry)`
  returns 14 types and `entity_id` is not among them. Then swept for a family and
  there is none — every `.ex` under `lib/` that names a `Registry.<type>()` without
  aliasing a Registry module was checked, and `record_log.ex` is the only hit
  (`state/supervisor.ex:39` is `ClassRegistry.ensure_table()`, a call). So it is one
  isolated instance, not a class. **Cost of fixing it now**: two spec lines and a
  moduledoc line — but charter §1b's red-first proof needs Dialyzer, and this repo
  gates no Dialyzer run, so proving two lines costs a PLT build. **What it buys**: a
  spec that names the type its callers actually pass. Area 5a reads every spec in
  `lib/flezha/state/` and is where the proof is affordable, the same treatment
  [[KI-41]] already has.

---

### KI-52 · Java and PHP still ship all five native triples in one artefact

- **Severity**: P3 · provisional, and a distribution-shape decision.
- **Where**: `sdk/java/flezha-sdk-native` and the PHP packaging.
- **What**: Node solves this with per-triple `optionalDependencies`, Ruby
  compiles at install, Python ships maturin wheels. Java and PHP do not, so a
  consumer downloads five platforms to use one.
- **Not the same as the repository purge.** `unbundle-committed-build-artifacts`
  changed what git carries; this is what a *release* carries. Conflating them is
  what made KI-37 read as six SDKs when it was two.
- **A fix must**: pick the per-platform mechanism for Java (classifier artifacts)
  and PHP, and decide whether it blocks going public.

---

### KI-46 · OTLP attributes are stringified where the protocol has typed values

- **Note added 2026-08-30**: `AuditLog.Codec.stringify_value/1` **already
  implements** the typed-recursive shape this entry asks for, for JSON — a
  working in-tree precedent, not a design from scratch. Deliberately left OUT of
  `close-prometheus-label-coercion`: that change's control is that
  `attribute_string/1` moves byte-identically, proved only by
  `export_test.exs`'s decode-the-protobuf tests, and typing the values would
  destroy exactly that control. Follow-up id: `close-otlp-typed-attributes`.

- **Severity**: P3 · **proposal owed**.
- **Where**: `lib/flezha/observability/export/otlp_codec.ex` — `kv_attribute/2`
  and `any_value_string/1`, which emit `AnyValue.string_value` (field 1) and
  nothing else.
- **What happens**: nothing breaks. Every attribute is present and readable
  after `close-otlp-export-crash`, but a structured value crosses as an Elixir
  rendering — `%{line: 412}`, `{Mod, :fun, 2}`, `[1, 2, 3]` — where OTLP's
  `AnyValue` has `array_value` (5) and `kvlist_value` (6) for exactly these
  shapes. A collector or a Grafana query cannot index into a string.
- **Why it was not folded into `close-otlp-export-crash`**: that change had to
  stop the exporter dying on its own payload, and stringifying is the smaller
  fix. Emitting typed values changes the shape on the wire for attributes that
  already export today, which is a compatibility question with its own answer
  (design `close-otlp-export-crash/design.md`, Open Questions).
- **A fix must**: map Elixir terms onto `AnyValue`'s oneof (`bool_value`,
  `int_value`, `double_value`, `array_value`, `kvlist_value`, `bytes_value`),
  decide what a tuple, a pid and a struct become — none has an OTLP shape, so
  inspection stays their answer — and keep one decision behind all attribute
  sites, as `attribute_string/1` is now.

---

### KI-80 · `--json` and `--output json` are two dialects, each silently ignored by the other's verbs

- **Severity**: P3 · a wart, but one that fails closed into wrong output rather
  than an error.
- **Where**: `cli/src/utils.rs:196 get_output_format` reads `--output` / `-o` and
  maps everything else to `"text"`; `cli/src/audit.rs:28 want_json` and
  `cli/src/observe.rs:28 want_json` (two byte-identical copies) read `--json` /
  `-o=json`; `cli/src/controller.rs:36` and `:229` inline the same predicate a
  third and fourth time.
- **Evidence** — a `flzvm` shim that echoes the RPC expression it was handed:

  ```
  cluster status --json           -> cluster_status("", "text")     <- flag ignored
  cluster status --output json    -> cluster_status("", "json")
  audit list --json               -> audit_log_render_list("json")
  audit list --output json        -> audit_log_render_list("text")  <- flag ignored
  controller status --json        -> controller_status("json")
  controller status --output json -> controller_status("text")      <- flag ignored
  ```

- **Mechanism**: two readers, written at different times for different verbs,
  neither aware of the other. Neither refuses an unrecognised spelling — both
  default to `"text"` — so a script piping `flz cluster status --json` into `jq`
  gets a human table and a parse error, with nothing on stderr to say why.
- **Why it was not fixed on the spot**: found by the `refactor-6-cli` survey.
  Choosing which spelling wins (or accepting both everywhere) changes the
  operator surface on eleven verbs, which charter §1c makes the user's call, not
  a refactor's.
- **A fix must** decide one of three: accept both spellings everywhere (largest
  surface, no breakage), keep `--output` as canonical and make `--json` an alias,
  or refuse an unrecognised output spelling instead of silently defaulting to
  text. The third is the smallest change that stops the silent failure and is
  compatible with either of the first two later. `refactor-6-cli` puts both
  readers in one module so whichever is chosen costs one line.

## Older defects, found before the refactor programme

These predate `docs/refactor/STATE.md` and were carried only in session memory
until 2026-08-30. Dates are when each was last established, not when it started.

- **Supervisor verdict 2026-08-31 — real, and which spelling wins is the USER's, not
  mine.** `--json` and `--output json` is a CLI surface question with no technically
  correct answer; both are cheap. Escalated to *Awaiting the user's decision*. Area 6
  does **not** unify them unasked — a refactor whose contract is zero behaviour change
  is the wrong vehicle for picking a flag spelling.

### KI-34 · Java host dispatch costs 27–55 % more than Rust

- **Severity**: P2 · **proposal owed** (`java-host-dispatch-cost`).
- Measured during the cross-SDK perf-parity campaign (2026-08-22/23). The
  campaign delivered the *proof* — `mix bench.roundtrip --runtime rust|java|elixir`
  through the existing `Fleet` seam — and the gap it exposed was never closed.
- Nothing has been written; the number is the whole of what is known.
- **Triage (2026-08-30)**: **OLD AND SOFT. The 27–55 % figure is not
  reproducible today, and it was never a gate-grade number even at source.**
- **The source qualifies itself.** The measurement was
  `mix bench.roundtrip -p 2 -m 3000` through the Kafka bench stack, and the
  campaign's own note records *"Box was loaded; spread is wide (rust keyed/off
  42.0–58.1k), so treat as direction."* A 28 % spread inside one arm cannot
  resolve a 27 % between-arm difference. The honest form of the original claim is
  its other sentence: **Java is under Rust on every tier.**
- **Re-measured today on the neighbouring row** — `mix bench --shape regression
  -n 100 -m 10000`, the same `Fleet` seam, arms interleaved inside each round,
  `FLEZHA_BENCH_SKIP_BUILD=1` against artifacts built today:

  ```
  round=1 rust receive=581,754 end_to_end=251,808
  round=1 java receive=513,686 end_to_end=211,937
  round=2 rust receive=536,635 end_to_end=243,849
  round=2 java receive=482,543 end_to_end=206,290

  java vs rust, mean of two rounds:  receive -10.9 %   end-to-end -15.6 %
  ```

  Java is behind in 2/2 pairs on both windows, so the *direction* holds. The
  *magnitude* does not: 11–16 %, not 27–55 %. And the receive-window gap is
  inside the rust arm's own run-to-run spread on this box (12.1 %, five runs —
  see KI-29), so at this sample size the receive number resolves nothing; only
  the end-to-end window separates the arms.
- **What could NOT be established**: the campaign's actual rows. `mix
  bench.roundtrip` needs the Kafka stack at `bench/flink/compose.yml`, which is
  `.git/info/exclude`d and not running (`docker ps` is empty), and bringing it up
  was out of scope for triage. **The row I measured is not the row the entry
  cites** — treat the two numbers as neighbours, not as a before/after.
- **Worth fixing**: the *entry* is worth rewriting before any proposal is. As it
  stands it advertises a precise-looking range that neither its own source nor a
  re-measurement supports, and a proposal opened against "27–55 %" would be
  chasing a number. Restate it as "Java is measurably behind Rust on every tier;
  magnitude unestablished" and keep `java-host-dispatch-cost` owed on that
  basis.

### KI-36 · The controller OOM fix was never validated under real load

- **Severity**: P1 for production, P2 here.
- **Background**: the 2026-06-19 controller OOM (2 GiB, exit 137, ~30 h outage)
  was reproduced and root-caused on 2026-06-21 — controller write buffers on the
  WAL persistence path with no ceiling. `WalWriter.append_async` is a
  fire-and-forget cast while `FsyncBatcher` coordinates fsync via a blocking
  call, so when the SDK ships state changes faster than the controller can fsync
  (the Pi's SD-card fsync being the bottleneck) the FsyncBatcher and
  per-partition WalWriter mailboxes grow without bound.
- **The gap**: fix `f877ccb` has never been validated against the mock Kraken
  load that reproduced the original incident.
- **Triage (2026-08-30)**: **STILL TRUE. The gap is exactly as described, and
  nothing this session changed it.**
- **The hash resolves and is live**, which is worth stating because most
  pre-rewrite citations in this file do not: `f877ccb5` (`feat(resilience): bound
  controller WAL writer and transport send buffers`) is an ancestor of HEAD and
  sits on `dev`, `origin/dev` and `origin/main`. Its post-rewrite twin `75471751`
  carries the same subject and is **not** an ancestor — cite `f877ccb5`.
- **The load harness that would validate it exists**: `test/apps/testnet` is the
  mock Kraken v2 feed (`test/apps/testnet/README.md:27`), driven by
  `FLEZHA_TESTNET_CONFIG`. So this is a "nobody has run it" gap, not a "the tool
  is gone" gap.
- **What could NOT be established, and deliberately so**: whether the fix holds
  under that load. Reproducing the original incident means an SD-card-class fsync
  bottleneck plus a sustained state-change rate — hours of soak against real
  hardware, which is a change, not a triage step. **A green unit or e2e run
  proves nothing here**: the failure mode is unbounded mailbox growth under
  sustained overload, and every short run is inside the bound by construction.
- **Worth fixing**: the fix is already in. What is owed is the *validation*, and
  it is a soak, not a patch. Keep it filed at P1-for-production and note that it
  is blocked on hardware and time rather than on any unknown.

## Notes — true, and not work owed

### Two test-coverage gaps whose `TODO` tags were removed by `refactor-7-tests`

Batch 6 deleted six `TagTODO` credo offences. Four named no work and went with the dead
code around them. **Two described real gaps**, and removing the tag would otherwise have
erased the only record of them, so they are written down here rather than lost:

- **`test/flezha_test.exs:129`** — the websocket-plugin enable test asserts only the
  success path. Nothing exercises a failed enable at that seam.
- **`test/flezha_test.exs:268`** — nothing on the Elixir side drives an over-long control
  or stream frame. `grep -rn max_frame_bytes lib/ test/` is empty: the limit is enforced
  in `flz_transport` and tested there, so this is arguably where it belongs. Recorded so
  the next reader knows the Elixir tier's silence is deliberate rather than an oversight.

**Neither is a defect and neither is work owed.** They are coverage observations, and a
coverage gap is only worth closing when someone can say what a new test would catch that
the existing tier does not.


Each of these was triaged and found correct. None is a defect: they are
deliberate trade-offs, environment facts, or measurements. They sit here so
the open list above contains only open work.

### KI-25 · A production `usleep` nothing states

- `sdk/php/src/Executor/SwooleExecutor.php:271` carries `usleep(5_000)`. The sleep
  metric counts test paths only, so no gate sees it. Probably legitimate and
  short and finite — but nothing says so.
- **Triage (2026-08-30)**: **TRUE on both halves, and it is a note, not a
  defect.** The sleep is at `sdk/php/src/Executor/SwooleExecutor.php:271` and it
  is provably short and finite from the three lines around it: `joinAll()`
  (`:265`) opens `$deadline = microtime(true) + self::JOIN_TIMEOUT_S` (`:267`,
  `JOIN_TIMEOUT_S = 5.0` at `:39`) and the poll is
  `while ($thread->isAlive() && microtime(true) < $deadline)`. A 5 ms poll inside
  a 5 s deadline.
- **Reproduction of the metric half — and it generalises further than the entry
  says.** The exemption is the *path* classifier, not the pattern:

  ```
  $ grep -rn 'usleep' sdk/php/src/
  sdk/php/src/Executor/SwooleExecutor.php:271:                    usleep(5_000);
  $ python3 -c "import json;a=json.load(open('docs/refactor/baseline.json'))['areas']['sdk/php'];print(a['prod_loc'],a['sleeps'],a['sleeps_by_file'])"
  4134 0 {}
  ```

  `SLEEP["php"]` is `\b(?:u)?sleep\(` (`scripts/refactor_metrics.py:340`) and
  *does* match this line — but `measure_lines` increments `sleeps` only under
  `if in_test[i]` (`:378`–`:379`), and `is_test_path` (`:102`) is false for
  `sdk/php/src/…`. So the metric is blind to production sleeps **in every
  language, by construction** — it can never be the thing that finds one. That is
  the fact worth carrying forward, not this particular `usleep`.
- **Worth fixing**: no. The code is correct and the `joinAll` docblock already
  explains why the drain is bounded at all. What the entry asks for is one
  comment, and the honest resolution is to record the metric's scope (above) and
  close this.

---

### KI-29 · Benchmark rows that cannot resolve what they are asked to

Measured floors on the reference box (`moon`, i7-8650U, `powersave`, on mains):

| Row | Floor | Verdict |
|---|---|---|
| Rust typed dispatch | 0.33 % | gate |
| Rust entity stress | 0.95 % | gate |
| `throughput_effectively_once_tiers` tier_off / tier_on | 9.04 % / 6.01 % | **direction only** |
| `host_loop` (erased) | 4.13 % | guard |
| whole-system round trip | ~18 % | **direction only** |
| veneer round trip (node) | 11.89 % | paired rounds only |

A row whose floor exceeds the effect being claimed is a direction, not a gate.
Do not run one as a gate and do not report its single-run number as a verdict.

- **Triage (2026-08-30)**: **TRUE, and correctly filed as a note rather than a
  defect.** The reproduction of a measurement note is the measurement, so this
  triage adds a row measured today rather than re-deriving six floors.
- **A seventh row, measured this session — the merge-gate row.** Five runs of
  `./scripts/bench.sh -n 100 -m 10000` (the `bench-regression` workload), same
  box, same tree, same session:

  ```
  receive window : 518,872  568,312  562,280  581,754  536,635   spread 12.1 %
  end-to-end     : 239,530  250,374  245,423  251,808  243,849   spread  5.1 %
  ```

  Two things follow. **The end-to-end window is less than half as noisy as the
  receive window** on this workload (5.1 % vs 12.1 %) — the opposite of what the
  gate's choice of metric would suggest, and worth knowing before anyone treats a
  single receive-window number as a verdict. And **the gate's own tolerance band
  is 10 % (`REGRESSION_TOLERANCE: "0.9"`, `.github/workflows/bench.yml:29`) while
  the metric it gates spreads 12.1 % run-to-run here.** The gate still passes
  with margin because it compares to a floor of 480,000 and the slowest run was
  518,872 — but the band is narrower than the noise, so a *near-floor* box would
  flip on scheduling alone.
- **What could NOT be established**: none of the six recorded floors was
  re-derived. Each needs its own paired-round campaign, and the `whole-system
  round trip` and `veneer round trip` rows additionally need the Kafka bench
  stack (`bench/flink/compose.yml`), which is not up and is `.git/info/exclude`d.
- **Worth fixing**: nothing here is a defect. The one thing worth *doing* is
  recording the seventh row above beside the other six, so the gate's row has a
  published floor like every other row does.

---

### KI-30 · Three environment facts that cost hours if unknown

- `/proc/sys/kernel/perf_event_paranoid` is `4` here, which refuses **both**
  `perf record` and `perf stat`; valgrind is not installed. Instruction-level
  attribution is therefore impossible on this box — say "not attributable here"
  rather than digging.
- **An arm built in a `git worktree` is not comparable to one built in the
  tree.** A Rust binary is not byte-reproducible across build paths; a 60-second
  microbench read +3.5 % against a worktree arm and −3.5 % against a tree-built
  arm of the same source. Every decisive comparison must be between arms built
  the same way.
- The scratchpad is per session, so benchmark arms do not survive a new session
  unless they are rebuilt from a tag.
- **Triage (2026-08-30)**: **TRUE. Fact 1 reproduced verbatim; facts 2 and 3 are
  correct but were not re-measured, and one fact is missing.**
- **Fact 1 — reproduced.** `perf` *is* on `PATH` (`/usr/bin/perf`), which is what
  makes this cost hours: it exists and then refuses.

  ```
  $ cat /proc/sys/kernel/perf_event_paranoid
  4
  $ perf stat -e cycles true
  Error:
  No supported events found.
  Access to performance monitoring and observability operations is limited.
  ...
  perf_event_paranoid setting is 4:
  $ perf record -o /tmp/perf.data true
  Error:
  Failure to open event 'cpu/cycles/Pu' on PMU 'cpu' which will be removed.
  $ which valgrind
  (nothing)
  ```

  Both subcommands refused, and valgrind is absent. "Not attributable here" is
  the correct answer, exactly as written.
- **Facts 2 and 3 — not re-measured, and deliberately not.** The worktree-vs-tree
  build asymmetry needs two 60-second microbench arms built two ways to
  re-derive; re-running it would cost more than it could tell, and nothing about
  it has changed. Believe it and build both arms the same way.
- **A fourth environment fact belongs here**: **there is no local cross-compile
  toolchain.** `cargo-zigbuild` is installed but `zig` is not, so any
  `--target x86_64-pc-windows-gnu` build dies in `ring`'s build script. See
  KI-28 — a cross-compile-only warning cannot be reproduced on this box.
- **Worth fixing**: not a defect. Keep the list and add the fourth fact.

---

### KI-31 · Duplication the programme has chosen not to remove yet

- `test/apps/rust_runner/src/main.rs` duplicates the Rust bench's four-stage
  stress pipeline (four jscpd pairs) — owed to the test-apps area.
- The nine `convert_value` copies (**count corrected 2026-08-31**: nine callback DEFINITION sites, of which 2 already delegate, 3 are genuinely per-provider coercion at 18/10/2 clauses, and only **4 small folds** exist — `parse_int` and `parse_bool` reimplemented, one deferred `:recv_pid` clause. The deferral verdict stands; the count sent readers hunting a fold that is not there) across SDKs are structural twins a 40-token
  window does not detect.
- Whole-repo `duplicate_pairs` rose 183 → 235 when three test masses became
  fifteen binaries, each repeating one `use` header.
- **Triage (2026-08-30)**: **TRUE, deliberate, and NOT WORTH FIXING here — but
  the second bullet names the wrong place, and that matters to whoever picks it
  up.**
- **Bullet 1 — confirmed, both halves present.** The four-stage pipeline exists
  twice: `test/apps/rust_runner/src/main.rs` declares `stress_a`/`stress_b`/
  `stress_c`/`stress_d` (`:2392`, `:2422`, `:2452`, `:2482`) plus `entity_stress`
  (`:2505`), and `sdk/rust/benches/dispatch.rs` declares `stress_bench_a`…`_d`
  (`:187`, `:218`, `:249`, `:280`) plus `entity_stress_bench_operative` (`:304`).
  A live scan pairs them across the seam:

  ```
  $ python3 scripts/refactor_metrics.py --area test/apps --area sdk/rust --json
  sdk/rust duplicate_pairs = 210
  ... "sdk/rust/benches/correctness.rs:268-274 <-> test/apps/rust_runner/src/main.rs:163-169 (40 tokens)"
  ```

  Note `sdk/rust` reads **210** today against the baseline's 215, so the number
  moves under ordinary work and is not a fixed target.
- **Bullet 2 — CORRECTION: the nine `convert_value` copies (**count corrected 2026-08-31**: nine callback DEFINITION sites, of which 2 already delegate, 3 are genuinely per-provider coercion at 18/10/2 clauses, and only **4 small folds** exist — `parse_int` and `parse_bool` reimplemented, one deferred `:recv_pid` clause. The deferral verdict stands; the count sent readers hunting a fold that is not there) are not "across
  SDKs".** All nine are Elixir controller plugin providers under
  `lib/flezha/plugin/`:

  ```
  provider_behaviour.ex   beam_msg_forward.ex   filter.ex
  operative_forward.ex    kafka.ex              monitored_io_forward.ex
  http_poll.ex            websocket.ex          postgres.ex
  ```

  Nine files, nine definition sites, one language, one directory. That is a
  materially different (and easier) refactor than a cross-SDK one, and it belongs
  to the `lib/flezha/plugin` area — which the baseline records at 16
  `duplicate_pairs` — not to any SDK area. Anyone reading "across SDKs" would go
  looking in six languages for something that lives in one.
- **Bullet 3 — the caveat is the content, and it is right.** `duplicate_pairs` is
  a *pair* count that a file split multiplies for unchanged source, and the
  baseline has no whole-repo figure to compare against (it records per-area
  numbers only), so the 183 → 235 delta is not re-derivable from
  `docs/refactor/baseline.json`. Always name the scan mode; prefer duplicated
  tokens.
- **Worth fixing**: no — this entry exists to say the programme chose to defer
  these, and that choice still holds. Fix the "across SDKs" wording so the
  deferral is at least accurately described.

---

---

## Production (state as of June 2026, unverified since)

Recorded here because nothing else tracks them. Every one predates this
programme and none has been re-checked against the current deployment. Append
every new production freeze to `incident_log.md` at the repository root.

- **PROD FREEZE, hub-merge wedge** (2026-06-19) — `/healthz` reported healthy for
  30 hours while the data plane was wedged. Still open: BEAM backtrace capture on
  a wedge, a healthz check that sees a data-plane stall, and OOM-under-churn.
- **PROD OUTAGE, rpi4 node wedge** (2026-06-12) — single point of failure still
  open. Recovery is: power-cycle the rpi4, then `start-asset.sh`.
- **Convergence reliability audit fixes** — on `dev`, **not deployed**.
- **Recurring freeze from a self-inflicted channel kill** — fixed, **not
  deployed**.
- **`unified-convergence-loop` Phase 5** — 5.D zero-loss FAILED; 5.OW left six
  overwatch failures.
- **sol6 recurrence** — per-pod hostPath registry is a structural split-brain.

---

## Owed follow-up changes (big, not yet written)

### `sdk-declaration-refusals-in-the-core`

- **Severity**: P2 · **proposal owed**, scheduled after the Java SDK area.
- The core refuses 19 declaration rules that all four veneer SDKs *also* refuse,
  in different wording. The core's cycle, reachability and single-root checks are
  workflow-only — a stream class's DAG is checked by every SDK and never by the
  core — and two veneer rules have no core counterpart.
- **Why it is big**: moving them changes refusal texts across every SDK, so it is
  a cross-SDK change under `CLAUDE.md`'s one-change-every-SDK rule. It removes
  roughly 19 rules × 4 SDKs of duplicated logic.
- **One rule has already diverged**: correction requires *every* event-time clock
  in the SDK and *at least one* in the core.

### `WorkflowOperative::procedures` accepts a stream builder

- **Severity**: P3 · a latent authoring hole.
- `sdk/rust/src/workflow/mod.rs` hands a workflow class the **stream**
  `ChannelBuilder`, so a class can call `c.effectively_once()` or
  `c.correction_window(…)`. `build_spec` never refuses it, `WorkflowSpec` has no
  field for it, and nothing reads it — silently inert. The core refuses both
  (`flz_core/src/host/register.rs:110,115`).
- Fold into `sdk-declaration-refusals-in-the-core`.

### Macro attribute branches with no coverage anywhere

- **Severity**: P3.
- Five `flezha-macros` attribute branches are exercised by no test in the
  repository: `idle_ttl =`, `max_entities =`, `capacity =`, `on_full =`,
  `clock =` (`procedure.rs` and all three arms of `on_full_call`). A pass fixture
  is owed before anyone refactors those emission paths.

---

### KI-64 · `config/config.exs` and `mix.exs` are ratcheted by no area

- **Severity**: P3 · a coverage gap in the ratchet, not a code defect.
- **Where**: `docs/refactor/baseline.json` — 36 areas, and their `credo_warnings`
  sum to **376** while the repo-level number measures **378**.
- **Evidence**: surfaced by segment D of `fix-opaque-module-names` when
  `--write-baseline` first filled `credo_warnings`. An independent
  `MIX_ENV=test mix credo --strict` gives repo 378; the per-area sum is 376. The
  two unaccounted findings are in `config/config.exs` and `mix.exs`, both of which
  `.credo.exs` includes and no area in the 36-area scan table covers.
- **Mechanism**: the scan table is a list of directory areas. Two files that
  credo reads sit outside every one of them, so their findings are counted in the
  repo total and ratcheted by nothing — a regression in either is invisible to
  `--check`, which is the gate that blocks.
- **A fix must**: either add the two files to an existing area's scan set (they
  are configuration, so the smallest honest home is whichever area owns build and
  config), or state in `baseline.json` that the repo number intentionally exceeds
  the area sum, so the discrepancy stops looking like a bug in the arithmetic.
  Prefer the first: an unratcheted file is the gate's blind spot, and naming it in
  a comment does not close it.

### KI-65 · `check-kind-naming.sh` gets redder every time the board discusses it

- **Severity**: P3 · a gate that cannot be greened while it is being described.
- **Where**: `scripts/check-kind-naming.sh`, and its 6 current hits — 3 in
  `docs/known_issues.md` (`:62`, `:70`, `:79`) and 3 in `docs/refactor/STATE.md`
  (`:302`, `:372`, `:423`).
- **Evidence**: the count was **4** at the start of session 3 and is **6** at its
  end. The two new hits are the board's own prose *about this gate being red* —
  `STATE.md:302` reads "(five `bounded` prose hits; …)" and `:372` reads
  "**`check-kind-naming.sh` is red and it is not this change** — four `bounded`".
  Writing down that the gate is red is itself a gate violation.
- **Mechanism**: `naming_gate.walk` scans every tracked human-readable file, and
  the gate has only an allowlist — which an earlier finding on this board records
  as erasing **destructively and in file order**, so a phrase-shaped entry can be
  half-consumed by an earlier rule. It has no way to say "this occurrence is a
  citation, not a use".
- **A fix must** reuse what already exists rather than add a third mechanism:
  `scripts/check-module-naming.sh` (landed 2026-08-31, `055054f2`) solved exactly
  this with `naming_gate`'s `record_tag`, which exempts a block a document marks
  as history. Giving `check-kind-naming.sh` the same `record_tag` closes the loop
  and costs one argument — charter §2's reuse order, and the reason this is filed
  rather than patched with six allowlist entries.

### KI-74 · `scripts/check-jacoco-coverage.py` has no caller — a threshold nothing enforces

- **Severity**: P3 · found by area 4's survey, outside its scope.
- **Where**: `scripts/check-jacoco-coverage.py`.
- **Evidence**: `grep -rn 'check-jacoco-coverage' --include='*.yml' --include='*.yaml'
  --include='*.sh' --include='*.md' .` returns **nothing** but the script itself.
  `java-sdk.yml` runs `-P coverage` and never reads the report.
- **Mechanism**: the script exists, is correct as far as anyone knows, and is invoked
  by no workflow, script or documented command. A coverage threshold that nothing runs
  is worse than no threshold: it reads, to anyone grepping the repo, as an enforced
  floor.
- **A fix must** either wire it into `java-sdk.yml` after the `-P coverage` run — and
  then state what the floor **is**, measured, rather than whatever the script's default
  happens to be — or delete it. Do not leave it as a third state.

### KI-75 · `sdk/java/flezha-sdk-protobuf/` is an orphan build tree

- **Severity**: P3 · cosmetic, but invisible to every gate.
- **Where**: `sdk/java/flezha-sdk-protobuf/`, holding only `target/` with 91 generated
  files.
- **Evidence**: `grep -c flezha-sdk-protobuf sdk/java/pom.xml` → **0**; the directory
  contains `target` and nothing else.
- **Mechanism**: the reactor no longer lists the module, but its build output was never
  removed. It is **untracked**, so `check-build-artifacts.sh` and every `git ls-files`
  walk are blind to it while `find` and a developer's IDE both see it.
- **A fix must** delete the directory. There is nothing to preserve: no source, no
  `pom.xml`, no reactor entry.

### KI-82 · Seven unreferenced public members in the Java SDK, ~50 lines

- **Severity**: P3 · dead surface, not a defect. **The decision is the user's.**
- **Where** (found by area 4 segment E, scanning declarations in
  `sdk/java/**/src/main` against every reference in the repo, docs and specs
  included): `RetryPolicy.backoffFor`, `TimerOp.encodeList`, `TimerOp.decodeList`,
  `PendingTimers.due`, `PendingTimers.asRegisterOps`, `TimerCounters.noteArmed`,
  `TimerCounters.firedTotal`, `KryoCodec.serializeWithHeader`,
  `StreamOperativeClassRegistry.getStreamOperativeClass` — plus
  `OperativeInstance.operativeId()`, found by segment D.
- **Why it is not funding for area 4**: every one is `public`, and that area's proof
  obligation is an **empty `javap -protected` diff** between the baseline tag and its
  close. Deleting them is an API change, which a zero-behaviour-change refactor is the
  wrong vehicle for. Both segments found them, both left them, and both said why —
  which is the correct call twice.
- **Why it is worth doing separately**: ~50 lines of surface that nothing in the repo,
  its docs or its specs references. The Java SDK is unpublished
  (`open-source-licensing`: "nothing is published"), so the cost of removing it now is
  **zero** and the cost after publication is a breaking change.
- **A fix must** confirm each member against the *published* surface as well as the
  repo — a public method with no in-repo caller may still be part of what the SDK
  offers deliberately — and then delete in one change with its own `javap` diff as the
  record of what left.

### KI-83 · `--write-baseline --timing` silently deletes every timing it is not given

- **Severity**: P2 · a defect in the programme's own instrument, so it corrupts the
  record rather than the code.
- **Where**: `scripts/refactor_metrics.py:793` —
  `data[key] = given or baseline.get(key, {})`, inside `write_baseline`, governing
  **both** `timings` and `bench`.
- **Mechanism**: `given` is built only from **this invocation's** `--timing`/`--bench`
  arguments, so it is either empty — and the baseline's map is preserved — or
  complete-as-passed, and the baseline's map is **replaced**. Recording one new timing
  therefore deletes every timing not re-passed on the same command line.
- **Evidence**: found by area 4 segment F while recording `mvn_test_sdk_java`. Writing
  it the way `tasks.md` 12.3 words the step would have dropped `mix_test_exunit` and
  `mix_test_wall`. The worker avoided it by editing `baseline.json` directly and
  reported the trap instead — the three timings are intact today, which is why this is
  a latent defect and not an incident.
- **Why it matters more than a lost number**: the only way to keep the others on the
  command line is to **re-pass their values**, which re-stamps measurements taken at
  `a147111c` with the current head. Segment F's own `mix test` read **77.3 s** against
  the recorded 56.2 — so obeying the tool would have written a number it could not
  support, attributed to the wrong commit.
- **It is the same failure class as `--lint`**, which writes `null` into every
  `credo_warnings`/`clippy_warnings` when omitted, and `--check` then **skips a null on
  either side** — silently disarming every credo ratchet. Two arguments on one script,
  both of which destroy state when you forget them, both silently. That is a pattern in
  the instrument, not two accidents.
- **A fix must** merge rather than replace — `data[key] = {**baseline.get(key, {}), **given}`
  — for `timings` **and** `bench`, with a test that records one key and asserts the
  others survive. It should also consider making `--lint`'s omission loud, since the
  same class of silence is the reason both exist as traps.

### KI-84 · `cli/tests/reset.rs`'s "dead controller" fixture hands the command a LIVE socket

- **Severity**: P2 · a test asserting a precondition it has not established. ~1 run in
  26 plain, 3 in 12 under load.
- **Where**: `cli/tests/reset.rs:19-28` (`stale_control_socket`), decided at
  `flz_transport/src/utility/socket_probe.rs:238` (`connect_probe`), consumed at
  `cli/src/reset.rs:381` (`Reach::Reachable => Action::Keep`) and
  `socket_dir.rs:227` (`first_creatable` skipping a rung another controller holds).
- **Mechanism, demonstrated rather than inferred**: the fixture makes its dead
  controller with `drop(UnixListener::bind(path))` — a socket that has been through
  `listen(2)`. A process a **sibling test** forks in the window between that `bind` and
  the `drop` **inherits the listener fd**, because `CLOEXEC` closes at `exec`, not at
  `fork`, and Rust's `Command` is fork-then-exec. Until that child execs, the socket
  the parent closed **still accepts a connect**:

  ```
  no concurrent fork             -> StaleSocket (ECONNREFUSED)
  a child forked in the window   -> Reachable  (connect succeeded)
  after that child exec'd/exited -> StaleSocket (ECONNREFUSED)
  ```

- **Rates measured**: 1/26 plain; 3/12 under eight busy loops at `--test-threads=32`;
  1/20 with the `reset` binary alone under load, so the concurrency is *inside* that
  binary; **1/12 at HEAD content with the new test file moved out — pre-existing**; and
  **0/150** driving `flz reset` sequentially against the same fixture, so the CLI is
  deterministic and the trigger is the harness.
- **It also leaves a directory behind.** On the ladder path `first_creatable` steps
  over the "live" rung and walks to `<cwd>/app_data/sockets`, which `Role::Controller`
  resolution then **creates** — `cli/app_data/sockets`, **not gitignored**, invisible
  only because it was empty and git does not track empty directories.
- **A fix must** stop the fixture asserting what it has not established: after
  dropping the listener, poll `UnixStream::connect` until it is refused (bounded), so
  no command under test can be handed a socket that still answers. **Not
  `--test-threads=1`**, which hides it. And not "never enter LISTEN" — that needs a raw
  `libc::bind`, since `UnixDatagram::bind` yields a SOCK_DGRAM inode a stream connect
  fails with `EPROTOTYPE` → `Reach::Unreachable`, breaking the tests that depend on the
  stale classification.

### KI-85 · `flz reset --include-sockets` can create a directory the operator did not name

- **Severity**: P3 · documented behaviour with a surprising edge, surfaced by KI-84.
- **Where**: `flz_transport/src/utility/socket_dir.rs:227` (`first_creatable`) reached
  from `cli/src/reset.rs`'s socket resolution.
- **What happens**: when the rung the operator pointed at looks **live**,
  `first_creatable` steps over it and resolves to a later rung — and `Role::Controller`
  resolution **creates** that directory. So a `reset` can `mkdir` somewhere the
  operator did not name. Observed as `cli/app_data/sockets` during KI-84's
  investigation.
- **It is not a bug in `first_creatable`**: `docs/dev_notes/fix-reset-socket-dir-resolution.md`
  records that resolving as the Controller *is* what "creatable" means, and that a run
  not asked about sockets must not consult the ladder at all. This run **was** asked.
- **A fix must** decide whether "reset the socket directory" should ever resolve past
  the rung it was given — the alternative being to refuse with the peer named, which is
  the shape colocation already uses. Cheap either way; it is a behaviour choice, not a
  defect.

### KI-86 · `extract_opts`'s `-o=value` branch is unreachable, so a short `=` flag is silently lost

- **Severity**: P2 · silent option loss, no error.
- **Where**: `cli/src/args.rs` (was `cli/src/utils.rs:385`), preserved byte-for-byte by
  area 6 batch 1's zero-behaviour-change contract.
- **Mechanism**: the chain is
  `if let Some(eq) = arg.find('=') { if arg.starts_with("--") { … } } else if …`. An
  argument containing `=` that does **not** start with `--` enters the outer branch,
  matches no inner arm, **does nothing**, and never reaches the three `else if`s. So
  `flz plugin enable p prov -o=json` **silently drops the flag**, and the short-key
  alias table for the `=` form has never executed.
- **A fix must** be the one missing `else` — and must decide, deliberately, whether a
  short flag with `=` is supported or **refused**, because silently accepting it after
  years of silently dropping it is a behaviour change either way. Prefer refusal with
  the flag named: an operator who typed it meant something.

### KI-87 · `refactor_metrics.py` is blind to untracked files, and the blindness always flatters

- **Severity**: P2 · the programme's own instrument, third silence of the same family.
- **Where**: `scripts/refactor_metrics.py`'s file walk, which reads `git ls-files`.
- **What happens**: a file that exists on disk but is not tracked is **invisible to
  every metric**. Because refactors *add* files, the error is one-directional — it
  **silently improves every count**. Area 6 batch 1 measured a naive `--area cli` at
  `prod_loc` **2745**, a phantom **−599**, with the whole `reset/` directory absent from
  every ceiling.
- **How often it has bitten**: five times this session, caught each time only because
  the supervisor runs `git add -N` before reading any number — segment B's
  `duplicate_pairs` read 83 instead of 88, segment C's `--area` read 7846 instead of
  8733, area 4 segment F's own dev note was invisible to `check-kind-naming`.
- **It is the third of a family**: `--lint`'s omission wrote `null` and `--check`
  skipped it ([[KI-83]]); `--timing` replaced rather than merged ([[KI-83]]); and this.
  **Every one silently reports better than the truth.** That is a design tendency in
  the tool, not three accidents.
- **A fix must** make the walk see the working tree — `git ls-files --others
  --exclude-standard` alongside the tracked set — or, if reading only tracked files is
  deliberate, **say so loudly** when untracked source files exist in a scanned area,
  the way `--check` now names the metrics it could not compare.

### KI-88 · `flz state help` omits `help` from its own subcommand list

- **Severity**: P4 · cosmetic, one line.
- **Where**: `cli/src/help.rs`'s `state` screen. It is the only screen of 31 that does
  not list `help` among its subcommands, while `flz state help` works.
- Pinned as a deliberate exception in `cli/tests/dispatch.rs` with a comment naming it,
  so the golden-file capture reproduces today's text rather than quietly fixing it.
- **A fix must** add the row **and** update the golden capture in the same commit, so
  the control still proves what it claims.

### KI-89 · Two `flz` help rows are misaligned, and the golden capture now pins them

- **Severity**: P4 · operator-visible cosmetics, two source lines.
- **Where**: `flz help`'s `runner` description at **column 18** against its **16**
  siblings at 20; `flz audit help`'s `stats` at **column 14** against its four siblings
  at 15. Both verified against `cli/tests/golden/flz.txt` and `audit.txt`.
- **Why it is filed rather than fixed**: area 6 batch 1's contract was zero behaviour
  change, so the golden capture reproduces today's text **including** the
  misalignments. That is what made "regenerate the table correctly" impossible as an
  approach — there was no correct version to regenerate to — and it is why the capture
  had to be taken from the HEAD binary before any edit.
- **A fix must** change the two source lines **and** re-capture the two golden files in
  the same commit, or the control stops proving what it claims. It is the same family
  as [[KI-88]] (`flz state help` omitting `help` from its own list) and the two should
  land together — one commit, three cosmetic corrections, three golden updates.

### KI-90 · `narrative_markers` cannot see a task or section id

- **Severity**: P4 · a gate that under-reports, found by area 5b's survey.
- **Where**: `scripts/refactor_metrics.py:351` — the regex matches `\btasks?\.md\b`
  and `\bD\d{1,2}\b`, but not a bare `task N.N` or `§N`.
- **Evidence**: `test/plugin/provider/websocket_subscription_source_test.exs` has
  `describe` blocks named `"8.5 validation"`, `"8.1 reconcile"`, `"8.2 commit
  gating"`; `producer_worker_test.exs` has `"worker swap on {:kafka_worker_dead,
  ...} (task 2.7)"` and `"effectively-once tier records (§5)"`. The `test` area's
  `narrative_markers` is **47** and none of these is among them — every one names a
  task id a future reader cannot resolve, which is precisely what §5 forbids.
- **Why it is a report and not an edit**: widening the regex raises the count in
  **every area at once**, and eleven of them are already blocking. It needs the
  widening and a re-baseline in one commit, which is a programme-level change, not
  an area's.
- **A fix must** land the regex and `--write-baseline --lint` together, and say how
  many of the new hits are genuine history versus correct prose reworded for a regex
  — area 5a measured that ratio at **9 of 17**, and 5b at **8 of 8**.

### KI-91 · A production busy-wait the `sleeps` metric is designed not to see

- **Severity**: P3 · real, small, behaviour-adjacent.
- **Where**: `lib/flezha/plugin/provider/worker_sup.ex:82` — `await_unregistered/3`,
  `Process.sleep(2)` for up to 50 attempts, on the worker-swap path.
- **Evidence**: `refactor_metrics.py` reports `sleeps: 0` for `lib/flezha/plugin`.
  The counter at `:401` is gated on `if in_test[i]`, so a `Process.sleep` in `lib/`
  is invisible **by design** — charter §9's sleep rule is a test rule, and this is
  production.
- **Mechanism**: `DynamicSupervisor.terminate_child/2` returns once the child exits,
  but `Registry` unregisters asynchronously on the `:DOWN`, so a wait is genuinely
  required or the restart is handed back `{:already_started, dead_pid}`. The code
  says so in its own comment. A monitor on the reaped pid would be exact.
- **Verdict (supervisor, 2026-08-31)**: **real but not taken in area 5b.** Replacing
  a bounded poll with a monitor changes the shape of a restart path under a zero-
  behaviour-change contract, and the area has no instrument that would prove it
  neutral. Recorded here so the next change touching `worker_sup.ex` takes it.
- **A fix must** monitor the reaped pid and wait for its `:DOWN` rather than poll,
  and prove the swap path unchanged under a forced worker death.

### KI-92 · The workflow bench's first run on this box is ~4× the plateau

- **Severity**: P2 for the refactor programme — it is an instrument defect, and
  **every A/B row measured on `mix bench.workflow` is exposed to it.**
- **Where**: the instrument, not the repo. `mix bench.workflow --scenario chain
  --procs 4 -n 1000 -c 100` on this laptop.
- **Evidence**: five consecutive runs of **one identical arm** (release
  `sha256sum` verified unchanged before and after the series):

  ```
  93.2  33.3  23.1  24.2  23.1   workflows/s
  wall 16 / 36 / 51 / 47 / 50 s
  1-min load 1.12 / 5.18 / 6.94 / 8.54 / 9.47
  ```

  **303 % spread over the five; 4.8 % over the plateau (runs 3–5).**
- **Two mechanisms refuted, which is what makes the number usable**: a sixth run
  of the same arm after 4.5 minutes idle read **91.1 workflows/s** at load 0.39,
  so nothing accumulates on disk or in the tree — and `state dir 0 MiB, 4080 wal,
  1020 retained` was identical on every run. `package_throttle_count` read
  **67467 before the cooldown, after the idle and after the cold run** — flat, so
  package thermal throttling is not it either. What remains is the box's
  sustained-load clock behaviour under the `powersave` governor, unseparated.
- **Why it matters beyond one row**: charter §8 requires a measured floor before a
  row is classified. **A floor computed from a series whose first sample is a cold
  run is not a floor** — it is 303 % here and 4.8 % one run later. Any past row
  whose arms were not interleaved could have read a warm-up as an effect.
- **A fix must** either state a settling protocol for every row on this driver
  (discard until the plateau, then interleave, with the sample count reported) or
  record the transient beside §8's rules so no future worker reads a first-run arm
  as a result. **The operational rule, in force now: the first run of a series on
  this box is not a result.**

### KI-96 · Websocket's `convert_value/2` answers a struct where the callback says `String.t()`

- **Severity**: P4 · off-contract, and strictly better than what it replaced.
- **Where**: `lib/flezha/plugin/provider/websocket.ex` `convert_value/2` — `ws_opts`,
  `post_connect`, `payload_metadata` and `subscription_headers` return
  `{:error, %Jason.DecodeError{}}` against the `@callback`'s `String.t()`.
- **History that matters**: before [[KI-93]] those four *raised*
  `Protocol.UndefinedError`, because the old builder interpolated the struct into a
  message. The fix stopped the raise, so the non-binary reason now propagates
  instead of exploding. **Strictly better, still wrong**, and the fault is the
  provider's own return value rather than the shared builder's.
- **A fix must** render the decode error to a string at the provider (`Exception.message/1`),
  and check the other eight providers for the same shape rather than fixing only
  the one that was found.

### KI-97 · `mix bench.extension` has been dead since the workload rename

- **Severity**: P3 · a dev instrument, no production impact — but it is a
  *benchmark*, and a benchmark that cannot run is a regression detector nobody
  knows they have lost.
- **Where**: `lib/mix/tasks/bench/extension.ex` — **THREE sites, not the two this
  entry first named, and its description of one was wrong** (corrected 2026-08-31 by
  area 5d): `:118`'s `entity_responses:`, `:120`'s `entity: 0` **inside the Kayrock
  response shape rather than the producer opts**, and `:137`'s in the opts. Fixing only
  the two named leaves `Wire.extract_partition/3` matching `%{partition_responses:
  parts}` against a map with no such key — a `CaseClauseError` in place of the
  `KeyError`. The opts pass `entity: 0` where `ProducerWorker.configured/1` does
  `Keyword.fetch!(opts, :partition)` (`producer_worker.ex:58`, `:166`).
- **Evidence** (reproduced by the supervisor at `041dd838`):

  ```
  $ mix bench.extension
  Kafka chain mailbox boundedness — 80 ev/s for 5 s
  ** (EXIT from #PID<0.95.0>) an exception was raised:
      ** (KeyError) key :partition not found in:
      [plugin_id: "bench", topic: "bench", entity: 0, ...]
  $ echo $?
  1
  ```

  The Kafka scenario runs **first**, so the task dies before reaching the
  `Extension.run/3` throughput rows. The whole task is unusable.
- **Mechanism, named to the commit**: `1926bd28` *"refactor!: rename the workload
  model to Operative → Procedure → Entity"* renamed `partition:` → `entity:` here.
  **That `partition` is a Kafka partition, not a Flezha Entity** — a false-positive
  rename in a mechanical sweep. `fetch!` then raises deterministically.
- **Blast radius, checked rather than assumed**: **nothing past is invalidated.**
  No `timings` or `bench` row in `docs/refactor/baseline.json` comes from this task
  (its bench keys are roundtrip and rust-dispatch), no dev note rests a claim on it,
  and the only mentions in `STATE.md` are area 5b's survey observing that **no floor
  for it exists anywhere in the programme** — which, read now, was the first
  symptom. The loss is prospective only.
- **Verdict (supervisor, 2026-08-31)**: **real, pre-existing, one-line fix at two
  sites** (`entity:` → `partition:`). It is in `lib/mix`, which is **area 5d's**
  tree, so 5d takes it as a fix-first item; area 5b correctly did not touch it.
- **Consequence for 5b**: its A/B row P1 reclassifies to **`direction` and is not
  run** — and for a stronger reason than area 4's J1 precedent. J1 was reclassified
  because its floor could not resolve the effect; **this instrument produces no
  number at all.** The arm `sha256sum` is recorded in its place.
- **A fix must** correct both sites, run the task to completion, and record the
  first floor this instrument has ever had — under [[KI-92]]'s rule that the first
  run of a series on this box is not a result.
- **RESOLVED 2026-08-31, `47003c22`.** All three sites corrected, red first, and the
  task now completes. **The first floor this instrument has ever had**, five runs with
  run 1 discarded: **≤ 4.0 %** on the four rows above 100 ns, **16.7 %** on the 60 ns
  row, which can therefore resolve nothing.
- **It also corrects [[KI-92]]'s scope**: the cold-start transient **does not appear on
  this driver** — run 1 sits within **3.6 %** of the plateau, against **303 %** on
  `bench.workflow`. KI-92 is a property of *that* driver, not of the box, and the
  settling rule is applied per instrument rather than assumed everywhere.

### KI-101 · `bench.workflow` prints a millisecond figure in scientific notation

- **Severity**: P4 · operator-visible output, and the exact thing an existing test
  was written to prevent. Found by area 5d while *refusing* a fold.
- **Where**: `lib/mix/tasks/bench/workflow/run.ex`'s `fmt_ms/1`.
- **Evidence** (reproduced by the supervisor):

  ```
  $ elixir -e 'IO.puts("in #{Float.round(1000.0, 1)} ms")'
  in 1.0e3 ms
  ```

  So `mix bench.workflow` prints `in 1.0e3 ms` whenever a window lands on a round
  millisecond.
- **Mechanism**: `Float.round(1000.0, 1)` is still a float, and Elixir's `to_string/1`
  renders a float of that magnitude in scientific notation. `Report.ms_from_ns/1`
  formats to a string and gives `"1000.0"`. The two agree on **2 011 of 2 016** vector
  inputs and disagree on **five** — all of them round values.
- **How it was found, which is worth recording**: area 5d was told to fold the four
  duration renderers into one shared helper. It measured the candidates against the
  shared implementation first, found the five disagreements, and **refused the fold on
  its own task's rule** — *"a disagreement is a behaviour change and stops the fold"*.
  The defect is a by-product of taking that rule literally rather than eyeballing two
  functions that look alike.
- **`bench_report_test.exs` already asserts a duration is never rendered "as a raw
  float"** — the test exists, and `Run.fmt_ms/1` is simply not the path it covers.
- **A fix must** render through the same formatter the report contract uses, and extend
  the existing assertion to the workflow task's own path so the two cannot drift again.
  **Not folded blindly**: three of the four renderers feed JSON and `BENCH_RESULT`
  contracts that read **numbers**, where the shared helper returns a string.

### KI-102 · Four small defects in `runner`/`operative`, three of them dead code

Filed as one entry because they share a shape and a remedy: found by area 5e-a's
survey, all four **deletion-funded or a one-line move**, none reachable from a live
path, so none needs a red-first test.

1. **`Operative.Builder.from_json/1` is dead AND cannot work.**
   `lib/flezha/operative/builder.ex:38`. It does
   `case Jason.decode!(json_str) do {:ok, m} -> …; {:error, _} -> … end`, but
   `Jason.decode!/1` returns **the decoded term**, so every valid JSON object raises
   `CaseClauseError`. Verified by the supervisor: the source reads exactly that, and
   `grep -rn "Builder.from_json\|Builder.from_map" lib/ test/ cli/` returns **0**.
   **Delete `from_json/1`, `from_map/1` and `get_from_map/2`** — repairing a function
   nothing calls is the wrong remedy.
2. **`FleetSupervisor.default_runner/0` mixes supervisor APIs, in dead code.**
   `lib/flezha/runner/fleet_supervisor.ex:31` calls `DynamicSupervisor.which_children/1`
   on a module started as a plain `Supervisor`. Zero callers for it or `stop_fleet`.
   Delete both.
3. **A `@doc` is attached to the wrong function.** `fleet_manager.ex:46`–`:53`. The text
   describes `connect_handler/4` — "`stream_ref` is a NIF StreamHandle resource",
   "`init_payload` is the decoded JSON" — and the next `def` is
   **`list_factories(namespace)`** at `:54`; `connect_handler/4` at `:58` has none.
   Verified by reading `:45`–`:59`. So `list_factories/1`'s published documentation
   describes a different function. Elixir attaches `@doc` to the following `def` and
   nothing checks that the text names it.
4. **A `TODO` in production code.** `control_unit.ex:292` — *"Not sure if we should
   convert to map from the beginning."* Credo `[D]`, and §5 deletes a comment that
   states no invariant.

- **Verdict (supervisor, 2026-08-31)**: **all four real, all four taken inside area
  5e-a** as ordinary deletion-funded work rather than as a fix-first commit — nothing
  here is reachable, so there is no behaviour to protect and no red to drive.

### KI-103 · `FleetManager.rebind_data_streams/1` is dead, and a dev note says it is live

- **Severity**: P3 · dead code is cheap; **a dev note asserting a capability the code
  does not have is not**.
- **Where**: `lib/flezha/runner/fleet_manager.ex:120`–`:128`, public and `@spec`'d.
- **Evidence**: `grep -rn 'rebind_data_streams' --include='*.ex' --include='*.exs' lib/
  test/ cli/` returns **only its own `@spec` and `def`.**
- **The half that matters**: `docs/dev_notes/fix-first-input-drop-on-bind.md:33`
  describes it as the remedy the convergence Loop uses for *"a stream that drops and
  rebinds mid-life without the operative rebinding"* — and
  **`lib/flezha/convergence/` has no such heal op.** The Diff mints
  `rebind_operative`, `replace_operative` and `resync_runner`, and nothing else. So
  either the capability was never wired to the Loop, or its caller was removed and the
  note went stale with it.
- **Mechanism**: the body issues `GenServer.call(control_unit,
  :command_sockets_connect)`; that command has exactly one live caller,
  `Session.runner_connect/3:401`, on every connect — i.e. the *connect* path rebinds
  streams, and the *mid-life* path the note describes does not exist.
- **Verdict (supervisor, 2026-08-31)**: **real, and NOT taken in 5e-a.** The worker was
  right to refuse it: deleting the function would rule the missing caller away, and
  wiring a data-stream rebind op into the Loop is a behaviour change. **The question is
  which of the two the platform wants**, and that is a product decision — a heal the
  convergence Loop cannot perform is either a gap or a stale note, and only one of those
  is free to fix.
- **A fix must** decide that first, then either wire the op into `Convergence.Diff`/
  `Heal` with a drill that proves it fires, or delete the function **and correct the dev
  note in the same commit** — the note is the reason this looked like a capability.

## Resolved

### KI-128 · `credit_flow_e2e_test.exs` reads the credit flag at a point in time after the burst, so a legitimate re-attach fails it with a message naming the wrong cause — RESOLVED (hardened)

- **Severity**: P3 · seen once in 20 five-file php arms (supervisor 1/4, worker 0/16 including 14 in the failing arm shape), never alone (php 3/3, rust 1/1). · Found 2026-09-02 by the supervisor's verification arm of R5 S2 batch 3, the first day the scenario ran under php.
- **Where**: `test/system/credit_flow_e2e_test.exs:133-134` — `active_credit_row(ce_id)`
  (`test/helpers/credit_test_helper.ex:46-49`) is a point-in-time read of the first
  `LivenessCounters` row for the runner with `credit_active == 1`, no wait. That flag is set
  by the `{:credit_ack, session}` handler (`stream_channel.ex:383`) after the SDK acks the
  `CreditWindowInit` offered on the first state frame of an attach, and cleared in exactly
  one function, `Records.reset_credit/1` (`records.ex:329-334`), whose only caller is the
  `{:flz_closed, _, _}` handler (`stream_channel.ex:467`): the state stream's socket closed
  and the next state frame re-runs the handshake.
- **Evidence**: `FLEZHA_E2E_RUNTIME=php mix test --include system --include sdk_smoke <sdk_smoke compaction credit_flow state_freshness partitioned_operatives>` →
  once `22/23 passed`, `no credit-speaking secondary channel row for credit_flow_e2e_8667569809`
  at `:134`, after a normal ~5 s burst (`await_seq(writer, 300)` at `:127` had passed). Test
  logging runs at `:error` (`config/test.exs:165`), so the discriminating lines — the
  `Connection closed for IO socket.` warning at `stream_channel.ex:452`, the credit-session
  info line, a credit-overrun warning, the runner's own `tracing::warn!` — are all suppressed
  and the log shows nothing between "runner up" and the assertion.
- **Mechanism (as far as the box got)**: the row was credit-speaking at `:109`, the burst
  landed, and between the last state frame and `:133` the secondary stream closed →
  `reset_credit` → `credit_active` 0; with the burst over no state frame re-ran the handshake,
  so the read finds no credit-speaking row. Two candidates for the close, unseparated: (A)
  runner-side, `state_sink.rs:228` `request_reconnect("broken_pipe_flood")` or the control
  reader's silence verdict (`control_unit.rs:338`); (B) controller-side, the `runner_retiring?`
  branch at `stream_channel.ex:449` or a gate close. php's cold single-loop tempo (the first
  runner's mean body 10 ms) widens the window; nothing php-specific in the port touches the
  path (the six fixtures are folds; node ran the same burst green four times, ruby twice).
- **The separating experiment**: the arm with the test logger at `:warning` so a close prints,
  and the failure at `:134` dumping the runner's rows (`credit_active`, `credit_ledger_level`,
  `credit_overrun_total`, `hb_credit_parked`) — an overrun > 0 at that moment means (B), zero
  with a close means (A).
- **A fix should** read the credit verdict from the row captured at `:109` (its counters ref
  is per attach and holds the burst's numbers whatever happens after) and dump the rows on
  failure so the message names the real cause; red-first by rate, which this box could not
  deliver.
- **Verdict (supervisor, 2026-09-02)**: a suspicion under §1b step 3 — 30-minute box spent,
  unreproduced; the php `state_persistence` flip lands (the scenario is green 19 of 20 and
  the failure shape is the harness's, not the port's). The test hardening is owed as a small
  item of its own.
- **Corrected at the code by the hardening (2026-09-02)**: a `snapshot_all/0` row is a VALUE map, so a row captured at `:109` holds the handshake's numbers, never the burst's — what survives the settle is the row's key `{runner_id, name}`; the `:counters` array is one per channel PROCESS (registered once in `StreamChannel.init/1`, kept across `{:flz_closed}` and a re-bind, pruned only on owner death); `reset_credit/1` zeroes `credit_active` and `credit_ledger_level` only, `credit_overrun_total` is bump-only. A controller-side `close_stream` drops the fd with no `{:flz_closed}`, so the mechanism was induced by sending the driver's own tuple to the channel the settled row names.
- **Resolved (hardened)**: `harden-credit-flow-verdict-read` — the test reads the verdict by the settled row's name through `counters_row/2` and every credit assertion dumps the runner's rows. Red first, induced: the old assertion failed with KI-128's exact message on the induced close; the new read on the same state is green (`hb_credit_parked 91 → 95`, overrun 0). rust, php ×3 (+2 by the supervisor), node `1 passed`. Which side closes the stream in the wild stays unseparated; the test now names it when it happens. `test/load/credit_soak_test.exs:162,189` reads the same way and is opt-in — left. ADDED requirement in `credit-grant-flow-control`.

### KI-125 · A Python runner aborts at interpreter exit after a graceful SIGTERM shutdown — RESOLVED

- **Severity**: P3 · the drain completes and every notification ships; then the main thread
  waits exactly 1 s and the process dies with `SIGABRT` (exit 134), which an orchestrator reads
  as a crash after every graceful stop of a runner whose stdin is a pipe. · Found by the R5 S2
  worker, 2026-09-01, timing the KI-123 fix under `strace`.
- **Where**: `sdk/python/src/flezha/runner.py:311-334` `_watch_stdin_eof` — a daemon thread
  blocked in `sys.stdin.buffer.read(1)`; CPython's finalisation waits one second for that
  buffer's lock and then aborts with `Fatal Python error: _enter_buffered_busy: could not
  acquire lock for <_io.BufferedReader name='<stdin>'> at interpreter shutdown`.
- **Evidence** (supervisor's own run, no SDK loaded):

      $ sleep 5 | sdk/python/.venv/bin/python stdin_daemon.py   # a daemon thread in stdin.buffer.read(1), sleep 0.2, normal exit
      Fatal Python error: _enter_buffered_busy: could not acquire lock for <_io.BufferedReader name='<stdin>'> at interpreter shutdown, possibly due to daemon threads
      exit=134

  Under the runner: after SIGTERM the main thread sits in one timed `FUTEX_WAIT` of 1.000 s
  (`ETIMEDOUT`) and then `tgkill(SIGABRT)`. An EOF-driven exit does not hit it — the watcher
  itself returns.
- **A fix must** stop holding the buffer lock at finalisation: read the descriptor unbuffered
  (`os.read(sys.stdin.fileno(), 1)`), or end the watcher before exit, or exit through
  `os._exit` after the drain. Red first: a subprocess test that SIGTERMs a runner with a piped
  stdin and asserts exit status 0.
- **Verdict (supervisor, 2026-09-01)**: real, P3, owed to its own python-only change after R5
  S2; does not pause the row.
- **Resolved**: `exit-cleanly-after-sigterm-in-python` — `_watch_stdin_eof` reads the descriptor with `os.read(sys.stdin.fileno(), 1)`, holding no `BufferedReader` lock at finalisation. Red first: `test_a_signalled_runner_with_a_piped_stdin_exits_cleanly` read `-6 == 0` with the abort on stderr at HEAD (the test keeps stdin OPEN — `communicate` would close it and turn the path into EOF); after, the python runner exits 0 after 53 ms where it was 134 after 2195 ms. Ruby, node and php measured clean (0 / 0 / no thread). ADDED requirement in `python-sdk`.

### KI-130 · A wedged PHP runner cannot die: evicted and ordered to exit, it stays up, reconnects under a fresh identity and wins the rebind — RESOLVED

- **Severity**: P2 · the convergence ladder's last rung (`runner_pool_wedged` → evict → terminal
  disconnect → the orchestrator restarts a failed process) does not end the process it targets
  on a single-loop erased host, and the controller re-identifies the returning process as a
  new runner and places work on it. Pre-existing on every count; visible on 2026-09-02
  because the full php system arm ran with `control_plane_wedge_e2e_test.exs` for the first
  time (R5 S5 close-out).
- **Where**: `sdk/php/src/Runner.php` — SIGTERM (`pcntl_signal`) and stdin EOF are polled
  between polls, so a `sync` loop inside a body (`sleep(3600)` in the `wedge_probe` fixture)
  observes neither; `flz_core/src/shutdown.rs:228` `exit_terminally` runs
  `run_active_graceful_shutdown()` before `process::exit(70)`, and the bridge's snapshot
  function serialises every Entity under `with_entity_exclusive`, which waits on the wedged
  Entity's gate without bound; the controller side (`FleetManager.evict_runner` with
  `proved_wedged`) clears the binding and nothing in the handshake refuses that process's
  return.
- **Evidence** (worker, `$SCRATCH/r5s2/s5/probe/`): `FLEZHA_E2E_RUNTIME=php mix test --include system test/system/control_plane_wedge_e2e_test.exs`
  → `3 passed` (5.9 s), `2/3 passed` (124.5 s), then the probe copy `3 passed` (7.7 s), `2/3
  passed` (124.6 s) — 3 red of 5, always `:71`, the phase-2 rebind wait expiring. The
  extended failure message shows the operative rebound onto a runner id that is neither A
  nor B, listed in `active_runners` beside B, with only two `runner up` lines in the namespace
  — runner A back under a fresh identity after `[flezha] controller ordered a terminal
  disconnect (reason=convergence:runner_pool_wedged); exiting 70`. Supervisor's own run:
  `3 passed`, and one `php … runner.php wedge_e2e_…` process left alive after it (killed by
  pid). Node's drill is green because `watchStdinEof` terminates its wedged runner after the
  5 s grace, so the zombie is dead before it can re-register.
- **A fix must** decide which half owns it, and probably take two: `exit_terminally` must
  not wait unboundedly on a wedged Entity for any SDK (the rung exists precisely for a
  runner that cannot finish its work — a bounded wait, or skipping the wedged Entity's
  serialise, keeps the ship for the others); and the controller should refuse or at least
  not place on a runner it has evicted as wedged until the orchestrator's restart is
  evidenced (a fresh process, not a fresh id). The PHP host's own inability to observe a
  signal inside a body is the SDK's documented single-loop posture and is the smallest
  part. Red first: the php drill above, 3 in 5.
- **Verdict (supervisor, 2026-09-02)**: real, P2, not R5's — none of the row's commits
  touches these paths — and larger than a close-out. Own change, queued as the next work
  item after R5 closes; the drill leaves one wedged php process per run until then, so a php
  full arm is followed by a kill by pid.
- **Mechanism corrected by the fix's survey (2026-09-02)**: (1) `Planner.wedge_ops/2` ran `evict_runner` first and `restart_runner` second, keyed on the session token — and every SDK's evicted process reconnected ~1.1 s later under a FRESH token, so the terminal rung was unreachable from a verdict on every SDK; it had only ever fired from the drill's hand call. (2) The unbounded wait is `PollCore::with_entity_exclusive`, reached from `finish_disconnect` → `run_active_graceful_shutdown` → `serialize_slot`, on every `flz_core` SDK (a restart order to a wedged rust runner: ack, no exit in 45 s). php was only where nothing else ended the process.
- **Resolved**: `57135d69` (the core: `try_with_entity_exclusive` at 5 s, the host-command wait during a ship at 5 s, `entity_busy:` refusal — a named-Entity live read refuses, the walks omit) and the convergence commit after it (the ladder inverted: `restart_runner` first on the session the verdict named, `evict_runner` for a process that did not exit; the Elixir SDK's RunnerServer no longer restarted after the terminal disconnect, KI-131). The drill `3 passed` on all seven runtimes with no process left behind; php `3 passed` three times.

### KI-131 · The Elixir SDK's RunnerServer is restarted by its supervisor after a terminal disconnect and re-registers the process the controller just ended — RESOLVED

- **Severity**: P2 · KI-126's Elixir twin: after `RunnerDisconnect{terminal}` the standalone
  `RunnerServer` returned `{:stop, :normal, _}` once `System.stop(70)` was called, and
  `Flezha.SDK.child_spec/1`'s `restart: :permanent` supervisor started a fresh server, which
  connected under the same stable id until `:init.stop` took the tree down — the ended process
  was placeable again for the window. · Found by the R5.1 worker on 2026-09-02 when the wedge
  drill's first test asserted `active_runners == [runner_b]` under `FLEZHA_E2E_RUNTIME=elixir`
  and read `2/3 passed` at `:137`.
- **Where**: `lib/flezha/sdk/runner_server.ex` `dispatch({:runner_disconnect, %{terminal: true}}, …)`,
  the non-`:native` branch.
- **Fix**: the branch returns `{:noreply, %{s | disconnecting?: true}}` after `System.stop/1`, so
  the server stays up until the application shutdown ends the VM; the `:native` (in-node)
  branch keeps its `{:stop, :normal, …}`. Elixir drill after: `3 passed` on three consecutive
  runs. Landed with `end-a-wedged-runner-on-terminal-disconnect` as the change's Cross-SDK
  obligation.


### KI-127 · The Java `store_input` twin serialises its state with camelCase keys, and the Node sugar twin does too — RESOLVED

- **Severity**: P3 · `test/apps/java_runner`'s `store_input` writes `{"binaryInputs": [], "textInputs": [...]}` where the reference (`test/apps/rust_runner/src/entity.rs:277-333`) and every other twin write `binary_inputs`/`text_inputs`; hidden until 2026-09-02 because the only scenario that reads the keys, `state_freshness_e2e_test.exs`, hard-spawned the Rust runner on every runtime. · Found by the R5 S2 worker running the java arm after that test was made honest.
- **Where**: `test/apps/java_runner/src/main/java/com/flezha/test/StoreInputProcedure.java:43` — `record State(List<byte[]> binaryInputs, List<String> textInputs)`; `SugarStoreState.java:10` — `public List<String> textInputs`. Node's `sugarStoreInput` in `test/apps/node_runner/fixtures.mjs` spells the same field `textInputs`; the hand-written node `store_input` (`fixtures_state.mjs`, S2) uses the reference's spelling.
- **Evidence** (worker, 2026-09-02):

      $ FLEZHA_E2E_RUNTIME=java mix test --include system test/system/state_freshness_e2e_test.exs
      Result: 0/1 passed   — at :128, the decoded snapshot is
      %{"binaryInputs" => [], "textInputs" => ["alpha-freshness_e2e_7269", "beta-…", "gamma-…"]}

  The mechanism works (the snapshot arrives with all three inputs); only the keys differ.
- **A fix must** spell the fields as the reference does through whatever the Java SDK's state serialiser already honours, and align the Node sugar twin in the same spirit; the java arm above is the red-first test.
- **Verdict (supervisor, 2026-09-02)**: real, P3, fixed at the R5 S2 checkpoint as its own commit because the honest `state_freshness` cannot land while the java arm is red on it.
- **Resolved**: java half in `35586558` (the two types renamed to the reference's spelling, no annotation, no new dependency; java `state_freshness` 1 passed, smoke tier 23 passed), node sugar twin in the node `state_persistence` port commit.

### KI-126 · Every FFI shutdown tells the coordinator to reconnect, so a stopping runner re-registers with a live controller — RESOLVED

- **Severity**: P2 · after shipping its state a node, python, ruby, php or java runner opens a
  fresh control connection and fresh data streams to the controller it is leaving, and a live
  controller can hand it work it will never do; against a dead controller it retries three
  times. · Found by the R5 S2 worker, 2026-09-01, tracing the KI-123 fix with `strace`.
- **Where**: `flz_core_ffi/src/lib.rs` `flz_core_shutdown` — after `poll_core.shutdown()` it
  sends `RunnerMessage::Reconnect("controller_requested")` on the runner channel (the same send
  the pre-fix entry made). `flz_core/src/coordinator.rs:392` `reconnect` returns only for an
  already-`Stopped` runner; `:591` `handle_reconnect` sets `Stopped`, calls
  `control_unit::open_reconnect` and sets `Running` again — for every reason, with nothing
  gating it on shutdown.
- **Evidence** (worker, python runner under `strace -D -f -tt`, `$SCRATCH/r5s2/py_strace.txt`
  lines 11, 42–57): at SIGTERM+8 ms the coordinator thread `connect`s to the still-live control
  socket and opens fresh primary/secondary stream sockets for the operative, then retries
  against the dead controller three times. Mechanism verified at the code by the supervisor.
- **A fix must** establish whether anything needs that send (the coordinator thread is a
  `recv()` loop over a channel whose sender the coordinator itself holds through `otx_arc`, so
  dropping the FFI's sender cannot close it, and nothing joins the thread) and, if nothing
  does, delete it — with an ABI-level assertion that the harness controller accepts no new
  connection after it acks the notification.
- **Verdict (supervisor, 2026-09-01)**: real, worth fixing now — fixed in the same change as
  KI-123 (`ship-shutdown-notification-from-erased-hosts`) as its own commit, 30-minute box on
  the one question above.
- **Resolved**: `4e20aeb2`, 2026-09-02 — `flz_core_shutdown` runs the bridge off the caller's thread before the poll core stops; every host (node, python, ruby, php, java) raises its stop flag only after the core's word; the shutdown-time `Reconnect` send, the `runner_tx` field and its take are deleted. Red-first at the ABI (notification awaited; connection count unchanged after the ack) and on the node/python `state_freshness` arms.


### KI-123 · The erased hosts never ship `RunnerShutdownNotification` on SIGTERM, so the controller's shutdown snapshot stays empty in four SDKs — RESOLVED

- **Severity**: P2 · a spec'd freshness feature is absent in node, python, ruby and php; the
  WAL still holds every change, so nothing is lost, but a deployment with
  `snapshot_interval: 0` sees `Overwatch.state.snapshot == nil` for ever. · Found by the R5 S2
  worker, 2026-09-01, the moment `state_freshness_e2e_test.exs` stopped spawning Rust on every
  runtime (it opened `cargo run` by hand and never called `start_runner_port/2`); python had
  "carried" `state_persistence` with this scenario green only for the reference's reasons.
- **Where**: `flz_core_ffi/src/lib.rs:412-424` `flz_core_shutdown` — sets
  `poll_core::global().shutdown()` and drops `runner_tx`, never calls
  `flz_core::shutdown::run_active_graceful_shutdown`; `flz_core_ffi/src/lib.rs:580`
  `flz_core_context_free` is a bare `drop(Box)` with no `Drop` impl. The core's own SIGTERM
  thread (`flz_core/src/shutdown.rs:252` `install_sigterm_handler`, installed by
  `register_bridge` at `flz_core_start`, `lib.rs:273-274`) is the only path that ships the
  notification, and every host runtime installs its own sigaction on top of it —
  `sdk/node/src/runner.ts:277` `process.on('SIGTERM')`, `sdk/python/src/flezha/runner.py:306`
  `signal.signal`, `runner.rb:98-99` `Signal.trap`, `Runner.php:190-191` `pcntl_signal`.
- **Evidence** (2026-09-01, supervisor's own runs after the worker's):

      $ FLEZHA_E2E_RUNTIME=node mix test --include system --trace test/system/state_freshness_e2e_test.exs
      1) test SIGTERM to the runner populates Overwatch.state.snapshot via RunnerShutdownNotification
         controller's Overwatch.state.snapshot was never populated
      Result: 0/1 passed        (5172.7ms — the test's whole wait)
      $ FLEZHA_E2E_RUNTIME=python mix test --include system --trace test/system/state_freshness_e2e_test.exs
      Result: 0/1 passed        (5201.4ms)
      $ FLEZHA_E2E_RUNTIME=rust  … → 1 passed (234.1ms), stderr: [flezha] SIGTERM received; running graceful shutdown

  Under node the runner writes nothing to stderr for the whole test. `strace -f -e
  trace=rt_sigaction` on the node runner: the core's `SA_SIGINFO` handler is installed first
  (`register_bridge`), then the main thread installs libuv's `SA_RESTART` handler over it
  (`uv_signal_start`, i.e. `process.on('SIGTERM')`); libuv does not chain.
- **The mechanism, named to the function.** On SIGTERM only the veneer's `stop()` runs:
  `native.shutdown()` = `flz_core_shutdown`, then the host stops its loops and frees the
  context. None of those reaches `run_active_graceful_shutdown`, whose callers are the core's
  signal thread, `exit_terminally`, and the controller-ordered disconnect
  (`command/mod.rs:250`). The bridge itself IS wired for the erased host —
  `coordinator.rs:275` `install_snapshot_fn` over `serialize_operative_state`, `:316`
  `set_control_stream` — so the gap is the stop path alone. Rust is spared twice: its process
  has no second sigaction, and `FlezhaRunner::drop` (`runner_handle.rs:564`) runs the bridge.
- **Why the obvious one-liner is wrong.** Calling `run_active_graceful_shutdown()` inside
  `flz_core_shutdown` before `poll_core.shutdown()` runs the `migration_serialize` host
  command synchronously on the thread that called `native.shutdown()` — in node's
  single-loop configuration that IS the loop that must answer it: a deadlock bounded only by
  `HOST_COMMAND_TIMEOUT` (30 s, `poll_core/mod.rs:604`).
- **The spec.** `openspec/specs/runner-controller-connection-healing/spec.md:144` SHALLs the
  notification for "the Rust/Java SDK" and `:148` lets SDKs that do not send it fall back to
  periodic snapshots — written before four more SDKs existed. The conformance rule
  (`sdk-conformance/spec.md:88`, no third state) makes this scenario a `state_persistence`
  proof, and the programme's standing instruction is that every SDK behaves the same.
- **A fix must** run the bridge OFF the caller's thread from the one FFI entry all four
  veneers already call — take `runner_tx` out of the context first, spawn a thread that runs
  `run_active_graceful_shutdown()` (the bridge latches) and only then
  `poll_core.shutdown()` — so the host loop stays free to answer `migration_serialize`; no
  context pointer may cross into that thread. Rebuild every veneer's own `flz_core_ffi`
  artefact before re-running an arm. Make the spec requirement SDK-agnostic, keeping the
  fallback for an SDK that defers persistence by name (Elixir).
- **Verdict (supervisor, 2026-09-01)**: real, worth fixing now — it blocks R5 S2's manifest
  flip. Fixed first as `ship-shutdown-notification-from-erased-hosts`, its own commit,
  red-first on the node and python arms above; ruby and php are proved when their S2 batches
  flip the manifests.
- **Resolved**: `4e20aeb2`, 2026-09-02 — `flz_core_shutdown` runs the bridge off the caller's thread before the poll core stops; every host (node, python, ruby, php, java) raises its stop flag only after the core's word; the shutdown-time `Reconnect` send, the `runner_tx` field and its take are deleted. Red-first at the ABI (notification awaited; connection count unchanged after the ack) and on the node/python `state_freshness` arms.


### KI-113 · `test/apps/testnet` installs with `npm ci || npm install`, under a comment promising reproducibility — RESOLVED

- **Severity**: P3 · the fallback fires exactly when the lockfile has drifted,
  which is the one case the lockfile exists for. Found by
  `pin-sdk-test-toolchains`'s survey, which covers the SDKs and not this tree.
- **Where**: three sites on one tree, **two of them merge gates** —
  `.github/workflows/elixir.yml:114`, `.github/workflows/java-sdk.yml:158` and
  `scripts/ci-local.sh:125`. The first carries a comment reading *"Equivalent to
  `mix test.setup` (mix.exs alias) but with `npm ci` for reproducible CI
  installs"*, and the command under it was:

  ```
      working-directory: test/apps/testnet
      run: |
        npm ci || npm install
        npm run build
  ```

  A fourth site on the same tree, in the same family:
  `docker/dev/Dockerfile.runtime:32` ran `npm install --omit=dev`, reading no
  lockfile at all.
- **Evidence**: the lockfile is tracked (`git ls-files` lists
  `test/apps/testnet/package-lock.json`) and `package.json` declares
  `typescript ^5.7.2`, `ws ^8.18.0` and `@types/ws ^8.5.13` — three carets whose
  resolution the lock is the only thing pinning.
- **Mechanism**: `npm ci` fails when the lockfile disagrees with `package.json`,
  and that failure is the signal. `|| npm install` converts it into a silent
  fresh resolution that rewrites the lock, which CI then discards — so the tree
  that was built is recorded nowhere, and the comment above it says the opposite.
  It is the same defect class `pin-sdk-test-toolchains` fixes in
  `scripts/build-node-sdk.sh`, and the worse half of it: there the pin was read
  by nothing, here the lockfile is read and then bypassed by a fallback that
  reads as correct.
- **A fix must** drop the `|| npm install` at every site so a stale lockfile
  fails loudly, and make the Dockerfile's install honour the same lockfile. If
  the fallback exists because the lock is known to drift, then the lock is what
  needs fixing and the comment is what needs deleting — a fallback that hides
  drift is not a remedy for it.
- **RESOLVED 2026-09-01** by `pin-sdk-test-toolchains`. All three fallback
  sites are a bare `npm ci` and `docker/dev/Dockerfile.runtime:32` is
  `npm ci --omit=dev`; each carries the reason beside it, and the Dockerfile's
  existing note on why `--omit=dev` keeps typescript is still true and stays.
  **Two of the three are merge gates, so the fix was driven before it was made
  rather than reasoned about** — `npm ci --dry-run` in `test/apps/testnet`:

  ```
  $ pwd
  /home/moon/Code/flezha/test/apps/testnet
  $ npm ci --dry-run
  up to date in 221ms
  $ echo $?
  0
  ```

  The committed lockfile is current, so removing the fallback cannot redden a
  gate: every run that reached `npm install` through the `||` would have been a
  run `npm ci` was about to serve anyway.
- **The spec now covers it, and did not before.** The requirement
  `pin-sdk-test-toolchains` adds to `code-quality-baseline` was written about
  *an SDK's* test toolchain, and `test/apps/testnet` is a test application, so
  the rule as first drafted would not have caught the defect it was filed
  beside. It carries an explicit clause instead — *where a lockfile is
  committed, the installer SHALL honour it and SHALL NOT fall back to a
  resolving install* — scoped to every directory that commits a lockfile,
  including a test application or worked example that is not itself an SDK.
- **The Dockerfile site was very nearly left standing, and that would have been
  the wrong call.** It was first reported as adjacent-but-not-this-defect, on
  the grounds that it builds a runtime image and has no fallback to remove. The
  supervisor refused the distinction and was right to: the clause this change
  lands reads *"where a lockfile is committed, the installer SHALL honour it"*,
  `test/apps/testnet` commits one, and `npm install` is a resolving install — so
  the site violated the requirement the same commit was introducing. **A change
  must not land a rule its own repository breaks at a site the change's own
  entry already names.** In a container the discarded rewrite is the harm the
  clause names, exactly: what shipped in the image is recorded nowhere.
- **This is a worse shape than the three defects the change was opened for, not
  a lesser one.** Those had a pin nothing read. This had a lockfile *and* a
  silent bypass: `npm ci || npm install` reads as correct, and re-resolves
  exactly when `ci` fails — which is exactly when the failure was worth
  hearing.

### KI-112 · `async` is an undeclared dependency of both the shipped fiber executor and the suite — RESOLVED, and the consumer-facing half of it is REFUTED

- **Severity**: P2 · a documented, advertised executor does not work on a fresh
  `gem install flezha`, and the suite has a hard dependency named in no manifest.
  Found by KI-110's fix; filed rather than fixed because whether it is a runtime or a
  development dependency is a support-matrix call.
- **Where**: `sdk/ruby/flezha.gemspec` names `async` nowhere.
  `sdk/ruby/lib/flezha/executor.rb:312` requires it lazily inside
  `FiberExecutor#build_scheduler`; `sdk/ruby/lib/flezha/runner.rb:166` reaches that
  from `Runner#executor(:fibers)`, and `:39` from `FLEZHA_RUBY_EXECUTOR=fibers`.
  `docs/sdk/ruby_sdk.md:202` advertises `:fibers` for I/O-heavy handlers and does not
  say the gem is needed; `test/apps/ruby_runner/conformance.json:31` advertises the
  variable too. The suite's `test/concurrency_test.rb:120`–`:142` builds a
  `FiberExecutor` with no `scheduler:`, so it needs the real gem.
- **Evidence** — driven on rbenv 3.4.10 with a shim making `require "async"` raise
  `LoadError`, which is what its absence looks like to the rescue:

      # the shipped library path
      Flezha::Error -> the fiber executor needs a Fiber::Scheduler. Add the `async`
      gem, or pass `scheduler:` a callable answering one.
      # the scheduler: override, which never reaches async
      scheduler: -> :a_scheduler

      # the suite
      $ rake test
      [flezha] the fiber dispatcher ended on Flezha::Error: the fiber executor needs …
      82 runs, 276 assertions, 1 failures, 0 errors, 0 skips

  The one failure is
  `test_both_executors_complete_every_activation_and_keep_item_order`, at
  `assert wait_until { core.completed.size == 8 }, "fibers completed everything"`.
- **Mechanism, and what it is NOT**: `async` is reachable from shipped code, so it is a
  **runtime** dependency in the strict sense — but an optional and guarded one. The
  default executor is `:threads` (`runner.rb:39`), the require is lazy, the failure is
  a named `Flezha::Error` carrying its own remedy, and `FiberExecutor.new(…,
  scheduler:)` bypasses `async` entirely. So a consumer is never silently broken; a
  consumer who takes a documented option is told, at boot, to install a gem the
  documentation never mentioned. It is simultaneously a **hard development**
  dependency, because the suite exercises the fiber arm for real.
- **A fix must** choose, and say which. A gemspec has no optional-dependency
  mechanism — no composer `suggest`, no Python extra — so the two honest shapes are:
  declare it as a **development** dependency pinned like `minitest` and `rake`, and add
  the install line to `docs/sdk/ruby_sdk.md`'s executor table so the advertised option
  is not advertised bare; or make it a **runtime** dependency, which forces `async` on
  every `:threads` consumer and contradicts the deliberate lazy require. The first is
  recommended. It must NOT be left as it is on the grounds that the error message is
  good: a documented feature that needs an undocumented gem is a documentation defect
  even when the runtime behaves well.

- **CORRECTED 2026-09-01 — the consumer-facing half is REFUTED by reading the page.**
  This entry claimed that `docs/sdk/ruby_sdk.md:202` advertises `:fibers` and that
  "neither says a gem is needed — a documented feature that does not work on a fresh
  `gem install flezha`". That is wrong. The paragraph **directly beneath that table**
  states the requirement:

  > The fiber executor keeps a real thread fetching … and hands activations to fibers
  > through a `Thread::Queue`, which is scheduler-aware. It needs a `Fiber::Scheduler`:
  > add the `async` gem, or pass `executor(:fibers, scheduler: -> { MyScheduler.new })`.

  It names the gem, names the alternative, and matches the `Flezha::Error` the shim
  drill above produced almost word for word. **A lazy require, plus a guarded error
  carrying its own remedy, plus a doc line stating the requirement, is a correctly
  designed optional dependency and not a defect.** The `conformance.json:31` half of
  the claim does not survive either: a capability manifest advertises a variable to the
  conformance harness and carries no install-documentation burden.
  **The error was mine and it was a search error** — the grep behind the claim looked
  for `gem .async` and the page reads "add the `async` gem", with the words the other
  way round, so the one line that refutes the claim is the one line the sweep could not
  match. A negative grep is not a reading.
- **What survives is the development-dependency half, and it is real**: the suite
  cannot run without `async` (`82 runs, 276 assertions, 1 failures` above), and the
  manifest named it nowhere. That is the same defect as `rake` and one line from it.
- **RESOLVED 2026-09-01** by `spec.add_development_dependency "async", "2.45.0"`, with
  `.github/workflows/ruby-sdk.yml` installing it through the same `pin` helper that
  reads the `rake` and `minitest` pins. **Development, not runtime, deliberately**: a
  runtime declaration would force `async` on every `:threads` consumer and contradict
  the lazy require the SDK designed on purpose. The gemspec says so beside the line,
  because promoting it to a runtime dependency is the obvious wrong fix.


### KI-111 · Only the Ruby SDK's dependency manifest is inside a `prod_loc` ratchet — RESOLVED

- **Severity**: P2 · declaring a dependency costs a blocking production line in
  `sdk/ruby` and costs nothing in the other five SDKs, so the gate charges one
  language for a hygiene fix it charges no one else for. Found by KI-110's fix,
  which it blocks.
- **Where**: `scripts/refactor_metrics.py:68`, `"ruby": "rb gemspec"`, and `:101`,
  which maps a bare `Rakefile` to Ruby as well. Every other SDK writes its manifest
  in JSON, TOML or XML, none of which has an entry in `LANG_EXTS`, so `language()`
  answers `None` and the file is never counted.
- **Evidence**, at `5e5f4404` plus KI-110's one added `add_development_dependency`
  line:

      $ python3 scripts/refactor_metrics.py --check
      BLOCKING sdk/ruby prod_loc: 1955 -> 1956  [sdk/ruby/flezha.gemspec (+1)]
      1 regression(s) against 4cd41f5e; exit 1

      $ python3 -c 'import sys; sys.path.insert(0, "scripts"); import refactor_metrics as m
      for f in ["sdk/ruby/flezha.gemspec", "sdk/node/package.json",
                "sdk/python/pyproject.toml", "sdk/php/composer.json",
                "sdk/java/pom.xml", "sdk/rust/Cargo.toml"]:
          print(f, m.language(f))'
      sdk/ruby/flezha.gemspec    ruby
      sdk/node/package.json      None
      sdk/python/pyproject.toml  None
      sdk/php/composer.json      None
      sdk/java/pom.xml           None
      sdk/rust/Cargo.toml        None

- **Mechanism**: `prod_loc` is summed over the area's own tracked files and the
  classifier reads a file's language from its extension. `.gemspec` is Ruby by
  extension and is counted; a manifest in a data format is invisible. Nothing here
  is wrong for `lib/**` — the asymmetry is only that one of the six SDKs writes its
  manifest in the language the metric measures.
- **Charter §11 gives no exemption.** The one accepted rise is code arriving from a
  higher layer against a larger deletion elsewhere, and the section closes "nothing
  else exempts a blocking area". So the gate blocks a one-line dependency
  declaration that changes no behaviour and cannot be funded: a `.gemspec` cannot
  declare a dependency in zero lines.
- **A fix must** choose between two, and say which: exclude a dependency manifest
  from `prod_loc` — `flezha.gemspec` and `Rakefile` are packaging rather than the
  SDK's production surface, which moves `sdk/ruby`'s number and so needs a
  re-baseline — or accept the row and re-baseline `sdk/ruby prod_loc` at the close.
  It must NOT be answered by deleting production Ruby to fund a dependency line.

- **RESOLVED 2026-09-01 by supervisor ruling: the instrument was wrong, so it is a
  correction and not an exemption.** `scripts/refactor_metrics.py:68` now reads
  `"ruby": "rb"`. The other five SDKs' manifests were already outside the metric, so
  the intended convention was unambiguous and Ruby was the outlier; this is the call
  the programme already made for area 4's batch 0 and for area 6's 75 lines of
  classifier artefact, and charter §11e exists because the instrument can be wrong.
  Here it did not flatter, it penalised.
- **Red first, then green.**

      before   LANG_EXTS["ruby"] == "rb gemspec"
               language("sdk/ruby/flezha.gemspec") -> "ruby"
      $ python3 scripts/refactor_metrics.py --check
      BLOCKING sdk/ruby prod_loc: 1955 -> 1956  [sdk/ruby/flezha.gemspec (+1)]
      1 regression(s) against 4cd41f5e; exit 1

      after    LANG_EXTS["ruby"] == "rb"
               language("sdk/ruby/flezha.gemspec") -> None
               language("sdk/ruby/Rakefile")       -> "ruby"   (unchanged: :101 maps it by basename)
      $ python3 scripts/refactor_metrics.py --check
      0 regression(s) against 4cd41f5e; exit 0

  `sdk/ruby prod_loc` reads **1921** against the recorded 1955 — the gemspec's 34
  counted lines leave, and the two declarations added beside them cost nothing. A
  decrease is not a regression, so `--check` passes with the baseline left stale-high;
  re-recording it belongs to the close, and `baseline.json` is untouched.
- **Scope, checked**: `LANG_EXTS` feeds only `LANG_BY_EXT` in the same file, the two
  other `gemspec` mentions under `scripts/` are a comment and a path string in
  `format-apply.sh`, and exactly one `.gemspec` exists in the tree.

### KI-110 · The Ruby SDK's test suite depends on minitest without declaring or pinning it — RESOLVED

- **Severity**: P2 · the suite passes here by accident of which minitest the ambient
  Ruby ships. It has already failed on `main` for exactly this reason, and nothing
  stops the same resolution reaching `dev`.
- **Where**: `sdk/ruby/flezha.gemspec` declares `msgpack` and pins
  `rubocop` to an exact `1.90.0` — and declares **no test dependency at all**.
  There is **no `Gemfile.lock`**. `sdk/ruby/test/runner_test.rb:21` and `:51` call
  `Process.stub(...)`, which is `Object#stub` from `minitest/mock`, and no file in
  `sdk/ruby/` requires `minitest/mock`.
- **Evidence** — it passes here only because this minitest's `autorun` happens to
  load `mock`:

      $ cd sdk/ruby && PATH=~/.rbenv/versions/3.4.10/bin:$PATH rake test
      82 runs, 277 assertions, 0 failures, 0 errors, 0 skips

      $ ruby -e 'require "minitest"; puts Minitest::VERSION'
      5.25.4
      $ ruby -e 'require "minitest/autorun"; puts Process.respond_to?(:stub)'
      true

      $ grep -rn 'minitest/mock' sdk/ruby/          # no output
      $ grep -n 'dependency' sdk/ruby/flezha.gemspec
      35:  spec.add_dependency "msgpack", "~> 1.7"
      36:  spec.add_development_dependency "rubocop", "1.90.0"

  The recorded `main` failure is the same code meeting a resolution where `autorun`
  does not pull in `mock`:
  `NoMethodError: undefined method 'stub' for module Process`.
- **Why this is not a `main`-only problem**: nothing here is version-gated in our
  favour. The suite works on whatever minitest the runner's Ruby happens to install,
  and neither the gemspec nor a lockfile constrains it. A CI image refresh reaches
  `dev` the same way it reached `main`.
- **A fix must** declare minitest as a development dependency and pin it the way
  `rubocop` already is, AND add the explicit `require "minitest/mock"` where `stub`
  is used — the require is what makes the code honest about what it needs, and the
  pin is what stops the resolution drifting. Prove it by resolving against a
  minitest whose `autorun` does not load `mock` and showing the suite still green.

- **RESOLVED 2026-09-01.** `sdk/ruby/flezha.gemspec` declares
  `spec.add_development_dependency "minitest", "5.25.4"` — exact, like the `rubocop`
  line beside it, because nothing else constrains the resolution — and
  `.github/workflows/ruby-sdk.yml` installs the version that line names, via the
  `grep -oP '"minitest", "\K[^"]+'` extraction `format.yml` already uses for rubocop.
  `sdk/ruby/test/runner_test.rb:6` requires `minitest/mock` explicitly.
- **The entry understates the mechanism, and the pin is the load-bearing half.**
  minitest **6.0** does not merely stop loading `mock` from `autorun`: it ships no
  `lib/minitest/mock.rb` at all, mock having been extracted into a separate
  `minitest-mock` gem (published at 5.27.0).

      $ gem unpack minitest-6.0.6.gem && cat minitest-6.0.6/lib/minitest/autorun.rb
      require_relative "../minitest"
      require_relative "spec"
      require_relative "hell" if ENV["MT_HELL"]
      Minitest.autorun
      $ ls minitest-6.0.6/lib/minitest/mock.rb
      ls: cannot access ...: No such file or directory

  On the resolution that broke `main`, therefore, the explicit require alone cannot
  help. Driven for real — minitest 6.0.6 installed into an isolated `GEM_HOME` and
  `rake test` run against it — the suite stops at load with
  `cannot load such file -- minitest/mock (LoadError)` from `test/runner_test.rb:6`,
  and installing the separate `minitest-mock` gem does not lift it either unless the
  gem is also activated explicitly, because the resolved `minitest` gem already owns
  the `minitest/` namespace. So what the require buys is a failure that names the
  missing library instead of a missing method on `Process`, plus correctness on any
  5.x whose `autorun` drops the line; what the pin buys is that the suite runs at all.
- **Proof — the ambient convenience removed.** `minitest/autorun` was replaced on
  `$LOAD_PATH` by minitest 6's own autorun shape (`require "minitest"; require
  "minitest/spec"; Minitest.autorun`), first verified to leave
  `Process.respond_to?(:stub)` **false**. Full `rake test` on rbenv 3.4.10, four cells:

  | | `require "minitest/mock"` present | removed |
  | --- | --- | --- |
  | this box's real `autorun`, which loads mock | 82 runs, 277 assertions, 0 failures | **82 runs, 277 assertions, 0 failures** |
  | an `autorun` that does not load mock | **82 runs, 277 assertions, 0 failures** | 3 × `NoMethodError: undefined method 'stub' for module Process` |

  The bottom-left cell is the fix. The **top-right** cell is why a green run on this
  box proved nothing — the defect is invisible here. The bottom-right reproduces the
  recorded `main` error verbatim.
- **`--check` is red by one line on the fix itself** — `sdk/ruby prod_loc 1955 → 1956`,
  the added `add_development_dependency` — because `flezha.gemspec` is the only SDK
  dependency manifest inside a `prod_loc` ratchet. Filed as KI-111; not answered here,
  because charter §11 offers no exemption and the only funding available would be
  deleting production Ruby.
- **`rake` was the same defect in the same file and is fixed here too**:
  `spec.add_development_dependency "rake", "13.2.1"`, and the workflow reads both pins
  through one `pin()` helper rather than repeating the grep. Once KI-111's classifier
  correction landed, the second declaration cost zero production lines.
- **`async` was the third one, and is fixed here too — KI-112.** It is reachable from
  the shipped library and not only from tests, but it is guarded and documented, so it
  is declared as a *development* dependency and deliberately not a runtime one.
- **The cross-SDK survey this row was also asked for**, recorded here because the row
  writes no dev note (no OpenSpec change: see the ruling below). Manifest, pin style,
  lockfile, and whether any test calls an API from a library it never requires:

  | SDK | framework declared | pinned or ranged | lockfile committed | undeclared API in a test | CI install |
  | --- | --- | --- | --- | --- | --- |
  | `sdk/node` | n/a, stdlib `node:test` | jscpd/prettier exact; `typescript`, `@types/node` caret | yes, `package-lock.json` | no | `build-node-sdk.sh` runs `npm install`, **not `npm ci`**, so the lock is bypassed on the test path |
  | `sdk/python` | yes, `pytest>=8.0` in `[dependency-groups] dev` | **ranged**; only `ruff==0.16.5` exact | **none** | no | **`pip install --quiet pytest pyright` — unpinned, and it never reads the declared group** |
  | `sdk/php` | yes, `phpunit/phpunit ^10.5` in `require-dev` | **ranged**; php-cs-fixer exact | **`composer.lock` is gitignored** | `Swoole\Thread` is in `suggest`, not `require-dev` — guarded, asserted on purpose in `PackagingTest`, covered by a ZTS job; not a defect | `composer install` with no lock is effectively `composer update` |
  | `sdk/java` | yes, JUnit/AssertJ/Mockito/slf4j, `scope=test` in the parent's `dependencyManagement` | **exact** (5.10.3 / 3.26.3 / 5.12.0 / 2.0.13) | n/a for Maven; every version pinned in the parent | no — zero hamcrest in the tree, so no transitive reliance | `mvn -B … verify` |
  | `sdk/rust` | yes, criterion/trybuild/loom in `[dev-dependencies]` | caret | yes, root `Cargo.lock` | no | pinned toolchain, cache keyed on `Cargo.lock` |
  | `sdk/ruby` | **was: none at all** | now exact for minitest, rake, rubocop | **none**, and correctly so for a gem | **yes — this entry** | reads the gemspec's pins |

  **No other SDK repeats the exact shape** (a test calling an API from a library named
  in no manifest anywhere). Node, Java and Rust are clean. Python is the closest
  relative: its declared dev group is PEP 735, which only `uv` and pip ≥ 25.1 read, and
  **nothing installs it** — `build-python-sdk.sh` installs only `msgpack` and the
  workflow installs `pytest pyright` unpinned, so as in Ruby the CI line is the
  effective manifest; `tests/test_typing.py:26` also tells the reader to run
  `pip install -e .[dev]`, which fails, because no `[project.optional-dependencies] dev`
  exists. Two inverse findings, free: `mockito-core` is declared in
  `flezha-sdk-runtime/pom.xml` and used by **zero** test classes, and `sdk/rust`, `cli`
  and `test/apps/rust_runner` each commit a `Cargo.lock` that cargo ignores because they
  are workspace members — three dead lockfiles beside the live root one.
- **No OpenSpec change, and the absence is the finding.** No spec states that an SDK
  declares and pins its own test toolchain: `ruby-sdk`, `ruby-sdk-packaging` and
  `ruby-sdk-testing` say nothing about dependencies, and across all spec directories
  only `controller-packaging` and `code-formatting` mention pinning at all —
  `code-formatting` stating the identical principle for the Rust formatter
  ("the same pinned version SHALL be the one the local format script invokes"). A
  correctly worded requirement is **cross-SDK** and three SDKs would fail it today per
  the table above, so it is queued as `pin-sdk-test-toolchains` with this row as the
  reference implementation, rather than written Ruby-shaped here.

### KI-107 · `scripts/format-check.sh` is red at HEAD on `cli_manager.ex` — RESOLVED

- **Severity**: P3 · a merge-gating formatter check that fails on a file no
  in-flight change touches, which trains people to ignore its output.
- **Where**: `lib/flezha/service/cli_manager.ex:89`–`:91`, a `defdelegate` whose
  `), to: Cli.Schedule` the formatter wants split across two lines.
- **Evidence**, on a tree whose only modifications are `docs/refactor/STATE.md`
  and `close-kind-naming-gate-red`'s own files:

  ```
  $ bash scripts/format-check.sh
  ...
  UNFORMATTED — run scripts/format-apply.sh:
    mix format
    php-cs-fixer
  FAILED — the output above says why:
    formatters (scripts/format-apply.sh --check)
  EXIT=1

  $ mix format --check-formatted
  ** (Mix) mix format failed due to --check-formatted.
  The following files are not formatted:
      /home/moon/Code/flezha/lib/flezha/service/cli_manager.ex
   89  89  |                arg7 \\ "workflow",
   90  90  |                arg8 \\ ""
   91     -|              ), to: Cli.Schedule
       91 +|              ),
       92 +|              to: Cli.Schedule

  $ git status --short lib/flezha/service/cli_manager.ex
  (empty — the file is exactly as committed)
  ```

  The useful half: **it reproduces with every file of the change in flight
  reverted**, and the file is unmodified in the working tree, so it is committed
  in this state.
- **Mechanism, and the reason it was NOT fixed on the spot**: `cli_manager.ex`
  lost a delegate in `retire-cluster-snapshot-remnants`, which is the most
  likely origin of the re-wrap. But this box runs Elixir 1.20 and CI runs 1.19,
  and the standing rule is never to run a bare `mix format` here — a local
  reformat can produce a diff CI then rejects. Deciding this needs the CI
  formatter's own verdict, not this box's.
- **The other half of that red is already recorded**: `php-cs-fixer` dies on a
  parse error inside `sdk/php/vendor/symfony/`, filed separately, and the diff
  contains no PHP.
- **A fix must** establish which Elixir version's formatting is authoritative
  before editing the file — reproduce under CI's 1.19, then apply that
  formatting — and must not reformat the file on a 1.20 toolchain and assume CI
  agrees. It is three lines in `lib/`, so it is a behaviour-neutral edit inside a
  finished area and belongs to whoever owns that lane, not to a naming-gate
  change.
- **CORRECTED 2026-09-01 (`collapse-observability-source-behaviours`): there is no
  version question. CI does not run 1.19.** Every workflow pins the same toolchain
  this box runs:

  ```
  $ grep -rn "elixir-version" .github/workflows/
  bench.yml:42,190  rust-sdk-bench-cron.yml:32  elixir.yml:62
  java-sdk.yml:109  format.yml:53  nightly-e2e.yml:129
  → all "1.20.2"

  $ elixir --version
  Elixir 1.20.2 (compiled with Erlang/OTP 29)
  ```

  `format.yml:53` is the pin the Format gate itself uses, so the verdict this
  entry defers to has already been given: the file is unformatted under the
  authoritative formatter and the Format workflow is red on `dev`. The blocker
  named in **A fix must** is therefore discharged — the remedy is the two-line
  re-wrap the formatter prints, and what remains is only whose lane it is.

- **Resolved 2026-09-01 by row 3.5's format fix.** `mix format
  lib/flezha/service/cli_manager.ex` — that file and no other; a bare `mix
  format` over the tree is still forbidden here. The diff is the two lines the
  formatter printed above and nothing else, proved twice: with every whitespace
  character stripped the file's sha256 is identical on both sides
  (`1e8928d4aab1fe25193b03df52bb765e6d805921ee32839c729813b3971741eb`), and the
  metadata-stripped `Code.string_to_quoted!` ASTs compare equal.
- **Exit codes, captured directly.** `$?` after a pipeline is the *last stage's*
  status, so `mix format --check-formatted … | tail -5` reports `tail`'s 0
  whatever the formatter said. That is how the earlier "it exits 0" reading was
  produced; measure it without a pipe:

  ```
  $ mix format --check-formatted lib/flezha/service/cli_manager.ex >/dev/null 2>&1; echo $?
  1        # at 787acf3f
  0        # after the re-wrap
  ```

  And the gate itself with all seven formatters present — rubocop 1.90.0 from
  `~/.rbenv/versions/3.4.10/bin`, PHP through the `flezha-php` image, which is
  the arm CI reproduces:

  ```
  $ PATH="$HOME/.rbenv/versions/3.4.10/bin:$PATH" \
    FLEZHA_PHP="docker run --rm -u $(id -u):$(id -g) -v $PWD:/work -w /work flezha-php php" \
    scripts/format-apply.sh --check; echo $?

  # before                          # after
  UNFORMATTED — run …:              every check ran and had nothing to report
    mix format
  1                                 0
  ```

- **The origin named under *Mechanism* above is wrong.** The re-wrap did not
  come from `retire-cluster-snapshot-remnants`. `git log -S '), to: Cli.Schedule'
  -- lib/flezha/service/cli_manager.ex` names exactly one commit, `716061cf`
  *refactor(5c): cli_manager 4294 lines becomes fifteen modules and a facade*,
  which wrote the `defdelegate` in that shape. What
  `retire-cluster-snapshot-remnants` did was delete `boot_import_legacy_snapshot`
  fifty lines above it, which is why the two looked related.
- **The version blocker in *A fix must* was false**, as the correction above
  records, and nothing was waiting on CI: `format.yml:53` pins 1.20.2, this box
  runs 1.20.2, and the local formatter *is* the authoritative one.
- **The other half of the local red is KI-76, and it is a tool error rather than
  an unformatted file** — settled here rather than assumed. Under the host `php`
  8.3.6 `php-cs-fixer` exits **255** on
  `sdk/php/vendor/symfony/console/Command/Command.php:681`, and `gate_run` files
  a non-zero exit under `UNFORMATTED`; under the 8.4.25 interpreter the vendor
  tree was installed for, the same run reports `Found 0 of 89 files that can be
  fixed`. `format.yml` installs PHP 8.4 and runs `composer install`, so this
  never reaches CI and the gate can reach 0 there.
- **Nothing else in the tree is unformatted.** All seven formatters ran in
  `--check` on the fixed tree: cargo fmt, mix format and mvn spotless silent,
  prettier `All matched files use Prettier code style!`, ruff `37 files already
  formatted`, rubocop `32 files inspected, no offenses detected`, php-cs-fixer
  `0 of 89`.
- **The blocking ratchet is unmoved.** `lib/flezha/service` is a blocking area;
  the re-wrap takes `cli_manager.ex` from 94 to 95 production lines and the area
  from 5778 to 5779 against a baseline of 5828, so
  `scripts/refactor_metrics.py --check` exits 0 with 49 lines of slack.
- **No OpenSpec change.** `openspec/specs/code-formatting/spec.md` already
  carries the rule this restores — *Requirement: Project SHALL enforce `mix
  format` in CI*, whose scenario is that an unformatted `*.ex` blocks merge.
  The gate behaved exactly as specified; what was wrong was the file. A delta
  would have restated a requirement that is already written down.

### KI-108 · A one-line migration-record pair blinds a naming gate to the rest of the file — RESOLVED

- **Severity**: P2 · three merge-gating naming checks stop reading six files
  partway through, silently, and the exemption that does it looks correct.
- **Where**: `scripts/naming_gate.py:104`–`:113`, the per-line loop inside
  `walk/1`. Live instances (a line carrying BOTH markers):
  `docs/dev_notes/close-overflow-silent-failures.md:200`,
  `docs/dev_notes/assertions-that-cannot-fail.md:5`,
  `docs/dev_notes/finish-platform-authn-authz.md:54`,
  `docs/dev_notes/fix-first-input-drop-on-bind.md:153`,
  `docs/known_issues.md:2485` and `:3966`.
- **Evidence**, `rename-kafka-queue-overflow`'s tree, the same violating line
  placed either side of `close-overflow-silent-failures.md:200` and the file
  restored byte-identical after:

  ```
  probe = "The push is refused under `OverflowPolicy::Block` policy."

  A  probe appended AFTER  line 200 -> exit 0, 'no hit in this file'
  B  probe inserted at line 151     -> exit 1,
     'docs/dev_notes/close-overflow-silent-failures.md:151: The push is
      refused under `OverflowPolicy::Block` policy.'
  restored, identical: True
  ```

  Arm A is the defect: lines 201–279 of that file are unchecked by every
  record-tagged gate, and the gate says `overflow naming OK`.
- **Mechanism, named to the line**: `walk/1` tests `begin in line` first and
  `continue`s, so the `end in line` branch below it never runs for a line that
  carries both. `in_record` latches `True` and is cleared only when the next
  file starts. The inline form is the one used in prose, where a marker pair
  around a phrase reads better than two lines around a paragraph — so the
  shape that breaks it is the shape a writer reaches for.
- **A fix must** make a line carrying both markers a self-contained record —
  skipped, with `in_record` unchanged — rather than an opening one, and be
  shown red first with the probe above at `close-overflow-silent-failures.md`.
  It must then re-run all five naming checks over the six files above, because
  each has an unread tail today and any of those tails may hold a real hit.
- **Not fixed on the spot**: it is not the rename this session owns, and the
  fix changes what three merge gates read — the verdict is the supervisor's.

- **Resolved 2026-09-01 by `fix-record-block-latch`.** `walk/1` reads both
  markers before deciding, and only a line carrying exactly one of them moves
  the block state — so a line carrying both exempts itself and leaves the
  surrounding block alone. A block left open at end of file is refused by name
  as well, because it reaches the same blindness by the other route; zero
  tracked files are open today, so that half is armed rather than owed.
- **Three corrections to the entry above.** The instances are **six across five
  walked files**, not six files: the sixth,
  `scripts/kind-naming-allowlist.txt:30`, is a `self_files` entry for
  `check-kind-naming` and is never opened, so the kind gate lost nothing at all.
  The two lines in this file are `:2527` and `:4008` at `1825c053`, not `:2485`
  and `:3966`. And **this file is the mild case, not the severe one**: its later
  record blocks carry `:end` markers that clear the latch, so the blinded region
  here is `2528`–`2540` and `4009`–EOF, **18 lines**. The severe case is a dev
  note whose inline pair sits near the top with nothing below to close it —
  `assertions-that-cannot-fail.md` was read **4 lines out of 159**.
- **What the unread tails held: nothing.** With the ordering corrected all five
  naming checks still exit 0. This was a **latent** defect, and that is the
  reason it was worth fixing rather than a reason it was not: a gate printed
  `OK` for two different reasons — nothing is wrong, or it stopped looking — and
  the two were indistinguishable from outside.
- **Coverage recovered**, counted by wrapping `check` and running the three
  record-tagged gates against the committed module and against the fixed one:

  | gate | file | lines read before → after |
  |---|---|---|
  | module | `docs/dev_notes/assertions-that-cannot-fail.md` | 4 → 159 |
  | module | `docs/dev_notes/finish-platform-authn-authz.md` | 53 → 126 |
  | module | `docs/dev_notes/fix-first-input-drop-on-bind.md` | 152 → 263 |
  | module | `docs/known_issues.md` | 3938 → 3956 |
  | overflow | `docs/dev_notes/close-overflow-silent-failures.md` | 199 → 278 |

  **436 lines** across five files, over three merge-gating checks. The kind gate
  is unchanged, for the `self_files` reason above.
- **Red first, on this entry's own probe**, run before any edit and again after,
  with the file restored byte-identical each time:

  ```
  probe appended AFTER line 200
    before the fix -> exit 0, "overflow naming OK"
    after  the fix -> exit 1, "…close-overflow-silent-failures.md:210: The push
                               is refused under `OverflowPolicy::Block` policy."
  probe inserted at line 151
    before and after -> exit 1, naming :151
  the file unmodified
    after the fix -> exit 0 — the inline pair still exempts its own line
  ```

- **The regression proof is automated, not a drill.** Nothing in this repository
  tested `scripts/` — `test/scripts/` is manual shell drills against a running
  system and `sdk/python/tests/` is the Python SDK's own suite — so rather than
  build a harness for a one-line fix, `scripts/naming_gate.py` gained a
  `--self-test` that runs the real `walk` over a throwaway `git init` tree with
  four fixtures: a record written on one line, a record spanning two markers, a
  block left open, and a file with no markers. It runs as a sixth step of the
  existing `kind-naming` job. Against the unfixed `walk` it reports
  `expected ['block.md:4', 'inline.md:2', 'unclosed.md:1']` / `found
  ['block.md:4']` — only the shape that already worked — which is both halves of
  the change red in one run.
- **An inline pair stays legal.** Refusing it would remove the footgun outright
  and give every record one shape, at the cost of converting five live
  instances, four of which wrap a phrase *inside a sentence* and one of which is
  a table cell where a line break ends the row. That is a prose edit forced on
  correct prose by a regex, which is the false economy [[KI-94]] closed. The
  cost of the choice is two shapes for one mechanism; it is answered by the
  comment at the branch and by the `inline.md` fixture, which is the rule in
  executable form.

### KI-105 · A retired kind identifier survives in two shipped javadocs, hidden by the gate's own allowlist — RESOLVED

<!-- kind-naming-record:begin -->

- **Severity**: P3 · stale prose in a shipped SDK API file. No runtime effect —
  the identifier names a method that does not exist — but it is exactly the
  occurrence `scripts/check-kind-naming.sh` exists to catch, and the gate could
  not see it.
- **Where**: `sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/procedure/Procedure.java:47`
  and `:54`, two javadoc lines reading *"Used at registerUnbounded time for
  wiring checks."*
- **Evidence**:

  ```
  $ grep -rn registerUnbounded --include='*.java' sdk/ test/
  .../procedure/Procedure.java:47:    /** Compile-time typed Input class. Used at registerUnbounded time for wiring checks. */
  .../procedure/Procedure.java:54:    /** Compile-time typed Output class. Used at registerUnbounded time for wiring checks. */

  $ git ls-files | grep -v '^openspec/changes' | xargs grep -ln registerUnbounded
  scripts/kind-naming-allowlist.txt
  sdk/java/flezha-sdk-api/src/main/java/com/flezha/sdk/procedure/Procedure.java
  ```

  There is no such method. `Flezha.registerUnbounded` was a real entry point and
  the kind rename retired it — `openspec/changes/archive/`
  `2026-08-13-rename-operative-kinds-to-stream-and-workflow/rename-table.md:58`
  lists it among the Java camelCase twins to rename. The two javadocs were
  missed, and the same file's neighbouring line already says *"Stable identifier
  within the StreamOperative class"*, so the rename reached the prose either
  side of them.
- **Mechanism — why no gate saw it for the rename's whole lifetime**: the
  allowlist erases **destructively and in file order**, and its plain-English
  rule for the capacity sense carried a `time` branch. Against
  `... registerUnbounded time for wiring ...` that branch matched the substring
  `Unbounded time` and deleted it, cutting the identifier in half; the residue
  `registerUn` contains no `bounded`, so the line came out clean. The rule was
  written for "bounded time" and had no idea it was covering an identifier. Any
  allowlist rule that can match inside a word has this failure mode, which is why
  every bare-word rule in that file now carries `(?<![A-Za-z0-9_])`.
- **How it surfaced**: replacing the enumerated-noun rule with a shape rule that
  refuses to match inside an identifier left these two lines as the gate's only
  remaining hits.
- **A fix must** correct the two javadocs to name the surviving entry point, and
  then delete the `Procedure.java:registerUnbounded` line from
  `scripts/kind-naming-allowlist.txt` — the entry exists only to keep the gate
  green while the text is wrong, and leaving it behind would re-hide the next
  one. It is a two-word prose edit in `sdk/java/**`, which is why it is filed
  rather than taken.

- **Resolved 2026-09-01 by `close-kind-naming-gate-red`.** Both javadocs now read
  `registerStream`, which is the house spelling already used by
  `StreamOperativeConfigurationException.java:165` and `ProcedureCtx.java:190`
  (`{@code registerStream} time`) and is what `rename-table.md:13` maps
  `Unbounded` to. The same file's `:43` already said "Stable identifier within
  the StreamOperative class", so the rename had reached the prose either side of
  these two lines.
- **The allowlist exemption is DELETED, not kept.** Those lines are now covered
  by the gate for real; the check no longer has a path entry for that file, and
  `grep -ln registerUnbounded` over every tracked non-archive file returns no
  Java source.
- **Evidence — the residue that hid it**, reproduced against HEAD's allowlist
  before the fix. The plain-English rule's `time` branch matched inside the
  identifier:

  ```
  line    : /** Compile-time typed Input class. Used at registerUnbounded time for wiring checks. */
  residue : '/** Compile-time typed Input class. Used at registerUn for wiring checks. */'
  contains 'bounded'? -> False  => the gate PASSED on a retired kind identifier
  ```

  The rule was written for "bounded time" and had no idea it was covering an
  identifier. Every bare-word rule in the allowlist now carries
  `(?<![A-Za-z0-9_])` or a `\b`, which is what surfaced these two lines.
- **Scope**: two javadoc words. No signature, no annotation, no behaviour; the
  emitted API is untouched.

<!-- kind-naming-record:end -->

### KI-106 · `check-module-naming.sh` is red at HEAD, on a defect entry citing a renamed module — RESOLVED

<!-- module-naming-record:begin -->

- **Severity**: P3 · a second permanently-red naming gate, red for the same
  reason [[KI-94]] describes: writing a defect down trips the gate that defect
  is about.
- **Where**: `docs/known_issues.md`, the two lines inside [[KI-104]] that quote
  the old module path. At the commit this was found on they are `:2208` and
  `:2214`; they move whenever anything above them is edited, which is part of
  the problem.
- **Evidence**, at `f967b106` with only `docs/refactor/STATE.md` modified:

  ```
  $ ./scripts/check-module-naming.sh; echo "EXIT=$?"
  retired module path still named (see scripts/check-module-naming.sh for the seven and their replacements):

    docs/known_issues.md:2208: `Flezha.State.Registry` (renamed `Flezha.State.Paths` by `fix-opaque-module-names`)
    docs/known_issues.md:2214:   MISSING: Flezha.State.Registry

  2 occurrence(s). A live reference takes the new name. A document citing the old name as history wraps it in a `module-naming-record` block.
  EXIT=1
  ```

  Both lines are **citations, not references**: the first says in so many words
  that the module was renamed, and the second is captured tool output quoted as
  evidence. Neither is a live use.
- **Mechanism**: `naming_gate.walk`'s `record_tag` exemption is per block, and
  [[KI-104]]'s entry was written without one. The gate's own hint names the
  remedy on the line it prints. `docs/known_issues.md` already carries
  `module-naming-record` markers elsewhere — the KI-42 row in the resolved table
  wraps the same module path inline — so the mechanism is in use in this very
  file and was simply not reached for here.
- **Not caused by, and not fixed by, `close-kind-naming-gate-red`**: proved by
  restoring that change's only shared file, `scripts/naming_gate.py`, to its
  committed content and re-running — identical output, same two lines.
- **A fix must** wrap the two cited lines in
  `module-naming-record:begin` / `:end`, and leave the surrounding prose
  unedited: the entry is accurate, and rewording it to satisfy a regex is the
  false economy [[KI-94]] closed. One block, two markers, no prose change.


- **Resolved 2026-09-01 by `close-kind-naming-gate-red`.**
  `./scripts/check-module-naming.sh` exits **0**. The two citations inside
  [[KI-104]] are wrapped in `module-naming-record` markers and **no prose was
  changed**: the inline one takes the same shape the resolved-table KI-42 row
  already used, and the captured tool output is wrapped *outside* its fence so
  the code block renders exactly as before.
- **Why it was in scope for a change about a different gate**:
  `.github/workflows/naming.yml` runs `check-kind-naming`,
  `check-overflow-naming`, `check-module-naming`, `check-sdk-boundary` and
  `check-mimic-global` as **five steps of one job**. A red step fails the Naming
  workflow whichever step it is, so greening only the kind gate would have left
  CI red and the branch still gated.

<!-- module-naming-record:end -->

### KI-94 · `check-kind-naming` has been red for weeks on the programme's own prose — RESOLVED

<!-- kind-naming-record:begin -->

- **Severity**: P3 · a permanently-red gate is worse than no gate, because people
  learn to skip it.
- **Where**: `scripts/check-kind-naming.sh`. **13 occurrences at `9b85b425`**, and
  **every one is prose in `docs/` using "bounded"/"unbounded" in the capacity or
  plain-English sense** — "a bounded poll", "a bounded spin on the clock", "OOM
  from unbounded WAL buffers". Not one is a kind-sense use.
- **Three of the hits are the gate's own reflection**: `known_issues.md:1639`–`:1640`
  and `STATE.md:1799`, `:1869` are text *about this gate being red*, which adds hits
  by describing the problem. The count has been driven up three separate times by
  supervisor prose and reworded away three separate times — **rewording correct
  prose to satisfy a regex**, which is precisely what area 5a's batch 2 identified
  as a false economy in `narrative_markers` (9 of its 17 were the same thing).
- **The mechanism to fix it already exists and has never been used for this.** The
  allowlist header names two: `scope:pattern` entries for narrow phrases, and
  `kind-naming-record:begin` / `:end` comments for a block that deliberately spells
  the words out — the latter chosen precisely because it "does not pin the prose's
  exact wording the way a regex here would".
- **Verdict (supervisor, 2026-08-31)**: **real, and to be fixed as its own small
  change — not inline by a worker and not by a fourth rewording.** Blinding whole
  files with a bare-word scope is refused: it would let a genuine kind-sense error
  hide in the two documents the programme reads most.
- **A fix must** wrap the recurring prose in `kind-naming-record` markers where it
  clusters, add narrow `scope:phrase` entries in the existing house style for the
  scattered ones, and **leave the gate green at HEAD** — then say what coverage, if
  any, was given up. Until it lands, `check-kind-naming` being red is not evidence
  about any change, and no worker should reword prose for it.

- **Resolved 2026-09-01 by `close-kind-naming-gate-red`.** The gate exits **0**.

  | | before | after |
  |---|---|---|
  | `./scripts/check-kind-naming.sh` | exit **1** | exit **0** |
  | occurrences, working tree | **26** | **0** |
  | occurrences, committed `docs/refactor/STATE.md` | **22** | **0** |
  | allowlist rules | **69** | **30** (45 deleted, 6 added) |

  The two counts differ because four of the 26 were in the board's own
  uncommitted edit, written while this change was in flight — the defect
  reproducing itself one more time.

- **What changed.** The gate now runs **two** judgements and fails a line that
  trips either, both built from `naming_gate.pattern_check` with no new
  mechanism: the allowlist (mask-then-match) carries the open-ended
  plain-English sense, and an unmaskable `KIND_SENSE` pattern set
  (match-directly, the judgement `check-overflow-naming.sh` already used)
  carries the closed kind sense — the word bound to `operative`, `kind`,
  `class`, `factory` or `one`, and a kind or operative predicated to *be* the
  retired name. Because no allowlist entry can mask those, the allowlist was
  free to stop enumerating nouns: a 45-noun alternation became one rule for the
  attributive shape, one for the word quoted as a token, one for a line-wrapped
  ending, and one for the adverb. **45 of the 69 rules were then dead and were
  deleted.** `KIND_SENSE` fires on **0** tracked lines outside record blocks.

- **Negative control — the gate still bites.** Each shape injected on its own
  into `docs/known_issues.md`, one gate run each, then reverted:

  ```
  a `bounded` operative.                             CAUGHT
  The operative this class declares is bounded       CAUGHT
  `BoundedOperative` is retired.                     CAUGHT
  def bounded_operative(x), do: x                    CAUGHT
  spawn_bounded(ref)                                 CAUGHT
  kind: :bounded                                     CAUGHT
  Each operative is a bounded one.                   CAUGHT
  BoundedOperative     — in Procedure.java           CAUGHT
  registerUnbounded    — in Procedure.java           CAUGHT
  ---- deliberately not caught ----
  We ship bounded workflows.                         PASSED
  We ship bounded streams.                           PASSED
  "…limited by a bounded" / "poll on the clock."     PASSED
  ```

  The last CAUGHT row is the one that matters most: while the allowlist carried
  a path entry for that file, `registerUnbounded` there **passed**. With the
  entry deleted it is caught, so the coverage was restored rather than moved.
  Detection is per injected line, not by exit code, because the board's
  uncommitted working copy holds an unrelated hit of its own.

  And for the record-block mechanism, in the same file:

  ```
  R1 · kind-sense line INSIDE the block              exit=0  not caught (by design)
  R2 · the SAME line outside it, same file           exit=1  CAUGHT
  R3 · a line below a closed block                   exit=1  CAUGHT
  ```

- **Coverage given up, named by shape.** Three, all in prose, none reachable by
  an identifier:
  1. **A wrapped line ending in the bare word after a determiner** — "…limited
     by a bounded" / "poll on the clock". Excused, because that is how prose
     wraps; a kind-sense sentence ending the same way passes. Ending after a
     *verb* ("…is bounded") stays caught, which is why the rule names
     determiners rather than any word.
  2. **The word attributive to `workflow` or `stream`** — "bounded workflows".
     Deliberate: those are the *current* kind names, so "bounded workflow" is a
     contradiction rather than a reintroduction, and binding them would fail the
     glossary's own sanctioned "folds an unbounded input".
  3. **Anything inside a `kind-naming-record` block**, including this entry.
     Scoped to the block and greppable, which is the whole point of the
     delimiter; proved by control R2 above.
- **One working-tree hit remains, and it is not this change's to fix.** With
  `docs/refactor/STATE.md` taken from HEAD the gate reports **0 occurrences**,
  exit **0**. The supervisor's uncommitted board edit adds one line quoting
  `registerUnbounded` while describing this very defect — the disease
  reproducing during its own cure. It needs a two-marker `kind-naming-record`
  block from the board's owner; a `+` line in `git diff docs/refactor/STATE.md`
  is where it lives.
- **Not done, and deliberately**: `docs/refactor/STATE.md` was not edited. Its
  15 hits are covered by path-independent shape rules, not by a bare-word scope
  over the file — which this entry's own verdict refuses — so the board can be
  written on without re-reddening the gate.

<!-- kind-naming-record:end -->

### KI-81 · Two `docker/prod` scripts call `flz cluster` verbs that do not exist — RESOLVED

- **Severity**: P2 · a production recovery path that logs an error and does
  nothing, on every controller stop and every controller boot.
- **Where**: `docker/prod/snapshot-on-stop.sh:32` runs
  `"$FLZ" cluster snapshot "$SNAPSHOT_TMP" --peer-timeout-ms "$PEER_TIMEOUT_MS"`;
  `docker/prod/controller-entrypoint.sh:104` runs `"$FLZ" cluster restore
  "$SNAPSHOT_FILE"`. `cli/src/cluster.rs` implements **`status` and `help`, and
  nothing else**, and `lib/flezha/service/cli_manager.ex` has no
  `cluster_snapshot` / `cluster_restore` (only `cluster_status/2` and
  `cluster_status_recovery/2`).
- **Evidence**:

  ```
  $ ./flz cluster snapshot /tmp/x --peer-timeout-ms 5000
  Error: Unknown cluster subcommand 'snapshot'
  ... (the cluster help screen) ...
  exit=1
  $ ./flz cluster restore /tmp/x
  Error: Unknown cluster subcommand 'restore'
  exit=1
  $ grep -n 'def cluster_' lib/flezha/service/cli_manager.ex
  777:  def cluster_status(namespace, output_format \\ "text") do
  805:  def cluster_status_recovery(namespace, nodes) do
  ```

- **Mechanism**: the verbs existed — `git log -S'cluster snapshot'` names
  `d80d8511 "Add cluster snapshot/restore for operator-driven recovery"` — and
  were removed when `9235bcde auto-restore-on-boot` replaced operator-driven
  recovery with an orphan reconcile. The two prod scripts were never updated. The
  failure is soft in both directions: the snapshot step logs
  `"ERROR: flz cluster snapshot failed; leaving previous ... in place"` and
  continues, and the restore step is guarded by `if [ -f "$SNAPSHOT_FILE" ]`,
  which is never true because nothing writes it. Auto-restore-on-boot covers the
  recovery these were for, so this is dead code that lies rather than a live data
  path — which is why it is P2 and not P1.
- **Why it was not fixed on the spot**: found by the `refactor-6-cli` survey,
  which changes no code and whose scope is `cli/**`. The remedy is a **product
  decision** (charter §1c): delete the dead path, or re-implement the two verbs.
- **A fix must** pick one and say why. Deleting is the smaller change and matches
  the architecture that replaced them; re-implementing needs a
  `CliManager.cluster_snapshot/2` and a spec, because operator-driven snapshot is
  a capability nothing currently specifies. Either way the entrypoint's comment
  block (`controller-entrypoint.sh:11`) must stop describing a step that does not
  run.
- **Supervisor verdict 2026-08-31 — CONFIRMED FROM SOURCE, and escalated.** Verified
  independently: `docker/prod/snapshot-on-stop.sh:32` invokes `flz cluster snapshot`
  and `controller-entrypoint.sh` invokes `flz cluster restore`, while
  `cli/src/cluster.rs` matches **only** `"status"` — `grep -cE '"snapshot"|"restore"'`
  returns **0**.
- **What that means in production**: the controller's pre-stop hook calls a command
  that does not exist, so **every graceful shutdown runs a snapshot step that cannot
  succeed**, and the entrypoint's restore path is equally dead. The scripts' own error
  handling makes this quiet — `snapshot-on-stop.sh:42` logs *"snapshot failed; leaving
  previous file in place"*, which reads like a transient failure rather than a missing
  verb.
- **Why it is the user's call, not mine**: the verbs were removed by `9235bcde`
  (`auto-restore-on-boot`) and the scripts were never updated. Restoring them, or
  deleting the scripts' use of them, is a decision about **whether cluster
  snapshot/restore is a supported operation at all** — a product question. Recorded
  under *Awaiting the user's decision* with options.
- **Not area 6's work.** It is `docker/prod/` and a controller capability, not the CLI
  refactor, and folding it in would put a behaviour decision inside a zero-behaviour-
  change area.

- **RESOLVED 2026-08-31 by `retire-cluster-snapshot-remnants`** — deleted, not
  re-implemented. The decisive fact was not a judgement: the published spec
  (`openspec/specs/controller-packaging/spec.md:179`) said of the snapshot branch
  *"is removed once `auto-restore-on-boot` lands and `cluster-snapshot-restore` is
  deleted"*, and both landed in `9235bcde` on 2026-05-09. The spec scheduled its own
  deletion three and a half months before this entry was filed.
- **Re-reproduced at `d4e62240` before anything was removed**, so the entry closes on a
  measurement and not on a reading:

  ```
  $ ./target/release/flz cluster snapshot /tmp/x --peer-timeout-ms 5000
  Error: Unknown cluster subcommand 'snapshot'
  ... (the cluster help screen) ...
  $ echo $?
  1
  $ ./target/release/flz cluster restore /tmp/x
  Error: Unknown cluster subcommand 'restore'
  $ echo $?
  1
  $ grep -cE '"snapshot"|"restore"' cli/src/cluster.rs
  0
  ```

- **What went**: `docker/prod/snapshot-on-stop.sh` (whole file), the entrypoint's
  restore branch and its `FLEZHA_SNAPSHOT_*` / `FLEZHA_RESTORE_WAIT_SOCKET_GLOB`
  reads, `Dockerfile.controller`'s `COPY`/`chmod` of the script, `.dockerignore`'s
  re-include, the `flz boot import-legacy-snapshot` verb on both sides (Rust verb,
  help, golden, dispatch row, `CliManager` delegate, `Cli.Boot` implementation, four
  private functions, the `:"boot.import_legacy_snapshot"` authz action and its admin
  grant), the ConfigMap's `FLEZHA_SNAPSHOT_DIR` and dead `FLEZHA_DATA_DIR`, and every
  stale document. Three spec deltas: `controller-packaging` modified,
  `runner-controller-connection-healing`'s preservation requirement removed,
  `auto-restore-on-boot`'s import bullet dropped.
- **Two claims in the verdict above were wrong, and stay visible so the file keeps
  "fixed" apart from "never true".** (a) *"every graceful shutdown runs a snapshot step
  that cannot succeed"* — it never ran on the default path;
  `controller-entrypoint.sh:122` said so explicitly, and the branch fired only under
  `FLEZHA_GRACEFUL_SHUTDOWN_ENABLED=false`. (b) *"the controller's pre-stop hook calls a
  command that does not exist"* — the Kubernetes `preStop` runs `flz controller stop`
  (`kubernetes/flezha-daemonset.yaml:179`) and never ran this script; three comments in
  the tree claimed otherwise and went with it.
- **What was NOT deleted, each with the measurement that saved it**: the entrypoint's
  disabled-shutdown branch (`FLEZHA_GRACEFUL_SHUTDOWN_ENABLED` is read by
  `config/runtime.exs:261` and specified by `controller-graceful-shutdown`, so removing
  the branch would kill a live operator kill-switch); `FLEZHA_RESTORE_WAIT_TIMEOUT_S`
  (it bounds the surviving control-socket wait, so it never became unread — only its
  name is now a wart); and `Cli.Boot`'s `safe_registry_count/1`, which `boot_status/0`
  still calls.

### KI-99 · `Source.handle_signal/2` is a ninth dead callback

- **Severity**: P4 · exactly the shape of the `Sink` callback deleted in `5b90ff2e`,
  but on a **different behaviour**, so it fell outside that ruling's words.
- **Where**: `lib/flezha/observability/source.ex:70`, `@optional_callbacks` at `:75`.
- **Evidence**: **no implementation and no invoker anywhere** in `lib/` or `test/`.
  `docs/operations/observability.md:257` already records that the foundation drives no
  `handle_poll/2` either.
- **Verdict (supervisor, 2026-08-31)**: **real — delete it, on the same reasoning that
  settled the `Sink` one.** An optional callback nothing implements and nothing calls
  is not an extension point; it is a claim about the system that is false. Area 5d
  takes it next segment.
- **A fix must** check `handle_poll/2` in the same pass rather than fixing only the one
  that was found — the doc already suggests it is in the same state.
- **RESOLVED 2026-08-31, `374a4031`, and it was larger than filed.** `handle_poll/2`
  was in the identical state and went with it, leaving `@optional_callbacks` empty and
  deleted too — which is why the verdict said to check it in the same pass.
- **Checking the pair turned up the fact that matters** and is now [[KI-100]]:
  `Flezha.Observability.Source` has **zero implementers in the tree.**

### KI-104 · `flezha_ui` is built on controller internals, and the refactor has already broken two

- **Severity**: P2 · a sibling product depends on ~30 internal `Flezha.*` modules with
  no public API between them, and the breakage is invisible until a user clicks.
- **Where**: `/home/moon/Code/flezha_ui` (a separate repo) reaches the controller by
  `erpc` through its own `NodeManager.erpc_call/3`. Confirmed broken today:
  <!-- module-naming-record:begin -->`Flezha.State.Registry`<!-- module-naming-record:end --> (renamed `Flezha.State.Paths` by `fix-opaque-module-names`)
  and `Flezha.Service.Identificator` (moved to `Flezha.Identificator` by area 5c).
- **Evidence**: every `Flezha.*` name in `flezha_ui`, resolved against
  `defmodule … do` in this tree:

  <!-- module-naming-record:begin -->
  ```
  MISSING: Flezha.State.Registry
  MISSING: Flezha.Service.Identificator
  MISSING: Flezha.Service.ConfigManager
  MISSING: Flezha.Service.PluginHelper
  MISSING: Flezha.Plugin.OperativeStdout
  MISSING: Flezha.Plugin.Provider.OperativeWebsocket
  ```
  <!-- module-naming-record:end -->

  The first two are ours and dated. The other four have no same-named successor and
  are NOT traced — they may predate the programme. `Flezha.Totally.Gone` also appears
  and is a deliberate fixture (`node_manager_distributed_test.exs:28`), not a break.
- **Mechanism**: the calls resolve at RUNTIME, not compile time, and `flezha_ui`
  already handles the miss — `{:error, {:api_missing, Module, fun, arity}}`. So a
  renamed module does not fail a build anywhere; one UI feature stops working, quietly,
  and the next rename takes another. That error path is why six breaks accumulated with
  nobody noticing.
- **Why this is not "the refactor should stop renaming"**: the user ruled explicitly
  that the programme should NOT slow down for this, and that `flezha_ui` is updated
  after the refactor. The defect is the missing seam, not the renames.
- **A fix must** give the controller a **public API surface** that `flezha_ui` consumes
  instead of internals — the user's own framing. Until that exists, the cheap
  mitigation is a resolver check (every `Flezha.*` name in the consumer against this
  tree's exports) so a break is reported at build time rather than discovered by a
  click. Note the consumer names things the tree has no module for at all, so the check
  must report unknown-and-unresolved separately from known-and-moved.

### KI-100 · The `Source` behaviour has no implementers, and two moduledocs said it did — RESOLVED

- **Severity**: P3 · a published extension point that is a false statement about the
  system. Found by area 5d while closing KI-99.
- **Where**: `lib/flezha/observability/source.ex`.
- **Evidence** (reproduced by the supervisor at `374a4031`):
  `grep -rn "@behaviour Flezha.Observability.Source" lib/ test/` → **no output**. The
  single hit before the fix was **inside `source.ex`'s own moduledoc heredoc**.
- **Mechanism**: the five real sources <!-- module-naming-record:begin -->`use Flezha.Observability.OtpBackend.Source`<!-- module-naming-record:end --> — a
  *different* module, a macro that never declares the behaviour — and **none defines
  `config_schema/0`**. `do_register_source/1` inserts a plain ETS row from a map: it
  never names a module and never calls `function_exported?`, so nothing enforces the
  contract either. `init/1` survives only because GenServer calls it under that name,
  and `config_schema/0` is read for **sinks** only.
- **Already corrected in `374a4031`**: `source_supervisor.ex`'s moduledoc claimed its
  children *"implement the `Flezha.Observability.Source` behaviour"* (false), and the
  moduledoc example taught `handle_poll/2`. Both now describe the GenServer that
  actually drives itself.
- **Verdict (supervisor, 2026-08-31)**: **the remaining question is the user's**, and
  it is under *Awaiting the user's decision*. Two honest options — delete the behaviour
  (removes a third-party extension point nobody uses), or make the claim true (have
  `OtpBackend.Source`'s macro declare `@behaviour` and enforce `config_schema/0`, which
  costs five small implementations and makes registration start checking). **Not taken
  in 5d**: it is a product-surface decision, not a refactor, and the false moduledocs —
  the part that actively misleads — are already fixed.


- **RULED 2026-09-01 (the user's call, shape chosen by the supervisor): COLLAPSE, not
  delete.** The root defect is a NAME COLLISION, found by the user asking why two
  modules share a name. They are not two versions of one idea — they declare different
  callbacks: `Flezha.Observability.Source` is `init/1` + `config_schema/0` (the
  registration contract), while <!-- module-naming-record:begin -->`Flezha.Observability.OtpBackend.Source`<!-- module-naming-record:end --> is
  `init_state/1` + `poll/1` and its own moduledoc calls it *"shared GenServer plumbing
  for the periodic observability sources"*. **That second module is a poller template,
  not a source**, and naming it `Source` is what made the real contract look redundant
  and the helper look authoritative. The fix: rename it
  `Flezha.Observability.OtpBackend.Poller`, and have its macro also declare
  `@behaviour Flezha.Observability.Source` with an overridable default
  `config_schema/0`. Deletion was the alternative and is REFUSED for a measured reason:
  three sources register with `flavour: :declarative` today
  (`telemetry_bridge.ex:171`, `registry.ex:252`, `:259`) and the poller template cannot
  describe them because it is `use GenServer` — so deleting removes the only written
  contract for the half that is actually in use. Collapse also matches the direction
  the user set in [[KI-104]]: the platform should offer a public surface, not internals.
- **RESOLVED 2026-09-01**, `collapse-observability-source-behaviours`. The template is
  `Flezha.Observability.OtpBackend.Poller` (`lib/flezha/observability/otp_backend/poller.ex`)
  and its macro declares `@behaviour Flezha.Observability.Source` on every source it
  builds, with an overridable `config_schema/0` returning `%{}`. The behaviour now has
  five implementers, read back from `module_info(:attributes)` by the lifecycle test in
  `test/flezha/observability/otp_backend_test.exs`.
- **The entry's mechanism is half right, and the missing half is why the behaviour was
  never declared.** `Source` declared `init/1`, and a process-backed source is a
  GenServer, which declares `init/1` too, so `@behaviour Flezha.Observability.Source` on
  such a module makes Elixir emit *"conflicting behaviours found. Callback function
  init/1 is defined by both GenServer and Flezha.Observability.Source"* — five warnings,
  one per source, which `--warnings-as-errors` refuses. The behaviour was not merely
  unimplemented, it was **undeclarable by the flavour it described**. `init/1` is
  therefore deleted from it, along with the `@type config` only it used, leaving
  `config_schema/0` as the sole callback; that is also the right shape for the
  declarative half, which owns no process and never had an `init/1` to offer.
- **The eighth entry in `check-module-naming.sh`'s `RETIRED` tuple** is <!-- module-naming-record:begin -->`Flezha.Observability.OtpBackend.Source`<!-- module-naming-record:end -->, so a stale
  reference in a moduledoc, a spec or a runbook is caught where `mix compile` cannot look.

### KI-98 · The cycle metrics silently drop every annotated cycle member

- **Severity**: P2 · a **ratcheted** gate under-reporting, and under-reporting
  flatters. §11e's fourth instance and §11f's fourth — and the second defect found
  in `max_cycle_files`, the ruler installed this same day.
- **Where**: `scripts/refactor_metrics.py:459` appends `line.strip()`, keeping
  `mix xref`'s ` (compile)` / ` (export)` suffix on the member path. `summarise()`
  at `:529`–`:531` then tests membership with `f in files`, where `files` holds
  bare `git ls-files` paths. **Both `cycles` and `max_cycle_files` are in
  `RATCHETED`** (`:48`).
- **Evidence** (reproduced by the supervisor at `848ade3c`): `mix xref graph
  --format cycles | grep -c '(compile)\|(export)'` → **10**. Every one of those ten
  members is invisible to both metrics, because an annotated string can never equal
  a bare path. `Cycle of length 56 (1 compile, 9 export)` names exactly ten.
- **Cross-area simulation on the real graph** — four areas move, **none blocking**,
  and `cycles` moves for none:

  | area | reported `max_cycle_files` | true |
  |---|---|---|
  | `lib/flezha` | 46 | **56** |
  | `lib/flezha/convergence` | 1 | **5** |
  | `lib/flezha/operative` | 4 | **9** |
  | `lib/flezha/service` | 5 | **6** |

- **Why it matters immediately**: area 5c's single highest-value change is cutting
  `operative_manager.ex:23`'s `@id_in_use Validator.id_in_use()` — a module
  attribute, hence **the** `1 compile` edge in the 56-file cycle. Cutting it today
  reports **nothing**; and had the edge merely been downgraded compile → runtime,
  the file becomes *visible* and the row goes **5 → 6 — a regression for progress.**
  That is §11f's exact failure mode landing on the best change in the area.
- **Verdict (supervisor, 2026-08-31)**: **fix it, in its own commit, before area 5c
  moves its first file**, per §11f's rule that a ruler disagreeing with the work is
  fixed first.
- **A fix must** strip the annotation before the membership test; add a
  `--self-test` case with an annotated member, **proved red first**; re-record the
  four moved rows in `baseline.json`; and **add no third metric** — §11f rules that
  out explicitly, and this is a correction to an existing ruler rather than a new
  one.
- **RESOLVED 2026-08-31, `3e9a1d28`.** `parse_cycles/1` strips
  `XREF_LABEL = \s+\([a-z]+\)$` at the append — **generic rather than a three-label
  allow-list on purpose**: no source path ends in ` (word)`, and a label Elixir adds
  later would otherwise reopen the same silence.
- **Red first on the mandated assertion.** The parser was extracted from
  `xref_cycles()` byte-unchanged, the fixture added, and the unfixed parser reported
  `(cycles 1, max_cycle_files 1)` against a true `(2, 2)` — the annotated cycle
  vanishing from the area **entirely**, not merely under-counted.
- **THREE area rows moved, not the four the supervisor briefed** — a correction the
  worker made and marked false in place. `lib/flezha` **is** an area, but it is the
  **ten root files** of `lib/flezha/` at `cycles` 0 / `max_cycle_files` 0, none of
  them in any cycle; `46 → 56` was the repo cycle's visible-member count, not an area
  row. Moved: `convergence` 1 → 5, `operative` 4 → 9, `service` 5 → 6. `cycles` moved
  for no area and no non-cycle row moved anywhere. Re-recorded **surgically** — three
  insertions, three deletions — rather than by `--write-baseline`, which would have
  rewritten every row against a tree holding a peer's uncommitted work.
- **Vindicated immediately, on the change it was fixed for.** Cutting the 56-cycle's
  only compile edge (`65e5d055`) did **not** remove that file from the annotated set —
  it **downgraded** it to `(export)`, so the cycle reads `(10 export)` where it read
  `(1 compile, 9 export)`. `lib/flezha/service` `max_cycle_files` is **6 before and 6
  after**; on the unfixed ruler the same cut would have reported **5 → 6, a regression
  for making the best change in the area.**


### KI-95 · The operator-facing parameter order varies with what the VM has loaded

- **Severity**: P3 · an operator-visible surface whose order is unspecified and
  irreproducible. Found by area 5b while proving a refactor byte-neutral.
- **Where**: `lib/flezha/plugin/provider_behaviour.ex` `fields/1`, and identically
  the pre-B1 `help/0`, `provider_info/0` and `build/1` bodies — **all four derive
  the field list from `Map.keys/1` on a struct.**
- **Mechanism, named**: for atom keys an Erlang flatmap orders by the **atom
  table**, i.e. interning order — not declaration order and not alphabetical.
  Reproduced by the supervisor:

  ```
  $ elixir -e 'IO.inspect(Map.keys(%{zebra: 1, apple: 2, mango: 3}))
               IO.inspect(Map.keys(Map.new([:mango,:apple,:zebra], &{&1,1})))'
  [:zebra, :apple, :mango]
  [:zebra, :apple, :mango]
  ```

  Both orders are identical and neither is the insertion order, so the result is a
  function of **which atoms the VM has already interned** — what else was loaded,
  and each provider beam's atom chunk.
- **It is not any one change's doing**: on one tree and one build, a script that
  loads only the nine providers puts `overwatcher_pid` **last**, while a script
  that also calls `help`/`build`/`validate` puts it **fifth** — and the first gives
  the same answer with the provider list forward and reversed.
- **And it cannot be preserved by a refactor**: under a *fixed* harness the order
  moved anyway when B1 removed ~110 generated lines from every provider, changing
  their atom chunks. **A golden file that pins this order is pinning noise**, which
  is the practical reason it matters to this programme.
- **Verdict (supervisor, 2026-08-31)**: **real, fix it** — pin the order to
  `defstruct` declaration order via `module.__info__(:struct)`. That is stable,
  reproducible, and the order the provider's author actually wrote. It is a
  deliberate one-time operator-visible change, so it lands in **its own commit**
  with the new order of all affected providers recorded, and it is taken **now**,
  while the golden harness that measured it is still warm — re-deriving it later
  costs more than doing it.
- **Measured cost**: reorders **7 of the 9** providers, most fields on each
  (`Kafka.Producer` and `Websocket` substantially).
- **RESOLVED 2026-08-31, `d5923bb8`.** `fields/1` reads `module.__info__(:struct)`.
  Proved stable on the two harnesses that used to disagree — providers-only
  forward, providers-only **reversed**, and the full capture now answer
  identically; the supervisor reproduced it independently under two load orders.
  **Six providers reorder, every field set identical.**
- **It deleted a second mechanism on the way out**, which is the better half of the
  fix: `required_fields/1` read `@enforce_keys` by matching the **text of the
  `ArgumentError`** that `struct!/2` raises. `__info__(:struct)` carries
  `required: true` per field and the two agreed on **9 of 9** providers, so the
  parse and its helper are gone — **−29 lines, and an operator-facing list no
  longer depends on parsing a compiler error message.**
- **One consequence, the same mechanism**: `build_struct/3` reports the *first*
  unconvertible field in declaration order, so a map with several invalid values
  now names a predictable field instead of an atom-table-dependent one. A single
  invalid value is unchanged.


### KI-93 · The generated `build/1` raises where its own `@callback` says it returns

- **Severity**: P2 · a documented public callback surface that cannot return its
  documented error. Found by area 5b, reproduced by the supervisor.
- **Where**: `lib/flezha/plugin/provider_behaviour.ex:329` `__using__`, body
  `:334`–`:388`.
- **Evidence** (run at `c78f7deb`):

  ```
  $ mix run -e 'try do IO.inspect(Flezha.Plugin.Provider.Kafka.Producer.build(
      %{"hosts" => "h:1", "topic" => "t", "linger_ms" => "zz"})) rescue e ->
      IO.puts("RAISED #{inspect(e.__struct__)}: " <> Exception.message(e)) end'
  RAISED BadMapError: expected a map, got:
      "Invalid value for linger_ms: Invalid value for linger_ms: zz"
  ```

- **Mechanism, named to the line**: `:347`'s `Enum.reduce_while(struct_fields,
  {:ok, %{}}, …)` accumulates `{:ok, map}` on `:cont` and `{:error, binary}` on
  `:halt` (`:362`). The next expression, `:368`–`:371`, destructures that as
  `{convert_status, params_data}` and calls `Map.has_key?(params_data, key)` **on
  the binary**. So `:377`'s `convert_status == :error -> {:error, "Failed to
  convert fields: …"}` arm **can never run** — and that arm is the author's own
  evidence the raise is unintended. The guard that hides it is `enforce_keys ==
  []`: with no required fields `Enum.filter/2` never calls the fun, which is why
  `StdOut` returns `{:ok, …}` and `Kafka.Producer` raises.
- **Blast radius, measured** by driving every optional field of all nine
  providers: **7 of 9 providers, 41 fields** — Filter 1, HttpPoll 5,
  Kafka.Producer 17, MonitoredIOForward 2, OperativeForward 4, Postgres 10,
  Websocket 5 (four of those `Protocol.UndefinedError`). Clean: StdOut,
  BeamMsgForward.
- **Why it has gone unnoticed**: `build/1` has **no production caller** in `lib/`
  — plugins are constructed as `%Flezha.Plugin{}` with typed opts ([[KI-62]]). It
  is reachable through the documented `@callback` and from tests only.
- **Secondary wart at `:362`**: the reason is re-wrapped although `parse_int/2`
  (`:86`) already formatted it, hence the doubled `Invalid value for linger_ms:`
  in the message above.
- **Verdict (supervisor, 2026-08-31)**: **real and worth fixing, and it goes
  FIRST** — before area 5b's segment B, which rewrites this exact macro. §1b.6:
  a fix precedes the refactor of the code it touches, or the bug is carried into
  the new shape.
- **A fix must** make the reduce's failure value reach the `cond` in the shape
  the `cond` already expects, restore the unreachable arm, stop double-wrapping
  the reason, and be shown RED first against at least one raising provider. It
  must also confirm **no test asserts the raise** before changing it.
- **RESOLVED 2026-08-31, `17ec6993`.** The halted reduce answers `{:error, reason}`
  and the required-field check runs only on the map the success arm built; the
  double wrap is gone. **44 raising fields across 7 providers → 0 across 9**, and
  the golden capture over `help/0`, `provider_info/0`, `build/1` and
  `validate_opts/1` differs in **exactly 5 rows**, every one a raise becoming its
  documented error. Red first: 4 of 8 new tests fail on the unfixed macro, and the
  supervisor re-drove the same 4-of-8 split against HEAD's macro. No test asserted
  the raise — checked before editing, as the verdict required.
- **One honest deviation from the verdict's wording**, reported rather than
  silently taken: `:377`'s literal arm could not be kept, because `params_data`
  would have to be a map (for `Map.has_key?/2`) and the reason (for the message) in
  one slot. Its **behaviour** is what became reachable; its text is superseded by
  the reason itself, since `inspect`ing that into `"Failed to convert fields: …"`
  would recreate the double wrap in another form.


- **Correction, 2026-09-01 — "Clean: BeamMsgForward" is true only of the path
  the scan drove.** The blast-radius measurement above drove every *optional*
  field; `BeamMsgForward`'s `:recv_pid` is `@enforce_keys`, so it was outside
  that scan rather than cleared by it. It still raises at HEAD:

  ```
  $ MIX_ENV=test mix run --no-start -e 'Flezha.Plugin.Provider.BeamMsgForward.build(%{"recv_pid" => "zzz"})'
  RAISED ArgumentError: errors were found at the given arguments:
    * 1st argument: not an already existing atom
  ```

  Ruled by the supervisor: **real, and it is an instance of [[KI-116]]'s
  mechanism 1** — a partial `convert_value/2` clause with the permissive
  catch-all behind it — not a separate defect and not a reopening of this entry's
  own fix, which did what it measured. `close-provider-value-validation` closes
  it where every other site is closed; no separate change is owed.

### KI-10 · Credo strict contradicts house style — RESOLVED 2026-08-31

- **Filed as**: strict credo reports 1349 issues, 967 of them `AliasUsage` and
  `ParenthesesOnZeroArityDefs`, both against house style; the credo count could
  not be ratcheted until someone decided. The entry said *"Area 5a owes the
  decision"* — and so did `STATE.md`.
- **RESOLVED, and it was resolved before 5a was ever surveyed.** Area 5a's survey
  went looking for the decision it was told it owed and found all four parts of it
  already in the tree:
  - `.credo.exs:19`–`:21` puts `ParenthesesOnZeroArityDefs` under **`disabled:`** —
    the check is off by decision, not by accident.
  - The other half was **adopted, not disabled**: row 3.5's alias pass took
    `AliasUsage` **689 → 0** (`STATE.md`, row 3.5's record).
  - The ratchet the entry said was "still absent" exists: **all 36 areas carry a
    recorded `credo_warnings`** in `docs/refactor/baseline.json`
    (`lib/flezha/state` = 28, which the survey re-measured to the digit).
  - `openspec/specs/code-quality-baseline/spec.md:489` carries the requirement
    *"A contested lint check SHALL be settled by decision, and the settled count
    SHALL be ratcheted"*.
- **The lesson is about the register, not about credo.** The 2026-08-30 triage
  marked this **REAL** and *"Nothing has decided this"* — and it was, by then,
  half decided; the rest landed in row 3.5 without anyone striking the entry. An
  open item that names a future area as its owner will be believed by that area's
  worker, so **an entry whose remedy lands elsewhere must be closed by whoever
  lands it**. Area 5a owes nothing here.


### KI-53 · `InputLog` traps exits, links a syncer, and handles no `EXIT` — RESOLVED

Closed by `close-input-log-syncer-blindness`. Both halves are fixed in
`lib/flezha/state/input_log.ex`: `sync_now/1` monitors the companion and answers
`:error`, `handle_info({:EXIT, syncer, _})` reports
`[:flezha, :state, :input_log_syncer_died]` and stops the log, and a catch-all
`handle_info/2` reproduces `GenServer`'s default `[error]` report.

**Both halves were driven red first**, in
`test/flezha/state/input_log_test.exs` → `describe "the fsync companion's
death"`, against HEAD's `input_log.ex`:

```
$ mix test test/flezha/state/input_log_test.exs \
    --only "describe:the fsync companion's death"

  1) test ... no fsync tick is sent to a companion that has exited
     code:  assert dead_syncs() == 0
     left:  10
     right: 0

  2) test ... a :fsync call is answered rather than parking the log for ever
     the log never answered :fsync: [status: :waiting, current_stacktrace:
       [{Flezha.State.InputLog, :sync_now, 1, [... line: 531]},
        {Flezha.State.InputLog, :handle_call, 3, [... line: 417]}, ...]]

Result: 0/2 passed
```

The ten are the fsync batcher's **own** ticks, one per append — no synthetic
casts — each `send(state.syncer, :sync)` traced as
`:send_to_non_existing_process`. After the fix both are green
(`Result: 33 passed` for the module).

**Two negative controls, each reverting one half of the fix, separate the two
mechanisms** — they are independent, not one wearing two hats:

| reverted | row 1 (dead ticks) | row 2 (parked call) |
| --- | --- | --- |
| `sync_now/1`'s monitor | green | **red**, `status: :waiting` in `sync_now/1` |
| the `{:EXIT, …}` clause (catch-all kept) | **red**, `left: 10` | green |

The catch-all was driven once with a stray message: it logs
`[error] Flezha.State.InputLog #PID<0.455.0> received unexpected message in
handle_info/2: :a_stray_message` and the log keeps serving.

**What was true in the original entry, and what was not.** The filed half —
silent fsyncs — is real and measured above. The unfiled half is worse and is
recorded in full below.

- **Supervisor ruling 2026-08-31 — REPRODUCED, and the entry understates it. Option
  B plus a mandatory `sync_now` bound.** Verified at HEAD: `handle_info` count in
  `input_log.ex` is **0**; `sync_now/1` (`:529`) is `receive do {^ref, :ok} -> :ok
  end` with **no `after`**; `terminate/2` (`:471`) does `:file.datasync` on the log's
  own descriptor.
- **The half nobody filed is the worse one.** The filed defect is silent fsyncs — 11
  sends to a dead pid with the batcher's dirty set empty, because `fsync_async` is a
  cast that cannot fail. But `sync_now/1`'s bare `receive` means a **`:fsync` call
  against a dead syncer wedges the GenServer permanently**: it stays alive, answers
  nothing, and every append for that operative class parks forever. The comment above
  it — *"the caller's call timeout bounds the wait"* — is why this survived, and it is
  simply false: a caller's timeout bounds the caller, never the callee. **Correct that
  comment as part of the fix.**
- **It is reachable through graceful shutdown**: `Shutdown.Coordinator:468` →
  `FsyncBatcher.flush_all` → `drain(state, :call)` → `GenServer.call(input_log,
  :fsync)`. A syncer that dies before shutdown makes the final flush of every dirty
  writer stall past the 5 s finalise timeout, be swallowed by `safe_finalise_step`,
  and the coordinator proceeds to stop transports with the batcher still blocked.
- **Why Option B and not A.** The question is not "A or B" — it is "bound + A" or
  "bound + B", because the wedge happens *before* any `handle_info` clause could run,
  so a re-spawn clause is unreachable from the wedged state. Given that: **A loops
  unboundedly in a reachable state** (class directory gone → `open_raw` badmatches
  instantly → EXIT → re-spawn → …) **with no supervisor budget in the way, because
  the log never dies**. Bounding that means an intensity counter inside the GenServer
  — reimplementing a supervisor inside a supervised child, which charter §2 refuses
  when OTP already owns the mechanism. **B is also stronger on the data**:
  `terminate/2` datasyncs the log's own descriptor, so stopping *flushes the window
  the dead syncer owed*, where a re-spawn only re-arms the next tick. And B
  **self-heals the one looping condition**, since `handle_continue(:open)` mkdir_p's
  before opening.
- **The residual is accepted**: repeated deaths spend `Flezha.State.Supervisor`'s
  restart budget, shared by every writer on the node. It introduces **no new loop
  class** — ENOSPC/EIO already crash the log directly at `:ok = :file.write(…)`.
- **Two constraints on the implementation.** `sync_now/1` monitors the syncer and
  answers `:error`, which `FsyncBatcher.drain/2` already reads as *"stays dirty for
  the next drain"* — no new contract. And **any `handle_info/2` added replaces
  GenServer's default handler**, so a catch-all is required and must reproduce
  today's loud `[error]`, never `{:noreply, state}` on an `{:EXIT, _, _}` — the shape
  this entry has always forbidden.

**The original entry, as filed:**


- **Severity**: P2 · **proposal owed** — the fix is a production behaviour
  decision, so `finish-the-suite-log-gate` filed it rather than folding it in,
  the way `green-the-suite-log-gate` filed KI-48.
- **Where**: `lib/flezha/state/input_log.ex` — `init/1` sets
  `Process.flag(:trap_exit, true)` (`:359`), the fsync companion is
  `spawn_link`ed (`:488`, `start_syncer/1`), and the module has **no
  `handle_info/2` clause at all**. Every message that is not a call or a cast
  therefore lands on `GenServer`'s default handler, which logs at `[error]`.
- **What happens**: the syncer's death arrives as `{:EXIT, pid, reason}` and is
  reported as

  ```
  [error] Flezha.State.InputLog #PID<0.9412.0> received unexpected message in
    handle_info/2: {:EXIT, #PID<0.9413.0>, :killed}
  ```

- **Observed at BEAM shutdown**, *after* `Result: 1840 passed` had printed —
  the application master's terminate sweep exits the app's remaining processes
  with `:kill`, and the order is unspecified, so a syncer can be killed while
  its still-trapping `InputLog` is alive to log it. Two lines in one full
  `mix test`; **1 of 6** full runs on this box (`errorlines=0` in the other
  five). Not reproducible from `mix test test/flezha/state/input_log_test.exs`
  alone (0 of 3 runs, 31 passed) — the logs there are stopped by their own
  supervisor before the sweep reaches them.
- **The log line is the small half.** With no `EXIT` clause a syncer that dies
  for a *real* reason — an `:file.datasync` error, a `{:badmatch, …}` out of
  `open_raw/1` — is logged as an "unexpected message" and then ignored, and
  every later `send(state.syncer, :sync)` (`:451`) goes to a dead pid and
  silently succeeds. **The fsync stops happening and nothing says so**, on the
  path every effectively-once source waits on. `terminate/2` already sends the
  syncer `:stop` (`:477`), so the syncer's lifetime *is* meant to be managed;
  the `EXIT` half is the omission.
- **A fix must** decide what a dead syncer means rather than just silencing the
  log: re-spawn it (and say so), or stop the `InputLog` so its supervisor
  restarts the pair. Matching `{:EXIT, _, _}` and returning `{:noreply, state}`
  turns a durability hole into a *silent* durability hole and must not be the
  answer.

- **Supervisor note at close 2026-08-31 — the shutdown-noise trade is accepted,
  deliberately.** At application shutdown the app master kills processes in
  unspecified order — the condition KI-53 was first observed under. Where HEAD logged
  one `[error] … received unexpected message`, the fix now **stops** the log with
  `{:input_log_syncer_died, :killed}`, which `gen_server` and the supervisor each
  report at `[error]`. It did not appear in the close-out run (0 gate hits), but
  KI-53's own line was 1-in-6, so one run settles little.
  **Left in place on purpose**: suppressing it means special-casing a shutdown-shaped
  reason, which re-hides the failure this entry exists to stop, and it would be
  optimising the gate rather than the system. **Row 8 (the uncaptured-log gate
  re-proof) is where this gets measured** — if it turns that gate red, that is
  information row 8 must have, not something to have hidden in advance.

### KI-73 · The send gate holds the sender one frame too late, and ~20 % of a burst is still discarded under an `:ok` — RESOLVED

- **Severity**: P1 · **REPRODUCED**, 4 of 4 runs, deterministic. Found while
  implementing the sender-side hold for `close-transport-send-buffer-loss`
  (row 6.5 segment B), by the test written to prove the hold works.
- **Where**: `flz_nif/src/stream.rs` `send_frame` reads
  `SendGate::is_raised`, which is written only by `SendGate::observe` —
  called from `uds_driver::flush_pending` **on the driver thread**.
  `flz_nif/src/uds_driver.rs` `enqueue_write` then offers each queued frame to
  `SendGate::offer`, which discards at the ceiling and answers `false`; that
  answer is used only to decide whether to close the stream.
- **What happens**: the gate bit lives on the far side of an unbounded command
  channel and nothing accounts for what the sender has already queued into it.
  A sender in a burst therefore issues hundreds of `send_frame` calls before
  the driver has crossed the high-water mark and set the bit. Every one of
  those is queued, offered to a buffer already at the cap, and **discarded** —
  and `send_frame` answered `:ok` for all of them. The hold itself works: the
  sender is refused exactly once and released when the buffer drains. It is
  simply taken after the loss.
- **Why it matters**: this is KI-3 surviving its own fix at one fifth the
  rate. The row's stated guarantee — a bounded send buffer signals congestion
  rather than discarding — and the `controller-send-buffer-bound` scenario
  *"Congestion increments nothing"* are both false while it stands.
- **It refutes a design claim that was measured**: `design.md` D13 recorded 83
  overshoot frames on the ungated build and stated *"The residual 83 are what
  batch 3 removes, by parking the sender instead of letting it discover the
  refusal one frame at a time."* Parking removes the *repeat* refusals, not the
  overshoot: the overshoot is everything the sender queued before the bit was
  set, and the park cannot begin until it is.
- **Reproduction** — KI-3's harness with the channel and the hold in the path:
  a real UDS driver, a real peer that connects and never reads, a real
  `StreamChannel` owning the stream, and one sender on `io_in/3`. The sender
  runs in its own process because `io_in/3` holds the calling one.

  ```elixir
  # /tmp/ki73.exs
  alias Flezha.Runner.StreamChannel
  alias Flezha.Transport.{LivenessCounters, Native}
  Application.ensure_all_started(:telemetry)
  LivenessCounters.ensure_table()

  path = Path.join(System.tmp_dir!(), "ki73_#{System.unique_integer([:positive])}.sock")
  {:ok, server} = Native.start_uds_server(path, self())
  {:ok, peer} =
    :gen_tcp.connect({:local, String.to_charlist(path)}, 0, [:binary, active: false, packet: :raw])
  stream = receive do: ({:flz_connection, s} -> s), after: (5_000 -> raise "no conn")

  name = :ki73_channel
  {:ok, pid} = StreamChannel.start_link({"ki73_runner", name, :primary})
  :ok = Native.controlling_process(stream, pid)
  :ok = GenServer.call(pid, {:bind, stream})

  test = self()
  payload = :binary.copy("x", 64 * 1024)
  spawn(fn ->
    r = Enum.map(1..400, fn i -> StreamChannel.io_in(name, "op_#{i}", {payload, %{}}) end)
    send(test, {:done, Enum.frequencies(r)})
  end)

  Process.sleep(700)
  st = hd(Native.driver_stats(server).streams)
  %{channels: ch} = LivenessCounters.snapshot_all()
  c = Enum.find(ch, &(&1.name == name))
  IO.puts("HELD: send_gate_held=#{c.send_gate_held} send_frame_calls=#{c.send_frame_calls}")
  IO.puts("DRIVER: frames_written=#{st.frames_written} write_buffer_dropped=#{st.write_buffer_dropped}")

  # Drain, so the gate lowers and the held sender is released.
  spawn(fn ->
    d = fn d -> if match?({:ok, _}, :gen_tcp.recv(peer, 0, 3_000)), do: d.(d) end
    d.(d)
  end)

  receive do: ({:done, f} -> IO.puts("SENDER: #{inspect(f)}")), after: (20_000 -> IO.puts("hung"))
  st = hd(Native.driver_stats(server).streams)
  IO.puts("FINAL: frames_written=#{st.frames_written} write_buffer_dropped=#{st.write_buffer_dropped}")
  _ = Native.stream_id(stream)
  Native.stop_server(server)
  ```

  ```
  $ MIX_ENV=test mix run --no-start /tmp/ki73.exs
  HELD: send_gate_held=1 send_frame_calls=211
  DRIVER: frames_written=131 write_buffer_dropped=79
  SENDER: %{ok: 400}
  FINAL: frames_written=321 write_buffer_dropped=79
  ```

  321 written + 79 discarded = 400, and the sender was told `:ok` 400 times.
  Four runs: 78, 79, 79, 92 discarded. The loss is a single burst at the
  crossing — the counter does not move again once the sender is pacing with
  the driver — so it is bounded by how far ahead the sender got, which nothing
  bounds.
- **A fix must** make the sender's own read exact rather than lagging, since
  no widening of the watermark gap bounds "how far ahead a sender can get".
  The shape that does it, for the verdict to weigh: `SendGate` gains
  `inflight` (bytes `send_frame` has queued and the driver has not yet
  offered) and `buffered` (the depth the driver last observed).
  `send_frame` adds its length to `inflight` and raises the bit itself when
  `buffered + inflight` crosses the high-water mark; `enqueue_write`
  subtracts before offering; the driver still lowers on the drain. The cost is
  one `fetch_add` and one extra relaxed load on a path that already crosses
  into a NIF — the same order as the load `design.md` D9 already argues from
  placement, so the declared A/B rows are unaffected. It also gives the
  invariant a test that can be written: `write_buffer_dropped == 0` after a
  congested burst, which is the assertion
  `test/flezha/runner/stream_channel_send_gate_test.exs` deliberately does not
  make today.
- **Not fixed in segment B**: the remedy changes the gate's mechanism rather
  than a caller, and the verdict on that is the supervisor's.

- **Supervisor verdict 2026-08-31 — REPRODUCED INDEPENDENTLY, and row 6.5 does not
  close until it is fixed.** I extracted the harness from this entry and ran it
  unchanged; it matches the filed numbers to the frame on the first run:

  ```
  HELD:   send_gate_held=1 send_frame_calls=211
  DRIVER: frames_written=131 write_buffer_dropped=79
  SENDER: %{ok: 400}
  ```

  **79 of 400 frames discarded, and all 400 answered `:ok`.** That is KI-3's exact
  symptom, still reproducing after the gate and the park are in place.
- **Why this blocks the row rather than being queued.** Row 6.5 exists so the
  platform's stated guarantee — nothing drops by default — becomes true. Closing it
  here would commit a change claiming to resolve KI-3 while KI-3's own reproduction
  still returns 79. Worse, the spec delta's *Congestion increments nothing* scenario
  is **not satisfiable in this state**, so archiving would publish a requirement the
  code does not meet. A requirement that is false on the day it lands is the failure
  this programme spent the session removing from `docs/known_issues.md`.
- **The mechanism is sound and the fix is small.** The gate bit is written by
  `SendGate::observe` on the **driver thread**, behind an unbounded command channel,
  so the sender's view is always behind by whatever is queued — ~210 frames here. The
  park cannot begin before the bit is set, so it removes the *repeat* refusals and not
  the *overshoot*. Fix shape, from the entry: `SendGate` gains `inflight` + `buffered`
  and **`send_frame` raises the bit itself** — one `fetch_add` and one relaxed load,
  the same order of cost D9 already argues from placement.
- **It refutes `design.md` D13**, which claimed the residual 83 frames measured at
  segment A were "what batch 3 removes". They are not: parking removes repeats, and
  the overshoot is everything queued before the bit was set. D13 must be corrected,
  not quietly dropped — it is a measured claim that turned out wrong.
- **Credit where due**: this was found by the test written to prove the hold works,
  and the worker then declined to assert `write_buffer_dropped == 0` in that test
  because the assertion would have been false. A test that quietly omits the
  assertion its subject cannot yet pass is how KI-67 happened; naming the omission is
  the opposite.

**RESOLVED by `close-transport-send-buffer-loss` (segment C).** The fix is the
shape this entry proposed: `SendGate` gains `inflight` (bytes a sender has
queued that the driver has not taken) and `buffered` (the depth the driver last
observed); `send_frame` adds its length and **raises the bit itself** when the
sum crosses the high-water mark; each driver releases a frame's bytes as it
takes them. The sender's read is exact rather than lagging, because the half it
is racing is the half it writes.

The entry's own harness, unchanged, on the fixed build — four runs, identical:

```
HELD: send_gate_held=1 send_frame_calls=108
DRIVER: frames_written=107 write_buffer_dropped=0
SENDER: %{ok: 400}
FINAL: frames_written=400 write_buffer_dropped=0
```

**0 discarded against 79, and all 400 frames delivered.** The crossing is now
where it is meant to be: 107 x 64 KiB is 7.0 MiB against a 6.71 MiB mark, and
the 108th call is the one refused.

**The assertion this entry said was owed is made.**
`test/flezha/runner/stream_channel_send_gate_test.exs` asserts
`write_buffer_dropped == 0` at the crossing and again over a completed run,
beside a peer that counted every frame's bytes off the socket. Negative
control: with the sender-side raise removed, those two assertions — and only
those two — fail, 2 of 4 on two seeds; restored, 4 of 4 on six seeds.

`design.md` D13 is corrected in place rather than dropped, as this entry asked.

### KI-71 · A second concurrent write on one QUIC client stream discards the first one's buffered remainder — RESOLVED

- **Severity**: P1 · **read-established from the code; not driven at runtime**.
  Found beside KI-70.
- **Where**: `flz_transport/src/transport/quic.rs`, `write_or_buffer`'s
  `Err(WriteError::Blocked)` arm: `slot.write_buf = data;` followed by
  `slot.pending_write_reply = Some((total_len, reply));`.
- **What happens**: both statements **assign**, they do not append. When a
  previous call left a partial write buffered (`slot.write_buf` non-empty and
  `pending_write_reply` `Some`), a second call that blocks replaces the
  buffered bytes with its own and replaces the pending reply sender. The first
  writer's remaining bytes are gone from the stream — a torn frame, not a
  dropped one — and its `reply_rx.recv()` sees the dropped sender and reports
  `BrokenPipe "Driver reply dropped"`, which names neither the stream nor the
  loss.
- **Reachability**: `QuicBidiStream::clone_handle` shares one `cmd_tx` and one
  `stream_id` across handles, and `impl Write for &QuicBidiStream` exists so an
  `Arc<FlezhaStream>` can be written from several threads. Two threads writing
  one stream while the peer's window is full is the condition.
- **Evidence**: the code. **Not driven**: reproducing it needs two concurrent
  writers on one stream against a stalled peer, i.e. the same live pair harness
  KI-70 is waiting on.
- **A fix must** append rather than assign, and answer the earlier waiter
  rather than dropping its sender — or refuse concurrent writers on one stream
  outright, and say so where `clone_handle` invites them.

- **Supervisor verdict 2026-08-31 — REAL, and fixed in the same pass as [[KI-70]]**,
  because it is in the same function. `slot.write_buf = data;` and
  `slot.pending_write_reply = Some(...)` both **assign**, so a second concurrent
  write on one stream discards the first's buffered remainder — a torn frame — and
  drops its reply sender.
- **Requirement on the fix**: it is **read-established, not driven**, and it does not
  ship on that footing. `write_or_buffer` needs a live `quinn_proto::Connection`, and
  segment A built exactly such a harness on the other side of a crate boundary
  (`flz_nif/src/quic_driver_tests.rs`). Porting it is the **first task of the fix**.
  If the drive cannot be made to fail inside the timebox, the fix does not land and
  the entry stays filed — a torn-frame fix nobody has seen fail is the KI-67 shape.

**RESOLVED by `close-transport-send-buffer-loss` (segment B).** `write_or_buffer`
appends rather than assigns, and keeps every waiting writer's reply sender
beside its bytes. The drive this entry's verdict required was performed:
restoring the assign loses the first writer's 4-byte remainder mid-frame
(`left: 65536, right: 65540`). The same function held an unfiled third defect —
a non-empty buffer did not stop a later frame going on the wire ahead of an
earlier frame's remainder — fixed in the same pass.

### KI-70 · The QUIC client drops a frame and tells its caller every byte was written — RESOLVED

- **Severity**: P1 · **read-established from the code; not driven at runtime**
  (see *Evidence*). Found 2026-08-31 while reading the send-buffer sites for
  `close-transport-send-buffer-loss`.
- **Where**: `flz_transport/src/transport/quic.rs:543` (`write_or_buffer`),
  reached from `QuicBidiStream::write` → `DriverCommand::Write` →
  `handle_command`.
- **What happens**: the QUIC **client**'s per-stream send buffer is bounded by
  `would_overflow_write_buffer`, and at the ceiling it does
  `slot.write_buffer_dropped += 1`, logs one `tracing::warn!`, and then
  `let _ = reply.send(Ok(total_len))` — it answers its caller that the whole
  frame was written. `std::io::Write::write` returning `Ok(n)` means `n` bytes
  were accepted; a caller has no way to tell this apart from a real write, and
  `slot.write_buffer_dropped` is per-slot state read by nothing.
- **Why it matters more than it looks**: this is not Rust-SDK-only code.
  `flz_core → flz_transport` and `flz_core_ffi → flz_transport`, so every SDK
  that reaches the controller over QUIC compiles this path in. It is KI-3's
  defect — a silent drop under an `:ok` — on the runner → controller side of
  the same socket, and the `close-transport-send-buffer-loss` proposal names
  neither the file nor the behaviour.
- **Evidence**: the code, quoted above; `grep -n would_overflow_write_buffer
  flz_transport/src/transport/quic.rs` → `543`, and the reply on the line after
  the counter bump. **A runtime drive was NOT run**: `write_or_buffer` needs a
  live `quinn_proto::Connection`, and the only in-memory pair harness in the
  tree is `flz_nif/src/quic_driver_tests.rs`, on the other side of a crate
  boundary. Porting it is the first task of the fix, not of the filing.
- **A fix must** decide this site together with KI-3 and KI-56 — one answer for
  all three send buffers — and in the meantime must not leave a `Write` impl
  reporting bytes it discarded.

- **Supervisor verdict 2026-08-31 — REAL, confirmed from source, and folded into
  row 6.5 (segment B).** Read at `flz_transport/src/transport/quic.rs:543`:
  `write_or_buffer` increments `write_buffer_dropped`, logs, and then
  `let _ = reply.send(Ok(total_len))` — it reports **every byte written** for a
  frame it discarded. That is KI-3's defect on the runner→controller side.
- **It belongs in this row, not a later one.** Row 6.5 exists to stop the transport
  losing data silently. Closing the UDS driver and the QUIC driver while leaving the
  **client** path answering `Ok(n)` on a dropped frame would ship a half-fix whose
  remaining half is in code **every SDK compiles in** (`flz_core → flz_transport`,
  `flz_core_ffi → flz_transport`). The proposal's Impact names neither the file nor
  the behaviour; that is a scope correction, recorded as D11, not a new change.

**RESOLVED by `close-transport-send-buffer-loss` (segment B).** `write_or_buffer`
answers a refused frame with an error naming the condition — `WouldBlock`, since
the stream is healthy and the peer is behind — and never `Ok(n)` for bytes it
discarded. Driven, not argued: the `quinn_proto` pair harness moved down into
`flz_transport` so one harness covers both send buffers, and restoring the
ceiling arm answers `Ok(n)` for a discarded frame while the control — a reading
peer refuses nothing — stays green.

### KI-56 · The QUIC driver's send buffer has no cap at all — RESOLVED

- **Severity**: P1 · **read-established, NOT reproduced** — filed 2026-08-30
  while answering a question about KI-3, by reading both drivers side by side.
  The reproduction is owed; see *What would confirm it* below.
- **Where**: `flz_nif/src/quic_driver.rs:533` (`write_stream`) and `:664`
  (`flush_quic_write`), against `flz_nif/src/uds_driver.rs:474`
  (`enqueue_write`).
- **What happens**: the UDS driver bounds its per-stream send buffer —
  `would_overflow_write_buffer(buffered, incoming, cap)`, 8 MiB by default,
  `DropNewest` once non-empty. **The QUIC driver calls that predicate nowhere.**
  `write_stream` does an unconditional `write_buffer.extend_from_slice(&data)`,
  and `flush_quic_write` treats `quinn_proto::WriteError::Blocked` — the peer's
  QUIC flow-control window being full — as a no-op that leaves the bytes
  buffered. So the same condition that makes UDS *drop* (a peer that stops
  draining) makes QUIC **grow without bound**.
- **Two transports, two different failure modes, neither reported.** UDS loses
  data silently; QUIC risks the process. This repository already carries a
  production OOM whose root cause was unbounded buffers
  (`controller-oom-rootcause-unbounded-wal-buffers`), so unbounded growth here
  is not a hypothetical class.
- **The documentation asserts the opposite.** `flz_transport/src/transport/
  config.rs:34` introduces the cap as "Per-stream send-buffer cap **shared by
  the UDS and QUIC drivers**", and `would_overflow_write_buffer`'s own doc calls
  itself "the one predicate behind **every** driver's send-buffer bound". Both
  sentences are false as written: `grep -n would_overflow_write_buffer
  flz_nif/src/quic_driver.rs` returns nothing. A reader checking whether QUIC is
  bounded finds a doc comment saying yes.
- **What would confirm it** (as filed): a QUIC stream whose peer connects,
  completes the handshake and never reads, then a sustained write; watch the
  driver's `write_buffer_len` (already emitted as `driver_write_buffer_bytes`
  by `TransportSource.emit_driver_stats/2`) grow past 8 MiB without bound and
  without a `write_buffer_dropped` bump.
- **That reproduction cannot be performed, and the reason is KI-72.** The QUIC
  driver produces no `DriverStats` at all: `flz_nif/src/server.rs:111` sets
  `stats: None` for `start_quic_server` and `driver_stats/1` answers
  `:undefined` for it, so `driver_write_buffer_bytes` is emitted for UDS only
  and has no QUIC value to grow. There is also no `write_buffer_dropped` on
  this driver to stay at zero.
- **REPRODUCED (2026-08-31), by another route**: an in-memory `quinn_proto`
  pair — `flz_nif/src/quic_driver_tests.rs` — whose client completes the
  handshake and never reads a byte, driving the driver's own
  `flush_quic_write` over the driver's own buffer.

  ```
  $ cargo test -p flz_nif --lib -- --nocapture quic_driver::tests
  ungated quic write_buffer: offered=52428800 buffered=44040192 cap=8388608
    (x5.2 the cap); after first burst=17825792 growth=26214400 refused=0
  quic write_buffer with a draining peer: buffered=0 read=26214400
  ```

  25 MiB offered leaves 17 MiB buffered — 2.1x the cap — and offering 25 MiB
  more buffers **all of it** (`growth=26214400`, byte for byte): there is no
  bound to hold at, which is a stronger statement than "it exceeded the cap".
  **Control**: the same harness against a peer that reads leaves
  `buffered=0`, so a clean result is one this probe can report.
- **A fix must** decide the two transports' behaviour together, not separately —
  which is the same decision KI-3 is waiting on. Bounding QUIC the way UDS is
  bounded would give it UDS's silent-drop defect; leaving it unbounded keeps the
  OOM path. Whatever backpressure answer KI-3 takes should cover both, and the
  doc comments are corrected in that change either way.

**RESOLVED by `close-transport-send-buffer-loss`.** The QUIC driver reaches the
same `SendGate` both other send buffers do: `WriteError::Blocked` raises it, a
drain below the low-water mark lowers it, and the ceiling holds the buffer at
`max_write_buffer_bytes`. The two false doc sentences in
`flz_transport/src/transport/config.rs` are true as written now, because the
predicate they describe is what every send buffer calls.

Driven on the in-memory `quinn_proto` pair this entry's reproduction was
replaced by (`cargo test -p flz_nif --lib -- --nocapture quic_driver::tests`):

```
gated quic write_buffer:   buffered=8388608 cap=8388608 raised=true refused=544 raises=1
ungated quic write_buffer: offered=52428800 buffered=44040192 cap=8388608 (x5.2 the cap)
quic write_buffer with a draining peer: buffered=0 read=26214400 refused=0 raised=false
```

The gated buffer holds at the cap exactly with the signal raised; the ungated
control still grows to 5.2x it; a peer that reads leaves nothing behind. The
counter is what the sender crossed the ceiling without honouring the gate,
which on this driver is the harness pushing deliberately.

**Not closed with it: KI-72.** The QUIC driver still produces no `DriverStats`,
so the reporting half of `controller-send-buffer-bound` is UDS-only — stated in
the requirement itself rather than left to be inferred.

### KI-3 · UDS write-buffer drops are invisible to the caller — RESOLVED

- **Severity**: P1 · **investigation first**, then proposal.
- **Where**: the UDS driver behind `Flezha.Transport.Native.send_frame/4`.
- **What happens**: the write buffer's overflow policy is `DropNewest`, and
  `send_frame/4` answers `:ok` regardless. Only
  `driver_stats.write_buffer_dropped` records the loss.
- **Impact**: this is the last silent-drop default in the platform, against a
  stated guarantee that nothing drops by default (see
  `docs/dev_notes/close-silent-data-loss-defaults.md`).
- **Why not proposed yet**: the fix shape is not obvious — the cast path cannot
  simply block without reintroducing the wedge that
  `nonblocking-drop-nothing-backpressure` removed. Needs a design pass first.
- **A fix must**: decide what a caller is told when the buffer is full, without
  parking a host thread.
- **Triage (2026-08-30)**: **REAL**, reproduces at defaults, and the blind spot
  is wider than filed. `enqueue_write` (`flz_nif/src/uds_driver.rs:474`) still
  drops on `would_overflow_write_buffer` and only bumps `write_buffer_dropped`;
  `send_frame` (`flz_nif/src/stream.rs:25`) still answers `atoms::ok()`
  unconditionally, and does not even check the `cmd_tx.send` result (`let _ =
  …`) — so a dead driver thread is a *second* drop path that is not counted at
  all. Correction to the entry: it is not only the caller that cannot see the
  loss. Nothing in `lib/` reads `write_buffer_dropped`.
  `TransportSource.emit_driver_stats/2`
  (`lib/flezha/observability/otp_backend/transport_source.ex:90`) emits
  `write_buffer_len` — the depth — and never the drop counter; no liveness
  verdict names it; the only reader in the tree is the printout at
  `lib/mix/tasks/bench/roundtrip.ex:698`. A production drop is invisible to the
  caller *and* to the platform.
- **Reproduction** — a UDS driver with a peer that connects and never reads. No
  env tuning: the default 8 MiB cap is enough.

  ```elixir
  # /tmp/ki3.exs
  alias Flezha.Transport.Native
  path = Path.join(System.tmp_dir!(), "ki3_#{System.unique_integer([:positive])}.sock")
  {:ok, server} = Native.start_uds_server(path, self())
  # A peer that connects and NEVER reads.
  {:ok, peer} = :gen_tcp.connect({:local, String.to_charlist(path)}, 0, [:binary, active: false, packet: :raw])
  stream = receive do: ({:flz_connection, s} -> s), after: (5_000 -> raise "no connection")
  payload = :binary.copy("x", 64 * 1024)
  replies = for i <- 1..400, do: Native.send_frame(stream, "p#{i}", payload, %{})
  Process.sleep(500)
  # Keep the handle live: a collected StreamHandle closes its stream, and the
  # driver then has no snapshot to report.
  _ = Native.stream_id(stream)
  st = hd(Native.driver_stats(server).streams)
  IO.puts("send_frame replies: #{inspect(Enum.frequencies(replies))} over #{length(replies)} calls")
  IO.puts("frames_written=#{st.frames_written} write_buffer_dropped=#{st.write_buffer_dropped} write_buffer_len=#{st.write_buffer_len}")
  :gen_tcp.close(peer)
  Native.stop_server(server)
  ```

  ```
  $ MIX_ENV=test mix run --no-start /tmp/ki3.exs
  send_frame replies: %{ok: 400} over 400 calls
  frames_written=131 write_buffer_dropped=269 write_buffer_len=8357440
  ```

  269 of 400 frames dropped; all 400 calls answered `:ok`. Deterministic, 3 of
  3 runs. **Control** (proving the probe can report a clean result): the same
  script under `FLEZHA_MAX_WRITE_BUFFER_BYTES=1073741824` prints
  `frames_written=400 write_buffer_dropped=0`.
- **Correction to the harness as first filed (2026-08-31)**: it raised
  `** (ArgumentError) :erlang.hd([])` on 3 of 3 runs, not the numbers above.
  `stream`'s last use was the `for` comprehension, so the `StreamHandle`
  resource was collectable before `driver_stats/1` ran; `StreamHandle::drop`
  sends `CloseStream`, the driver removes the stream, and the snapshot is
  empty. The `Native.stream_id(stream)` line above is what pins it. A harness
  in an entry is only durable if it runs.
- **Worth fixing**: yes. "Investigation first" still stands for the real
  question — what a caster is told — but two things need no design pass and end
  the silence on their own: emit `write_buffer_dropped` from `TransportSource`
  beside `driver_write_buffer_bytes`, and give the stall classifier a clause on
  it. Loud loss is not the fix, but it is strictly better than this.

**RESOLVED by `close-transport-send-buffer-loss`.** `enqueue_write` no longer
discards as a policy: the driver raises a congestion signal on crossing a
high-water mark, `send_frame` reads it and answers
`{:error, :send_buffer_full}`, and `StreamChannel.io_in/3` holds the calling
process until the buffer drains. The ceiling under it is a last resort a sender
honouring the gate never reaches, so `write_buffer_dropped` becomes an
invariant — emitted by `TransportSource` beside `write_buffer_len` and named by
a liveness verdict, which closes the second half of this entry's triage
("nothing in `lib/` reads the counter").

This entry's own harness on the fixed build, three runs, identical:

```
send_frame replies: %{:ok => 106, {:error, :send_buffer_full} => 294} over 400 calls
frames_written=106 write_buffer_dropped=0 write_buffer_len=6717990
```

**Nothing discarded, and every refusal reported to the caller.** This harness
drives the NIF directly, with no channel above it to hold on the refusal, which
is why 294 calls are answered rather than held.

**Negative control** — the gate forced off with the ceiling kept
(`with_watermarks(cap, MAX, MAX)`): this entry's numbers return byte for byte,
`%{ok: 400}`, `frames_written=131`, `write_buffer_dropped=269`,
`write_buffer_len=8357440`. With the ceiling removed as well, nothing is
discarded and the UDS buffer reaches **24.8 MiB against an 8 MiB cap** — KI-56's
failure mode on the other transport, which is what a bound without a signal and
a signal without a bound each get you.

The third drop path this entry's triage added — `let _ = cmd_tx.send(…)` on a
dead driver thread — is answered rather than discarded: `send_frame` returns
`{:error, :driver_gone}` and the caller reports the frame undelivered.

### KI-51 · The bench `--runtime` help names three runtimes, not seven — RESOLVED

Closed by `correct-runtime-and-conformance-docs`. The entry was right about the
count and understated the spread: **two** tasks carried the stale list.

- **The accepted set, read off the code.** `parse_runtime/2`
  (`lib/mix/tasks/bench/roundtrip.ex`) calls `PortPilot.runtimes/0`, which globs
  `test/apps/*_runner/conformance.json` and answers `[:elixir, :java, :node,
  :php, :python, :ruby, :rust]` — seven manifests, confirmed by `ls
  test/apps/*_runner/conformance.json`. The advertised set was `rust|java|elixir`.
- **`Mix.Tasks.Bench` had the same defect, one runtime short.** Its `--runtime`
  bullet named ``java``, ``elixir``, ``node``, ``ruby``, ``php`` and omitted
  `python`, which was added after the line was written, while its own
  `parse_runtime/1` reads the same `PortPilot.runtimes/0`.
- **The `elixir`-only durability sentence was a third copy.** `--runtime elixir
  carries only --durability off` restates a manifest's `deferred` key that
  `parse_runtime/2` already reads per runtime; the same restriction holds for
  `node`, `php` and `ruby`, and `python` defers only `workflow`.
- **The fix is one list, not a corrected second one.** Both help texts now name
  `test/apps/<runtime>_runner/conformance.json` and enumerate nothing; the
  refusals (`parse_enum/3`, `parse_runtime/1`) already print the derived set.
  This is the rule `sdk-conformance` states for the launch line and the dispatch
  model — "a per-runtime clause is a list a new SDK has to remember to join".
- **Gated, and proved red first.** `test/sdk_conformance/gate_test.exs` gained
  *"no bench task documents a runtime list beside the one it derives"*. At HEAD:

  ```
  $ mix test test/sdk_conformance/gate_test.exs
  Mix.Tasks.Bench names ["elixir", "java", "node", "ruby"] on one documentation
  line, a second copy of the set its `--runtime` reads from the manifests:
  ["elixir", "java", "node", "php", "python", "ruby", "rust"].
  Name the manifest path, not the runtimes
  Result: 18/19 passed
  ```

  Each half was proved separately: with only the `roundtrip.ex` line reverted the
  same test reports `Mix.Tasks.Bench.Roundtrip names ["elixir", "java", "rust"]`.
  With both corrected, `19 passed`.

### KI-13 · The PHP container arm needs `--network host`, and leaks containers — RESOLVED

Closed by `correct-runtime-and-conformance-docs` as an **environment note made
findable**, which is what its own triage recommended. No repo code changed.

- **The triage's verdict stands: not a defect here.** No shim is tracked — `git
  ls-files` matches no PHP shim — and `test/apps/php_runner/conformance.json`
  launches a bare `php {artifact} {namespace}`, so the container arm is a `PATH`
  arrangement an individual makes, not something the tree ships.
- **One triage fact is now false and is corrected rather than carried.** The
  entry recorded "there is no PHP on this box at all (`command -v php` →
  absent)". It resolves today:

  ```
  $ command -v php && php -v | head -1
  /usr/bin/php
  PHP 8.3.6 (cli) (built: Jul 16 2026 18:30:41) (NTS)
  $ php -m | grep -i ffi
  FFI
  ```

  That satisfies `sdk/php/composer.json` (`php >= 8.1`, `ext-ffi`) for the
  default `sync` executor, so the shim is not the only way to run the arm on
  this box. It remains the only way to run `FLEZHA_PHP_EXECUTOR=swoole`, which
  needs a ZTS build carrying `Swoole\Thread`. The arm was **not run** — that is
  a live conformance run, outside this change.
- **The remedy.** `docs/sdk/conformance.md` gained *"What an arm needs on the
  host"*, covering the Ruby interpreter floor and the PHP shim: the `docker run`
  invocation with `--network host` and `-i`, the three mounts at their same
  absolute paths, the `FLEZHA_*` forwarding, why each is load-bearing, and the
  container sweep the `wedge_probe` fixture forces. Every fact is cited to its
  source (`sdk/ruby/flezha.gemspec`, `sdk/php/composer.json`,
  `docker/dev/Dockerfile.php`, the PHP manifest); where
  `docs/dev_notes/refactor-2-ffi-veneers.md` records no exact spelling — the
  mount flags and the forwarded variables — the section states the requirement
  rather than inventing a command line.
- **The second remedy's condition is unmet, and that is the outcome.** The entry
  asks for a `--rm`-plus-name teardown *"if the shim ever becomes a tracked
  script"*. It has not, and none was written. The sweep is documented instead.
- **Nothing is leaked now**: `docker ps` lists no running containers.

### KI-67 · [[KI-66]]'s regression test passes on the defective helper — RESOLVED

Closed by `green-the-test-hygiene-tail` segment C, which **replaced the
construction rather than widening the band**.

- **What was wrong.** `test/flezha/state/test_helpers_test.exs` planted a
  512-wide band of directories at `flezha_state_test_<counter+1 … counter+512>`
  and asserted the mint was not one of them. `System.unique_integer([:positive])`
  is node-global **and per-scheduler striped**, so the value the test drew and
  the value the mint drew afterwards were not adjacent: the whole node draws
  from the same stream while the 512 directories are being written, and a
  scheduler migration moves the draw to a different stripe. Drift across five
  probe runs: `+6365`, `+829`, `+509`, `+29` and **`−260`** — a negative drift
  puts the mint *below* the band entirely. The vacuity guard
  `assert minted > counter + band` held in both the caught and the missed case,
  so it separated nothing, and the failure was one-sided: never a false red,
  only a false green. Measured **15 red of 20 seeds** by the worker and **7 red
  of 12** by the supervisor, independently.
- **The fix is a real second BEAM, which is what the defect is.** The child is
  `elixir -pa <flezha ebin> -e <script>`; it draws until it is past a floor of
  `4096` so the band the parent must plant is known here (a boot alone lands
  anywhere in `99..1417` on this box, which is why the first attempt still
  missed), reports the stream head and the stride it measured, then calls
  `Flezha.State.TestHelpers.tmp_app_data_dir/0` and prints what it got.
- **The vacuity guard now discriminates**, and that was demonstrated rather than
  argued: `assert (head + stride) in @band` names the first candidate the claim
  asks for, so a band the stream never reached fails **loudly**. In the run
  where the band was deliberately mis-sized, one of twelve seeds drew `325`
  against a band of `1024..2048` and the test reported
  `the claim asked for 325 first, outside the planted band 1024..2048` — the
  exact case in which the old test returned a green.
- **Acceptance, and it is 12 of 12, not a majority.** With
  `NodeTestHelper.claim_tmp_dir/1` reverted to `File.mkdir_p!/1`, at the twelve
  seeds the supervisor named:

  ```
  $ for s in 1 2 3 5 8 13 21 34 55 89 144 233; do
      mix test test/flezha/state/test_helpers_test.exs --seed $s; done
  seed=1    exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4162
  seed=2    exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4163
  seed=3    exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4164
  seed=5    exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4165
  seed=8    exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4162
  seed=13   exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4169
  seed=21   exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4165
  seed=34   exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4164
  seed=55   exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4165
  seed=89   exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4165
  seed=144  exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4165
  seed=233  exit=2  handed back a state root it did not create: /tmp/flezha_state_test_4164
  ```

  **12 of 12 red, and every one on the assertion the test exists for** — not on
  the vacuity guard. Restored: 12 of 12 green at the same seeds, 0.4 s each.
- **Robust to `/tmp` residue in both directions**, which the old test was not:
  the band is planted with `mkdir_p!` so leftovers inside it are absorbed, and
  the claim walks past leftovers outside it. Proved with 292 foreign trees
  planted at `4609..4900`, immediately adjacent to and past the band: 1 passed.
- **The process lesson, recorded because it is the real cause.** At segment A's
  checkpoint the supervisor ran the full suite and read the fix, and accepted
  the worker's negative-control claim **without running it**. A regression test
  is proved by its negative control and by nothing else. This also invalidated a
  claim in commit `656fd444`'s message; the KI-66 *fix* was never in doubt —
  `File.mkdir/1` is atomic by construction, not by sampling — but its *proof*
  was, and a test that green-lights defective code two times in five is worse
  than no test, because it will be trusted.

### KI-66 · A `/tmp` state tree left by a previous BEAM turns `append(pid, 0, …)` into `:duplicate` — RESOLVED

Closed by `green-the-test-hygiene-tail` segment A (`656fd444`) for
`tmp_app_data_dir/0`, and by segment B (`9820f75d`) for the four sibling mints.

- **Where it was**: `test/helpers/state_test_helpers.ex` (`tmp_app_data_dir/0`)
  and `test/flezha/state/wal_writer_outbox_test.exs:53` (`rc = "wo_" <> …`).
  Both mint their name from `System.unique_integer([:positive])`, which
  **restarts low in every BEAM**, so run *N+1* re-mints run *N*'s directory
  names. `File.mkdir_p!/1` on an existing directory succeeds silently, and the
  fresh writer then reads a WAL the previous run left there.
- **What happened**: `WalWriter`'s append handler
  (`lib/flezha/state/wal_writer.ex:547`) answers `:duplicate` for
  `seq <= state.last_persisted_seq`, so a test that starts at seq 0 over a
  re-minted tree fails on its own first append:

  ```
  1) test a held output holds the cursor until the sink confirms (Flezha.State.WalWriterOutboxTest)
     test/flezha/state/wal_writer_outbox_test.exs:605
     ** (MatchError) no match of right hand side value:

         :duplicate

     code: :ok = WalWriter.append(pid, 0, effect_header(), effect_body([output("k", "held")], 1))
  ```

  Seen once — `mix test --seed 202202`, exit 2, `1856/1857 passed`, with **124
  `/tmp/flezha_state_test_*` trees on the box**, eight of them holding a
  `_system/wo_<n>/proc/k1.wal` at exactly the path that module mints
  (`.../flezha_state_test_34051/_system/wo_34083/proc/k1.wal` — base and `rc`
  32 apart, which is this box's `unique_integer` stride).
- **The failing test was the messenger, not the defect.** All 47 call sites of
  `tmp_app_data_dir/0` were exposed the same way; the WAL path is only where a
  stale byte is loudest.
- **Supervisor verdict**: every link was verified before ruling — the mint, the
  silent `File.mkdir_p!/1`, and `wal_writer.ex:547`'s
  `seq <= state.last_persisted_seq -> {:reply, :duplicate, state}`. A per-BEAM
  counter, a silent mkdir and a seq-ordered dedup is a complete causal chain and
  needs no sampling to be believed.
- **Fixed by making the mint a CLAIM**: `File.mkdir/1` instead of `mkdir_p!/1`,
  retrying on `:eexist`. No new naming scheme, no call site changed, and the fix
  does not depend on `/tmp` being clean — which was the binding constraint,
  since leftovers are the whole defect. It also makes two concurrent BEAMs
  unable to share a state root, where `mkdir_p` let them.
- **One claim, five callers.** Segment B generalised it into
  `FlezhaTest.Helper.NodeTestHelper.claim_tmp_dir/1` — the lowest layer among
  the test helpers, and already the owner of the sibling concept `peer_dir/2` —
  and pointed the four sibling mints at it: `schedule_test_helper.ex:20`,
  `durability_backend_stub.ex:61`, `auth_test_helper.ex:40` and
  `tier_e2e_harness.ex`'s `unique_id_path/1`. Every minted name is byte-identical
  to before, so the claim is the only change; net 0 lines (`+20` in the helper,
  `−20` across the callers). `node_test_helper.ex`'s `peer_dir/2` is deliberately
  **excluded**, with a comment saying why: its name carries no per-BEAM counter
  to re-mint and is meant to be stable per peer.
- **`unique_id_path/1` claims a directory and returns a path inside it**,
  because there the file itself must be absent — a runner that finds an id at
  `FLEZHA_RUNNER_ID_PATH` adopts it — and `File.mkdir` is the atomic claim a
  bare file path has no equivalent of.
- **A shared mechanism with [[KI-23]] was refused, and the refusal is right.**
  The subjects differ: KI-23 holds a **length** under a 32-byte id cap, KI-66
  establishes **exclusive ownership of a filesystem name**. An operative id
  cannot be claimed at mint time — nothing exists on disk when the generator
  returns, so there is no `mkdir` to fail and no retry-until-free to perform.
  And the one scheme that would serve both, a cross-BEAM-monotonic suffix like
  `System.os_time(:microsecond)`, **spends KI-23's entire budget**: 16 digits
  plus a separator leaves 15 bytes of 32. What the two share is the diagnosis,
  and that is written once.
- **The planted-record probe used during triage is NOT the regression test**,
  and cannot be: it plants at a path the helper has already returned, so it
  asserts `WalWriter`'s dedup, which is correct behaviour. It answers
  `:duplicate` against the fixed helper too.
- **The regression test is `test/flezha/state/test_helpers_test.exs`**, and its
  first form was itself defective — see [[KI-67]], closed in segment C. It now
  forces the collision in a real second BEAM.
- **This changes a measurement `green-the-suite-log-gate` relied on**: the helper
  no longer reclaims a stale name, so a run can no longer show a *net loss* of
  `/tmp/flezha_*` directories. The timestamp-marker method that note prescribes
  is unaffected and remains the right one.

### KI-23 · `unique/1` suffixes can breach the operative-id cap — RESOLVED

Closed by `green-the-test-hygiene-tail` segment B (`9820f75d`): **the generator
carries its own bound**, so no rule about call sites has to hold it.

- **The filed remedy was unenforceable, and that is the part worth keeping.**
  "Hold prefixes to ≤ 23" cannot be checked at the call sites, because **three
  of the five are not literals**: `workflow_crash_resume_e2e_test.exs` truncated
  a *factory id* by hand, `sdk_smoke_e2e_test.exs` did the same a second time
  with a different width, and a third site computed `unique("pipe_#{factory}")`
  with no limit at all. A fourth carried a private second copy of `unique/1`.
  The bound belongs in the one function they all reach.
- **Two numbers in the original entry were off.** The breach starts at **8**
  digits, not 7 (24 + 1 + 7 = 32 is `:ok`; 24 + 1 + 8 = 33 is
  `{:error, "operative id exceeds 32 bytes"}` against the real
  `Flezha.Service.Validator`), and no prefix at HEAD exceeded 24, so "any prefix
  over 24 characters" named a population that was empty.
- **The fix**, `TierE2EHarness.unique/2` (`test/helpers/tier_e2e_harness.ex:195`):

  ```elixir
  def unique(prefix), do: unique(prefix, System.unique_integer([:positive]))

  def unique(prefix, counter) do
    suffix = "_" <> Integer.to_string(counter)
    binary_slice(prefix, 0, max(Validator.id_byte_limit() - byte_size(suffix), 0)) <> suffix
  end
  ```

  Exact and unconditional — the result fits at *any* counter width, where a
  reserved digit count only moves the cliff — and `32` is never written down,
  because `Validator.id_byte_limit/0` already surfaces
  `Flezha.Constant`'s `@operative_id_max_bytes`.
- **Its red-first is a negative control, not a natural red, and that is stated
  rather than dressed up.** The natural red needs a counter ≥ 10⁷ ≈ 281,250
  `unique_integer` calls at this box's stride of 32 — about **167× a full plain
  suite**. Reverting only the `binary_slice`: 3 of 5 contract tests red, first
  failure `24-byte prefix at 8 digits: checkpoint_batched_store_99999999 is 33
  bytes`, exactly the predicted row.
- **Inertness was proved, not asserted**: all **40** literal `unique("…")`
  prefixes in the tree × counter widths 1–7 = **280 pairs, 280 byte-identical,
  0 differing**. The inertness test is green in *both* arms of the negative
  control, which is what makes it an inertness assertion rather than a second
  bound assertion. Exactly **5** ids change, all from the two over-conservative
  hand truncations, all getting *longer*, all still inside 32 by construction.
- **Deletion-funded**: the private `unique/1` copy, two hand truncations with
  their comments, and **four** private `unique_id_path/0` copies (the design
  named one) collapse onto `unique_id_path/1` with a neutral default, leaving 15
  existing call sites untouched. Net over the three system modules: **−12**.
- **What remains, and is already refused**: the *partitioned* path has less
  headroom, because `lib/flezha/operative/partition_group.ex:111` requires
  `member_id(base, N-1)` to fit in 32, so the `#pK` suffix (3–4 bytes) comes out
  of the same budget. A partitioned start with a 32-byte base id is refused
  today by `id_budget_refusal/2` (`:125`–`:131`), which names the byte counts
  and the limit. Nothing is owed there.

### KI-21 · Three uncaptured `[error]` lines in a plain `mix test` — RESOLVED

All three lines are accounted for and none reproduces. The entry was **real
exactly as filed** — CI's gate (`.github/workflows/elixir.yml:150`–`:163`) is a
`grep -nE '\[(error|critical)\]'` over the run's own log and any hit is exit 1,
so the `mix test` job was red on a suite that was otherwise wholly green.

| filed line | what closed it |
|---|---|
| two sdk_lang-mismatch restore refusals | `green-the-suite-log-gate`. Both now run inside `ExUnit.CaptureLog.with_log/1` and **assert on what was captured** — that the line says `restore REFUSED`, names the Entity and carries the SDK's own reason: `test/flezha/operative/restore_refusal_test.exs:20` (the import), `:75` and `:118` (the two calls). `capture_log` without an assertion, and `@moduletag capture_log: true`, were both refused — they make a run quiet by making the module deaf |
| one `ControlUnit` abnormal shutdown | a **production** ordering defect, re-filed as KI-48 (`RunnerSupervisor` lists `ControlChannel` before `ControlUnit`, so the reverse-order terminate sweep stops the channel under a unit blocked in `GenServer.call`) and closed by `finish-the-suite-log-gate` |

**Verification, six full `mix test` runs at random seeds**, each redirected to
its own file and each grepped with CI's command verbatim. All six: exit 0,
`1857 passed, 121 excluded`, ~57 s.

| run | seed | result | gate hits |
|---|---|---|---|
| 1 | 44367 | 1857 passed | **0** |
| 2 | 462837 | 1857 passed | **0** |
| 3 | 109802 | 1857 passed | **0** |
| 4 | 844813 | 1857 passed | **0** |
| 5 | 147075 | 1857 passed | 2 — both [[KI-54]], now fixed |
| 6 | 253624 | 1857 passed | **0** |

**None of the three filed lines appears in any of the six.** The only hits were
the two KI-54 lines in run 5, which `green-the-test-hygiene-tail` segment A
closed; `green-the-test-hygiene-tail`'s own batch-7 runs at the fixed helper are
quiet.

**The gate itself was proved, not assumed** — it had never been seen to pass, so
a green run only showed it was quiet. A scratch test emitting one uncaptured
`Logger.error` turned CI's grep **RED while `mix test` still exited 0**, which is
the whole point of the gate; the same test wrapped in `capture_log` with an
assertion turned it **GREEN**. The scratch test was then deleted.

**The lesson this entry kept re-learning, and which stands**: *the population is
whatever the last N runs sampled, and N is always too small.* Its own count of
three was one run's sample. KI-48, KI-49, KI-50, KI-53 and KI-54 were each found
afterwards by the same gate, each in roughly 1 of 6 runs. All five are now
closed; KI-53 did not fire in these six runs and was closed on a driven
reproduction instead.

### KI-54 · A `ForwardWorker` retry outlives its test — RESOLVED

Closed by `green-the-test-hygiene-tail` segment A (`656fd444`).
**The filed landing site was wrong, and half the filed remedy with it** — that
is the part worth keeping. The entry named
`test/helpers/fake_start_registry.ex:186` (`pid_of/2`) and an ETS
`ArgumentError`; the sighting at this row's own HEAD was a `FunctionClauseError`
in a narrow Mimic global stub owned by a *different* module —
`test/flezha/service/operative_manager_request_stop_test.exs:71` and `:113`,
raised for the operative ids `"stuck_28962"` and `"reuse_29250"` that
`operative_forward_test.exs:281`/`:296` mint:

```
[error] Process #PID<0.8639.0> on node :"test_agent_node1@127.0.0.1" raised an exception
** (FunctionClauseError) no function clause matching in anonymous fn/2 in
   FlezhaTest.Service.OperativeManagerRequestStopTest."test stop_operative releases both
   the operative and entity quota"/1
    test/flezha/service/operative_manager_request_stop_test.exs:113: anonymous fn("default", "stuck_28962")
    lib/flezha/service/operative_manager.ex:53: … get_operative_pid/3
    lib/flezha/service/operative_manager.ex:597: … send_io_confirming/5
    lib/flezha/plugin/provider/operative_forward/forward_worker.ex:209:
      anonymous fn/7 in …ForwardWorker.deliver_next/1
```

So "make the registry lookup answer *gone* instead of raising" would have closed
one door in an open corridor: the leaked worker calls `HubManager.get_pid/2`
under Mimic **global** mode, and what answers is whatever stub the suite has
installed at that instant — two different modules in two sightings, and any
future one. It would also have made that helper lie about a genuinely deleted
table.

**The verified mechanism**, each link read at HEAD:

| link | evidence |
|---|---|
| `ForwardSup` is application-owned, not test-owned | `lib/flezha/application.ex:90` lists it; `MIX_ENV=test mix run -e '…Process.whereis…'` → `#PID<0.315.0>` |
| so the module's own guard never fired | its `unless Process.whereis(ForwardSup), do: start_supervised!(ForwardSup)` — the `unless` was always false. Proved unreachable and **deleted**: left beside a detector reading the same registry it would claim the module might own the supervisor |
| 26 tests created a worker, 1 stopped one | 26 `prepare/3` call sites, one `ForwardSup.stop_worker/1` |
| a refused delivery retries for ever | `forward_worker.ex:266` arms `retry_timer`; `:17` states the guarantee, "payloads are never dropped" |
| the raise is uncaptured | `forward_worker.ex:206`–`:212` is a bare `spawn` whose `try` catches `:exit` only, so a raise inside it is reported by the default error logger — an uncaptured `[error]` on a run where every test passed |

**The fix is row 2's remedy for KI-49, not a hardened lookup: the background
work stops with the test that started it.**
`test/flezha/plugin/provider/operative_forward_test.exs`'s `setup` (`:30`–`:51`)
now sweeps every key in `ForwardSup.registry_name()` on exit and asserts on entry
that the set was empty, naming the survivors.

**The acceptance is the registry, not the log.** The defect showed through the
log 1 run in 6 and through the registry **every** time; converting a sampled
symptom into a deterministic one is the whole reason the fix can be believed.
Detector red at **1 of 28** tests before the sweep, module green at 28 after.
Two deviations from the design, both forced by measurement: the sweep repeats
until the registry stays empty (`WorkerSup.ensure_worker/4` drops a key before it
re-registers the replacement, so a single `Registry.select` can miss a live
worker, which left 8 of 28 still failing), and the `on_exit` is armed **before**
the assertion, or the first leak disarms the sweep for every test behind it —
which is why one key appeared in 20 consecutive failures.

**The production question the entry asked is answered, and the answer is no
change.** `deliver_next/1`'s `try` still catches `:exit` and not `:error`:
`forward_worker.ex:176`–`:177` names the reason — a dead delivery process is
indistinguishable from an unreachable target, so it is retryable — and widening
it to `:error` would convert a real raise into a silent retry loop, the same call
`finish-the-suite-log-gate` made on KI-53. `ForwardSup` also stays in the
application supervision tree: production wants it there, and a test's hygiene is
not a reason to change a supervision tree.

### KI-63 · `.credo.exs`'s `test/apps/` exclusion is inert — FIXED

- **Severity**: P3 · one anchored regex; it inflates every credo count by
  whatever `test/apps/` carries and has never excluded anything.
- **Where**: `.credo.exs:9` —
  `excluded: [~r"/_build/", ~r"/deps/", ~r"/node_modules/", ~r"^test/apps/"]`.
  The first three are unanchored and work; the fourth is anchored at `^` and
  cannot match, because credo filters **absolute** paths.
- **Reproduction** (the probe file is untracked and is deleted again):

  ```
  $ cat > test/apps/elixir_runner/lib/zz_credo_probe.ex <<'EX'
  defmodule ZzCredoProbe do
    def probe do
      Flezha.State.Paths.state_root()
    end
  end
  EX
  $ MIX_ENV=test mix credo --strict --format oneline | grep zz_credo_probe
  [R] → test/apps/elixir_runner/lib/zz_credo_probe.ex:1:11 Modules should have a @moduledoc tag.
  [D] ↘ test/apps/elixir_runner/lib/zz_credo_probe.ex:3:5 Nested modules could be aliased at the top of the invoking module.
  $ rm test/apps/elixir_runner/lib/zz_credo_probe.ex
  ```

  And the regex itself:

  ```
  $ elixir -e 'p = Path.expand("test/apps/elixir_runner/lib/factories.ex"); \
      IO.inspect(String.match?(p, ~r"^test/apps/"))'
  false
  ```

- **Mechanism**: `Credo.Sources.convert_globs_to_local_paths/2`
  (`deps/credo/lib/credo/sources.ex:132`–`:139`) runs `Path.expand/2` on a
  **string** pattern and passes a `%Regex{}` through **untouched**. The set it
  then filters holds absolute paths, and the regex arm of `exclude/2` (`:184`)
  is a bare `Enum.reject(&String.match?(&1, pattern))`. So a repo-relative
  anchored regex is dead on arrival, while `~r"/deps/"` matches
  `/home/moon/Code/flezha/deps/credo/lib/credo.ex` because it is unanchored.
  Measured consequence at `1fe088ad`: the HEAD `AliasUsage` count of **689**
  includes **14** findings under `test/apps/elixir_runner/`, which
  `fix-opaque-module-names` batch 8 therefore had to fix to reach "0 anywhere".
- **A fix must**: decide what the file *means* before changing it. Dropping the
  `^` (or writing `~r"/test/apps/"`) makes the exclusion real and **lowers every
  credo count**, which moves the KI-10 ratchet number that row 3.5's segment D
  is about to record — so the two must land together or the baseline is recorded
  against a file set that changes underneath it. The alternative is to delete the
  entry and state that `test/apps/` is in scope, which is what the tree has
  actually been doing since the line was written.
- **Not fixed here**: `.credo.exs` is segment D's file and segment C was
  instructed not to edit it.

- **Supervisor verdict 2026-08-31 — real, confirmed from source, and fixed in
  segment D alongside the ratchet.** Verified without a probe: `include/2` builds
  the file set from `Path.expand`ed patterns, so it holds ABSOLUTE paths, while
  `convert_globs_to_local_paths/2` (`deps/credo/lib/credo/sources.ex:132`) expands
  only `is_binary` patterns and passes a `%Regex{}` through unchanged; `exclude/2`
  (`:183`) is then a bare `Enum.reject(&String.match?(&1, pattern))`. A `^`-anchored
  *relative* regex cannot match `/home/…/flezha/test/apps/…`. The three siblings
  work only because `~r"/_build/"`, `~r"/deps/"` and `~r"/node_modules/"` are
  **unanchored** — the config's one anchored entry is its one dead entry.
- **Why segment D and not later**: the fix must land with the ratchet or the
  baseline is recorded against a file set that shifts underneath it. It is also the
  cheapest possible moment — a full `--strict` run right now reports **zero**
  findings under `test/apps/`, so honouring the exclusion moves the number by 0
  today. Deferring it turns a no-op into a silent ratchet shift the first time
  anyone writes a flaggable construct in a test app.
- **Fix the mechanism, not the policy.** The line's intent is unambiguous, so it is
  honoured by making the regex unanchored. Whether `test/apps/` *should* be outside
  credo's view is a different question and belongs to `refactor-7-tests`, which owns
  the test apps. The 14 `AliasUsage` sites segment C already corrected under
  `test/apps/` stay corrected either way — they are right regardless of who looks.
- **Fixed** 2026-08-31 by `fix-opaque-module-names` segment D, in the same commit as
  the ratchet, because the two cannot be separated: `~r"^test/apps/"` became
  `~r"/test/apps/"`, the spelling its three working siblings already use, and the
  `excluded:` list carries a comment saying every pattern there is matched against an
  **absolute** path. The mechanism is fixed, not the policy — whether `test/apps/`
  belongs outside credo's view stays `refactor-7-tests`'s question.
- **Proved two-sided, because a one-sided check shows an exclusion is quiet, not that
  it works.** One scratch module, byte-identical, dropped in two places at once and a
  single `MIX_ENV=test mix credo --strict --format oneline` run over both:

  ```
  $ # test/apps/elixir_runner/lib/zz_credo_probe.ex  AND  test/zz_credo_probe_outside.ex
  $ grep -c 'test/apps/' credo_probe.txt
  0
  $ grep -n 'zz_credo_probe' credo_probe.txt
  379:[R] → test/zz_credo_probe_outside.ex:1:11 Modules should have a @moduledoc tag.
  380:[D] ↘ test/zz_credo_probe_outside.ex:3:5 Nested modules could be aliased at the top of the invoking module.
  ```

  The construct outside `test/apps/` is reported by both checks; the identical
  construct inside it is reported by neither, and no line of the whole run names
  `test/apps/` at all. Both probes were then deleted and `git status` is clean of them.
- **It moved the ratchet by 0, exactly as predicted.** The measured post-fix total is
  **378** and `test/apps` records `credo_warnings: 0` in `docs/refactor/baseline.json`
  — the tree carries no flaggable construct under `test/apps/` today. Deferring the
  fix would have turned that 0 into a silent ratchet shift the first time anyone wrote
  one.



### KI-62 · `convert_value/2` does not run on the struct plugin path — NOT A BUG

- **Raised**: 2026-08-31 by the `fix-opaque-module-names` segment-B live drive,
  which hit it before the drive was written correctly.
- **Where**: `lib/flezha/plugin/provider_behaviour.ex:334` and `:538` — `build/1`
  takes a `string_map` and is the only caller of `convert_value/2`
  (`:356`). A plugin handed to `builder.opts[:plugins]` as a `%Flezha.Plugin{}`
  never crosses `build/1`, so its `opts` map reaches the provider as written.
- **Observed**: `%Plugin{opts: %{"recv_pid" => "flz_segb_probe"}}` delivered the
  binary straight to `Process.send/3`, which raised
  `[Extension.Table] transform subscriber segb_sink raised :error: :badarg` —
  inside the sink chain, not as a refusal at enable.
- **Why it is not a defect**: the two paths have two contracts and both are
  written down. `provider_behaviour.ex:78` says `convert_value/2` exists because
  *"CLI plugin opts arrive as"* strings — it is the CLI adapter's coercion step,
  which is why its only caller takes a `string_map`. The struct path's contract
  is typed opts, and the provider states it: `beam_msg_forward.ex`'s moduledoc
  reads **"Required opts: `:recv_pid` (pid)"**. Passing a binary there is a
  caller violating a documented contract, not a layer failing to convert.
- **The residual, recorded and not filed as work**: the contract is enforced by
  a raise deep in the sink chain rather than a refusal when the plugin is
  enabled, so a caller's mistake surfaces far from its cause. That is a
  diagnosability wart on a first-party test provider, worth knowing before
  someone reads the same `:badarg` and starts hunting a routing bug. It is not
  worth a refusal layer on a path whose callers are all in-tree.
- **Not caused by, and not related to, the module renames or folds.**


### KI-45 · `PrometheusRegistry` re-encodes a metric label the same way the OTLP codec did — RESOLVED

Closed by `close-prometheus-label-coercion` (2026-08-31). What the entry got
wrong, recorded because a resolved entry that still misstates its cause misleads
as much as an open one:

- Its severity paragraph — "every metric source in the tree emits scalars
  today… a live crash waiting on one new source" — was **false**. Two live
  `TelemetryBridge` paths carry a non-scalar: `wal_backpressure`'s 3-tuple
  `entity_id` and `operative_forward.retry`'s tuple `reason`.
- The blast radius was **five ETS tables plus the Export supervisor**, not one
  dropped signal: the registry owns all five, so a crash wiped every metric
  since boot, and four crashes in a measured **376 ms** exceeded the 3-in-5 s
  intensity and took the listener, sink and registry down together.
- Its UTF-8 bullet was half wrong. A clean OTLP body is **never** valid UTF-8 —
  a fixed64 timestamp guarantees it, measured 0 of 20 on both signal kinds — so
  `String.valid?` was never diagnostic there; only the raw bytes' presence was.
- Filed *"Not driven"*. It is now driven end to end, and three further failures
  were found that do **not** raise and that a crash-only fix would have left
  standing: a list label rendering empty with raw bytes in the body, a key with
  a space rendering an exposition Prometheus rejects whole, and an `le` key
  rendering a duplicate label on a histogram.

The fix is one term→string rule (`Signal.metadata_text/1`) with four callers,
plus a Prometheus label-**name** rule, and it costs 1216 → 2166 ns/emit (1.78×)
after the fast path — the first cut was 4.85× and was measured before shipping.

### KI-59 · `flz observe tail --json` carries a fourth copy of the term→string rule — RESOLVED

Closed by `close-prometheus-label-coercion` (2026-08-31), key half only.
`CliManager.stringify_keys/1` is now the rule's fourth caller. **The value half
survives on purpose**: `inspect_or_pass/1` passes binaries, numbers and booleans
through as native JSON types, so `%{count: 1}` renders `{"count":1}`; collapsing
it onto the text rule would stringify every number and boolean in the CLI's
output. That is the same argument `AuditLog.Codec.stringify_value/1` carries.


| Issue | Closed by |
|---|---|
| KI-33 · Java workflow resume re-runs committed producers | `java-diamond-resume` (`4de341b5`) — **and its diagnosis was wrong in both halves: nothing was re-run, and the defect is not in resume.** The mechanism is one line of the emit funnel: `ActivationDispatcher.EmitContext.collectEdgeOutputs` added one `Edge` output **per consumer** of the producing channel. That is the correct *stream* shape — `flz_core`'s `host/activation.rs:509` destructures `Edge { channel, consumer }` and routes on it — and the wrong *workflow* one: `host/workflow.rs:70` matches `Edge { channel, .. }`, **discards the consumer**, collects every output as one released value, and the driver then fans that batch out to each child itself (`driver.rs::run_iteration` → `Children::accumulate`). `DiamA`'s `dfan` has two consumers, so every downstream node's work doubled. The fix is the arm the Rust SDK already has (`WorkflowCollectAdapter::emit`, one `sink.collect(value)`): on a structured workflow class one emitted value becomes exactly ONE edge output on the producing channel, untransformed and unkeyed, and each consumer evaluates its own edge at delivery in `valuesForEntity` — the seam a live run and a journal re-seed both cross. **Two probes refuted the filed diagnosis**, both on a `java_runner.jar` rebuilt from HEAD. (1) The journal holds exactly **one** `proc_committed` per `proc_idx` 0…4, every one at `iter=0`, on both runtimes — no procedure was re-run and no journal gate failed. What it shows instead is proc 0 releasing **two** byte-identical values on one channel (`handoff_n=2`, `outbox_n=1`, bytes `[17,17]`) where rust writes `handoff_n=1`; procs 1–3 `handoff_n=2` and the join `outbox_n=4` against rust's `2` all follow from that. (2) **The same 9 payloads arrive with no crash at all** — one runner, `FLEZHA_WORKFLOW_E2E_RELEASE` set, no SIGKILL, no rebind, no resume — `[a, c, c, b, b, j:c, j:c, j:b, j:b]` with the same `handoff_n=2` at proc 0. So the entry's *"the resume re-runs producers whose handoff was already journaled"* and its *"a fix must journal-gate the Java resume"* are both refuted; the resume-skip contract was working the whole time. The `@tag skip:` that hid this is deleted, not narrowed, and a **no-crash** diamond scenario now runs beside the crash one in `test/system/workflow_crash_resume_e2e_test.exs` so the next reader cannot re-acquire the resume reading. Red-then-green, measured: that file on java was **17/21** before the fix (both diamond cells and both no-crash cells failing, uds and quic) and **21/21** after, with rust **21/21** either way; the narrowest proof is `WorkflowDispatchTest.workflowStageReleasesOneEdgeOutputPerValue`, red on HEAD with `Expected size: 1 but was: 2` over `Edge[channel=1, consumer=1]` and `Edge[channel=1, consumer=2]`. The shared contract was what was wrong — `sdk-poll-core`'s producer-side rule stated the stream shape unconditionally — so it is amended for every SDK, and `workflow-branching` gains the scenario that a channel's consumer count SHALL NOT multiply the released batch. One defect found on the way and deliberately not fixed: KI-55. **The generalisable lesson, recorded here because `unmask-java-resume-test` was judged not to warrant a dev note of its own (area 8, 2026-09-01): a runtime-swapping tag can make a test never run the runtime it exists to exercise.** The `@tag skip:` did not merely hide a failure — it meant the java arm of a java defect went unrun, so the filed diagnosis survived on evidence nobody could contradict. Check what a skip actually stops running before trusting what a test reports. |
| KI-48 · `ControlUnit` exits abnormally whenever a runner tree comes down | `finish-the-suite-log-gate` — `ControlChannel.send_control/3` now wraps its `GenServer.call` in the two-clause `catch` `ControlUnit.ping_runner/2` already used one layer up: `:exit, {:timeout, _} -> {:error, :timeout}` and `:exit, reason -> {:error, {:control_channel_exit, reason}}`. Design D1's Shape B, in the single funnel all six `ControlUnit` `handle_call`s cross; no caller changed (nine sites read, each already carrying an `{:error, reason}` arm) and the child list was **not** reordered — starting `ControlChannel` first would open a window in which `handle_info`'s `GenServer.cast` to an unregistered `ControlUnit` returns `:ok` and drops an SDK reply, trading an `[error]` line for a silent drop. **Two corrections to this entry, both measured.** (1) Its premise was **inverted**: `runner_supervisor.ex:18`–`:39` lists `ControlUnit` **first** and `ControlChannel` third, and the reverse-order terminate sweep is what stops the channel first — so its own fix wording ("stop `ControlUnit` before `ControlChannel` in the child list") reads as *list it earlier*, which is already the case and would change nothing; to stop it first you must move it **later**. (2) Its reproduction claim did not hold: `test/flezha_test.exs` alone printed the line in **0 of 2** runs with the guard removed, against the "2 of 2" recorded when it was filed. The evidence is the full run instead — one hit at baseline, **zero** across four post-fix full runs including two on fixed seeds. Both new cases were run **red first** (`1/3 passed`: the caller *exits* rather than being answered) and are green after |
| KI-49 · `overwatch_suspension_test` leaks a retrying task onto the next test's stub | `finish-the-suite-log-gate` — the six tests that spawn a real bind task now `await_migrate_task/1`, which reads the `%{pid: pid}` handle they already hold and asserts `{:migrate_task_result, ^pid, {:park, _parked, {:ping_failed, _}}}`. That is strictly stronger than the `assert migrate_task != nil` it follows, and the `^inst` pin is untouched — the entry forbids loosening it and nothing did. **Two corrections.** (1) **Six tests leak, not one**: `:116`, `:155` (the one filed), `:185`, `:275`, `:320` and `:330`; the last two spawned through `stub_rebind/0` and **discarded the state** as `{:reply, :ok, _}`, so their leak was invisible to anyone grepping for `migrate_task != nil`. (2) The `startup attempt 1 failed … — retrying` line the entry quotes is **unreachable while the test is running**: the module's `setup` overrides `:overwatch_retry_backoff_ms` to `[1]`, so `rest == []` on the first attempt and there is no ladder. It appears because the orphan reaches `resolve_runner/4` only after its test's `on_exit` has **restored the production backoff** — which is the mechanism by which the task survives long enough to reach the next test at all. Measured: **3 of 10** seeds red at HEAD (777, 432113, 5150 — each `20 passed`, exit 0, two `[error]` lines), **0 of 10** on the same ten seeds after, at an unchanged 1.6 s. The await is proved load-bearing: expecting a wrong reason turns exactly those six red |
| KI-50 · `Extension.PhaseTest` "uncertain health probe" fails about 1 run in 3, in-suite only | `finish-the-suite-log-gate` — the structural defect is fixed and **the rate was never reproduced at this HEAD**; both halves are stated because the second one matters. The module's hand-rolled `:telemetry.attach` is replaced by `MessageTestHelper.forward_telemetry/3`, which gained an optional metadata predicate, and each test now mints its own `plugin_id` and forwards only events carrying it — so a foreign `fail_open` is never delivered rather than merely waited past (no settle, no `async: false`, both of which this entry forbids). **Not reproduced in 15 runs** at this HEAD: `phase_test.exs` alone 5/5 green, the plugin tree 8/8 and then 5/5 green, and two full `mix test` runs green — so the ~1-in-3 claim stands neither confirmed nor refuted here. **The census explains why the window is narrow.** A scratch telemetry recorder plus an `ExUnit` formatter on one monotonic clock, over two full runs: **exactly four `fail_open` emits per run**, identical population both times — three from `phase_test` itself and **one** from `table_health_test.exs` (`plugin_id: "r"`) — and the two `async: true` emitters **never overlapped** in either run. Design D3's second named candidate, `Convergence.Probe`'s Loop tick, emitted **zero** times in both runs and is removed from suspicion. What replaces the missing race as proof is a **deterministic** one: with only the filter argument removed, the change's new scoping case fails with `Unexpectedly received message {:fail_open, …, %{plugin_id: "foreign_fail_open_1380"}}` — the entry's mechanism, on demand |
| KI-19 · Test hygiene: leaked temp dirs and DLQ leftovers | `79e45353` — four modules now register cleanup where each directory is created; a run leaves the count no higher than it found it. Its DLQ half was measured NOT to grow in a plain run and is a different root (`app_data/dlq`) |
| KI-27 · Specs and docs name things that do not exist | triage: three of its four claims are measured gone (`libflz_transport` retired by `24127af0`; `flz_jni`'s only surviving mentions are this file and the board describing the issue; `config.exs` now says `key`). **One survives** and is refiled as KI-51 |
| KI-37 · SDK native binaries ship all five triples — provisional | triage: **two of six SDKs, not every SDK** — Node ships per-triple `optionalDependencies`, Ruby compiles at install, Python ships maturin wheels. Scoped to Java and PHP and refiled as KI-52. Today's artefact purge changed repository weight, not distribution shape — do not conflate them |
| KI-39 · Open-source obligations, none met | `4735c359` closed the last item. Three of the four obligations were already done: `SECURITY.md`, CLA + cla-assistant in `CONTRIBUTING.md`, and the trademark section in `README.md`. Whether cla-assistant is installed on GitHub, as opposed to documented, was not established |
| KI-12 · Veneer artifacts are debug builds by default | triage: the debug default is real but every `build.bench` passes `--release` and `report.ex` already warns when `FLEZHA_BENCH_SKIP_BUILD` is set — not a live defect |
| KI-17 · UDS socket paths overflow `sun_path`, and can fail a test | triage: **never true** — `git log --all -S` shows the quoted path shape only ever appeared in this entry's own prose; `_wus` is a Registry via-name suffix, not a filename. Measured worst case 107 bytes, longest ever written 89 |
| KI-22 · An inert drain in the output-stall drill | `5f381a3e` deleted the drain. Correction: it was inert 274 times in 200 000, not "every time" |
| KI-26 · A stale doc block on `channel_metadata` | `c5bdc26b` (`refactor(3f)`) — the doc block now documents `channel_metadata`, which exists; the retired variant appears nowhere in `sdk/rust/src` |
| KI-28 · A cross-compile warning only the dist build surfaces | `62d7e918` — it is the `unused import: socket_dir` this session gated to `#[cfg(unix)]`. Not reproducible locally: zig is absent, so the loop's local half does not exist on this box |
| KI-32 · The merge-blocking bench gate's own command does not complete | triage: **the gate passes** — 3/3 runs at 1 000 000/1 000 000, worst receive window 518 872 against a 432 000 threshold. Which change fixed it was not bisected; the original `:driver_loop_frozen` diagnosis is now known to have fired vacuously |
| KI-35 · `output_stall_drill_test:108` flakes ~40 % even alone | `5f381a3e`, the same commit as KI-22 — and **the stated cause was never the mechanism**: the test sets `tick_ms: 3_600_000`, so no periodic tick was ever in any window. Refuted over 164 executions (41 idle + 41 under six busy-loop hogs), 0 failures against a claimed ~40 % |
| KI-40 · CI does not run three test files or the kernel's own tests | triage: **never true** — `elixir.yml:141` runs `cargo test -p flz_core` and passes `--features bench-internals,testing`; dropping `testing` fails to compile 6 targets, so the check can go dirty. Correction: `entity_timers` is a capability, not a test target |
| KI-5 · `duplicate_pairs` is a pair count over whatever the run scanned, so `sdk/rust` reads 169 alone and 215 whole-repo, and `--check` differenced the two in silence | `small-mechanical-fixes` — `measure()` records `scan`, the sorted area set, beside the numbers it produced; `docs/refactor/baseline.json` carries it; `scan_mismatch/2` makes `--check` **refuse** a baseline recorded over a different set rather than report a delta, and a baseline with no `scan` at all is refused too, an unknown scan being exactly the comparison the rule forbids. Re-measured at `50304acb`: **169 / 169 / 215** for `sdk/rust` alone, with `sdk/java` + `sdk/node`, and whole-repo — the entry's 128 / 136 / 178 were stale by about a third, and the middle row is the point, since those two siblings share nothing with `sdk/rust` and add zero: *which* siblings you scan is what moves the number, not how many. Both arms drilled — whole-repo `--check` compares normally, `--check --area sdk/rust` prints `the baseline scanned 36 areas, this run 1 (only in the baseline: cli, flezha-macros, …)` and exits **2** — and `--self-test` pins all three arms including the legacy no-`scan` baseline. The key was added to the existing baseline by hand (**+38 lines, −0**) so that no metric value moved. Refusing on *any* difference is deliberate rather than over-strict: adding an area adds files to the jscpd run and so can add cross-area pairs to areas that were already there, which means every area's count really does move. The response is `--write-baseline`, and charter §11 now says so |
| KI-16 · `mix deps.get` reports packages with security advisories, unaudited | `small-mechanical-fixes` — **one** advisory, `postgrex 0.22.3` / EEF-CVE-2026-66838 (MEDIUM), SQL injection via the `:comment` option of `Postgrex.stream/4`, and **`lib/` never reaches it**: no `Postgrex.stream` call and no `:comment` anywhere, the callers being `Postgrex.query/4`, `Postgrex.child_spec/1`, `Postgrex.start_link/1` and an `after_connect: {Postgrex, :query!, …}`. `mix.exs` already declares `{:postgrex, "~> 0.22"}`, so 0.22.4 needed no constraint change. **`mix deps.update postgrex` moved two lines, not one** — `telemetry 1.3.0 => 1.4.2` came along, and postgrex does not depend on telemetry at all (`db_connection`'s `~> 0.4 or ~> 1.0` does), so the resolver simply took the newest satisfying version while it was re-resolving. telemetry was put back to 1.3.0 and `mix deps.get` accepted it, leaving the committed lock diff at one line. `mix hex.audit` after: *No retired or security advisory packages found* |
| KI-24 · `sdk/rust/tests/ui/*.rs` must never be rustfmt-formatted and nothing enforced it — the count was wrong too | `small-mechanical-fixes` — `rustfmt.toml`'s `ignore` gains `sdk/rust/tests/ui`, so a stray `rustfmt +nightly --edition 2021 sdk/rust/tests/ui/foo.rs` is a no-op; `ignore` is nightly-only, which is already the pinned toolchain. Measured on scratchpad copies before the tree was touched — without the entry the per-file loop rewrites **5 of 25** files (`procedure_ctx_with_key`, `sealed_disallows_on_full`, `sealed_disallows_wiring`, `stream_handle_cannot_escape`, `wire_wrong_input`), with it **0 of 25** — then run for real over the working tree, where `git status` on the directory comes back empty. The 20 unchanged files are why this hid: a loop over the whole list reports clean. The count is **five, not three**; `CLAUDE.md` and `docs/dev_notes/refactor-3-rust-sdk.md` both said three and both now say five |
| KI-4 · the format gate's `need` preflight tested every formatter BEFORE running any, so one absent veneer tool ended the run and said nothing about the six languages behind it | `format-gate-tells-the-truth` — the preflight's install hint survives; only the abort goes. `scripts/gate_lib.sh` holds one `gate_run`/`gate_report` pair that both format scripts source, keeps "tool unavailable" and "check failed" in separate lists, and carries that distinction **across a process boundary** in the exit status: **2** = a tool was missing, **1** = something is unformatted, failure winning when both are true (which is what lets `format-check.sh` roll `format-apply.sh` up without flattening it — `--nested`, and it has to be asked for, because prettier, ruff and rubocop all use exit 2 for "the tool errored"). Proved on the box the entry was filed from, where `rubocop` and `php` are genuinely absent: the gate now reports cargo fmt / mix format / mvn spotless / prettier / ruff format and NAMES Ruby and PHP as not run, where before it printed **not one `==>` line**. Three arms drilled: every tool present and the tree clean → **exit 0**, seven formatters; two tools absent → **exit 2**, and `reuse lint` plus the notices diff still ran behind them; a deliberately unformatted `.exs` with every tool present → **exit 1**, `UNFORMATTED: mix format`, and the four formatters after Elixir still ran. `format-check.sh` took the same shape, so an absent `reuse` no longer hides a stale `THIRD_PARTY_NOTICES.md` |
| KI-7 · CI ran no veneer formatter at all — `format-check.sh` was the only caller of prettier, ruff, rubocop and php-cs-fixer anywhere, and KI-4 is why it could silently not | `format-gate-tells-the-truth` — `.github/workflows/format.yml` installs all four (setup-node + `npm ci`, setup-python + `pip install ruff`, setup-ruby + `gem install rubocop`, setup-php with `ext-ffi` + `composer install`) and then runs **`scripts/format-apply.sh --check`** rather than keeping its own list of formatters, which is the drift this entry was: one list now, not two. Every version is read out of the SDK's own manifest (`package-lock.json`, `pyproject.toml`, `flezha.gemspec`, `composer.lock`) so no pin is written twice. The four installs are `continue-on-error` for the same reason the script collects instead of aborting — a failed install costs one language, not the report. The `paths` allow-list became a `paths-ignore` of `docs/**` and `openspec/**`: an allow-list that forgets a source tree is a gate that silently stops covering it, which is how four of the seven languages went unchecked |
| KI-8 · `scripts/format-apply.sh` ran `cargo +nightly fmt` while CI pinned `nightly-2026-05-07`, two genuinely different builds on this box | `format-gate-tells-the-truth` — the script now invokes `cargo +nightly-2026-05-07`, with the comment beside it naming the workflow and `rust-toolchain.toml` as the other two places to bump together. Re-measured before the change: `nightly` is `f964de49b 2026-05-07`, `nightly-2026-05-07` is `365c0e1d7 2026-05-06`, and both exit 0 with empty output on this tree — the risk was real and the realisation was zero, which is why this rode a change rather than justifying one |
| KI-9 · `.formatter.exs` `inputs` missed four tracked `.exs`, two of which were unformatted | `format-gate-tells-the-truth` — `inputs` gains `{bench,scripts}/**/*.exs`, `examples/plugins/**/*.exs` and `.credo.exs`, and the two files were reformatted in the same commit (widening alone turns the Elixir check red). `examples/**` is deliberately narrowed to `plugins`: `examples/coding-fleet/` is gitignored, has its own repo, and is unformatted, so the wide glob would fail the check on a file CI cannot see. The reflow is proved semantics-neutral twice — metadata-stripped ASTs are IDENTICAL for both files, and `summarize.exs` driven against the repo's own `results.quick.tsv`/`cells.quick.tsv` produces **byte-identical** stdout, `summary.quick.md` and saved baseline against HEAD's copy on both the report and the `--save-baseline` arm |
| KI-11 · every second Java build in a checkout failed at shade — `duplicate entry: META-INF/THIRD_PARTY_NOTICES.md`, and the conformance manifest's own build command carries no `clean` | `java-build-runs-twice` (`be1110c9`) — `<forceCreation>true</forceCreation>` on `maven-jar-plugin` in the two modules that shade (`sdk/java/flezha-sdk`, `test/apps/java_runner`), at a **pinned 3.4.1**. The pin is load-bearing, not hygiene: nothing pinned that plugin, so this box's Maven 3.8.7 resolved **2.4** and the release image's Maven 3.9.9 resolves **3.4.1**, and the `jar.forceCreation` user property — the form that avoids declaring the plugin, and so avoids Maven's missing-version warning — exists only up to 2.x. Under a 3.4.1 pin it is silently inert and the duplicate-entry failure reproduces unchanged, which is a fix that would have looked green here and shipped broken. **The filed cause was wrong twice over.** `build-dist.sh` is not involved: `clean verify` → 0 then a bare `verify` → 1 reproduces it with no dist build anywhere, so the condition is a non-empty `target/`, which is every checkout after its first build. And it is not the aggregator's missing sources either — that only makes it total. **Shade replaces the jar plugin's output file in place**, so on the next build the jar plugin's up-to-date check finds the *shaded* jar at its own destination and skips: with no sources it always skips, and with sources it skips whenever nothing is newer than the fat jar. Either way shade re-shades its own 15 MB output; `flezha-sdk` fails loudly only because `IncludeResourceTransformer` then attaches a notices file the jar already carries. **`test/apps/java_runner` had the identical defect and failed silently** — its filter excludes `META-INF/THIRD_PARTY_NOTICES.md` from every artifact including its own, so it re-shaded to a fixpoint and exited 0; measured by `original-java_runner.jar` coming out 16,082,694 bytes (the fat jar) instead of 209,955. Both of the entry's suggested one-line fixes are refuted: `<shadedArtifactAttached>` installs the *empty* jar as `com.flezha:flezha-sdk`, which is what `java_runner` compiles against; moving the notices to `src/main/resources` removes the duplicate-entry symptom and leaves the re-shading, which is the half that can carry a deleted native forward. Proof: the conformance `build.test` command (byte-identical to `build.bench`) three times in one checkout with no `clean`, all exit 0; `unzip -l` on the fat jar → one `META-INF/THIRD_PARTY_NOTICES.md`, exactly the five `native/<os>-<arch>/libflz_core_ffi.*` and no other native library, 1833 entries as before; `mvn -P coverage,native-verify clean verify` green (117 + 17 + 5 tests); `FLEZHA_E2E_RUNTIME=java mix test --include sdk_smoke` 1859 passed |
| KI-38 · `process_hub` pinned to a GitHub `dev` branch, so no version number described what a build contained | `adopt-process-hub-hex-release` (`4735c359`) — `mix.exs` declares `{:process_hub, "~> 0.7.0"}`; `mix.lock` carries the hex entry. **0.7.0 is the pinned code, published**: 78 `.ex` files each side, exactly one differs (`hub.ex`), and that difference is a pure relocation of `for_workers/1` — sorted, the two files are an identical multiset of lines. What the `.ex` comparison could **not** see is the package's own `mix.exs`, which raises `elector` from `~> 0.3.3` to `~> 0.3.4`; resolution fails until `elector` is unlocked, so the lock diff is two lines, not one. elector 0.3.4 is a real behaviour change on a live path (`erpc:multicall` replaces per-node `send_request`/`wait_response`, so the fan-out timeout is per batch rather than per node, and an election survives a candidate vanishing) — `declared_children` and `centralized_load_balancer` are its callers. `mix compile --force --warnings-as-errors` 272 files clean (negative control: a bogus `ProcessHub` call does fail it); `mix test` 1836 passed and `--include system` 1915 passed / 5 skipped, both unchanged from the HEAD baselines recorded before the swap, and `--include multi_node` 1845 passed — added because of the elector finding, since that tag is not covered by `--include system` and is where leader election actually runs |
| KI-42 · six sites named `<state_root>/<class>`, a path the system never writes — five `refute File.exists?` that could not fail, and a `File.rm_rf` that reclaimed nothing | `assertions-that-cannot-fail` (`9708e2e5`) — all six now derive from <!-- module-naming-record:begin -->`Flezha.State.Registry.class_dir/1`<!-- module-naming-record:end -->, and **every one was proved twice**: with the subject genuinely present at the real path the *old* assertion still passed (the two unit sites **34 passed**; `partitioned_operatives_e2e` **2 passed**; `remote_durability_e2e` under `POSTGRES_INTEGRATION=1` **3 passed**), and the *corrected* one goes red on the same subject and green once it is removed. **No deletion is broken** — all five are green against real behaviour, so `ClassRegistry`, `PartitionGroup.rescale/3`, `TierE2EHarness.destroy_local_state/1` and the rescale's tree removal all keep the promises their test names make; there was nothing to escalate. The `rm_rf` is proved by count, not by an assertion: one scenario (`workflow_crash_resume_e2e_test.exs:692`) left `app_data/state/workflow_loop_boot_2082/chat_loop_workflow_2114` with three class sidecars in it after its own `cleanup/3` ran, and leaves nothing after — the module's private `cleanup/3` is gone entirely, it imports `TierE2EHarness.cleanup/3`, which is the one teardown. **The sweep confirms the triage's number**: 46 `refute File.exists?` / `refute File.dir?` in `test/`, **exactly five vacuous** — the five filed — and 41 sound, each read against the layout it asserts on. **The seed is larger than filed**: not two moduledoc blocks but **seven**, across five modules (`state/registry.ex` twice, `operative/class_registry.ex`, `state/completion.ex`, `state/input_log.ex`, `state/proc_inbox.ex`, `state/proc_journal.ex`) plus four in `docs/`, every one dropping the `<namespace>` segment its own code writes. `Registry.namespace_dir/1` now owns that level and `class_dir/1` composes on it, so the two production sites that rebuilt it (`AuthZ.Quotas.compute_disk_bytes/1`, `Convergence.Loop.namespace_dir_mtimes/0`) and the two test sites (`TierE2EHarness.cleanup/3`, `quic_test.exs`) derive it too. Baselines unchanged: `mix test` **1836 passed**, `--include system` **1915 passed / 5 skipped** |
| KI-47 · `check-overflow-naming.sh` red on its own evidence — a triage entry has to quote the retired name to prove it is gone | the gate now separates a QUOTED mention from a bare use, scoped to `docs/known_issues.md`; drilled both ways (bare use still exits 1) |
| KI-44 · the OTLP codec crashed on any attribute value that is not a string, and silently re-encoded a list | `close-otlp-export-crash` (`ba0d3632`) — one private `attribute_string/1` renders every attribute **key and value**: a binary passes through untouched, an atom, integer or float takes its `String.Chars` rendering, everything else — lists included — goes through `inspect/1` with `limit: :infinity, printable_limit: :infinity`. Both halves were driven red first: a real `LoggerHandler.log/2` event through the real codec raised `Protocol.UndefinedError` on `logger:` (a Map) and on `mfa:` (a Tuple), while `%{l: [1, 2, 3]}` encoded to the bytes `<<1, 2, 3>>` **with no exception at all**. Three corrections to the filed entry, each measured: `:222` is the metric `NumberDataPoint` site, not the resource (`default_resource/0` never sees signal metadata); an attribute **key** can be a non-string too and crashed identically, since `Signal.new/1` validates that metadata is a map and never constrains its keys; and the escalation is no longer inferred — with the coercion reverted, `OtlpSink` dies inside `flush/1` with that exception, which the flush test now pins. `mix test test/flezha/observability/export_test.exs` with `attribute_string/1` reverted to `to_string/1`: **12 of 18**, six red including the list case, which fails without raising; restored: **18 of 18**, full suite **1836 passed, 0 failed**. The sibling site on the Prometheus exporter is KI-45; typed `AnyValue`s are KI-46 |
| KI-43 · 28 test modules set Mimic's global mode without releasing it | `close-mimic-global-leak` (`828a9f50`) — 26 modules moved to `FlezhaTest.Helper.MimicGlobal.set_mimic_global_until_exit/1`, two dropped global mode entirely (they register no stub anywhere reachable), and `scripts/check-mimic-global.sh` refuses the raw call in `test/` with `test/helpers/mimic_global.ex` exempt by path. Measured, not asserted: `mix test test/flezha/service/cli_manager_state_get_test.exs test/flezha/service/cli_manager_schedule_test.exs --seed 0` was **11/20 passed, 9 failed**, every failure `Only the global owner is allowed`, against **10 passed** for the schedule module alone; after, **20 passed** on the same seed. Read directly (a temporary `ExUnit.after_suite` probe on `:ets.lookup(Mimic.Server, :mode)`): at seed 0 exactly one of the 28 left the mode `:global`; after the change all 36 global-mode modules left it `:private`. The entry's own reading was too broad — see `docs/dev_notes/close-mimic-global-leak.md` for which of the 28 could actually leak and why |
| KI-2 · first input after `start_operative/2` journaled and never forwarded (both faces: the start window, and the restore re-forward onto a stream a fresh runner has not bound) | `fix-first-input-drop-on-bind` (`e16260a7`) — the operative bind now waits for its data-stream channel (`StreamChannel.await_bind/2`), and an undelivered payload arms `{:stream_bound, _}`, which re-forwards the journal tail through the replay path a restore already used. Deterministic proof: `test/flezha/operative/bind_reforward_test.exs` — its four delivery cases were run red on the pre-fix tree, in 2.1 s, with no sampling. `effectively_once_checkpoint_e2e_test.exs:37` 40x in isolation: **40/40 pass, 0 `[:flezha, :stream, :drop]`** |
| KI-20 · `sdk_smoke_e2e_test.exs` carried the old re-send workaround | `fix-first-input-drop-on-bind` (`e16260a7`) — `send_until_forwarded` deleted, the single `send_io` stands, and `NodeTestHelper.await_stream_bound/2` is gone from the tree with it (29 call sites across 15 modules) |
| KI-1 · the consumed watermark overwritten back after an effectively-once park (retention floor and credit under-reporting by one offset) | `fix-outbox-watermark-overwrite` (`9410eea7`) — `InputLog.record_consumed/3` now advances the stored offset or leaves it, atomically. `test/system/dlq_e2e_test.exs:121` in isolation: **0 of 6 runs passed before, 5 of 5 after**; the deterministic proof is `wal_writer_outbox_test.exs` "a nested window's advance survives the outer window closing", which fails on `refactor-baseline-2` as well as on the pre-fix HEAD |
| `cargo check --workspace --all-targets` red without a feature nobody passes | `119a20d4` — three `[[test]]` entries were missing `required-features` |
| `test/apps/node_runner/fixtures.mjs` a `SyntaxError` since `refactor(0a)`, so no Node e2e had run in four areas | `d6f6e16f` |
| Node `Source::get` costing four N-API calls per field read (~4 % on the Node dispatch row) | `d6f6e16f` |
| `refactor_metrics.py` measuring Rust functions short (`#` stripped as a comment, so `quote!` interpolations ended the brace count) | `6818eec1` |
| Three Rust SDK flakes, each with a mechanism rather than a retry | `588094e4` |
| Committed `.pyc` files and a missing `__pycache__` ignore | area 2 |
| `mix test --include system` "always exits 2" | refuted — the `:postgres_integration` modules self-skip now |
