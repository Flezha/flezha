# The Java diamond delivered 9 payloads for 5 (`java-diamond-resume`)

- **KI-33's diagnosis was wrong in both halves, and proving that is most of the
  change.** It said the Java resume re-runs producers whose handoff was already
  journaled, and that a fix must journal-gate the resume. **No procedure was
  ever re-run**: both runtimes' journals hold exactly one `proc_committed` per
  `proc_idx` 0…4, every one at `iter=0`. And **the same 9 payloads arrive with
  no crash at all** — one runner, `FLEZHA_WORKFLOW_E2E_RELEASE` set, no SIGKILL,
  no rebind, no resume. A resume defect cannot reproduce without a resume. Run
  that probe before believing any "it only fails on the resume path" entry.
- **The mechanism is one loop.** `EmitContext.collectEdgeOutputs`
  (`ActivationDispatcher.java:841`) added one `Edge` output **per consumer** of
  the producing channel. That is correct for a **stream** —
  `flz_core/src/host/activation.rs:509` destructures `Edge { channel, consumer }`
  and routes on it — and wrong for a **workflow stage**:
  `host/workflow.rs:70` matches `Edge { channel, .. }`, **discards the
  consumer**, collects every output as one released value, and the driver fans
  that batch out to each child itself. `DiamA`'s `dfan` has two consumers, so
  the root released the value twice and every downstream node's work doubled.
  Read as journal numbers: java `handoff_n=2` at proc 0 (two byte-identical
  17-byte items on one channel) against rust's `handoff_n=1`; procs 1–3
  `handoff_n=2`; the join `outbox_n=4` against rust's `2`.
- **The consumer half already existed and already ran**, which is why the fix is
  a branch and not a mechanism: `valuesForEntity` (`:318`) looks up
  `edgeInto(channel, cell.procedureId)` and applies that edge's transform and
  key filter for every `Work` item, and a workflow stage IS a `Work` item whose
  channel is the producing channel.
- **The `consumers().isEmpty()` early return is load-bearing, and only on the
  workflow arm.** A plugin-bound terminal (`j_out` in the diamond) has no
  consumer, so releasing there would seed the next stage with a terminal
  output — this is Rust's `WorkflowCollectAdapter::emit` `is_plugin_bound`
  guard, expressed as the condition that actually holds in the Java topology.
  For the stream arm it is a no-op: the `for` it precedes iterated an empty
  list.
- **The Edge target still names a consumer.** `OutputTarget::Edge.consumer` is a
  `ProcedureId` encoded as a postcard varint (`ActivationCodec:250`), so a
  sentinel like `-1` would not survive the codec. The workflow arm fills it with
  the channel's first consumer and the fold discards it.
- **D2's three drops were verified at the code, not argued.** The edge
  *transform* — `flz_core/src/host/register.rs:466` builds every host-class
  consumer edge with `transform: None, key: None`, so the core's `extend_mapped`
  is a plain extend and Java's was the only application. The edge *key* —
  `workflow/step.rs::partition` routes by `entities_for_erased` /
  `entity_keys` and never reads `HostPayload.keys`, and
  `entities_for_erased` is `Arc::new(|_| Vec::new())` for host classes
  (`register.rs:552`). The *event time* — `host_input_parts`
  (`activation.rs:172`) returns `(channel, bytes, metadata)` and no file under
  `flz_core/src/workflow/` mentions `event_time` at all.
- **FOUND, FILED, NOT FIXED — KI-55.** The same two-site shape on a **stream**
  inter-proc edge applies the transform twice: `=M:M:mid:v` where the contract
  says `=M:mid:v`. It hid because every transform in the tree is declared on an
  ENTRY channel, where there is no producer-side application. Two comments call
  the second application "symmetric" (`valuesForEntity`'s javadoc and
  `docs/dev_notes/stream-dsl-keyby-and-transforms.md:20`) — it is symmetric for
  a key and composition for a map. Node, Python and PHP carry the identical
  pair of sites (read, not run); Ruby declares no transform surface; Rust
  evaluates once in `route_to_consumers`. `ActivationDispatcherTest
  .interProcEdgeMapAppliesTwice` **asserts the wrong value on purpose** so the
  suite stays green and a fix cannot land silently — invert it when KI-55
  closes. Note the workflow arm now DEPENDS on `valuesForEntity` being the
  consumer-side application: removing that site would silently drop workflow
  edge transforms.
- **Task 1.1's `-P native-build` is the wrong profile for a test-path rebuild**
  (corrected in place): it runs `scripts/build-native.sh --targets all
  --skip-present --fetch-missing <version>`, which reaches the network and can
  regenerate the tracked `THIRD_PARTY_NOTICES.md`. `-P native-verify` is what
  `test/apps/java_runner/conformance.json`'s own `build.test` uses — it asserts
  the host triple is staged and builds nothing.
- **`original-java_runner.jar` at ~210 KB proves the shade defect is not back
  (210,102 B here), but it does NOT prove the fix is in the jar.** The check
  that does is one line:
  `unzip -p test/apps/java_runner/target/java_runner.jar
  'com/flezha/sdk/runtime/core/ActivationDispatcher$JavaInstance.class' |
  strings | grep -c isStructuredWorkflow` — 0 before the rebuild, 1 after. It is
  also what let batch 3's fix and batch 4's red-first e2e coexist: `clean verify`
  does not install, so the m2 jar and the runner jar stayed at HEAD while the
  working tree carried the fix.
- **Reproducing a skipped e2e without touching the repo**: copy the module to a
  scratch path OUTSIDE the tree, rename the module, delete the `@tag skip:`
  block, and `mix test /abs/path.exs --include system`. ExUnit compiles a file
  outside `test/` fine, and `git status` stays empty while you measure.
- **These cells are fast here**: the four diamond cells (2 crash + 2 no-crash,
  both transports) ran in **4.3 s** and the whole 21-test module in 22–25 s, an
  order below the ~25 s per cell the task list budgeted. A "this will take an
  hour" estimate is not a reason to narrow the run.
- **The full java tier is 1918/1920, and the two reds are not this change**:
  `schema_evolution_e2e_test.exs` dies with `[flz_nif] UDS driver fatal error:
  path must be shorter than libc::sockaddr_un.sun_path` →
  `StreamAcceptor.init/1` exits `"driver init failed"` →
  `FleetManager.runner_connect/3` refuses the connect, so the operative never
  runs. `secondary_stream_<runner_id>.sock` is `socket_dir() + 63` bytes and
  that module's namespace plus a late-run `unique/1` suffix pushes it past 108.
  The module alone: **2 passed in 1.5 s**. Filed as KI-57 — and it refutes
  KI-17's "never true, worst case 107 bytes" triage.
- **Only Java changed code, and that is established from the manifests, not
  assumed**: every other SDK lists `workflow` in its `conformance.json`
  `deferred` set, so none has a host workflow class to be wrong about. The
  contract is what was wrong — `sdk-poll-core`'s producer-side rule stated the
  stream shape unconditionally — so the spec is amended for every SDK and
  `docs/sdk/stream_transforms.md` gains the workflow-stage qualification.
  Python defers only `workflow` and is the SDK that will meet this next.
