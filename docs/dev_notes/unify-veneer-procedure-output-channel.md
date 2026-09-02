# unify-veneer-procedure-output-channel — implementation notes

R7 change 1 of 8. The four veneer SDKs — node, ruby, php, python — adopt the
channel model Rust and Java have always run: **one output channel per producing
procedure**. What follows is what cost time, not what the change does.

## The two models, and why the reference could only ever have had one

Take the reference's `hybrid_channel`
(`test/apps/rust_runner/src/channel.rs`):

```rust
c.input::<String>("input")
    .into::<HybridProducerProcedure>()
    .named("hybrid_indicator")
    .sink_with("hybrid_indicators", |v: &u32| …)
    .into::<HybridConsumerProcedure>();
```

Rust declares **two** channels: the entry, and one `hybrid_indicator` carrying a
plugin sink **and** a consumer. Every veneer declared **three** — the entry,
`hybrid_indicator` with one consumer and no sink, and a separate
`hybrid_indicators` of kind `plugin` with no consumer. Same fixture, same
capability list, two topologies.

The reference has no choice in the matter, and the core is where the choice is
made:

```rust
// flz_core/src/host/register.rs:567-571
let output_channel_id = decl.channels.iter()
    .position(|c| c.producer == Some(proc_id))
    .map(|id| id as u32);
```

A procedure's output channel is **one id, resolved by first match**. A second
channel with the same producer is not routed anywhere by the workflow driver,
which is `output_channel_id`'s only reader. On a stream class that is invisible,
because every output is addressed explicitly (`OutputTarget::Edge { channel,
consumer }`, `OutputTarget::Plugin(name)`) and nothing consults it. On a workflow
class it is the journaled handoff (`workflow/commit.rs`), the crash-resume
re-seed (`workflow/resume.rs`) and the driver's child fan-out
(`workflow/driver.rs`) — so the per-edge model does not merely differ from the
reference, it silently truncates a durable fan-out. That is [[KI-141]].

The veneers were not wrong by accident: the per-edge model is what a builder
falls into when `sink()` returns nothing. If `sink()` is terminal, a hybrid is
unwritable, so a hybrid has to be spelled as two channels, and once two channels
exist the funnel has to decide which one an emit belongs to. Every other
difference follows from that one.

## The three consequences

**`sink()` returns the edge.** `.sink(name, enc)` now attaches the sink to
`outputChannelFor(node)` and returns `this`, so `.sink(…).into(P)` is writable in
all four languages. In node and php that is a type change (`void` → the handle);
in ruby and python it is a `return self`. The duplicate-sink refusal keeps its
identifier and message.

**Transforms and keys move onto the consumer edge.** With one channel per
procedure, `.map(f).into(B)` and `.map(g).into(C)` from the same handle are two
different edges of one channel, so `transforms`/`keyBy` cannot live on the
channel any more. Each `ChannelSpec.consumers` entry became a two-field record
where an integer sat — `ConsumerEdge { procedure, transforms, keyBy }` — which
is what Java's `ChannelDescriptor` and the erased host's
`ConsumerEdge { procedure, transform: None, key: None }`
(`flz_core/src/host/register.rs`) have always carried. The core never evaluates
them; the host does. This was the largest single edit of the change and it
touched the emit funnel, `applyPending`, and the entry-side prepare crossing in
all four SDKs.

`branch(...)` needed **no** change, and that is the check that the model is
right: every veneer already implements it by handing each arm the *same* handle,
so under one channel per procedure it becomes "append two consumers" for free.
A test per SDK asserts the two spellings emit byte-identical declarations.

**The kind is derived at the decl seam, not stored.** Java's rule
(`TopologyMaterializer.java:63`), transliterated:

```
kind = (sink present AND consumers empty) ? plugin : the channel's own kind
```

so producer→sink stays `plugin`, producer→consumer stays `inter_proc`, and
producer→sink→consumer is `inter_proc` **with a sink** — the hybrid. The wire
gains nothing: `HostChannelDecl` (`flz_core/src/host/decl.rs:19-33`) has eleven
keys and the sink is not one of them, because a plugin output is addressed by
channel **name** on the output itself. That is the fact that makes the whole
change decl-compatible, and it is why `test/fixtures/host_class_decl_keys.txt`
did not move.

The keyed refusal was re-aimed rather than removed. `keyed()` used to be refused
where `kind != 'plugin'`; a hybrid is `inter_proc` with a sink, so the predicate
is now "the channel has no plugin sink". Same identifier, same message, one
predicate — without it, the reference's `.keyed().sink_with(...)` shape is
refused by three of the four veneers at registration, and would have looked like
a fixture bug five changes from now.

## `codec()` is owed to no veneer

The board carried "two builder surfaces every veneer lacks — an inter-procedure
`codec()` and a hybrid `sink().into()`" for the whole programme. One half of
that was already false when it was written:

```rust
// flz_core/src/host/register.rs:492-493
codec: (channel_decl.kind == HostChannelKind::InterProc)
    .then(|| host_payload_codec(channel_id)),
```

The core derives the journal codec **from the channel's kind**, because a host
over the erased poll core hands it bytes and there is nothing left to decide.
`HostChannelDecl` has no codec key and `host_class_decl_keys.txt` names none, so
a veneer `codec()` could not reach the core even if one were written. Rust and
Java declare a codec because *their* procedures hand the core typed values that
have no byte form until one is declared; Java's own `InterProcCodec`
encoder/decoder are read nowhere outside `TopologyMaterializer` construction.

So the change deliberately builds no `codec()` in any veneer — a no-op method in
four languages is four parallel mechanisms for a decision the core makes — and
`sealed()` likewise: no topology difference, no expressible compile-time meaning
in three of the four languages, and no fixture uses it.

`docs/architecture/runner.md` carried the claim and is corrected here. **It is
not the only place**: `docs/sdk/ruby_sdk.md:233`, `docs/sdk/php_sdk.md:401`,
`docs/dev_notes/ruby-sdk.md:176`, `docs/dev_notes/php-sdk.md:139` and
`docs/dev_notes/python-sdk.md:212` each repeat it in their own words. They are
outside this change's lane and are owed to R7's per-veneer fixture changes.

## The golden is hand-written on purpose

Four implementations, one design, and no compiler between them. The instrument is
`test/fixtures/reference_topologies.txt`, in the directory that already pins
`host_class_decl_keys.txt` and `runner_connect_env.txt`: two topologies —
`hybrid_channel` and `commit_block_chain` — one line per channel,
`topology | index | name | kind | producer | consumers | sink`. Each SDK's
existing decl-shape test builds both in its own spelling and asserts its
declaration is those lines, in order.

**It is transcribed from the Rust reference by hand and checked in.** A golden a
runtime derives at test time is not a golden, it is a second copy of the thing
under test — so the file names its two sources
(`test/apps/rust_runner/src/channel.rs`, `.../workflow.rs`) and the builder rules
its columns come from, and nothing computes it. The R6 change-4 lesson behind
that rule: a constant wrong by 2^8 survived a design review and was caught only
by a cross-language fixture.

The golden's negative control was run per SDK by perturbing one line and
restoring it — node `expected '… | NEGATIVE_CONTROL | …'`, ruby `1 failures`,
python `1 failed, 7 passed`, php `Failures: 1`.

## KI-144 — our own model change made a field ambiguous, and every gate stayed green

This is the most valuable thing in the change.

`ChannelSpec.encode` is the **plugin sink's** serializer. Its doc comment said
so; `Edge#sink(name, encode)` is its only writer. Before the merge, a channel
carrying it was kind `plugin` with an **empty consumer list**, so the edge path
could not reach it — the ambiguity was prevented by the topology, not by the
name. Merging the two channels into one removed that prevention, and every
veneer's edge path began serialising the *inter-procedure* frame with the
plugin's serializer. The consumer decodes with the class codec, dies on the first
byte, retries the ladder and parks the item to the DLQ. Silent data loss, all
four veneers, both transports.

**Everything that should have caught it was green while it was true**:
registration of `hybrid_channel` in all four runners' `conformance.json`, the
four decl-shape tests written by this very change, and the four SDK unit suites
(130 / 114 / 182 / 163). The declaration was correct in every respect. Only the
*bytes on the wire* were wrong, and nothing drove them.

What found it was the e2e that task 5.3 was rewritten to demand. The proposal
had claimed the `graph_builder` capability "covers `hybrid_channel` among its
eight fixtures"; it did not — that describe block held two declaration snapshots
and `grep -rn hybrid test/system/` returned nothing. Registration was already
passing while a hybrid channel lost every item, which is exactly the gap a
drive-it e2e exists to close:

    FLEZHA_E2E_RUNTIME=node mix test --include sdk_smoke \
      test/system/sdk_smoke_e2e_test.exs:131

    before  0/2 passed  RangeError: Extra 3 of 4 byte(s) found at buffer[1]
                        parking poison payload to DLQ after 3 attempt(s)

`<<0,0,0,42>>` is `hybrid_indicators`' big-endian-u32 serializer; msgpack of `42`
is one byte — hence "3 extra bytes". python gave `ExtraData`, ruby
`MessagePack::MalformedFormatError`, php a `JsonException`. Rust was green
throughout, because it has no such ambiguity by construction:
`sdk/rust/src/operative/dispatcher/emit_sink.rs` routes the inter-procedure value
to consumers as a typed `Arc<dyn Any>` — never serialised at all — and reads
`sink.serializer` only inside `emit_plugin_sink`.

**The cure was ruled structural, not four `if` removals.** Dropping the
`channel.encode ?` conditional at the four payload sites would fix today's bytes
and leave the next channel merge free to repeat this, so the field was renamed to
name its owner: `sinkEncode` (node, php) / `sink_encode` (python, ruby) on all
four `ChannelSpec`s, with the invariant stated once per SDK on the field itself.
The edge path encodes with the class codec, always. The **authoring** surface
`sink(name, encode:)` is unchanged in every veneer — no shim, no alias.

php needed one more cut: `Ctx::payload()` was a single helper serving both frame
kinds, which is why that SDK carried the bug three times over. It split into
`edgePayload` and `pluginPayload`, the latter falling back through the former so
the class codec is written once. **The rename is what found the third caller** —
the workflow arm inlined in `Ctx::release()`, which KI-144's filed coordinates do
not name. A conditional removal at the two named sites would have left it.

The symmetric case was checked and is clean: `decode` is written at exactly one
site per SDK, `Builder.input(name, decode:)`, which always constructs a fresh
`kind: "entry"` channel, so no producing channel can carry one. The asymmetry is
precisely that `input()` and `sink()` write **different** channels, while
`sink()` wrote the channel the edge path also reads.

## The php asymmetry

php is the outlier on every axis, and none of it is accidental.

**It needed no `effectivelyOnce` change.** node wrote a literal `false` on a
workflow class *and* carried an unreachable refusal beside it; ruby wrote a
literal; python omitted the field and took the `ClassSpec` default. php's
`Operative.php` already read `$builder->effectivelyOnce` for both kinds. Three
SDKs gained a read of a flag the builder already held; php was named in the task
list as needing none, so a worker transliterating three identical diffs would not
add a fourth.

**It cost the most lines** — `prod_loc` +51 against the change's stated `+35 ±
15` band, where node is +29, ruby +21 and python +45. The whole overshoot is in
`sdk/php/src/Ctx.php` (+51 by itself), and it is language shape rather than extra
behaviour: the old single funnel `outputsFor` (40 prod lines) decided edge-vs-
plugin with `$isPlugin` ternaries inside one loop, and one channel owing both
frames splits it into six named methods totalling 92 — `release` 27,
`edgeOutputs` 27, `pluginOutput` 24, `pluginPayload` 6, `edgePayload` 4,
`structuredWorkflow` 4. Ten of those 92 are KI-144's payload split. php is also
the only veneer where `ConsumerEdge` is a **file** (12 prod lines: `<?php`,
`declare(strict_types=1)`, `namespace`, class, promoted constructor) where ruby
spells it as a one-line `Struct.new`, python as a four-line dataclass and node as
a five-line interface — all three inside a file that already exists. Against
that, php's `Graph/` package gave the most back: `Builder.php` −15
(`addChannel` 30 → `outputChannelFor` 15) and `Validate.php` −11. Its
`duplicate_pairs` fell 18 → 12; the other three are unmoved.

**It was the only runtime to leak a runner.** See KI-145 below.

## Two entries this change leaves open

- **[[KI-142]]** — no veneer refuses a **dangling transform chain**, so
  `.map(f).sink('s')` silently drops the transform. Under this change's model
  transforms live on the consumer edge and a plugin frame is serialised from the
  value as emitted, so a transform with no consumer has nowhere to run. Both
  references refuse the shape outright (`TopologyIssue::DanglingTransformChain`,
  Java's `danglingTransformChain`, whose message says *"a dangling one would
  silently do nothing"*). Adding that refusal in four languages is its own
  cross-SDK change, red-first per language, and was deliberately not folded in
  here. Its concrete instance was `sdk/php/tests/Fixture.php`, which declared
  `flatMap(...)->keyed()->sink('fanned_out')` and **asserted two plugin frames
  from one emit** — a shape the reference cannot produce; that assertion is
  corrected here, the refusal is not.
- **[[KI-145]]** — a suspicion, not a defect: under identical treatment (a test
  that stops reading the runner's port), **only php left a runner alive**, three
  times out of three, parked in `futex_do_wait` and still holding four sockets
  after 75 s. node, ruby, python and rust each left a survivor ≤ 2 s old. The
  test was cured by taking the module family's existing `port_info(:os_pid)` +
  `on_exit` teardown — **that cures the test, not the runner**. Whether the php
  runner also fails to exit on a namespace teardown it *does* observe is unproven
  in both directions, and the control arm against node is the part that would
  make the answer mean anything.

## Coordinates worth keeping

| what | where |
|---|---|
| a procedure's output channel is one id, by first match | `flz_core/src/host/register.rs:567-571` |
| the journal codec is derived from the channel kind | `flz_core/src/host/register.rs:492-493` |
| the eleven decl keys, none of them a codec | `flz_core/src/host/decl.rs:19-33` |
| the kind rule at the decl seam | `TopologyMaterializer.java:63` |
| `ensureOutputChannel`, the ten lines that were ported | `StreamOperativeTopologyBuilder.java:664-671` |
| the reference never serialises an inter-proc value | `sdk/rust/src/operative/dispatcher/emit_sink.rs` |
| the cross-language golden | `test/fixtures/reference_topologies.txt` |
| the e2e that drives the hybrid on four runtimes | `test/system/sdk_smoke_e2e_test.exs` |
