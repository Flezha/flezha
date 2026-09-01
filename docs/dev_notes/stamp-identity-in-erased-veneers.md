# Identity stamps in the erased veneers — implementation notes

`stamp-identity-in-erased-veneers`: `flz.entity` / `flz.seq` / `flz.key` on a
non-tier keyed plugin emit in the Node, Ruby and PHP SDKs, then the
`keyed_channels` conformance flip. What `docs/refactor/charter.md` and
`openspec/changes/stamp-identity-in-erased-veneers/design.md` say is not
repeated; this is what the next person changing this code needs.

## The promise was in the doc comment and nowhere else

Before this change, `grep 'flz\.entity|flz\.seq|flz\.key'` over
`sdk/node/src`, `sdk/ruby/lib` and `sdk/php/src` returned **three** hits, and all
three were the `.keyed()` doc comment — *"Stamp identity and `flz.key` onto
emitted plugin frames"* — on a builder whose runtime stamped nothing. So the
capability was not merely unproved: `operative_forward` resolves a partition
member from `metadata["flz.key"]`
(`lib/flezha/plugin/provider/operative_forward.ex:189`), the Postgres sink upserts
on it (`postgres/sink_worker.ex:352`) and the Kafka producer keys its message with
it (`kafka/producer_worker/wire.ex:108`). A keyed channel on those three SDKs
declared an intent no downstream consumer could act on.

## The ordinal is the whole difficulty, and a single-emit test cannot see it

`flz.seq` is `<change_seq>.<emit_ordinal>`, and the ordinal advances **per emit
within one invocation**. A port that writes a constant `0` passes every
single-emit assertion, and it passes the e2e golden too:
`test/system/sdk_smoke_e2e_test.exs:243` compares the **sorted key names** of the
`flz.*` metadata against `test/fixtures/declarations/keyed_output_stamps.json` and
never reads a value.

So the load-bearing proof is per-SDK and in-process, and it is multi-emit:
`sdk/node/test/dispatch.test.mjs`, `sdk/ruby/test/dispatch_test.rb`,
`sdk/php/tests/DispatchTest.php`. Each asserts `"5.0"` then `"5.1"` from one
invocation, that a fresh work item restarts at `0`, and that user metadata
survives beside the stamps.

**Where the counter lives, and why nothing resets it.** Each context object *is*
one invocation — constructed per work item at `sdk/node/src/dispatch.ts:227`,
`sdk/ruby/lib/flezha/dispatch.rb:154` and
`sdk/php/src/Dispatch/Dispatcher.php:149` — so an instance field has exactly the
right lifetime and reset code would be a bug waiting to be wrong. It must be a
**second** field: all three already carry `ordinal` for `stableId`, and sharing
one would make a `ctx.stableId()` call shift a later `flz.seq`. Rust keeps them
apart for the same reason (`sdk/rust/src/procedure/ctx.rs:36` versus
`sdk/rust/src/operative/dispatcher/emit_sink.rs:50`).

**PHP stamps per produced output, not per emit call.** `Ctx::outputsFor` loops
over `Transforms::apply(...)`, which fans one emit into several; Node and Ruby
produce one output per channel per call. A test for the fan-out is in
`DispatchTest::testAFannedEmitAdvancesTheOrdinalOncePerProducedOutput`.

## Rust cannot express the topology where the two ordinal rules differ

This is the finding worth keeping, because a code-read said the opposite and two
people believed it.

Rust's increment sits inside `if self.keyed` (`emit_sink.rs:105` wrapping the
`+= 1` at `:374`); Java's (`EmitContext.java:287`) and Python's (`ctx.py:255`) run
on every plugin output. That difference is observable only when **one procedure
produces both a keyed and a non-keyed plugin channel** — and Rust refuses that
shape:

```
$ cargo test -p flezha_runner --features bench-internals,testing --test <probe>
Rust REFUSED with: operative topology validation failed:
  [DuplicatePluginSink { channel: "keyed_out" }]
```

A Rust procedure keeps one `output_channel_id`
(`sdk/rust/src/workflow/spec.rs:255-258`: *"A procedure producing two channels is
unroutable … Structurally impossible on the stream-handle builder"*) and a channel
takes one plugin sink (`stream.rs:379-382`). So Rust's guard is **unobservable in
Rust**, and for every topology Rust can express all three implementations already
agree.

**The erased hosts accept the shape.** `branch` gives each arm an independent
edge in Node, Ruby and PHP, and each arm declares its own plugin channel — the
fixtures in this change declare two, two and three. The open question is therefore
*may one procedure produce several plugin channels?* (filed as **KI-118**), not
*how does the ordinal count?* — the ordinal rule follows from that answer. This
change adopts Java's and Python's rule because it is the only **defined** answer
on a topology Rust has no opinion about, and it stays reversible: the primary
value proof runs on a single keyed channel, the topology every SDK can express,
and only the divergence scenario uses the two-channel shape.

## The `flz_core` seam that would replace five implementations, and why not here

`flz_core::host::activation::ship_plugin_outputs` (`activation.rs:606`) is the one
funnel every erased host's non-tier plugin frame passes through, and it holds
everything a stamp needs — `slot.dispatcher.entity_label()`, minted once at
`flz_core/src/entity.rs:422` as `"{instance_id}/{procedure_name}/{entity_key}"`
and byte-identical to what the hosts build by hand; `slot.change_seq`;
`slot.topology` for `keyed`; and a per-item call site that resets the ordinal for
free. **This is a candidate for a later row, not a defect.** Two things would have
to be answered first:

1. **It reads `change_seq` at a different point.** The hosts read it at handout;
   `settle_into_buffer` runs at `activation.rs:412` and `ship_plugin_outputs` at
   `:415`, and `frame_change_bodies` (`flz_core/src/flusher.rs:294`) advances
   `change_seq` as the invocation's changes are framed. Java and Python would ship
   different `flz.seq` values.
2. **It deletes the only proof of stamp *values* that exists.** Nothing asserts a
   value end to end; Java's and Python's in-process tests do, and below the FFI
   boundary they would have nothing to assert.

It also does not reach one implementation: Rust's typed path never enters
`host::activation`, and the Elixir SDK inherits nothing from `flz_core`.

## The conformance flip, and the gate that does NOT move

`keyed_channels` leaves `deferred` and enters `fixtures` as
`["partition_relay"]` — the reference's set, which
`gate_test.exs:290` requires exactly. The three test apps already declared
`partition_relay` with `.keyed()`, so **no fixture app changed**.

`@required_capabilities` is untouched, and that was verified rather than assumed:
`test/apps/elixir_runner/conformance.json` still defers `keyed_channels`, so
`deferred_somewhere` (`gate_test.exs:275`) still contains it. Making the elixir
manifest carry it too turns the gate red on exactly one test —

```
no manifest defers keyed_channels, so every SDK carries it: add it to
@required_capabilities in test/helpers/node_test_helper.ex, or defer it by name
in the manifest of the SDK that does not carry it
```

— which is the line R6 pays the day the Elixir SDK mints the stamps.

## The tooling this needs, which the gate does not find

- **Node's tests import `dist/esm/`, not `src/`.** `npm run build` before every
  run, or a source edit is invisible to the suite.
- **Ruby needs the rbenv toolchain.** The system Ruby 3.2 has no `msgpack` and no
  `bundle`; `/home/moon/.rbenv/versions/3.4.10/bin` carries the gemspec-pinned
  `msgpack`, `minitest 5.25.4`, `rake 13.2.1` and `rubocop 1.90.0`.
  `scripts/format-check.sh` reports rubocop absent because it resolves the system
  Ruby, not because it is missing.
- **PHP's `vendor/bin/phpunit` and `php-cs-fixer` are checked in and work as-is.**
