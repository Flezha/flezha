# rename-block-to-drop-nothing — implementation notes

## The naming argument, in one line

`OverflowPolicy` asks *what is discarded when the mailbox is full?* `DropNewest`
and `DropOldest` answer it; `Block` answered a different question (what the
producer's thread does). The gloss in `docs/configuration.md` — `` `Block`
(back-pressure) `` — existed because the name did not say what it guaranteed,
and "back-pressure" appeared exactly once in the whole `docs/` tree, inside
that parenthesis.

## Why the rename LEADS the mechanism change

`DropNothing` is true of today's implementation: `routing::enqueue`'s arm
retries until the push lands and discards nothing. Renaming first means the
name is accurate at every commit in both directions. Sequencing it after
`nonblocking-drop-nothing-backpressure` would leave a window in which the
shipped default is misdescribed — which is how the gloss came to exist.

## The task list under-counted the surface by three

The proposal enumerated the declaration sites. The gate found the rest, and
they are the interesting half:

- **`flz_core/src/poll_core.rs`** passes the policy at two timer-promotion
  sites. Neither is a declaration; both must compile.
- **Prose that names the policy** in `entity.rs`, `mailbox.rs`, `poll_core.rs`,
  `routing.rs`, `poll_pool.rs`, four `docs/sdk/*.md`, `docs/internals/
  transport_liveness.md`, `.github/workflows/rust-sdk-bench-cron.yml` and
  `bench/flink/README.md`. `docs/internals/transport_liveness.md` was on the
  proposal's **confirm-untouched** list; it names a different mechanism
  everywhere except one `blocked_parks` gloss, which did name the policy.
- **Four `sdk/rust/tests/operative_dispatch.rs` test names** plus its stress
  fixture. A test called `default_entry_policy_blocks_and_loses_nothing`
  survives a rename of the enum and then lies.

The proposal also named `default_policy_for_kind` (Rust) and
`defaultPolicyForKind` (Java) as the lockstep seams. **Neither exists.** Rust
takes the default from `#[default]` on the enum via `OverflowPolicy::default()`
at channel creation; Java assigns it in `declareChannel`. The spec delta was
corrected to name the real seams — a spec that names a function nobody wrote
cannot be checked.

## The `MODIFIED` trap

An OpenSpec `## MODIFIED Requirements` block **replaces the whole
requirement**, scenarios included. The first draft of the delta rewrote
"Default overflow policy by channel kind" and silently deleted its second
scenario ("An entry channel still drops when it says so"), which the rename
does not touch. Carry every surviving scenario across, or the archive deletes
it.

Four capabilities named the retired variant, not the two in the proposal's
Impact: `operative-procedure-entity-model` (three requirements, one of them by
title), `operative-state-persistence`, `rust-sdk-dispatch` and `sdk-poll-core`
(also by title). A requirement title changes with `## RENAMED Requirements`
(`FROM:`/`TO:`), not by editing the heading inside a `MODIFIED` block.

## The gate: a positive list, not a word plus an allowlist

<!-- kind-naming-record:begin -->
`scripts/check-kind-naming.sh` matches the token `bounded` everywhere and masks
the legitimate senses out with `scripts/kind-naming-allowlist.txt`.
<!-- kind-naming-record:end --> That model
is wrong for "block": the word is ordinary English *and* a common programming
term, so the allowlist would be longer than the tree.

`scripts/check-overflow-naming.sh` inverts it — narrow patterns pinned to the
policy's own shapes:

<!-- overflow-naming-record:begin -->
`OverflowPolicy::Block`, `onFull(…BLOCK)`, `on_full(:block)`,
`"overflow": "block"`, `bounded_mailbox_block`, and `Block`-policy prose.
<!-- overflow-naming-record:end -->

`block_parker`, `BLOCK_PARK_TIMEOUT`, `pushes_blocked_total`,
`*_block_workflow` and every blocking call are out of scope **by construction**,
not by exemption.

All three gates in the Naming job now share `scripts/naming_gate.py`: the git
walk, the path and binary-file filters, the `…-record:begin/end` escape hatch
and the failure report. Each supplies only its judgement, as a
`check(rel, line) -> [message]`. The two spelling gates build theirs from
`naming_gate.pattern_check(patterns, allowlist)`, which carries both matching
models; `check-sdk-boundary.sh` writes its own, because its violation is a
reference outside an allowed set rather than a spelling. That gate stopped
hand-rolling its own walk and report in the same pass — a third copy is the
signal to extract, and it was the third. All three gates' messages are
unchanged; the boundary gate's now go to stderr like the other two.

The next gate of this kind is its patterns plus a ten-line call.

One genuine false positive needed an exemption: `observability-foundation`'s
<!-- overflow-naming-record:begin -->
"Block policy is opt-in" is the **bus's** per-sink `overflow: :drop | :block`,
<!-- overflow-naming-record:end -->
a different mechanism that keeps its own vocabulary — like the Kafka producer's
`:queue_overflow` and the transport write buffer.

<!-- kind-naming-record:begin -->
The kind gate was **red at HEAD** ("bounded concurrency" in a notes file, the
capacity sense); `concurrency` joined the allowlist's `bounded X` alternation.
<!-- kind-naming-record:end -->

## What the wire literal actually does

`#[serde(rename_all = "snake_case")]` derives `"drop_nothing"` from the variant,
so the wire spelling changes with the Rust identifier and there is no mapping
to edit. Java's is hand-written (`HostClassDeclaration.java`, `case DROP_NOTHING
-> "drop_nothing"`) and must be edited, as must the annotation processor's
`case "drop_nothing" -> "DROP_NOTHING"`.

A payload carrying `"block"` is refused by serde's **unknown-variant** error
(which lists the three accepted spellings), not by `deny_unknown_fields` — that
attribute governs struct fields. `flz_core/src/host.rs::overflow_literal_tests`
pins both directions.

## Rebuild the committed artifacts, all of them

`guard-cross-sdk-placement` recorded that a stale `java_runner.jar` never
places. This change extends that: the committed **native** `libflz_core_ffi`
binaries are compiled `flz_core`, and `flz_core` is what parses the declaration
JSON. A Java runner emitting `"drop_nothing"` against a native lib that only
knows `"block"` is refused at declare time. `scripts/build-dist.sh --host`
rebuilds all five platforms plus both fat jars in about a minute.

## Fixture-internal names travel with the policy

The three bounded-mailbox conformance fixtures are parallel by construction
(`drop_newest_input` / `drop_newest_echo` / `drop_newest_test_procedure`). The
`block_*` set was renamed to `drop_nothing_*` across all three runners so the
triple still reads as one family; no controller-side test references those
names.

## Pre-existing flake found and characterised

`test/system/dlq_e2e_test.exs:119` ("tier-on poison payload freezes the
min-watermark") fails intermittently at the final watermark assertion. An
interleaved A/B against a HEAD worktree measured 2/4 on this branch and 1/4 at
baseline — it is load-sensitive and **pre-existing**. A run of eight
consecutive failures on a loaded box looked deterministic and was not;
interleaving is the only reading that settles it.
