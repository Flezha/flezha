# Three flags where Rust and Java have one seam

`imply-checkpoint-granularity-in-veneers` — KI-122. One private helper per veneer
SDK. What the proposal says is not repeated; this is what the next person needs.

## The seam, and why three flags could not be right

Rust routes `checkpoint_ack()`, `checkpoint_every(n)` and
`outputs_at_checkpoint(ms)` through one private `checkpoint_policy`
(`sdk/rust/src/channel/builder/mod.rs:195-205`) that sets
`AckGranularity::Checkpoint` and *then* amends the policy; Java's
`amendCheckpoint` (`StreamOperativeTopologyBuilder.java:239-242`) is the same
shape. Node, Ruby, PHP and Python instead kept `ack_granularity`,
`checkpoint_every`, `checkpoint_outputs` and `checkpoint_max_delay_ms` as four
independent fields, and only `checkpoint_every` ever wrote the first.

Three consequences followed from that one difference, and only the first was
filed:

1. **`checkpoint_ack()` did not exist** in Node, Ruby or PHP. A class wanting the
   checkpoint acknowledgement with no declared cadence — `checkpoint_store` and
   `checkpoint_batched_store` in the reference app — was inexpressible.
2. **`outputs_at_checkpoint()` did not imply it**, in all four including Python,
   whose fixture masks it by calling `.checkpoint_ack().outputs_at_checkpoint()`.
3. **`checkpoint_every()` implied the *tier***, which Rust and Java refuse. §3
   below is what that one cost.

**The tell for this class of defect**: `n` options that must agree on a shared
precondition, each setting it independently. Count how many of the `n` write it.
Here it was one of three, and the reference had a private helper precisely so the
answer could only ever be `n`.

## The red had to be the core's refusal, not a builder assertion

`flz_core/src/host/register.rs:160-170` refuses a declaration whose
`checkpoint_outputs` is `AtCheckpoint` while `ack_granularity` is not
`Checkpoint`. In Rust and Java that pairing is unreachable; in the veneers it was
the **only** thing `outputs_at_checkpoint` could produce. So the defect's face is
not a wrong field — it is a class that does not register, and the refusal names a
knob three of the four had no method for:

```
$ node ack2.mjs
declareClass REFUSED: checkpoint_every / checkpoint_outputs /
checkpoint_max_delay_ms require ack_granularity checkpoint
```

A builder-level assertion on `ack_granularity` would pass a fix that still
shipped an unregisterable class, so each SDK's red-first test drives the
declaration through the native ABI. Every SDK already had a real-ABI declare test
to extend — `native.test.mjs`, `native_test.rb`, `BindingTest.php`,
`test_native.py` — so no new test file was added anywhere.

## A test can name one rule and exercise another, and only making the earlier rule fire finds out

This is the strongest evidence for taking the whole seam, and no amount of
reading produced it.

Four tests claimed to prove the spec's *"Opt-in without the tier is refused"*
(`effectively-once-checkpoint-granularity/spec.md:32-35`). None of them reached
that refusal through the builder, because the builder always turned the tier on
with the granularity: `authoring.test.mjs` assigned `spec.ackGranularity` after
the fact, `test_authoring.py` assigned `c.ack_granularity`, `AuthoringTest.php`
took a `tierOff` escape hatch whose own docblock described the bypass **as a
property of the design**, and Ruby had no such test at all. A published SHALL was
untestable through the authoring surface in four SDKs.

Then the suite found a **fifth**, which nobody had looked for. PHP's case *"held
outputs with no maximum delay"* declared `outputsAtCheckpoint()` with no tier;
once the tier requirement existed, the tier refusal fired first:

```
Failed asserting that exception message 'under_test: declares checkpoint
acknowledgement without effectivelyOnce; …' contains 'declares
outputsAtCheckpoint with no maximum delay'.
```

It had been named for the max-delay rule and was reaching it only because an
earlier rule did not exist. **A test that passes for the wrong reason is
invisible until something in front of it starts failing** — so when a change adds
a precondition, the tests that break are not only regressions to fix; some of
them are tests whose subject was never what their name said. Read each one before
repairing it.

All five now declare through the builder, and each is parametrised over all three
checkpoint options rather than the single one it used to poke.

## What Option A costs, stated because someone will hit it

A class declaring any checkpoint option without an explicit `effectively_once()`
used to build in these four SDKs and now does not. That is deliberate: it is what
Rust and Java already do, and it is what makes the refusal reachable. One caller
in the tree relied on the old behaviour — `sdk/php/examples/E03_EffectivelyOnce.php`
declared `checkpointEvery(4)` alone, where its Java twin
(`E03_EffectivelyOnce.java:130-131`) declares `effectivelyOnce()` then
`checkpointAck()`. Every fixture and every decl-keys test already declared the
tier first, which is what made the blast radius one line.

## The e2e arm proves less than it looks like

Node, Ruby and PHP carry **no `effectively_once` fixture at all** — that is R5's
next segment — so **nothing under `test/system/` exercises this code**. The arm
run for this change proves only that the four test apps' existing declarations
still build. The proof is the ABI round-trip in each SDK's native suite, plus the
builder-level refusal and composition cases. Do not read the green arm as
coverage; when the `effectively_once` fixtures land, `checkpoint_batched_store`'s
golden (`test/fixtures/declarations/checkpoint_batched_store.json`, which pins
`"ack_granularity": "checkpoint"` with **no** `checkpoint_every`) becomes the
first end-to-end assertion that any of this works.

## Metrics

`prod_loc` rose **+31** — node 3004→3013, ruby 1936→1943, php 4153→4165, python
2898→2901 — against four blocking areas that each sat at exactly their baseline.
Three SDKs gained a method they lacked and all four gained a helper; the collapsed
assignments funded part but not all of it. Every other ratcheted metric is flat.

**Two `duplicate_pairs` rows appeared first and were removed rather than
exempted** (node 2→3, python 0→2). Both were the same mistake: the new
declaration tests declared a *stateful* procedure — `initialState`/`applyChange`
plus a change-recording handler — that they never invoke, and that shape is
already spelled in `dispatch.test.mjs` and `conftest.py`. A declaration test needs
a procedure that type-checks, not one that folds. Node's now inlines a
pass-through and Python's imports `conftest.echo`; Ruby's and PHP's were
simplified the same way for consistency. **Test-side duplication of this kind is
almost always a sign the test declared more than its subject needs.**
