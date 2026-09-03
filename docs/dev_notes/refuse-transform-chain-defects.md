# `refuse-transform-chain-defects` — three refusals from one reference function, and the seventh name that is not there

## The reference is one function, and both references are the same function

Rust `BuilderState::extend_pending` (`sdk/rust/src/channel/builder/state.rs`)
and Java `StreamOperativeTopologyBuilder.extendPending` push three issues, in
this order:

1. `TransformAfterTerminal` — **the channel already carries a plugin sink**.
2. `TransformAfterKeyBy` — the chain being extended carries a key and this link
   is a transform.
3. `DuplicateKeyBy` — the chain carries a key and this link is another key.

Everything about the veneers' version is fixed by that: the conditions, their
order, and the sentences, which Java's
`StreamOperativeConfigurationException` already carries verbatim. There was
nothing to design and everything to copy.

## The two facts that are load-bearing, and are easy to get wrong

**`TransformAfterTerminal` reads the CHANNEL, not the chain.** It fires only
when the sink is *already attached*. So

```
handle.map(f).into(P);   handle.sink("s")      # legal
handle.sink("s").map(f).into(P)                # refused
```

are the same topology declared in two orders, and only one is accepted. That is
not an accident of the implementation — the reference's own sentence says the
sink serializes the channel's own value, so the order says which value that is.
A veneer that deferred the sink test to `validate()` would refuse both, which is
wrong; the test must run at the link, against the channel as it stands then.

Concretely: a procedure's output channel is created on first use, so
`sinkAttached(source)` is *false* for a node with no materialised channel. That
is exactly what keeps the legal order legal, and it is why the helper cannot be
`channel.sink != nil` on a channel it materialises itself.

**A transform after a key is refused; a key after a transform is not.** The key
is evaluated on the final post-transform value, so it must be the last link.

## `DuplicateKeyBy` was the silent one

`Edge.key_by` was `_with(key_by=…)` in python and the identical `with(...)` in
node, ruby and php — a plain overwrite of the pending option. Measured rather
than asserted:

```
key_by(→"FIRST").key_by(→"SECOND")  →  surviving key on the wired edge: SECOND
```

The author's first key was discarded with no word, and every input routed to
the Entity the second key named. Same class of loss as the dangling chain that
`732cb6e9` closed.

## One list, and the order in it is the message

`732cb6e9` left each veneer with `danglingChains: string[]`. Rather than a
second list beside it, it generalises to one:

```
chainIssues() = linkIssues ++ [ {channel, DanglingTransformChain} | unclosed tips ]
```

`linkIssues` is what each link refused as it was declared; the tips are the
chains neither superseded nor consumed. **The order matters and is the
references' own**: `.key_by(a).key_by(b)` with no `into` produces a
`DuplicateKeyBy` *and* a dangling tip, and the veneers raise on the first, so
the author must read the sharper name. Put dangling first and the sharp defect
disappears behind the vague one.

`validate()` then holds one name→sentence table and one loop. Four refusals,
one mechanism, one message site.

## The fixture the sweep caught, and why grep would not have

`sdk/php/tests/Fixture.php` `MixedStampOperative` declared

```php
$e->into(StampEmitter::class)->named('keyed_out')
  ->keyed()->sink('keyed_out')
  ->flatMap(...)->into(FanTarget::class)
```

— `TransformAfterTerminal`, and 28 tests went red. **A grep for `map(` finds
nothing here**: PHP spells it `flatMap`, and `Edge::flatMap` is one `Edge::map`
underneath. Only loading the fixture and validating it finds this.

Its docblock argued the transform "fans onto a CONSUMER edge, which is where a
transform chain lives" — true of where the chain is *stored*, and silent about
the order it was declared in, which is the thing the refusal is about.

The correction is declaration order, not topology, and that was **measured**:
holding the handle, closing the `flatMap` on its `into(...)` and attaching the
sink last produces a byte-identical channel list — three channels, same
producers, same sink names, same `keyed` flag, same consumer edges with the
same transform counts — with the issue list empty instead of carrying one
`TransformAfterTerminal`.

The runner-app sweep was clean: node 65, python 62, ruby 52, php 52 specs
loaded and validated, 231 in all, before and after.

## The seventh name is absent, and worse than absent

`docs/sdk/stream_transforms.md` listed seven refusals as "the same names in
every SDK". After this change four are, by name, in all six. Two
(`KeyByOnUnkeyableTarget`, `UnkeyedEdgeIntoDynamicProcedure`) are carried by
name in Rust/Java/Elixir and refused in their own words by the four veneers.

`EdgeAlreadyWired` is **Rust and Java only** — and the gap is behavioural, not
cosmetic. Rust's `attach_consumer` looks for an existing consumer edge naming
the same procedure: it refuses when either side carries a closure, and makes
the bare re-declaration a no-op, which is the documented legal fan-in. Every
veneer's `into` — and the Elixir SDK's `do_into` — appends unconditionally, so
BOTH shapes produce two consumer edges to one procedure and the procedure is
wired twice. The doc now says so rather than implying the check exists.
