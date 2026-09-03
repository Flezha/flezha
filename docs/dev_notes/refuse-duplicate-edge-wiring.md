# `refuse-duplicate-edge-wiring` — one `find`, two answers, and the fixture that proved the second one

## The seam is consumer wiring, not chain extension

`732cb6e9` and `681669d9` carried four refusals into the veneers from ONE
reference function — Rust's `extend_pending`, Java's `extendPending`. This one
is not there. `EdgeAlreadyWired` is raised by
`BuilderState::attach_consumer` (`sdk/rust/src/channel/builder/state.rs:268`)
and by `StreamOperativeTopologyBuilder.attachEdge`, at the moment a consumer
edge is appended, and the veneers each have exactly one such site:
`EdgeStream.into`, `Edge.into`, `Edge#into`, `Edge::into`. Nothing about it
belongs in `chainIssues()`: a dangling chain is knowable only when the
declaration ends, while a re-declared edge is knowable at the `into(...)` that
declares it, and both references decide it right there.

## THE SPLIT IS THE CHANGE, and getting it wrong breaks documented behaviour

```
find the existing consumer edge naming this node
  Some(existing) and (existing carries a closure OR this declaration does)
                            -> EdgeAlreadyWired, and no edge is appended
  Some(_)                   -> {}   a TRUE no-op
  None                      -> append
```

The middle arm is the trap. **`edge.into(P); edge.into(P)` must stay legal and
must produce ONE consumer edge** — the fan-in / repeated-`into` case the
requirement calls a no-op. A veneer that refused it would break behaviour two of
this repo's own fixtures rely on; a veneer that appended it is the defect. Before
this change all four veneers appended, both shapes:

```
edge.into(P); edge.map(f).into(P)   ACCEPTED  [[0,0],[0,1]]
edge.into(P); edge.into(P)          ACCEPTED  [[0,0],[0,0]]
```

so one procedure was wired twice and every value on that channel was delivered
twice. That is a data defect, not an unraised warning.

`closeChain(...)` stays before the find, as in both references: the chain is
consumed by the declaration even when the edge is refused, so a refused
re-declaration reports `EdgeAlreadyWired` rather than a dangling chain.

## The message, and where the channel goes

The reference's sentence verbatim, minus its first three words, because the
veneers print the channel one field earlier — in the handler slot every
`DeclarationError(factory_id, handler, defect)` already names:

```
edge_already_wired: in: EdgeAlreadyWired: already feeds procedure `target`; an
edge carries exactly one transform and one key or neither, so a re-declaration
where either side carries one is refused rather than silently keeping the first
```

The sentence names no method, so unlike the four chain refusals it needs no
per-language spelling: it is identical in node, python, ruby and php.

## TWO FIXTURES WERE ALREADY DOUBLE-WIRING, and no grep would have found them

The no-op arm is not hypothetical. Instrumenting it in all four veneers and
running every suite plus every runner-app registry named exactly two, both
pre-existing, both green before and after:

- `sdk/python/tests/test_authoring.py` `diamond` — `entry.into(split)` twice on
  one entry channel, to prove "two unnamed mentions are one node". It was one
  node with **two edges**.
- `sdk/ruby/test/authoring_test.rb` `diamond` — `edge.named("left").into(shared)`
  then `edge.named("right").into(shared)`. `named` renames the ONE channel
  `upstream` produces on, so both `into`s land on the same channel. Same defect,
  hidden behind two different channel names.

Neither spells anything twice on one line, so the grep that finds
`.sink('s')->flatMap(...)` cannot find these. Each test now pins the edge count
beside the node count, which is the half that was wrong. The node and php
`Diamond` fixtures do NOT take the arm — theirs fan in from two different
channels, which is real fan-in and always was one edge each.

The runner-app sweep was clean: 52 specs each for node, python, ruby and php —
208 loaded and validated, 0 refused, before and after. (`681669d9` reported
node 65 / python 62 for the same fixtures; that count includes the
`fixtures_state` module twice, once directly and once through the `export *`
re-export. 52 is the registry's own de-duplicated count, which is what
`loadRegistry` builds.)

## The Elixir SDK is deferred, by name

`lib/flezha/sdk/channels.ex` `do_into` has the identical gap and
`topology.ex`'s `build_routes` de-duplicates nothing, so both shapes wire the
procedure twice there too. The Elixir SDK left the supervisor's work list on
2026-09-02; its track is `openspec/changes/carry-workflow-in-elixir/`, which
owes this refusal. `docs/sdk/stream_transforms.md`'s **Where** column says so in
the same row rather than implying six-for-six.
