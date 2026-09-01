# The required set stops being a chore, and the carve-out becomes a deferral

`@required_capabilities` (`test/helpers/node_test_helper.ex`) is the list a
conformance manifest may not defer from. It held six entries while **seven**
capabilities were deferred by no manifest at all. The odd one out was
`connect_discovery` — carried by all seven SDKs with the fixture `["do_nothing"]`
and proved end to end by `test/system/connect_discovery_e2e_test.exs`, yet
droppable tomorrow with the suite still green.

## The gate could not see this class of drift at all

`gate_test.exs` checked one direction only — `required ∩ deferred = ∅`. Nothing
in the tree checked the other: no script under `scripts/` parses
`conformance.json`, and every other manifest reader (`system_case.ex`,
`test_helper.exs`, `lib/mix/tasks/bench/fleet.ex`) consumes `deferred` alone. So
"move a capability into the required set once it is universal" was a step that
depended on somebody noticing, and `connect_discovery` is the proof that fails:
it sat universal-and-unratcheted in the one file whose whole purpose is catching
exactly this.

**The remedy is one test, not two, and two messages, not one.** The existing test
was extended in place: it is one invariant (`required == universal`) over the same
two data structures, and it already walked `runtimes() × deferred/1`. But the
directions have different subjects and different remedies, so each fails with its
own message — a deferred required capability names **that manifest**, a
universally carried capability missing from the list names **the declaration**.

## The list is written, never derived — and that is what makes the inverse safe

The tempting simplification is `required = universal`, computed. It is wrong, and
the reason is worth keeping: a capability every manifest defers would then leave
the required set **silently**, as a side effect of a manifest edit. Because the
list is a literal a person typed, removal is never mechanical — the first manifest
to defer a required capability turns the gate red naming it, and a human types the
edit either way. The gate refuses drift between a declaration and the manifests;
it never infers the declaration.

## The gate ratchets what a manifest CLAIMS, and a manifest can lie

`entry_idle` was carried by the Python manifest with no mechanism behind it until
`make-entry-idle-claims-honest`. Had all seven claimed it that way, this check
would have promoted the lie to a mandate. **The mitigation is not in this gate and
must not be read into it**: manifest honesty comes from the two neighbouring rules
— every vocabulary capability has a tagged `test/system/` module, and a carried
capability's fixture ids equal the reference's, so carrying is not satisfiable
with an empty list.

## The carve-out was legitimate; the mechanism was not

The spec said `connect_discovery` *"SHALL NOT be added to the required set: an SDK
whose transport is not the shared `flz_transport` may defer it by name"*. **The
case is real** — the Elixir SDK is one core with two deployment modes, and its
in-node `:native` mode passes BEAM messages inside the controller's own node and
walks no socket ladder. What makes it moot is narrower than "hypothetical":
`test/apps/elixir_runner/conformance.json` declares `transports: ["uds","quic"]`,
so the conformance app runs the standalone binary protocol like the other six.

A standing exemption is a permanent hole in a live ratchet bought for a case no
manifest has. The same freedom survives as a **deferral**: the SDK that cannot
carry it lists it under `deferred`, the gate goes red naming that manifest, and
the same change removes it from the required set — one reviewed diff, no hole.
The rewritten spec text keeps the reason and names the Elixir mode, because a
deleted rationale is one nobody can argue with and the next reader re-derives the
carve-out from scratch.

## Both directions were shown failing first

A gate that has never failed is a gate nobody has tested, and neither drive here
is synthetic.

**The new direction, on the real defect** — the check added before the list was
touched:

```
$ mix test test/sdk_conformance/gate_test.exs
  1) test manifests the required set is exactly the capabilities no manifest defers
     no manifest defers connect_discovery, so every SDK carries it: add it to
     @required_capabilities in test/helpers/node_test_helper.ex, or defer it by
     name in the manifest of the SDK that does not carry it
Result: 19/20 passed
```

**The existing direction, on the new entry** — `test/apps/node_runner/conformance.json`
made to defer it, then restored byte-for-byte:

```
  1) test manifests the required set is exactly the capabilities no manifest defers
     test/apps/node_runner/conformance.json defers connect_discovery, which every
     SDK carries (the required set is [:liveness, :runner_lifecycle,
     :connect_discovery, :graph_builder, :pool_wedge, :retry_ladder, :partitioned])
Result: 19/20 passed
```

Exactly one test reddens in each case, and `20 passed` either side.

## What this costs a future SDK

Nothing until it genuinely cannot carry a universal capability, and then one extra
line in the pull request that already had to edit its manifest — with the gate
naming the file and the line. The nine capabilities still deferred by four or five
manifests each join the list as their last deferrer disappears, and the gate says
so on the pull request that makes it happen.
