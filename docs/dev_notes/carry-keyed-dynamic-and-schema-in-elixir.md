# Keyed sinks, the idle reaper and the schema callbacks in the Elixir SDK — three more re-expressions of the kernel's rules, and five capabilities universal

`carry-keyed-dynamic-and-schema-in-elixir`: the identity stamps and the channel
name on every sink frame, `entity_idle_ttl_ms/0` with the sweep and its cap
(KI-135), and `schema_version/0` with the migrations, all in `lib/flezha/sdk`,
which inherits nothing from `flz_core`. What `proposal.md` and `design.md` say
is not repeated; this is what the next person touching an Elixir sink frame,
the Operative's `:reap` tick or `Record.fold/6` needs.

## The frame is named by the node, and three tests pinned the plugin

Every other SDK writes the producing channel's name into `channel`
(`emit_sink.rs` `channel_metadata`, the `sdk-graph-authoring` rule); this SDK
wrote the plugin's. `named/2` names the node here, so the channel name is
`Topology.node_channel` — the node's name, else `module.name()` — and
`operative_forward`, the Kafka `:channel` gate and the DLQ row now read the
same value off an Elixir runner as off a Rust one. Two unit assertions were
expected to move; a third (`dispatch_test.exs`, `FlakyProc` named `f`) was
found by the suite. Nothing e2e pins the plugin name: the sink subscriptions are
`on_output` with no channel filter, and the goldens compare declarations and
stamp key names.

## The stamps read the seq the dispatch STARTED at

`Entity.dispatch/2` captures `handout_seq` before `ship/2` advances
`change_seq` and hands it with the emits to the Operative, which stamps at the
routing seam where the sink frame is built. Reading the seq after the records
are framed would ship a different `flz.seq` from Java and Python
(`stamp-identity-in-erased-veneers.md`). The ordinal is the reduce index over
the invocation's plugin outputs — every sink output counts, stamped or not, and
a `flat_map` output counts once each (PHP's rule, the spec's wording); an
inter-procedure output does not count. `emit_with_meta/3`'s metadata had never
reached the wire (`flush_emits/2` dropped it); the stamps go after it, so the
drop had to close first.

## The key comes from the emit, the route, or the Entity — and the ctx refuses early

A keyed sink's key is the emit's own (`emit_keyed/3`), else the sink route's
`key_by`, else the emitting Entity's. `Ctx.new/4` takes two topology facts from
the Entity — `keyed_sink?` and `key_required?` (a keyed sink with no `key_by`
on a Single node) — and refuses at the call, as `{:reserved_metadata_key, k}`
is refused, because by the time routing sees the emit `run/3` has returned.
`keyed/1` sets `Stream.keyed_sink`, a second field beside `keyed` (which means
"a `key_by` was applied"); a keyed handle reaching `into/2` before a sink is
`KeyedInterProcedureChannel`.

## The router IS the drain gate, and the slot must go with the Entity

The reference's reaper takes each slot's inflight CAS and re-checks the
mailbox. Here every push is routed by the Operative, so a sweep that runs in
`handle_info(:reap)` holds routing by construction: no push can reach an Entity
between the probe and the eviction. The handshake is two calls —
`Entity.idle_for?/3`, bounded at 50 ms and changing nothing (a timeout is
"busy" and the late reply is inert in the catch-all), then `Entity.evict/1`,
synchronous, so the Entity's `delete_entity_state` is on its stream before any
push can mint a successor that writes seq 0 to the same stream. The Entity
ships the delete itself, on its own secondary, un-metered (the reference's
`send_oneshot` is outside the credit window too), and **deletes its slot from
the Operative's ETS**: without that the successor seeds from the evicted state
and the cold start the spec promises is a warm one. Measured at 1 000 idle
Entities on one node: a probe-only sweep 5.8 ms; evict-all 37 ms unbudgeted,
which is why `@evict_budget 256` per tick exists (10.6 ms, the rest next
tick). A `last_push_ms` filter on the Operative side would not have moved that
number — it is the evictions, not the probes.

## `max_entities/0` binds where a key would mint

`entities` became per node (`%{node_ref => %{key => pid}}`) so the cap is a
`map_size/1` and the sweep walks one node. The drop is silent and counted
(`[:flezha, :sdk, :dynamic]`, `event: :capped`) — no park, no reply to the
controller, the reference's best-effort cap. A restore that would materialise a
key past the cap answers `:entity_capped`. KI-135's red run: three live
Entities against a cap of two.

## Migrations take the term, and `:undeclared` is not an error string

`migrate_state/2` and `migrate_change/2` receive the DECODED old term, because
the body codec is `:erlang.binary_to_term/1` and there is no second codec to
choose — Rust hands bytes and re-exports postcard, Python hands bytes and the
author calls the codec. The accessors answer `:undeclared` when the callback is
absent so `Record.decode/4` can keep the present wording (`no migration from
schema_version N: the procedure declares none (change)`) apart from a failing
one (`migration from schema_version N failed (snapshot): <reason>`). Two
modules of one `name/0` in two classes (`evolving_cart` v1 and v2) are legal;
`DuplicatePersistentProcedure` is per class.

## KI-136: a non-terminal disconnect must close the runner's own write half

The old branch answered `{:stop, :normal, …}`; under the app's `:permanent`
child spec the supervisor restarted the RunnerServer, which reconnected and
registered into the session the controller was tearing down — the
`command_assign_runner_id` timeout in the smoke tier's `RunnerDisconnect`
scenario. The reference (`command/mod.rs` `finish_disconnect`) writes the ack,
then `shutdown(Write)`, and `Stopped` makes `reconnect` a no-op. Mirrored: the
ack, `teardown_streams/1`, the control handle closed, `stopped?: true`, the
process stays. The controller's `ControlChannel` turns the runner's socket
close into `runner_disconnected` — a first cut that only set the flag and kept
the streams open left `stop_runner/2` waiting for a close that never came.
The terminal branch is untouched.

## The gate that printed, again — and the gate that fails fast

`mix test … --warnings-as-errors` exited 0 on a cached compile with a
`clauses with the same name and arity should be grouped together` warning
(`materialise/3` between two `handle_call/3` clauses); only `mix compile --force
--warnings-as-errors; echo $?` sees this class. `gate_test.exs`'s inverse
direction names the FIRST capability no manifest defers and stops, so three
flips show one name; adding the atoms one at a time is how it names the others.
`--only capability:<x>` does not narrow `sdk_smoke_e2e_test` (the describe tags
do not filter the module) — the whole module runs.

## Small things that will bite

- `refactor_metrics.py --check` counts a guarded `refute_receive` all the same;
  `test` had no headroom, so the three became positive assertions
  (`Counters.dispatches/1`, the `:evicted` telemetry).
- Credo's "nested modules could be aliased" fires on a fully-qualified
  `Flezha.SDK.Channels.keyed()` in a fixture — twice per site.
- A test keyed by `key_by(&"k#{&1}")` re-pushes the SAME input to reach the
  same key; a cold-start assertion on a different key reads `entity_evicted`
  for the old one and looks like a reaper bug.
- The e2e's reap-and-delete scenario is `reference_runtime: true` (a Java gap),
  so the Elixir reaper's e2e proof is the live-read scenario; the delete frame
  is proved in `persistence_test.exs` off the real stream.
- KI-134 leaked after the `quic` smoke arm (per-module) this time; SIGTERM did
  not end it, SIGKILL did.
- The bench twins `bench_keyed` / `bench_keyed_nopersist` are keyed now, as the
  reference's are: the durable bench path pays for its stamps on both runtimes.

## What the change found that it did not fix

`sdk_smoke_e2e_test`'s `RunnerDisconnect` scenario was red at HEAD before any
edit (the mechanism is KI-136, fixed here as its own commit). The evict-all
sweep's 37 ms is bounded by the budget, not removed. `DynamicProc` and
`PartitionRelayProc` are a duplicate pair (both journal `{:push, input}`, as
both reference procedures do) — ruled a fixture twin.
