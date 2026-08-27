# Several controllers on one machine

Implementation notes for `support-multi-controller-colocation`. Read before
touching `Flezha.Socket.Support.prepare_socket_dir/0`, the state-root claim, or
any peer BEAM the test harness spawns.

## The socket probe is reused, not rewritten

`improve-runner-controller-discovery` had already landed `path_helper::probe`,
which answers exactly the question this change asks from the other side: is a
controller listening in this directory. Task 1.1's "if it has landed, use it"
applies, so the only new code is one NIF (`socket_dir_live/1`) over
`probe(..).reachable()`. An Elixir-side connect would have been a second answer
to drift from.

It lives in `flz_nif/src/instance_claim.rs` beside the state-root lock, not in
`client.rs`: the two answer one question — may this instance take this
resource — and `client.rs` is client streams plus the ladder resolution both
sides share.

`Reach` is deliberately untouched. Naming the peer by its OS pid (via
`SO_PEERCRED` on the probing connect) would have meant widening a variant that
every SDK matches on, for a message nicety; the refusal names the peer by the
control socket that answered instead.

A regular file named `runner_control_channel.sock` probes as
`Reach::Unreachable` (`ENOTSOCK`), **not** `StaleSocket` — a stale fixture must
bind a real socket and close it, so the connect gets `ECONNREFUSED`.

## The state root takes `fcntl` record locks, not `flock`

`F_SETLK`/`F_GETLK`, not `flock(2)`, and the reason is the refusal: `F_GETLK`
reports the blocking process's pid, and "naming the holder" is spec-mandated.
The cost is POSIX record locks' footgun — they belong to the *process*, and
closing **any** descriptor to the file drops every lock the process holds on
it. Two consequences, both load-bearing in `flz_nif/src/instance_claim.rs`:

- The claim is held in a Rust `static`, never in a resource an Elixir process
  could let go of. It lives as long as the OS process, which is the guarantee.
- Re-claiming a path already held **short-circuits**. Taking it again through a
  second descriptor would succeed (same process) and then release the whole
  claim the moment either descriptor closed. This is also what makes a
  controller restarting inside one BEAM a no-op rather than a self-refusal.

Only `EACCES`/`EAGAIN` mean "someone else holds it"; any other errno is a
filesystem that will not lock at all and is reported as itself, not as a peer —
and `errno` is read by the function that made the syscall, never by its caller.

The lock is also the only place the claim lives: `held_state_root/0` asks the
`static`, because an `Application.put_env` copy beside it is a second answer
that goes stale on a repoint. `Flezha.Boot.StateRootLock`'s counterpart is
`Path.expand/1` — the root is absolutised once, at the claim, so the lock file,
the refusal and `held/0` cannot each be naming a different tree.

Rustler binds **one Elixir module per NIF library**, so both claims live on
`Flezha.Transport.Native` even though the state root is not transport. A second
module would have meant a second cdylib.

## A boot refusal was being replaced by `:noproc`

`Flezha.Application.start_controller/0` discarded `Supervisor.start_link/2`'s
result and called `validate_boot_ready/0` unconditionally. A child that refused
to start therefore surfaced as
`{:EXIT, {:noproc, {GenServer, :call, [Flezha.BootLoader, :boot_ready?, 30000]}}}`
— the refusal text, the path and the variable all lost. Only a tree that came
up is asked whether it reached ready now. The drive found this; no unit test
could, because the refusal reads correctly at the point it is raised.

## Every multi-node test shared one state root

`NodeTestHelper.start_peer_node/1` copies the host's **whole** `:flezha`
env into the peer, and `:state_root` is unset in `config/test.exs` — so every
peer BEAM fell back to the same `app_data/state` the host had just claimed, and
would now refuse to boot. Peers get their own tree (and their own
`:process_hub_registry_root`, whose `<root>/<hub_id>/registry.dets` is not keyed
by node either).

The exception is the three `wal_transfer` multi-node tests, which exist
*because* the peer holds this node's files: they pass
`state_root: Registry.state_root()` explicitly, and the host repoints itself
away immediately after. That is a handover, not a shared root — the host's own
claim is on the boot root, never on the per-test tmp one.

`start_bare_peer/1` is the peer plumbing without a controller, extracted from
`start_peer_node/2`: the lock scenario is about what two OS processes may hold,
and does not want two controller boots to find out.

`@owned_resources` is the list of env keys a peer gets its own of, and
`owned_resources/0` is what the co-location scenario asserts is disjoint — a
resource added to the harness is covered without touching the scenario.

`stop_peer_node/2`'s `Process.alive?` guard is not enough and never was: the
peer BEAM is linked to the *test* process, so by the time an `on_exit` runs the
control process can still be alive while the distribution connection is gone,
and `:erpc.call` raises `{:erpc, :noconnection}` on a test that has already
passed. Both teardown calls go through `tolerate/1`. (`catch_exit` is the wrong
tool here twice over — it raises when the call returns normally.)

## The lock file is inside the state root, which tests wipe

`FlezhaTest.Helper.StateTestHelpers.reset_all_entities/0` does
`File.rm_rf(state_root)` — on the live controller's root, that unlinks
`.instance.lock`. A test may therefore **not** assert the lock file exists on
disk; assert that re-claiming the root still answers, which is the claim itself
rather than its file. The same is true of any operator command that clears the
state root under a running controller.

## Smaller traps

- `assert_raise Err, fn -> … end |> Map.fetch!(:message)` pipes the **function**
  into `Map.fetch!`, not the exception. Bind the result first.
- `Flezha.Runner.FleetManager.active_runners/1` returns a bare list and is
  node-local — which is what makes "each controller serves only its own
  runners" assertable from either side.
- `Flezha.State.Supervisor.live_writers/1` reads an Elixir `Registry`, which
  unregisters a dead process on its own `:DOWN`. `close_writers/1` returning is
  not the list being empty; `wal_writer_test` waits. Pre-existing, unrelated to
  this change, fixed because it made the suite red.
