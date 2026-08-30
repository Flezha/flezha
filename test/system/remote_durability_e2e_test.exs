defmodule FlezhaTest.System.RemoteDurabilityE2ETest do
  @moduledoc """
  The kill-the-disk drill for `remote-durability-tier`, against a live Rust
  runner and a real Postgres backend.

  This is the failure the tier exists for and nothing else in the suite covers:
  every durable byte flezha owns is controller-local, so a controller that dies
  **with its disk** has always taken its operatives' state — and the record of
  which operatives existed — with it. Both cases here destroy exactly that,
  after the operative has committed real work, and demand it come back.

    * **workflow, synchronous** — a class declaring `SYNC_DURABILITY`. Its
      commit barrier is acked only after the barrier reaches the backend, so
      destroying the state directory afterwards loses *nothing*: the resumed
      run skips the committed procedure rather than re-running it.

    * **stream, checkpoint** — a checkpoint-ack class. The interval that closed
      is remote; the state it folded comes back byte for byte after the disk is
      gone, and the operative resumes from the shipped cut rather than from
      nothing.

    * **two controllers, one backend** — a peer collecting throughout a live
      shipper's run. This is the configuration the tier exists to serve, and the
      one where a global, time-blind collection deleted a blob a manifest was
      about to name. It asserts against a real store that every artifact the
      live manifest references still exists, and that recovery still works.

  Tagged `:system` and `:postgres_integration` — it boots a real runner AND
  needs the compose Postgres. Run with:

      POSTGRES_INTEGRATION=1 mix test --include system --include postgres_integration \\
        test/system/remote_durability_e2e_test.exs

  Pre-req: `docker compose -f docker/dev/docker-compose.yml up -d postgres-sink`.
  """

  use FlezhaTest.SystemCase, capability: :effectively_once

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]
  import FlezhaTest.Helper.TierE2EHarness

  alias Flezha.Operative.Builder
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Completion
  alias Flezha.State.DurabilityTier.{Backend, Manifest, Shipper}
  alias Flezha.State.{InputLog, ProcJournal, Registry}

  @moduletag :postgres_integration
  @moduletag timeout: 300_000

  @postgres_skip FlezhaTest.SystemCase.skip_unless(
                   System.get_env("POSTGRES_INTEGRATION") == "1",
                   "the durability drill needs POSTGRES_INTEGRATION=1"
                 )
  @moduletag skip: @postgres_skip

  @tag capability: :workflow, skip: FlezhaTest.SystemCase.deferral(:workflow) || @postgres_skip
  test "a sync-declared workflow loses nothing when the controller's disk is destroyed" do
    backend_opts = install_durability_tier()
    namespace = unique("dur_wf")
    operative_id = unique("sync_durable_workflow")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    # Runner A blocks in proc B, so the crash lands with proc A committed and
    # the chain unfinished.
    {port_a, exec_a} = boot_runner(namespace, id_path)

    builder = %Builder{
      id: operative_id,
      factory_id: "sync_durable_workflow",
      kind: :workflow,
      init_values: %{"seed" => "go"},
      opts: []
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    assert info.runner_id == exec_a

    assert wait_until(fn -> ClassRegistry.sync_durability?(operative_id) end, 30_000),
           "the sync-durability declaration never reached the controller"

    assert wait_until(fn -> ProcJournal.restore_state(operative_id) != nil end, 60_000),
           "proc A never committed its barrier on the first runner"

    # The barrier's journal is on the backend. A sync-declared class gets there
    # before its ack; the wait covers the one window where it cannot — the
    # very first barrier, which the SDK can reach before the controller has
    # finished decoding the class declaration that turns the gate on. The gate
    # itself is pinned deterministically in
    # `test/flezha/runner/control_channel_proc_commit_test.exs`.
    journal_ref = {:entity, {operative_id, operative_id, Registry.journal_entity_key()}}

    assert wait_until(
             fn -> match?({:ok, _}, fetch(backend_opts, Manifest.key(journal_ref))) end,
             60_000
           ),
           "the committed barrier never reached the durability backend"

    refute Completion.exists?(operative_id), "the run finished before the drill could start"

    # --- the drill: the controller's disk for this operative is gone ---------
    sigkill(os_pid(port_a))
    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 60_000)

    destroy_local_state(operative_id)
    refute File.exists?(Path.join(Registry.state_root(), operative_id))
    assert ProcJournal.restore_state(operative_id) == nil, "local journal survived the drill"

    # A replacement runner with the release flag: the resumed chain can finish.
    {_port_b, exec_b} =
      boot_runner(namespace, id_path, env: [{"FLEZHA_WORKFLOW_E2E_RELEASE", "1"}])

    assert exec_b != exec_a
    kill_overwatch(namespace, operative_id)

    # Zero loss: the bind hydrated the journal out of the manifest, so the
    # resumed run SKIPS proc A and completes.
    assert wait_until(
             fn -> match?({:ok, %{status: "ok"}}, Completion.read(operative_id)) end,
             120_000
           ),
           "the resumed run never completed after hydrating from the remote store"

    {:ok, entries} = ProcJournal.read(operative_id)
    committed = for %{type: :proc_committed, proc_idx: idx} <- entries, do: idx

    assert 0 in committed, "proc A's pre-drill commit did not survive the disk loss"
    assert 1 in committed, "the resumed proc B never committed its boundary"

    assert Enum.count(committed, &(&1 == 0)) == 1,
           "proc A was re-run after the drill — the hydrated journal was not honoured"
  end

  test "a checkpoint stream class comes back from the shipped cut after the disk is destroyed" do
    backend_opts = install_durability_tier()
    namespace = unique("dur_cp")
    operative_id = unique("checkpoint_store")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {port_a, exec_a} = boot_runner(namespace, id_path)

    sink = %Flezha.Plugin{
      id: "dur_sink",
      namespace: namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self(), include_metadata: true},
      subscriptions: [{:on_output, []}]
    }

    builder = %Builder{
      id: operative_id,
      factory_id: "checkpoint_store",
      init_values: %{},
      opts: [plugins: [sink]]
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)

    assert info.runner_id == exec_a

    assert wait_until(fn -> ClassRegistry.checkpoint_ack?(operative_id) end, 30_000)

    # Fold two keys, then close the interval. The checkpoint is the cut whose
    # state and consumed offset are consistent — exactly what ships.
    send_inputs(operative_id, namespace, ["k1:a", "k2:a", "commit:1"])
    assert await_changelog(%{"k1" => "k1=a", "k2" => "k2=a"}, 60_000)

    assert wait_until(fn -> InputLog.watermark(operative_id) == 2 end, 60_000),
           "the checkpoint never advanced the durable consumed-offset"

    assert wait_until(fn -> Registry.entities_of(operative_id) != [] end, 60_000)
    [entity_id | _] = Registry.entities_of(operative_id)
    entity_ref = {:entity, entity_id}

    assert wait_until(
             fn -> match?({:ok, _}, fetch(backend_opts, Manifest.key(entity_ref))) end,
             60_000
           ),
           "the closed interval never reached the durability backend"

    # The log the cut resumes from ships with it. Without this the hydrated
    # Entity comes back holding a consumed floor and nothing to replay from.
    assert wait_until(
             fn ->
               match?({:ok, _}, fetch(backend_opts, Manifest.key({:input_log, operative_id})))
             end,
             60_000
           ),
           "the input log never reached the durability backend"

    # --- the drill ----------------------------------------------------------
    shipped_before = shipped_through(backend_opts, entity_ref)

    sigkill(os_pid(port_a))
    assert wait_until(fn -> exec_a not in FleetManager.active_runners(namespace) end, 60_000)

    destroy_local_state(operative_id)
    refute File.exists?(Path.join(Registry.state_root(), operative_id))

    {port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a

    force_fresh_rebind(namespace, operative_id)

    # The checkpointed state is back: the live Entity holds both keys folded
    # before the disk was destroyed. Loss is bounded to what was never
    # checkpointed, which is the mode's documented contract.
    assert wait_until(
             fn ->
               case OperativeManager.get_entity_state(namespace, operative_id) do
                 {:ok, json} -> json =~ "k1" and json =~ "k2"
                 _ -> false
               end
             end,
             120_000
           ),
           "the checkpointed state did not survive the disk loss"

    # And the recovered operative still works: a new input folds onto the
    # restored state rather than onto a cold start.
    send_inputs(operative_id, namespace, ["k3:a", "commit:2"])
    assert await_changelog(%{"k3" => "k3=a"}, 60_000)

    # --- the drill, again ---------------------------------------------------
    # Recovering once is worth nothing if it leaves the operative unprotected
    # against the next failure, and a recovered Entity is only protected if
    # hydration put back the local files that describe it as well as its data.
    # This round is the end-to-end repeatability statement: the recovered Entity
    # ships again, and survives a second disk loss.
    #
    # It does NOT pin the epoch specifically — this fixture's epoch never
    # diverges, because a runner replacement restores contiguous seqs and so
    # never realigns. `durability_tier_test.exs` sets a bumped epoch directly
    # and is where "a recovered Entity keeps shipping instead of fencing itself
    # off" actually bites.
    assert wait_until(
             fn -> shipped_through(backend_opts, entity_ref) > shipped_before end,
             60_000
           ),
           "the recovered Entity never shipped again — it fenced itself off"

    sigkill(os_pid(port_b))
    assert wait_until(fn -> exec_b not in FleetManager.active_runners(namespace) end, 60_000)

    destroy_local_state(operative_id)
    {_port_c, exec_c} = boot_runner(namespace, id_path)
    assert exec_c != exec_b
    force_fresh_rebind(namespace, operative_id)

    assert wait_until(
             fn ->
               case OperativeManager.get_entity_state(namespace, operative_id) do
                 {:ok, json} -> json =~ "k1" and json =~ "k2" and json =~ "k3"
                 _ -> false
               end
             end,
             120_000
           ),
           "the second recovery lost the work the first one recovered"
  end

  test "a second controller collecting throughout cannot break the shipper's lineage" do
    backend_opts = install_durability_tier()
    namespace = unique("dur_gc")
    operative_id = unique("checkpoint_store")
    id_path = unique_id_path()

    on_exit(fn -> cleanup(namespace, operative_id, id_path) end)

    {_port_a, exec_a} = boot_runner(namespace, id_path)

    builder = %Builder{
      id: operative_id,
      factory_id: "checkpoint_store",
      init_values: %{},
      opts: []
    }

    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    assert info.runner_id == exec_a
    assert wait_until(fn -> ClassRegistry.checkpoint_ack?(operative_id) end, 30_000)

    # A second controller sharing this backend, collecting on every round while
    # the first ships. It runs against the SAME state root on purpose: that puts
    # the references inside its scope, so the scoping cannot be what saves them
    # and the grace is left carrying the property alone — the sharper of the two
    # statements, and the one that has to hold against a real store.
    {:ok, peer} = GenServer.start(Shipper, [])
    on_exit(fn -> FlezhaTest.Helper.NodeTestHelper.ensure_stopped(peer) end)

    for round <- 1..3 do
      send_inputs(operative_id, namespace, ["k#{round}:v#{round}", "commit:#{round}"])
      assert wait_until(fn -> InputLog.watermark(operative_id) >= round * 2 - 1 end, 60_000)
      assert {:ok, _collected} = GenServer.call(peer, :gc, 60_000)
    end

    assert wait_until(fn -> Registry.entities_of(operative_id) != [] end, 60_000)
    [entity_id | _] = Registry.entities_of(operative_id)
    entity_ref = {:entity, entity_id}

    assert wait_until(
             fn -> shipped_through(backend_opts, entity_ref) >= 0 end,
             60_000
           ),
           "nothing ever reached the backend"

    # Every artifact the live manifest names must still be there. Under a global,
    # time-blind collection this is exactly where a blob went missing: uploaded,
    # collected by the peer, then named by a manifest written a moment later.
    assert {:ok, blob} = fetch(backend_opts, Manifest.key(entity_ref))
    assert {:ok, manifest} = Manifest.decode(blob)

    for artifact <- Manifest.blob_keys(manifest) do
      assert {:ok, _} = fetch(backend_opts, artifact),
             "the manifest names #{artifact}, which the peer's collection removed"
    end

    # And the only test that matters: it still recovers.
    destroy_local_state(operative_id)
    {_port_b, exec_b} = boot_runner(namespace, id_path)
    assert exec_b != exec_a
    force_fresh_rebind(namespace, operative_id)

    assert wait_until(
             fn ->
               case OperativeManager.get_entity_state(namespace, operative_id) do
                 {:ok, json} -> json =~ "k1"
                 _ -> false
               end
             end,
             120_000
           ),
           "the operative did not come back after a peer collected throughout its run"
  end

  defp fetch(opts, key), do: Backend.Postgres.get(key, opts)

  # The highest seq the stored manifest covers, or -1 when there is none. Its
  # growth is the observable that the reference is still being shipped.
  defp shipped_through(opts, ref) do
    with {:ok, blob} <- fetch(opts, Manifest.key(ref)),
         {:ok, manifest} <- Manifest.decode(blob) do
      Manifest.shipped_through(manifest)
    else
      _ -> -1
    end
  end
end
