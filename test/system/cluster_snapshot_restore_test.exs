defmodule Flezha.Cluster.SnapshotRestoreSystemTest do
  @moduledoc """
  Two-node cluster system test for `flz cluster snapshot` / `flz cluster restore`.

  Boots a peer BEAM via `:peer.start_link`, distributes processors across
  the two nodes via ProcessHub's hash ring, drives the snapshot path
  through real cross-node `:rpc.call` fan-out (the local-node arm is
  covered in `test/cluster/snapshot_e2e_test.exs`), and asserts the
  restore path re-spawns processors with primed state.

  Tagged `:cluster_snapshot_cst` for selective running with
  `mix test --only cluster_snapshot_cst`. Heavy but matches the existing
  `flezha_test.exs` peer-node CST conventions.
  """

  use ExUnit.Case

  alias Flezha.Cluster.Snapshot.Coordinator
  alias Flezha.Cluster.Snapshot.Encoding
  alias Flezha.Processor.Builder
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias Flezha.Service.ProcessorManager
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "default"
  @factory "store_input"

  setup do
    on_exit(fn -> ProcessorManager.stop_all_processors(@namespace) end)
    :ok
  end

  @tag :cluster_snapshot_cst
  test "two-node round-trip: snapshot gathers cross-node, restore re-spawns with primed state" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:cs_snap1)

    on_exit(fn ->
      try do
        NodeTestHelper.stop_peer_node(peer_pid, peer_node)
      catch
        _, _ -> :ok
      end
    end)

    wait_for_hub_join(peer_node)

    # Two processors so ProcessHub's hash ring has at least a chance of
    # distributing across both nodes. The exact placement is ring-decided
    # and not what the test pins on — we only require that whatever
    # nodes end up hosting the processors before snapshot, the same set
    # is hosting them after restore.
    processor_ids = ["cs-snap-proc-a", "cs-snap-proc-b"]

    placements_before =
      for id <- processor_ids, into: %{} do
        builder = %Builder{
          id: id,
          factory_id: @factory,
          init_values: %{},
          opts: [snapshot_interval: :disabled]
        }

        {:ok, info} = ProcessorManager.start_processor(@namespace, builder)
        {id, info.node}
      end

    # ProcessHub's distributed registry settles asynchronously; let both
    # nodes catch up to the new placements before driving snapshots.
    wait_for_processors_visible(processor_ids)

    # Trigger one synchronous snapshot per processor so each Overwatch
    # has a non-nil cached state binary at gather time.
    captured =
      for id <- processor_ids, into: %{} do
        {:ok, bin} = ProcessorManager.create_snapshot(@namespace, id)
        assert is_binary(bin)
        {id, bin}
      end

    tmp_path = Path.join(System.tmp_dir!(), "cs_snap_#{System.unique_integer([:positive])}.flz")
    on_exit(fn -> File.rm(tmp_path) end)

    # Phase A — snapshot. Cross-node :rpc.call fan-out runs against the
    # peer's `Flezha.Cluster.Snapshot.local_state/0`. Other tests in the
    # suite may have leaked processors into the default namespace, so
    # we assert on the *subset* belonging to this test rather than the
    # exact total.
    assert {:ok, _summary} = Coordinator.snapshot(tmp_path, peer_timeout_ms: 5_000)
    assert File.exists?(tmp_path)
    refute File.exists?(tmp_path <> ".tmp")

    assert {:ok, payload} = Encoding.decode(File.read!(tmp_path))

    # Both nodes appear in the cluster manifest.
    assert node() in payload.cluster.nodes
    assert peer_node in payload.cluster.nodes

    # Our two processors landed in the file with the right
    # `last_observed_node` tagging (gather→reconcile path).
    decoded_placements =
      for entry <- payload.processors,
          entry.processor.id in processor_ids,
          into: %{} do
        {entry.processor.id, entry.last_observed_node}
      end

    assert decoded_placements == placements_before

    # Captured state binaries round-trip from each Overwatch through
    # the gather RPC into the file.
    for id <- processor_ids do
      assert payload.captured_states[id] == captured[id]
    end

    # Phase B — restore the file. Other tests' processors may still be
    # running in the namespace; restoring our snapshot must re-spawn ours
    # without killing theirs.
    for id <- processor_ids do
      :ok = ProcessorManager.stop_processor(@namespace, id)
    end

    Process.sleep(300)

    for id <- processor_ids do
      assert {:error, _} = ProcessorManager.get_processor_pid(@namespace, id)
    end

    assert {:ok, %{processors: snapshot_count}} =
             Coordinator.restore(tmp_path, warn_if_older_than_ms: :infinity)

    assert snapshot_count >= 2

    # Wait for ProcessHub to re-register our two processors.
    wait_for_processors_visible(processor_ids)
    after_restore_ids =
      HubManager.get_all_processors(@namespace)
      |> Enum.map(fn {id, _pid} -> id end)

    for id <- processor_ids do
      assert id in after_restore_ids
    end

    # State priming: a fresh snapshot per processor must yield the same
    # binary as before — proving `:initial_state_binary` was applied
    # before the first input.
    for id <- processor_ids do
      {:ok, bin_after} = ProcessorManager.create_snapshot(@namespace, id)

      assert bin_after == captured[id],
             "post-restore snapshot for #{id} differs from pre-restore — state was not primed"
    end
  end

  defp wait_for_processors_visible(processor_ids, attempts \\ 20) do
    visible =
      processor_ids
      |> Enum.all?(fn id ->
        case ProcessorManager.get_processor_pid(@namespace, id) do
          {:ok, pid} when is_pid(pid) -> true
          _ -> false
        end
      end)

    cond do
      visible ->
        :ok

      attempts <= 0 ->
        flunk("Processors #{inspect(processor_ids)} never became visible across the cluster")

      true ->
        Process.sleep(100)
        wait_for_processors_visible(processor_ids, attempts - 1)
    end
  end

  defp wait_for_hub_join(peer_node) do
    hub_id = Identificator.hub(@namespace)
    test_pid = self()

    ProcessHub.register_hook_handlers(
      hub_id,
      ProcessHub.Constant.Hook.post_node_join(),
      [
        %ProcessHub.Service.HookManager{
          id: "cs_snap_hub_join_wait",
          m: Process,
          f: :send,
          a: [test_pid, :peer_node_joined, []]
        }
      ]
    )

    if length(HubManager.get_hub_nodes(@namespace, true)) < 2 do
      receive do
        :peer_node_joined -> :ok
      after
        5_000 -> flunk("Peer node #{inspect(peer_node)} did not join the hub cluster within 5s")
      end
    end

    ProcessHub.cancel_hook_handlers(hub_id, ProcessHub.Constant.Hook.post_node_join(), [
      "cs_snap_hub_join_wait"
    ])
  end
end
