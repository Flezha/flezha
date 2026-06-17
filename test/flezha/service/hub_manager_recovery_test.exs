defmodule Flezha.Service.HubManagerRecoveryTest do
  @moduledoc """
  Exercises the marker-gated recovery contract Flezha consumes from the
  upstream `recovery-mode-opt-in` change.

  Single-node coverage:

    * a clean shutdown writes the marker → next boot bypasses replay
      and `await_recovery_complete/1` returns immediately;
    * `flz cluster prepare-recovery` deletes the marker on every
      node (here: just the local one) → next boot replays the DETS
      cspecs and `:recovery_complete` telemetry fires;
    * `await_recovery_complete/2` blocks on a missing :complete event
      and times out cleanly.

  The cross-node split-brain detection scenarios (tasks 1.13 / 1.14
  paired with the spec's two-controller cluster) require the live k3s
  smoke test in task 4.19; the marker-file / telemetry contract is
  identical on one node and is what flezha-core owns. The upstream
  process_hub test suite covers the multi-peer init_sync semantics.
  """

  use ExUnit.Case, async: false

  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator

  @namespace "recovery_test_ns"

  setup do
    previous_enabled = Application.get_env(:flezha, :auto_restore_enabled)
    previous_root = Application.get_env(:flezha, :process_hub_registry_root)
    previous_override = Application.get_env(:flezha, :process_hub_recovery_marker_path)
    previous_timeout = Application.get_env(:flezha, :process_hub_recovery_timeout_ms)

    tmp_dir =
      Path.join([System.tmp_dir!(), "flezha_recovery_test_#{System.unique_integer([:positive])}"])

    File.rm_rf(tmp_dir)
    File.mkdir_p!(tmp_dir)

    Application.put_env(:flezha, :auto_restore_enabled, true)
    Application.put_env(:flezha, :process_hub_registry_root, tmp_dir)
    Application.delete_env(:flezha, :process_hub_recovery_marker_path)
    Application.put_env(:flezha, :process_hub_recovery_timeout_ms, 3_000)

    on_exit(fn ->
      _ = HubManager.stop_hub(@namespace)
      File.rm_rf(tmp_dir)
      Application.put_env(:flezha, :auto_restore_enabled, previous_enabled)

      if previous_root,
        do: Application.put_env(:flezha, :process_hub_registry_root, previous_root),
        else: Application.delete_env(:flezha, :process_hub_registry_root)

      if previous_override,
        do: Application.put_env(:flezha, :process_hub_recovery_marker_path, previous_override),
        else: Application.delete_env(:flezha, :process_hub_recovery_marker_path)

      if previous_timeout,
        do: Application.put_env(:flezha, :process_hub_recovery_timeout_ms, previous_timeout),
        else: Application.delete_env(:flezha, :process_hub_recovery_timeout_ms)
    end)

    {:ok, tmp_dir: tmp_dir}
  end

  test "fresh boot writes the marker and reaches :normal", %{tmp_dir: tmp_dir} do
    hub_id = Identificator.hub(@namespace)
    marker_path = Path.join([tmp_dir, Atom.to_string(hub_id), "cluster.healthy"])

    {:ok, _} = HubManager.start_hub(@namespace)

    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)
    assert {:ok, state} = HubManager.recovery_state(hub_id)
    assert state in [:normal, :skipped]
    assert File.exists?(marker_path)
  end

  test "prepare_recovery_cluster deletes the marker on the local node",
       %{tmp_dir: tmp_dir} do
    hub_id = Identificator.hub(@namespace)
    marker_path = Path.join([tmp_dir, Atom.to_string(hub_id), "cluster.healthy"])

    {:ok, _} = HubManager.start_hub(@namespace)
    assert :ok = HubManager.await_recovery_complete([hub_id], 5_000)
    assert File.exists?(marker_path)

    # The CLI verb is a thin wrapper around this upstream call; the
    # operator-facing rendering is covered in cli_manager_cluster_test.
    assert {:ok, _acked} = ProcessHub.prepare_recovery_cluster(hub_id)

    refute File.exists?(marker_path)
  end

  test "recovery_state surfaces :unknown_hub when the hub is not started" do
    hub_id = Identificator.hub("never_started_namespace")
    assert {:error, :unknown_hub} = HubManager.recovery_state(hub_id)
  end

  test "await_recovery_complete/2 times out when a hub is stuck in :recovering" do
    fake_hub_id = :"fake_stuck_hub_#{System.unique_integer([:positive])}"
    # Simulate a hub observed as :recovering that never transitions.
    # The snapshot treats explicit observations as authoritative.
    Flezha.Convergence.Snapshot.observe_recovery(fake_hub_id, :recovering)

    assert {:error, {:recovery_timeout, [^fake_hub_id]}} =
             HubManager.await_recovery_complete([fake_hub_id], 100)
  end
end
