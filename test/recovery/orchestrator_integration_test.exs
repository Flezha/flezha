defmodule Flezha.Recovery.OrchestratorIntegrationTest do
  @moduledoc """
  Integration test for `Cluster.Orchestrator` → `Recovery.SysMode` wiring.

  Rather than spinning up a full peer node (covered in the e2e suite for
  section 9.3), this test simulates the two-node scenario within a single
  BEAM: seed DETS so SysMode enters `:recover`, then hand-deliver the
  `{:peer_sysmode, _peer, :normal}` cast that the Orchestrator would trigger
  after a successful `Node.connect/1`, and assert SysMode transitions to
  `:normal` *without* running the recovery loop.

  Also performs a static check that `Cluster.Orchestrator` wires
  `SysMode.peer_connected/1` into both connect paths (initial and
  scheduled reconnect).
  """

  use ExUnit.Case, async: false

  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode
  alias Flezha.Recovery.Coordinator
  alias Flezha.Processor.Builder

  test "node A with DETS short-circuits :recover on peer_sysmode :normal" do
    tmp = Path.join(System.tmp_dir!(), "flz_orch_int_#{System.unique_integer([:positive])}")
    File.mkdir_p!(tmp)

    prev = snapshot_env()
    Application.put_env(:flezha, :recovery_data_dir, tmp)
    Application.put_env(:flezha, :recovery_enabled, true)
    # Long window — forces the short-circuit path to do the work.
    Application.put_env(:flezha, :recovery_window_ms, 60_000)

    on_exit(fn ->
      stop_if_running([Coordinator, SysMode, Store])
      restore_env(prev)
      File.rm_rf!(tmp)
    end)

    stop_if_running([Coordinator, SysMode, Store])

    # Seed the DETS file so SysMode starts in :recover.
    {:ok, table} =
      :dets.open_file(:flz_recovery, [
        {:type, :set},
        {:file, Path.join(tmp, "recovery.dets") |> String.to_charlist()}
      ])

    row = %{
      namespace: "ns",
      processor: %Builder{id: "p-seed", factory_id: "f"},
      stored_node: node(),
      inserted_at: System.system_time(:millisecond)
    }

    :ok = :dets.insert(table, {"p-seed", row})
    :ok = :dets.close(table)

    # Track recovery ran / didn't run via telemetry.
    handler_id = "orch-int-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach_many(
      handler_id,
      [
        [:flezha, :recovery, :sysmode_changed],
        [:flezha, :recovery, :completed]
      ],
      fn name, _m, meta, _cfg -> send(test_pid, {:telemetry, name, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    {:ok, _} = Store.start_link([])
    {:ok, _} = SysMode.start_link([])
    {:ok, _} = Coordinator.start_link([])

    assert SysMode.get() == :recover

    # Simulate what Orchestrator does: peer connected, peer reports :normal.
    GenServer.cast(SysMode, {:peer_sysmode, :"peerB@127.0.0.1", :normal})

    assert_receive {:telemetry, [:flezha, :recovery, :sysmode_changed],
                    %{from: :recover, to: :normal, reason: :peer_normal}},
                   500

    assert SysMode.get() == :normal

    # Give time for any incorrect :completed telemetry to fire. It should NOT.
    refute_receive {:telemetry, [:flezha, :recovery, :completed], _}, 200
  end

  test "Cluster.Orchestrator wires SysMode.peer_connected/1 on both connect paths" do
    {:ok, source} = File.read(Path.relative_to_cwd("lib/flezha/cluster/orchestrator.ex"))

    # Both the scheduled-reconnect branch and the initial connect_nodes loop
    # must call notify_recovery_peer_connected after Node.connect returns true.
    occurrences =
      source
      |> String.split("notify_recovery_peer_connected(")
      |> length()
      |> Kernel.-(1)

    assert occurrences >= 2,
           "expected at least two call sites for notify_recovery_peer_connected (initial + reconnect), got #{occurrences}"

    # And the SysMode alias / call is present.
    assert source =~ "Flezha.Recovery.SysMode.peer_connected"
  end

  defp stop_if_running(mods) do
    Enum.each(mods, fn mod ->
      try do
        case Process.whereis(mod) do
          nil -> :ok
          pid -> if Process.alive?(pid), do: GenServer.stop(pid, :normal, 1_000)
        end
      catch
        :exit, _ -> :ok
      end
    end)
  end

  defp snapshot_env do
    %{
      data_dir: Application.get_env(:flezha, :recovery_data_dir),
      enabled: Application.get_env(:flezha, :recovery_enabled),
      window: Application.get_env(:flezha, :recovery_window_ms)
    }
  end

  defp restore_env(%{data_dir: d, enabled: e, window: w}) do
    restore(:recovery_data_dir, d)
    restore(:recovery_enabled, e)
    restore(:recovery_window_ms, w)
  end

  defp restore(key, nil), do: Application.delete_env(:flezha, key)
  defp restore(key, value), do: Application.put_env(:flezha, key, value)
end
