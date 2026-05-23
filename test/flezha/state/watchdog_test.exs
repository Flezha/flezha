defmodule Flezha.State.WatchdogTest do
  @moduledoc """
  Defect B: the per-Partition output-staleness sweep and the
  compaction-handshake timeout. A live Partition whose persisted seq
  freezes is detected and resynced; a genuinely idle Partition is not
  flagged; a `CompactPartition` whose snapshot never returns is timed out
  and recovered, and stays retriable.
  """

  use ExUnit.Case, async: false

  alias Flezha.State.{Supervisor, TestHelpers, Watchdog, WalWriter}

  @stall_ms 50
  @compaction_ms 50

  setup do
    base = TestHelpers.tmp_app_data_dir()

    for spec <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher,
          # Long auto-tick: the tests drive sweeps explicitly via sweep_now/0.
          {Watchdog, interval_ms: 600_000}
        ] do
      case start_supervised(spec) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    Application.put_env(:flezha, :default_output_stall_timeout_ms, @stall_ms)
    Application.put_env(:flezha, :compaction_timeout_ms, @compaction_ms)

    parent = self()

    Application.put_env(:flezha, :state_resync_fun, fn partition_id, opts ->
      send(parent, {:resync_called, partition_id, opts})
      {:ok, make_ref()}
    end)

    out_id = {__MODULE__, :output_stall, System.unique_integer([:positive])}
    comp_id = {__MODULE__, :compaction_timeout, System.unique_integer([:positive])}

    :telemetry.attach(
      out_id,
      [:flezha, :state, :output_stall],
      fn _e, meas, meta, _ -> send(parent, {:output_stall, meas, meta}) end,
      nil
    )

    :telemetry.attach(
      comp_id,
      [:flezha, :state, :compaction_timeout],
      fn _e, meas, meta, _ -> send(parent, {:compaction_timeout, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(out_id)
      :telemetry.detach(comp_id)
      Application.delete_env(:flezha, :state_resync_fun)
      Application.delete_env(:flezha, :default_output_stall_timeout_ms)
      Application.delete_env(:flezha, :compaction_timeout_ms)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{header: TestHelpers.stub_header()}
  end

  defp live_writer(suffix, header) do
    pid = TestHelpers.sample_partition_id(suffix <> uniq())
    {:ok, _} = Supervisor.find_or_start(pid)
    {:ok, w} = Supervisor.lookup(pid)
    :ok = WalWriter.append(w, 1, header, "a")
    :ok = WalWriter.append(w, 2, header, "b")
    {pid, w}
  end

  test "a live partition whose seq froze is flagged and resynced", %{header: header} do
    {partition_id, writer} = live_writer("stall_", header)

    # Let the advance window lapse, then keep Changes arriving (duplicates)
    # without advancing the seq — the stall signature.
    Process.sleep(@stall_ms * 2)
    :duplicate = WalWriter.append(writer, 1, header, "a")

    :ok = Watchdog.sweep_now()

    assert_receive {:output_stall, %{stalled_ms: ms, last_seq: 2}, meta}, 1_000
    assert ms >= @stall_ms
    assert meta.relay == "TestRelay"
    assert_receive {:resync_called, ^partition_id, opts}, 1_000
    assert opts[:reason] == :output_stall
  end

  test "a genuinely idle partition is not flagged", %{header: header} do
    {_partition_id, _writer} = live_writer("idle_", header)

    # No Changes arrive after the window lapses — last_change_seen_at goes
    # stale alongside last_advance_at, so this is idle, not stalled.
    Process.sleep(@stall_ms * 2)

    :ok = Watchdog.sweep_now()

    refute_receive {:output_stall, _, _}, 200
    refute_receive {:resync_called, _, _}, 200
  end

  test "a stalled compaction times out, resyncs, and stays retriable" do
    partition_id = TestHelpers.sample_partition_id("comp_" <> uniq())

    Watchdog.compaction_started(partition_id)
    Process.sleep(@compaction_ms * 2)

    :ok = Watchdog.sweep_now()

    assert_receive {:compaction_timeout, %{waited_ms: ms}, meta}, 1_000
    assert ms >= @compaction_ms
    assert meta.relay == "TestRelay"
    assert_receive {:resync_called, ^partition_id, opts}, 1_000
    assert opts[:reason] == :compaction_timeout

    # Entry was cleared on timeout — a second sweep does not re-fire, and
    # compaction is eligible to retry on a later trigger.
    :ok = Watchdog.sweep_now()
    refute_receive {:compaction_timeout, _, _}, 200
  end

  test "a compaction that completes before the bound never times out" do
    partition_id = TestHelpers.sample_partition_id("compok_" <> uniq())

    Watchdog.compaction_started(partition_id)
    Watchdog.compaction_completed(partition_id)
    Process.sleep(@compaction_ms * 2)

    :ok = Watchdog.sweep_now()
    refute_receive {:compaction_timeout, _, _}, 200
  end

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
