defmodule Flezha.State.GapRecoveryTest do
  @moduledoc """
  Defect B / `relay-state-persistence`: a persistence seq gap must
  trigger a `RestoreInitState` resync (and let the WAL advance again)
  rather than wedge the Partition forever with `{:error, {:gap}}`.

  Drives the production signature end-to-end: WAL frozen at
  `last_persisted_seq`, a gap arrives, the resync rewinds the SDK, and
  the WAL resumes — a bounded, self-healed blip with telemetry.
  """

  use ExUnit.Case, async: false

  alias Flezha.Executor.StreamChannel
  alias Flezha.State.{Supervisor, TestHelpers, WalWriter, Wire}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    for spec <- [
          {Elixir.Registry, keys: :unique, name: Supervisor.registry_name()},
          Supervisor,
          Flezha.State.FsyncBatcher
        ] do
      case start_supervised(spec) do
        {:ok, _} -> :ok
        {:error, {:already_started, _}} -> :ok
      end
    end

    partition_id = TestHelpers.sample_partition_id("gap_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(partition_id)
    {:ok, writer} = Supervisor.lookup(partition_id)
    header = TestHelpers.stub_header()

    :ok = WalWriter.append(writer, 0, header, "z")

    :ok = WalWriter.append(writer, 1, header, "a")
    :ok = WalWriter.append(writer, 2, header, "b")
    :ok = WalWriter.append(writer, 3, header, "c")

    parent = self()
    handler_id = {__MODULE__, :seq_gap, System.unique_integer([:positive])}

    :telemetry.attach(
      handler_id,
      [:flezha, :state, :seq_gap],
      fn _e, meas, meta, _ -> send(parent, {:seq_gap, meas, meta}) end,
      nil
    )

    on_exit(fn ->
      :telemetry.detach(handler_id)
      Application.delete_env(:flezha, :state_resync_fun)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{partition_id: partition_id, writer: writer, header: header, parent: parent}
  end

  defp simulate_sdk_resync(parent, header) do
    # The real SDK rewinds and resends from terminal_seq + 1; here we
    # append the next contiguous seq directly so the WAL resumes.
    fn partition_id, opts ->
      result =
        case Supervisor.lookup(partition_id) do
          {:ok, w} -> WalWriter.append(w, WalWriter.last_persisted_seq(w) + 1, header, "resync")
          _ -> :no_writer
        end

      send(parent, {:resync_called, partition_id, opts, result})
      {:ok, make_ref()}
    end
  end

  defp start_channel(executor_id) do
    name = :"sc_#{uniq()}"
    {:ok, pid} = StreamChannel.start_link({executor_id, name, :primary})
    pid
  end

  defp frame({rc, sh, pk}, seq, header, body) do
    pid_bytes = Wire.encode_partition_id(rc, sh, pk)

    <<byte_size(pid_bytes)::16-big, pid_bytes::binary, seq::64-big, header::binary,
      byte_size(body)::32-big, body::binary, :erlang.crc32(body)::32-big>>
  end

  test "a gap resyncs the partition and the WAL advances again", ctx do
    Application.put_env(:flezha, :state_resync_fun, simulate_sdk_resync(ctx.parent, ctx.header))
    sc = start_channel("exec-gap")

    send(
      sc,
      {:flz_data, 0, "partition_state_change", frame(ctx.partition_id, 37, ctx.header, "z"), %{}}
    )

    # Gap is detected (expected 4, got 37) and telemetry is emitted.
    assert_receive {:seq_gap, %{expected: 4, got: 37}, meta}, 1_000
    assert meta.relay == "TestRelay"

    # Resync runs for the Partition; the simulated SDK resend advances the
    # WAL past the frozen seq 3.
    assert_receive {:resync_called, partition_id, opts, :ok}, 1_000
    assert partition_id == ctx.partition_id
    assert opts[:executor_id] == "exec-gap"
    assert WalWriter.last_persisted_seq(ctx.writer) == 4

    # And subsequent contiguous Changes are accepted (no permanent wedge).
    assert :ok = WalWriter.append(ctx.writer, 5, ctx.header, "e")
  end

  test "contiguous and stale frames do not resync", ctx do
    Application.put_env(:flezha, :state_resync_fun, simulate_sdk_resync(ctx.parent, ctx.header))
    sc = start_channel("exec-noop")

    # Contiguous (seq 4) — accepted, no gap.
    send(
      sc,
      {:flz_data, 0, "partition_state_change", frame(ctx.partition_id, 4, ctx.header, "d"), %{}}
    )

    # Stale (seq 2) — deduped, no gap.
    send(
      sc,
      {:flz_data, 0, "partition_state_change", frame(ctx.partition_id, 2, ctx.header, "x"), %{}}
    )

    refute_receive {:seq_gap, _, _}, 200
    refute_receive {:resync_called, _, _, _}, 200

    # The contiguous frame advanced the WAL to 4 via the normal path.
    assert WalWriter.last_persisted_seq(ctx.writer) == 4
  end

  test "a snapshot frame is written off the channel loop", ctx do
    sc = start_channel("exec-snap")

    send(
      sc,
      {:flz_data, 0, "partition_state_snapshot", frame(ctx.partition_id, 3, ctx.header, "snap"),
       %{}}
    )

    # The snapshot write runs in a spawned process; poll until it lands.
    assert eventually(fn -> Flezha.State.Snapshot.latest_snapshot(ctx.partition_id) != nil end)
  end

  defp eventually(fun), do: eventually(fun, 50)
  defp eventually(_fun, 0), do: false
  defp eventually(fun, retries), do: fun.() || (Process.sleep(10) && eventually(fun, retries - 1))

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
