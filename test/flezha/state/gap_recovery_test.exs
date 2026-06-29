defmodule Flezha.State.GapRecoveryTest do
  @moduledoc """
  Defect B / `operative-state-persistence`: a persistence seq gap must
  trigger a `RestoreInitState` resync (and let the WAL advance again)
  rather than wedge the Segment forever with `{:error, {:gap}}`.

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

    segment_id = TestHelpers.sample_segment_id("gap_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(segment_id)
    {:ok, writer} = Supervisor.lookup(segment_id)
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
      # Close the writer and drop its fsync mark before deleting the dir,
      # else the app's 5 ms batcher fsyncs a now-missing file and the writer
      # crashes (the resync leaves seq 4/5 dirty and unflushed).
      Supervisor.delete_segment(segment_id)
      Application.delete_env(:flezha, :state_resync_fun)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{segment_id: segment_id, writer: writer, header: header, parent: parent}
  end

  defp simulate_sdk_resync(parent, header) do
    # The real SDK rewinds and resends from terminal_seq + 1; here we
    # append the next contiguous seq directly so the WAL resumes.
    fn segment_id, opts ->
      result =
        case Supervisor.lookup(segment_id) do
          {:ok, w} -> WalWriter.append(w, WalWriter.last_persisted_seq(w) + 1, header, "resync")
          _ -> :no_writer
        end

      send(parent, {:resync_called, segment_id, opts, result})
      {:ok, make_ref()}
    end
  end

  defp start_channel(executor_id) do
    name = :"sc_#{uniq()}"
    {:ok, pid} = StreamChannel.start_link({executor_id, name, :primary})
    pid
  end

  defp frame({rc, sh, pk}, seq, header, body) do
    pid_bytes = Wire.encode_segment_id(rc, sh, pk)

    <<byte_size(pid_bytes)::16-big, pid_bytes::binary, seq::64-big, header::binary,
      byte_size(body)::32-big, body::binary, :erlang.crc32(body)::32-big>>
  end

  test "a gap resyncs the segment and the WAL advances again", ctx do
    Application.put_env(:flezha, :state_resync_fun, simulate_sdk_resync(ctx.parent, ctx.header))
    sc = start_channel("exec-gap")

    send(
      sc,
      {:flz_data, 0, "segment_state_change", frame(ctx.segment_id, 37, ctx.header, "z"), %{}}
    )

    # Gap is detected (expected 4, got 37) and telemetry is emitted.
    assert_receive {:seq_gap, %{expected: 4, got: 37}, meta}, 1_000
    assert meta.operative == "TestOperative"

    # Resync runs for the Segment; the simulated SDK resend advances the
    # WAL past the frozen seq 3.
    assert_receive {:resync_called, segment_id, opts, :ok}, 1_000
    assert segment_id == ctx.segment_id
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
      {:flz_data, 0, "segment_state_change", frame(ctx.segment_id, 4, ctx.header, "d"), %{}}
    )

    # Stale (seq 2) — deduped, no gap.
    send(
      sc,
      {:flz_data, 0, "segment_state_change", frame(ctx.segment_id, 2, ctx.header, "x"), %{}}
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
      {:flz_data, 0, "segment_state_snapshot", frame(ctx.segment_id, 3, ctx.header, "snap"), %{}}
    )

    # The snapshot write runs in a spawned process; poll until it lands.
    assert eventually(fn -> Flezha.State.Snapshot.latest_snapshot(ctx.segment_id) != nil end)
  end

  defp eventually(fun), do: eventually(fun, 50)
  defp eventually(_fun, 0), do: false
  defp eventually(fun, retries), do: fun.() || (Process.sleep(10) && eventually(fun, retries - 1))

  defp uniq(), do: Integer.to_string(System.unique_integer([:positive]))
end
