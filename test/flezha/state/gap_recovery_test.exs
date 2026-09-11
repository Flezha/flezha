defmodule Flezha.State.GapRecoveryTest do
  @moduledoc """
  Defect B / `operative-state-persistence`: a persistence seq gap must
  trigger a `RestoreInitState` resync (and let the WAL advance again)
  rather than wedge the Entity forever with `{:error, {:gap}}`.

  Drives the production signature end-to-end: WAL frozen at
  `last_persisted_seq`, a gap arrives, the resync rewinds the SDK, and
  the WAL resumes — a workflow, self-healed blip with telemetry.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use FlezhaTest.StateCase, async: false

  import FlezhaTest.Helper.MessageTestHelper, only: [become_loop: 0]

  alias Flezha.Convergence.Heal
  alias Flezha.Runner.StreamChannel
  alias Flezha.State.Restore
  alias Flezha.State.Snapshot, as: StateSnapshot
  alias Flezha.State.{Supervisor, TestHelpers, WalWriter, Wire}
  alias FlezhaTest.Helper.NodeTestHelper

  setup %{base: base} do
    entity_id = TestHelpers.sample_entity_id("gap_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(entity_id)
    {:ok, writer} = Supervisor.lookup(entity_id)
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
      Supervisor.delete_entity(entity_id)
      Application.delete_env(:flezha, :state_resync_fun)
      File.rm_rf!(base)
    end)

    %{entity_id: entity_id, writer: writer, header: header, parent: parent}
  end

  defp simulate_sdk_resync(parent, header) do
    # The real SDK rewinds and resends from terminal_seq + 1; here we
    # append the next contiguous seq directly so the WAL resumes.
    fn entity_id, opts ->
      result =
        case Supervisor.lookup(entity_id) do
          {:ok, w} -> WalWriter.append(w, WalWriter.last_persisted_seq(w) + 1, header, "resync")
          _ -> :no_writer
        end

      send(parent, {:resync_called, entity_id, opts, result})
      {:ok, make_ref()}
    end
  end

  defp start_channel(runner_id) do
    name = :"sc_#{uniq()}"
    {:ok, pid} = StreamChannel.start_link({runner_id, name, :primary})
    pid
  end

  defp frame({rc, sh, pk}, seq, header, body) do
    pid_bytes = Wire.encode_entity_id(rc, sh, pk)

    <<byte_size(pid_bytes)::16-big, pid_bytes::binary, seq::64-big, header::binary,
      byte_size(body)::32-big, body::binary, :erlang.crc32(body)::32-big>>
  end

  test "a gap is declared, the planned resync repairs it, and the episode closes", ctx do
    become_loop()
    Application.put_env(:flezha, :state_resync_fun, simulate_sdk_resync(ctx.parent, ctx.header))
    sc = start_channel("exec-gap")

    send(
      sc,
      {:flz_data, 0, "entity_state_change", frame(ctx.entity_id, 37, ctx.header, "z"), %{}}
    )

    # Gap is detected (expected 4, got 37) and telemetry is emitted.
    assert_receive {:seq_gap, %{expected: 4, got: 37}, meta}, 1_000
    assert meta.operative == "TestOperative"

    # The writer declares the gap once, naming the runner; it resyncs nothing.
    entity_id = ctx.entity_id

    assert_receive {:declare, :seq_gap, ^entity_id, %{runner_id: "exec-gap", trigger: :seq_gap}},
                   1_000

    refute_received {:resync_called, _, _, _}

    # The remedy the loop plans from that fact, executed as the loop would.
    assert :ok =
             Heal.execute(%{
               primitive: :resync_runner,
               namespace: nil,
               target: entity_id,
               args: %{reason: :seq_gap, entity_id: entity_id, runner_id: "exec-gap"}
             })

    # Resync ran for the Entity; the simulated SDK resend advances the WAL
    # past the frozen seq 3, and the contiguous append closes the episode.
    assert_receive {:resync_called, ^entity_id, opts, :ok}, 1_000
    assert opts[:runner_id] == "exec-gap"
    assert WalWriter.last_persisted_seq(ctx.writer) == 4
    assert_receive {:declare, :seq_gap_closed, ^entity_id, %{}}, 1_000

    # And subsequent contiguous Changes are accepted (no permanent wedge).
    assert :ok = WalWriter.append(ctx.writer, 5, ctx.header, "e")
  end

  test "contiguous and stale frames declare no gap", ctx do
    become_loop()
    Application.put_env(:flezha, :state_resync_fun, simulate_sdk_resync(ctx.parent, ctx.header))
    sc = start_channel("exec-noop")

    # Contiguous (seq 4) — accepted, no gap.
    send(
      sc,
      {:flz_data, 0, "entity_state_change", frame(ctx.entity_id, 4, ctx.header, "d"), %{}}
    )

    # Stale (seq 2) — deduped, no gap.
    send(
      sc,
      {:flz_data, 0, "entity_state_change", frame(ctx.entity_id, 2, ctx.header, "x"), %{}}
    )

    # Both frames are through the channel and the writer once the channel answers.
    _ = GenServer.call(sc, :stream_ref_status)
    refute_received {:seq_gap, _, _}
    refute_receive {:declare, :seq_gap, _, _}, 100

    # The contiguous frame advanced the WAL to 4 via the normal path.
    assert WalWriter.last_persisted_seq(ctx.writer) == 4
  end

  test "a fresh controller declares a baseline pull from a live runner instead of wiping it",
       ctx do
    become_loop()
    # Cold takeover: this controller has no WAL/snapshot for the Entity,
    # while the live runner keeps streaming deltas from seq 5001.
    cold_id = TestHelpers.sample_entity_id("cold_" <> uniq())
    {:ok, _} = Supervisor.find_or_start(cold_id)
    {:ok, writer} = Supervisor.lookup(cold_id)
    on_exit(fn -> Supervisor.delete_entity(cold_id) end)

    sc = start_channel("exec-cold")

    deliver = fn payload_id, seq, body ->
      send(sc, {:flz_data, 0, payload_id, frame(cold_id, seq, ctx.header, body), %{}})
    end

    deliver.("entity_state_change", 5001, "d1")
    deliver.("entity_state_change", 5002, "d2")

    # The pull is declared exactly once, as a gap episode; no fatal seq_gap fires.
    assert_receive {:declare, :seq_gap, ^cold_id,
                    %{trigger: :baseline_seed, runner_id: "exec-cold"}},
                   1_000

    _ = GenServer.call(sc, :stream_ref_status)
    refute_receive {:declare, :seq_gap, ^cold_id, _}, 100
    refute_received {:seq_gap, _, _}

    # The pulled full-state snapshot lands via the real data-plane handler
    # and becomes the Entity's baseline.
    deliver.("entity_state_snapshot", 5005, "full")

    assert NodeTestHelper.wait_until(
             fn -> WalWriter.last_persisted_seq(writer) == 5005 end,
             500,
             10
           )

    # The baseline closes the episode; the runner's next delta appends
    # contiguously past it.
    assert_receive {:declare, :seq_gap_closed, ^cold_id, %{}}, 1_000
    deliver.("entity_state_change", 5006, "d3")

    assert NodeTestHelper.wait_until(
             fn -> WalWriter.last_persisted_seq(writer) == 5006 end,
             500,
             10
           )

    # A controller restart restores the complete state — snapshot baseline
    # plus the post-takeover delta, not just the deltas. `assemble_payload`
    # is exactly what the post-restart RestoreInitState would ship.
    :ok = WalWriter.close(writer)
    {:ok, payload} = Restore.assemble_payload(cold_id)
    assert payload.snapshot_bytes == "full"
    assert payload.terminal_seq_hint == 5006
    assert byte_size(payload.change_log_bytes) > 0
  end

  test "a snapshot frame is written off the channel loop", ctx do
    sc = start_channel("exec-snap")

    send(
      sc,
      {:flz_data, 0, "entity_state_snapshot", frame(ctx.entity_id, 3, ctx.header, "snap"), %{}}
    )

    # The snapshot write runs in a spawned process; poll until it lands.
    assert NodeTestHelper.wait_until(
             fn -> StateSnapshot.latest_snapshot(ctx.entity_id) != nil end,
             500,
             10
           )
  end
end
