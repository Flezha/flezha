defmodule Flezha.State.InputLogTest do
  @moduledoc """
  §2.4 `durable-input-log`: offset monotonicity across appends and restarts,
  torn-tail discard, watermark truncation that respects the slowest Entity,
  frozen-watermark telemetry, and the tier-off cost-free default (no
  journaling work, no file).
  """
  use ExUnit.Case, async: false
  use Mimic

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operative.Ingest
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Replay
  alias Flezha.State.{FsyncBatcher, InputLog, Registry, Supervisor, TestHelpers}

  setup do
    base = TestHelpers.tmp_app_data_dir()

    case start_supervised({Elixir.Registry, keys: :unique, name: Supervisor.registry_name()}) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(Supervisor) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    case start_supervised(FsyncBatcher) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{base: base, class: "il_" <> Integer.to_string(System.unique_integer([:positive]))}
  end

  test "offsets are strictly monotonic and payloads round-trip in order", %{class: class} do
    offsets =
      for i <- 0..4 do
        {:ok, offset} = InputLog.append(class, "msg-#{i}", %{"input_type" => "2", "i" => i})
        offset
      end

    assert offsets == [0, 1, 2, 3, 4]
    assert InputLog.last_offset(class) == 4

    records = InputLog.read_range(class, 0, 4)
    assert Enum.map(records, &elem(&1, 0)) == [0, 1, 2, 3, 4]
    assert Enum.map(records, &elem(&1, 1)) == for(i <- 0..4, do: "msg-#{i}")
    assert records |> List.first() |> elem(2) == %{"input_type" => "2", "i" => 0}
  end

  test "a restarted log continues from the last durable offset", %{class: class} do
    for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    stop_log(class)

    {:ok, offset} = InputLog.append(class, "m3", %{})
    assert offset == 3
  end

  test "a torn tail is discarded without affecting earlier records", %{class: class} do
    for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    stop_log(class)

    # Simulate a crash mid-append: garbage where the next record would start.
    File.write!(Registry.input_log_path(class), <<9, 9, 9, 9, 9>>, [:append])

    assert InputLog.read_range(class, 0, 99) |> Enum.map(&elem(&1, 0)) == [0, 1, 2]
    {:ok, offset} = InputLog.append(class, "m3", %{})
    assert offset == 3
  end

  test "truncation removes only offsets below the slowest entity", %{class: class} do
    for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})

    InputLog.record_consumed(class, {class, "proc", "a"}, 8)
    InputLog.record_consumed(class, {class, "proc", "b"}, 4)
    assert InputLog.watermark(class) == 4

    :ok = InputLog.sweep(class)

    assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [4, 5, 6, 7, 8, 9]
  end

  describe "the hold window (stream-late-input-correction G4)" do
    setup do
      original = Application.get_env(:flezha, :input_log_hold_window_ms, 0)
      on_exit(fn -> Application.put_env(:flezha, :input_log_hold_window_ms, original) end)
      :ok
    end

    test "records carry their append time", %{class: class} do
      before = System.system_time(:millisecond)
      for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      later = System.system_time(:millisecond)

      # Nothing was appended before `before`, and everything was appended before
      # `later` — so the time→offset lookup brackets the whole log.
      assert InputLog.offset_before_time(class, before) == nil
      assert InputLog.offset_before_time(class, later + 1) == 2
    end

    test "a consumed record is held until it also ages out of the window", %{class: class} do
      for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      InputLog.record_consumed(class, {class, "proc", "a"}, 9)

      # A correction replays CONSUMED inputs, so the watermark alone must not be
      # allowed to reclaim them. An hour-long window holds everything just written.
      Application.put_env(:flezha, :input_log_hold_window_ms, 3_600_000)
      :ok = InputLog.sweep(class)
      assert InputLog.read_range(class, 0, 9) |> length() == 10

      # This is the property that used to be missing entirely: `truncation_floor/1`
      # returned nil for ANY non-zero hold window, i.e. it held everything
      # forever. A window the records have aged out of must actually reclaim them
      # (down to the floor, which is retained — the existing conservative
      # keep-at-or-above-the-floor contract).
      Application.put_env(:flezha, :input_log_hold_window_ms, 1)
      Process.sleep(5)
      :ok = InputLog.sweep(class)
      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [9]
    end

    test "the window never reclaims past the watermark", %{class: class} do
      for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      # A lagging Entity has only consumed up to 4.
      InputLog.record_consumed(class, {class, "proc", "b"}, 4)

      # Every record has aged out, but an unconsumed input must never be dropped:
      # the floor is min(watermark, aged_out), not the age bound alone.
      Application.put_env(:flezha, :input_log_hold_window_ms, 1)
      Process.sleep(5)
      :ok = InputLog.sweep(class)

      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [4, 5, 6, 7, 8, 9]
    end
  end

  test "no reported consumed-offset holds the watermark at the log start", %{class: class} do
    for i <- 0..4, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})

    assert InputLog.watermark(class) == nil
    :ok = InputLog.sweep(class)

    assert InputLog.read_range(class, 0, 4) |> length() == 5
  end

  test "a frozen watermark past the size threshold emits telemetry", %{class: class} do
    for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    InputLog.record_consumed(class, {class, "proc", "slow"}, 1)

    ref = make_ref()
    parent = self()

    :telemetry.attach(
      "input-log-frozen-#{inspect(ref)}",
      [:flezha, :state, :input_log_frozen_watermark],
      fn _event, measurements, metadata, _ -> send(parent, {ref, measurements, metadata}) end,
      nil
    )

    Application.put_env(:flezha, :wal_high_watermark_bytes, 1)

    on_exit(fn ->
      Application.delete_env(:flezha, :wal_high_watermark_bytes)
      :telemetry.detach("input-log-frozen-#{inspect(ref)}")
    end)

    # First sweep truncates (watermark newly known); second sees it frozen.
    :ok = InputLog.sweep(class)
    :ok = InputLog.sweep(class)

    assert_receive {^ref, %{size_bytes: size}, meta}
    assert size > 0
    assert meta.operative == class
    assert meta.lagging_entity == {class, "proc", "slow"}
  end

  test "tier-off operative journals nothing and forwards untouched", %{class: class} do
    state = %Overwatch{
      id: class,
      builder: %Flezha.Operative.Builder{id: class, factory_id: "f"},
      effectively_once: false
    }

    payload = {"message", %{"input_type" => "2"}}
    assert Ingest.journal_input(state, payload) == payload
    assert Ingest.journal_input(state, nil) == nil
    refute File.exists?(Registry.input_log_path(class))
    refute class in InputLog.all_classes()
  end

  test "tier-on operative journals before forward and stamps flz.offset", %{class: class} do
    state = %Overwatch{
      id: class,
      builder: %Flezha.Operative.Builder{id: class, factory_id: "f"},
      effectively_once: true
    }

    {message, metadata} = Ingest.journal_input(state, {"message", %{"input_type" => "2"}})
    assert message == "message"
    assert metadata["flz.offset"] == "0"

    assert [{0, "message", %{"input_type" => "2"}}] = InputLog.read_range(class, 0, 0)
    assert class in InputLog.all_classes()
  end

  describe "crash-replay re-forward (§6.1)" do
    setup :set_mimic_global

    # The forward lands on the stream through `StreamChannel.io_in/3`; the
    # test stands in for it.
    setup do
      test_pid = self()

      stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, id, payload ->
        send(test_pid, {:forwarded, id, payload})
        :ok
      end)

      :ok
    end

    test "re-forwards only the tail above the min watermark, offsets intact", %{class: class} do
      for i <- 0..4, do: {:ok, _} = InputLog.append(class, "m#{i}", %{"input_type" => "2"})
      InputLog.record_consumed(class, {class, "proc", "a"}, 2)

      state = %Overwatch{
        id: class,
        builder: %Flezha.Operative.Builder{id: class, factory_id: "f"},
        channel: :"chan_#{class}",
        effectively_once: true
      }

      :ok = Replay.replay_journaled_inputs(state)

      assert_receive {:forwarded, ^class, {"m3", md3}}
      assert md3["flz.offset"] == "3"
      assert md3["input_type"] == "2"
      assert_receive {:forwarded, ^class, {"m4", md4}}
      assert md4["flz.offset"] == "4"
      refute_receive {:forwarded, _, _}, 50
    end

    test "no journal file means no re-forward work", %{class: class} do
      state = %Overwatch{
        id: class,
        builder: %Flezha.Operative.Builder{id: class, factory_id: "f"},
        channel: :"chan_#{class}",
        effectively_once: true
      }

      :ok = Replay.replay_journaled_inputs(state)
      refute_receive {:forwarded, _, _}, 50
      refute File.exists?(Registry.input_log_path(class))
    end
  end

  describe "source-ingestion credit (§7)" do
    setup do
      Application.put_env(:flezha, :source_credit_window_bytes, 16)
      on_exit(fn -> Application.delete_env(:flezha, :source_credit_window_bytes) end)
      :ok
    end

    test "a frozen watermark parks the source; the mint resumes it", %{class: class} do
      # First append always admits (empty-buffer rule); the second overruns
      # the 16-byte window and parks — the source's blocking ingest call.
      {:ok, 0} = InputLog.append(class, "m0", %{})

      parked =
        Task.async(fn -> InputLog.append(class, "m1", %{}) end)

      refute Task.yield(parked, 150), "append must park at zero source credit"

      # The watermark advance past offset 0 drains its bytes and mints
      # credit (batched by the low watermark) — the parked append resumes.
      InputLog.record_consumed(class, {class, "proc", "a"}, 0)

      assert {:ok, 1} = Task.await(parked, 2_000)
    end

    test "one starved operative parks only its own dispatch", %{class: class} do
      healthy = class <> "_healthy"

      {:ok, 0} = InputLog.append(class, "m0", %{})
      starved = Task.async(fn -> InputLog.append(class, "m1", %{}) end)
      refute Task.yield(starved, 150)

      # The healthy operative's ledger is independent: appends keep flowing.
      {:ok, 0} = InputLog.append(healthy, "h0", %{})
      InputLog.record_consumed(healthy, {healthy, "proc", "a"}, 0)
      {:ok, 1} = InputLog.append(healthy, "h1", %{})

      # Unpark the starved one so no waiter leaks past the test.
      InputLog.record_consumed(class, {class, "proc", "a"}, 0)
      assert {:ok, 1} = Task.await(starved, 2_000)
    end
  end

  describe "the hold window derives from the declared correction window" do
    alias Flezha.Operative.ClassRegistry

    setup do
      ClassRegistry.ensure_table()
      prev = Application.get_env(:flezha, :input_log_hold_window_ms, 0)
      on_exit(fn -> Application.put_env(:flezha, :input_log_hold_window_ms, prev) end)
      :ok
    end

    test "a declared window becomes the hold window on default config" do
      class = "corrector-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :input_log_hold_window_ms, 0)
      :ok = ClassRegistry.put_correction_window(class, 60_000)

      # This is the whole point: correction used to be unreachable, because the
      # operator-configured floor defaults to 0 and start was refused against it.
      assert InputLog.hold_window_ms(class) == 60_000
    end

    test "a more generous operator floor wins" do
      class = "corrector-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :input_log_hold_window_ms, 300_000)
      :ok = ClassRegistry.put_correction_window(class, 60_000)

      assert InputLog.hold_window_ms(class) == 300_000
    end

    test "a class declaring no window has no hold" do
      class = "plain-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :input_log_hold_window_ms, 0)
      :ok = ClassRegistry.put_correction_window(class, nil)

      assert InputLog.hold_window_ms(class) == 0
    end

    test "a declared window holds a fully-consumed tail against truncation" do
      class = "held-#{System.unique_integer([:positive])}"
      Application.put_env(:flezha, :input_log_hold_window_ms, 0)
      :ok = ClassRegistry.put_correction_window(class, 600_000)

      {:ok, 0} = InputLog.append(class, "i0", %{})
      {:ok, 1} = InputLog.append(class, "i1", %{})
      InputLog.record_consumed(class, {class, "proc", "a"}, 1)

      # Everything is consumed, so the watermark alone would truncate it all —
      # but it is newer than the derived 10-minute window, so it stays.
      :ok = InputLog.sweep(class)

      assert InputLog.read_range(class, 0, 1) |> length() == 2,
             "a declared correction window must hold the inputs it would replay"
    end
  end

  describe "producing-liveness cursors (§8)" do
    test "a class with no live log yields nil and no log is created" do
      class = "cursorless-#{System.unique_integer([:positive])}"

      assert InputLog.cursors(class) == nil

      # Lookup-only: polling this from the convergence tick must never conjure a
      # file, a process, or a watermark for an operative that never asked for one.
      assert Supervisor.lookup({class, :input_log}) == :error
      refute File.exists?(Registry.input_log_path(class))
    end

    test "a live log yields its tail and each Entity's own consumed offset" do
      class = "cursors-#{System.unique_integer([:positive])}"
      seg_a = {class, "proc", "a"}
      seg_b = {class, "proc", "b"}

      {:ok, 0} = InputLog.append(class, "i0", %{})
      {:ok, 1} = InputLog.append(class, "i1", %{})
      {:ok, 2} = InputLog.append(class, "i2", %{})

      InputLog.record_consumed(class, seg_a, 2)
      InputLog.record_consumed(class, seg_b, 1)

      assert %{tail: 2, consumed: consumed} = InputLog.cursors(class)

      # Per Entity, NOT the min across them — the classifier needs to name the
      # Entity that is actually lagging, which the min cannot do.
      assert consumed[seg_a] == 2
      assert consumed[seg_b] == 1

      # The min IS the watermark, derived rather than pre-computed.
      assert InputLog.watermark(class) == 1
    end

    test "an Entity that has reported nothing is absent from consumed" do
      class = "cursors-quiet-#{System.unique_integer([:positive])}"
      {:ok, 0} = InputLog.append(class, "i0", %{})

      assert %{tail: 0, consumed: consumed} = InputLog.cursors(class)
      assert consumed == %{}
    end
  end

  defp stop_log(class) do
    {:ok, pid} = Supervisor.lookup({class, :input_log})
    :ok = GenServer.stop(pid)
    # The Registry sweeps dead entries asynchronously; wait so the next
    # find_or_start starts a fresh server instead of racing the dead pid.
    assert wait_until(fn -> Supervisor.lookup({class, :input_log}) == :error end, 2_000)
  end

  describe "a truncation costs the tail, not the log" do
    test "a floor past the index samples keeps exactly the tail", %{class: class} do
      for i <- 0..599, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      InputLog.record_consumed(class, {class, "proc", "a"}, 400)
      :ok = InputLog.sweep(class)

      assert InputLog.read_range(class, 0, 599) |> Enum.map(&elem(&1, 0)) ==
               Enum.to_list(400..599)

      # The second cut works against the rebased index, and appends land at the
      # right place afterwards.
      InputLog.record_consumed(class, {class, "proc", "a"}, 550)
      :ok = InputLog.sweep(class)
      {:ok, 600} = InputLog.append(class, "m600", %{})

      assert InputLog.read_range(class, 0, 600) |> Enum.map(&elem(&1, 0)) ==
               Enum.to_list(550..600)
    end

    test "a restarted log truncates the records it never indexed", %{class: class} do
      for i <- 0..299, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      {:ok, pid} = Flezha.State.Supervisor.lookup({class, :input_log})
      :ok = GenServer.stop(pid)
      # The registry drops the dead pid a moment after the exit; `append/3`
      # retries a `:noproc` only once, with no delay.
      assert Enum.any?(1..200, fn _ ->
               Flezha.State.Supervisor.lookup({class, :input_log}) == :error or
                 (Process.sleep(5) && false)
             end)

      {:ok, 300} = InputLog.append(class, "m300", %{})
      InputLog.record_consumed(class, {class, "proc", "a"}, 290)
      :ok = InputLog.sweep(class)

      assert InputLog.read_range(class, 0, 300) |> Enum.map(&elem(&1, 0)) ==
               Enum.to_list(290..300)
    end
  end
end
