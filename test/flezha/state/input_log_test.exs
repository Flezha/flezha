defmodule Flezha.State.InputLogTest do
  @moduledoc """
  `durable-input-log`: offset monotonicity across appends and restarts,
  torn-tail discard, watermark truncation that respects the slowest reporter,
  the message-id index, answers that wait for their sync, frozen-watermark
  telemetry, the fsync companion's death, and the tier-off cost-free default
  (no journaling work, no file).
  """
  # async: false: writes Application env; Mimic global mode; Mimic stubs.
  use FlezhaTest.StateCase, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2, wait_until: 3]
  import FlezhaTest.Helper.MessageTestHelper, only: [collect: 2]

  alias Flezha.Relay.Ingest
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.State.{FsyncBatcher, InputLog, Paths, RecordFrame, Supervisor, TestHelpers}
  alias Flezha.State.InputLog.Sweep
  alias FlezhaTest.Helper.Telemetry

  setup %{base: base} do
    class = "il_" <> uniq()
    # Every cut rewrites the file at once, so the cases can read it; the case
    # that proves the threshold raises it again.
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))

    on_exit(fn ->
      # The log lives in the shared state tree and truncates in its own turn
      # (`sweep/1` is a cast), so it must be closed — its queued cut done —
      # before the root under it goes away.
      TestHelpers.close_input_log(class)
      File.rm_rf!(base)
    end)

    %{class: class}
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
    File.write!(Paths.input_log_path(class), <<9, 9, 9, 9, 9>>, [:append])

    assert InputLog.read_range(class, 0, 99) |> Enum.map(&elem(&1, 0)) == [0, 1, 2]
    {:ok, offset} = InputLog.append(class, "m3", %{})
    assert offset == 3
  end

  test "truncation removes only offsets below the slowest cell", %{class: class} do
    for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})

    InputLog.record_consumed(class, {class, "task", "a"}, 8)
    InputLog.record_consumed(class, {class, "task", "b"}, 4)
    assert InputLog.watermark(class) == 4

    :ok = Sweep.run(class)

    assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [4, 5, 6, 7, 8, 9]
  end

  # fails when: the sweep reopens the log of a relay that moved away, so
  # its return sets the stale copy aside under an open writer.
  test "the sweep never opens a closed log", %{class: class} do
    for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    InputLog.record_consumed(class, {class, "task", "a"}, 8)
    on_exit(TestHelpers.override_env(:input_log_hold_window_ms, 60_000))
    :ok = Supervisor.close_writers(class)

    :ok = Sweep.run(class)
    assert InputLog.offset_before_time(class, System.system_time(:millisecond)) == nil

    assert Supervisor.lookup({class, :input_log}) == :error
  end

  describe "the confirmed processed prefix" do
    # An idle Cell's own offset no longer holds the floor once the runner's
    # prefix is confirmed past it.
    test "is the floor, past an idle Cell", %{class: class} do
      for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      InputLog.record_consumed(class, {class, "task", "BTC-USD"}, 1)
      InputLog.record_consumed(class, {class, "task", "SOL-USD"}, 9)

      assert InputLog.watermark(class) == 1
      :ok = InputLog.confirm_prefix(class, 9)
      assert InputLog.watermark(class) == 9

      :ok = Sweep.run(class)
      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [9]
    end

    # A controller restart loses the confirmed prefix; the floor falls back to
    # the slowest Cell, and a rebind re-forwards from there.
    test "falls back to the slowest Cell when it is lost", %{class: class} do
      for i <- 0..5, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 1)
      InputLog.record_consumed(class, {class, "task", "b"}, 5)
      :ok = InputLog.confirm_prefix(class, 5)

      :ets.delete(InputLog.watermark_table(), {class, :processed_prefix})

      assert InputLog.tail(class) == {:ok, 1, 5}
      assert InputLog.undelivered(class) |> Enum.map(&elem(&1, 0)) == [2, 3, 4, 5]
    end

    # A runner that spread the instance over two streams sent the marker on
    # each: one stream's confirmation alone moves nothing.
    test "a multi-stream marker moves the floor once every stream confirmed", %{class: class} do
      for i <- 0..3, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 0)
      stream_a = spawn(fn -> :ok end)
      stream_b = spawn(fn -> :ok end)

      :ok = InputLog.confirm_prefix(class, 3, streams: 2, stream: stream_a)
      :ok = InputLog.confirm_prefix(class, 3, streams: 2, stream: stream_a)
      _ = InputLog.last_offset(class)
      assert InputLog.watermark(class) == 0

      :ok = InputLog.confirm_prefix(class, 3, streams: 2, stream: stream_b)
      _ = InputLog.last_offset(class)
      assert InputLog.watermark(class) == 3
    end

    test "only moves forward, and a correction rewind clears it", %{class: class} do
      InputLog.record_consumed(class, {class, "task", "a"}, 3)
      :ok = InputLog.confirm_prefix(class, 7)
      :ok = InputLog.confirm_prefix(class, 4)
      assert InputLog.watermark(class) == 7

      InputLog.rewind_consumed(class, {class, "task", "a"}, 2)
      assert InputLog.watermark(class) == 2
    end
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
      InputLog.record_consumed(class, {class, "task", "a"}, 9)

      # A correction replays CONSUMED inputs, so the watermark alone must not be
      # allowed to reclaim them. An hour-long window holds everything just written.
      Application.put_env(:flezha, :input_log_hold_window_ms, 3_600_000)
      :ok = Sweep.run(class)
      assert InputLog.read_range(class, 0, 9) |> length() == 10

      # A window the records have aged out of must actually reclaim them
      # (down to the floor, which is retained — the existing conservative
      # keep-at-or-above-the-floor contract).
      Application.put_env(:flezha, :input_log_hold_window_ms, 1)
      aged_out()
      :ok = Sweep.run(class)
      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [9]
    end

    test "the window never reclaims past the watermark", %{class: class} do
      for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      # A lagging Cell has only consumed up to 4.
      InputLog.record_consumed(class, {class, "task", "b"}, 4)

      # Every record has aged out, but an unconsumed input must never be dropped:
      # the floor is min(watermark, aged_out), not the age bound alone.
      Application.put_env(:flezha, :input_log_hold_window_ms, 1)
      aged_out()
      :ok = Sweep.run(class)

      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [4, 5, 6, 7, 8, 9]
    end
  end

  test "no reported consumed-offset holds the watermark at the log start", %{class: class} do
    for i <- 0..4, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})

    assert InputLog.watermark(class) == nil
    :ok = Sweep.run(class)

    assert InputLog.read_range(class, 0, 4) |> length() == 5
  end

  test "a frozen watermark past the size threshold emits telemetry", %{class: class} do
    for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    InputLog.record_consumed(class, {class, "task", "slow"}, 1)

    ref = make_ref()

    Telemetry.capture([:flezha, :state, :input_log_frozen_watermark],
      as: ref
    )

    Application.put_env(:flezha, :wal_high_watermark_bytes, 1)

    on_exit(fn ->
      Application.delete_env(:flezha, :wal_high_watermark_bytes)
    end)

    # First sweep truncates (watermark newly known); second sees it frozen.
    :ok = Sweep.run(class)
    :ok = Sweep.run(class)

    assert_receive {^ref, %{size_bytes: size}, meta}
    assert size > 0
    assert meta.relay == class
    assert meta.lagging_cell == {class, "task", "slow"}
  end

  # The Overwatch's state as the ingest and replay paths need it: an empty
  # extension table, no channel unless given.
  defp overwatch(class, overrides) do
    struct!(
      %Overwatch{
        id: class,
        builder: %Flezha.Relay.Builder{id: class, factory_id: "f"},
        extensions: Table.new(Overwatch.__extensions_catalog__())
      },
      overrides
    )
  end

  test "tier-off relay journals nothing and forwards untouched", %{class: class} do
    state = overwatch(class, effectively_once: false)

    payload = {"message", %{"input_type" => "2"}}
    assert Ingest.ingest(state, "message", %{"input_type" => "2"}, :cast) == {payload, state}
    refute File.exists?(Paths.input_log_path(class))
    refute class in Sweep.all_classes()
  end

  test "tier-on relay journals before forward and stamps flz.offset", %{class: class} do
    test_pid = self()

    stub(Flezha.Runner.StreamChannel, :io_in, fn _channel, ^class, payload ->
      send(test_pid, {:forwarded, payload})
      :ok
    end)

    state = overwatch(class, effectively_once: true)

    # The append is asynchronous: the log answers this process, and handling
    # that answer is what stamps the offset and forwards — nothing reaches the
    # channel before the record is journaled.
    assert {:pending, state} = Ingest.ingest(state, "message", %{"input_type" => "2"}, :cast)

    refute_received {:forwarded, _}
    # The answer waits for a datasync, which a busy disk takes past 100 ms.
    assert_receive reply, 1_000
    assert {:ok, %Overwatch{}} = Ingest.journal_reply(state, reply)

    assert_receive {:forwarded, {"message", %{"flz.offset" => "0", "input_type" => "2"}}}
    assert [{0, "message", %{"input_type" => "2"}}] = InputLog.read_range(class, 0, 0)
    assert class in Sweep.all_classes()
  end

  describe "append_async/5 (the Overwatch's append)" do
    test "answers through the collection with the offset append/3 would give", %{class: class} do
      reqids = InputLog.append_async(class, "m0", %{}, :gen_server.reqids_new(), :first)

      assert {{:reply, {:ok, 0}}, :first, reqids} =
               :gen_server.wait_response(reqids, 1_000, true)

      assert :gen_server.reqids_size(reqids) == 0
      assert {:ok, 1} = InputLog.append(class, "m1", %{})
      assert [{0, "m0", %{}}, {1, "m1", %{}}] = InputLog.read_range(class, 0, 1)
    end

    test "a parked append answers in arrival order once credit mints", %{class: class} do
      on_exit(TestHelpers.override_env(:source_credit_window_bytes, 32))

      # One record fills the window on its own (an empty ledger admits any
      # size); the two behind it park, and both fit once it is consumed.
      {:ok, 0} = InputLog.append(class, String.duplicate("x", 40), %{})
      reqids = InputLog.append_async(class, "m1", %{}, :gen_server.reqids_new(), :m1)
      reqids = InputLog.append_async(class, "m2", %{}, reqids, :m2)
      assert held(class).parked == 2
      assert :timeout = :gen_server.wait_response(reqids, 0, true)

      InputLog.record_consumed(class, {class, "task", "a"}, 0)
      assert {{:reply, {:ok, 1}}, :m1, reqids} = :gen_server.wait_response(reqids, 2_000, true)
      assert {{:reply, {:ok, 2}}, :m2, _} = :gen_server.wait_response(reqids, 2_000, true)
    end

    test "a log that exits answers the collection with an error", %{class: class} do
      on_exit(TestHelpers.override_env(:source_credit_window_bytes, 16))
      {:ok, 0} = InputLog.append(class, "m0", %{})
      {:ok, log} = Supervisor.lookup({class, :input_log})

      reqids = InputLog.append_async(class, "m1", %{}, :gen_server.reqids_new(), :parked)
      assert held(class).parked == 1
      assert :timeout = :gen_server.wait_response(reqids, 0, true)

      # The parked append dies with the log: the monitored request reports it
      # instead of leaving the caller owed forever.
      :ok = GenServer.stop(log)

      assert {{:error, {:normal, ^log}}, :parked, _} =
               :gen_server.wait_response(reqids, 1_000, true)
    end
  end

  describe "crash-replay re-forward (§6.1)" do
    setup :set_mimic_global_until_exit

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
      InputLog.record_consumed(class, {class, "task", "a"}, 2)

      state = overwatch(class, channel: :"chan_#{class}", effectively_once: true)
      assert ^state = Replay.reforward_undelivered_tail(state)

      assert_receive {:forwarded, ^class, {"m3", md3}}
      assert md3["flz.offset"] == "3"
      assert md3["input_type"] == "2"
      assert_receive {:forwarded, ^class, {"m4", md4}}
      assert md4["flz.offset"] == "4"
      refute_received {:forwarded, _, _}
    end

    test "no journal file means no re-forward work", %{class: class} do
      state = overwatch(class, channel: :"chan_#{class}", effectively_once: true)
      assert ^state = Replay.reforward_undelivered_tail(state)
      refute_received {:forwarded, _, _}
      refute File.exists?(Paths.input_log_path(class))
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

      assert wait_until(fn -> held(class).parked == 1 end, 2_000),
             "append must park at zero source credit"

      refute Task.yield(parked, 0)

      # The watermark advance past offset 0 drains its bytes and mints
      # credit (batched by the low watermark) — the parked append resumes.
      InputLog.record_consumed(class, {class, "task", "a"}, 0)

      assert {:ok, 1} = Task.await(parked, 2_000)
    end

    test "one starved relay parks only its own dispatch", %{class: class} do
      healthy = class <> "_healthy"

      {:ok, 0} = InputLog.append(class, "m0", %{})
      starved = Task.async(fn -> InputLog.append(class, "m1", %{}) end)
      assert wait_until(fn -> held(class).parked == 1 end, 2_000)
      refute Task.yield(starved, 0)

      # The healthy relay's ledger is independent: appends keep flowing.
      {:ok, 0} = InputLog.append(healthy, "h0", %{})
      InputLog.record_consumed(healthy, {healthy, "task", "a"}, 0)
      {:ok, 1} = InputLog.append(healthy, "h1", %{})

      # Unpark the starved one so no waiter leaks past the test.
      InputLog.record_consumed(class, {class, "task", "a"}, 0)
      assert {:ok, 1} = Task.await(starved, 2_000)
    end
  end

  describe "the hold window derives from the declared correction window" do
    alias Flezha.Relay.ClassRegistry

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

      # The declaration alone must reach the floor: the user-configured
      # window defaults to 0, so nothing else would raise it.
      assert InputLog.hold_window_ms(class) == 60_000
    end

    test "a more generous user floor wins" do
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
      class = opened_class("held")
      Application.put_env(:flezha, :input_log_hold_window_ms, 0)
      :ok = ClassRegistry.put_correction_window(class, 600_000)

      {:ok, 0} = InputLog.append(class, "i0", %{})
      {:ok, 1} = InputLog.append(class, "i1", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 1)

      # Everything is consumed, so the watermark alone would truncate it all —
      # but it is newer than the derived 10-minute window, so it stays.
      :ok = Sweep.run(class)

      assert InputLog.read_range(class, 0, 1) |> length() == 2,
             "a declared correction window must hold the inputs it would replay"
    end
  end

  describe "producing-liveness cursors (§8)" do
    test "a class with no live log yields nil and no log is created" do
      class = "cursorless-#{System.unique_integer([:positive])}"

      assert InputLog.cursors(class) == nil

      # Lookup-only: polling this from the convergence tick must never conjure a
      # file, a process, or a watermark for a relay that never asked for one.
      assert Supervisor.lookup({class, :input_log}) == :error
      refute File.exists?(Paths.input_log_path(class))
    end

    test "a live log yields its tail and each Cell's own consumed offset" do
      class = opened_class("cursors")
      seg_a = {class, "task", "a"}
      seg_b = {class, "task", "b"}

      {:ok, 0} = InputLog.append(class, "i0", %{})
      {:ok, 1} = InputLog.append(class, "i1", %{})
      {:ok, 2} = InputLog.append(class, "i2", %{})

      InputLog.record_consumed(class, seg_a, 2)
      InputLog.record_consumed(class, seg_b, 1)

      assert %{tail: 2, consumed: consumed} = InputLog.cursors(class)

      # Per Cell, NOT the min across them — the classifier needs to name the
      # Cell that is actually lagging, which the min cannot do.
      assert consumed[seg_a] == 2
      assert consumed[seg_b] == 1

      # The min IS the watermark, derived rather than pre-computed.
      assert InputLog.watermark(class) == 1
    end

    test "a Cell that has reported nothing is absent from consumed" do
      class = opened_class("cursors-quiet")
      {:ok, 0} = InputLog.append(class, "i0", %{})

      assert %{tail: 0, consumed: consumed} = InputLog.cursors(class)
      assert consumed == %{}
    end
  end

  # A class whose log this test opens, closed when the test ends. The setup's
  # `on_exit` closes the class it minted, and a test that shadows it with one of
  # its own leaves a live, registered `InputLog` behind the module — which every
  # `FsyncBatcher` started afterwards seeds its dirty set from.
  defp opened_class(prefix) do
    class = "#{prefix}-#{System.unique_integer([:positive])}"
    on_exit(fn -> TestHelpers.close_input_log(class) end)
    class
  end

  describe "a sender's message id (the one duplicate identity)" do
    test "a re-sent id is absorbed; a message without one never is", %{class: class} do
      meta = %{"flz.msg_id" => "m-1"}

      assert {:ok, 0} = InputLog.append(class, "hello", meta)
      assert {:duplicate, 0} = InputLog.append(class, "hello", meta)
      # A re-send with the same id but different bytes is still the same message.
      assert {:duplicate, 0} = InputLog.append(class, "hello (edited)", meta)

      # Two identical payloads without an id are two messages; an empty id is none.
      assert {:ok, 1} = InputLog.append(class, "anon", %{})
      assert {:ok, 2} = InputLog.append(class, "anon", %{})
      assert {:ok, 3} = InputLog.append(class, "anon", %{"flz.msg_id" => ""})

      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 1)) ==
               ["hello", "anon", "anon", "anon"]
    end

    test "the index is rebuilt from the file after a restart", %{class: class} do
      {:ok, 0} = InputLog.append(class, "anon", %{})
      {:ok, 1} = InputLog.append(class, "hello", %{"flz.msg_id" => "m-9"})
      stop_log(class)

      assert {:duplicate, 1} = InputLog.append(class, "hello", %{"flz.msg_id" => "m-9"})
      assert {:ok, 2} = InputLog.append(class, "other", %{"flz.msg_id" => "m-10"})
    end

    test "a log whose senders stamp no id builds no index", %{class: class} do
      for i <- 0..2, do: {:ok, ^i} = InputLog.append(class, "m#{i}", %{})
      {:ok, log} = Supervisor.lookup({class, :input_log})

      assert :sys.get_state(log).msg_ids == nil
    end

    test "a truncated id is a new message; a retained one is still absorbed",
         %{class: class} do
      for {id, n} <- [{"a", 0}, {"b", 1}, {"c", 2}],
          do: {:ok, ^n} = InputLog.append(class, id, %{"flz.msg_id" => id})

      InputLog.record_consumed(class, {class, "task", "a"}, 1)
      :ok = Sweep.run(class)
      assert wait_until(fn -> InputLog.read_range(class, 0, 9) |> length() == 2 end, 2_000)

      assert {:ok, 3} = InputLog.append(class, "a", %{"flz.msg_id" => "a"})
      assert {:duplicate, 1} = InputLog.append(class, "b", %{"flz.msg_id" => "b"})
    end
  end

  describe "an append's answer" do
    setup do
      batcher = Process.whereis(FsyncBatcher)
      on_exit(fn -> if Process.alive?(batcher), do: :sys.resume(batcher) end)
      %{batcher: batcher}
    end

    test "waits for the sync that makes its record durable", %{class: class, batcher: batcher} do
      {:ok, 0} = InputLog.append(class, "seed", %{})
      :ok = :sys.suspend(batcher)

      reqids = InputLog.append_async(class, "m1", %{}, :gen_server.reqids_new(), :m1)
      assert held(class).owed == 1
      assert :timeout = :gen_server.wait_response(reqids, 0, true)

      :ok = :sys.resume(batcher)
      assert {{:reply, {:ok, 1}}, :m1, _} = :gen_server.wait_response(reqids, 2_000, true)
    end

    test "a log that stops answers what its last sync made durable",
         %{class: class, batcher: batcher} do
      {:ok, 0} = InputLog.append(class, "seed", %{})
      :ok = :sys.suspend(batcher)

      reqids = InputLog.append_async(class, "m1", %{}, :gen_server.reqids_new(), :m1)
      assert held(class).owed == 1
      assert :timeout = :gen_server.wait_response(reqids, 0, true)

      stop_log(class)
      assert {{:reply, {:ok, 1}}, :m1, _} = :gen_server.wait_response(reqids, 1_000, true)
      :ok = :sys.resume(batcher)
      assert InputLog.read_range(class, 0, 1) |> Enum.map(&elem(&1, 1)) == ["seed", "m1"]
    end
  end

  describe "a kept effect's answer" do
    # Fails when: a log stopped while its companion still holds an effect
    # answer exits first, and the caller gets the exit instead of `:kept`.
    test "reaches the caller when the log stops before the companion gave it",
         %{class: class} do
      {:ok, 0} = InputLog.append(class, "m0", %{})
      {:ok, 1} = InputLog.append(class, "m1", %{})
      # The first effect opens the effect log through the companion.
      :kept = InputLog.keep_effect(class, 0, "id-0", "cell", "r0")
      {:ok, log} = Supervisor.lookup({class, :input_log})
      syncer = :sys.get_state(log).syncer
      true = :erlang.suspend_process(syncer)

      caller = Task.async(fn -> InputLog.keep_effect(class, 1, "id-1", "cell", "r1") end)
      assert wait_until(fn -> length(InputLog.effect_results(class, -1)) == 2 end, 2_000)

      stopper = Task.async(fn -> GenServer.stop(log) end)

      assert wait_until(
               fn ->
                 not Process.alive?(log) or
                   Process.info(log, :current_function) ==
                     {:current_function, {Flezha.State.InputLog.Syncer, :call, 2}}
               end,
               2_000
             )

      true = :erlang.resume_process(syncer)
      assert Task.await(caller) == :kept
      assert Task.await(stopper) == :ok
    end
  end

  describe "the instance's tail" do
    test "is the floor and the last offset; what lies above the floor is owed",
         %{class: class} do
      for i <- 0..3, do: {:ok, ^i} = InputLog.append(class, "m#{i}", %{})
      assert InputLog.tail(class) == {:ok, -1, 3}

      InputLog.record_consumed(class, Paths.journal_id(class), 1)

      assert InputLog.tail(class) == {:ok, 1, 3}
      assert InputLog.undelivered(class) |> Enum.map(&elem(&1, 0)) == [2, 3]
    end

    test "starts no log for a key that has none", %{class: class} do
      assert InputLog.tail(class) == {:ok, -1, -1}
      assert InputLog.undelivered(class) == []
      refute File.exists?(Paths.input_log_path(class))
      assert Supervisor.lookup({class, :input_log}) == :error
    end
  end

  test "a released prefix below the cut threshold is left for a later sweep",
       %{class: class} do
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 65_536))
    for i <- 0..3, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})

    InputLog.record_consumed(class, {class, "task", "a"}, 2)
    :ok = Sweep.run(class)
    {:ok, log} = Supervisor.lookup({class, :input_log})
    # Answered strictly after the log has handled the sweep's cast.
    _ = GenServer.call(log, :last_offset, 1_000)
    assert InputLog.read_range(class, 0, 3) |> length() == 4

    Application.put_env(:flezha, :record_log_cut_after_bytes, 0)
    InputLog.record_consumed(class, {class, "task", "a"}, 3)
    :ok = Sweep.run(class)

    assert wait_until(
             fn -> InputLog.read_range(class, 0, 3) |> Enum.map(&elem(&1, 0)) == [3] end,
             2_000
           )
  end

  defp stop_log(class) do
    {:ok, pid} = Supervisor.lookup({class, :input_log})
    :ok = GenServer.stop(pid)
    # The Registry sweeps dead entries asynchronously; wait so the next
    # find_or_start starts a fresh server instead of racing the dead pid.
    assert wait_until(fn -> Supervisor.lookup({class, :input_log}) == :error end, 2_000)
  end

  # `"m<i>"` for each `i`, all in flight at once: one sync answers the lot.
  defp append_all(class, range) do
    reqids =
      Enum.reduce(range, :gen_server.reqids_new(), fn i, reqids ->
        InputLog.append_async(class, "m#{i}", %{}, reqids, i)
      end)

    for _ <- range do
      assert {{:reply, {:ok, i}}, i, _} = :gen_server.receive_response(reqids, 5_000, true)
    end
  end

  # What the log holds back once it has handled every request sent so far:
  # appends parked for credit, and answers owed to the next sync.
  defp held(class) do
    {:ok, log} = Supervisor.lookup({class, :input_log})
    %{waiters: waiters, owed: owed} = :sys.get_state(log)
    %{parked: :queue.len(waiters), owed: length(owed)}
  end

  # Every record appended so far is older than a 1 ms hold window.
  defp aged_out() do
    appended_at = System.system_time(:millisecond)
    assert wait_until(fn -> System.system_time(:millisecond) > appended_at + 1 end, 1_000, 1)
  end

  describe "a truncation costs the tail, not the log" do
    test "a floor past the index samples keeps exactly the tail", %{class: class} do
      append_all(class, 0..599)
      InputLog.record_consumed(class, {class, "task", "a"}, 400)
      :ok = Sweep.run(class)

      assert InputLog.read_range(class, 0, 599) |> Enum.map(&elem(&1, 0)) ==
               Enum.to_list(400..599)

      # The second cut works against the rebased index, and appends land at the
      # right place afterwards.
      InputLog.record_consumed(class, {class, "task", "a"}, 550)
      :ok = Sweep.run(class)
      {:ok, 600} = InputLog.append(class, "m600", %{})

      assert InputLog.read_range(class, 0, 600) |> Enum.map(&elem(&1, 0)) ==
               Enum.to_list(550..600)
    end

    test "a restarted log truncates the records it never indexed", %{class: class} do
      append_all(class, 0..299)
      {:ok, pid} = Flezha.State.Supervisor.lookup({class, :input_log})
      :ok = GenServer.stop(pid)
      # The registry drops the dead pid a moment after the exit; `append/3`
      # retries a `:noproc` only once, with no delay.
      assert wait_until(
               fn -> Flezha.State.Supervisor.lookup({class, :input_log}) == :error end,
               1_000,
               5
             )

      {:ok, 300} = InputLog.append(class, "m300", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 290)
      :ok = Sweep.run(class)

      assert InputLog.read_range(class, 0, 300) |> Enum.map(&elem(&1, 0)) ==
               Enum.to_list(290..300)
    end
  end

  describe "a corrupt record mid-log" do
    test "refuses the class's log instead of truncating it", %{class: class} do
      for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{"i" => i})
      TestHelpers.close_input_log(class)

      path = Paths.input_log_path(class)
      [_first, second, third] = TestHelpers.record_offsets(path)
      size = File.stat!(path).size
      TestHelpers.corrupt_record_body(path, second)

      handler =
        :telemetry_test.attach_event_handlers(self(), [
          [:flezha, :state, :record_frame, :refused]
        ])

      log =
        capture_log(fn ->
          # The log opens through the same reader the per-Cell WAL opens
          # through, so it must give the same answer to the same record.
          assert {:error, {:crc_mismatch, 0, ^second}} = InputLog.last_offset(class)
        end)

      assert_receive {[:flezha, :state, :record_frame, :refused], ^handler, %{count: 1},
                      %{path: ^path, reason: :crc_mismatch, last_seq: 0, offset: ^second}}

      :telemetry.detach(handler)
      assert log =~ ~r/record_frame_refused .*reason=crc_mismatch/
      assert log =~ path

      # Nothing was cut, and the record after the corrupt one still decodes.
      assert File.stat!(path).size == size
      {:ok, device} = :file.open(path, [:raw, :read, :binary])
      assert {:ok, 2, _header, _body, _} = RecordFrame.decode_record(device, third)
      :file.close(device)
    end

    test "serves neither appends nor reads, and never starts an fsync companion",
         %{class: class} do
      for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      TestHelpers.close_input_log(class)

      path = Paths.input_log_path(class)
      [_first, second, _third] = TestHelpers.record_offsets(path)
      TestHelpers.corrupt_record_body(path, second)

      capture_log(fn ->
        assert {:error, {:crc_mismatch, 0, ^second}} = InputLog.append(class, "m3", %{})
        assert {:error, {:crc_mismatch, 0, ^second}} = InputLog.read_range(class, 0, 2)

        {:ok, pid} = Flezha.State.Supervisor.lookup({class, :input_log})

        # No companion was spawned, so nothing is linked to a log that will
        # never sync — and the `:fsync` call answers instead of parking in
        # `companion_call/2`, which is the wedge a companion-less log could reach.
        assert :sys.get_state(pid).syncer == nil
        assert {:error, {:crc_mismatch, 0, ^second}} = GenServer.call(pid, :fsync, 500)

        # Producing-liveness degrades to WAL-seq-only rather than reporting a
        # tail that is really the prefix before the failure.
        assert InputLog.cursors(class) == nil

        # A watermark advance and a retention sweep are casts: both are dropped
        # rather than reaching a companion that was never started.
        InputLog.record_consumed(class, {class, "task", "a"}, 1)
        :ok = Sweep.run(class)
        assert Process.alive?(pid)
        assert {:error, {:crc_mismatch, 0, ^second}} = GenServer.call(pid, :last_offset)
      end)
    end
  end

  describe "a stored consumed-offset only moves forward" do
    test "a first report for an absent key stores that offset", %{class: class} do
      refute InputLog.watermark(class)

      InputLog.record_consumed(class, {class, "task", "a"}, 7)

      assert InputLog.watermark(class) == 7
    end

    test "two reporters resolve to the higher offset, whichever arrives last", %{class: class} do
      # One class per case: `watermark/1` is the minimum across a relay's
      # Cells, so a shared class would answer for the slowest of the three.
      ascending = class <> "_asc"
      InputLog.record_consumed(ascending, {ascending, "task", "a"}, 3)
      InputLog.record_consumed(ascending, {ascending, "task", "a"}, 5)
      assert InputLog.watermark(ascending) == 5

      # The direction the unconditional insert got wrong.
      descending = class <> "_desc"
      InputLog.record_consumed(descending, {descending, "task", "a"}, 5)
      InputLog.record_consumed(descending, {descending, "task", "a"}, 3)
      assert InputLog.watermark(descending) == 5

      # Reporters are separate processes: the compare and the write are one
      # operation, so neither can read 5 and then write 3 over it.
      concurrent = class <> "_conc"

      [5, 3]
      |> Enum.map(
        &Task.async(fn ->
          InputLog.record_consumed(concurrent, {concurrent, "task", "a"}, &1)
        end)
      )
      |> Task.await_many(2_000)

      assert InputLog.watermark(concurrent) == 5
    end

    test "an explicit rewind lowers it, and the next report advances it again", %{class: class} do
      cell = {class, "task", "a"}
      InputLog.record_consumed(class, cell, 5)

      # The correction rewound the WAL to 2: the records that reported 5 are
      # gone, so the watermark must follow them down.
      InputLog.rewind_consumed(class, cell, 2)
      assert InputLog.watermark(class) == 2

      # The guard is against moving BACKWARDS, not against re-reaching a value
      # once held: the replay re-consumes and reports 5 again.
      InputLog.record_consumed(class, cell, 5)
      assert InputLog.watermark(class) == 5
    end

    test "a report the guard rejects still drains credit and unparks", %{class: class} do
      # Each body encodes to 53 bytes, so a 160-byte window admits exactly
      # three and parks the fourth.
      on_exit(TestHelpers.override_env(:source_credit_window_bytes, 160))
      cell = {class, "task", "a"}
      body = String.duplicate("x", 40)

      # Reported before the log exists: the advance reaches the table and no
      # cast reaches the log, so the log seeds this offset at init having
      # drained nothing for it.
      InputLog.record_consumed(class, cell, 2)

      for i <- 0..2, do: {:ok, ^i} = InputLog.append(class, body, %{})

      # 159 of 160 bytes outstanding: the next append parks.
      reqids = InputLog.append_async(class, body, %{}, :gen_server.reqids_new(), :parked)
      assert held(class).parked == 1
      assert :timeout = :gen_server.wait_response(reqids, 0, true)

      # A stale reporter's offset of 1: the write is rejected, and the report
      # still reaches the log, which drains offsets 0..2 and releases the park.
      InputLog.record_consumed(class, cell, 1)

      assert InputLog.watermark(class) == 2
      assert {{:reply, {:ok, 3}}, :parked, _} = :gen_server.wait_response(reqids, 2_000, true)
    end
  end

  describe "effect results" do
    test "results follow the floor", %{class: class} do
      for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      assert InputLog.keep_effect(class, 4, "id-4", "cell", "four") == :kept
      assert InputLog.keep_effect(class, 7, "id-7", "cell", "seven") == :kept

      InputLog.record_consumed(class, {class, "task", "a"}, 5)
      :ok = Sweep.run(class)

      assert InputLog.effect_results(class, -1) == [{7, "id-7", "cell", "seven"}]
      stop_log(class)

      assert InputLog.effect_results(class, -1) == [{7, "id-7", "cell", "seven"}],
             "the rewritten file holds only what is kept"
    end

    test "the first result for an identity wins", %{class: class} do
      for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      assert InputLog.keep_effect(class, 2, "id", "cell", "first") == :kept
      assert InputLog.keep_effect(class, 2, "id", "cell", "first") == :kept
      assert InputLog.keep_effect(class, 2, "id", "cell", "second") == {:first, "first"}
      assert InputLog.effect_results(class, -1) == [{2, "id", "cell", "first"}]
    end

    test "a restarted log reads its results back and ships only those above a floor",
         %{class: class} do
      for i <- 0..3, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      assert InputLog.keep_effect(class, 1, "b", "cell", <<0, 1>>) == :kept
      assert InputLog.keep_effect(class, 1, "a", "cell", "x") == :kept
      assert InputLog.keep_effect(class, 3, "c", "other", "y") == :kept
      stop_log(class)

      assert InputLog.effect_results(class, -1) == [
               {1, "a", "cell", "x"},
               {1, "b", "cell", <<0, 1>>},
               {3, "c", "other", "y"}
             ]

      assert InputLog.effect_results(class, 1) == [{3, "c", "other", "y"}]
      assert InputLog.keep_effect(class, 3, "c", "other", "z") == {:first, "y"}
    end

    test "a result for an input the log does not hold, or over the cap, is refused",
         %{class: class} do
      {:ok, 0} = InputLog.append(class, "m0", %{})
      assert {:error, _} = InputLog.keep_effect(class, 1, "id", "cell", "beyond the head")
      on_exit(TestHelpers.override_env(:effect_max_bytes, 4))
      stop_log(class)
      assert {:error, _} = InputLog.keep_effect(class, 0, "id", "cell", "too long")
      assert InputLog.effect_results(class, -1) == []
    end

    test "results follow the floor while the inputs wait for the cut threshold", %{class: class} do
      on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 65_536))
      for i <- 0..9, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      assert InputLog.keep_effect(class, 4, "id-4", "cell", "four") == :kept
      assert InputLog.keep_effect(class, 7, "id-7", "cell", "seven") == :kept

      InputLog.record_consumed(class, {class, "task", "a"}, 5)
      :ok = Sweep.run(class)

      assert InputLog.read_range(class, 0, 9) |> length() == 10, "the inputs wait for the cut"
      assert InputLog.effect_results(class, -1) == [{7, "id-7", "cell", "seven"}]
    end

    test "an instance that never keeps one has no effect log", %{class: class} do
      for i <- 0..4, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 3)
      :ok = Sweep.run(class)

      assert InputLog.effect_results(class, -1) == []
      refute File.exists?(Paths.effects_log_path(class))
      assert InputLog.read_range(class, 0, 9) |> Enum.map(&elem(&1, 0)) == [3, 4]
    end
  end

  describe "the fsync companion's death" do
    test "no fsync tick is sent to a companion that has exited", %{class: class} do
      {log, syncer} = live_log(class)
      died = attach_syncer_died()
      :erlang.trace(log, true, [:send])

      capture_log(fn ->
        Process.exit(syncer, :kill)
        # Once the companion is gone its `EXIT` is in the log's queue, so this
        # call is answered strictly after the log has handled it — or not at all.
        assert wait_until(fn -> not Process.alive?(syncer) end, 1_000)
        settle(log)

        # No synthetic casts: every append marks the class dirty and the shared
        # `FsyncBatcher`'s own tick casts `:fsync` at whichever log is
        # registered, which is the whole durability path this log has.
        for i <- 1..10 do
          await_live_log(class)
          {:ok, _} = InputLog.append(class, "m#{i}", %{})
          await_fsync_tick(class)
        end
      end)

      if Process.alive?(log), do: :erlang.trace(log, false, [:send])

      assert dead_syncs() == 0
      assert_receive {^died, %{relay: ^class, reason: :killed}}, 500
      assert Process.alive?(:sys.get_state(await_live_log(class)).syncer)
      assert InputLog.last_offset(class) == 10
    end

    test "a :fsync call is answered rather than parking the log for ever", %{class: class} do
      {log, syncer} = live_log(class)

      on_exit(fn ->
        # A parked log never reaches a system message, so the shared teardown's
        # polite stop would not return.
        if Process.alive?(log), do: Process.exit(log, :kill)
      end)

      capture_log(fn ->
        # Suspended, the call queues ahead of the companion's `EXIT`, so the log
        # reaches `:fsync` with a dead companion and no `EXIT` seen yet — the
        # order the shutdown flush arrives in.
        :ok = :sys.suspend(log)
        reqid = :gen_server.send_request(log, :fsync)
        assert wait_until(fn -> queued?(log) end, 1_000)
        Process.exit(syncer, :kill)
        :ok = :sys.resume(log)

        assert :gen_server.receive_response(reqid, 300) == {:reply, :error},
               "the log never answered :fsync: " <>
                 inspect(Process.info(log, [:status, :current_stacktrace]))

        await_live_log(class)
        assert InputLog.last_offset(class) == 0
      end)
    end
  end

  # A live log for `class` with its companion running and nothing owed to the
  # shared batcher, so a later tick can only come from an append made after it.
  defp live_log(class) do
    {:ok, 0} = InputLog.append(class, "seed", %{})
    log = await_live_log(class)
    await_fsync_tick(class)
    settle(log)
    syncer = :sys.get_state(log).syncer
    assert Process.alive?(syncer)
    {log, syncer}
  end

  # The log has handled everything queued ahead of this call, or has stopped.
  defp settle(log) do
    GenServer.call(log, :last_offset, 1_000)
  catch
    :exit, _ -> :stopped
  end

  # A log for `class` that answers. Alive is not enough: one inside `terminate/2`
  # is alive and takes no more requests, and an append handed to it exits.
  defp await_live_log(class) do
    assert pid = wait_until(fn -> responding_log(class) end, 2_000)
    pid
  end

  defp responding_log(class) do
    with {:ok, pid} <- Supervisor.lookup({class, :input_log}),
         true <- Process.alive?(pid),
         offset when is_integer(offset) <- settle(pid) do
      pid
    else
      _ -> nil
    end
  end

  # The shared batcher has drained: its tick cast `:fsync` at the class's log.
  defp await_fsync_tick(class) do
    assert wait_until(
             fn -> not MapSet.member?(FsyncBatcher.dirty_set(), {class, :input_log}) end,
             2_000
           )
  end

  defp queued?(log) do
    match?({:message_queue_len, n} when n > 0, Process.info(log, :message_queue_len))
  end

  # Every `:sync` the traced log sent to a pid that had already exited.
  defp dead_syncs() do
    ref = :erlang.trace_delivered(:all)
    assert_receive {:trace_delivered, :all, ^ref}, 1_000

    dead =
      collect after: 0 do
        {:trace, _pid, :send_to_non_existing_process, {:sync, _}, _to} -> true
        {:trace, _pid, _event, _message, _to} -> false
      end

    Enum.count(dead, & &1)
  end

  defp attach_syncer_died() do
    ref = make_ref()

    Telemetry.capture([:flezha, :state, :input_log_syncer_died],
      as: fn _event, _measurements, metadata -> {ref, metadata} end
    )

    ref
  end
end
