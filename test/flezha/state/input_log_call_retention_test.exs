defmodule Flezha.State.InputLogCallRetentionTest do
  @moduledoc """
  `durable-input-log`: a record carrying a live call (a request in its
  callee's log, a `Response` in its caller's) is kept until its
  `flz.deadline_at` plus `call_retention_margin_ms` has passed, and one
  carrying a message id until its append time plus that margin, whatever the
  watermark allows; then it goes with the watermark like any consumed record,
  and a log whose records carry neither truncates as it always did.
  """
  # async: false: writes Application env.
  use FlezhaTest.StateCase, async: false

  use Mimic

  import ExUnit.CaptureLog
  import FlezhaTest.Helper.NodeTestHelper, only: [override_log_level: 1, wait_until: 2]

  alias Flezha.State.{InputLog, Paths, RecordFrame, Supervisor, TestHelpers}
  alias Flezha.State.InputLog.{MsgIndex, Sweep}

  setup %{base: base} do
    class = "ilc_" <> uniq()
    on_exit(TestHelpers.override_env(:record_log_cut_after_bytes, 0))
    on_exit(TestHelpers.override_env(:call_retention_margin_ms, :timer.minutes(10)))

    on_exit(fn ->
      TestHelpers.close_input_log(class)
      File.rm_rf!(base)
    end)

    %{class: class}
  end

  defp offsets(class), do: class |> InputLog.read_range(0, 99) |> Enum.map(&elem(&1, 0))

  defp now, do: System.system_time(:millisecond)

  defp call_record(id, deadline_at),
    do: %{"flz.msg_id" => id, "flz.deadline_at" => Integer.to_string(deadline_at)}

  # A call's deadline already reached: the margin alone decides, so the test
  # moves the margin rather than waiting on the clock.
  defp record(id, :call), do: call_record(id, now() - 1)
  defp record(id, :input), do: %{"flz.msg_id" => id}

  for {what, id, kind} <- [
        {"a consumed request", "c1", :call},
        {"a Response", "r:c1", :call},
        {"a consumed input with an id", "m1", :input}
      ] do
    test "#{what} is kept for the margin, then goes with the watermark", %{class: class} do
      {:ok, 0} = InputLog.append(class, "before", %{})
      {:ok, 1} = InputLog.append(class, "held", record(unquote(id), unquote(kind)))
      {:ok, 2} = InputLog.append(class, "after", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 3)

      # The read is answered by the log after any cut the sweep cast to it.
      :ok = Sweep.run(class)
      assert offsets(class) == [1, 2]
      assert {:duplicate, 1} = InputLog.append(class, "again", %{"flz.msg_id" => unquote(id)})

      # The margin gone: the next sweep takes it like any record, and its id is
      # a new message again.
      on_exit(TestHelpers.override_env(:call_retention_margin_ms, 0))
      :ok = Sweep.run(class)
      assert offsets(class) == []
      assert {:ok, 3} = InputLog.append(class, "again", %{"flz.msg_id" => unquote(id)})
    end
  end

  test "a log holds its ids within the margin, and none at margin 0", %{class: class} do
    {:ok, 0} = InputLog.append(class, "sent", %{"flz.msg_id" => "m0"})
    {:ok, 1} = InputLog.append(class, "sent", %{"flz.msg_id" => "m1"})
    {:ok, 2} = InputLog.append(class, "after", %{})
    InputLog.record_consumed(class, {class, "task", "a"}, 2)

    # The walk stops at the first record, younger than the margin: all held.
    :ok = Sweep.run(class)
    assert offsets(class) == [0, 1, 2]
    assert {:duplicate, 0} = InputLog.append(class, "sent", %{"flz.msg_id" => "m0"})

    # Every record aged out: the walk stops at the floor, and both ids go.
    on_exit(TestHelpers.override_env(:call_retention_margin_ms, 0))
    :ok = Sweep.run(class)
    assert offsets(class) == [2]
    assert {:ok, 3} = InputLog.append(class, "sent", %{"flz.msg_id" => "m0"})
  end

  test "a log whose walk is refused holds every id below the floor", %{class: class} do
    on_exit(TestHelpers.override_env(:call_retention_margin_ms, 0))
    {:ok, 0} = InputLog.append(class, "sent", %{"flz.msg_id" => "m0"})
    first_end = RecordFrame.file_size(Paths.input_log_path(class))
    {:ok, 1} = InputLog.append(class, "next", %{})
    {:ok, 2} = InputLog.append(class, "last", %{})
    # The middle record's first body byte (the term format's 131, past its
    # 12-byte frame and 16-byte header) zeroed, with a complete record behind it.
    {:ok, file} = :file.open(Paths.input_log_path(class), [:raw, :read, :write, :binary])
    :ok = :file.pwrite(file, first_end + 28, <<0>>)
    :ok = :file.close(file)

    refused = capture_log(fn -> assert InputLog.held_floor(class, 3) == 0 end)
    assert refused =~ "record_frame_refused"
    assert refused =~ "reason=crc_mismatch"
  end

  test "a log that does not answer the sweep's question is not cut", %{class: class} do
    for i <- 0..2, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    InputLog.record_consumed(class, {class, "task", "a"}, 3)

    # The sweep finds the live log; its question then reaches a log gone
    # since, and is refused at once.
    gone = spawn(fn -> :ok end)
    ref = Process.monitor(gone)
    assert_receive {:DOWN, ^ref, :process, ^gone, _}
    expect(Supervisor, :lookup, &Mimic.call_original(Supervisor, :lookup, [&1]))
    expect(Supervisor, :lookup, fn {^class, :input_log} -> {:ok, gone} end)

    # The refusal is a warning, below the suite's level.
    on_exit(override_log_level(:warning))
    refused = capture_log(fn -> assert :ok = Sweep.run(class) end)
    assert refused =~ "request=:held_floor reason={:noproc"
    assert offsets(class) == [0, 1, 2]
  end

  test "a log without ids or deadlines truncates as before", %{class: class} do
    for i <- 0..3, do: {:ok, _} = InputLog.append(class, "m#{i}", %{})
    InputLog.record_consumed(class, {class, "task", "a"}, 2)
    :ok = Sweep.run(class)
    assert wait_until(fn -> offsets(class) == [2, 3] end, 2_000)
  end

  test "the deadlines are learned again from the file after a restart", %{class: class} do
    # No margin: only the call's deadline holds its record, not its id.
    on_exit(TestHelpers.override_env(:call_retention_margin_ms, 0))
    now = System.system_time(:millisecond)
    {:ok, 0} = InputLog.append(class, "call", call_record("c1", now + 60_000))
    {:ok, 1} = InputLog.append(class, "after", %{})
    {:ok, log} = Supervisor.lookup({class, :input_log})
    :ok = GenServer.stop(log)
    assert wait_until(fn -> Supervisor.lookup({class, :input_log}) == :error end, 2_000)

    InputLog.record_consumed(class, {class, "task", "a"}, 2)
    # A fresh log that has seen no append since learns its live call first.
    {:ok, _log} = Supervisor.find_or_start({class, :input_log})
    assert InputLog.held_floor(class, 2) == 0
    # The read is answered by the log after any cut the sweep cast to it.
    :ok = Sweep.run(class)
    assert offsets(class) == [0, 1]
  end

  describe "the id hold, judged by MsgIndex.floor/3" do
    test "only ids below the floor are judged, walked from the lowest of them" do
      parent = self()

      aged_out = fn _before, from ->
        send(parent, {:walked_from, from})
        4
      end

      ids = index([{"a", 2}, {"b", 4}, {"c", 6}, {"d", 9}])
      # 2 and 4 aged out, 6 is recent, 9 is at or above the floor.
      assert {6, _index} = MsgIndex.floor(ids, 8, aged_out)
      assert_received {:walked_from, 2}
    end

    test "with no id below the floor nothing is walked" do
      assert {nil, _index} =
               MsgIndex.floor(index([{"a", 5}]), 5, fn _, _ -> flunk("walked") end)
    end

    test "nothing aged, or a refused walk, holds every id below the floor" do
      for answer <- [nil, {:error, :refused}],
          do:
            assert({2, _} = MsgIndex.floor(index([{"a", 2}, {"b", 3}]), 8, fn _, _ -> answer end))
    end

    test "the walk is asked for what was appended by the margin's end" do
      parent = self()
      before = now()
      MsgIndex.floor(index([{"a", 2}]), 8, fn time, _ -> send(parent, {:time, time}) && nil end)
      margin = :timer.minutes(10)
      assert_received {:time, time}
      assert time in (before - margin + 1)..(now() - margin + 1)
    end
  end

  defp index(ids),
    do: Enum.reduce(ids, %MsgIndex{}, fn {id, at}, acc -> MsgIndex.note(acc, id, at) end)
end
