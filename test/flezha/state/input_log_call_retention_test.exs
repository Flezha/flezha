defmodule Flezha.State.InputLogCallRetentionTest do
  @moduledoc """
  `durable-input-log`: a record carrying a live call (a request in its
  callee's log, a `Response` in its caller's) is kept until its
  `flz.deadline_at` plus `call_retention_margin_ms` has passed, whatever the
  watermark allows; then it goes with the watermark like any consumed record,
  and a log whose records carry no deadline truncates as it always did.
  """
  # async: false: writes Application env.
  use FlezhaTest.StateCase, async: false

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.State.{InputLog, Supervisor, TestHelpers}
  alias Flezha.State.InputLog.Sweep

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

  defp call_record(id, deadline_at),
    do: %{"flz.msg_id" => id, "flz.deadline_at" => Integer.to_string(deadline_at)}

  for {what, id} <- [{"a consumed request", "c1"}, {"a Response", "r:c1"}] do
    test "#{what} is kept until its call is over, then goes with the watermark", %{class: class} do
      # A deadline already reached: the margin alone decides, so the test moves
      # the margin rather than waiting on the clock.
      deadline_at = System.system_time(:millisecond) - 1
      {:ok, 0} = InputLog.append(class, "before", %{})
      {:ok, 1} = InputLog.append(class, "call", call_record(unquote(id), deadline_at))
      {:ok, 2} = InputLog.append(class, "after", %{})
      InputLog.record_consumed(class, {class, "task", "a"}, 3)

      # The read is answered by the log after any cut the sweep cast to it.
      :ok = Sweep.run(class)
      assert offsets(class) == [1, 2]
      assert {:duplicate, 1} = InputLog.append(class, "again", %{"flz.msg_id" => unquote(id)})

      # Deadline plus margin gone: the next sweep takes it like any record.
      on_exit(TestHelpers.override_env(:call_retention_margin_ms, 0))
      :ok = Sweep.run(class)
      assert offsets(class) == []
    end
  end

  test "a log without deadlines truncates as before", %{class: class} do
    for i <- 0..3, do: {:ok, _} = InputLog.append(class, "m#{i}", %{"flz.msg_id" => "m#{i}"})
    InputLog.record_consumed(class, {class, "task", "a"}, 2)
    :ok = Sweep.run(class)
    assert wait_until(fn -> offsets(class) == [2, 3] end, 2_000)
  end

  test "the deadlines are learned again from the file after a restart", %{class: class} do
    now = System.system_time(:millisecond)
    {:ok, 0} = InputLog.append(class, "call", call_record("c1", now + 60_000))
    {:ok, 1} = InputLog.append(class, "after", %{})
    {:ok, log} = Supervisor.lookup({class, :input_log})
    :ok = GenServer.stop(log)
    assert wait_until(fn -> Supervisor.lookup({class, :input_log}) == :error end, 2_000)

    InputLog.record_consumed(class, {class, "task", "a"}, 2)
    # A fresh log that has seen no append since learns its live call first.
    {:ok, _log} = Supervisor.find_or_start({class, :input_log})
    assert InputLog.call_floor(class) == 0
    # The read is answered by the log after any cut the sweep cast to it.
    :ok = Sweep.run(class)
    assert offsets(class) == [0, 1]
  end
end
