defmodule Flezha.Transport.LivenessCountersTest do
  @moduledoc """
  Unit coverage for `Flezha.Transport.LivenessCounters`: register/bump/set,
  the read-side snapshot, and dead-owner pruning.
  """

  # async: false: node-global tables.

  use ExUnit.Case, async: false

  alias Flezha.Transport.LivenessCounters

  test "channel counters advance under bump/set and surface in snapshot" do
    exec = "exec_#{System.unique_integer([:positive])}"
    name = :"chan_#{System.unique_integer([:positive])}"
    ref = LivenessCounters.register_channel(exec, name)

    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :send_frame_calls)
    LivenessCounters.bump_channel(ref, :nil_drops)
    LivenessCounters.bump_channel(ref, :flz_data_received)
    LivenessCounters.set_channel(ref, :last_send_ms, 1234)

    row = find_channel(exec, name)
    assert row.io_in_received == 2
    assert row.send_frame_calls == 1
    assert row.nil_drops == 1
    assert row.flz_data_received == 1
    assert row.last_send_ms == 1234
  end

  test "operative counters advance and surface in snapshot" do
    operative = "operative_#{System.unique_integer([:positive])}"
    ref = LivenessCounters.register_operative(operative)

    LivenessCounters.bump_operative(ref, :io_in_received)
    LivenessCounters.bump_operative(ref, :forwarded)
    LivenessCounters.bump_operative(ref, :filtered_nil)
    LivenessCounters.bump_operative(ref, :io_in_received)

    row = find_operative(operative)
    assert row.io_in_received == 2
    assert row.forwarded == 1
    assert row.filtered_nil == 1
  end

  test "nil ref is a no-op (parked/unregistered callers)" do
    assert LivenessCounters.bump_channel(nil, :io_in_received) == :ok
    assert LivenessCounters.set_channel(nil, :last_send_ms, 1) == :ok
    assert LivenessCounters.bump_operative(nil, :forwarded) == :ok
  end

  test "snapshot prunes entries whose owner has died" do
    operative = "operative_dead_#{System.unique_integer([:positive])}"

    parent = self()

    {:ok, pid} =
      Task.start(fn ->
        LivenessCounters.register_operative(operative)
        send(parent, :registered)

        receive do
          :go -> :ok
        end
      end)

    assert_receive :registered, 1_000
    assert find_operative(operative) != nil

    ref = Process.monitor(pid)
    send(pid, :go)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

    # First snapshot prunes the dead owner's entry.
    LivenessCounters.snapshot_all()
    assert find_operative(operative) == nil
  end

  # The collapse is owned by the slot layout so the two readers cannot
  # disagree: a mirrored heartbeat slot is one fact about the runner and keeps
  # its value across the runner's channels; a frame count or a credit session
  # is one fact about the channel and sums; the send stamp keeps the latest.
  test "by_runner/1 keeps a heartbeat slot once per runner and sums the per-channel ones" do
    exec = "exec_#{System.unique_integer([:positive])}"
    a = LivenessCounters.register_channel(exec, :secondary_1)
    b = LivenessCounters.register_channel(exec, :secondary_2)
    mirrored = for key <- Flezha.Runner.StreamChannel.heartbeat_keys(), do: :"hb_#{key}"

    for ref <- [a, b] do
      for slot <- mirrored, do: LivenessCounters.set_channel(ref, slot, 5)
      LivenessCounters.bump_channel(ref, :hb_count)
      LivenessCounters.bump_channel(ref, :send_frame_calls)
      LivenessCounters.bump_channel(ref, :io_in_received)
      LivenessCounters.set_channel(ref, :credit_active, 1)
    end

    # One stream took a later heartbeat than the other.
    for slot <- mirrored, do: LivenessCounters.set_channel(b, slot, 6)
    LivenessCounters.set_channel(a, :last_send_ms, 100)
    LivenessCounters.set_channel(b, :last_send_ms, 200)

    rows = Enum.filter(LivenessCounters.snapshot_all().channels, &(&1.runner_id == exec))
    assert %{^exec => row} = LivenessCounters.by_runner(rows)

    for slot <- mirrored, do: assert({slot, row[slot]} == {slot, 6})
    assert row.hb_count == 2
    assert row.send_frame_calls == 2
    assert row.io_in_received == 2
    assert row.credit_active == 2
    assert row.last_send_ms == 200
  end

  test "by_runner/1 keeps runners apart and reads an SDK without the self-report as zero" do
    exec_a = "exec_a_#{System.unique_integer([:positive])}"
    exec_b = "exec_b_#{System.unique_integer([:positive])}"
    a = LivenessCounters.register_channel(exec_a, :secondary_1)
    _b = LivenessCounters.register_channel(exec_b, :secondary_1)
    LivenessCounters.set_channel(a, :hb_timers_fired, 9)

    rows =
      Enum.filter(LivenessCounters.snapshot_all().channels, &(&1.runner_id in [exec_a, exec_b]))

    by_runner = LivenessCounters.by_runner(rows)

    assert Map.keys(by_runner) |> Enum.sort() == Enum.sort([exec_a, exec_b])
    assert by_runner[exec_a].hb_timers_fired == 9
    assert by_runner[exec_b].hb_timers_fired == 0
  end

  defp find_channel(exec, name) do
    LivenessCounters.snapshot_all().channels
    |> Enum.find(fn c -> c.runner_id == exec and c.name == name end)
  end

  defp find_operative(operative) do
    LivenessCounters.snapshot_all().operatives
    |> Enum.find(fn r -> r.operative_id == operative end)
  end
end
