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

  defp find_channel(exec, name) do
    LivenessCounters.snapshot_all().channels
    |> Enum.find(fn c -> c.runner_id == exec and c.name == name end)
  end

  defp find_operative(operative) do
    LivenessCounters.snapshot_all().operatives
    |> Enum.find(fn r -> r.operative_id == operative end)
  end
end
