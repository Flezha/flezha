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

  test "operator counters advance and surface in snapshot" do
    operator = "operator_#{System.unique_integer([:positive])}"
    ref = LivenessCounters.register_operator(operator)

    LivenessCounters.bump_operator(ref, :io_in_received)
    LivenessCounters.bump_operator(ref, :forwarded)
    LivenessCounters.bump_operator(ref, :filtered_nil)
    LivenessCounters.bump_operator(ref, :io_in_received)

    row = find_operator(operator)
    assert row.io_in_received == 2
    assert row.forwarded == 1
    assert row.filtered_nil == 1
  end

  test "nil ref is a no-op (parked/unregistered callers)" do
    assert LivenessCounters.bump_channel(nil, :io_in_received) == :ok
    assert LivenessCounters.set_channel(nil, :last_send_ms, 1) == :ok
    assert LivenessCounters.bump_operator(nil, :forwarded) == :ok
  end

  test "snapshot prunes entries whose owner has died" do
    operator = "operator_dead_#{System.unique_integer([:positive])}"

    parent = self()

    {:ok, pid} =
      Task.start(fn ->
        LivenessCounters.register_operator(operator)
        send(parent, :registered)

        receive do
          :go -> :ok
        end
      end)

    assert_receive :registered, 1_000
    assert find_operator(operator) != nil

    ref = Process.monitor(pid)
    send(pid, :go)
    assert_receive {:DOWN, ^ref, :process, ^pid, _}, 1_000

    # First snapshot prunes the dead owner's entry.
    LivenessCounters.snapshot_all()
    assert find_operator(operator) == nil
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

  # The two stamps are written by different channels of the same runner: the
  # control channel writes `last_control_frame_ms` and the data channels write
  # `last_send_ms`, so every collapse of a live runner mixes a real monotonic
  # reading with the 0 of a channel that never wrote that slot. A stamp is the
  # latest reading among the channels that took one, and 0 is absent rather
  # than a value, so both halves have to hold at once.
  test "runner_liveness/3 reads a stamp one channel wrote, and only a stamp nobody wrote is nil" do
    stamped = "exec_stamped_#{System.unique_integer([:positive])}"
    silent = "exec_silent_#{System.unique_integer([:positive])}"

    control = LivenessCounters.register_channel(stamped, :control)
    LivenessCounters.register_channel(stamped, :secondary_1)
    data = LivenessCounters.register_channel(stamped, :secondary_2)

    LivenessCounters.register_channel(silent, :control)
    LivenessCounters.register_channel(silent, :secondary_1)

    now = System.monotonic_time(:millisecond)
    LivenessCounters.set_channel(control, :last_control_frame_ms, now - 50)
    LivenessCounters.set_channel(data, :last_send_ms, now - 10)

    rows =
      Enum.filter(LivenessCounters.snapshot_all().channels, &(&1.runner_id in [stamped, silent]))

    liveness = LivenessCounters.runner_liveness(rows, nil, now)

    assert liveness[stamped].control_silent_ms == 50
    assert liveness[stamped].data_silent_ms == 10

    assert liveness[silent].control_silent_ms == nil
    assert liveness[silent].data_silent_ms == nil
  end

  defp find_channel(exec, name) do
    LivenessCounters.snapshot_all().channels
    |> Enum.find(fn c -> c.runner_id == exec and c.name == name end)
  end

  defp find_operator(operator) do
    LivenessCounters.snapshot_all().operators
    |> Enum.find(fn r -> r.operator_id == operator end)
  end

  # The declaration is the seam, so the test that keeps it honest asks the
  # table about itself: a row that reaches nothing, a metric nobody emits, a
  # heartbeat key with no slot and a layout that outgrows the source's
  # cardinality budget are all failures here rather than silences in
  # production.
  describe "the declaration is complete" do
    test "every slot declares the surface that reads it" do
      undeclared =
        for row <- LivenessCounters.channel_rows() ++ LivenessCounters.operator_rows(),
            Keyword.get(row, :surface, []) == [],
            do: Keyword.fetch!(row, :name)

      assert undeclared == [],
             "liveness slots reaching no surface: #{inspect(undeclared)} — a slot must name " <>
               "a metric, a verdict, a bridged event, or an internal reason in words"
    end

    test "every declared surface is one the readers understand" do
      for row <- LivenessCounters.channel_rows() ++ LivenessCounters.operator_rows(),
          surface <- Keyword.fetch!(row, :surface) do
        assert match?({kind, _} when kind in [:metric, :verdict, :telemetry, :internal], surface),
               "#{inspect(Keyword.fetch!(row, :name))} declares #{inspect(surface)}, which no " <>
                 "reader knows how to consume"
      end
    end

    test "every heartbeat key holds a slot, and the mirror is that list" do
      pairs = Map.new(LivenessCounters.heartbeat_pairs())

      for row <- LivenessCounters.channel_rows(),
          key = Keyword.get(row, :heartbeat),
          key != nil do
        assert Map.fetch!(pairs, key) == Keyword.fetch!(row, :name)
        assert key in LivenessCounters.heartbeat_keys()
      end

      assert Enum.sort(LivenessCounters.heartbeat_keys()) ==
               Enum.sort(Flezha.Runner.StreamChannel.heartbeat_keys()),
             "the mirror keeps a key list of its own again"
    end

    # Every `{:verdict, _}` row names a reader that must actually exist: the
    # classifier or the probe row that consumes it. A verdict nothing reads is
    # the same silence as a metric nobody emits.
    test "every declared verdict surface has a reader in the tree" do
      readers =
        File.read!("lib/flezha/convergence/stall_classifier.ex") <>
          File.read!("lib/flezha/convergence/probe/table.ex") <>
          File.read!("lib/flezha/convergence/probe/readers.ex")

      for {_group, row, verdict} <- LivenessCounters.surfaces(:verdict) do
        assert readers =~ Atom.to_string(verdict),
               "#{inspect(Keyword.fetch!(row, :name))} declares the verdict " <>
                 "#{inspect(verdict)}, which no convergence reader names"
      end
    end

    # The source declares `cardinality_budget: 40`; the rows must fit inside it
    # with the driver stats and the connection count that share the source, so
    # a layout that would blow the budget fails here rather than at runtime.
    test "the derived metric rows stay inside the source's declared budget" do
      budget = Flezha.Observability.OtpBackend.TransportSource.cardinality_budget()

      assert length(LivenessCounters.surfaces(:metric)) <= budget,
             "the layout declares #{length(LivenessCounters.surfaces(:metric))} metric rows " <>
               "against a budget of #{budget}"
    end
  end
end
