defmodule FlezhaTest.System.StateFreshnessE2ETest do
  @moduledoc """
  End-to-end SIGTERM verification for `ship-fresh-state-on-sdk-shutdown`.

  Spins up a real runner subprocess for the run's selected runtime, registers
  a relay (the controller takes no periodic snapshot, so the only path
  to populate `Overwatch.state.snapshot` is the SDK's shutdown notification), then sends SIGTERM to the OS pid and asserts
  the controller's snapshot is populated within the 2 s ack window.

  Tagged `:system` so it stays out of the default `mix test` budget —
  it boots a real SDK subprocess, which can be slow on cold caches. Run via:

      mix test test/system/state_freshness_e2e_test.exs
      mix test --include system test/system/state_freshness_e2e_test.exs

  The complementary unit-level coverage lives in
  `test/relay/overwatch_external_snapshot_test.exs` and
  `test/flezha/runner/control_channel_shutdown_notification_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :state_persistence

  alias Flezha.Relay.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "state_freshness_e2e"
  @factory "store_input"

  # One scenario, whose runner is SIGTERMed by the test itself.
  setup do
    on_exit(fn ->
      try do
        RelayManager.stop_all_relays(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  test "SIGTERM to the runner populates Overwatch.state.snapshot via RunnerShutdownNotification" do
    {port, ce_id} = NodeTestHelper.start_runner_port(@namespace, transport: :uds)
    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    {:os_pid, os_pid} = :erlang.port_info(port, :os_pid)

    relay_id = "freshness_e2e_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: relay_id,
      factory_id: @factory,
      # The only path for `state.snapshot` to become non-nil is the SDK's
      # `RunnerShutdownNotification`. If the notification path were broken,
      # the assertion at the bottom would still see nil and fail loudly.
      init_values: %{}
    }

    assert {:ok, info} = RelayManager.start_relay(@namespace, builder)
    assert info.runner_id == ce_id

    {:ok, overwatch_pid} = RelayManager.get_relay_pid(@namespace, relay_id)
    pre_state = :sys.get_state(overwatch_pid)

    assert pre_state.snapshot == nil,
           "precondition: snapshot must be nil before SIGTERM"

    # Drive a few inputs so the SDK's per-relay state has something
    # distinguishable from the empty initial state. The `store_input`
    # factory appends each text input to `state.text_inputs`; the SDK's
    # graceful-shutdown serializer then ships JSON containing those
    # entries. Without this step the snapshot would only carry the
    # empty-initial-state JSON, which is true but uninteresting — it
    # would not catch a regression where the notification fires before
    # processing in-flight inputs.
    inputs_sent = ["alpha-#{relay_id}", "beta-#{relay_id}", "gamma-#{relay_id}"]

    Enum.each(inputs_sent, fn line ->
      RelayManager.send_io(relay_id, @namespace, line, %{"input_type" => "2"})
    end)

    # Inputs travel via primary stream and the relay's `run` runs
    # async in the SDK's worker pool: the live state read shows them applied
    # before SIGTERM is sent.
    assert NodeTestHelper.wait_until(
             fn ->
               case RelayManager.get_cell_state(@namespace, relay_id) do
                 {:ok, json} -> Enum.all?(inputs_sent, &String.contains?(json, &1))
                 _ -> false
               end
             end,
             15_000
           ),
           "the inputs never reached the relay's state"

    {_, 0} =
      System.cmd("kill", ["-TERM", Integer.to_string(os_pid)], stderr_to_stdout: true)

    snapshot = wait_for_snapshot(overwatch_pid, 5_000)

    assert is_binary(snapshot), "controller's Overwatch.state.snapshot was never populated"
    assert byte_size(snapshot) > 0

    # The SDK's `store_input` factory serializes its state as JSON
    # `{"binary_inputs":[...],"text_inputs":[...]}`; assert the shape
    # AND that our inputs landed in `text_inputs`. A regression that
    # ships some other binary (e.g. an error frame, or the empty
    # initial state) would fail one of these.
    decoded = Jason.decode!(snapshot)
    assert is_map(decoded)
    # Relay state serializes as `%{tasklet_name => tasklet_state}`;
    # this single-tasklet relay has exactly one entry.
    assert map_size(decoded) == 1
    inner = decoded |> Map.values() |> hd()
    assert Map.has_key?(inner, "text_inputs")
    assert Map.has_key?(inner, "binary_inputs")

    received_inputs = Map.fetch!(inner, "text_inputs")
    assert is_list(received_inputs)

    # The snapshot SHALL reflect the state at SIGTERM time — i.e. the
    # inputs we sent must be present (in some order; relay input
    # ordering is sticky per relay, so they appear in send order).
    assert Enum.sort(received_inputs) == Enum.sort(inputs_sent),
           "graceful-shutdown snapshot did not include all sent inputs.\n" <>
             "  sent:     #{inspect(inputs_sent)}\n" <>
             "  received: #{inspect(received_inputs)}"
  end

  defp wait_for_snapshot(overwatch_pid, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_snapshot(overwatch_pid, deadline)
  end

  defp do_wait_for_snapshot(overwatch_pid, deadline) do
    NodeTestHelper.wait_until(
      fn -> Process.alive?(overwatch_pid) and :sys.get_state(overwatch_pid).snapshot end,
      max(deadline - System.monotonic_time(:millisecond), 0)
    ) || nil
  end
end
