defmodule FlezhaTest.System.StateFreshnessE2ETest do
  @moduledoc """
  End-to-end SIGTERM verification for `ship-fresh-state-on-sdk-shutdown`.

  Spins up a real Rust compute-engine subprocess, registers a processor
  with `snapshot_interval: 0` (so the agent has no periodic capture and
  the only path to populate `Overwatch.state.snapshot` is the SDK's
  shutdown notification), then sends SIGTERM to the OS pid and asserts
  the agent's snapshot is populated within the 2 s ack window.

  Tagged `:system` so it stays out of the default `mix test` budget —
  it boots `cargo run` and a real subprocess, which can be slow on cold
  caches. Run via:

      mix test --only state_freshness_e2e
      mix test --include system test/system/state_freshness_e2e_test.exs

  The complementary unit-level coverage lives in
  `test/processor/overwatch_external_snapshot_test.exs` and
  `test/flezha/compute_engine/control_channel_shutdown_notification_test.exs`.
  """

  use ExUnit.Case, async: false

  alias Flezha.ComputeEngine.FleetManager
  alias Flezha.Processor.Builder
  alias Flezha.Service.EventManager
  alias Flezha.Service.PortPilot
  alias Flezha.Service.ProcessorManager

  @moduletag :system
  @namespace "state_freshness_e2e"
  @factory "store_input"

  setup do
    on_exit(fn ->
      try do
        ProcessorManager.stop_all_processors(@namespace)
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

  @tag :state_freshness_e2e
  test "SIGTERM to the rust SDK populates Overwatch.state.snapshot via OperatorShutdownNotification" do
    {port, ce_id} = start_compute_engine()
    on_exit(fn -> ensure_port_dead(port) end)

    {:os_pid, os_pid} = :erlang.port_info(port, :os_pid)

    processor_id = "freshness_e2e_#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: processor_id,
      factory_id: @factory,
      init_values: %{},
      # Disabled-snapshot — flezha_market-shape. The only path for
      # `state.snapshot` to become non-nil is the SDK's
      # `OperatorShutdownNotification`. If the notification path were
      # broken, the assertion at the bottom would still see nil and
      # fail loudly.
      opts: [snapshot_interval: 0]
    }

    assert {:ok, info} = ProcessorManager.start_processor(@namespace, builder)
    assert info.compute_engine_id == ce_id

    {:ok, overwatch_pid} = ProcessorManager.get_processor_pid(@namespace, processor_id)
    pre_state = :sys.get_state(overwatch_pid)

    assert pre_state.snapshot == nil,
           "precondition: snapshot must be nil before SIGTERM (snapshot_interval=0)"

    # Drive a few inputs so the SDK's per-processor state has something
    # distinguishable from the empty initial state. The `store_input`
    # factory appends each text input to `state.text_inputs`; the SDK's
    # graceful-shutdown serializer then ships JSON containing those
    # entries. Without this step the snapshot would only carry the
    # empty-initial-state JSON, which is true but uninteresting — it
    # would not catch a regression where the notification fires before
    # processing in-flight inputs.
    inputs_sent = ["alpha-#{processor_id}", "beta-#{processor_id}", "gamma-#{processor_id}"]
    Enum.each(inputs_sent, fn line ->
      ProcessorManager.send_io(processor_id, @namespace, line, %{"input_type" => "2"})
    end)

    # Inputs travel via primary stream and the processor's `run` runs
    # async in the SDK's worker pool. Settle briefly so the state has
    # been mutated before we trigger SIGTERM.
    Process.sleep(200)

    {_, 0} =
      System.cmd("kill", ["-TERM", Integer.to_string(os_pid)],
        stderr_to_stdout: true
      )

    snapshot = wait_for_snapshot(overwatch_pid, 5_000)

    assert is_binary(snapshot), "agent's Overwatch.state.snapshot was never populated"
    assert byte_size(snapshot) > 0

    # The SDK's `store_input` factory serializes its state as JSON
    # `{"binary_inputs":[...],"text_inputs":[...]}`; assert the shape
    # AND that our inputs landed in `text_inputs`. A regression that
    # ships some other binary (e.g. an error frame, or the empty
    # initial state) would fail one of these.
    decoded = Jason.decode!(snapshot)
    assert is_map(decoded)
    assert Map.has_key?(decoded, "text_inputs")
    assert Map.has_key?(decoded, "binary_inputs")

    received_inputs = Map.fetch!(decoded, "text_inputs")
    assert is_list(received_inputs)

    # The snapshot SHALL reflect the state at SIGTERM time — i.e. the
    # inputs we sent must be present (in some order; processor input
    # ordering is sticky per processor, so they appear in send order).
    assert Enum.sort(received_inputs) == Enum.sort(inputs_sent),
           "graceful-shutdown snapshot did not include all sent inputs.\n" <>
             "  sent:     #{inspect(inputs_sent)}\n" <>
             "  received: #{inspect(received_inputs)}"
  end

  defp start_compute_engine() do
    {:ok, current_path} = :file.get_cwd()

    command =
      "cargo run --quiet --manifest-path=#{current_path}/test/apps/rust_compute_engine/Cargo.toml " <>
        "--target-dir=./target -- " <> @namespace

    EventManager.add_handler(:compute_engine_connected_event)
    {port, _command} = PortPilot.open_port(command, transport: :uds)

    ce_id =
      receive do
        {:compute_engine_connected_event, {@namespace, ce_id}} -> ce_id
      after
        # Cold cargo cache warmup can blow past the usual 2 s — match the
        # existing healing test's 120 s budget for the first run.
        120_000 -> raise "compute engine never connected"
      end

    EventManager.rem_handler(:compute_engine_connected_event)
    {port, ce_id}
  end

  defp wait_for_snapshot(overwatch_pid, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_snapshot(overwatch_pid, deadline)
  end

  defp do_wait_for_snapshot(overwatch_pid, deadline) do
    cond do
      not Process.alive?(overwatch_pid) ->
        nil

      System.monotonic_time(:millisecond) >= deadline ->
        nil

      true ->
        case :sys.get_state(overwatch_pid).snapshot do
          nil ->
            Process.sleep(50)
            do_wait_for_snapshot(overwatch_pid, deadline)

          snap ->
            snap
        end
    end
  end

  defp ensure_port_dead(port) do
    if is_port(port) and Enum.member?(:erlang.ports(), port) do
      try do
        Port.close(port)
      catch
        _, _ -> :ok
      end
    end
  end
end
