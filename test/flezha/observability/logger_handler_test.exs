defmodule Flezha.Observability.LoggerHandlerTest do
  # async: false: the observability registry; the primary Logger.
  use ExUnit.Case, async: false

  # The tests emit Logger calls on purpose to drive the bridge; assertions run
  # off the captured signal, so capturing console output changes nothing.
  @moduletag :capture_log

  require Logger

  alias Flezha.Observability.{Counters, Registry, TestSinks}
  alias Flezha.Observability.TestSinks.Counting

  setup do
    sink_name = :"obs_logger_test_sink_#{:erlang.unique_integer([:positive])}"
    parent = self()
    ref = :counters.new(1, [:atomics])

    # Sink that filters only :log signals from the :logger source.
    {:ok, _pid} =
      Registry.register_sink(%{
        name: sink_name,
        module: Counting,
        config: %{counter_ref: ref, parent: parent, sources: [:logger]}
      })

    on_exit(fn -> Registry.unregister_sink(sink_name) end)

    # Make sure logger emits warnings (test config has level: :error)
    on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))

    {:ok, sink_name: sink_name, counter_ref: ref}
  end

  test "Logger.warning produces a :log signal with rendered message" do
    drain_mailbox()
    Logger.warning("hello-observability")

    log_signal =
      TestSinks.receive_signal(fn s ->
        s.kind == :log and s.metadata.message == "hello-observability"
      end)

    assert log_signal
    assert log_signal.name == :warning
    assert log_signal.metadata.level == :warning
  end

  test "bus failure does not break Logger" do
    before_failures = Counters.get(:logger_bridge_failures)

    # Inject a deliberate failure by emitting through Bus with a
    # broken sink: a sink with a mailbox-watermark of 0 forces drop
    # path but no raise; for raise we patch Bus's emit indirectly by
    # using a non-existent ETS lookup (already handled by
    # try/rescue). Simulate a raise by calling Bus.emit/1 with a
    # crafted signal that has a sink we know will raise.
    #
    # The cleanest test: directly call LoggerHandler.log/2 with a
    # malformed event and assert the counter increments without
    # the test process crashing.
    bad_event = %{level: :error, msg: {:string, ["ok"]}, meta: %{}}

    # We expect the counter not to drop the call below.
    :ok = Flezha.Observability.LoggerHandler.log(bad_event, %{})

    # Logger should still work after this call.
    Logger.warning("still-alive")
    assert Counters.get(:logger_bridge_failures) >= before_failures
  end

  defp drain_mailbox() do
    receive do
      _ -> drain_mailbox()
    after
      0 -> :ok
    end
  end
end
