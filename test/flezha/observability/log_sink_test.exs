defmodule Flezha.Observability.LogSinkTest do
  # async: false: the observability registry and the global log level.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.Observability.{Bus, LogSink, Registry, Signal}
  alias FlezhaTest.Helper.NodeTestHelper

  @sink :log_sink_test

  setup do
    on_exit(NodeTestHelper.override_log_level(:info))
    on_exit(fn -> Registry.unregister_sink(@sink) end)
    :ok
  end

  test "registered for a source, it writes one info line per signal it accepts" do
    {:ok, pid} =
      Registry.register_sink(%{
        name: @sink,
        module: LogSink,
        filter: %{kinds: :any, sources: [:beam_vm]}
      })

    log =
      capture_log([level: :info], fn ->
        emit(:beam_vm, :memory_total, 4_096)
        emit(:log_sink_test_other, :memory_total, 8_192)
        :sys.get_state(pid)
      end)

    assert [_, _] = String.split(log, "[beam_vm] memory_total value=4096")
    assert log =~ "[info]"
    refute log =~ "8192"
  end

  test "not registered, it writes nothing" do
    assert Registry.lookup_sink(:log) == :error

    log = capture_log([level: :info], fn -> emit(:beam_vm, :memory_total, 4_096) end)

    refute log =~ "[beam_vm] memory_total"
  end

  defp emit(source, name, value),
    do:
      Bus.emit(
        Signal.new(kind: :metric, source: source, name: name, measurements: %{value: value})
      )
end
