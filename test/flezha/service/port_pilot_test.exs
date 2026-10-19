defmodule Flezha.Service.PortPilotTest do
  @moduledoc """
  A runner the controller starts is heard in the controller's log, a line at a
  time, naming the runner, at the level the line's own prefix names.
  """
  # async: false: a second pilot over the configured processes; the primary Logger level.
  use FlezhaTest.Case, fixtures: [:tmp_dir], async: false

  alias Flezha.Service.PortPilot
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    on_exit(NodeTestHelper.override_log_level(:info))
  end

  # Every entry a pilot over `command` logged until it handled `last_line`.
  # The child prints its lines and then waits on its standard input, which the
  # pilot's exit closes.
  defp pilot_logging(command, last_line) do
    restore = TestHelpers.override_env(:external_processes, [command])

    {_, log} =
      ExUnit.CaptureLog.with_log(fn ->
        {:ok, pilot} = GenServer.start(PortPilot, nil)
        restore.()
        :erlang.trace(pilot, true, [:receive])
        assert_receive {:trace, ^pilot, :receive, {_port, {:data, {:eol, ^last_line}}}}, 30_000
        _handled = :sys.get_state(pilot)
        GenServer.stop(pilot)
      end)

    String.split(log, ~r/^(?=\d\d:\d\d:\d\d\.\d{3} )/m, trim: true)
  end

  test "a line longer than the line limit and a line written in two writes are each logged whole",
       %{tmp: dir} do
    long = String.duplicate("x", 5_000)
    script = Path.join(dir, "child.sh")

    File.write!(script, """
    printf '%s\\n' #{long}
    printf '[flezha] warning: split '
    sleep 0.1
    printf 'line\\n'
    echo '[flezha] error: on stderr' >&2
    read _
    """)

    command = "sh #{script}"

    runner = "[runner #{command}]"

    assert [
             "[info] #{runner} <the long line>",
             "[warning] #{runner} [flezha] warning: split line",
             "[error] #{runner} [flezha] error: on stderr"
           ] ==
             command
             |> pilot_logging("[flezha] error: on stderr")
             |> Enum.map(fn entry ->
               entry
               |> String.replace(~r/^\S+ (\S+=\S+ )*/, "")
               |> String.replace(long, "<the long line>")
               |> String.trim_trailing()
             end)
  end
end
