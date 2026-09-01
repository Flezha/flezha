defmodule FlezhaTest.Plugin.Provider.MonitoredIOForwardTest do
  @moduledoc """
  `init_provider/1` converts its options before it links a process. The
  heartbeat interval reaches `Process.send_after/3` inside the child's
  `init/1`, and a linked child's crash is not something the caller's own
  `try`/`catch` can turn into a refusal — only a caller that traps exits
  survives it at all. So the conversion has to happen first.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.MonitoredIOForward

  defp opts(extra) do
    Map.merge(
      %{recv_pid: self(), plugin_id: "mio-test", operative_id: "op", namespace: "ns"},
      extra
    )
  end

  test "a heartbeat interval it cannot interpret is refused before anything is linked" do
    Process.flag(:trap_exit, true)

    assert {:error, message} =
             MonitoredIOForward.init_provider(opts(%{heartbeat_interval: "abc"}))

    assert is_binary(message)
    assert message =~ "heartbeat_interval"
    refute_received {:EXIT, _pid, _reason}
  end

  test "the interval is accepted as an integer and as its CLI string" do
    Process.flag(:trap_exit, true)

    for {value, expected} <- [{250, 250}, {"250", 250}] do
      assert {:ok, prepared, pid} =
               MonitoredIOForward.init_provider(opts(%{heartbeat_interval: value}))

      assert prepared.heartbeat_interval == expected
      assert is_pid(pid)
      Process.exit(pid, :shutdown)
    end
  end
end
