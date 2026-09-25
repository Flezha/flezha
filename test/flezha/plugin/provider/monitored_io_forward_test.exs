defmodule FlezhaTest.Plugin.Provider.MonitoredIOForwardTest do
  @moduledoc """
  The heartbeat interval is a declared option, converted before anything is
  started.

  It reaches `Process.send_after/3` inside the worker's `init/1`, so a value the
  declaration did not convert would reach a live process rather than a refusal.
  Preparation now starts nothing at all, which is the stronger form of the same
  guarantee: a value that cannot be interpreted is refused with no process in
  existence to crash.
  """

  use ExUnit.Case, async: true

  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  defp opts(extra) do
    Map.merge(
      %{recv_pid: self(), plugin_id: "mio-test", operator_id: "op", namespace: "ns"},
      extra
    )
  end

  defp prepare(extra) do
    with {:ok, opts} <- ProviderOptions.normalise_opts(MonitoredIOForward, opts(extra)),
         do: MonitoredIOForward.init_provider(opts)
  end

  test "a heartbeat interval it cannot interpret is refused, and nothing is linked" do
    Process.flag(:trap_exit, true)

    assert {:error, message} = prepare(%{heartbeat_interval: "abc"})
    assert is_binary(message)
    assert message =~ "heartbeat_interval"
    refute_received {:EXIT, _pid, _reason}
  end

  test "the interval is accepted as an integer and as its CLI string" do
    for {value, expected} <- [{250, 250}, {"250", 250}] do
      assert {:ok, prepared} = prepare(%{heartbeat_interval: value})
      assert prepared.heartbeat_interval == expected
    end
  end

  test "preparation holds no process, and the declared worker is what runs" do
    assert {:ok, prepared} = prepare(%{})
    assert prepared.heartbeat_interval == 5000

    assert %{module: MonitoredIOForward, restart: :temporary} =
             MonitoredIOForward.__provider__().worker

    assert [{"mio-test", _worker_opts}] = MonitoredIOForward.worker_specs(prepared)
  end

  test "each member of a partition group holds a heartbeat of its own" do
    assert {:ok, prepared} = prepare(%{})

    keys =
      for index <- [0, 1] do
        [{key, _opts}] =
          MonitoredIOForward.worker_specs(Map.put(prepared, :partition_index, index))

        key
      end

    assert keys == ["mio-test#p0", "mio-test#p1"]
  end
end
