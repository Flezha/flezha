defmodule Flezha.Executor.ControlChannelSendControlTest do
  @moduledoc """
  Fails when: `ControlChannel.send_control/2` swallows transport failures and
  always returns `{:ok, ref}`, so a relay talking to a reset/dead stream
  registers a reply ref that never resolves, then blocks and crashes instead
  of receiving a clean error and healing.
  """
  use ExUnit.Case, async: false

  alias Flezha.Executor.ControlChannel

  test "send_control surfaces a transport error when no stream is bound" do
    write_fun = fn _ref, _payload -> :ok end

    {:ok, pid} =
      ControlChannel.start_link({"ns_sc", "exec_sc", nil, write_fun})

    command = {:ping, %ExecutorCommands.Ping{nonce: "n1"}}

    assert {:error, :no_connection_available} =
             ControlChannel.send_control("exec_sc", command)

    GenServer.stop(pid)
  end
end
