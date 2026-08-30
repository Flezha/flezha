defmodule Flezha.Runner.ControlChannelSendControlTest do
  @moduledoc """
  Fails when: `ControlChannel.send_control/2` swallows transport failures and
  always returns `{:ok, ref}`, so a operative talking to a reset/dead stream
  registers a reply ref that never resolves, then blocks and crashes instead
  of receiving a clean error and healing.

  Also fails when a channel that dies or stalls *mid-call* takes its caller down
  with it: the `ControlUnit` is the caller, and an exiting unit loses every
  waiter in its `response_refs` and is reported `shut down abnormally`.
  """
  # async: false: registered runner channels.
  use ExUnit.Case, async: false

  alias Flezha.Runner.ControlChannel

  test "send_control surfaces a transport error when no stream is bound" do
    write_fun = fn _ref, _payload_id, _message, _metadata -> :ok end

    {:ok, pid} =
      ControlChannel.start_link({"ns_sc", "exec_sc", nil, write_fun})

    command = {:ping, %RunnerCommands.Ping{nonce: "n1"}}

    assert {:error, :no_connection_available} =
             ControlChannel.send_control("exec_sc", command)

    GenServer.stop(pid)
  end

  test "a channel that dies mid-call answers its caller instead of exiting it" do
    # The teardown race of KI-48: the supervisor's reverse-order sweep stops the
    # channel while a ControlUnit is still blocked in this call.
    Process.flag(:trap_exit, true)

    {:ok, pid} = start_stuck_channel("exec_sc_die")

    caller =
      Task.async(fn ->
        ControlChannel.send_control("exec_sc_die", {:ping, %RunnerCommands.Ping{nonce: "n2"}})
      end)

    assert_receive :writing, 1_000
    Process.exit(pid, :kill)

    # `Task.await` returning a value at all is the survival proof: an exiting
    # caller propagates its exit here instead of answering.
    assert {:error, {:control_channel_exit, {:killed, {GenServer, :call, _}}}} =
             Task.await(caller, 1_000)
  end

  test "a channel that does not reply in time answers :timeout instead of exiting its caller" do
    Process.flag(:trap_exit, true)

    {:ok, pid} = start_stuck_channel("exec_sc_slow")

    assert {:error, :timeout} =
             ControlChannel.send_control(
               "exec_sc_slow",
               {:ping, %RunnerCommands.Ping{nonce: "n3"}},
               50
             )

    # The caller reached this line, so it survived the timeout; the channel is
    # still alive and merely slow, which is the case that used to kill the unit.
    assert_received :writing
    assert Process.alive?(pid)

    Process.exit(pid, :kill)
  end

  # A channel whose write never returns, so the caller stays blocked in
  # `GenServer.call` until the test decides how the call ends.
  defp start_stuck_channel(runner_id) do
    test_pid = self()

    write_fun = fn _ref, _payload_id, _message, _metadata ->
      send(test_pid, :writing)
      Process.sleep(:infinity)
    end

    ControlChannel.start_link({"ns_" <> runner_id, runner_id, :stuck_stream, write_fun})
  end
end
