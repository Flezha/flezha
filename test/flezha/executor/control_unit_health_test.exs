defmodule FlezhaTest.Executor.ControlUnitHealthTest do
  @moduledoc """
  ControlUnit out-of-band recovery. A control reply left unacked past the evict
  threshold is a positive dead-executor signal: the ControlUnit fails the
  waiter, drops the ref, and synthesizes the FleetManager disconnect that evicts
  the executor and rebinds its operatives — never routing through the (possibly
  wedged) Overwatch operative process.

  Fail surfaces as: market data stops and never recovers because a silent
  executor's operative is never rebound onto a live executor.
  """
  use ExUnit.Case, async: false

  # Asserts dead-executor / eviction paths that log at warn/error by design;
  # capture so a green run stays quiet (logs still print on failure).
  @moduletag :capture_log

  alias Flezha.Executor.ControlUnit

  setup do
    prev = Logger.level()
    Logger.configure(level: :warning)
    on_exit(fn -> Logger.configure(level: prev) end)
  end

  test "heartbeat evicts the executor when a control reply is unacked past the threshold" do
    now = System.monotonic_time(:millisecond)
    reply_ref = make_ref()

    state = %{
      namespace: "default",
      executor_id: "ex_dead",
      response_refs: %{
        make_ref() => {:reply, {self(), reply_ref}, now - 120_000}
      }
    }

    assert {:noreply, %{response_refs: refs}} = ControlUnit.handle_info(:heartbeat, state)

    # The stuck ref is dropped and its caller is unblocked with a clean error
    # (so a blocking caller never hangs forever on the dead executor).
    assert refs == %{}
    assert_receive {^reply_ref, {:error, :timeout}}
  end

  test "heartbeat leaves a fresh reply ref alone (no false eviction)" do
    now = System.monotonic_time(:millisecond)
    ref = make_ref()

    state = %{
      namespace: "default",
      executor_id: "ex_ok",
      response_refs: %{
        ref => {:reply, {self(), make_ref()}, now - 1_000}
      }
    }

    assert {:noreply, %{response_refs: refs}} = ControlUnit.handle_info(:heartbeat, state)

    assert Map.has_key?(refs, ref)
    refute_receive {_ref, {:error, :timeout}}, 50
  end
end
