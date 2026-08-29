defmodule FlezhaTest.Runner.ControlUnitHealthTest do
  @moduledoc """
  ControlUnit stuck-caller unblocking. A control reply left unacked past the
  threshold would otherwise hang the caller: the ControlUnit fails the waiter
  with `:timeout` and drops the ref so `response_refs` cannot grow unbounded.

  Runner liveness/eviction is NOT this sweep's job — it is the
  `ControlChannel` watchdog's (runner-controller-connection-healing, D1). The
  old `synthesize_disconnect` eviction role was removed from this heartbeat.
  """
  # async: false: the primary Logger; registered runner channels.
  use ExUnit.Case, async: false

  # The stuck-caller sweep logs at warn by design; capture so a green run stays
  # quiet (logs still print on failure).
  @moduletag :capture_log

  alias Flezha.Runner.ControlUnit

  setup do
    on_exit(FlezhaTest.Helper.NodeTestHelper.override_log_level(:warning))
  end

  test "heartbeat unblocks the stuck caller when a control reply is unacked past the threshold" do
    now = System.monotonic_time(:millisecond)
    reply_ref = make_ref()

    state = %{
      namespace: "default",
      runner_id: "ex_dead",
      response_refs: %{
        make_ref() => {:reply, {self(), reply_ref}, now - 120_000, false}
      }
    }

    assert {:noreply, %{response_refs: refs}} = ControlUnit.handle_info(:heartbeat, state)

    # The stuck ref is dropped and its caller is unblocked with a clean error
    # (so a blocking caller never hangs forever on the dead runner).
    assert refs == %{}
    assert_receive {^reply_ref, {:error, :timeout}}
  end

  test "heartbeat leaves a fresh reply ref alone (no false eviction)" do
    now = System.monotonic_time(:millisecond)
    ref = make_ref()

    state = %{
      namespace: "default",
      runner_id: "ex_ok",
      response_refs: %{
        ref => {:reply, {self(), make_ref()}, now - 1_000, false}
      }
    }

    assert {:noreply, %{response_refs: refs}} = ControlUnit.handle_info(:heartbeat, state)

    assert Map.has_key?(refs, ref)
    refute_received {_ref, {:error, :timeout}}
  end
end
