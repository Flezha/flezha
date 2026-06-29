defmodule FlezhaTest.Operative.OverwatchSegmentStateGateTest do
  @moduledoc """
  2.1/3.1/3.2: `:get_segment_state` is gated on the operative kind's
  `persistent_state?` capability. A stateful kind (`:continuous`) proceeds to query
  the executor exactly as before; a synthetic stateless kind is rejected with
  `{:error, {:unsupported_for_kind, :persistent_state}}` and never contacts the
  executor (the handler replies synchronously instead of spawning the send).
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch

  defp state_for(kind) do
    %Overwatch{
      id: "r_gate",
      namespace: "ns",
      # An executor id whose control unit is not registered, so the :continuous
      # path's spawned send resolves to a clean error rather than a crash.
      executor_id: "no_such_executor_#{System.unique_integer([:positive])}",
      builder: %Builder{id: "r_gate", factory_id: "f", kind: kind}
    }
  end

  test "a :continuous (stateful) kind proceeds to query the executor" do
    state = state_for(:continuous)

    # {:noreply, state} == the gate passed and the handler went on to contact
    # the executor off-loop (replies later via GenServer.reply), as it does today.
    assert {:noreply, ^state} =
             Overwatch.handle_call(:get_segment_state, {self(), make_ref()}, state)
  end

  test "a stateless kind is rejected and does not contact the executor" do
    state = state_for(:bounded)

    # A synchronous {:reply, error, state} (no spawn) proves the executor was not
    # contacted for state.
    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call(:get_segment_state, {self(), make_ref()}, state)
  end
end
