defmodule FlezhaTest.Operative.OverwatchSegmentStateGateTest do
  @moduledoc """
  §5b.2: `:get_segment_state` is gated on `persistent_state?` (kind+class).
  `:unbounded` and a durable-by-default `:bounded` class proceed to query the
  executor exactly as before; a `:bounded` class that opted out of persistence —
  or any stateless kind — is rejected with
  `{:error, {:unsupported_for_kind, :persistent_state}}` and never contacts the
  executor (the handler replies synchronously instead of spawning the send).
  """
  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch

  setup do
    on_exit(fn -> Application.delete_env(:flezha, :operative_persistence) end)
    :ok
  end

  defp state_for(kind, id \\ "r_gate") do
    %Overwatch{
      id: id,
      namespace: "ns",
      # An executor id whose control unit is not registered, so the proceeding
      # path's spawned send resolves to a clean error rather than a crash.
      executor_id: "no_such_executor_#{System.unique_integer([:positive])}",
      builder: %Builder{id: id, factory_id: "f", kind: kind}
    }
  end

  test "an :unbounded kind proceeds to query the executor" do
    state = state_for(:unbounded)

    # {:noreply, state} == the gate passed and the handler went on to contact
    # the executor off-loop (replies later via GenServer.reply), as it does today.
    assert {:noreply, ^state} =
             Overwatch.handle_call(:get_segment_state, {self(), make_ref()}, state)
  end

  test "a durable :bounded kind proceeds to query the executor" do
    state = state_for(:bounded)

    assert {:noreply, ^state} =
             Overwatch.handle_call(:get_segment_state, {self(), make_ref()}, state)
  end

  test "an opted-out :bounded class is rejected and does not contact the executor" do
    Application.put_env(:flezha, :operative_persistence, %{"opted_out" => false})
    state = state_for(:bounded, "opted_out")

    # A synchronous {:reply, error, state} (no spawn) proves the executor was not
    # contacted for state.
    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call(:get_segment_state, {self(), make_ref()}, state)
  end

  test "a stateless kind is rejected and does not contact the executor" do
    state = state_for(:made_up_kind)

    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call(:get_segment_state, {self(), make_ref()}, state)
  end
end
