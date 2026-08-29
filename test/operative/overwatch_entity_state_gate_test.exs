defmodule FlezhaTest.Operative.OverwatchEntityStateGateTest do
  @moduledoc """
  §5b.2: `:get_entity_state` is gated on `persistent_state?` (kind+class).
  `:stream` and a durable-by-default `:workflow` class proceed to query the
  runner exactly as before; a `:workflow` class that opted out of persistence —
  or any stateless kind — is rejected with
  `{:error, {:unsupported_for_kind, :persistent_state}}` and never contacts the
  runner (the handler replies synchronously instead of spawning the send).
  """
  # async: false: writes Application env.
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
      # A runner id whose control unit is not registered, so the proceeding
      # path's spawned send resolves to a clean error rather than a crash.
      runner_id: "no_such_runner_#{System.unique_integer([:positive])}",
      builder: %Builder{id: id, factory_id: "f", kind: kind}
    }
  end

  test "an :stream kind proceeds to query the runner" do
    state = state_for(:stream)

    # {:noreply, state} == the gate passed and the handler went on to contact
    # the runner off-loop (replies later via GenServer.reply), as it does today.
    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end

  test "a durable :workflow kind proceeds to query the runner" do
    state = state_for(:workflow)

    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end

  test "an opted-out :workflow class is rejected and does not contact the runner" do
    Application.put_env(:flezha, :operative_persistence, %{"opted_out" => false})
    state = state_for(:workflow, "opted_out")

    # A synchronous {:reply, error, state} (no spawn) proves the runner was not
    # contacted for state.
    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end

  test "a stateless kind is rejected and does not contact the runner" do
    state = state_for(:made_up_kind)

    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end
end
