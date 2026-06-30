defmodule FlezhaTest.Executor.ExecutorReconnectBindingTest do
  @moduledoc """
  Reconnect binding continuity (unify-executor-liveness-and-identity, Phase 2).

  An executor that restarts on the same persistence path re-presents the same
  durable STABLE IDENTITY. The controller keys operatives on the per-connection
  session token, so on reconnect it SWAPS the prior session's bindings onto the
  new session token (no mint-and-evict, no lost operative) — exactly the
  recurring-outage fix. This drives a real Rust executor through the proven
  stop → restart pattern (matrix 18.7) with a fixed `FLEZHA_EXECUTOR_ID_PATH`,
  and asserts the operative follows the executor onto its new session.
  """
  use ExUnit.Case, async: false

  @moduletag :capture_log

  alias Flezha.Executor.FleetManager
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "reconnect-binding-test"
  @factory "do_nothing"

  setup do
    id_path =
      Path.join(System.tmp_dir!(), "flezha-stable-id-#{System.unique_integer([:positive])}")

    on_exit(fn ->
      FleetManager.stop_namespace(@namespace)
      File.rm_rf(id_path)
    end)

    {:ok, id_path: id_path}
  end

  test "an operative survives an executor restart via the stable-id binding swap", %{
    id_path: id_path
  } do
    env = [{"FLEZHA_EXECUTOR_ID_PATH", id_path}]

    # First boot: the SDK derives + persists a stable id; the operative binds to
    # this connection's session token (session1).
    session1 = NodeTestHelper.start_executor(@namespace, env: env)
    assert session1 in FleetManager.active_executors(@namespace)

    operative = %Flezha.Operative.Builder{id: "reconnect_binding_op", factory_id: @factory}
    {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert bound_to?(session1, operative.id)

    # Restart on the SAME persistence path → the fresh process re-presents the
    # SAME stable id. The controller mints session2 and swaps the operative's
    # binding onto it rather than minting-and-evicting.
    NodeTestHelper.stop_executor(@namespace, session1)

    session2 = NodeTestHelper.start_executor(@namespace, env: env)
    assert session2 != session1, "restart must mint a fresh session token"
    assert session2 in FleetManager.active_executors(@namespace)

    # The operative now belongs to the new session, and the old session is gone.
    assert eventually(fn -> bound_to?(session2, operative.id) end),
           "operative must follow the executor onto its new session token"

    refute session1 in FleetManager.active_executors(@namespace)
  end

  defp bound_to?(session_token, operative_id) do
    HubManager.executor_operatives(@namespace, session_token)
    |> Enum.any?(fn {oid, _node_pids} -> oid == operative_id end)
  end

  defp eventually(fun, attempts \\ 40) do
    cond do
      fun.() ->
        true

      attempts <= 0 ->
        false

      true ->
        Process.sleep(25)
        eventually(fun, attempts - 1)
    end
  end
end
