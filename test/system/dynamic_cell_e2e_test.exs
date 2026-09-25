defmodule FlezhaTest.System.DynamicCellE2ETest do
  @moduledoc """
  End-to-end proof for dynamic cell mode against a live runner of the
  selected SDK runtime: an input for a fresh key lazily creates a Cell whose state is
  shipped (controller writes a WAL file) at the identity
  `{operator_id, tasklet, key}`; after the 200 ms TTL the reaper evicts it,
  ships `delete_cell_state`, and the controller drops the files — the SDK
  reaper → delete-RPC → agent-handler path with no other end-to-end coverage.

  The key is declared on the WIRING (`.key_by` / `.keyBy`), so this is also
  what pins `stream-transforms`' promise that an equivalent topology reaches
  byte-identical Cell identities on disk: the assertions below are unchanged
  from when the key was a Tasklet trait hook.

  Tagged `:system` (boots a real subprocess). Run with
  `mix test --include system test/system/dynamic_cell_e2e_test.exs`.
  """

  use FlezhaTest.SystemCase, capability: :dynamic_cell

  import FlezhaTest.Helper.NodeTestHelper, only: [wait_until: 2]

  alias Flezha.Operator.Builder
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Paths
  alias FlezhaTest.Helper.NodeTestHelper

  @namespace "dynamic_cell_e2e"
  @factory "dynamic_cell"
  @tasklet "dynamic_cell_tasklet"

  # A fresh fleet per test: the two scenarios start their runner under
  # different environments (one with a 200 ms idle TTL).
  setup do
    on_exit(fn ->
      try do
        OperatorManager.stop_all_operators(@namespace)
      catch
        _, _ -> :ok
      end

      try do
        FleetManager.stop_namespace(@namespace)
      catch
        _, _ -> :ok
      end
    end)

    :ok
  end

  # The WAL assertion is reference-only: the Java SDK does not register a
  # lazily-created Cell with its flusher, so a dynamic Cell's state never
  # reaches disk there. That gap predates edge keying; the live-state read
  # below is what every SDK is held to.
  @tag reference_runtime: true
  test "lazy-created Cell's state is shipped, then reaped + deleted controller-side" do
    run_dynamic_cell_case()
  end

  test "a live dynamic Cell is readable by name, narrowed to one key" do
    run_live_read_case()
  end

  # `flz state get`'s round-trip against a real runner: a lazily-created Cell
  # is readable by name, the reply carries the seq it was serialized at, and the
  # selector really narrows — the other live key is not serialized. No TTL is
  # armed here, so nothing races the read.
  defp run_live_read_case do
    # Own namespace: placement is HRW over every runner active in the
    # namespace, so a peer case's runner must not be a candidate.
    namespace = "dyn_live_read"

    {port, ce_id} = NodeTestHelper.start_runner_port(namespace)

    on_exit(fn ->
      NodeTestHelper.ensure_port_dead(port)

      try do
        OperatorManager.stop_all_operators(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    operator_id = "dyn_read_e2e_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"
    other = "#{key}b"

    builder = %Builder{id: operator_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperatorManager.start_operator(namespace, builder)
    assert info.runner_id == ce_id
    OperatorManager.send_io(operator_id, namespace, "#{key}:hello", %{"input_type" => "2"})
    OperatorManager.send_io(operator_id, namespace, "#{other}:bye", %{"input_type" => "2"})

    assert wait_until(fn -> cell_keys(namespace, operator_id) == [key, other] end, 15_000),
           "both keys should be live before the read is narrowed"

    assert {:ok, json, seq} =
             OperatorManager.get_cell_state(namespace, operator_id, %{
               tasklet: @tasklet,
               cell_key: key
             })

    assert Payloader.json_to_map(json) == %{
             @tasklet => %{key => %{"seen" => ["#{key}:hello"]}}
           },
           "the read must return exactly the Cell asked for, holding what run() just applied"

    assert is_integer(seq), "a single-Cell read reports the change_seq it was serialized at"

    assert {:error, refusal} =
             OperatorManager.get_cell_state(namespace, operator_id, %{
               tasklet: @tasklet,
               cell_key: "never-seen"
             })

    assert refusal =~ "cell_not_found"
  end

  # The Java arm asserts against LIVE Cell state rather than the WAL: the
  # Java SDK does not register a lazily-created Cell with its flusher, so a
  # dynamic Cell's state never reaches disk there. That gap predates edge
  # keying (it was reached identically through the deleted `cellKey` hook)
  # and closing it is a Java durability change, not an authoring-surface one.
  defp cell_keys(namespace, operator_id) do
    with {:ok, json} <- OperatorManager.get_cell_state(namespace, operator_id),
         %{@tasklet => by_key} when is_map(by_key) <-
           Payloader.json_to_map(json) do
      by_key |> Map.keys() |> Enum.sort()
    else
      _ -> nil
    end
  end

  defp run_dynamic_cell_case do
    # 200 ms idle TTL so the operator's reaper evicts + ships
    # delete_cell_state within the test (default is no eviction).
    {port, ce_id} =
      NodeTestHelper.start_runner_port(@namespace, env: [{"DYN_TEST_TTL_MS", "200"}])

    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operator_id = "dyn_e2e_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"
    cell_id = {operator_id, @tasklet, key}

    builder = %Builder{id: operator_id, factory_id: @factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperatorManager.start_operator(@namespace, builder)
    assert info.runner_id == ce_id
    # An input for a never-seen key: the edge's key function names the Cell,
    # the SDK lazily creates it, runs it, and ships the Change. The controller
    # writes a WAL file — at the identity the trait hook names.
    OperatorManager.send_io(operator_id, @namespace, "#{key}:hello", %{"input_type" => "2"})

    assert wait_until(fn -> File.exists?(Paths.wal_path(cell_id)) end, 15_000),
           "controller never wrote a WAL file for the lazily-created Cell"

    # A filtered input reaches no Cell at all — there is no keyless-drop path
    # to fall through any more.
    OperatorManager.send_io(operator_id, @namespace, "drop", %{"input_type" => "2"})

    # No further input for the live key: after the 200 ms idle TTL the reaper
    # evicts the Cell and ships delete_cell_state; the controller removes
    # the file. Generous budget for sweep cadence + wire + teardown.
    assert wait_until(fn -> not File.exists?(Paths.wal_path(cell_id)) end, 15_000),
           "reaper/delete-RPC never removed the evicted Cell's WAL file"

    assert Paths.snapshot_paths(cell_id) == []
  end
end
