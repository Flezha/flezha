defmodule FlezhaTest.System.DynamicEntityE2ETest do
  @moduledoc """
  End-to-end proof for dynamic entity mode against a live runner, on BOTH
  runtimes: an input for a fresh key lazily creates an Entity whose state is
  shipped (controller writes a WAL file) at the identity
  `{operative_id, procedure, key}`; after the 200 ms TTL the reaper evicts it,
  ships `delete_entity_state`, and the controller drops the files — the SDK
  reaper → delete-RPC → agent-handler path with no other end-to-end coverage.

  The key is declared on the WIRING (`.key_by` / `.keyBy`), so this is also
  what pins `stream-transforms`' promise that an equivalent topology reaches
  byte-identical Entity identities on disk: the assertions below are unchanged
  from when the key was a Procedure trait hook.

  Tagged `:system` (boots a real subprocess); the Java arm additionally needs
  `:java_sdk` and the committed jar. Run with
  `mix test --include system --include java_sdk test/system/dynamic_entity_e2e_test.exs`.
  """

  use ExUnit.Case, async: false

  alias FlezhaTest.Helper.NodeTestHelper
  alias Flezha.Operative.Builder
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.Registry

  @moduletag :system
  @namespace "dynamic_entity_e2e"
  @procedure "dynamic_entity_procedure"

  setup do
    on_exit(fn ->
      try do
        OperativeManager.stop_all_operatives(@namespace)
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

  @tag :dynamic_entity_e2e
  test "lazy-created Entity's state is shipped, then reaped + deleted controller-side (rust)" do
    run_dynamic_entity_case(:rust, "dynamic_entity")
  end

  @tag :dynamic_entity_e2e
  @tag :java_sdk
  test "a keyed edge routes to one Entity per key, created on first sight (java)" do
    jar = Path.join(File.cwd!(), "test/apps/java_runner/dist/java_runner.jar")

    if not File.exists?(jar) do
      flunk("committed java_runner.jar missing — run scripts/build-dist.sh")
    end

    {port, ce_id} =
      NodeTestHelper.start_runner_port(@namespace, runtime: :java, connect_timeout: 120_000)

    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "dyn_e2e_java_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"

    builder = %Builder{
      id: operative_id,
      factory_id: "java_dynamic_entity",
      init_values: %{},
      opts: []
    }

    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    # Two keys and one filtered value: the edge's key function is what creates
    # the Entities, and the filter is what discards — there is no keyless-drop
    # path left to fall through.
    OperativeManager.send_io(operative_id, @namespace, "#{key}:one", %{"input_type" => "2"})
    OperativeManager.send_io(operative_id, @namespace, "drop", %{"input_type" => "2"})
    OperativeManager.send_io(operative_id, @namespace, "#{key}b:two", %{"input_type" => "2"})

    assert wait_until(
             fn -> entity_keys(@namespace, operative_id) == [key, "#{key}b"] end,
             15_000
           ),
           "expected one Entity per key seen, got #{inspect(entity_keys(@namespace, operative_id))}"
  end

  @tag :dynamic_entity_e2e
  test "a live dynamic Entity is readable by name, narrowed to one key (rust)" do
    run_live_read_case(:rust, "dynamic_entity")
  end

  @tag :dynamic_entity_e2e
  @tag :java_sdk
  test "a live dynamic Entity is readable by name, narrowed to one key (java)" do
    run_live_read_case(:java, "java_dynamic_entity")
  end

  # `flz state get`'s round-trip against a real runner: a lazily-created Entity
  # is readable by name, the reply carries the seq it was serialized at, and the
  # selector really narrows — the other live key is not serialized. No TTL is
  # armed here, so nothing races the read.
  defp run_live_read_case(runtime, factory) do
    # Own namespace per runtime: placement is HRW over every runner active in
    # the namespace, so a peer case's runner (which does not declare this
    # factory) must not be a candidate.
    namespace = "dyn_live_read_#{runtime}"

    {port, ce_id} =
      NodeTestHelper.start_runner_port(namespace, runtime: runtime, connect_timeout: 120_000)

    on_exit(fn ->
      NodeTestHelper.ensure_port_dead(port)

      try do
        OperativeManager.stop_all_operatives(namespace)
        FleetManager.stop_namespace(namespace)
      catch
        _, _ -> :ok
      end
    end)

    operative_id = "dyn_read_e2e_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"
    other = "#{key}b"

    builder = %Builder{id: operative_id, factory_id: factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperativeManager.start_operative(namespace, builder)
    assert info.runner_id == ce_id

    OperativeManager.send_io(operative_id, namespace, "#{key}:hello", %{"input_type" => "2"})
    OperativeManager.send_io(operative_id, namespace, "#{other}:bye", %{"input_type" => "2"})

    assert wait_until(fn -> entity_keys(namespace, operative_id) == [key, other] end, 15_000),
           "both keys should be live before the read is narrowed"

    assert {:ok, json, seq} =
             OperativeManager.get_entity_state(namespace, operative_id, %{
               procedure: @procedure,
               entity_key: key
             })

    assert Flezha.Service.Payloader.json_to_map(json) == %{
             @procedure => %{key => %{"seen" => ["#{key}:hello"]}}
           },
           "the read must return exactly the Entity asked for, holding what run() just applied"

    assert is_integer(seq), "a single-Entity read reports the change_seq it was serialized at"

    assert {:error, refusal} =
             OperativeManager.get_entity_state(namespace, operative_id, %{
               procedure: @procedure,
               entity_key: "never-seen"
             })

    assert refusal =~ "entity_not_found"
  end

  # The Java arm asserts against LIVE Entity state rather than the WAL: the
  # Java SDK does not register a lazily-created Entity with its flusher, so a
  # dynamic Entity's state never reaches disk there. That gap predates edge
  # keying (it was reached identically through the deleted `entityKey` hook)
  # and closing it is a Java durability change, not an authoring-surface one.
  defp entity_keys(namespace, operative_id) do
    with {:ok, json} <- OperativeManager.get_entity_state(namespace, operative_id),
         %{@procedure => by_key} when is_map(by_key) <-
           Flezha.Service.Payloader.json_to_map(json) do
      by_key |> Map.keys() |> Enum.sort()
    else
      _ -> nil
    end
  end

  defp run_dynamic_entity_case(runtime, factory) do
    # 200 ms idle TTL so the operative's reaper evicts + ships
    # delete_entity_state within the test (default is no eviction).
    {port, ce_id} =
      NodeTestHelper.start_runner_port(@namespace,
        runtime: runtime,
        connect_timeout: 120_000,
        env: [{"DYN_TEST_TTL_MS", "200"}]
      )

    on_exit(fn -> NodeTestHelper.ensure_port_dead(port) end)

    operative_id = "dyn_e2e_#{System.unique_integer([:positive])}"
    key = "k#{System.unique_integer([:positive])}"
    entity_id = {operative_id, @procedure, key}

    builder = %Builder{id: operative_id, factory_id: factory, init_values: %{}, opts: []}
    assert {:ok, info} = OperativeManager.start_operative(@namespace, builder)
    assert info.runner_id == ce_id

    # An input for a never-seen key: the edge's key function names the Entity,
    # the SDK lazily creates it, runs it, and ships the Change. The controller
    # writes a WAL file — at the identity the trait hook used to produce.
    OperativeManager.send_io(operative_id, @namespace, "#{key}:hello", %{"input_type" => "2"})

    assert wait_until(fn -> File.exists?(Registry.wal_path(entity_id)) end, 15_000),
           "controller never wrote a WAL file for the lazily-created Entity"

    # A filtered input reaches no Entity at all — there is no keyless-drop path
    # to fall through any more.
    OperativeManager.send_io(operative_id, @namespace, "drop", %{"input_type" => "2"})

    # No further input for the live key: after the 200 ms idle TTL the reaper
    # evicts the Entity and ships delete_entity_state; the controller removes
    # the file. Generous budget for sweep cadence + wire + teardown.
    assert wait_until(fn -> not File.exists?(Registry.wal_path(entity_id)) end, 15_000),
           "reaper/delete-RPC never removed the evicted Entity's WAL file"

    assert Registry.snapshot_paths(entity_id) == []
  end

  defp wait_until(fun, timeout_ms) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_until(fun, deadline)
  end

  defp do_wait_until(fun, deadline) do
    cond do
      fun.() ->
        true

      System.monotonic_time(:millisecond) >= deadline ->
        false

      true ->
        Process.sleep(25)
        do_wait_until(fun, deadline)
    end
  end
end
