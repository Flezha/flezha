defmodule FlezhaTest do
  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias Flezha.Testnet
  alias FlezhaTest.Fixtures.OperatorFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OperatorTestHelper
  alias ProcessHub.Constant.Hook

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"
  @node_name NodeTestHelper.node_name()

  @test_store_input_factory "store_input"
  @test_operator_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  # WS-dependent tests below connect to a Node.js testnet daemon at
  # `Testnet.ws_uri/0`. The harness starts it (`Flezha.Testnet`, on the port the
  # run derives) but the bundle it runs is built by `mix test.setup`, so these
  # tests are tagged `:testnet` and excluded from the default `mix test` run via
  # `test/test_helper.exs`. Run them with:
  #     mix test.setup
  #     mix test --include testnet

  setup_all do
    %{}
  end

  setup _context do
    # Restore now rehydrates an operator from its WAL on (re)bind, so leftover
    # per-Cell state from an earlier test that reused the same operator_id
    # would leak in. Start each test from empty cells.
    TestHelpers.reset_all_cells()

    on_exit(fn ->
      OperatorManager.stop_all_operators(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  test "runner node connect" do
    res = FleetManager.active_runners(@default_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  @tag :operator_start
  test "operator adding" do
    operator = %Flezha.Operator.Builder{
      id: "default_operator_test",
      factory_id: @test_operator_factory
    }

    {status, info} = OperatorManager.start_operator(@default_namespace, operator)

    assert status === :ok,
           "start_operator failed: #{inspect(info)}"

    assert info[:operator_id] === "default_operator_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:runner_id])
  end

  test "operator removing" do
    operator_id = "default_operator_stop_test"

    {:ok, %{pid: _operator_pid}} =
      operator_id
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start()

    assert :ok === OperatorManager.stop_operator(@default_namespace, operator_id)

    assert NodeTestHelper.wait_until(
             fn -> not OperatorManager.exist?(@default_namespace, operator_id) end,
             5_000
           ),
           "the stopped operator is still registered"
  end

  test "operator adding non existing factory" do
    operator_id = "non_existing_factory_err"

    operator = %Flezha.Operator.Builder{
      id: operator_id,
      factory_id: "non_existing_factory"
    }

    {status, %{operator_id: pid, node: node, reason: reason}} =
      OperatorManager.start_operator(@default_namespace, operator)

    assert status === :error
    assert pid === operator_id
    assert node === @node_name
    # An unknown factory is refused cluster-wide now: no local runner
    # advertises it and the cluster directory names no capable peer either.
    assert reason ===
             "no runners for factory non_existing_factory in namespace " <>
               "#{@default_namespace} on any cluster node"
  end

  @tag :websocket_plugin
  @tag :testnet
  test "websocket plugin connection start" do
    operator_id = "websocket_plugin_test"

    operator = %Flezha.Operator.Builder{
      id: operator_id,
      factory_id: @test_operator_factory
    }

    {:ok, _} = OperatorManager.start_operator(@default_namespace, operator)

    res =
      PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
        id: "websocket_plugin",
        namespace: @default_namespace,
        operator_id: operator_id,
        provider: Flezha.Plugin.Provider.Websocket,
        opts: %{
          uri: Testnet.ws_uri(),
          post_connect: %{
            payload: %{
              "type" => "text",
              "message" => Testnet.subscribe_message("hello world", 1000)
            }
          }
        },
        hooks: [{:on_terminate, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :testnet
  test "beam msg forward plugin connection start" do
    operator_id = "beam_msg_forw_plugin_test"

    operator = %Flezha.Operator.Builder{
      id: operator_id,
      factory_id: @test_operator_factory
    }

    {:ok, _} = OperatorManager.start_operator(@default_namespace, operator)

    res =
      PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
        id: "beam_msg_forward",
        namespace: @default_namespace,
        operator_id: operator_id,
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self()},
        hooks: [{:on_output, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :operator_flow_msg_forward
  @tag :testnet
  test "operator flow generation -> beam msg forward" do
    operator_id = "data_flow_test"

    operator = %Flezha.Operator.Builder{
      id: operator_id,
      factory_id: @test_compare_filter_factory,
      init_values: %{
        "compare_filter_tasklet" => %{"comparable" => "hello world", "inverted" => false}
      }
    }

    {:ok, _} = OperatorManager.start_operator(@default_namespace, operator)

    PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
      id: "beam_msg_forward",
      namespace: @default_namespace,
      operator_id: operator_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      hooks: [{:on_output, []}]
    })

    PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
      id: "websocket_plugin",
      namespace: @default_namespace,
      operator_id: operator_id,
      provider: Flezha.Plugin.Provider.Websocket,
      opts: %{
        uri: Testnet.ws_uri(),
        post_connect: %{
          "payload" => %{
            "type" => "text",
            "message" => Testnet.subscribe_message("hello world", 1000)
          }
        }
      },
      hooks: [{:on_terminate, []}]
    })

    for _ <- 1..1 do
      receive do
        {:message_forward, msg} ->
          assert msg === "hello world"
      after
        2000 -> assert false
      end
    end
  end

  @tag :get_cell_state
  test "get operator state" do
    operator_id = "get_state_test"

    {:ok, %{pid: _operator_pid}} =
      operator_id
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start()
      |> OperatorFixture.attach_output_msg()

    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "state_1", %{})
    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "state_2", %{})

    {:ok, json_state} =
      OperatorManager.get_cell_state(@default_namespace, operator_id)

    cell_state = Payloader.json_to_map(json_state)

    assert cell_state === %{
             "store_input_tasklet" => %{
               "binary_inputs" => [],
               "text_inputs" => ["state_1", "state_2"]
             }
           }
  end

  @tag :operator_migration
  test "operator migration" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    operator_id = "operator_migration_test"

    {:ok, %{pid: _operator_pid, node: pnode}} =
      operator_id
      |> OperatorFixture.get(@test_store_input_factory)
      |> OperatorFixture.start()
      |> OperatorFixture.attach_output_msg()

    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "test_state_1", %{})
    OperatorTestHelper.send_sync_io(operator_id, @default_namespace, "test_state_2", %{})

    test_state = %{
      "store_input_tasklet" => %{
        "binary_inputs" => [],
        "text_inputs" => ["test_state_1", "test_state_2"]
      }
    }

    PluginManager.enable_plugin("stdout_plugin", %Flezha.Plugin{
      id: "stdout_plugin",
      namespace: @default_namespace,
      operator_id: operator_id,
      provider: Flezha.Plugin.Provider.StdOut,
      opts: %{},
      hooks: [{:on_output, []}]
    })

    {:ok, json_state} =
      OperatorManager.get_cell_state(@default_namespace, operator_id)

    {:ok, old_overwatch_state} =
      OperatorManager.get_overwatch_state(@default_namespace, operator_id)

    cell_state = Payloader.json_to_map(json_state)

    assert cell_state === test_state

    test_pid = self()

    ProcessHub.register_hook_handlers(
      Identificator.hub(@default_namespace),
      Hook.post_children_start(),
      [
        %ProcessHub.Service.HookManager{
          id: "migration_wait",
          m: Process,
          f: :send,
          a: [test_pid, :migration_complete, []]
        }
      ]
    )

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)

    if pnode == peer_node do
      receive do
        :migration_complete -> :ok
      after
        5000 -> flunk("Migration did not complete within 5 seconds")
      end
    end

    ProcessHub.cancel_hook_handlers(
      Identificator.hub(@default_namespace),
      Hook.post_children_start(),
      ["migration_wait"]
    )

    {:ok, json_state} =
      OperatorManager.get_cell_state(@default_namespace, operator_id)

    {:ok, new_overwatch_state} =
      OperatorManager.get_overwatch_state(@default_namespace, operator_id)

    cell_state = Payloader.json_to_map(json_state)

    assert cell_state === test_state

    old_plugins = Map.keys(old_overwatch_state.extensions.plugins) |> Enum.sort()
    new_plugins = Map.keys(new_overwatch_state.extensions.plugins) |> Enum.sort()
    assert new_plugins === old_plugins
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end
end
