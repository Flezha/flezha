defmodule FlezhaTest do
  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.OperativeManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Fixtures.OperativeFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.OperativeTestHelper
  alias ProcessHub.Constant.Hook

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"
  @node_name NodeTestHelper.node_name()
  @testnet_ws_uri "ws://localhost:4500"

  @test_store_input_factory "store_input"
  @test_operative_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  # WS-dependent tests below connect to a Node.js testnet daemon at
  # `@testnet_ws_uri` (the bundle lives at `test/apps/testnet/dist/index.js`,
  # built by `mix test.setup`; the daemon is started by
  # `npm --prefix test/apps/testnet start`). The daemon is not always
  # running, so these tests are tagged `:testnet` and excluded from the
  # default `mix test` run via `test/test_helper.exs`. Run them with:
  #     mix test.setup
  #     npm --prefix test/apps/testnet start &
  #     mix test --include testnet

  setup_all do
    %{}
  end

  setup _context do
    # Restore now rehydrates a operative from its WAL on (re)bind, so leftover
    # per-Entity state from an earlier test that reused the same operative_id
    # would leak in. Start each test from empty entities.
    TestHelpers.reset_all_entities()

    on_exit(fn ->
      OperativeManager.stop_all_operatives(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  test "runner node connect" do
    res = FleetManager.active_runners(@default_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  @tag :operative_start
  test "operative adding" do
    operative = %Flezha.Operative.Builder{
      id: "default_operative_test",
      factory_id: @test_operative_factory
    }

    {status, info} = OperativeManager.start_operative(@default_namespace, operative)

    assert status === :ok,
           "start_operative failed: #{inspect(info)}"

    assert info[:operative_id] === "default_operative_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:runner_id])
  end

  test "operative removing" do
    operative_id = "default_operative_stop_test"

    {:ok, %{pid: _operative_pid}} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start()

    assert :ok === OperativeManager.stop_operative(@default_namespace, operative_id)

    assert NodeTestHelper.wait_until(
             fn -> not OperativeManager.exist?(@default_namespace, operative_id) end,
             5_000
           ),
           "the stopped operative is still registered"
  end

  test "operative adding non existing factory" do
    operative_id = "non_existing_factory_err"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: "non_existing_factory"
    }

    {status, %{operative_id: pid, node: node, reason: reason}} =
      OperativeManager.start_operative(@default_namespace, operative)

    assert status === :error
    assert pid === operative_id
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
    operative_id = "websocket_plugin_test"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: @test_operative_factory
    }

    {:ok, _} = OperativeManager.start_operative(@default_namespace, operative)

    res =
      PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
        id: "websocket_plugin",
        namespace: @default_namespace,
        operative_id: operative_id,
        provider: Flezha.Plugin.Provider.Websocket,
        opts: %{
          uri: @testnet_ws_uri,
          post_connect: %{
            payload: %{
              "type" => "text",
              "message" => ~s(
                {
                  "event_type": "subscribe",
                  "event_data": {
                    "default_message": "hello world",
                    "interval": 1000
                  }
                }
              )
            }
          }
        },
        subscriptions: [{:on_terminate, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :testnet
  test "beam msg forward plugin connection start" do
    operative_id = "beam_msg_forw_plugin_test"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: @test_operative_factory
    }

    {:ok, _} = OperativeManager.start_operative(@default_namespace, operative)

    res =
      PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
        id: "beam_msg_forward",
        namespace: @default_namespace,
        operative_id: operative_id,
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self()},
        subscriptions: [{:on_output, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :operative_flow_msg_forward
  @tag :testnet
  test "operative flow generation -> beam msg forward" do
    operative_id = "data_flow_test"

    operative = %Flezha.Operative.Builder{
      id: operative_id,
      factory_id: @test_compare_filter_factory,
      init_values: %{
        "compare_filter_procedure" => %{"comparable" => "hello world", "inverted" => false}
      }
    }

    {:ok, _} = OperativeManager.start_operative(@default_namespace, operative)

    PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
      id: "beam_msg_forward",
      namespace: @default_namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      subscriptions: [{:on_output, []}]
    })

    PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
      id: "websocket_plugin",
      namespace: @default_namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.Websocket,
      opts: %{
        uri: @testnet_ws_uri,
        post_connect: %{
          "payload" => %{
            "type" => "text",
            "message" => ~s(
              {
                "event_type": "subscribe",
                "event_data": {
                  "default_message": "hello world",
                  "interval": 1000
                }
              }
          )
          }
        }
      },
      subscriptions: [{:on_terminate, []}]
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

  @tag :get_entity_state
  test "get operative state" do
    operative_id = "get_state_test"

    {:ok, %{pid: _operative_pid}} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start()
      |> OperativeFixture.attach_output_msg()

    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "state_1", %{})
    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "state_2", %{})

    {:ok, json_state} =
      OperativeManager.get_entity_state(@default_namespace, operative_id)

    entity_state = Payloader.json_to_map(json_state)

    assert entity_state === %{
             "store_input_procedure" => %{
               "binary_inputs" => [],
               "text_inputs" => ["state_1", "state_2"]
             }
           }
  end

  @tag :operative_migration
  test "operative migration" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    operative_id = "operative_migration_test"

    {:ok, %{pid: _operative_pid, node: pnode}} =
      operative_id
      |> OperativeFixture.get(@test_store_input_factory)
      |> OperativeFixture.start()
      |> OperativeFixture.attach_output_msg()

    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "test_state_1", %{})
    OperativeTestHelper.send_sync_io(operative_id, @default_namespace, "test_state_2", %{})

    test_state = %{
      "store_input_procedure" => %{
        "binary_inputs" => [],
        "text_inputs" => ["test_state_1", "test_state_2"]
      }
    }

    PluginManager.enable_plugin("stdout_plugin", %Flezha.Plugin{
      id: "stdout_plugin",
      namespace: @default_namespace,
      operative_id: operative_id,
      provider: Flezha.Plugin.Provider.StdOut,
      opts: %{},
      subscriptions: [{:on_output, []}]
    })

    {:ok, json_state} =
      OperativeManager.get_entity_state(@default_namespace, operative_id)

    {:ok, old_overwatch_state} =
      OperativeManager.get_overwatch_state(@default_namespace, operative_id)

    entity_state = Payloader.json_to_map(json_state)

    assert entity_state === test_state

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
      OperativeManager.get_entity_state(@default_namespace, operative_id)

    {:ok, new_overwatch_state} =
      OperativeManager.get_overwatch_state(@default_namespace, operative_id)

    entity_state = Payloader.json_to_map(json_state)

    assert entity_state === test_state

    old_plugins = Map.keys(old_overwatch_state.extensions.plugins) |> Enum.sort()
    new_plugins = Map.keys(new_overwatch_state.extensions.plugins) |> Enum.sort()
    assert new_plugins === old_plugins
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end
end
