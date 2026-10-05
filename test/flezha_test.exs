defmodule FlezhaTest do
  alias Flezha.Identificator
  alias Flezha.Payloader
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.RelayManager
  alias Flezha.Service.PluginManager
  alias Flezha.State.TestHelpers
  alias Flezha.Testnet
  alias FlezhaTest.Fixtures.RelayFixture
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RelayTestHelper
  alias ProcessHub.Constant.Hook

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"
  @node_name NodeTestHelper.node_name()

  @test_store_input_factory "store_input"
  @test_relay_factory "do_nothing"
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
    # Restore now rehydrates a relay from its WAL on (re)bind, so leftover
    # per-Cell state from an earlier test that reused the same relay_id
    # would leak in. Start each test from empty cells.
    TestHelpers.reset_all_cells()

    on_exit(fn ->
      RelayManager.stop_all_relays(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  test "runner node connect" do
    res = FleetManager.active_runners(@default_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  @tag :relay_start
  test "relay adding" do
    relay = %Flezha.Relay.Builder{
      id: "default_relay_test",
      factory_id: @test_relay_factory
    }

    {status, info} = RelayManager.start_relay(@default_namespace, relay)

    assert status === :ok,
           "start_relay failed: #{inspect(info)}"

    assert info[:relay_id] === "default_relay_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:runner_id])
  end

  test "relay removing" do
    relay_id = "default_relay_stop_test"

    {:ok, %{pid: _relay_pid}} =
      relay_id
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start()

    assert :ok === RelayManager.stop_relay(@default_namespace, relay_id)

    assert NodeTestHelper.wait_until(
             fn -> not RelayManager.exist?(@default_namespace, relay_id) end,
             5_000
           ),
           "the stopped relay is still registered"
  end

  test "relay adding non existing factory" do
    relay_id = "non_existing_factory_err"

    relay = %Flezha.Relay.Builder{
      id: relay_id,
      factory_id: "non_existing_factory"
    }

    # An unknown factory is refused cluster-wide now: no local runner
    # advertises it and the cluster directory names no capable peer either.
    assert {:error, {:factory_unknown, @default_namespace, "non_existing_factory"}} =
             RelayManager.start_relay(@default_namespace, relay)
  end

  @tag :websocket_plugin
  @tag :testnet
  test "websocket plugin connection start" do
    relay_id = "websocket_plugin_test"

    relay = %Flezha.Relay.Builder{
      id: relay_id,
      factory_id: @test_relay_factory
    }

    {:ok, _} = RelayManager.start_relay(@default_namespace, relay)

    res =
      PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
        id: "websocket_plugin",
        namespace: @default_namespace,
        relay_id: relay_id,
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
    relay_id = "beam_msg_forw_plugin_test"

    relay = %Flezha.Relay.Builder{
      id: relay_id,
      factory_id: @test_relay_factory
    }

    {:ok, _} = RelayManager.start_relay(@default_namespace, relay)

    res =
      PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
        id: "beam_msg_forward",
        namespace: @default_namespace,
        relay_id: relay_id,
        provider: Flezha.Plugin.Provider.BeamMsgForward,
        opts: %{recv_pid: self()},
        hooks: [{:on_output, []}]
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :relay_flow_msg_forward
  @tag :testnet
  test "relay flow generation -> beam msg forward" do
    relay_id = "data_flow_test"

    relay = %Flezha.Relay.Builder{
      id: relay_id,
      factory_id: @test_compare_filter_factory,
      init_values: %{
        "compare_filter_tasklet" => %{"comparable" => "hello world", "inverted" => false}
      }
    }

    {:ok, _} = RelayManager.start_relay(@default_namespace, relay)

    PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
      id: "beam_msg_forward",
      namespace: @default_namespace,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      hooks: [{:on_output, []}]
    })

    PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
      id: "websocket_plugin",
      namespace: @default_namespace,
      relay_id: relay_id,
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
  test "get relay state" do
    relay_id = "get_state_test"

    {:ok, %{pid: _relay_pid}} =
      relay_id
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start()
      |> RelayFixture.attach_output_msg()

    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "state_1", %{})
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "state_2", %{})

    {:ok, json_state} =
      RelayManager.get_cell_state(@default_namespace, relay_id)

    cell_state = Payloader.json_to_map(json_state)

    assert cell_state === %{
             "store_input_tasklet" => %{
               "binary_inputs" => [],
               "text_inputs" => ["state_1", "state_2"]
             }
           }
  end

  @tag :relay_migration
  test "relay migration" do
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    relay_id = "relay_migration_test"

    {:ok, %{pid: _relay_pid, node: pnode}} =
      relay_id
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start()
      |> RelayFixture.attach_output_msg()

    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "test_state_1", %{})
    RelayTestHelper.send_sync_io(relay_id, @default_namespace, "test_state_2", %{})

    test_state = %{
      "store_input_tasklet" => %{
        "binary_inputs" => [],
        "text_inputs" => ["test_state_1", "test_state_2"]
      }
    }

    PluginManager.enable_plugin("stdout_plugin", %Flezha.Plugin{
      id: "stdout_plugin",
      namespace: @default_namespace,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.StdOut,
      opts: %{},
      hooks: [{:on_output, []}]
    })

    {:ok, json_state} =
      RelayManager.get_cell_state(@default_namespace, relay_id)

    {:ok, old_overwatch_state} =
      RelayManager.get_overwatch_state(@default_namespace, relay_id)

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
      RelayManager.get_cell_state(@default_namespace, relay_id)

    {:ok, new_overwatch_state} =
      RelayManager.get_overwatch_state(@default_namespace, relay_id)

    cell_state = Payloader.json_to_map(json_state)

    assert cell_state === test_state

    old_plugins = Map.keys(old_overwatch_state.extensions.plugins) |> Enum.sort()
    new_plugins = Map.keys(new_overwatch_state.extensions.plugins) |> Enum.sort()
    assert new_plugins === old_plugins
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end
end
