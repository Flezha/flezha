defmodule FlezhaTest do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Executor.FleetManager
  alias FlezhaTest.Fixtures.RelayFixture
  alias Flezha.Service.PluginManager
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.RelayTestHelper
  alias FlezhaTest.Helper.MessageTestHelper

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"
  @node_name NodeTestHelper.node_name()
  @testnet_ws_uri "ws://localhost:4500"

  @test_store_input_factory "store_input"
  @test_relay_factory "do_nothing"
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
    # Restore now rehydrates a relay from its WAL on (re)bind, so leftover
    # per-Partition state from an earlier test that reused the same relay_id
    # would leak in. Start each test from empty partitions.
    Flezha.State.TestHelpers.reset_all_partitions()

    on_exit(fn ->
      RelayManager.stop_all_relays(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  test "executor node connect" do
    res = Flezha.Executor.FleetManager.active_executors(@default_namespace)
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
    assert is_binary(info[:executor_id])
  end

  test "relay removing" do
    relay_id = "default_relay_stop_test"

    {:ok, %{pid: _relay_pid}} =
      relay_id
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start()

    assert :ok === RelayManager.stop_relay(@default_namespace, relay_id)

    # TODO: add hook instead to listen to process removal.
    Process.sleep(100)

    assert RelayManager.exist?(@default_namespace, relay_id) === false
  end

  test "relay adding non existing factory" do
    relay_id = "non_existing_factory_err"

    relay = %Flezha.Relay.Builder{
      id: relay_id,
      factory_id: "non_existing_factory"
    }

    {status, %{relay_id: pid, node: node, reason: reason}} =
      RelayManager.start_relay(@default_namespace, relay)

    assert status === :error
    assert pid === relay_id
    assert node === @node_name
    assert reason === "Factory with id non_existing_factory does not exist"
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

    # TODO: add failures also.
    res =
      PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
        id: "websocket_plugin",
        namespace: @default_namespace,
        relay_id: relay_id,
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
        subscriptions: [{:on_output, []}]
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
        "compare_filter_shard" => %{"comparable" => "hello world", "inverted" => false}
      }
    }

    {:ok, _} = RelayManager.start_relay(@default_namespace, relay)

    PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin{
      id: "beam_msg_forward",
      namespace: @default_namespace,
      relay_id: relay_id,
      provider: Flezha.Plugin.Provider.BeamMsgForward,
      opts: %{recv_pid: self()},
      subscriptions: [{:on_output, []}]
    })

    PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin{
      id: "websocket_plugin",
      namespace: @default_namespace,
      relay_id: relay_id,
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

  @tag :get_partition_state
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
      RelayManager.get_partition_state(@default_namespace, relay_id)

    partition_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert partition_state === %{
             "store_input_shard" => %{
               "binary_inputs" => [],
               "text_inputs" => ["state_1", "state_2"]
             }
           }
  end

  # # # # TODO: add test to handle large control requests and stream requests.

  @tag :relay_migration
  test "relay migration" do
    # TODO: we should spawn a new elixir node, wait till the process gets migrated there(using dynamic hooks) and
    # then get the state of the new migrated process to see if it matches the state1 string.

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
      "store_input_shard" => %{
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
      subscriptions: [{:on_output, []}]
    })

    {:ok, json_state} =
      RelayManager.get_partition_state(@default_namespace, relay_id)

    {:ok, old_overwatch_state} =
      RelayManager.get_overwatch_state(@default_namespace, relay_id)

    partition_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert partition_state === test_state

    test_pid = self()

    ProcessHub.register_hook_handlers(
      Identificator.hub(@default_namespace),
      ProcessHub.Constant.Hook.post_children_start(),
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
      ProcessHub.Constant.Hook.post_children_start(),
      ["migration_wait"]
    )

    {:ok, json_state} =
      RelayManager.get_partition_state(@default_namespace, relay_id)

    {:ok, new_overwatch_state} =
      RelayManager.get_overwatch_state(@default_namespace, relay_id)

    partition_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert partition_state === test_state

    old_plugins = Map.keys(old_overwatch_state.extensions.plugins) |> Enum.sort()
    new_plugins = Map.keys(new_overwatch_state.extensions.plugins) |> Enum.sort()
    assert new_plugins === old_plugins
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end

  @tag :executor_disconnect
  test "executor disconnect" do
    namespace = @temp_namespace
    executor_id = NodeTestHelper.start_executor(namespace)

    NodeTestHelper.stop_executor(namespace, executor_id)

    assert Flezha.Executor.FleetManager.active_executors(namespace) === []
  end

  @tag :executor_disconnect_migrate_op
  test "executor disconnect migrate op" do
    namespace = @temp_namespace
    relay_id_1 = "relay_1"
    relay_id_2 = "relay_2"

    # Start 2 executors under the same namespace.
    executor_id_1 = NodeTestHelper.start_executor(namespace)
    executor_id_2 = NodeTestHelper.start_executor(namespace)

    # Start a few relays and store some state on them.
    {:ok, %{pid: _relay_pid_1, executor_id: pipe1_op}} =
      relay_id_1
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)
      |> RelayFixture.attach_output_msg(namespace)

    {:ok, %{pid: _relay_pid_2, executor_id: _pipe2_op}} =
      relay_id_2
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)
      |> RelayFixture.attach_output_msg(namespace)

    RelayTestHelper.send_sync_io(relay_id_1, namespace, "state_1", %{})
    RelayTestHelper.send_sync_io(relay_id_2, namespace, "state_2", %{})

    RelayManager.create_snapshot(namespace, relay_id_1)
    RelayManager.create_snapshot(namespace, relay_id_2)

    active_executor =
      if pipe1_op === executor_id_1, do: executor_id_2, else: executor_id_1

    # Trigger the executor disconnect.
    NodeTestHelper.stop_executor(namespace, pipe1_op)

    # Make sure all the relays are migrated to the other executor.
    RelayManager.relay_registry(namespace)
    |> Enum.each(fn {_relay_id, {_cspec, _cnodes, %{tag: new_executor}}} ->
      assert new_executor === active_executor
    end)

    # Make sure the states have been migrated.
    {:ok, json_state_1} = RelayManager.get_partition_state(namespace, relay_id_1)
    {:ok, json_state_2} = RelayManager.get_partition_state(namespace, relay_id_2)

    assert json_state_1 ===
             "{\"store_input_shard\":{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}}"

    assert json_state_2 ===
             "{\"store_input_shard\":{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}}"
  end

  @tag :executor_disconnect_preserve
  test "executor disconnect preserve" do
    namespace = @temp_namespace
    num_relays = 15
    relay_ids = for i <- 1..num_relays, do: "relay_#{i}"

    # Fresh-process executor takeover (an :inactive peer's bindings
    # are migrated onto the freshly-connected executor) is driven by
    # `Flezha.Convergence.Loop`, which is OFF in the default test config
    # to avoid global-tick races. Start it locally for this test, gated
    # on `:executor_connected_event` so the rebind fires within ~ms of
    # `executor_id_3` connecting.
    {:ok, _} =
      start_supervised({Flezha.Convergence.Loop, tick_ms: 60_000})

    executor_id_1 = NodeTestHelper.start_executor(namespace)
    executor_id_2 = NodeTestHelper.start_executor(namespace)

    # Start relays and store some state on them.

    relay_infos =
      for relay_id <- relay_ids do
        {:ok, info} =
          relay_id
          |> RelayFixture.get(@test_store_input_factory)
          |> RelayFixture.start(namespace)
          |> RelayFixture.attach_output_msg(namespace)

        {relay_id, info}
      end

    # Send state to all relays
    for {relay_id, _info} <- relay_infos do
      RelayTestHelper.send_sync_io(relay_id, namespace, "state_#{relay_id}", %{})
    end

    # Create snapshots for all relays
    for {relay_id, _info} <- relay_infos do
      RelayManager.create_snapshot(namespace, relay_id)
    end

    # Trigger the executor disconnect.
    NodeTestHelper.stop_executor(namespace, executor_id_1)
    NodeTestHelper.stop_executor(namespace, executor_id_2)

    # Start a new executor under the same namespace.
    executor_id_3 = NodeTestHelper.start_executor(namespace)

    # The convergence Loop reseats every relay tagged to a dead executor
    # onto `executor_id_3` on its next tick; the
    # `:executor_connected_event` above triggers that tick within ~ms.
    # Poll rather than assume the rebind already landed.
    assert eventually(fn ->
             RelayManager.relay_registry(namespace)
             |> Enum.all?(fn {_relay_id, {_cspec, _cnodes, %{tag: tag}}} ->
               tag === executor_id_3
             end)
           end),
           "relays did not reseat to executor_id_3 within timeout"

    # Make sure the states have been migrated.
    for {relay_id, _info} <- relay_infos do
      {:ok, json_state} = RelayManager.get_partition_state(namespace, relay_id)

      assert json_state ===
               "{\"store_input_shard\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"
    end

    # Stop the new executor.
    NodeTestHelper.stop_executor(namespace, executor_id_3)

    # Adjust the executor cleanup interval and trigger the cleanup.
    Application.put_env(:flezha, :executor_keepalive, "0")
    GenServer.call(FleetManager, :executor_cleanup)

    # Reset the executor keepalive to its default value.
    Application.put_env(:flezha, :executor_keepalive, "30000")

    # Make sure the hub is stopped.
    assert HubManager.hub_alive?(namespace) === false
  end

  @tag :executor_disconnect_migrate_hub
  test "executor disconnect migrate hub" do
    namespace = @temp_namespace
    num_relays = 15
    relay_ids = for i <- 1..num_relays, do: "relay_#{i}"

    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    # Start the executors.
    executor_id_1 = NodeTestHelper.start_executor(namespace)
    rem_executor_1 = NodeTestHelper.start_executor_remote(peer_node, namespace)
    rem_executor_2 = NodeTestHelper.start_executor_remote(peer_node, namespace)

    # Wait for peer node to join the ProcessHub hub cluster
    hub_id = Identificator.hub(namespace)
    test_pid = self()

    ProcessHub.register_hook_handlers(
      hub_id,
      ProcessHub.Constant.Hook.post_node_join(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_node_join_wait",
          m: Process,
          f: :send,
          a: [test_pid, :peer_node_joined, []]
        }
      ]
    )

    if length(HubManager.get_hub_nodes(namespace, true)) < 2 do
      receive do
        :peer_node_joined -> :ok
      after
        5_000 -> flunk("Peer node did not join the hub cluster within 5 seconds")
      end
    end

    ProcessHub.cancel_hook_handlers(hub_id, ProcessHub.Constant.Hook.post_node_join(), [
      "hub_node_join_wait"
    ])

    self = self()
    _node = node()

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      ProcessHub.Constant.Hook.child_registered(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_insert_hook_test",
          m: Process,
          f: :send,
          a: [self, :hook_insert_recv, []]
        }
      ]
    )

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      ProcessHub.Constant.Hook.post_children_start(),
      [
        %ProcessHub.Service.HookManager{
          id: "hub_pstartup_hook_test",
          m: Process,
          f: :send,
          a: [self, :hook_pstartup_recv, []]
        }
      ]
    )

    # Start relays and store some state on them.
    relay_infos =
      for relay_id <- relay_ids do
        {:ok, info} =
          relay_id
          |> RelayFixture.get(@test_store_input_factory)
          |> RelayFixture.start(namespace)
          |> RelayFixture.attach_output_msg(namespace)

        {relay_id, info}
      end

    executor_ids =
      Enum.map(relay_infos, fn {_id, %{executor_id: ce_id}} -> ce_id end)

    # Make sure that atleast one of the relays is running on the remote executor.
    assert Enum.any?(executor_ids, &(&1 !== executor_id_1))

    # Send state to all relays
    for {relay_id, _info} <- relay_infos do
      RelayTestHelper.send_sync_io(relay_id, namespace, "state_#{relay_id}", %{})
    end

    # Check the states before executor migrations.
    for {relay_id, _info} <- relay_infos do
      {:ok, json_state} = RelayManager.get_partition_state(namespace, relay_id)

      assert json_state ===
               "{\"store_input_shard\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"
    end

    # Create snapshots for all relays
    for {relay_id, _info} <- relay_infos do
      RelayManager.create_snapshot(namespace, relay_id)
    end

    # Trigger the executor disconnect.
    NodeTestHelper.stop_executor_remote(peer_node, namespace, rem_executor_1)
    NodeTestHelper.stop_executor_remote(peer_node, namespace, rem_executor_2)

    # Wait for cross-node ProcessHub sync via hook messages (pure message passing).
    # Each :hook_insert_recv arrives when the local registry syncs a tag update.
    # We check the condition after each message — no polling, no sleeping.
    MessageTestHelper.recv_until(:hook_insert_recv, fn ->
      RelayManager.relay_registry(namespace)
      |> Enum.all?(fn {_, {_, _, %{tag: tag}}} -> tag === executor_id_1 end)
    end)

    MessageTestHelper.flush(:hook_pstartup_recv)

    # Make sure the states have been migrated.
    for {relay_id, _info} <- relay_infos do
      {:ok, json_state} = RelayManager.get_partition_state(namespace, relay_id)

      assert json_state ===
               "{\"store_input_shard\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"
    end

    assert length(HubManager.get_hub_nodes(namespace, true)) === 1

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  defp eventually(fun, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout

    do_eventually = fn do_self, fun, deadline ->
      cond do
        fun.() -> true
        System.monotonic_time(:millisecond) >= deadline -> false
        true ->
          Process.sleep(25)
          do_self.(do_self, fun, deadline)
      end
    end

    do_eventually.(do_eventually, fun, deadline)
  end
end
