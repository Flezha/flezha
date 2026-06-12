defmodule FlezhaTest.SickProvider do
  @moduledoc false
  # Convergence plugin-health test double: the health verdict and the
  # activation count are controlled through :persistent_term.
  def on_output(_ctx, _opts), do: :ok

  def activate(_opts) do
    :persistent_term.put({__MODULE__, :activations}, activations() + 1)
    :ok
  end

  def health(_opts), do: :persistent_term.get({__MODULE__, :health}, :healthy)

  def set_health(verdict), do: :persistent_term.put({__MODULE__, :health}, verdict)
  def activations(), do: :persistent_term.get({__MODULE__, :activations}, 0)

  def reset() do
    :persistent_term.put({__MODULE__, :health}, :healthy)
    :persistent_term.put({__MODULE__, :activations}, 0)
  end
end

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
    # The Loop's recovery gate (`Probe.await_recovery_complete/2`) requires
    # every hub the always-on Snapshot has ever observed to be settled. That
    # cache accumulates across test files and is never cleared, so a hub left
    # mid-recovery by an earlier test would block this Loop's first tick
    # forever. Reset it so the gate only weighs this test's hubs.
    Flezha.Convergence.Snapshot.reset_recovery_states()

    # Short periodic tick so the reseat reconciles within the assertion
    # budget even if the single `:executor_connected_event` is missed under
    # in-suite contention (other tests also drive the global EventManager).
    {:ok, _} =
      start_supervised({Flezha.Convergence.Loop, tick_ms: 1_000})

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
    # onto `executor_id_3`. In production a periodic tick + the
    # `:executor_connected_event` drive this; in-suite, async tick delivery
    # and the global recovery gate are non-deterministic, so drive the real
    # reconcile path synchronously via `tick_now/0` on each poll and wait
    # for the reseat to land.
    assert eventually(
             fn ->
               Flezha.Convergence.Loop.tick_now()

               RelayManager.relay_registry(namespace)
               |> Enum.all?(fn {_relay_id, {_cspec, _cnodes, %{tag: tag}}} ->
                 tag === executor_id_3
               end)
             end,
             15_000
           ),
           "relays did not reseat to executor_id_3 within timeout"

    # The rebind tag flips before the WAL restore finishes replaying the
    # change-log onto the reseated executor, so poll the migrated state
    # rather than reading it the instant the tag lands.
    expected_state = fn relay_id ->
      "{\"store_input_shard\":{\"binary_inputs\":[],\"text_inputs\":[\"state_#{relay_id}\"]}}"
    end

    # The convergence Loop dispatches the WAL restore once, inside the rebind
    # heal — but once the tag is `executor_id_3` the relay is no longer
    # misbound, so a later tick won't re-dispatch. Re-drive the documented,
    # idempotent restore re-dispatch path per relay each poll so a restore
    # that raced the new executor's readiness retries until state converges.
    assert eventually(
             fn ->
               for {relay_id, _info} <- relay_infos do
                 case HubManager.get_pid(namespace, relay_id) do
                   {:ok, pid} when is_pid(pid) ->
                     Flezha.Relay.Overwatch.kick_restore_for_bound_executor(
                       pid,
                       executor_id_3
                     )

                   _ ->
                     :ok
                 end
               end

               Enum.all?(relay_infos, fn {relay_id, _info} ->
                 RelayManager.get_partition_state(namespace, relay_id) ===
                   {:ok, expected_state.(relay_id)}
               end)
             end,
             15_000
           ),
           "relay states did not migrate to executor_id_3 within timeout"

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

  test "pluginless running relay re-attaches its declared plugins (2026-06-12 incident shape)" do
    namespace = @temp_namespace
    Flezha.Convergence.Snapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Flezha.Convergence.Loop, tick_ms: 60_000})

    executor_id = NodeTestHelper.start_executor(namespace)

    {:ok, %{relay_id: relay_id}} =
      "self_heal_relay"
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)
      |> RelayFixture.attach_output_msg(namespace)

    plugin_id = relay_id <> "_output_msg_handler"
    {:ok, ow} = HubManager.get_pid(namespace, relay_id)
    assert subscribed_plugin?(ow, plugin_id)

    # Direct Overwatch unsubscribe bypasses PluginManager, so the declared
    # cspec manifest keeps the plugin: running-but-pluginless, exactly the
    # warm-restart incident state.
    :ok = GenServer.call(ow, {:plugin_unsubscribe, plugin_id})
    refute subscribed_plugin?(ow, plugin_id)

    assert eventually(
             fn ->
               Flezha.Convergence.Loop.tick_now()
               subscribed_plugin?(ow, plugin_id)
             end,
             15_000
           ),
           "loop did not re-attach the declared plugin"

    NodeTestHelper.stop_executor(namespace, executor_id)
  end

  test "declared plugin self-reporting unhealthy is restarted by the loop" do
    namespace = @temp_namespace
    Flezha.Convergence.Snapshot.reset_recovery_states()
    {:ok, _} = start_supervised({Flezha.Convergence.Loop, tick_ms: 60_000})

    executor_id = NodeTestHelper.start_executor(namespace)

    {:ok, %{relay_id: relay_id}} =
      "sick_plugin_relay"
      |> RelayFixture.get(@test_store_input_factory)
      |> RelayFixture.start(namespace)

    plugin_id = relay_id <> "_sick"
    FlezhaTest.SickProvider.reset()

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(plugin_id, %Flezha.Plugin{
        id: plugin_id,
        namespace: namespace,
        relay_id: relay_id,
        provider: FlezhaTest.SickProvider,
        opts: %{},
        subscriptions: [{:on_output, []}]
      })

    {:ok, ow} = HubManager.get_pid(namespace, relay_id)
    assert subscribed_plugin?(ow, plugin_id)
    activations_before = FlezhaTest.SickProvider.activations()

    # Healthy plugin: a tick must not restart it.
    Flezha.Convergence.Loop.tick_now()
    assert FlezhaTest.SickProvider.activations() == activations_before

    FlezhaTest.SickProvider.set_health({:unhealthy, :test_sick})

    assert eventually(
             fn ->
               Flezha.Convergence.Loop.tick_now()
               FlezhaTest.SickProvider.activations() > activations_before
             end,
             15_000
           ),
           "loop did not restart the unhealthy plugin"

    assert subscribed_plugin?(ow, plugin_id)

    # Verdict cleared: no further restarts once healthy again.
    FlezhaTest.SickProvider.set_health(:healthy)
    Flezha.Convergence.Loop.tick_now()
    stable = FlezhaTest.SickProvider.activations()
    Flezha.Convergence.Loop.tick_now()
    assert FlezhaTest.SickProvider.activations() == stable

    NodeTestHelper.stop_executor(namespace, executor_id)
  end

  defp subscribed_plugin?(ow, plugin_id) do
    case GenServer.call(ow, :get_overwatch_state) do
      %{extensions: %{plugins: plugins}} -> Map.has_key?(plugins, plugin_id)
      _ -> false
    end
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
