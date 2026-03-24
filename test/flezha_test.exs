defmodule FlezhaTest do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.ComputeEngine.FleetManager
  alias FlezhaTest.Fixtures.ProcessorFixture
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ProcessorTestHelper
  alias FlezhaTest.Helper.MessageTestHelper

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_namespace "default"
  @temp_namespace "test_temp_namespace"
  @node_name NodeTestHelper.node_name()

  @test_store_input_factory "store_input"
  @test_processor_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  setup_all do
    %{}
  end

  setup _context do
    on_exit(fn ->
      ProcessorManager.stop_all_processors(@default_namespace)
      FleetManager.stop_namespace(@temp_namespace)
    end)

    :ok
  end

  test "compute_engine node connect" do
    res = Flezha.ComputeEngine.FleetManager.active_compute_engines(@default_namespace)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  @tag :processor_start
  test "processor adding" do
    processor = %Flezha.Processor.Builder{
      id: "default_processor_test",
      factory_id: @test_processor_factory
    }

    {status, info} = ProcessorManager.start_processor(@default_namespace, processor)

    assert status === :ok,
           "start_processor failed: #{inspect(info)}"

    assert info[:processor_id] === "default_processor_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:compute_engine_id])
  end

  test "processor removing" do
    processor_id = "default_processor_stop_test"

    {:ok, %{pid: _processor_pid}} =
      processor_id
      |> ProcessorFixture.get(@test_store_input_factory)
      |> ProcessorFixture.start()

    assert :ok === ProcessorManager.stop_processor(@default_namespace, processor_id)

    # TODO: add hook instead to listen to process removal.
    Process.sleep(100)

    assert ProcessorManager.exist?(@default_namespace, processor_id) === false
  end

  test "processor adding non existing factory" do
    processor_id = "non_existing_factory_err"

    processor = %Flezha.Processor.Builder{
      id: processor_id,
      factory_id: "non_existing_factory"
    }

    {status, %{processor_id: pid, node: node, reason: reason}} =
      ProcessorManager.start_processor(@default_namespace, processor)

    assert status === :error
    assert pid === processor_id
    assert node === @node_name
    assert reason === "Factory with id non_existing_factory does not exist"
  end

  @tag :websocket_plugin
  test "websocket plugin connection start" do
    processor_id = "websocket_plugin_test"

    processor = %Flezha.Processor.Builder{
      id: processor_id,
      factory_id: @test_processor_factory
    }

    {:ok, _} = ProcessorManager.start_processor(@default_namespace, processor)

    # TODO: add failures also.
    res =
      PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin.Driver.IOBindingPlugin{
        namespace: @default_namespace,
        processor_id: processor_id,
        trigger: :proactive,
        provider: %Flezha.Plugin.Provider.Websocket{
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
        }
      })

    assert res === {:ok, "plugin enabled"}
  end

  test "beam msg forward plugin connection start" do
    processor_id = "beam_msg_forw_plugin_test"

    processor = %Flezha.Processor.Builder{
      id: processor_id,
      factory_id: @test_processor_factory
    }

    {:ok, _} = ProcessorManager.start_processor(@default_namespace, processor)

    res =
      PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin.Driver.IOBindingPlugin{
        namespace: @default_namespace,
        processor_id: processor_id,
        trigger: :on_output,
        provider: %Flezha.Plugin.Provider.BeamMsgForward{
          recv_pid: self()
        }
      })

    assert res === {:ok, "plugin enabled"}
  end

  @tag :processor_flow_msg_forward
  test "processor flow generation -> beam msg forward" do
    processor_id = "data_flow_test"

    processor = %Flezha.Processor.Builder{
      id: processor_id,
      factory_id: @test_compare_filter_factory,
      init_values: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, _} = ProcessorManager.start_processor(@default_namespace, processor)

    PluginManager.enable_plugin("beam_msg_forward", %Flezha.Plugin.Driver.IOBindingPlugin{
      namespace: @default_namespace,
      processor_id: processor_id,
      trigger: :on_output,
      provider: %Flezha.Plugin.Provider.BeamMsgForward{
        recv_pid: self()
      }
    })

    PluginManager.enable_plugin("websocket_plugin", %Flezha.Plugin.Driver.IOBindingPlugin{
      namespace: @default_namespace,
      processor_id: processor_id,
      trigger: :proactive,
      provider: %Flezha.Plugin.Provider.Websocket{
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
      }
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

  @tag :get_processor_state
  test "get processor state" do
    processor_id = "get_state_test"

    {:ok, %{pid: _processor_pid}} =
      processor_id
      |> ProcessorFixture.get(@test_store_input_factory)
      |> ProcessorFixture.start()
      |> ProcessorFixture.attach_output_msg()

    ProcessorTestHelper.send_sync_io(processor_id, @default_namespace, "state_1", %{})
    ProcessorTestHelper.send_sync_io(processor_id, @default_namespace, "state_2", %{})

    {:ok, json_state} =
      ProcessorManager.get_processor_state(@default_namespace, processor_id)

    processor_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert processor_state === %{
             "binary_inputs" => [],
             "text_inputs" => ["state_1", "state_2"]
           }
  end

  # # # # TODO: add test to handle large control requests and stream requests.

  @tag :processor_migration
  test "processor migration" do
    # TODO: we should spawn a new elixir node, wait till the process gets migrated there(using dynamic hooks) and
    # then get the state of the new migrated process to see if it matches the state1 string.

    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    processor_id = "processor_migration_test"

    {:ok, %{pid: _processor_pid, node: pnode}} =
      processor_id
      |> ProcessorFixture.get(@test_store_input_factory)
      |> ProcessorFixture.start()
      |> ProcessorFixture.attach_output_msg()

    ProcessorTestHelper.send_sync_io(processor_id, @default_namespace, "test_state_1", %{})
    ProcessorTestHelper.send_sync_io(processor_id, @default_namespace, "test_state_2", %{})

    test_state = %{
      "binary_inputs" => [],
      "text_inputs" => ["test_state_1", "test_state_2"]
    }

    PluginManager.enable_plugin("stdout_plugin", %Flezha.Plugin.Driver.IOBindingPlugin{
      namespace: @default_namespace,
      processor_id: processor_id,
      trigger: :proactive,
      provider: %Flezha.Plugin.Provider.StdOut{}
    })

    {:ok, json_state} =
      ProcessorManager.get_processor_state(@default_namespace, processor_id)

    {:ok, old_overwatch_state} =
      ProcessorManager.get_overwatch_state(@default_namespace, processor_id)

    processor_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert processor_state === test_state

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
      ProcessorManager.get_processor_state(@default_namespace, processor_id)

    {:ok, new_overwatch_state} =
      ProcessorManager.get_overwatch_state(@default_namespace, processor_id)

    processor_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert processor_state === test_state
    assert new_overwatch_state.bindings === old_overwatch_state.bindings
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end

  @tag :compute_engine_disconnect
  test "compute_engine disconnect" do
    namespace = @temp_namespace
    compute_engine_id = NodeTestHelper.start_compute_engine(namespace)

    NodeTestHelper.stop_compute_engine(namespace, compute_engine_id)

    assert Flezha.ComputeEngine.FleetManager.active_compute_engines(namespace) === []
  end

  @tag :compute_engine_disconnect_migrate_op
  test "compute_engine disconnect migrate op" do
    namespace = @temp_namespace
    processor_id_1 = "processor_1"
    processor_id_2 = "processor_2"

    # Start 2 compute_engines under the same namespace.
    compute_engine_id_1 = NodeTestHelper.start_compute_engine(namespace)
    compute_engine_id_2 = NodeTestHelper.start_compute_engine(namespace)

    # Start a few processors and store some state on them.
    {:ok, %{pid: _processor_pid_1, compute_engine_id: pipe1_op}} =
      processor_id_1
      |> ProcessorFixture.get(@test_store_input_factory)
      |> ProcessorFixture.start(namespace)
      |> ProcessorFixture.attach_output_msg(namespace)

    {:ok, %{pid: _processor_pid_2, compute_engine_id: _pipe2_op}} =
      processor_id_2
      |> ProcessorFixture.get(@test_store_input_factory)
      |> ProcessorFixture.start(namespace)
      |> ProcessorFixture.attach_output_msg(namespace)

    ProcessorTestHelper.send_sync_io(processor_id_1, namespace, "state_1", %{})
    ProcessorTestHelper.send_sync_io(processor_id_2, namespace, "state_2", %{})

    ProcessorManager.create_snapshot(namespace, processor_id_1)
    ProcessorManager.create_snapshot(namespace, processor_id_2)

    active_compute_engine =
      if pipe1_op === compute_engine_id_1, do: compute_engine_id_2, else: compute_engine_id_1

    # Trigger the compute_engine disconnect.
    NodeTestHelper.stop_compute_engine(namespace, pipe1_op)

    # Make sure all the processors are migrated to the other compute_engine.
    ProcessorManager.processor_registry(namespace)
    |> Enum.each(fn {_processor_id, {_cspec, _cnodes, %{tag: new_compute_engine}}} ->
      assert new_compute_engine === active_compute_engine
    end)

    # Make sure the states have been migrated.
    {:ok, json_state_1} = ProcessorManager.get_processor_state(namespace, processor_id_1)
    {:ok, json_state_2} = ProcessorManager.get_processor_state(namespace, processor_id_2)

    assert json_state_1 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}"
    assert json_state_2 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}"
  end

  @tag :compute_engine_disconnect_preserve
  test "compute_engine disconnect preserve" do
    namespace = @temp_namespace
    num_processors = 15
    processor_ids = for i <- 1..num_processors, do: "processor_#{i}"

    compute_engine_id_1 = NodeTestHelper.start_compute_engine(namespace)
    compute_engine_id_2 = NodeTestHelper.start_compute_engine(namespace)

    # Start processors and store some state on them.

    processor_infos =
      for processor_id <- processor_ids do
        {:ok, info} =
          processor_id
          |> ProcessorFixture.get(@test_store_input_factory)
          |> ProcessorFixture.start(namespace)
          |> ProcessorFixture.attach_output_msg(namespace)

        {processor_id, info}
      end

    # Send state to all processors
    for {processor_id, _info} <- processor_infos do
      ProcessorTestHelper.send_sync_io(processor_id, namespace, "state_#{processor_id}", %{})
    end

    # Create snapshots for all processors
    for {processor_id, _info} <- processor_infos do
      ProcessorManager.create_snapshot(namespace, processor_id)
    end

    # Trigger the compute_engine disconnect.
    NodeTestHelper.stop_compute_engine(namespace, compute_engine_id_1)
    NodeTestHelper.stop_compute_engine(namespace, compute_engine_id_2)

    # Start a new compute_engine under the same namespace.
    compute_engine_id_3 = NodeTestHelper.start_compute_engine(namespace)

    # Make sure all the processors are migrated to the new compute_engine.
    ProcessorManager.processor_registry(namespace)
    |> Enum.each(fn {_processor_id, {_cspec, _cnodes, %{tag: new_compute_engine}}} ->
      assert new_compute_engine === compute_engine_id_3
    end)

    # Make sure the states have been migrated.
    for {processor_id, _info} <- processor_infos do
      {:ok, json_state} = ProcessorManager.get_processor_state(namespace, processor_id)
      assert json_state === "{\"binary_inputs\":[],\"text_inputs\":[\"state_#{processor_id}\"]}"
    end

    # Stop the new compute_engine.
    NodeTestHelper.stop_compute_engine(namespace, compute_engine_id_3)

    # Adjust the compute_engine cleanup interval and trigger the cleanup.
    Application.put_env(:flezha, :compute_engine_keepalive, "0")
    GenServer.call(FleetManager, :compute_engine_cleanup)

    # Reset the compute_engine keepalive to its default value.
    Application.put_env(:flezha, :compute_engine_keepalive, "30000")

    # Make sure the hub is stopped.
    assert HubManager.hub_alive?(namespace) === false
  end

  @tag :compute_engine_disconnect_migrate_hub
  test "compute_engine disconnect migrate hub" do
    namespace = @temp_namespace
    num_processors = 15
    processor_ids = for i <- 1..num_processors, do: "processor_#{i}"

    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    # Start the compute_engines.
    compute_engine_id_1 = NodeTestHelper.start_compute_engine(namespace)
    rem_compute_engine_1 = NodeTestHelper.start_compute_engine_remote(peer_node, namespace)
    rem_compute_engine_2 = NodeTestHelper.start_compute_engine_remote(peer_node, namespace)

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

    # Start processors and store some state on them.
    processor_infos =
      for processor_id <- processor_ids do
        {:ok, info} =
          processor_id
          |> ProcessorFixture.get(@test_store_input_factory)
          |> ProcessorFixture.start(namespace)
          |> ProcessorFixture.attach_output_msg(namespace)

        {processor_id, info}
      end

    compute_engine_ids =
      Enum.map(processor_infos, fn {_id, %{compute_engine_id: ce_id}} -> ce_id end)

    # Make sure that atleast one of the processors is running on the remote compute_engine.
    assert Enum.any?(compute_engine_ids, &(&1 !== compute_engine_id_1))

    # Send state to all processors
    for {processor_id, _info} <- processor_infos do
      ProcessorTestHelper.send_sync_io(processor_id, namespace, "state_#{processor_id}", %{})
    end

    # Check the states before compute_engine migrations.
    for {processor_id, _info} <- processor_infos do
      {:ok, json_state} = ProcessorManager.get_processor_state(namespace, processor_id)
      assert json_state === "{\"binary_inputs\":[],\"text_inputs\":[\"state_#{processor_id}\"]}"
    end

    # Create snapshots for all processors
    for {processor_id, _info} <- processor_infos do
      ProcessorManager.create_snapshot(namespace, processor_id)
    end

    # Trigger the compute_engine disconnect.
    NodeTestHelper.stop_compute_engine_remote(peer_node, namespace, rem_compute_engine_1)
    NodeTestHelper.stop_compute_engine_remote(peer_node, namespace, rem_compute_engine_2)

    # Wait for process registered and post-startup hooks from processor migration.
    # With each compute_engine disconnect we update the tag of the processor.
    MessageTestHelper.recv_drain(:hook_insert_recv, 500)
    MessageTestHelper.recv_drain(:hook_pstartup_recv, 500)

    # Make sure all the processors are migrated to the other compute_engine.
    reg = ProcessorManager.processor_registry(namespace)

    Enum.each(reg, fn {_processor_id, {_cspec, _cnodes, %{tag: new_compute_engine}}} ->
      assert new_compute_engine === compute_engine_id_1
    end)

    # Make sure the states have been migrated.
    for {processor_id, _info} <- processor_infos do
      {:ok, json_state} = ProcessorManager.get_processor_state(namespace, processor_id)
      assert json_state === "{\"binary_inputs\":[],\"text_inputs\":[\"state_#{processor_id}\"]}"
    end

    assert length(HubManager.get_hub_nodes(namespace, true)) === 1

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
