defmodule FlezhaTest do
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Operator.FleetManager
  alias FlezhaTest.Fixtures.ComputationFixture
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ComputationManager
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.ComputationTestHelper
  alias FlezhaTest.Helper.MessageTestHelper

  use ExUnit.Case
  use Flezha.Constant

  doctest Flezha

  @default_group "default"
  @temp_group "test_temp_group"
  @node_name NodeTestHelper.node_name()

  @test_store_input_factory "store_input"
  @test_pipeline_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  setup_all do
    %{}
  end

  setup _context do
    on_exit(fn ->
      ComputationManager.stop_all_pipelines(@default_group)
      FleetManager.stop_group(@temp_group)
    end)

    :ok
  end

  test "operator node connect" do
    res = Flezha.Operator.FleetManager.active_operators(@default_group)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  @tag :pipeline_start
  test "pipeline adding" do
    pipeline = %Flezha.Computation.Builder{
      id: "default_pipeline_test",
      factory_id: @test_pipeline_factory
    }

    {status, info} = ComputationManager.start_pipeline(@default_group, pipeline)

    assert status === :ok
    assert info[:computation_id] === "default_pipeline_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:operator_id])
  end

  test "pipeline removing" do
    computation_id = "default_pipeline_stop_test"

    {:ok, %{pid: _pipeline_pid}} =
      computation_id
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start()

    assert :ok === ComputationManager.stop_pipeline(@default_group, computation_id)

    # TODO: add hook instead to listen to process removal.
    Process.sleep(100)

    assert ComputationManager.exist?(@default_group, computation_id) === false
  end

  test "pipeline adding non existing factory" do
    computation_id = "non_existing_factory_err"

    pipeline = %Flezha.Computation.Builder{
      id: computation_id,
      factory_id: "non_existing_factory"
    }

    {status, %{computation_id: pid, node: node, reason: reason}} =
      ComputationManager.start_pipeline(@default_group, pipeline)

    assert status === :error
    assert pid === computation_id
    assert node === @node_name
    assert reason === "Factory with id non_existing_factory does not exist"
  end

  test "websocket plugin connection start" do
    computation_id = "websocket_plugin_test"

    pipeline = %Flezha.Computation.Builder{
      id: computation_id,
      factory_id: @test_pipeline_factory
    }

    {:ok, _} = ComputationManager.start_pipeline(@default_group, pipeline)

    # TODO: add failures also.
    res =
      PluginManager.enable_plugin(%Flezha.Plugin.Provider.Websocket{
        group: @default_group,
        computation_id: computation_id,
        uri: @testnet_ws_uri,
        trigger: :proactive,
        post_connect: %{
          payload: {
            :text,
            ~s(
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
      })

    assert res === {:ok, "binding added"}
  end

  test "beam msg forward plugin connection start" do
    computation_id = "beam_msg_forw_plugin_test"

    pipeline = %Flezha.Computation.Builder{
      id: computation_id,
      factory_id: @test_pipeline_factory
    }

    {:ok, _} = ComputationManager.start_pipeline(@default_group, pipeline)

    res =
      PluginManager.enable_plugin(%Flezha.Plugin.Provider.BeamMsgForward{
        group: @default_group,
        computation_id: computation_id,
        trigger: :on_output,
        recv_pid: self()
      })

    assert res === {:ok, "binding added"}
  end

  test "pipeline flow generation -> beam msg forward" do
    computation_id = "data_flow_test"

    pipeline = %Flezha.Computation.Builder{
      id: computation_id,
      factory_id: @test_compare_filter_factory,
      init_values: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, _} = ComputationManager.start_pipeline(@default_group, pipeline)

    PluginManager.enable_plugin(%Flezha.Plugin.Provider.BeamMsgForward{
      group: @default_group,
      computation_id: computation_id,
      trigger: :on_output,
      recv_pid: self()
    })

    PluginManager.enable_plugin(%Flezha.Plugin.Provider.Websocket{
      group: @default_group,
      computation_id: computation_id,
      trigger: :proactive,
      uri: @testnet_ws_uri,
      post_connect: %{
        payload: %{
          type: :text,
          message: ~s(
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
    })

    for _ <- 1..1 do
      receive do
        {:message_forward, msg} ->
          assert msg === {:text, "hello world"}
      after
        2000 -> assert false
      end
    end
  end

  @tag :get_computation_state
  test "get pipeline state" do
    computation_id = "get_state_test"

    {:ok, %{pid: _pipeline_pid}} =
      computation_id
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start()
      |> ComputationFixture.attach_output_msg()

    ComputationTestHelper.send_sync_io(computation_id, @default_group, {:text, "state_1"})
    ComputationTestHelper.send_sync_io(computation_id, @default_group, {:text, "state_2"})

    {:ok, json_state} = ComputationManager.get_computation_state(@default_group, computation_id)
    computation_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert computation_state === %{
             "binary_inputs" => [],
             "text_inputs" => ["state_1", "state_2"]
           }
  end

  # # # # TODO: add test to handle large control requests and stream requests.

  @tag :pipeline_migration
  test "pipeline migration" do
    # TODO: we should spawn a new elixir node, wait till the process gets migrated there(using dynamic hooks) and
    # then get the state of the new migrated process to see if it matches the state1 string.

    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    computation_id = "pipeline_migration_test"

    {:ok, %{pid: _pipeline_pid, node: _pnode}} =
      computation_id
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start()
      |> ComputationFixture.attach_output_msg()

    ComputationTestHelper.send_sync_io(computation_id, @default_group, {:text, "test_state_1"})
    ComputationTestHelper.send_sync_io(computation_id, @default_group, {:text, "test_state_2"})

    test_state = %{
      "binary_inputs" => [],
      "text_inputs" => ["test_state_1", "test_state_2"]
    }

    PluginManager.enable_plugin(%Flezha.Plugin.Provider.StdOut{
      group: @default_group,
      computation_id: computation_id,
      trigger: :proactive
    })

    {:ok, json_state} = ComputationManager.get_computation_state(@default_group, computation_id)
    {:ok, old_overwatch_state} = ComputationManager.get_overwatch_state(@default_group, computation_id)
    computation_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert computation_state === test_state

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)

    # TODO: add hook here instead
    Process.sleep(500)

    {:ok, json_state} = ComputationManager.get_computation_state(@default_group, computation_id)
    {:ok, new_overwatch_state} = ComputationManager.get_overwatch_state(@default_group, computation_id)
    computation_state = Flezha.Service.Payloader.json_to_map(json_state)

    assert computation_state === test_state
    assert new_overwatch_state.bindings === old_overwatch_state.bindings
    assert new_overwatch_state.init_values === old_overwatch_state.init_values
  end

  @tag :operator_disconnect
  test "operator disconnect" do
    group_id = @temp_group
    operator_id = NodeTestHelper.start_operator(group_id)

    NodeTestHelper.stop_operator(group_id, operator_id)

    assert Flezha.Operator.FleetManager.active_operators(group_id) === []
  end

  @tag :operator_disconnect_migrate_op
  test "operator disconnect migrate op" do
    group_id = @temp_group
    computation_id_1 = "pipeline_1"
    computation_id_2 = "pipeline_2"

    # Start 2 operators under the same group.
    operator_id_1 = NodeTestHelper.start_operator(group_id)
    operator_id_2 = NodeTestHelper.start_operator(group_id)

    # Start a few pipelines and store some state on them.
    {:ok, %{pid: _pipeline_pid_1, operator_id: pipe1_op}} =
      computation_id_1
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start(group_id)
      |> ComputationFixture.attach_output_msg(group_id)

    {:ok, %{pid: _pipeline_pid_2, operator_id: _pipe2_op}} =
      computation_id_2
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start(group_id)
      |> ComputationFixture.attach_output_msg(group_id)

    ComputationTestHelper.send_sync_io(computation_id_1, group_id, {:text, "state_1"})
    ComputationTestHelper.send_sync_io(computation_id_2, group_id, {:text, "state_2"})

    ComputationManager.create_snapshot(group_id, computation_id_1)
    ComputationManager.create_snapshot(group_id, computation_id_2)

    active_operator = if pipe1_op === operator_id_1, do: operator_id_2, else: operator_id_1

    # Trigger the operator disconnect.
    NodeTestHelper.stop_operator(group_id, pipe1_op)

    # Make sure all the pipelines are migrated to the other operator.
    ComputationManager.pipeline_registry(group_id)
    |> Enum.each(fn {_computation_id, {_cspec, _cnodes, %{tag: new_operator}}} ->
      assert new_operator === active_operator
    end)

    # Make sure the states have been migrated.
    {:ok, json_state_1} = ComputationManager.get_computation_state(group_id, computation_id_1)
    {:ok, json_state_2} = ComputationManager.get_computation_state(group_id, computation_id_2)

    assert json_state_1 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}"
    assert json_state_2 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}"
  end

  @tag :operator_disconnect_preserve
  test "operator disconnect preserve" do
    # Start 2 operators under the same group.
    group_id = @temp_group
    operator_id_1 = NodeTestHelper.start_operator(group_id)
    operator_id_2 = NodeTestHelper.start_operator(group_id)

    # Start a few pipelines and store some state on them.
    computation_id_1 = "pipeline_1"
    computation_id_2 = "pipeline_2"

    {:ok, %{pid: _pipeline_pid_1, operator_id: _pipe1_op}} =
      computation_id_1
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start(group_id)
      |> ComputationFixture.attach_output_msg(group_id)

    {:ok, %{pid: _pipeline_pid_2, operator_id: _pipe2_op}} =
      computation_id_2
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start(group_id)
      |> ComputationFixture.attach_output_msg(group_id)

    ComputationTestHelper.send_sync_io(computation_id_1, group_id, {:text, "state_1"})
    ComputationTestHelper.send_sync_io(computation_id_2, group_id, {:text, "state_2"})

    ComputationManager.create_snapshot(group_id, computation_id_1)
    ComputationManager.create_snapshot(group_id, computation_id_2)

    # Trigger the operator disconnect.
    NodeTestHelper.stop_operator(group_id, operator_id_1)
    NodeTestHelper.stop_operator(group_id, operator_id_2)

    # Start a new operator under the same group.
    operator_id_3 = NodeTestHelper.start_operator(group_id)

    # Make sure all the pipelines are migrated to the new operator.
    ComputationManager.pipeline_registry(group_id)
    |> Enum.each(fn {_computation_id, {_cspec, _cnodes, %{tag: new_operator}}} ->
      assert new_operator === operator_id_3
    end)

    # Make sure the states have been migrated.
    {:ok, json_state_1} = ComputationManager.get_computation_state(group_id, computation_id_1)
    {:ok, json_state_2} = ComputationManager.get_computation_state(group_id, computation_id_2)

    assert json_state_1 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}"
    assert json_state_2 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}"

    # Stop the new operator.
    NodeTestHelper.stop_operator(group_id, operator_id_3)

    # Adjust the operator cleanup interval and trigger the cleanup.
    Application.put_env(:flezha, :operator_keepalive, "0")
    GenServer.call(FleetManager, :operator_cleanup)

    # Reset the operator keepalive to its default value.
    Application.put_env(:flezha, :operator_keepalive, "30000")

    # Make sure the hub is stopped.
    assert HubManager.hub_alive?(group_id) === false
  end

  @tag :operator_disconnect_migrate_hub
  test "operator disconnect migrate hub" do
    group_id = @temp_group
    computation_id_1 = "pipeline_1"
    computation_id_2 = "pipeline_2"

    {_peer_pid, peer_node} = NodeTestHelper.start_peer_node(:anode1)

    # Start the operators.
    operator_id_1 = NodeTestHelper.start_operator(group_id)
    rem_operator_1 = NodeTestHelper.start_operator_remote(peer_node, group_id)
    rem_operator_2 = NodeTestHelper.start_operator_remote(peer_node, group_id)

    self = self()
    _node = node()

    ProcessHub.Service.HookManager.register_handler(
      Identificator.hub(group_id),
      ProcessHub.Constant.Hook.registry_pid_inserted(),
      %ProcessHub.Service.HookManager{
        id: "hub_insert_hook_test",
        m: Process,
        f: :send,
        a: [self, :hook_insert_recv, []]
      }
    )

    ProcessHub.Service.HookManager.register_handler(
      Identificator.hub(group_id),
      ProcessHub.Constant.Hook.process_startups(),
      %ProcessHub.Service.HookManager{
        id: "hub_pstartup_hook_test",
        m: Process,
        f: :send,
        a: [self, :hook_pstartup_recv, []]
      }
    )

    # Start a few pipelines and store some state on them.
    {:ok, %{pid: _pipeline_pid_1, operator_id: pipe1_op}} =
      computation_id_1
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start(group_id)
      |> ComputationFixture.attach_output_msg(group_id)

    {:ok, %{pid: _pipeline_pid_2, operator_id: pipe2_op}} =
      computation_id_2
      |> ComputationFixture.get(@test_store_input_factory)
      |> ComputationFixture.start(group_id)
      |> ComputationFixture.attach_output_msg(group_id)

    # Make sure that atleast one of the pipelines is running on the remote operator.
    assert pipe1_op !== operator_id_1 or pipe2_op !== operator_id_1

    ComputationTestHelper.send_sync_io(computation_id_1, group_id, {:text, "state_1"})
    ComputationTestHelper.send_sync_io(computation_id_2, group_id, {:text, "state_2"})

    # Check the states before operator migrations.
    {:ok, json_state_1} = ComputationManager.get_computation_state(group_id, computation_id_1)
    {:ok, json_state_2} = ComputationManager.get_computation_state(group_id, computation_id_2)
    assert json_state_1 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}"
    assert json_state_2 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}"

    ComputationManager.create_snapshot(group_id, computation_id_1)
    ComputationManager.create_snapshot(group_id, computation_id_2)

    # Trigger the operator disconnect.
    NodeTestHelper.stop_operator_remote(peer_node, group_id, rem_operator_1)
    NodeTestHelper.stop_operator_remote(peer_node, group_id, rem_operator_2)

    # Need to wait 4 times process inserted hook to be called.
    # With each operator disconnect we update the tag of the pipeline.
    # Wait for other hooks as well.
    MessageTestHelper.recv_num(:hook_insert_recv, 4)
    MessageTestHelper.recv_num(:hook_pstartup_recv, 2)

    # Make sure all the pipelines are migrated to the other operator.
    reg = ComputationManager.pipeline_registry(group_id)

    Enum.each(reg, fn {_computation_id, {_cspec, _cnodes, %{tag: new_operator}}} ->
      assert new_operator === operator_id_1
    end)

    # Make sure the states have been migrated.
    {:ok, json_state_1} = ComputationManager.get_computation_state(group_id, computation_id_1)
    {:ok, json_state_2} = ComputationManager.get_computation_state(group_id, computation_id_2)

    assert json_state_1 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_1\"]}"
    assert json_state_2 === "{\"binary_inputs\":[],\"text_inputs\":[\"state_2\"]}"

    assert length(HubManager.get_hub_nodes(group_id, true)) === 1
  end

  # @tag :flow_test
  # test "flow test" do
  #   num_pipelines = 10
  #   test_messages_per_pipeline = 10_000

  #   IO.puts("Starting flow test with #{num_pipelines} pipelines...")

  #   {pipeline_creation_time, pipeline_infos} = :timer.tc(fn ->
  #     for i <- 1..num_pipelines do
  #       computation_id = "flow_test_#{i}"

  #       pipeline = %Flezha.Computation.Builder{
  #         id: computation_id,
  #         factory_id: @test_pipeline_factory
  #       }

  #       case ComputationManager.start_pipeline(@default_group, pipeline) do
  #         {:ok, info} -> {computation_id, info}
  #         error ->
  #           IO.puts("Failed to start pipeline #{computation_id}: #{inspect(error)}")
  #           nil
  #       end
  #     end
  #     |> Enum.filter(&(&1 != nil))
  #   end)

  #   IO.puts("Computation creation took: #{pipeline_creation_time / 1000} ms for #{length(pipeline_infos)} pipelines")

  #   # Step 2: Enable plugins for all pipelines
  #   {plugin_setup_time, _} = :timer.tc(fn ->
  #     Enum.each(pipeline_infos, fn {computation_id, _info} ->
  #       # Enable BeamMsgForward plugin
  #       PluginManager.enable_plugin(%Flezha.Plugin.Provider.BeamMsgForward{
  #         group: @default_group,
  #         computation_id: computation_id,
  #         trigger: :on_output,
  #         recv_pid: self()
  #       })

  #       # Enable StdOut plugin
  #       PluginManager.enable_plugin(%Flezha.Plugin.Provider.StdOut{
  #         group: @default_group,
  #         computation_id: computation_id,
  #         trigger: :proactive
  #       })
  #     end)
  #   end)

  #   IO.puts("Plugin setup took: #{plugin_setup_time / 1000} ms for #{length(pipeline_infos)} pipelines")

  #   # Step 3: Send test messages to all pipelines
  #   total_messages = length(pipeline_infos) * test_messages_per_pipeline
  #   IO.puts("Sending #{total_messages} total messages (#{test_messages_per_pipeline} per pipeline)...")

  #   {message_send_time, _} = :timer.tc(fn ->
  #     Enum.each(pipeline_infos, fn {computation_id, %{pid: pipeline_pid}} ->
  #       for msg_num <- 1..test_messages_per_pipeline do
  #         GenServer.cast(pipeline_pid, {:io_in, {:text, "test_msg_#{computation_id}_#{msg_num}"}, 1})
  #       end
  #     end)
  #   end)

  #   IO.puts("Message sending took: #{message_send_time / 1000} ms")

  #   # Step 4: Receive and measure response time
  #   {message_receive_time, _} = :timer.tc(fn ->
  #     for x <- 1..total_messages do
  #       receive do
  #         {:message_forward, _msg} ->
  #           :ok
  #       after
  #         10_000 ->
  #           IO.puts("Timeout waiting for message #{x}/#{total_messages}")
  #           assert false, "Timeout waiting for messages"
  #       end
  #     end
  #   end)

  #   IO.puts("Message processing took: #{message_receive_time / 1000} ms for #{total_messages} messages")

  #   # Calculate throughput metrics
  #   total_time = pipeline_creation_time + plugin_setup_time + message_send_time + message_receive_time
  #   throughput_msgs_per_sec = (total_messages * 1_000_000) / message_receive_time

  #   IO.puts("\n=== Performance Summary ===")
  #   IO.puts("Computations created: #{length(pipeline_infos)}")
  #   IO.puts("Total messages processed: #{total_messages}")
  #   IO.puts("Computation creation: #{pipeline_creation_time / 1000} ms")
  #   IO.puts("Plugin setup: #{plugin_setup_time / 1000} ms")
  #   IO.puts("Message sending: #{message_send_time / 1000} ms")
  #   IO.puts("Message processing: #{message_receive_time / 1000} ms")
  #   IO.puts("Total time: #{total_time / 1000} ms")
  #   IO.puts("Throughput: #{Float.round(throughput_msgs_per_sec, 2)} messages/second")
  #   IO.puts("Average time per pipeline: #{(total_time / length(pipeline_infos)) / 1000} ms")

  #   assert length(pipeline_infos) > 0, "No pipelines were created successfully"
  #   assert total_messages > 0, "No messages were processed"
  # end
end
