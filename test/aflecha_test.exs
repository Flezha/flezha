defmodule AflechaTest do
  alias Aflecha.Service.PipelineManager
  alias AflechaTest.Fixtures.PipelineFixture
  alias Aflecha.Service.PluginManager

  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha

  @default_group "default"
  @node_name TestHelper.node_name()

  @test_store_input_factory "store_input"
  @test_pipeline_factory "do_nothing"
  @test_compare_filter_factory "compare_filter"

  setup_all do
    %{}
  end

  setup do
    on_exit(fn ->
      nil
    end)

    :ok
  end

  test "operator node connect" do
    res = Aflecha.Operator.FleetManager.get_operators(@default_group)
    assert length(res) === 2
    Enum.each(res, &assert(is_binary(&1)))
  end

  test "pipeline adding" do
    pipeline = %Aflecha.Pipeline.Builder{
      id: "default_pipeline_test",
      factory_id: @test_pipeline_factory
    }

    {status, info} = PipelineManager.create_pipeline(@default_group, pipeline)

    assert status === :ok
    assert info[:pipeline_id] === "default_pipeline_test"
    assert info[:node] === @node_name
    assert is_pid(info[:pid])
    assert is_binary(info[:operator_id])
  end

  test "pipeline removing" do
    pipeline_id = "default_pipeline_rem_test"

    {:ok, %{pid: _pipeline_pid}} =
      pipeline_id
      |> PipelineFixture.get(@test_store_input_factory)
      |> PipelineFixture.start()

    assert {:ok, :pipeline_removed} === PipelineManager.rem_pipeline(@default_group, pipeline_id)

    # TODO: add hook instead to listen to process removal.
    Process.sleep(100)

    assert PipelineManager.exist?(@default_group, pipeline_id) === false
  end

  test "pipeline adding non existing factory" do
    pipeline_id = "non_existing_factory_err"

    pipeline = %Aflecha.Pipeline.Builder{
      id: pipeline_id,
      factory_id: "non_existing_factory"
    }

    {status, info} = PipelineManager.create_pipeline(@default_group, pipeline)

    assert status === :error
    assert info[:pipeline_id] === pipeline_id
    assert info[:node] === @node_name
    assert info[:reason] === "Factory with id non_existing_factory does not exist"
  end

  test "websocket plugin connection start" do
    pipeline_id = "websocket_plugin_test"

    pipeline = %Aflecha.Pipeline.Builder{
      id: pipeline_id,
      factory_id: @test_pipeline_factory
    }

    {:ok, _} = PipelineManager.create_pipeline(@default_group, pipeline)

    # TODO: add failures also.
    res =
      PluginManager.enable_plugin(%Aflecha.Plugin.Provider.Websocket{
        group: @default_group,
        pipeline_id: pipeline_id,
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
    pipeline_id = "beam_msg_forw_plugin_test"

    pipeline = %Aflecha.Pipeline.Builder{
      id: pipeline_id,
      factory_id: @test_pipeline_factory
    }

    {:ok, _} = PipelineManager.create_pipeline(@default_group, pipeline)

    res =
      PluginManager.enable_plugin(%Aflecha.Plugin.Provider.BeamMsgForward{
        group: @default_group,
        pipeline_id: pipeline_id,
        trigger: :on_output,
        recv_pid: self()
      })

    assert res === {:ok, "binding added"}
  end

  test "pipeline flow generation -> beam msg forward" do
    pipeline_id = "data_flow_test"

    pipeline = %Aflecha.Pipeline.Builder{
      id: pipeline_id,
      factory_id: @test_compare_filter_factory,
      variables: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, _} = PipelineManager.create_pipeline(@default_group, pipeline)

    PluginManager.enable_plugin(%Aflecha.Plugin.Provider.BeamMsgForward{
      group: @default_group,
      pipeline_id: pipeline_id,
      trigger: :on_output,
      recv_pid: self()
    })

    PluginManager.enable_plugin(%Aflecha.Plugin.Provider.Websocket{
      group: @default_group,
      pipeline_id: pipeline_id,
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
        msg ->
          assert msg === {:text, "hello world"}
      after
        2000 -> assert false
      end
    end
  end

  test "get pipeline state" do
    pipeline_id = "get_state_test"

    {:ok, %{pid: pipeline_pid}} =
      pipeline_id
      |> PipelineFixture.get(@test_store_input_factory)
      |> PipelineFixture.start()

    GenServer.call(pipeline_pid, {:io_in, {:text, "state_1"}})
    GenServer.call(pipeline_pid, {:io_in, {:text, "state_2"}})

    {:ok, json_state} = PipelineManager.get_pipeline_state(@default_group, pipeline_id)
    pipeline_state = Aflecha.Service.Payloader.json_to_map(json_state)

    assert pipeline_state === %{
             "binary_inputs" => [],
             "text_inputs" => ["state_1", "state_2"]
           }
  end

  # TODO: add test to handle large control requests and stream requests.

  test "pipeline migration" do
    # TODO: we should spawn a new elixir node, wait till the process gets migrated there(using dynamic hooks) and
    # then get the state of the new migrated process to see if it matches the state1 string.

    {peer_pid, peer_node} = TestHelper.start_peer_node(:anode1)

    pipeline_id = "pipeline_migration_test"

    {:ok, %{pid: pipeline_pid, node: _pnode}} =
      pipeline_id
      |> PipelineFixture.get(@test_store_input_factory)
      |> PipelineFixture.start()

    GenServer.cast(pipeline_pid, {:io_in, {:text, "test_state_1"}})
    GenServer.cast(pipeline_pid, {:io_in, {:text, "test_state_2"}})
    GenServer.call(pipeline_pid, :ping)

    test_state = %{
      "binary_inputs" => [],
      "text_inputs" => ["test_state_1", "test_state_2"]
    }

    # TODO: add here hook instead
    Process.sleep(100)

    {:ok, json_state} = PipelineManager.get_pipeline_state(@default_group, pipeline_id)
    pipeline_state = Aflecha.Service.Payloader.json_to_map(json_state)

    assert pipeline_state === test_state

    TestHelper.stop_peer_node(peer_pid, peer_node)

    # TODO: add hook here instead
    Process.sleep(1000)

    {:ok, json_state} = PipelineManager.get_pipeline_state(@default_group, pipeline_id)
    pipeline_state = Aflecha.Service.Payloader.json_to_map(json_state)

    assert pipeline_state === test_state
  end
end
