defmodule AflechaTest do
  alias Aflecha.Service.PipelineManager
  alias Aflecha.HubManager
  alias AflechaTest.Fixtures.PipelineFixture

  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha

  @default_group "default"
  @node_name TestHelper.node_name()

  # @test_forward_orig_factory "forward_orig"
  # @test_store_input_factory "store_input"
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

  # test "operator node connect" do
  #   res = Aflecha.Operator.FleetManager.get_operators(@default_group)
  #   assert length(res) === 2
  #   Enum.each(res, &assert(is_binary(&1)))
  # end

  # test "pipeline adding" do
  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: "default_pipeline_test",
  #     factory_id: @test_pipeline_factory
  #   }

  #   {status, info} = PipelineManager.create_pipeline(@default_group, pipeline)

  #   assert status === :ok
  #   assert info[:pipeline_id] === "default_pipeline_test"
  #   assert info[:node] === @node_name
  #   assert is_pid(info[:pid])
  #   assert is_binary(info[:operator_id])
  # end

  # test "pipeline adding non existing factory" do
  #   pipeline_id = "non_existing_factory_err"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: "non_existing_factory"
  #   }

  #   {status, info} = PipelineManager.create_pipeline(@default_group, pipeline)

  #   assert status === :error
  #   assert info[:pipeline_id] === pipeline_id
  #   assert info[:node] === @node_name
  #   assert info[:reason] === "Factory with id non_existing_factory does not exist"
  # end

  # test "websocket plugin connection start" do
  #   pipeline_id = "websocket_plugin_test"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: @test_pipeline_factory
  #   }

  #   {:ok, _} = PipelineManager.create_pipeline(@default_group, pipeline)

  #   # TODO: add failures also.
  #   res = Aflecha.Service.PipelineManager.add_binding(
  #     @default_group,
  #     pipeline_id,
  #     %Aflecha.Plugin.Handler.Websocket{
  #       uri: @testnet_ws_uri,
  #       post_connect: %{
  #         payload: {
  #           :text,
  #           ~s(
  #               {
  #                 "event_type": "subscribe",
  #                 "event_data": {
  #                   "default_message": "hello world",
  #                   "interval": 1000
  #                 }
  #               }
  #             )
  #         }
  #       }
  #     },
  #     :proactive
  #   )

  #   assert res === :ok
  # end

  # test "beam msg forward plugin connection start" do
  #   pipeline_id = "beam_msg_forw_plugin_test"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: @test_pipeline_factory
  #   }

  #   {:ok, _} = PipelineManager.create_pipeline(@default_group, pipeline)

  #   res = Aflecha.Service.PipelineManager.add_binding(
  #     @default_group,
  #     pipeline_id,
  #     %Aflecha.Plugin.Handler.BeamMsgForward{
  #       recv_pid: self()
  #     },
  #     :on_output
  #   )

  #   assert res === :ok
  # end

  # test "pipeline flow generation -> beam msg forward" do
  #   pipeline_id = "data_flow_test"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: @test_compare_filter_factory,
  #     variables: %{"comparable" => "hello world", "inverted" => false}
  #   }

  #   {:ok, _} = PipelineManager.create_pipeline(@default_group, pipeline)

  #   Aflecha.Service.PipelineManager.add_binding(
  #     @default_group,
  #     pipeline_id,
  #     %Aflecha.Plugin.Handler.BeamMsgForward{
  #       recv_pid: self()
  #     },
  #     :on_output
  #   )

  #   Aflecha.Service.PipelineManager.add_binding(
  #     @default_group,
  #     pipeline_id,
  #     %Aflecha.Plugin.Handler.Websocket{
  #       uri: @testnet_ws_uri,
  #       post_connect: %{
  #         payload: {
  #           :text,
  #           ~s(
  #               {
  #                 "event_type": "subscribe",
  #                 "event_data": {
  #                   "default_message": "hello world",
  #                   "interval": 1000
  #                 }
  #               }
  #             )
  #         }
  #       }
  #     },
  #     :proactive
  #   )

  #   for _ <- 1..1 do
  #     receive do
  #       msg ->
  #         assert msg === "hello world"
  #     after
  #       2000 -> assert false
  #     end
  #   end
  # end

  test "Pipeline migration" do
    pipeline =
      PipelineFixture.get("Migration pipeline", "store_input") |> PipelineFixture.start() |> dbg()

    {:ok, pipeline_pid} = HubManager.get_pid(@default_group, pipeline.id)

    GenServer.cast(pipeline_pid, {:io_in, "state1"})

    # TODO: we should spawn a new elixir node, wait till the process gets migrated there(using dynamic hooks) and
    # then get the state of the new migrated process to see if it matches the state1 string.

    assert false
  end
end
