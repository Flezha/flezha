defmodule AflechaTest do
  alias Aflecha.Service.WorkerManager

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

    {:ok, {pipeline_id, [{@node_name, _pid}]}} =
      WorkerManager.start_pipeline(@default_group, pipeline)

    assert pipeline_id === "default_pipeline_test"
  end

  # test "websocket plugin connection start" do
  #   pipeline_id = "websocket_plugin_test"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: @test_pipeline_factory
  #   }

  #   {:ok, _} = WorkerManager.start_pipeline(@default_group, pipeline)

  #   # TODO: add assertions and test websocket connection.
  #   Aflecha.Service.WorkerManager.add_binding(
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
  # end

  # test "beam msg forward plugin connection start" do
  #   pipeline_id = "beam_msg_forw_plugin_test"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: @test_pipeline_factory
  #   }

  #   {:ok, _} = WorkerManager.start_pipeline(@default_group, pipeline)

  #   # TODO: add assertions.
  #   Aflecha.Service.WorkerManager.add_binding(
  #     @default_group,
  #     pipeline_id,
  #     %Aflecha.Plugin.Handler.BeamMsgForward{
  #       recv_pid: self()
  #     },
  #     :on_output
  #   )
  # end

  # test "pipeline flow generation -> beam msg forward" do
  #   pipeline_id = "data_flow_test"

  #   pipeline = %Aflecha.Pipeline.Builder{
  #     id: pipeline_id,
  #     factory_id: @test_compare_filter_factory,
  #     variables: %{"comparable" => "hello world", "inverted" => false}
  #   }

  #   {:ok, _} = WorkerManager.start_pipeline(@default_group, pipeline)

  #   Aflecha.Service.WorkerManager.add_binding(
  #     @default_group,
  #     pipeline_id,
  #     %Aflecha.Plugin.Handler.BeamMsgForward{
  #       recv_pid: self()
  #     },
  #     :on_output
  #   )

  #   # TODO: add STDIO test
  #   # Aflecha.add_io(
  #   #   @default_group,
  #   #   pipeline.id,
  #   #   %Aflecha.IO.Handler.StdIO{},
  #   #   :output,
  #   #   []
  #   # )

  #   Aflecha.Service.WorkerManager.add_binding(
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
  #       _msg ->
  #         assert true
  #     after
  #       2000 -> assert false
  #     end
  #   end
  # end
end
