defmodule AflechaTest do
  alias Aflecha.Service.WorkerManager

  use ExUnit.Case
  use Aflecha.Constant

  doctest Aflecha

  @default_group "default"
  @node_name TestHelper.node_name()

  # @test_forward_orig_factory "forward_orig"
  # @test_store_input_factory "store_input"
  @test_worker_factory "do_nothing"
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

  # TODO: uncomment these tests and refactor, use fixtures.
  # test "operator node connect" do
  #   res = Aflecha.Operator.FleetManager.get_operators(@default_group)
  #   assert length(res) === 2
  #   Enum.each(res, &assert(is_binary(&1)))
  # end

  # test "worker adding" do
  #   worker = %Aflecha.Worker.Builder{
  #     id: "default_worker_test",
  #     factory_id: @test_worker_factory
  #   }

  #   {:ok, {worker_id, [{@node_name, _pid}]}} =
  #     WorkerManager.start_worker(@default_group, worker)

  #   assert worker_id === "default_worker_test"
  # end

  # test "kafka plugin connection start" do
  #   worker_id = "kafka_plugin_test"

  #   worker = %Aflecha.Worker.Builder{
  #     id: worker_id,
  #     factory_id: @test_worker_factory
  #   }

  #   {:ok, _} = WorkerManager.start_worker(@default_group, worker)

  #   # TODO: add assertion.
  #   Aflecha.Service.WorkerManager.add_binding(
  #     @default_group,
  #     worker_id,
  #     :reactive,
  #     %Aflecha.Plugin.Handler.Kafka.Producer{
  #       hosts: [{"kafka", 9092}],
  #       topic: "aflecha_test_topic",
  #       partition: 0
  #     }
  #   )

  #   {:ok, pid} = Aflecha.HubManager.get_pid(@default_group, worker_id)
  #   GenServer.cast(pid, {:io_out, "my test text", :text})

  #   Process.sleep(1000)
  # end

  # test "websocket plugin connection start" do
  #   worker_id = "websocket_plugin_test"

  #   worker = %Aflecha.Worker.Builder{
  #     id: worker_id,
  #     factory_id: @test_worker_factory
  #   }

  #   {:ok, _} = WorkerManager.start_worker(@default_group, worker)

  #   # TODO: add assertions and test websocket connection.
  #   Aflecha.Service.WorkerManager.add_binding(
  #     @default_group,
  #     worker_id,
  #     :proactive,
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
  #     }
  #   )
  # end

  # test "beam msg forward plugin connection start" do
  #   worker_id = "beam_msg_forw_plugin_test"

  #   worker = %Aflecha.Worker.Builder{
  #     id: worker_id,
  #     factory_id: @test_worker_factory
  #   }

  #   {:ok, _} = WorkerManager.start_worker(@default_group, worker)

  #   # TODO: add assertions.
  #   Aflecha.Service.WorkerManager.add_binding(
  #     @default_group,
  #     worker_id,
  #     :reactive,
  #     %Aflecha.Plugin.Handler.BeamMsgForward{
  #       recv_pid: self()
  #     }
  #   )
  # end

  test "worker flow generation -> beam msg forward" do
    worker_id = "data_flow_test"

    worker = %Aflecha.Worker.Builder{
      id: worker_id,
      factory_id: @test_compare_filter_factory,
      variables: %{"comparable" => "hello world", "inverted" => false}
    }

    {:ok, _} = WorkerManager.start_worker(@default_group, worker)

    Aflecha.Service.WorkerManager.add_binding(
      @default_group,
      worker_id,
      :reactive,
      %Aflecha.Plugin.Handler.BeamMsgForward{
        recv_pid: self()
      }
    )

    # TODO: add STDIO test
    # Aflecha.add_io(
    #   @default_group,
    #   worker.id,
    #   %Aflecha.IO.Handler.StdIO{},
    #   :output,
    #   []
    # )

    Aflecha.Service.WorkerManager.add_binding(
      @default_group,
      worker_id,
      :proactive,
      %Aflecha.Plugin.Handler.Websocket{
        uri: @testnet_ws_uri,
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
      }
    )

    for _ <- 1..1 do
      receive do
        _msg ->
          assert true
      after
        2000 -> assert false
      end
    end
  end
end
