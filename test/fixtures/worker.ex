defmodule AflechaTest.Fixtures.Worker do
  use Aflecha.Constant

  # TODO: refactor based on new plugin system.

  def get(id, factory_id) do
    %Aflecha.Worker.Builder{
      id: id || "my_worker",
      factory_id: factory_id || "forward_orig"
    }
  end

  def start(worker) do
    Aflecha.Service.WorkerManager.start_worker(@default_operator, worker)

    worker
  end

  def stop(worker_id) do
    Aflecha.Service.WorkerManager.stop_worker(@default_operator, worker_id)
  end

  # TODO: refactor.
  # def attach_input(worker, handler, opts \\ []) do
  #   {:ok, __id} =
  #     Aflecha.add_io(
  #       @default_operator,
  #       worker.id,
  #       get_io_handler(handler, :input),
  #       :input,
  #       handling_type: Keyword.get(opts, :handling_type, :task)
  #     )

  #   worker
  # end

  # def attach_output(worker, handler) do
  #   {:ok, _id} =
  #     Aflecha.add_io(
  #       @default_operator,
  #       worker.id,
  #       get_io_handler(handler, :output),
  #       :output,
  #       []
  #     )

  #   worker
  # end

  # defp get_io_handler(handler, type) do
  #   default_handler =
  #     case type do
  #       :input -> default_input_handler()
  #       :output -> default_output_handler()
  #     end

  #   cond do
  #     is_struct(handler) -> handler
  #     handler === nil -> default_handler
  #   end
  # end

  # defp default_input_handler() do
  #   %Aflecha.IO.Handler.Websocket{
  #     uri: @testnet_ws_uri,
  #     post_connect: %{
  #       payload: {
  #         :text,
  #         ~s(
  #             {
  #               "event_type": "subscribe",
  #               "event_data": {
  #                 "default_message": "hello world",
  #                 "interval": 1000
  #               }
  #             }
  #           )
  #       }
  #     }
  #   }
  # end

  # defp default_output_handler() do
  #   %Aflecha.IO.Handler.StdIO{}
  # end
end
