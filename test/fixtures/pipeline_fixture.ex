defmodule AflechaTest.Fixtures.PipelineFixture do
  use Aflecha.Constant

  # TODO: refactor based on new plugin system.

  def get(id, factory_id) do
    %Aflecha.Pipeline.Builder{
      id: id || "my_pipeline",
      factory_id: factory_id || "do_nothing"
    }
  end

  def start(pipeline) do
    Aflecha.Service.PipelineManager.create_pipeline(@default_operator, pipeline)
  end

  def stop(pipeline_id) do
    Aflecha.Service.PipelineManager.rem_pipeline(@default_operator, pipeline_id)
  end

  # TODO: refactor.
  # def attach_input(pipeline, handler, opts \\ []) do
  #   {:ok, __id} =
  #     Aflecha.add_io(
  #       @default_operator,
  #       pipeline.id,
  #       get_io_handler(handler, :input),
  #       :input,
  #       handling_type: Keyword.get(opts, :handling_type, :task)
  #     )

  #   pipeline
  # end

  # def attach_output(pipeline, handler) do
  #   {:ok, _id} =
  #     Aflecha.add_io(
  #       @default_operator,
  #       pipeline.id,
  #       get_io_handler(handler, :output),
  #       :output,
  #       []
  #     )

  #   pipeline
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
