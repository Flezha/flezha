defmodule AflechaTest.Fixtures.Computation do
  use Aflecha.Constant

  def get(id, factory_id) do
    %Aflecha.Computation.Builder{
      id: id || "my_computation",
      factory_id: factory_id || "forward_orig"
    }
  end

  def start(computation) do
    Aflecha.Computation.ComputeManager.start_computation(@default_operator, computation)

    computation
  end

  def stop(computation_id) do
    Aflecha.Computation.ComputeManager.stop_computation(@default_operator, computation_id)
  end

  def attach_input(computation, handler, opts \\ []) do
    {:ok, __id} =
      Aflecha.add_io(
        @default_operator,
        computation.id,
        get_io_handler(handler, :input),
        :input,
        handling_type: Keyword.get(opts, :handling_type, :task)
      )

    computation
  end

  def attach_output(computation, handler) do
    {:ok, _id} =
      Aflecha.add_io(
        @default_operator,
        computation.id,
        get_io_handler(handler, :output),
        :output,
        []
      )

    computation
  end

  defp get_io_handler(handler, type) do
    default_handler =
      case type do
        :input -> default_input_handler()
        :output -> default_output_handler()
      end

    cond do
      is_struct(handler) -> handler
      handler === nil -> default_handler
    end
  end

  defp default_input_handler() do
    %Aflecha.IO.Handler.Websocket{
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
  end

  defp default_output_handler() do
    %Aflecha.IO.Handler.StdIO{}
  end
end
