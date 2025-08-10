defmodule FlezhaTest.Fixtures.ComputationFixture do
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Identificator
  alias ProcessHub.Constant.Hook

  use Flezha.Constant

  # TODO: refactor based on new plugin system.

  def get(id, factory_id) do
    %Flezha.Computation.Builder{
      id: id || "my_computation",
      factory_id: factory_id || "do_nothing"
    }
  end

  def start(computation, namespace \\ @default_namespace) do
    self = self()

    ProcessHub.Service.HookManager.register_handler(
      Identificator.hub(namespace),
      Hook.registry_pid_inserted(),
      %ProcessHub.Service.HookManager{
        id: "registry_pid_inserted",
        m: Process,
        f: :send,
        a: [self, :hook_reg_insert_recv, []]
      }
    )

    result = Flezha.Service.ComputationManager.start_computation(namespace, computation)

    receive do
      :hook_reg_insert_recv -> :ok
    after
      2000 -> raise "Timeout waiting for process registration"
    end

    ProcessHub.Service.HookManager.cancel_handler(
      Identificator.hub(namespace),
      Hook.registry_pid_inserted(),
      "registry_pid_inserted"
    )

    result
  end

  def stop(computation_id, namespace \\ @default_namespace) do
    Flezha.Service.ComputationManager.stop_computation(namespace, computation_id)
  end

  def attach_output_msg(
        {:ok, %{computation_id: cid}} = start_res,
        namespace \\ @default_namespace
      ) do
    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin("output_msg_handler", %Flezha.Plugin.Driver.IOBindingPlugin{
        namespace: namespace,
        computation_id: cid,
        trigger: :on_output,
        provider: %Flezha.Plugin.Provider.BeamMsgForward{
          recv_pid: self()
        },
        plugin_group: "0"
      })

    start_res
  end

  # TODO: refactor.
  # def attach_input(computation, handler, opts \\ []) do
  #   {:ok, __id} =
  #     Flezha.add_io(
  #       @default_namespace,
  #       computation.id,
  #       get_io_handler(handler, :input),
  #       :input,
  #       handling_type: Keyword.get(opts, :handling_type, :task)
  #     )

  #   computation
  # end

  # def attach_output(computation, handler) do
  #   {:ok, _id} =
  #     Flezha.add_io(
  #       @default_namespace,
  #       computation.id,
  #       get_io_handler(handler, :output),
  #       :output,
  #       []
  #     )

  #   computation
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
  #   %Flezha.IO.Handler.Websocket{
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
  #   %Flezha.IO.Handler.StdIO{}
  # end
end
