defmodule FlezhaTest.Fixtures.RelayFixture do
  alias Flezha.Service.PluginManager
  alias Flezha.Service.Identificator
  alias ProcessHub.Constant.Hook

  use Flezha.Constant

  # TODO: refactor based on new plugin system.

  def get(id, factory_id) do
    %Flezha.Relay.Builder{
      id: id || "my_relay",
      factory_id: factory_id || "do_nothing"
    }
  end

  def start(relay, namespace \\ @default_namespace) do
    self = self()

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %ProcessHub.Service.HookManager{
          id: "child_registered",
          m: Process,
          f: :send,
          a: [self, :hook_reg_insert_recv, []]
        }
      ]
    )

    result = Flezha.Service.RelayManager.start_relay(namespace, relay)

    receive do
      :hook_reg_insert_recv -> :ok
    after
      2000 -> raise "Timeout waiting for process registration"
    end

    ProcessHub.cancel_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      ["child_registered"]
    )

    result
  end

  def stop(relay_id, namespace \\ @default_namespace) do
    Flezha.Service.RelayManager.stop_relay(namespace, relay_id)
  end

  def attach_output_msg(
        {:ok, %{relay_id: cid}} = start_res,
        namespace \\ @default_namespace
      ) do
    plugin_id = cid <> "_output_msg_handler"

    {:ok, "plugin enabled"} =
      PluginManager.enable_plugin(
        plugin_id,
        %Flezha.Plugin{
          id: plugin_id,
          namespace: namespace,
          relay_id: cid,
          provider: Flezha.Plugin.Provider.BeamMsgForward,
          opts: %{recv_pid: self()},
          subscriptions: [{:on_output, []}]
        }
      )

    start_res
  end

  # TODO: refactor.
  # def attach_input(relay, handler, opts \\ []) do
  #   {:ok, __id} =
  #     Flezha.add_io(
  #       @default_namespace,
  #       relay.id,
  #       get_io_handler(handler, :input),
  #       :input,
  #       handling_type: Keyword.get(opts, :handling_type, :task)
  #     )

  #   relay
  # end

  # def attach_output(relay, handler) do
  #   {:ok, _id} =
  #     Flezha.add_io(
  #       @default_namespace,
  #       relay.id,
  #       get_io_handler(handler, :output),
  #       :output,
  #       []
  #     )

  #   relay
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
