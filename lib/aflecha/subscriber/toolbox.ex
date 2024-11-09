defmodule Aflecha.Subscriber.Toolbox do
  alias Aflecha.Subscriber.SubscriberSupervisor
  alias Aflecha.Service.Payloader
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator

  require Logger

  def sub_conn_fn(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"connect_control_channel", subscriber_id} | rest] = Payloader.parse_payload(payload)
        control_unit = Identificator.control_unit(subscriber_id)

        SubscriberSupervisor.start_link({subscriber_id, sock})

        SocketSupport.pass_ownership(
          sock,
          SubscriberSupervisor.find_control_channel(subscriber_id)
        )

        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        GenServer.call(control_unit, :command_sockets_connect)

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive engine id: #{inspect(reason)}")
    end
  end
end
