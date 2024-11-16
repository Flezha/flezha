defmodule Aflecha.Operator.Toolbox do
  alias Aflecha.Operator.OperatorUnitSup
  alias Aflecha.Operator.OperatorsSup
  alias Aflecha.Service.Payloader
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator

  require Logger

  def op_conn_fn(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"connect_control_channel", group_id} | rest] = Payloader.parse_payload(payload)
        control_unit = Identificator.control_unit(group_id)

        OperatorsSup.start_operator(group_id, sock)

        SocketSupport.pass_ownership(
          sock,
          OperatorUnitSup.find_control_channel(group_id)
        )

        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        GenServer.call(control_unit, :command_sockets_connect)

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive operator id: #{inspect(reason)}")
    end
  end
end
