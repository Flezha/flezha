defmodule Aflecha.Engine.Toolbox do
  alias Aflecha.Engine.EngineSupervisor
  alias Aflecha.Service.Payloader
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator

  require Logger

  def engine_dispatch_fn(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"connect_control_channel", engine_id} | rest] = Payloader.parse_payload(payload)
        control_unit = Identificator.control_unit(engine_id)

        EngineSupervisor.start_link({engine_id, sock})
        SocketSupport.pass_ownership(sock, EngineSupervisor.find_control_channel(engine_id))
        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        GenServer.call(control_unit, :command_sockets_connect)

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive engine id: #{inspect(reason)}")
    end
  end
end
