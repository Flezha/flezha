defmodule Flecha.Engine.Toolbox do
  alias Flecha.Engine.ControlUnit
  alias Flecha.Engine.EngineSupervisor
  alias Flecha.Service.Payloader
  alias Flecha.Utility.Identifier

  require Logger

  def engine_dispatch_fn(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"0", engine_id_string} | rest] = Payloader.parse_payload(payload)

        engine_id = String.to_atom(engine_id_string)
        control_unit = Identifier.control_unit(engine_id)

        EngineSupervisor.start_link({engine_id, sock})
        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        ControlUnit.command_sockets_connect(engine_id)

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive engine id: #{inspect(reason)}")
    end
  end
end
