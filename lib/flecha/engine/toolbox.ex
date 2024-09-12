defmodule Flecha.Engine.Toolbox do
  alias Flecha.Engine.EngineSupervisor
  alias Flecha.Engine.ControlChannel

  require Logger

  def engine_dispatch_fn(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, engine_id_string} ->
        engine_id = String.to_atom(engine_id_string)

        EngineSupervisor.start_link({engine_id, sock})
        ControlChannel.command_sockets_connect(engine_id)

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive engine id: #{inspect(reason)}")
    end
  end
end
