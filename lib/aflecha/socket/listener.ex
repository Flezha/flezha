defmodule Aflecha.Socket.Listener do
  require Logger

  use GenServer

  def start_link({name, sock_path_resolver}) do
    GenServer.start_link(__MODULE__, sock_path_resolver, name: name)
  end

  @impl true
  def init(sock_path_resolver) do
    sock_path = sock_path_resolver.()

    opts = socket_opts(sock_path)
    _res = socket_cleanup(sock_path)

    {:ok, socket} = socket_listen(opts)

    {:ok, %{socket: socket}}
  end

  @impl true
  def handle_call(:get_socket, _from, state) do
    {:reply, state[:socket], state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at listener channel: #{inspect(msg)}")

    {:noreply, state}
  end

  defp socket_listen(opts) do
    case :gen_tcp.listen(0, opts) do
      {:ok, port} ->
        {:ok, port}

      {:error, reason} ->
        Logger.error("Failed to listen on socket: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp socket_opts(sock_path) do
    [
      :binary,
      :local,
      {:active, false},
      {:ifaddr, {:local, sock_path}}
    ]
  end

  defp socket_cleanup(sock_path) do
    if File.exists?(sock_path) do
      File.rm(sock_path)
      true
    else
      false
    end
  end
end
