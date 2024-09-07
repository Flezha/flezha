defmodule Flecha.CommandChannel do
  alias Flecha.CommandChannel
  alias Flecha.EngineManager

  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: CommandChannel)
  end

  @impl true
  def init(_) do
    # TODO: this should be coming from the configuration.
    socket_path = "app_data/sockets/command_default.sock"

    # TODO: move to a separate service to handle sockets

    # Clean up socket file if it already exists
    if File.exists?(socket_path) do
      File.rm(socket_path)
    end

    opts = [
      :binary,
      :local,
      {:active, true},
      {:ifaddr, {:local, socket_path}}
    ]

    {:ok, port} = :gen_tcp.listen(0, opts)

    {:ok, %{port: port, socket_connection: nil}, {:continue, :accept_connection}}
  end

  @impl true
  def handle_continue(:accept_connection, state) do
    {:ok, socket_connection} = :gen_tcp.accept(state[:port])

    {:noreply, %{state | socket_connection: socket_connection}}
  end

  @impl true
  def handle_call({:send_command, payload}, _from, state) do
    # Logger.info("Sending command: #{inspect(payload)}")
    :ok = :gen_tcp.send(state[:socket_connection], payload)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    GenServer.cast(EngineManager, {:handle_payload, payload})

    {:noreply, state}
  end

  # TODO: handle this correctly.
  @impl true
  def handle_info({:tcp_closed, _port}, state) do
    IO.puts("Channel closed")

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Unexpected message at command channel: #{inspect(msg)}")

    {:noreply, state}
  end
end
