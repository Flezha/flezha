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

    # Clean up socket file if it already exists
    if File.exists?(socket_path) do
      File.rm(socket_path)
    end

    # TODO: move to a separate service
    opts = [
      :binary,
      {:packet, 0},
      {:active, false},
      {:reuseaddr, true},
      {:ip, {:local, socket_path}}
    ]

    {:ok, socket} = :gen_tcp.listen(0, opts)

    {:ok, %{socket: socket, computation_mapping: %{}}}
  end

  @impl true
  def handle_call({:send_command, payload}, _from, state) do
    Logger.info("Sending command: #{inspect(payload)}")

    :gen_tcp.send(state.socket, payload)

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
end
