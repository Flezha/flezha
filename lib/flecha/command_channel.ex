defmodule Flecha.CommandChannel do
  require Logger

  alias Flecha.Service.Payloader

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    socket_path = "app_data/sockets/command.sock"
    opts = [:binary, active: true, reuseaddr: true]

    {:ok, port} = :gen_tcp.connect({:local, socket_path}, 0, opts)

    {:ok, %{socket: port, computation_mapping: %{}}}
  end

  @impl true
  def handle_info({:tcp, _port, _payload}, state) do
    # TODO: implement

    {:noreply, state}
  end

  # TODO: handle this correctly.
  @impl true
  def handle_info({:tcp_closed, _port}, state) do
    IO.puts("Channel closed")

    {:noreply, state}
  end
end
