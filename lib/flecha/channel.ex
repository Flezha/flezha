defmodule Flecha.Channel do
  require Logger

  alias Flecha.EngineManager
  alias Flecha.Service.Payloader

  use GenServer

  def start_link(name) do
    GenServer.start_link(__MODULE__, nil, name: name)
  end

  @impl true
  def init(_) do
    id = to_string(:erlang.pid_to_list(self()))

    socket_path = "app_data/sockets/#{id}.sock"

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

    # TODO: we are no longer opening socket from sdk but connecting to it, rename accordingly.
    GenServer.call(EngineManager, {:open_socket, id})

    {:ok, %{port: port, computation_mapping: %{}, socket_connection: nil},
     {:continue, :accept_connection}}
  end

  @impl true
  def handle_continue(:accept_connection, state) do
    {:ok, socket_connection} = :gen_tcp.accept(state[:port])

    {:noreply, %{state | socket_connection: socket_connection}}
  end

  @impl true
  def handle_cast({:io_in, _type, computation_id, message}, %{socket_connection: sc} = state) do
    :gen_tcp.send(sc, Payloader.build_payload(computation_id, message))

    {:noreply, state}
  end

  @impl true
  def handle_cast({:computation_pid_update, computation_id, updated_pids}, state) do
    computation_mapping = Map.put(state[:computation_mapping], computation_id, updated_pids)

    {:noreply, Map.put(state, :computation_mapping, computation_mapping)}
  end

  @impl true
  def handle_info({:tcp, _port, payload}, state) do
    compute_mappings = state[:computation_mapping]

    Payloader.parse_payload(payload)
    |> Enum.each(fn {computation_id, message} ->
      computations = Map.get(compute_mappings, computation_id)

      Enum.each(computations, fn pid -> GenServer.cast(pid, {:io_out, :text, message}) end)
    end)

    {:noreply, state}
  end

  # TODO: handle this.
  @impl true
  def handle_info({:tcp_closed, _port}, state) do
    {:noreply, state}
  end
end
