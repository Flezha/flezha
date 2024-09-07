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

    GenServer.call(EngineManager, {:open_socket, id})

    socket_path = "app_data/sockets/#{id}.sock"

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
  def handle_cast({:io_in, _type, computation_id, message}, %{socket: socket} = state) do
    :gen_tcp.send(socket, Payloader.build_payload(computation_id, message))

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

  # TODO: handle this correctly.
  @impl true
  def handle_info({:tcp_closed, _port}, state) do
    IO.puts("Channel closed")

    {:noreply, state}
  end
end
