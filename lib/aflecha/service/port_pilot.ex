defmodule Aflecha.Service.PortPilot do
  require Logger

  # TODO: change back to 5 or make it configurable
  @port_down_counter 1

  use GenServer

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    ports = initialize_ports()

    schedule_counter_reset()

    {:ok, %{ports: ports, response_refs: %{}, port_down_counter: 0}}
  end

  @impl true
  def handle_info(:reset_counter, state) do
    schedule_counter_reset()

    {:noreply, Map.put(state, :port_down_counter, 0)}
  end

  @impl true
  def handle_info({_port, {:data, port_msg}}, state) do
    Logger.warning("Received unexpected message from port: #{inspect(port_msg)}")

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, port, :normal}, state) do
    Logger.error("Port application down")

    if state.port_down_counter > @port_down_counter do
      raise "Port application malfunction"
    end

    ports = Map.delete(state.ports, port)

    ports =
      case Map.get(state.ports, port) do
        nil ->
          ports

        command ->
          {port, _} = open_port(command)
          Map.put(ports, port, command)
      end

    new_state =
      state
      |> Map.put(:ports, ports)
      |> Map.put(:port_down_counter, Map.get(state, :port_down_counter, 0) + 1)

    {:noreply, new_state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.warning("Port manager received unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp initialize_ports() do
    external_processes = Application.fetch_env!(:aflecha, :external_processes)

    Enum.map(external_processes, fn command ->
      open_port(command)
    end)
    |> Map.new()
  end

  defp open_port(command) do
    socket_files_path = Application.fetch_env!(:aflecha, :socket_files_path)

    port =
      Port.open(
        {:spawn, command},
        [
          :binary,
          {:env, [{~c"AFLECHA_SOCKET_FILES_PATH", String.to_charlist(socket_files_path)}]}
        ]
      )

    Port.monitor(port)

    {port, command}
  end

  defp schedule_counter_reset() do
    Process.send_after(self(), :reset_counter, 30_000)
  end
end
