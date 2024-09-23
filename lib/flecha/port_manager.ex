defmodule Flecha.PortManager do
  require Logger

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
    Logger.info("Received data from port: #{inspect(port_msg)}")

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, port, :normal}, state) do
    if state.port_down_counter > 5 do
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
    Logger.info("Port manager received unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp initialize_ports() do
    external_processes = Application.fetch_env!(:flecha_core, :external_processes)

    Enum.map(external_processes, fn command ->
      open_port(command)
    end)
    |> Map.new()
  end

  defp open_port(command) do
    port =
      Port.open(
        {:spawn, command},
        [:binary]
      )

    Port.monitor(port)

    {port, command}
  end

  defp schedule_counter_reset() do
    Process.send_after(self(), :reset_counter, 30_000)
  end
end
