defmodule Flecha.PortManager do
  require Logger

  use GenServer

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    ports = initialize_ports()

    {:ok, %{ports: ports, response_refs: %{}}}
  end

  @impl true
  def handle_info({_port, {:data, port_msg}}, state) do
    Logger.info("Received data from port: #{inspect(port_msg)}")

    {:noreply, state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :port, _port, :normal}, state) do
    # TODO: Try restarting the port process but do not try indefinetly.

    {:noreply, state}
  end

  @impl true
  def handle_info(msg, state) do
    Logger.info("Received unexpected message: #{inspect(msg)}")

    {:noreply, state}
  end

  defp initialize_ports() do
    external_processes = Application.fetch_env!(:flecha_core, :external_processes)

    Enum.map(external_processes, fn command ->
      port =
        Port.open(
          {:spawn, command},
          [:binary]
        )

      Port.monitor(port)

      port
    end)
  end

  # TODO: move to somewhere else this logic.
  # @impl true
  # def handle_call({:add_computation, computation}, from, state) do
  #   ref =
  #     send_command(
  #       @payload_id_computation_new,
  #       %{
  #         "computation_id" => computation.id,
  #         "template_id" => computation.template_id,
  #         "variables" => computation.variables
  #       }
  #     )

  #   {:noreply, Map.put(state, :response_refs, Map.put(state.response_refs, ref, from))}
  # end
end
