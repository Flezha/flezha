defmodule Aflecha.Operator.FleetManager do
  alias Aflecha.Operator.OperatorSupervisor
  alias Aflecha.Operator.FleetSupervisor
  alias Aflecha.Service.Payloader
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator
  alias Aflecha.HubManager
  alias Aflecha.Service.EventManager

  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  def connect_handler(sock) do
    :ok = SocketSupport.pass_ownership(sock, Process.whereis(__MODULE__))
    GenServer.call(__MODULE__, {:operator_connect, sock})
  end

  def get_operators(group_id) do
    GenServer.call(__MODULE__, {:get_operators, group_id})
  end

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:get_operators, group_id}, _from, state) do
    operator_ids = Map.get(state, group_id, [])

    {:reply, operator_ids, state}
  end

  @impl true
  def handle_call({:operator_connect, sock}, _from, state) do
    case operator_connect(sock) do
      {:ok, group_id, operator_id} ->
        operator_ids = Map.get(state, group_id, []) ++ [operator_id]
        state = Map.put(state, group_id, operator_ids)

        EventManager.dispatch(:operator_connected_event, {group_id, operator_id})

        {:reply, {:ok, {group_id, operator_id}}, state}

      {:error, :no_group} ->
        {:reply, {:error, :no_group}, state}
    end
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp operator_connect(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"connect_control_channel", init_message} | rest] = Payloader.parse_payload(payload)

        init_payload = Payloader.json_to_map(init_message)
        group_id = Map.get(init_payload, "group_id")

        # TODO: register factory ids.
        _factory_ids = Map.get(init_payload, "factory_ids")

        operator_id = Identificator.unique_id(group_id)
        control_unit = Identificator.control_unit(operator_id)

        # Only one hub per group.
        if !HubManager.hub_alive?(group_id) do
          HubManager.start_hub(group_id)
        end

        # Need to register handler for each operator.
        HubManager.register_handler(group_id, operator_id)

        FleetSupervisor.start_operator(group_id, operator_id, sock)

        :ok =
          SocketSupport.pass_ownership(
            sock,
            OperatorSupervisor.find_control_channel(operator_id)
          )

        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        GenServer.call(control_unit, :command_assign_operator_id)
        GenServer.call(control_unit, :command_sockets_connect)

        {:ok, group_id, operator_id}

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive group id: #{inspect(reason)}")

        {:error, :no_group}
    end
  end
end
