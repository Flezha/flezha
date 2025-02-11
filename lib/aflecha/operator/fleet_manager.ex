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
    GenServer.call(__MODULE__, {:worker_connect, sock})
  end

  def get_operators(group_id) do
    GenServer.call(__MODULE__, {:get_workers, group_id})
  end

  @impl true
  def init(_) do
    # TODO: expect_factories()

    {:ok, %{}}
  end

  @impl true
  def handle_call({:get_workers, group_id}, _from, state) do
    operator_ids = Map.get(state, group_id, [])

    {:reply, operator_ids, state}
  end

  @impl true
  def handle_call({:worker_connect, sock}, _from, state) do
    case worker_connect(sock) do
      {:ok, group_id, operator_id} ->
        operator_ids = Map.get(state, group_id, []) ++ [operator_id]
        state = Map.put(state, group_id, operator_ids)

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

  defp worker_connect(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, payload} ->
        [{"connect_control_channel", group_id} | rest] = Payloader.parse_payload(payload)
        operator_id = Identificator.unique_id(group_id)
        control_unit = Identificator.control_unit(operator_id)

        if !HubManager.hub_alive?(group_id) do
          HubManager.start_hub(group_id, operator_id)
        end

        FleetSupervisor.start_operator(group_id, operator_id, sock)

        :ok =
          SocketSupport.pass_ownership(
            sock,
            OperatorSupervisor.find_control_channel(operator_id)
          )

        GenServer.call(control_unit, {:handle_initial_payloads, rest})
        GenServer.call(control_unit, :command_sockets_connect)

        {:ok, group_id, operator_id}

      {:error, reason} ->
        :gen_tcp.close(sock)
        Logger.error("Failed to receive group id: #{inspect(reason)}")

        {:error, :no_group}
    end
  end

  defp expect_factories do
    EventManager.add_handler(:factory_registered)

    count = Application.get_env(:aflecha, :factories_expected_count)

    if is_integer(count) && count > 0 do
      for _x <- 1..count do
        receive do
          {:factory_registered, _factory_id} ->
            :ok
        after
          3000 -> raise "Registered factories not equal to expected count"
        end
      end
    end
  end
end
