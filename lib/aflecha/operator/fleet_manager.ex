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

    case GenServer.call(__MODULE__, {:operator_connect, sock}) do
      {:error, reason} ->
        Logger.error("Failed to connect operator: #{inspect(reason)}")
        nil

      {:ok, _result} ->
        :ok
    end
  end

  def active_operators(group_id) do
    GenServer.call(__MODULE__, {:active_operators, group_id})
  end

  @impl true
  def init(_) do
    {:ok, %{operator_groups: %{}}}
  end

  @impl true
  def handle_call({:active_operators, group_id}, _from, state) do
    operator_ids =
      state
      |> Map.get(:operator_groups, %{})
      |> Map.get(group_id, [])
      |> Enum.filter(fn {_, status, _} -> status == :active end)
      |> Enum.map(fn {operator_id, _, _} -> operator_id end)

    {:reply, operator_ids, state}
  end

  @impl true
  def handle_call({:operator_connect, sock}, _from, state) do
    case operator_connect(state.operator_groups, sock) do
      {:ok, group_id, operator_id} ->
        timestamp = DateTime.utc_now() |> DateTime.to_unix()
        operator = {operator_id, :active, timestamp}
        operator_ids = Map.get(state.operator_groups, group_id, []) ++ [operator]
        state = put_in(state, [:operator_groups, group_id], operator_ids)

        EventManager.dispatch(
          :operator_connected_event,
          {group_id, operator_id},
          %{members: :local}
        )

        {:reply, {:ok, {group_id, operator_id}}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp extract_control_info(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, frame} ->
        {payloads, _buffer} = Payloader.parse_frame(frame)

        [{"connect_control_channel", init_message} | bytes_rest] = payloads

        init_payload = Payloader.json_to_map(init_message)
        group_id = Map.get(init_payload, "group_id")
        factory_ids = Map.get(init_payload, "factory_ids")

        {:ok, group_id, factory_ids, bytes_rest}

      {:error, reason} ->
        :gen_tcp.close(sock)

        {:error, reason}
    end
  end

  defp operator_connect(_operator_groups, sock) do
    case extract_control_info(sock) do
      # TODO: register factory ids.
      {:ok, group_id, _factory_ids, bytes_rest} ->
        operator_id = Identificator.unique_id(group_id)
        control_unit = Identificator.control_unit(operator_id)
        control_channel = Identificator.control_channel(operator_id)

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

        GenServer.call(control_channel, {:handle_frame, bytes_rest})
        GenServer.call(control_unit, :command_assign_operator_id)
        GenServer.call(control_unit, :command_sockets_connect)

        {:ok, group_id, operator_id}

      err ->
        err
    end
  end
end
