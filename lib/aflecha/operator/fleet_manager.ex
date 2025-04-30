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
  def handle_call({:operator_disconnect, group_id, operator_id}, _from, state) do
    state = handle_operator_disconnect(state, group_id, operator_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_operators, group_id}, _from, state) do
    operator_ids =
      state
      |> Map.get(:operator_groups, %{})
      |> Map.get(group_id, [])
      |> Enum.filter(fn {_, {status, _}} -> status == :active end)
      |> Enum.map(fn {operator_id, {_, _}} -> operator_id end)

    {:reply, operator_ids, state}
  end

  @impl true
  def handle_call({:operator_connect, sock}, _from, state) do
    case operator_connect(state.operator_groups, sock) do
      {:ok, group_id, operator_id} ->
        timestamp = DateTime.utc_now() |> DateTime.to_unix()

        state =
          case Map.get(state.operator_groups, group_id, nil) do
            nil -> put_in(state, [:operator_groups, group_id], %{})
            _ -> state
          end

        state = put_in(state, [:operator_groups, group_id, operator_id], {:active, timestamp})

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

  defp handle_operator_disconnect(state, group_id, operator_id) do
    # TODO: kill the existing operator supervisor.

    operator_ids =
      state
      |> Map.get(:operator_groups, %{})
      |> Map.get(group_id, [])
      |> Enum.filter(fn {id, {status, _}} -> status == :active && id != operator_id end)
      |> Enum.map(fn {id, {_, _}} -> id end)

    other_agents = HubManager.get_hub_nodes(group_id)

    group_operators =
      cond do
        # We can delegate the work to other operators in the same group.
        !Enum.empty?(operator_ids) ->
          operator_reroute(state, group_id, operator_id, operator_ids)

        # Current agent has no other operators to delegate the work to, but
        # there are other agents in the group who can take over the work.
        Enum.empty?(operator_ids) && !Enum.empty?(other_agents) ->
          # TODO:
          nil

        # No other agents in the group or operators to delegate the work to.
        # We need to preserve the state of the operator for some time until
        # new operators connect.
        Enum.empty?(operator_ids) && Enum.empty?(other_agents) ->
          # TODO:
          nil
      end

    EventManager.dispatch(
      :operator_disconnected_event,
      {group_id, operator_id},
      %{members: :local}
    )

    state = put_in(state, [:operator_groups, group_id], group_operators)

    state
  end

  defp operator_reroute(state, group_id, old_operator, available_operators) do
    # TODO: select the best operator to delegate the work to.
    new_operator = Enum.random(available_operators)

    # Switch pipeline overwatchers to the new operator.
    HubManager.operator_switch(group_id, old_operator, new_operator)

    # Let all overwatchers migrate their state to the new operator.
    HubManager.operator_pipelines(group_id, old_operator)
    |> Enum.each(fn {_pipeline_id, {_node, pipeline_pid}} ->
      GenServer.call(pipeline_pid, {:operator_migrate, new_operator})
    end)

    # Reset the hub handler to pick up any process restarts and pid changes.
    HubManager.unregister_handler(group_id, old_operator)
    HubManager.register_handler(group_id, new_operator)

    # Notify the new operator stream channels to pick up the
    # overwatchers pids.
    overwatchers =
      HubManager.operator_pipelines(group_id, new_operator)
      |> Enum.map(fn {pipeline_id, node_pids} ->
        {pipeline_id, Enum.map(node_pids, &elem(&1, 1))}
      end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(new_operator)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(new_operator))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.call(pid, {:pipeline_bulk_insert, overwatchers})
    end)

    # Remove the old operator from the group.
    Map.delete(state.operator_groups[group_id], old_operator)
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

        FleetSupervisor.start_operator_sup(group_id, operator_id, sock)

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
