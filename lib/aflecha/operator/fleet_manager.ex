defmodule Aflecha.Operator.FleetManager do
  alias Aflecha.Operator.OperatorSupervisor
  alias Aflecha.Operator.FleetSupervisor
  alias Aflecha.Service.Payloader
  alias Aflecha.Socket.Support, as: SocketSupport
  alias Aflecha.Service.Identificator
  alias Aflecha.Service.HubManager
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

  def disconnect_operator(group_id, operator_id) do
    GenServer.call(__MODULE__, {:disconnect_operator, group_id, operator_id})
  end

  @impl true
  def init(_) do
    schedule_operator_cleanup()

    {:ok, %{operator_groups: %{}}}
  end

  @impl true
  def handle_call({:disconnect_operator, group_id, operator_id}, _from, state) do
    # Check if the operator is in the group.
    case get_in(state.operator_groups, [group_id, operator_id]) do
      nil ->
        {:reply, {:error, "operator not found"}, state}

      _ ->
        Logger.info("Disconnecting operator #{operator_id} from group #{group_id}")
        GenServer.call(Identificator.control_unit(operator_id), :disconnect_operator)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:operator_disconnected, group_id, operator_id}, _from, state) do
    state = handle_operator_disconnect(state, group_id, operator_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_operators, group_id}, _from, state) do
    operator_ids = get_operator_ids(state, group_id, :active)

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

        state = try_migrate_operator(state, group_id, operator_id)

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
  def handle_call(:operator_cleanup, _from, state) do
    {:reply, :ok, operator_cleanup(state)}
  end

  @impl true
  def handle_info(:schedule_operator_cleanup, state) do
    schedule_operator_cleanup()

    {:noreply, operator_cleanup(state)}
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp schedule_operator_cleanup() do
    Process.send_after(
      self(),
      :schedule_operator_cleanup,
      get_operator_keepalive() + 10_000
    )
  end

  defp get_operator_keepalive() do
    keepalive = Application.get_env(:aflecha, :operator_keepalive, "30000")

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  defp operator_cleanup(state) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    keep_alive = get_operator_keepalive()

    # Get all groups.
    groups = Map.keys(state.operator_groups)

    removal =
      Enum.map(groups, fn group ->
        in_active_operators = get_operator_ids(state, group, :inactive)

        removal_operators =
          Enum.map(in_active_operators, fn operator_id ->
            {_status, timestamp} = state.operator_groups[group][operator_id]

            # Check if the operator is still inactive after X seconds.
            if timestamp + keep_alive <= now do
              # Stop the group hub.
              HubManager.get_registry(group)
              HubManager.stop_hub(group)
              operator_id
            else
              nil
            end
          end)

        {group, removal_operators}
      end)

    # Remove the inactive operators from the state.
    operator_groups =
      Enum.reduce(removal, state.operator_groups, fn {group, removal_operators}, acc ->
        group_operators =
          Enum.reduce(removal_operators, acc[group], fn operator_id, acc ->
            if operator_id == nil do
              acc
            else
              # Remove the operator from the group.
              Map.delete(acc, operator_id)
            end
          end)

        Map.put(acc, group, group_operators)
      end)

    %{state | operator_groups: operator_groups}
  end

  defp try_migrate_operator(state, group_id, operator_id) do
    # Check if any existing inactive operators are present in the group.
    # If so, we need to migrate the overwatchers to the new operator.
    in_active_operators = get_operator_ids(state, group_id, :inactive)

    group_operators =
      case List.first(in_active_operators) do
        nil ->
          state.operator_groups[group_id]

        inactive_operator_id ->
          operator_reroute(state, group_id, inactive_operator_id, [operator_id])
      end

    Logger.info("Attempting to migrate operator #{operator_id} in group #{group_id}")

    put_in(state, [:operator_groups, group_id], group_operators)
  end

  defp get_operator_ids(state, group_id, status) do
    state
    |> Map.get(:operator_groups, %{})
    |> Map.get(group_id, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {operator_id, {_, _}} -> operator_id end)
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
    FleetSupervisor.stop_operator_sup(operator_id)

    # Cancel any hub handlers for the operator.
    HubManager.unregister_handler(group_id, operator_id)

    operator_ids =
      state
      |> get_operator_ids(group_id, :active)
      |> Enum.filter(fn id -> id != operator_id end)

    other_agents = HubManager.get_hub_nodes(group_id)

    group_operators =
      cond do
        # We can delegate the work to other operators in the same group.
        !Enum.empty?(operator_ids) ->
          operator_reroute(state, group_id, operator_id, operator_ids)

        # Current agent has no other operators to delegate the work to, but
        # there are other agents in the group who can take over the work.
        Enum.empty?(operator_ids) && !Enum.empty?(other_agents) ->
          hub_migrate(state, group_id, operator_id)

        # No other agents in the group or operators to delegate the work to.
        # We need to preserve the state of the operator for some time until
        # new operators connect.
        Enum.empty?(operator_ids) && Enum.empty?(other_agents) ->
          operator_preserve(state, group_id, operator_id)
      end

    EventManager.dispatch(
      :operator_disconnected_event,
      {group_id, operator_id},
      %{members: :local}
    )

    put_in(state, [:operator_groups, group_id], group_operators)
  end

  defp hub_migrate(state, group_id, _operator_id) do
    HubManager.stop_hub(group_id)

    Map.delete(state.operator_groups, group_id)
  end

  defp operator_preserve(state, group_id, operator_id) do
    operator = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    # Mark the operator as inactive.
    put_in(state.operator_groups[group_id], [operator_id], operator)
  end

  defp operator_reroute(state, group_id, old_operator, available_operators) do
    # TODO: select the best operator to delegate the work to.
    new_operator = Enum.random(available_operators)

    # Let all overwatchers migrate their state to the new operator.
    HubManager.operator_pipelines(group_id, old_operator)
    |> Enum.each(fn {_pipeline_id, [{_node, pipeline_pid}]} ->
      GenServer.call(pipeline_pid, {:operator_migrate, new_operator})
    end)

    # Switch pipeline overwatchers to the new operator.
    HubManager.operator_switch(group_id, old_operator, new_operator)

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
