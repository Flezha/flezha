defmodule Flezha.Operator.FleetManager do
  alias Flezha.Operator.OperatorSupervisor
  alias Flezha.Operator.FleetSupervisor
  alias Flezha.Service.Payloader
  alias Flezha.Socket.Support, as: SocketSupport
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.EventManager

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

  def active_operators(namespace) do
    GenServer.call(__MODULE__, {:active_operators, namespace})
  end

  def disconnect_operator(namespace, operator_id) do
    GenServer.call(__MODULE__, {:disconnect_operator, namespace, operator_id})
  end

  def stop_namespace(namespace) do
    GenServer.call(__MODULE__, {:stop_namespace, namespace})
  end

  @impl true
  def init(_) do
    schedule_operator_cleanup()

    {:ok, %{operator_namespaces: %{}}}
  end

  @impl true
  def handle_call({:disconnect_operator, namespace, operator_id}, _from, state) do
    # Check if the operator is in the namespace.
    case get_in(state.operator_namespaces, [namespace, operator_id]) do
      nil ->
        {:reply, {:error, "operator not found"}, state}

      _ ->
        Logger.info("Disconnecting operator #{operator_id} from namespace #{namespace}")
        GenServer.call(Identificator.control_unit(operator_id), :disconnect_operator)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:operator_disconnected, namespace, operator_id}, _from, state) do
    state = handle_operator_disconnect(state, namespace, operator_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_operators, namespace}, _from, state) do
    operator_ids = get_operator_ids(state, namespace, :active)

    {:reply, operator_ids, state}
  end

  @impl true
  def handle_call({:stop_namespace, namespace}, _from, state) do
    {reply, operator_namespaces} =
      case Map.get(state.operator_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.operator_namespaces}

        _ ->
          # Stop all the operators in the namespace.
          operator_ids = get_operator_ids(state, namespace, :active)

          Enum.each(operator_ids, fn operator_id ->
            FleetSupervisor.stop_operator_sup(operator_id)
          end)

          # Stop the namespace hub.
          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.operator_namespaces, namespace)}
      end

    {:reply, reply, Map.put(state, :operator_namespaces, operator_namespaces)}
  end

  @impl true
  def handle_call({:operator_connect, sock}, _from, state) do
    case operator_connect(state.operator_namespaces, sock) do
      {:ok, namespace, operator_id} ->
        timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

        state =
          case Map.get(state.operator_namespaces, namespace, nil) do
            nil -> put_in(state, [:operator_namespaces, namespace], %{})
            _ -> state
          end

        state =
          put_in(state, [:operator_namespaces, namespace, operator_id], {:active, timestamp})

        state = try_migrate_operator(state, namespace, operator_id)

        EventManager.dispatch(
          :operator_connected_event,
          {namespace, operator_id},
          %{members: :local}
        )

        {:reply, {:ok, {namespace, operator_id}}, state}

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
    keepalive = Application.get_env(:flezha, :operator_keepalive, "30000")

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  defp operator_cleanup(state) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    keep_alive = get_operator_keepalive()

    # Get all namespaces.
    namespaces = Map.keys(state.operator_namespaces)

    removal =
      Enum.map(namespaces, fn namespace ->
        in_active_operators = get_operator_ids(state, namespace, :inactive)

        removal_operators =
          Enum.map(in_active_operators, fn operator_id ->
            {_status, timestamp} = state.operator_namespaces[namespace][operator_id]

            # Check if the operator is still inactive after X seconds.
            if timestamp + keep_alive <= now do
              # Stop the namespace hub.
              HubManager.get_registry(namespace)
              HubManager.stop_hub(namespace)
              operator_id
            else
              nil
            end
          end)

        {namespace, removal_operators}
      end)

    # Remove the inactive operators from the state.
    operator_namespaces =
      Enum.reduce(removal, state.operator_namespaces, fn {namespace, removal_operators}, acc ->
        namespace_operators =
          Enum.reduce(removal_operators, acc[namespace], fn operator_id, acc ->
            if operator_id == nil do
              acc
            else
              # Remove the operator from the namespace.
              Map.delete(acc, operator_id)
            end
          end)

        Map.put(acc, namespace, namespace_operators)
      end)

    %{state | operator_namespaces: operator_namespaces}
  end

  defp try_migrate_operator(state, namespace, operator_id) do
    # Check if any existing inactive operators are present in the namespace.
    # If so, we need to migrate the overwatchers to the new operator.
    in_active_operators = get_operator_ids(state, namespace, :inactive)

    namespace_operators =
      case List.first(in_active_operators) do
        nil ->
          state.operator_namespaces[namespace]

        inactive_operator_id ->
          operator_reroute(state, namespace, inactive_operator_id, [operator_id])
      end

    Logger.info("Attempting to migrate operator #{operator_id} in namespace #{namespace}")

    put_in(state, [:operator_namespaces, namespace], namespace_operators)
  end

  defp get_operator_ids(state, namespace, status) do
    state
    |> Map.get(:operator_namespaces, %{})
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {operator_id, {_, _}} -> operator_id end)
  end

  defp extract_control_info(sock) do
    case :gen_tcp.recv(sock, 0, 5000) do
      {:ok, frame} ->
        {payloads, _buffer} = Payloader.parse_frame(frame)

        [{"connect_control_channel", init_message, _metadata} | bytes_rest] = payloads

        bytes_rest = if bytes_rest === [], do: "", else: bytes_rest

        init_payload = Payloader.json_to_map(init_message)
        namespace = Map.get(init_payload, "namespace")
        factory_ids = Map.get(init_payload, "factory_ids")

        {:ok, namespace, factory_ids, bytes_rest}

      {:error, reason} ->
        :gen_tcp.close(sock)

        {:error, reason}
    end
  end

  defp handle_operator_disconnect(state, namespace, operator_id) do
    FleetSupervisor.stop_operator_sup(operator_id)

    # Cancel any hub handlers for the operator.
    if HubManager.hub_alive?(namespace) do
      HubManager.unregister_handler(namespace, operator_id)
    end

    operator_ids =
      state
      |> get_operator_ids(namespace, :active)
      |> Enum.filter(fn id -> id != operator_id end)

    other_agents = HubManager.get_hub_nodes(namespace)

    namespace_operators =
      cond do
        # We can delegate the work to other operators in the same namespace.
        !Enum.empty?(operator_ids) ->
          operator_reroute(state, namespace, operator_id, operator_ids)

        # Current agent has no other operators to delegate the work to, but
        # there are other agents in the namespace who can take over the work.
        Enum.empty?(operator_ids) && !Enum.empty?(other_agents) ->
          hub_migrate(state, namespace, operator_id)

        # No other agents in the namespace or operators to delegate the work to.
        # We need to preserve the state of the operator for some time until
        # new operators connect.
        Enum.empty?(operator_ids) && Enum.empty?(other_agents) ->
          operator_preserve(state, namespace, operator_id)
      end

    EventManager.dispatch(
      :operator_disconnected_event,
      {namespace, operator_id},
      %{members: :local}
    )

    put_in(state, [:operator_namespaces, namespace], namespace_operators)
  end

  defp hub_migrate(state, namespace, _operator_id) do
    HubManager.stop_hub(namespace)

    Map.delete(state.operator_namespaces, namespace)
  end

  defp operator_preserve(state, namespace, operator_id) do
    operator = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    # Mark the operator as inactive.
    put_in(state.operator_namespaces[namespace], [operator_id], operator)
  end

  defp operator_reroute(state, namespace, old_operator, available_operators) do
    # TODO: select the best operator to delegate the work to.
    new_operator = Enum.random(available_operators)

    # Let all overwatchers migrate their state to the new operator.
    HubManager.operator_computations(namespace, old_operator)
    |> Enum.each(fn {_computation_id, [{_node, computation_pid}]} ->
      GenServer.call(computation_pid, {:operator_migrate, new_operator})
    end)

    # Switch computation overwatchers to the new operator.
    HubManager.operator_switch(namespace, old_operator, new_operator)

    # Reset the hub handler to pick up any process restarts and pid changes.
    HubManager.unregister_handler(namespace, old_operator)
    HubManager.register_handler(namespace, new_operator)

    # Notify the new operator stream channels to pick up the
    # overwatchers pids.
    overwatchers =
      HubManager.operator_computations(namespace, new_operator)
      |> Enum.map(fn {computation_id, node_pids} ->
        {computation_id, Enum.map(node_pids, &elem(&1, 1))}
      end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(new_operator)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(new_operator))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.call(pid, {:computation_bulk_insert, overwatchers})
    end)

    # Remove the old operator from the namespace.
    Map.delete(state.operator_namespaces[namespace], old_operator)
  end

  defp operator_connect(_operator_namespaces, sock) do
    case extract_control_info(sock) do
      # TODO: register factory ids.
      {:ok, namespace, _factory_ids, bytes_rest} ->
        operator_id = Identificator.unique_id(namespace)
        control_unit = Identificator.control_unit(operator_id)
        control_channel = Identificator.control_channel(operator_id)

        # Only one hub per namespace.
        if !HubManager.hub_alive?(namespace) do
          HubManager.start_hub(namespace)
        end

        # Need to register handler for each operator.
        HubManager.register_handler(namespace, operator_id)

        FleetSupervisor.start_operator_sup(namespace, operator_id, sock)

        owner_result =
          SocketSupport.pass_ownership(
            sock,
            OperatorSupervisor.find_control_channel(operator_id)
          )

        case owner_result do
          :ok ->
            GenServer.call(control_channel, {:handle_frame, bytes_rest})
            GenServer.call(control_unit, :command_assign_operator_id)
            GenServer.call(control_unit, :command_sockets_connect)

            {:ok, namespace, operator_id}

          {:error, reason} ->
            {:error, reason}
        end

      err ->
        err
    end
  end
end
