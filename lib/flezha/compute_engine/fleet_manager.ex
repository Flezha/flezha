defmodule Flezha.ComputeEngine.FleetManager do
  alias Flezha.ComputeEngine.ComputeEngineSupervisor
  alias Flezha.ComputeEngine.FleetSupervisor
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

    case GenServer.call(__MODULE__, {:compute_engine_connect, sock}) do
      {:error, reason} ->
        Logger.error("Failed to connect compute_engine: #{inspect(reason)}")
        nil

      {:ok, _result} ->
        :ok
    end
  end

  def active_compute_engines(namespace) do
    GenServer.call(__MODULE__, {:active_compute_engines, namespace})
  end

  def disconnect_compute_engine(namespace, compute_engine_id) do
    GenServer.call(__MODULE__, {:disconnect_compute_engine, namespace, compute_engine_id})
  end

  def stop_namespace(namespace) do
    GenServer.call(__MODULE__, {:stop_namespace, namespace})
  end

  @impl true
  def init(_) do
    schedule_compute_engine_cleanup()

    {:ok, %{compute_engine_namespaces: %{}}}
  end

  @impl true
  def handle_call({:disconnect_compute_engine, namespace, compute_engine_id}, _from, state) do
    # Check if the compute_engine is in the namespace.
    case get_in(state.compute_engine_namespaces, [namespace, compute_engine_id]) do
      nil ->
        {:reply, {:error, "compute_engine not found"}, state}

      _ ->
        Logger.info(
          "Disconnecting compute_engine #{compute_engine_id} from namespace #{namespace}"
        )

        GenServer.call(Identificator.control_unit(compute_engine_id), :disconnect_compute_engine)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:compute_engine_disconnected, namespace, compute_engine_id}, _from, state) do
    state = handle_compute_engine_disconnect(state, namespace, compute_engine_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_compute_engines, namespace}, _from, state) do
    compute_engine_ids = get_compute_engine_ids(state, namespace, :active)

    {:reply, compute_engine_ids, state}
  end

  @impl true
  def handle_call({:stop_namespace, namespace}, _from, state) do
    {reply, compute_engine_namespaces} =
      case Map.get(state.compute_engine_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.compute_engine_namespaces}

        _ ->
          # Stop all the compute_engines in the namespace.
          compute_engine_ids = get_compute_engine_ids(state, namespace, :active)

          Enum.each(compute_engine_ids, fn compute_engine_id ->
            FleetSupervisor.stop_compute_engine_sup(compute_engine_id)
          end)

          # Stop the namespace hub.
          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.compute_engine_namespaces, namespace)}
      end

    {:reply, reply, Map.put(state, :compute_engine_namespaces, compute_engine_namespaces)}
  end

  @impl true
  def handle_call({:compute_engine_connect, sock}, _from, state) do
    case compute_engine_connect(state.compute_engine_namespaces, sock) do
      {:ok, namespace, compute_engine_id} ->
        timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

        state =
          case Map.get(state.compute_engine_namespaces, namespace, nil) do
            nil -> put_in(state, [:compute_engine_namespaces, namespace], %{})
            _ -> state
          end

        state =
          put_in(
            state,
            [:compute_engine_namespaces, namespace, compute_engine_id],
            {:active, timestamp}
          )

        state = try_migrate_compute_engine(state, namespace, compute_engine_id)

        EventManager.dispatch(
          :compute_engine_connected_event,
          {namespace, compute_engine_id},
          %{members: :local}
        )

        {:reply, {:ok, {namespace, compute_engine_id}}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call(:compute_engine_cleanup, _from, state) do
    {:reply, :ok, compute_engine_cleanup(state)}
  end

  @impl true
  def handle_info(:schedule_compute_engine_cleanup, state) do
    schedule_compute_engine_cleanup()

    {:noreply, compute_engine_cleanup(state)}
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp schedule_compute_engine_cleanup() do
    Process.send_after(
      self(),
      :schedule_compute_engine_cleanup,
      get_compute_engine_keepalive() + 10_000
    )
  end

  defp get_compute_engine_keepalive() do
    keepalive = Application.get_env(:flezha, :compute_engine_keepalive, "30000")

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  defp compute_engine_cleanup(state) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    keep_alive = get_compute_engine_keepalive()

    # Get all namespaces.
    namespaces = Map.keys(state.compute_engine_namespaces)

    removal =
      Enum.map(namespaces, fn namespace ->
        in_active_compute_engines = get_compute_engine_ids(state, namespace, :inactive)

        removal_compute_engines =
          Enum.map(in_active_compute_engines, fn compute_engine_id ->
            {_status, timestamp} = state.compute_engine_namespaces[namespace][compute_engine_id]

            # Check if the compute_engine is still inactive after X seconds.
            if timestamp + keep_alive <= now do
              # Stop the namespace hub.
              HubManager.get_registry(namespace)
              HubManager.stop_hub(namespace)
              compute_engine_id
            else
              nil
            end
          end)

        {namespace, removal_compute_engines}
      end)

    # Remove the inactive compute_engines from the state.
    compute_engine_namespaces =
      Enum.reduce(removal, state.compute_engine_namespaces, fn {namespace,
                                                                removal_compute_engines},
                                                               acc ->
        namespace_compute_engines =
          Enum.reduce(removal_compute_engines, acc[namespace], fn compute_engine_id, acc ->
            if compute_engine_id == nil do
              acc
            else
              # Remove the compute_engine from the namespace.
              Map.delete(acc, compute_engine_id)
            end
          end)

        Map.put(acc, namespace, namespace_compute_engines)
      end)

    %{state | compute_engine_namespaces: compute_engine_namespaces}
  end

  defp try_migrate_compute_engine(state, namespace, compute_engine_id) do
    # Check if any existing inactive compute_engines are present in the namespace.
    # If so, we need to migrate the overwatchers to the new compute_engine.
    in_active_compute_engines = get_compute_engine_ids(state, namespace, :inactive)

    namespace_compute_engines =
      case List.first(in_active_compute_engines) do
        nil ->
          state.compute_engine_namespaces[namespace]

        inactive_compute_engine_id ->
          compute_engine_reroute(state, namespace, inactive_compute_engine_id, [compute_engine_id])
      end

    Logger.info(
      "Attempting to migrate compute_engine #{compute_engine_id} in namespace #{namespace}"
    )

    put_in(state, [:compute_engine_namespaces, namespace], namespace_compute_engines)
  end

  defp get_compute_engine_ids(state, namespace, status) do
    state
    |> Map.get(:compute_engine_namespaces, %{})
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {compute_engine_id, {_, _}} -> compute_engine_id end)
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

  defp handle_compute_engine_disconnect(state, namespace, compute_engine_id) do
    if not Map.has_key?(state.compute_engine_namespaces, namespace) do
      state
    else
      do_handle_compute_engine_disconnect(state, namespace, compute_engine_id)
    end
  end

  defp do_handle_compute_engine_disconnect(state, namespace, compute_engine_id) do
    FleetSupervisor.stop_compute_engine_sup(compute_engine_id)

    # Cancel any hub handlers for the compute_engine.
    if HubManager.hub_alive?(namespace) do
      HubManager.unregister_handler(namespace, compute_engine_id)
    end

    compute_engine_ids =
      state
      |> get_compute_engine_ids(namespace, :active)
      |> Enum.filter(fn id -> id != compute_engine_id end)

    other_agents = HubManager.get_hub_nodes(namespace)

    namespace_compute_engines =
      cond do
        # We can delegate the work to other compute_engines in the same namespace.
        !Enum.empty?(compute_engine_ids) ->
          compute_engine_reroute(state, namespace, compute_engine_id, compute_engine_ids)

        # Current agent has no other compute_engines to delegate the work to, but
        # there are other agents in the namespace who can take over the work.
        Enum.empty?(compute_engine_ids) && !Enum.empty?(other_agents) ->
          hub_migrate(state, namespace, compute_engine_id)

        # No other agents in the namespace or compute_engines to delegate the work to.
        # We need to preserve the state of the compute_engine for some time until
        # new compute_engines connect.
        Enum.empty?(compute_engine_ids) && Enum.empty?(other_agents) ->
          compute_engine_preserve(state, namespace, compute_engine_id)
      end

    EventManager.dispatch(
      :compute_engine_disconnected_event,
      {namespace, compute_engine_id},
      %{members: :local}
    )

    put_in(state, [:compute_engine_namespaces, namespace], namespace_compute_engines)
  end

  defp hub_migrate(state, namespace, _compute_engine_id) do
    HubManager.stop_hub(namespace)

    Map.delete(state.compute_engine_namespaces, namespace)
  end

  defp compute_engine_preserve(state, namespace, compute_engine_id) do
    compute_engine = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    # Mark the compute_engine as inactive.
    put_in(state.compute_engine_namespaces[namespace], [compute_engine_id], compute_engine)
  end

  defp compute_engine_reroute(state, namespace, old_compute_engine, available_compute_engines) do
    # TODO: select the best compute_engine to delegate the work to.
    new_compute_engine = Enum.random(available_compute_engines)

    # Let all overwatchers migrate their state to the new compute_engine.
    HubManager.compute_engine_processors(namespace, old_compute_engine)
    |> Enum.each(fn {_processor_id, [{_node, processor_pid}]} ->
      GenServer.call(processor_pid, {:compute_engine_migrate, new_compute_engine})
    end)

    # Switch processor overwatchers to the new compute_engine.
    HubManager.compute_engine_switch(namespace, old_compute_engine, new_compute_engine)

    # Reset the hub handler to pick up any process restarts and pid changes.
    HubManager.unregister_handler(namespace, old_compute_engine)
    HubManager.register_handler(namespace, new_compute_engine)

    # Notify the new compute_engine stream channels to pick up the
    # overwatchers pids.
    overwatchers =
      HubManager.compute_engine_processors(namespace, new_compute_engine)
      |> Enum.map(fn {processor_id, node_pids} ->
        {processor_id, Enum.map(node_pids, &elem(&1, 1))}
      end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(new_compute_engine)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(new_compute_engine))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.call(pid, {:processor_bulk_insert, overwatchers})
    end)

    # Remove the old compute_engine from the namespace.
    Map.delete(state.compute_engine_namespaces[namespace], old_compute_engine)
  end

  defp compute_engine_connect(_compute_engine_namespaces, sock) do
    case extract_control_info(sock) do
      # TODO: register factory ids.
      {:ok, namespace, _factory_ids, bytes_rest} ->
        compute_engine_id = Identificator.unique_id(namespace)
        control_unit = Identificator.control_unit(compute_engine_id)
        control_channel = Identificator.control_channel(compute_engine_id)

        # Only one hub per namespace.
        if !HubManager.hub_alive?(namespace) do
          HubManager.start_hub(namespace)
        end

        # Need to register handler for each compute_engine.
        HubManager.register_handler(namespace, compute_engine_id)

        FleetSupervisor.start_compute_engine_sup(namespace, compute_engine_id, sock)

        owner_result =
          SocketSupport.pass_ownership(
            sock,
            ComputeEngineSupervisor.find_control_channel(compute_engine_id)
          )

        case owner_result do
          :ok ->
            GenServer.call(control_channel, {:handle_frame, bytes_rest})
            GenServer.call(control_unit, :command_assign_compute_engine_id)
            GenServer.call(control_unit, :command_sockets_connect)

            {:ok, namespace, compute_engine_id}

          {:error, reason} ->
            {:error, reason}
        end

      err ->
        err
    end
  end
end
