defmodule Flezha.ComputeEngine.FleetManager do
  alias Flezha.ComputeEngine.ComputeEngineSupervisor
  alias Flezha.ComputeEngine.FleetSupervisor
  alias Flezha.Service.Identificator
  alias Flezha.Service.HubManager
  alias Flezha.Service.EventManager
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @doc """
  Unified connection handler for all transports.

  Called by Transport.Server when a new SDK connects.
  `stream_ref` is a NIF StreamHandle resource.
  `namespace` is extracted from the connect_control_channel frame.
  `init_payload` is the decoded JSON from the initial frame.
  """
  def connect_handler(stream_ref, namespace, init_payload) do
    factory_ids = Map.get(init_payload, "factory_ids")

    case GenServer.call(
           __MODULE__,
           {:compute_engine_connect, stream_ref, namespace, factory_ids}
         ) do
      {:error, reason} ->
        Logger.error("Failed to connect compute_engine: #{inspect(reason)}")
        {:error, reason}

      {:ok, {_namespace, compute_engine_id}} ->
        {:ok, compute_engine_id}
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
          compute_engine_ids = get_compute_engine_ids(state, namespace, :active)

          Enum.each(compute_engine_ids, fn compute_engine_id ->
            FleetSupervisor.stop_compute_engine_sup(compute_engine_id)
          end)

          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.compute_engine_namespaces, namespace)}
      end

    {:reply, reply, Map.put(state, :compute_engine_namespaces, compute_engine_namespaces)}
  end

  @impl true
  def handle_call(
        {:compute_engine_connect, stream_ref, namespace, _factory_ids},
        _from,
        state
      ) do
    case compute_engine_connect(stream_ref, namespace) do
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

    namespaces = Map.keys(state.compute_engine_namespaces)

    removal =
      Enum.map(namespaces, fn namespace ->
        in_active_compute_engines = get_compute_engine_ids(state, namespace, :inactive)

        removal_compute_engines =
          Enum.map(in_active_compute_engines, fn compute_engine_id ->
            {_status, timestamp} = state.compute_engine_namespaces[namespace][compute_engine_id]

            if timestamp + keep_alive <= now do
              HubManager.get_registry(namespace)
              HubManager.stop_hub(namespace)
              compute_engine_id
            else
              nil
            end
          end)

        {namespace, removal_compute_engines}
      end)

    compute_engine_namespaces =
      Enum.reduce(removal, state.compute_engine_namespaces, fn {namespace,
                                                                removal_compute_engines},
                                                               acc ->
        namespace_compute_engines =
          Enum.reduce(removal_compute_engines, acc[namespace], fn compute_engine_id, acc ->
            if compute_engine_id == nil do
              acc
            else
              Map.delete(acc, compute_engine_id)
            end
          end)

        Map.put(acc, namespace, namespace_compute_engines)
      end)

    %{state | compute_engine_namespaces: compute_engine_namespaces}
  end

  defp try_migrate_compute_engine(state, namespace, compute_engine_id) do
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

  defp handle_compute_engine_disconnect(state, namespace, compute_engine_id) do
    if not Map.has_key?(state.compute_engine_namespaces, namespace) do
      state
    else
      do_handle_compute_engine_disconnect(state, namespace, compute_engine_id)
    end
  end

  defp do_handle_compute_engine_disconnect(state, namespace, compute_engine_id) do
    FleetSupervisor.stop_compute_engine_sup(compute_engine_id)

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
        !Enum.empty?(compute_engine_ids) ->
          compute_engine_reroute(state, namespace, compute_engine_id, compute_engine_ids)

        Enum.empty?(compute_engine_ids) && !Enum.empty?(other_agents) ->
          hub_migrate(state, namespace, compute_engine_id)

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

    put_in(state.compute_engine_namespaces[namespace], [compute_engine_id], compute_engine)
  end

  defp compute_engine_reroute(state, namespace, old_compute_engine, available_compute_engines) do
    new_compute_engine = Enum.random(available_compute_engines)

    HubManager.compute_engine_processors(namespace, old_compute_engine)
    |> Enum.each(fn {_processor_id, [{_node, processor_pid}]} ->
      GenServer.call(processor_pid, {:compute_engine_migrate, new_compute_engine})
    end)

    HubManager.compute_engine_switch(namespace, old_compute_engine, new_compute_engine)

    HubManager.unregister_handler(namespace, old_compute_engine)
    HubManager.register_handler(namespace, new_compute_engine)

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

    Map.delete(state.compute_engine_namespaces[namespace], old_compute_engine)
  end

  defp compute_engine_connect(stream_ref, namespace) do
    compute_engine_id = Identificator.unique_id(namespace)
    control_unit = Identificator.control_unit(compute_engine_id)

    if !HubManager.hub_alive?(namespace) do
      HubManager.start_hub(namespace)
    end

    HubManager.register_handler(namespace, compute_engine_id)

    FleetSupervisor.start_compute_engine_sup(namespace, compute_engine_id, stream_ref)

    # Transfer ownership of the stream to the ControlChannel process
    control_channel_pid =
      ComputeEngineSupervisor.find_control_channel(compute_engine_id)

    Native.controlling_process(stream_ref, control_channel_pid)
    Native.set_active(stream_ref)

    # Send setup commands
    GenServer.call(control_unit, :command_assign_compute_engine_id)
    GenServer.call(control_unit, :command_sockets_connect)

    {:ok, namespace, compute_engine_id}
  end
end
