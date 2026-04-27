defmodule Flezha.ComputeEngine.FleetManager do
  alias Flezha.ComputeEngine.ComputeEngineSupervisor
  alias Flezha.ComputeEngine.FleetSupervisor
  alias Flezha.ComputeEngine.OrphanFinder
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
  def list_factories(namespace) do
    GenServer.call(__MODULE__, {:list_factories, namespace})
  end

  def connect_handler(stream_ref, namespace, init_payload, transport) do
    factories = parse_factories(init_payload)

    case GenServer.call(
           __MODULE__,
           {:compute_engine_connect, stream_ref, namespace, factories, transport}
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

  @doc """
  Returns active compute engines in `namespace` that advertise `factory_id`.

  Used by `ProcessorManager.assign_compute_engine/2` so we never route a
  processor to a node that doesn't own its factory — without this filter
  every input frame for a misrouted processor hits a dead registry entry
  on the foreign node and generates a `enqueue: processor X not found`
  warning on every input.
  """
  def compute_engines_for_factory(namespace, factory_id) do
    GenServer.call(__MODULE__, {:compute_engines_for_factory, namespace, factory_id})
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

    {:ok, %{compute_engine_namespaces: %{}, namespace_factories: %{}}}
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
  def handle_call({:compute_engines_for_factory, namespace, factory_id}, _from, state) do
    active_ids = MapSet.new(get_compute_engine_ids(state, namespace, :active))

    ce_ids =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Map.get(factory_id, %{ce_ids: MapSet.new()})
      |> Map.get(:ce_ids)
      |> MapSet.intersection(active_ids)
      |> MapSet.to_list()

    {:reply, ce_ids, state}
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

    state = Map.put(state, :compute_engine_namespaces, compute_engine_namespaces)
    state = Map.update!(state, :namespace_factories, &Map.delete(&1, namespace))
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:list_factories, namespace}, _from, state) do
    factories =
      state.namespace_factories
      |> Map.get(namespace, %{})
      |> Enum.map(fn {factory_id, info} ->
        %{id: factory_id, description: info.description}
      end)
      |> Enum.sort_by(& &1.id)

    {:reply, {:ok, factories}, state}
  end

  @impl true
  def handle_call(
        {:compute_engine_connect, stream_ref, namespace, factories, transport},
        _from,
        state
      ) do
    # compute_engine_connect/3 currently always returns {:ok, ...} — no error
    # path exists at the agent level yet. If one is ever added, re-introduce
    # the {:error, reason} branch here.
    {:ok, namespace, compute_engine_id} =
      compute_engine_connect(stream_ref, namespace, transport)

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

    state = store_factories(state, namespace, compute_engine_id, factories)
    {state, claimed_processor_ids} = try_migrate_compute_engine(state, namespace, compute_engine_id)
    active_ids = MapSet.new(get_compute_engine_ids(state, namespace, :active))

    reissue_local_orphans(
      namespace,
      compute_engine_id,
      factories,
      claimed_processor_ids,
      active_ids
    )

    EventManager.dispatch(
      :compute_engine_connected_event,
      {namespace, compute_engine_id},
      %{members: :local}
    )

    {:reply, {:ok, {namespace, compute_engine_id}}, state}
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

    state = %{state | compute_engine_namespaces: compute_engine_namespaces}

    # Clean factory refs for removed CEs
    Enum.reduce(removal, state, fn {namespace, removal_compute_engines}, acc ->
      Enum.reduce(removal_compute_engines, acc, fn
        nil, acc -> acc
        ce_id, acc -> remove_ce_factories(acc, namespace, ce_id)
      end)
    end)
  end

  defp try_migrate_compute_engine(state, namespace, compute_engine_id) do
    in_active_compute_engines = get_compute_engine_ids(state, namespace, :inactive)

    Logger.info(
      "Attempting to migrate compute_engine #{compute_engine_id} in namespace #{namespace}"
    )

    case List.first(in_active_compute_engines) do
      nil ->
        {state, MapSet.new()}

      inactive_compute_engine_id ->
        # Capture which processor_ids the migrate path is about to claim so the
        # reissue catch-up can skip them — the migrate already calls
        # Overwatch.compute_engine_migrate, which fires AddProcessor.
        claimed_ids =
          HubManager.compute_engine_processors(namespace, inactive_compute_engine_id)
          |> Enum.map(fn {processor_id, _node_pids} -> processor_id end)
          |> MapSet.new()

        namespace_compute_engines =
          compute_engine_reroute(
            state,
            namespace,
            inactive_compute_engine_id,
            [compute_engine_id]
          )

        new_state =
          put_in(state, [:compute_engine_namespaces, namespace], namespace_compute_engines)

        {new_state, claimed_ids}
    end
  end

  @doc false
  # Public for tests. Re-issues AddProcessor (via Overwatch.compute_engine_migrate)
  # to the freshly connected operator for every local Overwatch whose registered
  # compute_engine_id no longer points at an active operator. Without this,
  # processors ProcessHub keeps assigned to a dead compute_engine_id sit silent
  # until ProcessHub redistributes them.
  def reissue_local_orphans(namespace, new_ce_id, factories, claimed_ids, active_ids) do
    factory_id_set = OrphanFinder.factory_ids_from(factories)
    curr_node = node()

    case HubManager.get_registry(namespace) do
      {:error, reason} ->
        Logger.debug(
          "[FleetManager] reissue_local_orphans: hub registry unavailable for " <>
            "#{namespace}: #{inspect(reason)}"
        )

        :ok

      {:ok, registry} ->
        registry
        |> OrphanFinder.collect(curr_node, claimed_ids, active_ids, factory_id_set)
        |> Enum.each(fn {processor_id, pid} ->
          Logger.info(
            "[FleetManager] Reissuing AddProcessor for orphan processor " <>
              "#{processor_id} → compute_engine #{new_ce_id}"
          )

          try do
            GenServer.call(pid, {:compute_engine_migrate, new_ce_id})
            HubManager.processor_set_compute_engine(namespace, processor_id, new_ce_id)
          catch
            :exit, exit_reason ->
              Logger.warning(
                "[FleetManager] Failed to reissue processor #{processor_id}: " <>
                  inspect(exit_reason)
              )
          end
        end)

        :ok
    end
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

    state = remove_ce_factories(state, namespace, compute_engine_id)

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

  defp hub_migrate(_state, namespace, _compute_engine_id) do
    HubManager.stop_hub(namespace)

    %{}
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

  defp compute_engine_connect(stream_ref, namespace, transport) do
    compute_engine_id = Identificator.unique_id(namespace)
    control_unit = Identificator.control_unit(compute_engine_id)

    if !HubManager.hub_alive?(namespace) do
      HubManager.start_hub(namespace)
    end

    HubManager.register_handler(namespace, compute_engine_id)

    FleetSupervisor.start_compute_engine_sup(namespace, compute_engine_id, stream_ref, transport)

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

  defp parse_factories(init_payload) do
    case Map.get(init_payload, "factories") do
      nil ->
        # Backward compat: old SDKs only send factory_ids
        (Map.get(init_payload, "factory_ids") || [])
        |> Enum.map(fn id -> %{"id" => id, "description" => nil} end)

      factories when is_list(factories) ->
        factories
    end
  end

  defp store_factories(state, namespace, compute_engine_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]
        existing = Map.get(acc, factory_id, %{description: nil, ce_ids: MapSet.new()})

        description = factory["description"] || existing.description

        Map.put(acc, factory_id, %{
          description: description,
          ce_ids: MapSet.put(existing.ce_ids, compute_engine_id)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  defp remove_ce_factories(state, namespace, compute_engine_id) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      ns_factories
      |> Enum.map(fn {factory_id, info} ->
        {factory_id, %{info | ce_ids: MapSet.delete(info.ce_ids, compute_engine_id)}}
      end)
      |> Enum.reject(fn {_factory_id, info} -> MapSet.size(info.ce_ids) == 0 end)
      |> Map.new()

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end
end
