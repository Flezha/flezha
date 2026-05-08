defmodule Flezha.Executor.FleetManager do
  alias Flezha.Executor.ExecutorSupervisor
  alias Flezha.Executor.FleetSupervisor
  alias Flezha.Executor.OrphanFinder
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
           {:executor_connect, stream_ref, namespace, factories, transport}
         ) do
      {:error, reason} ->
        Logger.error("Failed to connect executor: #{inspect(reason)}")
        {:error, reason}

      {:ok, {_namespace, executor_id}} ->
        {:ok, executor_id}
    end
  end

  def active_executors(namespace) do
    GenServer.call(__MODULE__, {:active_executors, namespace})
  end

  @doc """
  Returns active compute engines in `namespace` that advertise `factory_id`.

  Used by `RelayManager.assign_executor/2` so we never route a
  relay to a node that doesn't own its factory — without this filter
  every input frame for a misrouted relay hits a dead registry entry
  on the foreign node and generates a `enqueue: relay X not found`
  warning on every input.
  """
  def executors_for_factory(namespace, factory_id) do
    GenServer.call(__MODULE__, {:executors_for_factory, namespace, factory_id})
  end

  def disconnect_executor(namespace, executor_id) do
    GenServer.call(__MODULE__, {:disconnect_executor, namespace, executor_id})
  end

  def stop_namespace(namespace) do
    GenServer.call(__MODULE__, {:stop_namespace, namespace})
  end

  @impl true
  def init(_) do
    schedule_executor_cleanup()

    {:ok, %{executor_namespaces: %{}, namespace_factories: %{}}}
  end

  @impl true
  def handle_call({:disconnect_executor, namespace, executor_id}, _from, state) do
    case get_in(state.executor_namespaces, [namespace, executor_id]) do
      nil ->
        {:reply, {:error, "executor not found"}, state}

      _ ->
        Logger.info(
          "Disconnecting executor #{executor_id} from namespace #{namespace}"
        )

        GenServer.call(Identificator.control_unit(executor_id), :disconnect_executor)

        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:executor_disconnected, namespace, executor_id}, _from, state) do
    state = handle_executor_disconnect(state, namespace, executor_id)

    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:active_executors, namespace}, _from, state) do
    executor_ids = get_executor_ids(state, namespace, :active)

    {:reply, executor_ids, state}
  end

  @impl true
  def handle_call({:executors_for_factory, namespace, factory_id}, _from, state) do
    active_ids = MapSet.new(get_executor_ids(state, namespace, :active))

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
    {reply, executor_namespaces} =
      case Map.get(state.executor_namespaces, namespace, nil) do
        nil ->
          {:namespace_not_found, state.executor_namespaces}

        _ ->
          executor_ids = get_executor_ids(state, namespace, :active)

          Enum.each(executor_ids, fn executor_id ->
            FleetSupervisor.stop_executor_sup(executor_id)
          end)

          HubManager.stop_hub(namespace)

          {:ok, Map.delete(state.executor_namespaces, namespace)}
      end

    state = Map.put(state, :executor_namespaces, executor_namespaces)
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
        {:executor_connect, stream_ref, namespace, factories, transport},
        _from,
        state
      ) do
    # executor_connect/3 currently always returns {:ok, ...} — no error
    # path exists at the controller level yet. If one is ever added, re-introduce
    # the {:error, reason} branch here.
    {:ok, namespace, executor_id} =
      executor_connect(stream_ref, namespace, transport)

    timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

    state =
      case Map.get(state.executor_namespaces, namespace, nil) do
        nil -> put_in(state, [:executor_namespaces, namespace], %{})
        _ -> state
      end

    state =
      put_in(
        state,
        [:executor_namespaces, namespace, executor_id],
        {:active, timestamp}
      )

    state = store_factories(state, namespace, executor_id, factories)
    {state, claimed_relay_ids} = try_migrate_executor(state, namespace, executor_id)
    active_ids = MapSet.new(get_executor_ids(state, namespace, :active))

    reissue_local_orphans(
      namespace,
      executor_id,
      factories,
      claimed_relay_ids,
      active_ids
    )

    EventManager.dispatch(
      :executor_connected_event,
      {namespace, executor_id},
      %{members: :local}
    )

    {:reply, {:ok, {namespace, executor_id}}, state}
  end

  @impl true
  def handle_call(:executor_cleanup, _from, state) do
    {:reply, :ok, executor_cleanup(state)}
  end

  @impl true
  def handle_info(:schedule_executor_cleanup, state) do
    schedule_executor_cleanup()

    {:noreply, executor_cleanup(state)}
  end

  @impl true
  def handle_info(any, state) do
    Logger.info("Received unexpected message at node manager: #{inspect(any)}")

    {:noreply, state}
  end

  defp schedule_executor_cleanup() do
    Process.send_after(
      self(),
      :schedule_executor_cleanup,
      get_executor_keepalive() + 10_000
    )
  end

  defp get_executor_keepalive() do
    keepalive = Application.get_env(:flezha, :executor_keepalive, "30000")

    cond do
      is_binary(keepalive) -> String.to_integer(keepalive)
      is_integer(keepalive) -> keepalive
    end
  end

  defp executor_cleanup(state) do
    now = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    keep_alive = get_executor_keepalive()

    namespaces = Map.keys(state.executor_namespaces)

    removal =
      Enum.map(namespaces, fn namespace ->
        in_active_executors = get_executor_ids(state, namespace, :inactive)

        removal_executors =
          Enum.map(in_active_executors, fn executor_id ->
            {_status, timestamp} = state.executor_namespaces[namespace][executor_id]

            if timestamp + keep_alive <= now do
              HubManager.get_registry(namespace)
              HubManager.stop_hub(namespace)
              executor_id
            else
              nil
            end
          end)

        {namespace, removal_executors}
      end)

    executor_namespaces =
      Enum.reduce(removal, state.executor_namespaces, fn {namespace,
                                                                removal_executors},
                                                               acc ->
        namespace_executors =
          Enum.reduce(removal_executors, acc[namespace], fn executor_id, acc ->
            if executor_id == nil do
              acc
            else
              Map.delete(acc, executor_id)
            end
          end)

        Map.put(acc, namespace, namespace_executors)
      end)

    state = %{state | executor_namespaces: executor_namespaces}

    # Clean factory refs for removed CEs
    Enum.reduce(removal, state, fn {namespace, removal_executors}, acc ->
      Enum.reduce(removal_executors, acc, fn
        nil, acc -> acc
        ce_id, acc -> remove_ce_factories(acc, namespace, ce_id)
      end)
    end)
  end

  defp try_migrate_executor(state, namespace, executor_id) do
    in_active_executors = get_executor_ids(state, namespace, :inactive)

    Logger.info(
      "Attempting to migrate executor #{executor_id} in namespace #{namespace}"
    )

    case List.first(in_active_executors) do
      nil ->
        {state, MapSet.new()}

      inactive_executor_id ->
        # Capture which relay_ids the migrate path is about to claim so the
        # reissue catch-up can skip them — the migrate already calls
        # Overwatch.executor_migrate, which fires AddRelay.
        claimed_ids =
          HubManager.executor_relays(namespace, inactive_executor_id)
          |> Enum.map(fn {relay_id, _node_pids} -> relay_id end)
          |> MapSet.new()

        namespace_executors =
          executor_reroute(
            state,
            namespace,
            inactive_executor_id,
            [executor_id]
          )

        new_state =
          put_in(state, [:executor_namespaces, namespace], namespace_executors)

        {new_state, claimed_ids}
    end
  end

  @doc false
  # Public for tests. Re-issues AddRelay (via Overwatch.executor_migrate)
  # to the freshly connected executor for every local Overwatch whose registered
  # executor_id no longer points at an active executor. Without this,
  # relays ProcessHub keeps assigned to a dead executor_id sit silent
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
        |> Enum.each(fn {relay_id, pid} ->
          Logger.info(
            "[FleetManager] Reissuing AddRelay for orphan relay " <>
              "#{relay_id} → executor #{new_ce_id}"
          )

          try do
            GenServer.call(pid, {:executor_migrate, new_ce_id})
            HubManager.relay_set_executor(namespace, relay_id, new_ce_id)
          catch
            :exit, exit_reason ->
              Logger.warning(
                "[FleetManager] Failed to reissue relay #{relay_id}: " <>
                  inspect(exit_reason)
              )
          end
        end)

        :ok
    end
  end

  defp get_executor_ids(state, namespace, status) do
    state
    |> Map.get(:executor_namespaces, %{})
    |> Map.get(namespace, %{})
    |> Enum.filter(fn {_, {s, _}} -> s == status end)
    |> Enum.map(fn {executor_id, {_, _}} -> executor_id end)
  end

  defp handle_executor_disconnect(state, namespace, executor_id) do
    if not Map.has_key?(state.executor_namespaces, namespace) do
      state
    else
      do_handle_executor_disconnect(state, namespace, executor_id)
    end
  end

  defp do_handle_executor_disconnect(state, namespace, executor_id) do
    FleetSupervisor.stop_executor_sup(executor_id)

    state = remove_ce_factories(state, namespace, executor_id)

    if HubManager.hub_alive?(namespace) do
      HubManager.unregister_handler(namespace, executor_id)
    end

    executor_ids =
      state
      |> get_executor_ids(namespace, :active)
      |> Enum.filter(fn id -> id != executor_id end)

    other_agents = HubManager.get_hub_nodes(namespace)

    namespace_executors =
      cond do
        !Enum.empty?(executor_ids) ->
          executor_reroute(state, namespace, executor_id, executor_ids)

        Enum.empty?(executor_ids) && !Enum.empty?(other_agents) ->
          hub_migrate(state, namespace, executor_id)

        Enum.empty?(executor_ids) && Enum.empty?(other_agents) ->
          executor_preserve(state, namespace, executor_id)
      end

    EventManager.dispatch(
      :executor_disconnected_event,
      {namespace, executor_id},
      %{members: :local}
    )

    put_in(state, [:executor_namespaces, namespace], namespace_executors)
  end

  defp hub_migrate(_state, namespace, _executor_id) do
    HubManager.stop_hub(namespace)

    %{}
  end

  defp executor_preserve(state, namespace, executor_id) do
    executor = {:inactive, DateTime.utc_now() |> DateTime.to_unix(:millisecond)}

    put_in(state.executor_namespaces[namespace], [executor_id], executor)
  end

  defp executor_reroute(state, namespace, old_executor, available_executors) do
    new_executor = Enum.random(available_executors)

    HubManager.executor_relays(namespace, old_executor)
    |> Enum.each(fn {_relay_id, [{_node, relay_pid}]} ->
      GenServer.call(relay_pid, {:executor_migrate, new_executor})
    end)

    HubManager.executor_switch(namespace, old_executor, new_executor)

    HubManager.unregister_handler(namespace, old_executor)
    HubManager.register_handler(namespace, new_executor)

    overwatchers =
      HubManager.executor_relays(namespace, new_executor)
      |> Enum.map(fn {relay_id, node_pids} ->
        {relay_id, Enum.map(node_pids, &elem(&1, 1))}
      end)

    stream_channels =
      Supervisor.which_children(Identificator.stream_sup_primary(new_executor)) ++
        Supervisor.which_children(Identificator.stream_sup_secondary(new_executor))

    Enum.each(stream_channels, fn {_, pid, _, _} ->
      GenServer.call(pid, {:relay_bulk_insert, overwatchers})
    end)

    Map.delete(state.executor_namespaces[namespace], old_executor)
  end

  defp executor_connect(stream_ref, namespace, transport) do
    executor_id = Identificator.unique_id(namespace)
    control_unit = Identificator.control_unit(executor_id)

    if !HubManager.hub_alive?(namespace) do
      HubManager.start_hub(namespace)
    end

    HubManager.register_handler(namespace, executor_id)

    FleetSupervisor.start_executor_sup(namespace, executor_id, stream_ref, transport)

    # Transfer ownership of the stream to the ControlChannel process
    control_channel_pid =
      ExecutorSupervisor.find_control_channel(executor_id)

    Native.controlling_process(stream_ref, control_channel_pid)
    Native.set_active(stream_ref)

    # Send setup commands
    GenServer.call(control_unit, :command_assign_executor_id)
    GenServer.call(control_unit, :command_sockets_connect)

    {:ok, namespace, executor_id}
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

  defp store_factories(state, namespace, executor_id, factories) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      Enum.reduce(factories, ns_factories, fn factory, acc ->
        factory_id = factory["id"]
        existing = Map.get(acc, factory_id, %{description: nil, ce_ids: MapSet.new()})

        description = factory["description"] || existing.description

        Map.put(acc, factory_id, %{
          description: description,
          ce_ids: MapSet.put(existing.ce_ids, executor_id)
        })
      end)

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end

  defp remove_ce_factories(state, namespace, executor_id) do
    ns_factories = Map.get(state.namespace_factories, namespace, %{})

    ns_factories =
      ns_factories
      |> Enum.map(fn {factory_id, info} ->
        {factory_id, %{info | ce_ids: MapSet.delete(info.ce_ids, executor_id)}}
      end)
      |> Enum.reject(fn {_factory_id, info} -> MapSet.size(info.ce_ids) == 0 end)
      |> Map.new()

    put_in(state, [:namespace_factories, namespace], ns_factories)
  end
end
