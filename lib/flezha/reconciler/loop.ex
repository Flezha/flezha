defmodule Flezha.Reconciler.Loop do
  @moduledoc """
  The authoritative reconciliation control-loop.

  Treats the durable ProcessHub registry as *desired* state and drives *actual*
  state toward it. It reconciles a namespace's relays on events — executor
  connect/disconnect (`Flezha.Service.EventManager`) and ProcessHub registry/cluster
  hooks (`child_registered`/`child_unregistered`/`post_node_join`/`post_node_leave`/
  `post_recovery_replay`) — each debounced and targeted, plus a bounded periodic
  full sweep (`:reconcile_sweep_interval_ms`) as the backstop for anything no event
  covered (e.g. a `kill -9` node that never `nodedown`'d).

  Reconcile is gated on `Flezha.Cluster.Orchestrator.cluster_ready?/0` so it never
  acts before the cluster has formed. The cluster-wide, registry-mutating actions
  run only on the elected leader (`Flezha.Reconciler.Leader.is_leader?/0`); the
  node-local actions always run, so the data path is never leaderless-blocked. The
  loop never touches the per-frame send path.
  """

  use GenServer

  alias Flezha.Cluster.Orchestrator
  alias Flezha.Executor.FleetManager
  alias Flezha.Reconciler.{Actions, Health, Leader, Probe}
  alias Flezha.Service.{EventManager, HubManager, Identificator}
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.{HookManager, ProcessRegistry}

  require Logger

  @hooks [
    Hook.child_registered(),
    Hook.child_unregistered(),
    Hook.post_node_join(),
    Hook.post_node_leave(),
    Hook.post_recovery_replay()
  ]

  defstruct sweep_interval_ms: 20_000,
            debounce_ms: 250,
            pending: MapSet.new(),
            hooked: MapSet.new(),
            health: %{}

  # ---------- API ----------

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @doc """
  Reconcile one namespace synchronously and return `[{relay_id, health, actions}]`.
  Primarily a test/operability seam; the loop drives itself via events + sweep.
  """
  @spec reconcile_namespace(binary()) :: [{binary(), Health.t(), [Actions.action()]}]
  def reconcile_namespace(namespace) do
    GenServer.call(__MODULE__, {:reconcile_namespace, namespace}, 30_000)
  end

  @doc "ProcessHub hook callback: enqueue a debounced reconcile for the hook's hub."
  @spec hook_event(atom(), map()) :: :ok
  def hook_event(hub_id, _hook_data) do
    GenServer.cast(__MODULE__, {:hook_trigger, hub_id})
  catch
    _, _ -> :ok
  end

  # ---------- callbacks ----------

  @impl true
  def init(opts) do
    EventManager.add_handler(:executor_connected_event)
    EventManager.add_handler(:executor_disconnected_event)

    state = %__MODULE__{
      sweep_interval_ms:
        Keyword.get(opts, :sweep_interval_ms, config(:reconcile_sweep_interval_ms, 20_000)),
      debounce_ms: Keyword.get(opts, :debounce_ms, config(:reconcile_debounce_ms, 250))
    }

    schedule_sweep(state)
    {:ok, state}
  end

  @impl true
  def handle_call({:reconcile_namespace, namespace}, _from, state) do
    {results, state} = reconcile(namespace, state)
    {:reply, results, state}
  end

  @impl true
  def handle_info(:sweep, state) do
    state =
      Enum.reduce(running_namespaces(), state, fn namespace, acc ->
        {_results, acc} = reconcile(namespace, acc)
        acc
      end)

    schedule_sweep(state)
    {:noreply, state}
  end

  def handle_info({:executor_connected_event, {namespace, _id}}, state) do
    {:noreply, schedule_debounced(namespace, state)}
  end

  def handle_info({:executor_disconnected_event, {namespace, _id}}, state) do
    {:noreply, schedule_debounced(namespace, state)}
  end

  def handle_info({:debounced_reconcile, namespace}, state) do
    {_results, state} =
      reconcile(namespace, %{state | pending: MapSet.delete(state.pending, namespace)})

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  @impl true
  def handle_cast({:hook_trigger, hub_id}, state) do
    {:noreply, schedule_debounced(namespace_from_hub(hub_id), state)}
  end

  # ---------- reconcile ----------

  defp reconcile(namespace, state) do
    if Orchestrator.cluster_ready?() do
      state = ensure_hub_hooks(namespace, state)
      is_leader? = leader?()
      hub_id = Identificator.hub(namespace)

      case safe_dump(hub_id) do
        {:ok, entries} ->
          ctx = build_ctx(namespace)

          {results, health} =
            Enum.map_reduce(entries, state.health, fn {relay_id, entry}, health_acc ->
              probe = Probe.probe(namespace, relay_id, entry, ctx)
              health = Health.classify(probe)
              actions = Actions.converge(probe, health, is_leader?)

              health_acc =
                track_health(namespace, relay_id, probe, health, is_leader?, health_acc)

              {{relay_id, health, actions}, health_acc}
            end)

          {results, %{state | health: prune_health(health, namespace, entries)}}

        :error ->
          {[], state}
      end
    else
      {[], state}
    end
  end

  defp build_ctx(namespace) do
    active = MapSet.new(safe_call(fn -> FleetManager.active_executors(namespace) end, []))

    %{
      active_executors: active,
      factory_available?: fn factory_id ->
        match?(
          [_ | _],
          safe_call(fn -> FleetManager.executors_for_factory(namespace, factory_id) end, [])
        )
      end
    }
  end

  # Persist + emit a health transition only for the relay's authority node (the
  # node that owns its pid, or the leader when it has no live pid) so peers do not
  # race the same metadata write.
  defp track_health(namespace, relay_id, probe, health, is_leader?, health_acc) do
    key = {namespace, relay_id}
    previous = Map.get(health_acc, key)

    authority? = probe.local? or (is_leader? and not probe.relay_alive?)

    if authority? and health != previous do
      persist_health(namespace, relay_id, health)

      :telemetry.execute(
        [:flezha, :reconcile, :health],
        %{count: 1},
        %{
          node: node(),
          namespace: namespace,
          relay_id: relay_id,
          health: health,
          previous: previous
        }
      )
    end

    Map.put(health_acc, key, health)
  end

  defp persist_health(namespace, relay_id, health) do
    hub_id = Identificator.hub(namespace)

    try do
      ProcessRegistry.update(hub_id, relay_id, fn cspec, node_pids, metadata ->
        {cspec, node_pids, Map.put(metadata, :health, health)}
      end)
    catch
      _, _ -> :ok
    end
  end

  # Forget tracked health for relays no longer in the namespace's registry.
  defp prune_health(health, namespace, entries) do
    present = MapSet.new(entries, fn {relay_id, _} -> {namespace, relay_id} end)

    Map.filter(health, fn {{ns, _rid} = key, _} ->
      ns != namespace or MapSet.member?(present, key)
    end)
  end

  # ---------- triggers / hooks ----------

  defp schedule_debounced(namespace, state) do
    if MapSet.member?(state.pending, namespace) do
      state
    else
      Process.send_after(self(), {:debounced_reconcile, namespace}, state.debounce_ms)
      %{state | pending: MapSet.put(state.pending, namespace)}
    end
  end

  defp schedule_sweep(%{sweep_interval_ms: ms}) when is_integer(ms) and ms > 0 do
    Process.send_after(self(), :sweep, ms)
  end

  defp schedule_sweep(_), do: :ok

  # Register the loop's reconcile-triggering hooks on a hub once. The handlers
  # only `cast` to this loop (never call back into the coordinator), so they are
  # safe to fire from the synchronous coordinator hooks.
  defp ensure_hub_hooks(namespace, state) do
    hub_id = Identificator.hub(namespace)

    if MapSet.member?(state.hooked, hub_id) do
      state
    else
      Enum.each(@hooks, fn hook ->
        try do
          ProcessHub.register_hook_handlers(hub_id, hook, [
            %HookManager{
              id: "flezha_reconciler_#{hook}_#{hub_id}",
              m: __MODULE__,
              f: :hook_event,
              a: [hub_id, :_]
            }
          ])
        catch
          _, _ -> :ok
        end
      end)

      %{state | hooked: MapSet.put(state.hooked, hub_id)}
    end
  end

  # ---------- helpers ----------

  defp leader? do
    Leader.is_leader?()
  catch
    _, _ -> false
  end

  defp running_namespaces do
    HubManager.get_running_namespaces()
  catch
    _, _ -> []
  end

  defp safe_dump(hub_id) do
    {:ok, ProcessRegistry.dump(hub_id)}
  catch
    _, _ -> :error
  end

  defp safe_call(fun, default) do
    fun.()
  catch
    _, _ -> default
  end

  defp config(key, default), do: Application.get_env(:flezha, key, default)

  defp namespace_from_hub(hub_id) do
    hub_id |> Atom.to_string() |> String.replace_suffix("_hub", "")
  end
end
