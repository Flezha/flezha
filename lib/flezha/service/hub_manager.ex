defmodule Flezha.Service.HubManager do
  alias Flezha.Plugin
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager
  alias Flezha.Service.ProcessorManager

  require Logger

  use Supervisor

  # Mirrors `Flezha.Service.ProcessorManager.init_processor/3`'s retry
  # budget so a transient `:no_response` from the local hub's
  # `dist_sup` (mid-restart, snapshot/restore in flight, parallel-suite
  # churn) does not surface as a hard failure on plugin enable/disable.
  @no_response_retry_attempts 8
  @no_response_retry_initial_ms 150

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def stop_hub_manager() do
    Supervisor.stop(__MODULE__)
  end

  def start_hub(namespace) do
    Supervisor.start_child(__MODULE__, process_hub(namespace))
  end

  def stop_hub(namespace) do
    Supervisor.terminate_child(__MODULE__, Identificator.hub(namespace))
    Supervisor.delete_child(__MODULE__, Identificator.hub(namespace))
  end

  def hub_alive?(namespace) do
    ProcessHub.is_alive?(Identificator.hub(namespace))
  end

  def get_running_namespaces() do
    Supervisor.which_children(__MODULE__)
    |> Enum.map(fn {id, _pid, _type, _modules} -> id end)
    |> Enum.filter(fn id -> ProcessHub.is_alive?(id) end)
    |> Enum.map(&extract_namespace_from_hub/1)
    |> Enum.reject(&is_nil/1)
  end

  defp extract_namespace_from_hub(hub_id) when is_atom(hub_id) do
    hub_id
    |> Atom.to_string()
    |> String.replace_suffix("_hub", "")
  end

  defp extract_namespace_from_hub(_), do: nil

  def get_registry(namespace) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        {:ok, ProcessHub.registry_dump(hub_id)}

      false ->
        {:error, "hub not alive"}
    end
  end

  def get_hub_nodes(namespace, include_local \\ false) do
    opts =
      case include_local do
        true ->
          [:include_local]

        false ->
          []
      end

    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.nodes(hub_id, opts)
    else
      []
    end
  end

  def get_pid(namespace, processor_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.get_pids(hub_id, processor_id) do
      [] ->
        {:error, "no pid found"}

      pids ->
        {:ok, List.first(pids)}
    end
  end

  def get_all_processors(namespace) do
    hub_id = Identificator.hub(namespace)

    if ProcessHub.is_alive?(hub_id) do
      ProcessHub.process_list(hub_id, :global)
      |> Enum.map(fn {processor_id, [{_node, processor_pid}]} ->
        {processor_id, processor_pid}
      end)
    else
      []
    end
  end

  def register_handler(namespace, compute_engine_id) do
    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %HookManager{
          id: compute_engine_id <> "_hub_pid_insertion",
          m: Flezha.Service.HubManager,
          f: :hook_insert_pid,
          a: [compute_engine_id, :_]
        }
      ]
    )

    ProcessHub.register_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [
        %HookManager{
          id: compute_engine_id <> "_hub_pid_removal",
          m: Flezha.Service.HubManager,
          f: :hook_remove_pid,
          a: [compute_engine_id, :_]
        }
      ]
    )
  end

  def unregister_handler(namespace, compute_engine_id) do
    ProcessHub.cancel_hook_handlers(
      Identificator.hub(namespace),
      Hook.child_registered(),
      [compute_engine_id <> "_hub_pid_insertion"]
    )
  end

  def compute_engine_processors(namespace, compute_engine_id) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.tag_query(hub_id, compute_engine_id)
  end

  def compute_engine_switch(namespace, old_compute_engine_id, new_compute_engine_id) do
    hub_id = Identificator.hub(namespace)

    # Updates the compute_engine id in the registry.
    compute_engine_processors(namespace, old_compute_engine_id)
    |> Enum.each(fn {processor_id, [{_node, _processor_pid}]} ->
      processor_set_compute_engine(namespace, processor_id, new_compute_engine_id)
    end)

    # Propagate the change to all nodes in the namespace.
    hub_id
    |> ProcessHub.Coordinator.get_hub()
    |> ProcessHub.Service.Synchronizer.trigger_sync()
  end

  def processor_set_compute_engine(namespace, processor_id, compute_engine_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      true ->
        ProcessRegistry.update(hub_id, processor_id, fn cspec, node_pids, metadata ->
          {cspec, node_pids, Map.put(metadata, :tag, compute_engine_id)}
        end)

        :ok

      false ->
        {:error, "hub not alive"}
    end
  end

  def hook_insert_pid(compute_engine_id, %{child_id: processor_id, node_pids: node_pids}) do
    curr_node = node()

    updated_pids =
      Enum.filter(node_pids, fn {node, _pid} ->
        node === curr_node
      end)
      |> Enum.map(fn {_, pid} -> pid end)

    case updated_pids do
      [] ->
        nil

      _ ->
        stream_channels =
          Supervisor.which_children(Identificator.stream_sup_primary(compute_engine_id)) ++
            Supervisor.which_children(Identificator.stream_sup_secondary(compute_engine_id))

        Enum.each(stream_channels, fn {_, pid, _, _} ->
          GenServer.call(pid, {:processor_pid_update, processor_id, updated_pids})
        end)
    end
  end

  def hook_remove_pid(_compute_engine_id, %{child_id: _processor_id}) do
    # TODO: Implement.
  end

  @doc """
  Apply `update_fn` to the plugin list embedded in a processor's
  ProcessHub child-spec start-args and write the rewritten cspec back
  through `ProcessHub.Service.ProcessRegistry.update/3`.

  `update_fn` receives the current plugin list as `[{plugin_id, %Flezha.Plugin{}}]`
  tuples (existing cold-start cspec entries stored as bare `%Flezha.Plugin{}`
  structs are normalised to tuples on read) and returns the new list in
  the same shape.

  Both child-spec start-args tuple shapes Flezha emits are supported:

    * `{namespace, processor}` — cold-start shape written by
      `Flezha.Service.ProcessorManager.init_processor/3`.
    * `{namespace, compute_engine_id, processor}` — post-`alter_child_data/2`
      shape.

  Wraps the registry update in the `:no_response` retry shape used by
  `processor_manager.ex:157-167` (8 attempts, exponential backoff capped
  at 5 s). `:no_response` here surfaces when the hub's registry
  GenServer is mid-restart alongside `dist_sup`; the dist_sup heals on
  its own and the next attempt succeeds. Stable-state failure (8
  attempts, ~14 s of wait) propagates.
  """
  @spec update_processor_plugins(
          String.t(),
          String.t(),
          ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])
        ) :: :ok | {:error, term()}
  def update_processor_plugins(namespace, processor_id, update_fn)
      when is_function(update_fn, 1) do
    do_update_processor_plugins(namespace, processor_id, update_fn, 0)
  end

  @doc """
  Read the current plugin list embedded in a processor's child-spec
  start args. Returns `{:ok, [{plugin_id, %Flezha.Plugin{}}]}` (entries
  are normalised to tuple form regardless of how they are stored).
  """
  @spec processor_plugins(String.t(), String.t()) ::
          {:ok, [{binary(), Plugin.t()}]} | {:error, term()}
  def processor_plugins(namespace, processor_id) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      false ->
        {:error, "hub not alive"}

      true ->
        case ProcessRegistry.lookup(hub_id, processor_id) do
          nil ->
            {:error, "processor not found"}

          {%{start: {_m, _f, [arg]}}, _node_pids} ->
            {:ok, normalize_plugins(extract_plugins(arg))}

          {%{start: {_m, _f, [arg]}}, _node_pids, _metadata} ->
            {:ok, normalize_plugins(extract_plugins(arg))}
        end
    end
  end

  defp do_update_processor_plugins(namespace, processor_id, update_fn, attempt) do
    hub_id = Identificator.hub(namespace)

    case ProcessHub.is_alive?(hub_id) do
      false ->
        {:error, "hub not alive"}

      true ->
        result =
          try do
            ProcessRegistry.update(hub_id, processor_id, fn cspec, node_pids, metadata ->
              {update_cspec_plugins(cspec, update_fn), node_pids, metadata}
            end)
          catch
            :exit, _ -> {:error, :no_response}
          end

        cond do
          result == :ok ->
            :ok

          match?({:error, :no_response}, result) and
              attempt < @no_response_retry_attempts ->
            backoff =
              min(@no_response_retry_initial_ms * Bitwise.bsl(1, attempt), 5_000)

            Process.sleep(backoff)
            do_update_processor_plugins(namespace, processor_id, update_fn, attempt + 1)

          true ->
            result
        end
    end
  end

  defp update_cspec_plugins(%{start: {m, f, [arg]}} = cspec, update_fn) do
    new_arg =
      case arg do
        {namespace, processor} ->
          {namespace, rewrite_processor_plugins(processor, update_fn)}

        {namespace, compute_engine_id, processor} ->
          {namespace, compute_engine_id, rewrite_processor_plugins(processor, update_fn)}
      end

    Map.put(cspec, :start, {m, f, [new_arg]})
  end

  defp rewrite_processor_plugins(processor, update_fn) do
    current = normalize_plugins(extract_plugins({nil, processor}))
    new_plugins = update_fn.(current)
    new_opts = Keyword.put(processor.opts, :plugins, new_plugins)
    %{processor | opts: new_opts}
  end

  defp extract_plugins({_namespace, processor}),
    do: Keyword.get(processor.opts, :plugins, [])

  defp extract_plugins({_namespace, _compute_engine_id, processor}),
    do: Keyword.get(processor.opts, :plugins, [])

  defp normalize_plugins(plugins) when is_list(plugins) do
    Enum.map(plugins, fn
      {id, %Plugin{} = plugin} -> {id, plugin}
      %Plugin{id: id} = plugin -> {id, plugin}
    end)
  end

  def alter_child_data(_hub_id, %{child_spec: cs} = child_data) do
    %{start: {m, f, [arg]}} = cs

    {namespace, processor} =
      case arg do
        {namespace, processor} ->
          {namespace, processor}

        {namespace, _existing_compute_engine, processor} ->
          {namespace, processor}
      end

    factory_id = Map.get(processor, :factory_id)
    assigned_compute_engine = ProcessorManager.assign_compute_engine(namespace, factory_id)

    Logger.info(
      "[hub_manager] alter_child_data processor=#{inspect(Map.get(processor, :id))} " <>
        "factory=#{inspect(factory_id)} assigned_ce=#{inspect(assigned_compute_engine)}"
    )

    if is_nil(assigned_compute_engine) do
      raise "No active compute engines available in namespace #{namespace} for factory #{inspect(factory_id)}"
    end

    new_args = [{namespace, assigned_compute_engine, processor}]

    child_data
    |> put_in([:child_spec, :start], {m, f, new_args})
    |> put_in([:metadata, :tag], assigned_compute_engine)
  end

  @impl true
  def init(_init_arg) do
    Supervisor.init([], strategy: :one_for_one)
  end

  defp process_hub(namespace) do
    hub_id = Identificator.hub(namespace)

    # TODO: The hub settings should be configurable.
    ProcessHub.child_spec(%ProcessHub{
      hub_id: hub_id,
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      # TODO: we may want to provide options for the partition tolerance strategy.
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
      distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
      migration_strategy: %ProcessHub.Strategy.Migration.HotSwap{
        handover: true
      },
      # Processor startup involves multiple GenServer hops and socket communication
      # to the compute engine (Rust), so the default 5s is too tight.
      cross_node_request_timeout: 10_000,
      hooks: %{
        Hook.child_data_alter() => [
          %HookManager{
            id: ProcessHub.Constant.Hook.child_data_alter(),
            m: __MODULE__,
            f: :alter_child_data,
            a: [hub_id, :_]
          }
        ]
      }
    })
  end
end
