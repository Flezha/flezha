defmodule Flezha.Recovery.Coordinator do
  @moduledoc """
  Bridges ProcessHub + PluginManager lifecycle events to the recovery stores
  and runs the two-phase recovery loop when `Flezha.Recovery.SysMode`
  requests it.

  Processor hooks are registered in each hub's spec (see
  `Flezha.Service.HubManager.process_hub/1`); plugin hooks fire from
  `Flezha.Service.PluginManager` directly into this module's
  `on_plugin_enabled/2` / `on_plugin_disabled/1`.

  The Coordinator exposes its own ETS table (`:flz_recovery_abort`) as the
  shared abort-flag carrier so the recovery loop can poll it between rows
  without blocking on a GenServer call from SysMode. The flag halts whichever
  phase (processors or plugins) is currently in-flight.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin.Driver.IOBindingPlugin
  alias Flezha.Plugin.Provider.Kafka
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Recovery.PluginStore
  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode

  @ets :flz_recovery_abort
  @abort_key :abort

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Asynchronously starts the recovery loop. Called by SysMode when its window
  timer fires while in `:recover`. When finished, notifies SysMode via
  `Flezha.Recovery.SysMode.notify_recovery_done/1`.
  """
  @spec start_recovery_async() :: :ok
  def start_recovery_async do
    GenServer.cast(__MODULE__, :start_recovery_async)
  end

  @doc """
  Synchronously runs the recovery loop and returns the combined
  processor + plugin counts. Primarily used by tests; SysMode uses
  `start_recovery_async/0`.
  """
  @spec run_recovery() :: map()
  def run_recovery do
    GenServer.call(__MODULE__, :run_recovery, 60_000)
  end

  @doc "Enable or disable the mid-loop abort flag."
  @spec set_abort_flag(boolean()) :: :ok
  def set_abort_flag(value) when is_boolean(value) do
    ensure_ets()
    :ets.insert(@ets, {@abort_key, value})
    :ok
  end

  @doc false
  def abort_flag? do
    ensure_ets()

    case :ets.lookup(@ets, @abort_key) do
      [{@abort_key, true}] -> true
      _ -> false
    end
  end

  ## ProcessHub hook handlers (wired via HubManager)

  @doc """
  Registered as `Hook.post_children_start()`. Payload shape:
  `%{children: [%{child_id, pid, result, nodes, child_spec, metadata}]}`.

  For each child hosted on the current node, persists a row in the Store.
  """
  def on_post_children_start(%{children: children}) when is_list(children) do
    current_node = node()

    Enum.each(children, fn
      %{child_id: child_id, nodes: nodes, child_spec: child_spec, metadata: metadata} ->
        if current_node in List.wrap(nodes) do
          persist_child(child_id, child_spec, metadata)
        end

      _ ->
        :ok
    end)

    :ok
  end

  def on_post_children_start(_), do: :ok

  @doc """
  Registered as `Hook.child_unregistered()`. Payload shape: `%{child_id: cid}`.
  """
  def on_child_unregistered(%{child_id: child_id}) when is_binary(child_id) do
    Store.delete(child_id)
    :ok
  end

  def on_child_unregistered(_), do: :ok

  ## PluginManager hook handlers (called directly from PluginManager)

  @doc """
  Invoked by `PluginManager` after a successful `enable_plugin/2`. Persists
  the plugin iff its driver reports `recoverable?/1 == true`.
  """
  @spec on_plugin_enabled(binary(), struct()) :: :ok
  def on_plugin_enabled(plugin_id, plugin) do
    try do
      if recoverable?(plugin) do
        stripped = strip_transient_fields(plugin)
        PluginStore.put(plugin_id, stripped)
      end

      :ok
    catch
      kind, reason ->
        Logger.warning(
          "[recovery] on_plugin_enabled raised #{inspect(kind)}: #{inspect(reason)}"
        )

        :ok
    end
  end

  @doc """
  Invoked by `PluginManager` after a successful `disable_plugin/1`. Removes
  any persisted row for the given plugin_id (idempotent).
  """
  @spec on_plugin_disabled(binary()) :: :ok
  def on_plugin_disabled(plugin_id) do
    try do
      PluginStore.delete(plugin_id)
      :ok
    catch
      kind, reason ->
        Logger.warning(
          "[recovery] on_plugin_disabled raised #{inspect(kind)}: #{inspect(reason)}"
        )

        :ok
    end
  end

  ## Transient-field stripping

  @doc """
  Nils fields that hold a live pid or external-connection ref before
  persisting. Re-enable re-populates them by invoking the driver's normal
  enable path. See design §D12.
  """
  @spec strip_transient_fields(struct()) :: struct()
  def strip_transient_fields(%IOBindingPlugin{} = plugin) do
    %IOBindingPlugin{plugin | pid: nil, monitor_ref: nil, provider: strip_provider(plugin.provider)}
  end

  def strip_transient_fields(other), do: other

  defp strip_provider(%Kafka.Producer{} = p), do: %Kafka.Producer{p | conn: nil}
  defp strip_provider(%Websocket{} = p), do: %Websocket{p | conn: nil}
  defp strip_provider(other), do: other

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    ensure_ets()
    set_abort_flag(false)
    {:ok, %{}}
  end

  @impl true
  def handle_cast(:start_recovery_async, state) do
    result = do_run_recovery()
    SysMode.notify_recovery_done(result)
    {:noreply, state}
  end

  @impl true
  def handle_call(:run_recovery, _from, state) do
    {:reply, do_run_recovery(), state}
  end

  ## Private helpers

  defp persist_child(child_id, child_spec, metadata) do
    case extract_namespace_and_processor(child_spec, metadata) do
      {:ok, namespace, processor} ->
        Store.put(child_id, namespace, processor)

      :skip ->
        :ok
    end
  end

  defp extract_namespace_and_processor(%{start: {_m, _f, [arg]}}, _metadata) do
    case arg do
      {namespace, %Flezha.Processor.Builder{} = processor} ->
        {:ok, namespace, processor}

      {namespace, _compute_engine_id, %Flezha.Processor.Builder{} = processor} ->
        {:ok, namespace, processor}

      _ ->
        :skip
    end
  end

  defp extract_namespace_and_processor(_, _), do: :skip

  defp do_run_recovery do
    t0 = System.monotonic_time(:millisecond)

    {proc_recovered, proc_failed, aborted_after_proc} = run_processor_phase()

    {plugin_recovered, plugin_failed, plugin_skipped_missing_processor, aborted_after_plugin} =
      if aborted_after_proc do
        {0, 0, 0, true}
      else
        run_plugin_phase()
      end

    duration_ms = System.monotonic_time(:millisecond) - t0
    aborted = aborted_after_proc or aborted_after_plugin

    measurements = %{
      # Back-compat names for existing :completed consumers.
      recovered: proc_recovered,
      failed: proc_failed,
      duration_ms: duration_ms,
      plugins_recovered: plugin_recovered,
      plugins_failed: plugin_failed,
      plugins_skipped_missing_processor: plugin_skipped_missing_processor
    }

    :telemetry.execute(
      [:flezha, :recovery, :completed],
      measurements,
      %{
        aborted: aborted,
        node: node(),
        total_rows: proc_recovered + proc_failed,
        total_plugin_rows: plugin_recovered + plugin_failed + plugin_skipped_missing_processor
      }
    )

    %{
      recovered: proc_recovered,
      failed: proc_failed,
      plugins_recovered: plugin_recovered,
      plugins_failed: plugin_failed,
      plugins_skipped_missing_processor: plugin_skipped_missing_processor,
      duration_ms: duration_ms,
      aborted: aborted
    }
  end

  defp run_processor_phase do
    rows = Store.all()

    Enum.reduce_while(rows, {0, 0, false}, fn {child_id, row}, {ok, err, _} ->
      if abort_flag?() do
        {:halt, {ok, err, true}}
      else
        case attempt_start(row) do
          :ok ->
            {:cont, {ok + 1, err, false}}

          {:error, reason, :permanent} ->
            Logger.warning(
              "[recovery] dropping stale row #{inspect(child_id)} — #{inspect(reason)}"
            )

            Store.delete(child_id)
            {:cont, {ok, err + 1, false}}

          {:error, reason, :transient} ->
            Logger.warning(
              "[recovery] transient failure for #{inspect(child_id)} — #{inspect(reason)}"
            )

            {:cont, {ok, err + 1, false}}
        end
      end
    end)
  end

  defp run_plugin_phase do
    rows = PluginStore.all()

    Enum.reduce_while(rows, {0, 0, 0, false}, fn
      {plugin_id, %{plugin: plugin}}, {ok, err, skipped, _} ->
        if abort_flag?() do
          {:halt, {ok, err, skipped, true}}
        else
          case processor_present?(plugin) do
            false ->
              Logger.warning(
                "[recovery] dropping plugin #{inspect(plugin_id)} — owning processor " <>
                  "#{inspect(plugin.processor_id)} in namespace " <>
                  "#{inspect(plugin.namespace)} not running after phase 1"
              )

              PluginStore.delete(plugin_id)
              {:cont, {ok, err, skipped + 1, false}}

            true ->
              case attempt_plugin_enable(plugin_id, plugin) do
                :ok ->
                  {:cont, {ok + 1, err, skipped, false}}

                {:error, reason, :permanent} ->
                  Logger.warning(
                    "[recovery] dropping plugin row #{inspect(plugin_id)} — " <>
                      "#{inspect(reason)}"
                  )

                  PluginStore.delete(plugin_id)
                  {:cont, {ok, err + 1, skipped, false}}

                {:error, reason, :transient} ->
                  Logger.warning(
                    "[recovery] transient plugin-enable failure for " <>
                      "#{inspect(plugin_id)} — #{inspect(reason)}"
                  )

                  {:cont, {ok, err + 1, skipped, false}}
              end
          end
        end
    end)
  end

  defp processor_present?(%{namespace: ns, processor_id: pid_id}) do
    case Flezha.Service.ProcessorManager.get_processor_pid(ns, pid_id) do
      {:ok, pid} when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  end

  defp processor_present?(_), do: false

  defp attempt_start(%{namespace: namespace, processor: processor}) do
    case Flezha.Service.ProcessorManager.start_processor(namespace, processor) do
      {:ok, _info} ->
        :ok

      {:error, reason} when is_binary(reason) ->
        {:error, reason, :permanent}

      {:error, %{reason: reason}} ->
        {:error, reason, :transient}

      other ->
        {:error, other, :transient}
    end
  end

  defp attempt_plugin_enable(plugin_id, plugin) do
    try do
      case Flezha.Service.PluginManager.enable_plugin(plugin_id, plugin) do
        {:ok, _msg} -> :ok
        {:error, reason} -> classify_plugin_error(reason)
      end
    catch
      :exit, {:noproc, _} -> {:error, :plugin_manager_not_running, :transient}
      :exit, reason -> {:error, reason, :transient}
      kind, reason -> {:error, {kind, reason}, :transient}
    end
  end

  # Permanent errors are those where retrying without intervention won't help:
  # module missing, id collision, or explicit validation failures. Everything
  # else (network errors, broker unreachable, etc.) stays transient so the
  # next recovery window gets another shot.
  defp classify_plugin_error("plugin already registered"), do: {:error, "plugin already registered", :permanent}
  defp classify_plugin_error(%UndefinedFunctionError{} = e), do: {:error, e, :permanent}
  defp classify_plugin_error({:undef, _} = e), do: {:error, e, :permanent}
  defp classify_plugin_error(reason), do: {:error, reason, :transient}

  defp recoverable?(plugin) do
    module = plugin.__struct__

    if function_exported?(module, :recoverable?, 1) do
      module.recoverable?(plugin)
    else
      true
    end
  end

  defp ensure_ets do
    case :ets.whereis(@ets) do
      :undefined ->
        _ = :ets.new(@ets, [:named_table, :public, :set, read_concurrency: true])
        :ok

      _ ->
        :ok
    end
  end
end
