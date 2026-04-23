defmodule Flezha.Recovery.Coordinator do
  @moduledoc """
  Bridges ProcessHub + PluginManager lifecycle events to the recovery stores
  and runs the two-phase recovery loop when `Flezha.Recovery.SysMode`
  requests it.

  Plugin rows persisted here are `%Flezha.Plugin{}` structs with transient
  fields (worker pids, KafkaEx conns, Websocket conns) stripped. The stored
  row carries a `:version` tag — on boot, mismatched versions clear the
  DETS file to prevent struct-shape drift from leaking across releases.
  """

  use GenServer

  require Logger

  alias Flezha.Plugin
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Recovery.PluginStore
  alias Flezha.Recovery.Store
  alias Flezha.Recovery.SysMode

  @ets :flz_recovery_abort
  @abort_key :abort

  # Bump when the persisted shape of `%Flezha.Plugin{}` changes in a way
  # that requires discarding on-disk rows.
  @plugin_shape_version 2

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc "Asynchronously starts the recovery loop."
  @spec start_recovery_async() :: :ok
  def start_recovery_async do
    GenServer.cast(__MODULE__, :start_recovery_async)
  end

  @doc "Synchronously runs the recovery loop and returns combined counts."
  @spec run_recovery() :: map()
  def run_recovery do
    GenServer.call(__MODULE__, :run_recovery, 60_000)
  end

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

  ## ProcessHub hook handlers

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

  def on_child_unregistered(%{child_id: child_id}) when is_binary(child_id) do
    Store.delete(child_id)
    :ok
  end

  def on_child_unregistered(_), do: :ok

  ## PluginManager hook handlers

  @spec on_plugin_enabled(binary(), Plugin.t()) :: :ok
  def on_plugin_enabled(plugin_id, %Plugin{} = plugin) do
    try do
      if recoverable?(plugin) do
        stripped = strip_transient_fields(plugin)
        row = %{plugin: stripped, version: @plugin_shape_version}
        PluginStore.put(plugin_id, row)
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
  persisting. Re-enable re-populates them via `init_provider/1`.
  """
  @spec strip_transient_fields(Plugin.t()) :: Plugin.t()
  def strip_transient_fields(%Plugin{opts: opts} = plugin) do
    %Plugin{plugin | opts: strip_opts(opts)}
  end

  defp strip_opts(opts) when is_map(opts) do
    opts
    |> Map.delete(:conn)
    |> Map.delete(:binding_pid)
  end

  defp strip_opts(opts), do: opts

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    ensure_ets()
    set_abort_flag(false)

    # Clear any persisted plugin rows whose shape version doesn't match.
    clear_incompatible_plugin_rows()

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
      {:ok, namespace, processor} -> Store.put(child_id, namespace, processor)
      :skip -> :ok
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
    installer_ids = installed_ids_safe()

    Enum.reduce_while(rows, {0, 0, 0, false}, fn
      {plugin_id, %{plugin: %Plugin{} = plugin}}, {ok, err, skipped, _} ->
        if abort_flag?() do
          {:halt, {ok, err, skipped, true}}
        else
          cond do
            # If the installer is running, persisted plugin rows whose id
            # belongs to the installer's catalog must still be there when we
            # restart. Missing means the operator uninstalled out-of-band;
            # skip the row rather than crash the recovery loop.
            orphaned_installed_plugin?(plugin_id, installer_ids) ->
              Logger.warning(
                "[recovery] skipping orphaned plugin row #{inspect(plugin_id)} — " <>
                  "installer catalog no longer tracks it"
              )

              :telemetry.execute(
                [:flezha, :recovery, :orphaned_plugin],
                %{count: 1},
                %{plugin_id: plugin_id}
              )

              PluginStore.delete(plugin_id)
              {:cont, {ok, err, skipped + 1, false}}

            not processor_present?(plugin) ->
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

      {plugin_id, _malformed_row}, {ok, err, skipped, _} ->
        Logger.warning(
          "[recovery] dropping malformed plugin row #{inspect(plugin_id)}"
        )

        PluginStore.delete(plugin_id)
        {:cont, {ok, err, skipped + 1, false}}
    end)
  end

  # Returns `:none` if the installer isn't started (feature flag off) or
  # the set of known installer-owned plugin ids otherwise. `:none` disables
  # the orphan check — legacy compile-time plugins aren't in any catalog.
  defp installed_ids_safe do
    if function_exported?(Flezha.Plugin.Installer, :installed_ids, 0) do
      case Process.whereis(Flezha.Plugin.Installer) do
        nil -> :none
        _ -> Flezha.Plugin.Installer.installed_ids()
      end
    else
      :none
    end
  rescue
    _ -> :none
  end

  defp orphaned_installed_plugin?(_plugin_id, :none), do: false

  # An installer-owned recovery row is orphaned iff its plugin id is a
  # provision id (plugin_id:provision_id shape) whose package is no longer
  # tracked by the Installer catalog. For legacy (non-installer) rows the
  # id has no colon and we leave the row alone.
  defp orphaned_installed_plugin?(plugin_id, id_set) do
    with [pkg, _provision] <- String.split(plugin_id, ":", parts: 2) do
      not MapSet.member?(id_set, pkg)
    else
      _ -> false
    end
  end

  defp processor_present?(%Plugin{namespace: ns, processor_id: pid_id}) do
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

  defp classify_plugin_error("plugin already registered"),
    do: {:error, "plugin already registered", :permanent}

  defp classify_plugin_error(%UndefinedFunctionError{} = e), do: {:error, e, :permanent}
  defp classify_plugin_error({:undef, _} = e), do: {:error, e, :permanent}
  defp classify_plugin_error(reason), do: {:error, reason, :transient}

  # A plugin is recoverable iff none of its subscriptions carry a
  # transient-bound provider (e.g. a caller pid that won't survive a BEAM
  # restart). The test fixtures currently flag `BeamMsgForward` and
  # `MonitoredIOForward` as non-recoverable.
  defp recoverable?(%Plugin{provider: provider}) do
    provider not in [BeamMsgForward, MonitoredIOForward]
  end

  defp clear_incompatible_plugin_rows do
    try do
      rows = PluginStore.all()

      Enum.each(rows, fn {id, row} ->
        case row do
          %{version: v} when v == @plugin_shape_version -> :ok
          _ -> PluginStore.delete(id)
        end
      end)
    catch
      _, _ -> :ok
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
