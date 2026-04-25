defmodule Flezha.Cluster.Snapshot do
  @moduledoc """
  Per-node API for the cluster snapshot/restore protocol.

  `local_state/0` is invoked by the snapshot coordinator on every reachable
  peer (including the initiator) and returns this node's hosted processors,
  their plugins, and the most-recent captured state binaries.

  `apply_local_state/1` is invoked by the restore coordinator on each target
  node with the slice of state belonging to it; it spawns processors with
  `:initial_state_binary` set in opts and re-enables plugins.

  The two functions form the public RPC surface — keep their argument shapes
  stable across schema versions, or bump
  `Flezha.Cluster.Snapshot.Encoding`'s version.
  """

  alias Flezha.Cluster.Snapshot.PluginRecoverability
  alias Flezha.Plugin
  alias Flezha.Processor.Builder
  alias Flezha.Service.HubManager
  alias Flezha.Service.PluginManager
  alias Flezha.Service.ProcessorManager

  require Logger

  @type processor_entry :: %{
          processor: Builder.t(),
          namespace: binary(),
          last_observed_node: node()
        }

  @type plugin_entry :: %{
          plugin: Plugin.t(),
          last_observed_node: node()
        }

  @type local_state :: %{
          node: node(),
          processors: [processor_entry()],
          plugins: [plugin_entry()],
          captured_states: %{binary() => binary()}
        }

  @doc """
  Collects this node's locally-hosted processors, their currently-subscribed
  plugins, and the most recent state-snapshot binary for each.

  Built from ProcessHub's authoritative `process_list` per running namespace
  rather than DETS rows so the path works regardless of the deprecated
  recovery store's enabled/disabled state.
  """
  @spec local_state() :: local_state()
  def local_state do
    pid_pairs = local_processor_pids()
    entries = Enum.map(pid_pairs, &fetch_snapshot_data/1)
    valid = Enum.reject(entries, &is_nil/1)

    processors =
      Enum.map(valid, fn %{namespace: ns, builder: builder} ->
        %{processor: builder, namespace: to_string(ns), last_observed_node: node()}
      end)

    plugins = collect_plugins(valid)

    captured_states =
      valid
      |> Enum.filter(fn %{snapshot_binary: bin} -> is_binary(bin) end)
      |> Enum.into(%{}, fn %{builder: %Builder{id: id}, snapshot_binary: bin} -> {id, bin} end)

    %{
      node: node(),
      processors: processors,
      plugins: plugins,
      captured_states: captured_states
    }
  end

  @doc """
  Apply a slice of snapshot state on this node.

  For `{:processor, builder, namespace, captured_state_binary}`, starts the
  processor via `Flezha.Service.ProcessorManager.start_processor/2`, threading
  the state binary into `opts[:initial_state_binary]` when present so
  `Overwatch` can prime the SDK before the first input.

  For `{:plugin, plugin}`, calls `Flezha.Service.PluginManager.enable_plugin/2`.
  """
  @spec apply_local_state(
          {:processor, Builder.t(), binary(), binary() | nil}
          | {:plugin, Plugin.t()}
        ) :: {:ok, term()} | {:error, term()}
  def apply_local_state({:processor, %Builder{} = builder, namespace, state_binary})
      when is_binary(namespace) do
    new_opts =
      if is_binary(state_binary) do
        Keyword.put(builder.opts, :initial_state_binary, state_binary)
      else
        builder.opts
      end

    new_builder = %Builder{builder | opts: new_opts}

    case ProcessorManager.start_processor(namespace, new_builder) do
      {:ok, info} ->
        {:ok, info}

      {:error, %{reason: "child already started"}} ->
        Logger.info(
          "[snapshot] processor #{builder.id} already running on #{node()} — skipping apply"
        )

        {:ok, :already_registered}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def apply_local_state({:plugin, %Plugin{id: id} = plugin}) do
    case PluginManager.enable_plugin(id, plugin) do
      {:ok, msg} ->
        {:ok, msg}

      {:error, "plugin already registered"} ->
        {:ok, :already_registered}

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Helpers

  defp local_processor_pids do
    HubManager.get_running_namespaces()
    |> Enum.flat_map(&HubManager.get_all_processors/1)
    |> Enum.filter(fn {_id, pid} -> is_pid(pid) and node(pid) == node() end)
  end

  defp fetch_snapshot_data({_processor_id, pid}) do
    GenServer.call(pid, :get_snapshot_data, 5_000)
  catch
    kind, reason ->
      Logger.warning(
        "[snapshot] get_snapshot_data failed for #{inspect(pid)}: #{inspect({kind, reason})}"
      )

      nil
  end

  defp collect_plugins(entries) do
    entries
    |> Enum.flat_map(fn %{plugins: plugins} -> plugins end)
    |> Enum.flat_map(fn {_plugin_id, %Plugin{} = plugin} ->
      if PluginRecoverability.recoverable?(plugin) do
        [%{plugin: plugin, last_observed_node: node()}]
      else
        Logger.warning(
          "[snapshot] skipping non-recoverable plugin #{inspect(plugin.id)} " <>
            "(provider #{inspect(plugin.provider)})"
        )

        []
      end
    end)
  end
end
