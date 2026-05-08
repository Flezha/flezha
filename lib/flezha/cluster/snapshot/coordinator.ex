defmodule Flezha.Cluster.Snapshot.Coordinator do
  @moduledoc """
  Initiator-side orchestration for `flz cluster snapshot` / `flz cluster restore`.

  The coordinator runs on the node where the executor invokes the CLI command.
  It enumerates reachable peers, fans out per-node `local_state/0` /
  `apply_local_state/1` RPCs, reconciles results against the pre-gather
  ProcessHub view, and writes / reads the on-disk ETF artefact atomically.

  See `openspec/changes/add-cluster-snapshot-restore/design.md` for the
  full lifecycle. The snapshot file format is owned by
  `Flezha.Cluster.Snapshot.Encoding`.
  """

  alias Flezha.Cluster.Snapshot
  alias Flezha.Cluster.Snapshot.Encoding
  alias Flezha.Plugin
  alias Flezha.Relay.Builder

  require Logger

  @default_peer_timeout_ms 5_000
  @default_state_cap_bytes 8 * 1024 * 1024

  @type snapshot_opts :: [
          peer_timeout_ms: pos_integer(),
          strict?: boolean(),
          state_cap_bytes: pos_integer(),
          namespace: binary() | nil
        ]

  @type restore_opts :: [
          peer_timeout_ms: pos_integer(),
          warn_if_older_than_ms: pos_integer() | :infinity,
          namespace: binary() | nil
        ]

  @doc """
  Capture a cluster-wide snapshot to `path`.

  Defaults to best-effort: peers that fail or time out are logged at WARN and
  the file is written with what the rest of the cluster contributed. Pass
  `strict?: true` to abort instead of partially capturing.
  """
  @spec snapshot(Path.t(), snapshot_opts()) ::
          {:ok,
           %{relays: non_neg_integer(), plugins: non_neg_integer(), bytes: non_neg_integer()}}
          | {:error, term()}
  def snapshot(path, opts \\ []) do
    peer_timeout = Keyword.get(opts, :peer_timeout_ms, @default_peer_timeout_ms)
    strict? = Keyword.get(opts, :strict?, false)
    namespace_filter = Keyword.get(opts, :namespace)
    state_cap = state_cap_bytes(opts)

    nodes = visible_nodes()
    ring = capture_process_ring()

    %{ok: ok_results, failed: failed} = gather(nodes, peer_timeout)

    cond do
      strict? and failed != [] ->
        Logger.error(
          "[cluster] snapshot strict-mode aborting — peer failures: #{inspect(failed)}"
        )

        {:error, {:strict_mode, Enum.map(failed, &elem(&1, 0))}}

      true ->
        if failed != [] do
          Enum.each(failed, fn {peer, reason} ->
            Logger.warning("[cluster] peer #{peer} unreachable: #{inspect(reason)}")
          end)
        end

        payload =
          ok_results
          |> build_payload(ring, state_cap, strict?)
          |> apply_namespace_filter(namespace_filter)

        case write_atomic(path, Encoding.encode(payload)) do
          {:ok, bytes} ->
            summary = %{
              relays: length(payload.relays),
              plugins: length(payload.plugins),
              bytes: bytes
            }

            Logger.info(
              "[cluster] snapshot written: #{summary.relays} relays, " <>
                "#{summary.plugins} plugins, #{summary.bytes} bytes -> #{path}"
            )

            {:ok, summary}

          {:error, reason} = err ->
            Logger.error("[cluster] snapshot write failed: #{inspect(reason)}")
            err
        end
    end
  end

  @doc """
  Restore from a snapshot file at `path`.

  Reads the file, validates its schema version, notifies SYSMODE so the
  bootstrap timer is suppressed, and per-entry RPCs `apply_local_state/1` to
  the target nodes. `--namespace` filters the entries that are applied.
  """
  @spec restore(Path.t(), restore_opts()) ::
          {:ok, %{relays: non_neg_integer(), plugins: non_neg_integer()}}
          | {:error, term()}
  def restore(path, opts \\ []) do
    peer_timeout = Keyword.get(opts, :peer_timeout_ms, @default_peer_timeout_ms)
    warn_age = Keyword.get(opts, :warn_if_older_than_ms, :infinity)
    namespace_filter = Keyword.get(opts, :namespace)

    with {:ok, bytes} <- read_file(path),
         {:ok, payload} <- Encoding.decode(bytes) do
      log_age(payload, warn_age)

      try do
        finalize_restore(apply_payload(payload, peer_timeout, namespace_filter))
      catch
        kind, reason -> {:error, {kind, reason}}
      end
    end
  end

  ## Helpers — snapshot

  defp gather(nodes, peer_timeout) do
    tasks =
      Enum.map(nodes, fn peer ->
        {peer, Task.async(fn -> rpc_local_state(peer, peer_timeout) end)}
      end)

    Enum.reduce(tasks, %{ok: [], failed: []}, fn {peer, task}, acc ->
      case Task.yield(task, peer_timeout + 500) || Task.shutdown(task, :brutal_kill) do
        {:ok, {:ok, slice}} ->
          %{acc | ok: [{peer, slice} | acc.ok]}

        {:ok, {:error, reason}} ->
          %{acc | failed: [{peer, reason} | acc.failed]}

        {:exit, reason} ->
          %{acc | failed: [{peer, {:exit, reason}} | acc.failed]}

        nil ->
          %{acc | failed: [{peer, :timeout} | acc.failed]}
      end
    end)
  end

  defp rpc_local_state(peer, peer_timeout) do
    case :rpc.call(peer, Snapshot, :local_state, [], peer_timeout) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      slice when is_map(slice) -> {:ok, slice}
      other -> {:error, {:unexpected_return, other}}
    end
  end

  defp build_payload(ok_results, ring, state_cap, strict?) do
    {relays, captured_states} = process_collected(ok_results, ring, state_cap)
    plugins = Enum.flat_map(ok_results, fn {_peer, slice} -> slice.plugins end)

    %{
      taken_at_ms: System.system_time(:millisecond),
      cluster: %{nodes: Enum.map(ok_results, fn {peer, _} -> peer end)},
      relays: relays,
      plugins: plugins,
      captured_states: captured_states,
      strict_mode: strict?,
      initiator_node: node()
    }
  end

  defp process_collected(ok_results, ring, state_cap) do
    Enum.reduce(ok_results, {[], %{}}, fn {peer, slice}, {procs, states} ->
      slice_procs =
        Enum.map(slice.relays, fn entry ->
          reconcile_with_ring(entry, peer, ring)
        end)

      slice_states = drop_oversized(slice.captured_states, state_cap)

      {procs ++ slice_procs, Map.merge(states, slice_states)}
    end)
  end

  defp reconcile_with_ring(%{relay: %Builder{id: id}} = entry, peer, ring) do
    case Map.get(ring, id) do
      nil ->
        Map.put(entry, :last_observed_node, peer)

      ^peer ->
        Map.put(entry, :last_observed_node, peer)

      expected ->
        Logger.warning(
          "[cluster] relay #{id} migrated during gather — ring expected " <>
            "#{inspect(expected)} but observed on #{inspect(peer)}; trusting observation"
        )

        Map.put(entry, :last_observed_node, peer)
    end
  end

  # Trim a built payload to a single namespace. Mirrors restore's
  # `filter_namespace/2` so executors see symmetric semantics: pass
  # `--namespace prod` on either end and the result is restricted to
  # `prod` entries. The kept payload's `captured_states` map is pruned
  # to just the surviving relay ids so the on-disk file size scales
  # with the filtered set.
  defp apply_namespace_filter(payload, nil), do: payload

  defp apply_namespace_filter(payload, namespace) when is_binary(namespace) do
    kept_relays = filter_namespace(payload.relays, namespace)
    kept_plugins = filter_namespace(payload.plugins, namespace)

    kept_ids = MapSet.new(kept_relays, fn entry -> entry.relay.id end)

    pruned_states =
      Map.filter(payload.captured_states, fn {id, _bin} -> MapSet.member?(kept_ids, id) end)

    %{
      payload
      | relays: kept_relays,
        plugins: kept_plugins,
        captured_states: pruned_states
    }
  end

  defp drop_oversized(captured_states, cap) do
    Enum.reduce(captured_states, %{}, fn {relay_id, bin}, acc ->
      cond do
        not is_binary(bin) ->
          acc

        byte_size(bin) > cap ->
          Logger.warning(
            "[snapshot] dropping captured_state_binary for #{relay_id}: " <>
              "#{byte_size(bin)} bytes exceeds cap #{cap}"
          )

          acc

        true ->
          Map.put(acc, relay_id, bin)
      end
    end)
  end

  defp visible_nodes do
    [node() | Node.list(:visible)]
  end

  defp capture_process_ring do
    Flezha.Service.HubManager.get_running_namespaces()
    |> Enum.flat_map(fn namespace ->
      hub_id = Flezha.Service.Identificator.hub(namespace)

      if ProcessHub.is_alive?(hub_id) do
        ProcessHub.process_list(hub_id, :global)
        |> Enum.map(fn
          {relay_id, [{node, _pid}]} -> {relay_id, node}
          {relay_id, [{node, _pid} | _]} -> {relay_id, node}
          {relay_id, _other} -> {relay_id, nil}
        end)
      else
        []
      end
    end)
    |> Enum.into(%{})
  end

  defp state_cap_bytes(opts) do
    case Keyword.fetch(opts, :state_cap_bytes) do
      {:ok, n} when is_integer(n) and n > 0 ->
        n

      _ ->
        Application.get_env(:flezha, :snapshot_state_cap_bytes, @default_state_cap_bytes)
    end
  end

  defp write_atomic(path, binary) when is_binary(binary) do
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, binary, [:binary, :write]),
         :ok <- :file.rename(tmp, path) do
      {:ok, byte_size(binary)}
    else
      {:error, reason} ->
        _ = File.rm(tmp)
        {:error, reason}
    end
  end

  ## Helpers — restore

  defp read_file(path) do
    case File.read(path) do
      {:ok, bytes} -> {:ok, bytes}
      {:error, reason} -> {:error, {:read_failed, path, reason}}
    end
  end

  defp log_age(%{taken_at_ms: t}, warn_age) when is_integer(t) do
    age_ms = System.system_time(:millisecond) - t

    if is_integer(warn_age) and age_ms > warn_age do
      Logger.warning(
        "[cluster] restore: snapshot is #{age_ms}ms old, exceeds threshold #{warn_age}ms"
      )
    else
      Logger.info("[cluster] restore: snapshot age #{age_ms}ms")
    end
  end

  defp log_age(_, _), do: :ok

  defp apply_payload(payload, peer_timeout, namespace_filter) do
    relays_filtered = filter_namespace(payload.relays, namespace_filter)
    plugins_filtered = filter_namespace(payload.plugins, namespace_filter)
    captured = Map.get(payload, :captured_states, %{})

    proc_results =
      Enum.map(relays_filtered, fn entry ->
        apply_relay_entry(entry, captured, peer_timeout)
      end)

    plugin_results =
      Enum.map(plugins_filtered, fn entry ->
        apply_plugin_entry(entry, peer_timeout)
      end)

    %{relays: proc_results, plugins: plugin_results}
  end

  defp filter_namespace(entries, nil), do: entries

  defp filter_namespace(entries, ns) when is_binary(ns) do
    Enum.filter(entries, &matches_namespace?(&1, ns))
  end

  defp matches_namespace?(%{namespace: entry_ns}, ns), do: entry_ns == ns

  defp matches_namespace?(%{plugin: %Plugin{namespace: entry_ns}}, ns) do
    to_string(entry_ns) == ns
  end

  defp matches_namespace?(_entry, _ns), do: false

  defp apply_relay_entry(
         %{relay: %Builder{} = builder, namespace: ns} = entry,
         captured,
         peer_timeout
       ) do
    state_binary = Map.get(captured, builder.id)
    target = Map.get(entry, :last_observed_node, node())
    target = if target in [node() | Node.list(:visible)], do: target, else: node()

    arg = {:relay, builder, ns, state_binary}

    case rpc_apply(target, arg, peer_timeout) do
      {:ok, _info} = ok ->
        {builder.id, target, ok}

      {:error, reason} = err ->
        Logger.warning(
          "[cluster] restore: relay #{builder.id} on #{target} failed: " <>
            inspect(reason)
        )

        {builder.id, target, err}
    end
  end

  defp apply_plugin_entry(%{plugin: %Plugin{id: id} = plugin} = entry, peer_timeout) do
    target = Map.get(entry, :last_observed_node, node())
    target = if target in [node() | Node.list(:visible)], do: target, else: node()

    arg = {:plugin, plugin}

    case rpc_apply(target, arg, peer_timeout) do
      {:ok, _msg} = ok -> {id, target, ok}
      {:error, _reason} = err -> {id, target, err}
    end
  end

  defp rpc_apply(node, arg, _timeout) when node == node() do
    Snapshot.apply_local_state(arg)
  end

  defp rpc_apply(node, arg, timeout) do
    case :rpc.call(node, Snapshot, :apply_local_state, [arg], timeout) do
      {:badrpc, reason} -> {:error, {:badrpc, reason}}
      other -> other
    end
  end

  defp finalize_restore(%{relays: ps, plugins: pls}) do
    {:ok, %{relays: length(ps), plugins: length(pls)}}
  end
end
