defmodule Flezha.Executor.OrphanFinder do
  @moduledoc """
  Pure helper for finding local relays orphaned by a dead executor.

  When an executor (Rust/Java SDK) reconnects after the controller or executor
  restarted, ProcessHub may still hold registry entries tagged with a now-dead
  `executor_id`. Those entries' Overwatch GenServers need their
  `executor_id` repointed at the connecting executor and an `AddRelay`
  reissued so the SDK side rebuilds its local registry. This module decides
  which entries qualify for that catch-up.

  An entry is an orphan iff all hold:

    1. Its local pid (under `node()`) is non-nil — it lives on this controller.
    2. Its `:tag` metadata names a executor no longer in the active set.
    3. Its factory_id is one of the connecting executor's declared factories.
    4. It was not already claimed by `try_migrate_executor`.

  Caller passes a `ProcessHub.Service.ProcessRegistry.registry_dump/0`-shaped
  map and the rest as plain MapSets / atoms / sets.
  """

  @type registry :: %{
          String.t() => {map(), [{node(), pid()}], %{optional(atom()) => any()}}
        }

  @spec collect(registry, node(), MapSet.t(String.t()), MapSet.t(String.t()), MapSet.t(String.t())) ::
          [{String.t(), pid()}]
  def collect(registry, curr_node, claimed_ids, active_ce_ids, factory_id_set) do
    Enum.reduce(registry, [], fn {relay_id, {cspec, node_pids, metadata}}, acc ->
      with false <- MapSet.member?(claimed_ids, relay_id),
           pid when is_pid(pid) <- local_pid(node_pids, curr_node),
           true <- orphaned?(metadata, active_ce_ids),
           true <- factory_match?(cspec, factory_id_set) do
        [{relay_id, pid} | acc]
      else
        _ -> acc
      end
    end)
  end

  @spec factory_ids_from([map()]) :: MapSet.t(String.t())
  def factory_ids_from(factories) do
    Enum.reduce(factories, MapSet.new(), fn factory, acc ->
      case factory["id"] do
        nil -> acc
        id -> MapSet.put(acc, id)
      end
    end)
  end

  @spec factory_id_from_cspec(map()) :: String.t() | nil
  def factory_id_from_cspec(%{start: {_m, _f, [arg]}}) do
    case arg do
      {_ns, relay} -> relay_factory_id(relay)
      {_ns, _ce, relay} -> relay_factory_id(relay)
      _ -> nil
    end
  end

  def factory_id_from_cspec(_), do: nil

  defp relay_factory_id(%{factory_id: fid}), do: fid
  defp relay_factory_id(%{"factory_id" => fid}), do: fid
  defp relay_factory_id(_), do: nil

  defp local_pid(node_pids, curr_node) do
    Enum.find_value(node_pids, fn
      {n, p} when n == curr_node -> p
      _ -> nil
    end)
  end

  # No tag → can't classify; conservative skip (don't accidentally migrate
  # something tagged for a peer node).
  defp orphaned?(metadata, active_ce_ids) do
    case Map.get(metadata, :tag) do
      nil -> false
      tag -> not MapSet.member?(active_ce_ids, tag)
    end
  end

  defp factory_match?(cspec, factory_id_set) do
    case factory_id_from_cspec(cspec) do
      nil -> false
      fid -> MapSet.member?(factory_id_set, fid)
    end
  end
end
