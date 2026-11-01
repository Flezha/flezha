defmodule Flezha.Plugin.Extension.Table.Record do
  @moduledoc """
  One plugin's record across tables (its plugin, lifecycle entry, monitors and
  hooks): dropping it, and merging the table a bind task built with the one
  its host kept changing meanwhile.

  It takes the table as a plain map and never names `Extension.Table`.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table.Chain
  alias Flezha.Plugin.Extension.Table.Entry
  alias Flezha.Plugin.Extension.Table.Workers
  alias Flezha.Plugin.Provider.WorkerSup

  @doc "`plugin_id`'s hooks and record off the table, releasing nothing it holds."
  @spec forget(map(), binary(), Plugin.t()) :: map()
  def forget(table, plugin_id, %Plugin{hooks: hooks}) do
    %{
      Chain.remove_hooks(table, plugin_id, hooks)
      | plugins: Map.delete(table.plugins, plugin_id),
        entries: Map.delete(table.entries, plugin_id),
        monitors: Map.reject(table.monitors, fn {_ref, {id, _key}} -> id == plugin_id end)
    }
  end

  @doc """
  Merge `bound`, the table a bind task built from `base`, with `live`, the one
  its host kept changing while the task bound. A plugin `live` changed since
  `base` keeps its `live` record, and the task's copy of it is released
  (`terminate` tears down a provider instance the task prepared). Any other
  plugin keeps its bound record, its workers monitored and the timer hooks the
  task registered armed from this process instead.

  A plugin the task added from the declared set and one the host unbound after
  the task read that set look the same here, absent from `base` and `live`:
  both are kept, and the convergence loop unbinds the undeclared one.
  """
  @spec keep_changed(map(), map(), map(), (map(), binary(), Plugin.t() -> :ok)) :: map()
  def keep_changed(bound, base, live, terminate) do
    [bound, base, live]
    |> Enum.flat_map(&Map.keys(&1.entries))
    |> Enum.uniq()
    |> Enum.reduce(bound, fn id, acc ->
      added? = not Map.has_key?(base.plugins, id)

      case {record(base, id), record(bound, id), record(live, id)} do
        {_base, same, same} -> acc
        {same, _bound, same} -> acc |> Workers.rehost(id) |> rearm_timers(id, added?)
        _changed_here -> acc |> release(live, id, added?, terminate) |> take(live, id)
      end
    end)
  end

  defp record(table, plugin_id), do: {table.plugins[plugin_id], table.entries[plugin_id]}

  # The task's copy of a plugin the host's record supersedes, released as a
  # restart releases one while the plugin lives on: its workers the host does
  # not hold stop, and an instance the task prepared itself is torn down.
  defp release(table, live, plugin_id, added?, terminate) do
    case table.plugins do
      %{^plugin_id => %Plugin{provider: provider} = plugin} ->
        if added? and not Workers.joined?(table, plugin_id),
          do: terminate.(table, plugin_id, plugin)

        held = Entry.keys(Entry.of(live, plugin_id))

        (Entry.keys(Entry.of(table, plugin_id)) -- held)
        |> Enum.reduce(table, fn
          :shared, acc -> Workers.leave(acc, plugin_id, :idle)
          key, acc -> Workers.stop(acc, plugin_id, WorkerSup.family(provider), key)
        end)

      _absent ->
        table
    end
  end

  # `plugin_id` as `from` holds it, its hooks registered again from here.
  defp take(table, from, plugin_id) do
    table = if held = table.plugins[plugin_id], do: forget(table, plugin_id, held), else: table

    case from.plugins do
      %{^plugin_id => plugin} ->
        monitors = Map.filter(from.monitors, &match?({_ref, {^plugin_id, _key}}, &1))

        %{
          table
          | plugins: Map.put(table.plugins, plugin_id, plugin),
            entries: Map.put(table.entries, plugin_id, Map.fetch!(from.entries, plugin_id)),
            monitors: Map.merge(table.monitors, monitors)
        }
        |> Chain.add_hooks(plugin_id, plugin)

      _absent ->
        table
    end
  end

  # A timer hook the task registered fires at the task, which has ended.
  defp rearm_timers(table, plugin_id, true = _added?) do
    %Plugin{hooks: hooks} = plugin = table.plugins[plugin_id]
    timers = Enum.filter(hooks, fn {name, _opts} -> table.catalog[name][:kind] == :timer end)
    Chain.resubscribe(table, plugin_id, %Plugin{plugin | hooks: timers})
  end

  defp rearm_timers(table, _plugin_id, false), do: table
end
