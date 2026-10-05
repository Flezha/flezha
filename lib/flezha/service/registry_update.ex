defmodule Flezha.Service.RegistryUpdate do
  @moduledoc false
  # Transform functions handed to `ProcessHub.Service.ProcessRegistry.update/3`
  # with `propagate: true` execute on EVERY node's registry, so the shipped
  # function must resolve on peer nodes. Building them here — outside the
  # service modules the test suite Mimic-copies (HubManager, PluginManager) —
  # keeps each closure bound to a module that exists on peers. A closure built
  # inside a Mimic-copied module is bound to `<Mod>.Mimic.Original.Module`, an
  # in-memory test artifact that does not exist on peers, and crashes the
  # remote registry with `UndefinedFunctionError`.

  alias Flezha.Plugin
  alias Flezha.Service.HubManager

  @doc """
  Registry transform that re-tags a relay's owning runner. A partition
  member's identity, read off the row's own cspec, rides the same write, so
  `HubManager.partition_members/2` stays an index lookup even for a row whose
  metadata was rebuilt.
  """
  @spec set_runner_tag(String.t() | nil) :: (map(), keyword(), map() -> {map(), keyword(), map()})
  def set_runner_tag(runner_id) do
    fn cspec, node_pids, metadata ->
      metadata = Map.put(metadata, :tag, runner_id)

      metadata =
        case HubManager.cspec_partition(cspec) do
          nil -> metadata
          partition -> Map.put(metadata, :partition, partition)
        end

      {cspec, node_pids, metadata}
    end
  end

  @doc "Registry transform that rewrites a relay's cspec plugin manifest."
  @spec rewrite_cspec_plugins(([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])) ::
          (map(), keyword(), map() -> {map(), keyword(), map()})
  def rewrite_cspec_plugins(plugins_fn) do
    fn cspec, node_pids, metadata ->
      {HubManager.rewrite_cspec_plugins(cspec, plugins_fn), node_pids, metadata}
    end
  end

  @doc "Plugin-list transform that adds (or replaces) a plugin entry."
  @spec add_plugin(binary(), Plugin.t()) ::
          ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])
  def add_plugin(plugin_id, plugin) do
    fn plugins ->
      plugins
      |> Enum.reject(fn {id, _} -> id == plugin_id end)
      |> Kernel.++([{plugin_id, plugin}])
    end
  end

  @doc "Plugin-list transform that removes a plugin entry."
  @spec remove_plugin(binary()) :: ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])
  def remove_plugin(plugin_id) do
    fn plugins ->
      Enum.reject(plugins, fn {id, _plugin} -> id == plugin_id end)
    end
  end

  @doc """
  Plugin-list transform that sets one option on an existing plugin entry, for
  provider state that must survive a controller restart. A plugin_id absent
  from the manifest is left alone rather than resurrected — it was disabled
  while the write was in flight, and its state is not owed.
  """
  @spec put_plugin_opt(binary(), atom(), term()) ::
          ([{binary(), Plugin.t()}] -> [{binary(), Plugin.t()}])
  def put_plugin_opt(plugin_id, key, value) do
    fn plugins ->
      Enum.map(plugins, fn
        {^plugin_id, %Plugin{} = plugin} ->
          {plugin_id, %Plugin{plugin | opts: Map.put(plugin.opts || %{}, key, value)}}

        other ->
          other
      end)
    end
  end
end
