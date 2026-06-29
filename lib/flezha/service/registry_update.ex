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

  alias Flezha.Service.HubManager
  alias Flezha.Plugin

  @doc "Registry transform that re-tags an operative's owning executor."
  @spec set_executor_tag(String.t()) :: (map(), keyword(), map() -> {map(), keyword(), map()})
  def set_executor_tag(executor_id) do
    fn cspec, node_pids, metadata ->
      {cspec, node_pids, Map.put(metadata, :tag, executor_id)}
    end
  end

  @doc "Registry transform that rewrites an operative's cspec plugin manifest."
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
end
