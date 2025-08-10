defmodule Flezha do
  alias Flezha.Service.HubManager
  alias Flezha.Service.ProcessorManager
  alias Flezha.Service.OperatorManager
  alias Flezha.Service.PluginManager

  @moduledoc """
  Documentation for `Flezha`.
  """

  defdelegate start_processor(namespace, processor), to: ProcessorManager

  defdelegate stop_processor(namespace, processor), to: ProcessorManager

  defdelegate list_namespaces(), to: HubManager, as: :get_running_namespaces

  defdelegate list_operators(namespace), to: OperatorManager, as: :get_global_operators

  defdelegate list_processors(namespace), to: ProcessorManager, as: :get_processor_ids

  defdelegate enable_plugin(plugin_id, plugin), to: PluginManager

  defdelegate disable_plugin(plugin_id), to: PluginManager

  defdelegate processor_state(namespace, processor),
    to: ProcessorManager,
    as: :get_processor_state
end
