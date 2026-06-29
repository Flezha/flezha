defmodule Flezha.Convergence.Intent do
  @moduledoc """
  Pure reader of declared, durable state — "what the system should look
  like". Derived from the ProcessHub registry: every running namespace's
  operatives, each operative's declared plugins (read from the durable cspec
  start-args via `HubManager.operative_plugins/2`), and its factory.
  Read-only; never mutates the registry.

  The Loop diffs this against `Probe.snapshot/0` (reality) each tick.
  """

  alias Flezha.Plugin
  alias Flezha.Service.HubManager

  @type operative :: %{
          namespace: atom(),
          operative_id: binary(),
          factory_id: binary() | nil,
          plugins: [{binary(), Plugin.t()}]
        }
  @type t :: %__MODULE__{operatives: [operative()]}

  defstruct operatives: []

  @spec declared() :: t()
  def declared() do
    operatives =
      HubManager.get_running_namespaces()
      |> Enum.flat_map(&declared_for_namespace/1)

    %__MODULE__{operatives: operatives}
  end

  defp declared_for_namespace(namespace) do
    namespace
    |> HubManager.get_all_operatives()
    |> Enum.map(fn {operative_id, _pid} ->
      %{
        namespace: namespace,
        operative_id: operative_id,
        factory_id: nil,
        plugins: declared_plugins(namespace, operative_id)
      }
    end)
  catch
    :exit, _ -> []
  end

  # The declared plugin manifest is the durable cspec start-args list,
  # the single persistence path written by `PluginManager` on `flz plugin
  # enable/disable`. Returns `{plugin_id, %Plugin{}}` tuples; empty on any
  # read failure so a transient registry hiccup never spuriously stops a
  # plugin.
  defp declared_plugins(namespace, operative_id) do
    case HubManager.operative_plugins(to_string(namespace), operative_id) do
      {:ok, plugins} when is_list(plugins) -> plugins
      _ -> []
    end
  end
end
