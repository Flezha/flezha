defmodule Flezha.Convergence.Intent do
  @moduledoc """
  Pure reader of declared, durable state — "what the system should look
  like" (design.md §1). Derived from the ProcessHub registry: every
  running namespace's relays, each relay's declared plugins (read from
  the durable cspec start-args via `HubManager.relay_plugins/2` — the
  same record `PluginManager` writes through `ProcessRegistry.update/3`),
  and its factory. Read-only; never mutates the registry.

  The Loop diffs this against `Probe.snapshot/0` (reality) each tick.
  """

  alias Flezha.Plugin
  alias Flezha.Service.HubManager

  @type relay :: %{
          namespace: atom(),
          relay_id: binary(),
          factory_id: binary() | nil,
          plugins: [{binary(), Plugin.t()}]
        }
  @type t :: %__MODULE__{relays: [relay()]}

  defstruct relays: []

  @spec declared() :: t()
  def declared() do
    relays =
      HubManager.get_running_namespaces()
      |> Enum.flat_map(&declared_for_namespace/1)

    %__MODULE__{relays: relays}
  end

  defp declared_for_namespace(namespace) do
    namespace
    |> HubManager.get_all_relays()
    |> Enum.map(fn {relay_id, _pid} ->
      %{
        namespace: namespace,
        relay_id: relay_id,
        factory_id: nil,
        plugins: declared_plugins(namespace, relay_id)
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
  defp declared_plugins(namespace, relay_id) do
    case HubManager.relay_plugins(to_string(namespace), relay_id) do
      {:ok, plugins} when is_list(plugins) -> plugins
      _ -> []
    end
  end
end
