defmodule Flezha.Convergence.Intent do
  @moduledoc """
  Pure reader of declared, durable state — "what the system should look
  like". Derived from the hub's declared set (`HubManager.declared_operatives/1`):
  every running namespace's declared operatives with the factory their start
  args recorded, plus each one's declared plugins. Intent, not runtime — a
  wiped runtime registry does not empty this list, and a deliberate stop
  removes its entry for good. Read-only; never mutates the declared set.

  Plugins still come from the durable registry cspec (`operative_plugins/2`),
  the record `flz plugin enable/disable` keeps current; the declared entry
  holds the start-time manifest, which only a total registry loss falls back
  to (via the reconcile replaying it).

  The Loop diffs this against `Probe.snapshot/0` (reality) each tick. Starting
  a declared-but-absent operative is NOT this pipeline's job — the hub's own
  reconcile converges running children toward the declared set through the
  idempotent start authority.
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
    |> intent_entries()
    |> Enum.map(fn %{id: operative_id, factory_id: factory_id} ->
      %{
        namespace: namespace,
        operative_id: operative_id,
        factory_id: factory_id,
        plugins: declared_plugins(namespace, operative_id)
      }
    end)
  catch
    :exit, _ -> []
  end

  # Under the auto-restore kill-switch nothing is ever declared (a hub
  # without the reconcile refuses durable starts), so an empty declared set
  # means "durable intent is off", not "nothing should run" — intent degrades
  # to the live-derived registry, the pre-declared-set behavior, and the loop
  # keeps healing what actually runs.
  defp intent_entries(namespace) do
    if HubManager.durable_starts?() do
      HubManager.declared_operatives(namespace)
    else
      namespace
      |> HubManager.get_all_operatives()
      |> Enum.map(fn {operative_id, _pid} -> %{id: operative_id, factory_id: nil} end)
    end
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
