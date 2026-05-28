defmodule Flezha.Convergence.Intent do
  @moduledoc """
  Pure reader of declared, durable state — "what the system should look
  like" (design.md §1). Derived from the ProcessHub registry: every
  running namespace's relays, each relay's declared plugins
  (`metadata.plugins`, empty until Phase A populates the cspec), and its
  factory. Read-only; never mutates the registry.

  The Loop diffs this against `Probe.snapshot/0` (reality) each tick.
  """

  alias Flezha.Service.HubManager

  @type relay :: %{
          namespace: atom(),
          relay_id: binary(),
          factory_id: binary() | nil,
          plugins: [map()]
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

  # `metadata.plugins` is the declared plugin manifest Phase A writes via
  # `ProcessRegistry.update/3`. Absent until then → no declared plugins.
  defp declared_plugins(_namespace, _relay_id), do: []
end
