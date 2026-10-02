defmodule Flezha.Plugin.Provider.NotStarted do
  @moduledoc """
  What stands in a table for a sink binding that could not come up: its
  provider is not loaded on this controller, or its plugin is not enabled. It is
  never enabled by name and is no row of the provider table.

  A guaranteed output is held and never settled, so it waits in the outbox and
  is driven again when the binding starts. A plain output makes no delivery
  promise and is dropped, counted as `[:flezha, :plugin, :not_started, :dropped]`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox

  use ProviderBehaviour

  @impl true
  def on_output(%Ctx.IO{metadata: metadata} = ctx, opts) do
    if Outbox.guaranteed?(metadata) do
      Outbox.hold(metadata)
    else
      ProviderBehaviour.emit(:not_started, :dropped, %{count: 1}, %{
        plugin_id: opts[:source_plugin_id],
        operator_id: opts[:source_operator_id]
      })
    end

    {:ok, ctx}
  end
end
