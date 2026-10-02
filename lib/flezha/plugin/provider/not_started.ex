defmodule Flezha.Plugin.Provider.NotStarted do
  @moduledoc """
  What stands in a table for a sink binding that could not come up: its
  provider is not loaded on this controller, or its plugin is not enabled. It is
  never enabled by name and is no row of the provider table.

  A guaranteed output is held and never settled, so it waits in the outbox and
  is driven again when the binding starts. A plain output makes no delivery
  promise and is dropped, counted on its operator's liveness counters as
  `not_started_drops`.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.State.Outbox
  alias Flezha.Transport.LivenessCounters

  use ProviderBehaviour,
    options: [
      counters: [
        type: :any,
        internal: true,
        transient: true,
        doc: "The host operator's liveness counters, which count the dropped outputs"
      ]
    ]

  @impl true
  def on_output(%Ctx.IO{metadata: metadata} = ctx, opts) do
    if Outbox.guaranteed?(metadata),
      do: Outbox.hold(metadata),
      else: LivenessCounters.bump_operator(opts[:counters], :not_started_drops)

    {:ok, ctx}
  end
end
