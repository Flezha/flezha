defmodule Flezha.Reconciler.Supervisor do
  @moduledoc """
  Supervises the reconciler control-loop subsystem.

  Gated by `:reconciler_enabled` (env `FLEZHA_RECONCILER_ENABLED`, default `true`):
  when disabled the Controller boots and serves relays on the pre-reconciler code
  paths with no loop running and no egress-activity table. `Flezha.Reconciler.Leader`
  is supervised separately in `Flezha.Application` because orchestration leadership
  is governed by its own `:leadership_enabled` flag, independent of this kill-switch.

  Children start before the transport accepts executor connects, so the
  `:flezha_relay_activity` table exists before any `StreamChannel` records a frame.
  """

  use Supervisor

  def start_link(_), do: Supervisor.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(_) do
    children =
      if Application.get_env(:flezha, :reconciler_enabled, true) do
        [Flezha.Reconciler.Activity, {Flezha.Reconciler.Loop, []}]
      else
        []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end
end
