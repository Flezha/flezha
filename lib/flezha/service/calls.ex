defmodule Flezha.Service.Calls do
  @moduledoc """
  Delivers relays' calls: each request to its callee, each answer back to
  its caller, off every Overwatch's process, so no Overwatch waits on another.

  One `Flezha.Plugin.Provider.RelayForward.ForwardWorker` per caller and
  target delivers requests in the order the caller's Overwatch sent them, and
  one per callee and caller delivers answers; each is started on first use and
  stops once its queue has been empty for `call_worker_idle_ms`. The router
  answers to `Flezha.Process.CallDelivery`, the name an Overwatch reaches it by.
  The workers' supervisor starts first and the router second, `:one_for_all`:
  a crash of either takes every worker down, so none outlives the router that
  knew it, and a starting router finds the supervisor it starts workers under.
  A durable answer a lost worker held is dispatched again by the callee's
  outbox once the router reports the loss; a request it held ends at its
  call's deadline.
  """

  use Supervisor

  @workers Flezha.Service.Calls.Workers

  def start_link(_opts), do: Supervisor.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    children = [
      {DynamicSupervisor, name: @workers, strategy: :one_for_one},
      {Flezha.Service.Calls.Router, supervisor: @workers}
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end
end
