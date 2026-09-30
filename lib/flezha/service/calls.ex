defmodule Flezha.Service.Calls do
  @moduledoc """
  Delivers operators' calls: each request to its callee, each answer back to
  its caller, off every Overwatch's process, so no Overwatch waits on another.

  One `Flezha.Plugin.Provider.OperatorForward.ForwardWorker` per caller and
  target delivers requests in the order the caller's Overwatch sent them, and
  one per callee and caller delivers answers; each is started on first use and
  stops once its queue has been empty for `call_worker_idle_ms`. The router
  answers to `Flezha.Process.CallDelivery`, the name an Overwatch reaches it by,
  and is started first: a router that crashes takes every worker down with it,
  so none outlives the router that knew it. A durable answer they held is
  dispatched again by the callee's outbox; a request they held ends at its
  call's deadline.
  """

  use Supervisor

  @workers Flezha.Service.Calls.Workers

  def start_link(_opts), do: Supervisor.start_link(__MODULE__, nil, name: __MODULE__)

  @impl true
  def init(nil) do
    children = [
      Flezha.Service.Calls.Router,
      {DynamicSupervisor, name: @workers, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc "The supervisor the delivery workers run under."
  @spec workers() :: atom()
  def workers, do: @workers
end
