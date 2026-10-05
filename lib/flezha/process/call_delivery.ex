defmodule Flezha.Process.CallDelivery do
  @moduledoc """
  The name relay calls are delivered under, and the two hand-offs an
  Overwatch makes to it: a request its graph made, and an answer it owes a
  relay caller. Both are casts, so no Overwatch waits on the delivery or on
  another Overwatch; the process registered here (`Flezha.Service.Calls`)
  delivers each in order per caller and target, retrying until the call's
  deadline.
  """

  @doc "The registered name of the delivery."
  @spec name() :: atom()
  def name, do: __MODULE__

  @doc """
  Deliver the request `request` (a pending call's row, with its `id`) of the
  relay `caller` in `namespace` to its target.
  """
  @spec request(binary(), binary(), map()) :: :ok
  def request(namespace, caller, request),
    do: GenServer.cast(__MODULE__, {:request, namespace, caller, request})

  @doc """
  Hand the outcome of `call` (its caller, id and deadline) from the callee
  `callee` to its caller. `metadata` is the answer's own, whose outbox hold the
  delivery answers once the caller took the outcome or gave it up.
  """
  @spec answer(binary(), binary(), map(), map(), map()) :: :ok
  def answer(namespace, callee, call, outcome, metadata),
    do: GenServer.cast(__MODULE__, {:answer, namespace, callee, call, outcome, metadata})
end
