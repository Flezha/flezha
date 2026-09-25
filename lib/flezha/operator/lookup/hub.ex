defmodule Flezha.Operator.Lookup.Hub do
  @moduledoc """
  Resolves an operator's pid from its namespace's ProcessHub registry, under
  the liveness guard every hub read takes: a hub that is gone is `:hub_down`,
  never an exit into the caller.
  """

  @behaviour Flezha.Operator.Lookup.Resolver

  alias Flezha.Identificator
  alias Flezha.Process.Hub

  @impl true
  def key_kind(), do: :namespaced

  @impl true
  def resolve({namespace, operator_id}) do
    namespace
    |> Identificator.hub()
    |> Hub.with_live({:error, :hub_down}, fn hub_id ->
      case ProcessHub.get_pids(hub_id, operator_id) do
        [] -> {:error, :not_found}
        [pid | _] -> {:ok, pid}
      end
    end)
  end
end
