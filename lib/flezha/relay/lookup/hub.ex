defmodule Flezha.Relay.Lookup.Hub do
  @moduledoc """
  Resolves a relay's pid from its namespace's ProcessHub registry, under
  the liveness guard every hub read takes: a hub that is gone is `:hub_down`,
  never an exit into the caller.
  """

  @behaviour Flezha.Relay.Lookup.Resolver

  alias Flezha.Identificator
  alias Flezha.Process.Hub

  @impl true
  def key_kind(), do: :namespaced

  @impl true
  def resolve({namespace, relay_id}) do
    namespace
    |> Identificator.hub()
    |> Hub.with_live({:error, :hub_down}, fn hub_id ->
      # A second entry is a hand-over's arriving copy, answered after the head,
      # or the row a move left: a dead node's entry, or the old copy's before
      # its exit reached this node. Only then is the row filtered.
      case ProcessHub.child_lookup(hub_id, relay_id) do
        {_spec, [{_node, pid}]} ->
          {:ok, pid}

        {_spec, node_pids} ->
          case node_pids |> Hub.on_live_nodes() |> Enum.find(&Hub.running?/1) do
            {_node, pid} -> {:ok, pid}
            nil -> {:error, :not_found}
          end

        nil ->
          {:error, :not_found}
      end
    end)
  end
end
