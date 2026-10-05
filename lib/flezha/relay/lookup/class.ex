defmodule Flezha.Relay.Lookup.Class do
  @moduledoc """
  Resolves the live Overwatch pid a relay class is registered under, on
  this node. The Overwatch records itself at bind, so this answers for a
  frame that arrives before any hub row is published.
  """

  @behaviour Flezha.Relay.Lookup.Resolver

  alias Flezha.Relay.ClassRegistry

  @impl true
  def key_kind(), do: :class

  @impl true
  def resolve(relay_class) when is_binary(relay_class) do
    case ClassRegistry.overwatch_pid(relay_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, :not_found}
    end
  end
end
