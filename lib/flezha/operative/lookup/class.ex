defmodule Flezha.Operative.Lookup.Class do
  @moduledoc """
  Resolves the live Overwatch pid an operative class is registered under, on
  this node. The Overwatch records itself at bind, so this answers for a
  frame that arrives before any hub row is published.
  """

  @behaviour Flezha.Operative.Lookup.Resolver

  alias Flezha.Operative.ClassRegistry

  @impl true
  def key_kind(), do: :class

  @impl true
  def resolve(operative_class) when is_binary(operative_class) do
    case ClassRegistry.overwatch_pid(operative_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, :not_found}
    end
  end
end
