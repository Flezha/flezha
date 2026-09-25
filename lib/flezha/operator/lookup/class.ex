defmodule Flezha.Operator.Lookup.Class do
  @moduledoc """
  Resolves the live Overwatch pid an operator class is registered under, on
  this node. The Overwatch records itself at bind, so this answers for a
  frame that arrives before any hub row is published.
  """

  @behaviour Flezha.Operator.Lookup.Resolver

  alias Flezha.Operator.ClassRegistry

  @impl true
  def key_kind(), do: :class

  @impl true
  def resolve(operator_class) when is_binary(operator_class) do
    case ClassRegistry.overwatch_pid(operator_class) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, :not_found}
    end
  end
end
