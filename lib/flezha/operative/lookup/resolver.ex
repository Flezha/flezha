defmodule Flezha.Operative.Lookup.Resolver do
  @moduledoc """
  One implementer per kind of key that names an operative's process. A
  resolver answers for its own kind and nothing else — a class registered
  locally, a row in a namespace's hub — and every one of them answers in the
  same three words, so a caller that swaps one for another does not change.
  """

  @typedoc "Whatever names an operative for this resolver's kind."
  @type key :: term()

  @typedoc """
  `:not_found` — nothing is registered under the key.
  `:dead` — something is registered, and it is not alive.
  `:hub_down` — the registry that would know is not reachable.
  """
  @type reason :: :not_found | :dead | :hub_down

  @callback key_kind() :: atom()
  @callback resolve(key()) :: {:ok, pid()} | {:error, reason()}
end
