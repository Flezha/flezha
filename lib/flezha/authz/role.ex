defmodule Flezha.AuthZ.Role do
  @moduledoc """
  Fixed role enumeration for v1.

  The four-role design is deliberate: `viewer`, `operator`, `admin`,
  `executor`. Custom roles are not supported in v1 — see
  `Flezha.AuthZ.Policy` for the role→action mapping and
  `design.md` Decision 4 for the rationale.
  """

  @roles [:viewer, :operator, :admin, :executor]

  @type t :: :viewer | :operator | :admin | :executor

  @doc "Returns the full set of role atoms."
  @spec all() :: [t()]
  def all, do: @roles

  @doc "True when `role` is a recognised member of the enumeration."
  @spec known?(atom()) :: boolean()
  def known?(role) when is_atom(role), do: role in @roles

  @doc """
  Validates a role atom, returning `:ok` or `{:error, :unknown_role}`.
  """
  @spec validate(atom()) :: :ok | {:error, :unknown_role}
  def validate(role) do
    if known?(role), do: :ok, else: {:error, :unknown_role}
  end
end
