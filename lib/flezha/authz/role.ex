defmodule Flezha.AuthZ.Role do
  @moduledoc """
  Fixed role enumeration for v1.

  The set is deliberate and closed: `viewer`, `app`, `operator`, `admin`,
  `runner`, `spawner`, `stopper`. Custom roles are not supported in v1 — see
  `Flezha.AuthZ.Policy` for the role→action mapping and
  `design.md` Decision 4 for the rationale. An identity holds a SET of these in
  its namespace, and an action is allowed when any role in the set grants it.

  `app` sits between `viewer` and `operator` and exists for the identity a
  connected runner holds: an application that administers the namespace its
  connection proved. Its set is a strict subset of `operator`, which
  additionally carries actions that destroy a record or move work.

  `spawner` is for the same identity and grants less: `operative.start` and
  `operative.send` in its namespace, which a procedure's `ctx.spawn_workflow`
  needs and which also open `Operative.start` and `Operative.send` on the
  administration surface; no stop and no plugin verbs.

  `stopper` is the other half of that pair: `operative.stop` in its namespace
  and nothing else, so an application that starts and stops its own work holds
  `spawner` and `stopper` and still enables no plugin.
  """

  @roles [:viewer, :app, :operator, :admin, :runner, :spawner, :stopper]

  @type t :: :viewer | :app | :operator | :admin | :runner | :spawner | :stopper

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
