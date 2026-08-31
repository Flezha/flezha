defmodule Flezha.AuthZ.Policy do
  @moduledoc """
  Compile-time role→action mapping.

  Roles inherit upward: `admin` ⊇ `operator` ⊇ `viewer`. The
  `runner` role is intentionally outside this hierarchy and grants
  only the three data-plane verbs.

  Cross-namespace auth-state mutations are additionally gated in
  `Flezha.AuthZ.authorize/3` by requiring the bound namespace to be
  `_system`; the role table alone does not express that constraint.
  """

  # Read-only actions usable by viewers on the bound namespace.
  @viewer_actions [
    :"operative.list",
    :"plugin.list",
    :"plugin.info",
    :"audit.read",
    :"observability.query",
    :"observability.subscribe",
    :"authz.whoami",
    :"authz.token_list",
    :"authz.namespace_list",
    :"authz.namespace_show",
    :"authz.role_list",
    :"controller.status",
    :"boot.status",
    :"boot.await",
    :"state.get",
    :"state.list",
    :"state.at"
  ]

  # Operator adds operative/plugin lifecycle, audit rotation, observability mutate.
  @operator_extra [
    :"operative.start",
    :"operative.stop",
    :"plugin.enable",
    :"plugin.disable",
    :"audit.rotate",
    :"observability.enable_source",
    :"observability.disable_source",
    :"state.rotate",
    :"dlq.replay",
    :"dlq.drop"
  ]

  # Admin adds everything that touches auth state, plus reset / shutdown
  # and irreversible state destruction.
  @admin_extra [
    :"plugin.install",
    :"plugin.uninstall",
    :"plugin.upgrade",
    :"reset.run",
    :"controller.stop",
    :"state.retention",
    :"state.delete",
    :"authz.bootstrap",
    :"authz.token_issue",
    :"authz.token_revoke",
    :"authz.namespace_create",
    :"authz.namespace_delete",
    :"authz.namespace_set_quota",
    :"authz.role_bind",
    :"authz.role_unbind"
  ]

  @runner_actions [
    :"runner.connect",
    :"runner.checkpoint",
    :"runner.report"
  ]

  @viewer MapSet.new(@viewer_actions)
  @operator MapSet.union(@viewer, MapSet.new(@operator_extra))
  @admin MapSet.union(@operator, MapSet.new(@admin_extra))
  @runner MapSet.new(@runner_actions)

  @grants %{
    viewer: @viewer,
    operator: @operator,
    admin: @admin,
    runner: @runner
  }

  @doc """
  True when `role` grants `action`.
  """
  @spec grants?(atom(), atom()) :: boolean()
  def grants?(role, action) when is_atom(role) and is_atom(action) do
    case Map.fetch(@grants, role) do
      {:ok, set} -> MapSet.member?(set, action)
      :error -> false
    end
  end

  @doc """
  Actions that imply auth-state mutation; these are additionally
  gated by `admin@_system` in `Flezha.AuthZ.authorize/3` rather than
  the bound role's own namespace.
  """
  @spec cross_namespace_admin_action?(atom()) :: boolean()
  def cross_namespace_admin_action?(action) do
    action in [
      :"authz.namespace_create",
      :"authz.namespace_delete",
      :"authz.namespace_set_quota",
      :"authz.role_bind",
      :"authz.role_unbind",
      :"authz.token_issue",
      :"authz.token_revoke"
    ]
  end
end
