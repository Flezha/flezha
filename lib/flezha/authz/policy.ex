defmodule Flezha.AuthZ.Policy do
  @moduledoc """
  Compile-time role→action mapping.

  Roles inherit upward: `admin` ⊇ `operator` ⊇ `app` ⊇ `viewer`, with three
  roles outside the hierarchy: `runner` grants only `runner.connect`, `spawner`
  adds to it only `operative.start` and `operative.send`, which a
  procedure-requested spawn needs and which also judge the administration
  surface's start and send, and `stopper` adds to it only `operative.stop`.
  `app` holds everything `spawner` and `stopper` do, and `operator` inherits
  everything `app` adds except the connect. An operator credential is not a
  runner.

  An identity holds a set of these roles, and `Flezha.AuthZ.authorize/3` allows
  an action when any role in the set grants it. So `spawner` and `stopper`
  together are start, send and stop, and no plugin verb.

  Cross-namespace auth-state mutations are additionally gated in
  `Flezha.AuthZ.authorize/3` by requiring the bound namespace to be
  `_system`; the role table alone does not express that constraint.
  """

  # Read-only actions usable by viewers on the bound namespace. `schedule.list`
  # is here because the schedule registry names no business data — a viewer sees
  # what fires and when, not what any of it produced.
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
    :"state.get",
    :"state.list",
    :"state.at",
    :"schedule.list"
  ]

  # What a procedure-requested spawn performs: a start, or a delivery.
  @spawn_actions [:"operative.start", :"operative.send"]

  # What a stop performs. Held apart from the spawn so `spawner` and `stopper`
  # combine, and so `app` is spelled from both rather than repeating either.
  @stop_actions [:"operative.stop"]

  # App adds the namespace-scoped lifecycle an application performs on its own
  # work, and nothing that destroys a record or moves work it did not create.
  # `operative.scale` is deliberately absent: rescaling moves live work between
  # partitions, which is an operator's call over a whole namespace rather than
  # an application's over its own instance.
  @app_extra @spawn_actions ++ @stop_actions ++ [:"plugin.enable", :"plugin.disable"]

  # Operator adds the rest of the operative/plugin lifecycle, audit rotation,
  # observability mutate, schedule management, the dead-letter queue whole and
  # the records in flight. The DLQ reads and `state.records` sit here rather
  # than with the viewer reads because a parked payload or a journaled input is
  # the business record itself: whoever may read one may already replay or
  # destroy it, and no viewer workflow needs it.
  @operator_extra [
    :"operative.replay",
    :"audit.rotate",
    :"observability.enable_source",
    :"observability.disable_source",
    :"state.rotate",
    :"operative.scale",
    :"operative.rewind",
    :"dlq.list",
    :"dlq.show",
    :"dlq.replay",
    :"dlq.drop",
    :"state.records",
    :"schedule.add",
    :"schedule.remove",
    :"schedule.assert",
    :"schedule.retract"
  ]

  # Admin adds everything that touches auth state, plus shutdown, the
  # controller-wide settings and irreversible state destruction.
  @admin_extra [
    :"plugin.install",
    :"plugin.uninstall",
    :"plugin.upgrade",
    :"controller.stop",
    :"state.retention",
    :"state.delete",
    :"authz.token_issue",
    :"authz.token_revoke",
    :"authz.namespace_create",
    :"authz.namespace_delete",
    :"authz.namespace_set_quota",
    :"authz.role_bind",
    :"authz.role_unbind"
  ]

  @runner_actions [:"runner.connect"]

  @viewer MapSet.new(@viewer_actions)
  @runner MapSet.new(@runner_actions)
  # `app` and `spawner` are the roles a runner principal is bound to, so each
  # carries the connect that principal already held; `operator` takes app's
  # extra actions without it, which is why the unions are written out rather
  # than chained through `@app`.
  @spawner MapSet.union(@runner, MapSet.new(@spawn_actions))
  @stopper MapSet.union(@runner, MapSet.new(@stop_actions))
  @app @viewer |> MapSet.union(@runner) |> MapSet.union(MapSet.new(@app_extra))
  @operator MapSet.union(@viewer, MapSet.new(@app_extra ++ @operator_extra))
  @admin MapSet.union(@operator, MapSet.new(@admin_extra))

  @grants %{
    viewer: @viewer,
    app: @app,
    operator: @operator,
    admin: @admin,
    runner: @runner,
    spawner: @spawner,
    stopper: @stopper
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
  True when `role` is one a runner's principal is bound to.

  A runner proves its namespace with a `runner` credential, so the operator
  surface issues no credential for any other role carrying the connect. The
  property is the grant itself, so a role added to the enumeration with
  `runner.connect` is refused a credential without any list being edited.
  """
  @spec runner_principal_role?(atom()) :: boolean()
  def runner_principal_role?(role), do: grants?(role, :"runner.connect")

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
