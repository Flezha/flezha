defmodule Flezha.AuthZ do
  @moduledoc """
  Single policy point for the Flezha Controller.

  Every caller-action function in `Flezha.Service.*` invokes
  `Flezha.AuthZ.authorize/3` at the top of its body before any side
  effect. The function returns `:ok` or
  `{:error, {:forbidden, reason}}`, and `{:error, {:forbidden,
  :bootstrap_pending, token_path}}` while the bootstrap is unclaimed.

  Enforcement is the default posture (`enforcement_posture/0`):
  `authorize/3` returns `:ok` unconditionally only under the explicit
  configuration opt-out (`FLEZHA_AUTHZ_ENABLED=false`, warned on every
  boot). On a boot where the bootstrap has not been claimed yet, gated
  verbs refuse with `:bootstrap_pending` while dataflow continues;
  once claimed, every call is policy-checked against the role binding
  owned by `Flezha.AuthZ.RoleStore`.

  Every denial emits a `:event` signal `:authorization_denied` on the
  `:platform_authz` source so downstream audit / observability tooling
  can record refused attempts. `allowed?/3` is the same decision without that
  record, for a permitted call that only chooses what to show.

  ## The `:internal` caller source

  `source: :internal` is pre-authorized and reserved for calls
  originating inside the controller's own BEAM. No wire- or CLI-facing
  code path constructs it: `Flezha.Service.Cli` stamps `:cli` (identity from a
  verified token only) and the runner connect seam stamps `:sdk`
  (identity from the authenticated connection's namespace handshake).
  The only way to present `:internal` from outside is arbitrary BEAM
  eval over distribution, which the provisioned release cookie gates —
  the same trust boundary as node ownership.

  An `:sdk` context's namespace is the one its connection proved and its roles
  are resolved from `RoleStore` per call, so a caller grants and revokes a
  namespace's applications without touching the connection. See
  `Flezha.Runner.FleetManager.Session.runner_caller_context/1`.

  ## What a caller is judged with

  An identity holds a SET of roles, and an action is allowed when any of them
  grants it. `held_roles/1` answers the set, in the one order every caller kind
  is resolved in: an internal caller, then a credential's own role, then a
  connection's stamped set, then the store's set for the principal.
  """

  alias Flezha.AuthZ.Action
  alias Flezha.AuthZ.{AuthState, Config, Namespace, Policy, Principal, RoleStore, Scope}
  alias Flezha.Observability.{Bus, Signal}

  @source :platform_authz

  @type caller_context :: %{
          required(:source) => :cli | :ui | :sdk | :http | :mcp | :internal,
          optional(:principal) => Principal.t(),
          optional(:namespace) => binary() | nil,
          optional(:role) => atom() | nil,
          optional(:roles) => [atom()] | nil,
          optional(:request_id) => binary() | nil
        }

  @type action :: atom()
  @type forbid_reason ::
          :principal_not_bound
          | :unknown_action
          | :unknown_role
          | :no_role_binding
          | :role_insufficient
          | :namespace_mismatch
          | :cross_namespace_admin_denied
          | :invalid_scope

  @doc """
  Authorize an action against a caller context and a scope.

  Returns `:ok` if the call is permitted, or
  `{:error, {:forbidden, reason}}` if denied. Denials emit an
  audit-ready `:authorization_denied` event regardless of whether the
  audit-log subsystem is configured to consume them.
  """
  @spec authorize(caller_context(), action(), Scope.t()) ::
          :ok
          | {:error, {:forbidden, forbid_reason()}}
          | {:error, {:forbidden, :bootstrap_pending, String.t()}}
  def authorize(caller_context, action, scope),
    do: caller_context |> decide(action, scope) |> recorded(action)

  @doc """
  The judgment `authorize/3` makes, without recording a denial: for a caller
  that only chooses what to show, never for a gate.
  """
  @spec allowed?(caller_context(), action(), Scope.t()) :: boolean()
  def allowed?(caller_context, action, scope), do: decide(caller_context, action, scope) == :ok

  @doc """
  The effective enforcement posture:

    * `:open` — the explicit configuration opt-out; every call is allowed.
    * `:bootstrap_pending` — enforcement is configured on but no
      `admin@_system` claim exists yet: gated verbs refuse (naming the
      bootstrap tasklet) while dataflow — internal machinery and
      runner connections over the authenticated transport — continues.
    * `:enforced` — the bootstrap was claimed; full policy checks apply.
  """
  @spec enforcement_posture() :: :open | :bootstrap_pending | :enforced
  def enforcement_posture() do
    cond do
      not Config.config_enabled?() -> :open
      AuthState.enabled?() -> :enforced
      true -> :bootstrap_pending
    end
  end

  # `:ok`, an unrecorded `{:error, reason}`, or `{:error, refusal, record}`: a
  # refusal and what its record names besides the action.
  defp decide(caller_context, action, scope) do
    case enforcement_posture() do
      :open -> :ok
      :enforced -> do_authorize(caller_context, action, scope)
      :bootstrap_pending -> authorize_bootstrap_pending(caller_context, action, scope)
    end
  end

  # An un-bootstrapped upgrade must not take down pipelines, but must not be
  # open either: only the controller's own machinery and transport-bound
  # runner identities pass; every administration verb refuses naming the way in.
  defp authorize_bootstrap_pending(caller_context, action, scope) do
    case caller_context do
      %{source: :internal} ->
        :ok

      # A transport-bound runner identity is judged by the policy, whatever
      # role the store resolved for it: the connect itself only ever needs
      # `runner.connect`, and a namespace a caller bound before the reset
      # keeps the authority that caller granted.
      %{source: :sdk} ->
        do_authorize(caller_context, action, scope)

      ctx ->
        refusal = {:forbidden, :bootstrap_pending, Config.bootstrap_token_path()}
        refused(refusal, Map.get(ctx, :principal), scope)
    end
  end

  @doc """
  Ensure the caller context carries a bound principal. Used by every
  service function while `:enabled` is `true` so that no internal
  caller can forge a `caller_context` whose `:principal` was not
  established by the authentication layer.
  """
  @spec require_bound_principal(caller_context()) ::
          :ok | {:error, {:forbidden, :principal_not_bound}}
  def require_bound_principal(%{source: :internal}), do: :ok

  def require_bound_principal(%{principal: principal} = _ctx) when is_binary(principal) do
    if principal != "", do: :ok, else: unbound()
  end

  def require_bound_principal(_ctx), do: if(enabled?(), do: unbound(), else: :ok)

  defp unbound(), do: :principal_not_bound |> refused(nil, nil) |> recorded(nil)

  @doc """
  Returns `true` when `:authn_authz, :enabled` is configured `true`
  AND the runtime durable flag in `Flezha.AuthZ.AuthState` agrees.

  Until `AuthState` has been started (very early boot) or
  the config is missing, this returns `false`.
  """
  @spec enabled?() :: boolean()
  def enabled?() do
    Config.config_enabled?() and AuthState.enabled?()
  end

  # ---------- Internal ----------

  defp do_authorize(caller_context, action, scope) do
    principal = Map.get(caller_context, :principal)

    with :ok <- validate_action(action),
         :ok <- Scope.validate(scope),
         :ok <- check_principal_bound(caller_context, scope),
         {:ok, held} <- resolve_roles(caller_context, scope),
         {:ok, granting} <- check_role_grants(held, action, principal, scope) do
      check_namespace_scope(held, granting, action, principal, scope)
    end
  end

  defp validate_action(action),
    do: if(Action.known?(action), do: :ok, else: refused(:unknown_action, nil, nil))

  # Internal callers (`source: :internal`) are pre-authorized; the
  # boundary that exposes them to a service surface is responsible
  # for never propagating untrusted input here.
  defp check_principal_bound(%{source: :internal}, _scope), do: :ok
  defp check_principal_bound(%{principal: p}, _scope) when is_binary(p) and p != "", do: :ok

  defp check_principal_bound(ctx, scope),
    do: refused(:principal_not_bound, Map.get(ctx, :principal), scope)

  @doc """
  The roles a caller is judged with, and the namespace they are held in.

  In order: an internal caller is `admin` in `_system`; a credential's own role
  is the WHOLE of that caller's authority, whatever the store holds for its
  principal; a connection's stamped set is that set; otherwise the store's set
  for the principal. `:error` when the principal holds none.

  Public because the audit reads the same answer the judgment used.
  """
  @spec held_roles(caller_context()) :: {:ok, [atom(), ...], binary()} | :error
  def held_roles(%{source: :internal}), do: {:ok, [:admin], Namespace.system_name()}

  def held_roles(%{role: role, namespace: ns})
      when is_atom(role) and role != nil and is_binary(ns),
      do: {:ok, [role], ns}

  def held_roles(%{roles: [_ | _] = roles, namespace: ns}) when is_binary(ns),
    do: {:ok, roles, ns}

  def held_roles(ctx) do
    case RoleStore.lookup(Map.get(ctx, :principal)) do
      {:ok, %{roles: roles, namespace: ns}} -> {:ok, roles, ns}
      :error -> :error
    end
  end

  # What the caller was judged with, as one value: the set and the namespace it
  # is held in always travel together, and both the decision below and the
  # record it writes read them.
  defp resolve_roles(caller_context, scope) do
    case held_roles(caller_context) do
      {:ok, roles, bound_ns} -> {:ok, %{roles: roles, namespace: bound_ns}}
      :error -> refused(:no_role_binding, Map.get(caller_context, :principal), scope)
    end
  end

  # The roles that grant, so the namespace rule below reads `admin` from among
  # them rather than from a single role. None granting is the refusal, and the
  # record names the whole held set rather than the part that granted.
  defp check_role_grants(%{roles: roles}, action, principal, scope) do
    case Enum.filter(roles, &Policy.grants?(&1, action)) do
      [] -> refused(:role_insufficient, principal, scope, roles)
      granting -> {:ok, granting}
    end
  end

  defp check_namespace_scope(
         %{roles: roles, namespace: bound_ns},
         granting,
         action,
         principal,
         %{namespace: target_ns} = scope
       ) do
    admin? = :admin in granting
    auth_state? = Policy.cross_namespace_admin_action?(action)

    cond do
      # admin@_system mutates any namespace's auth state, and an admin its own
      # namespace's (e.g. issuing caller tokens for the namespace it administers).
      auth_state? and admin? and (Namespace.system?(bound_ns) or bound_ns == target_ns) ->
        :ok

      auth_state? ->
        refused(:cross_namespace_admin_denied, principal, scope, roles)

      bound_ns == target_ns ->
        :ok

      Namespace.system?(bound_ns) and admin? ->
        # admin@_system can act on any namespace's non-auth-state
        # caller actions (e.g. list relays cross-namespace for the
        # console). Read-only actions are also covered here.
        :ok

      true ->
        refused(:namespace_mismatch, principal, scope, roles)
    end
  end

  # A refusal (a forbid reason, or the whole refusal) and what its record names:
  # `caller_roles` is the set it was judged with, and `nil` for a refusal that
  # came before any role was resolved.
  defp refused(reason, principal, scope, roles \\ nil) do
    refusal = if is_atom(reason), do: {:forbidden, reason}, else: reason
    {:error, refusal, %{principal: principal, scope: scope, caller_roles: roles}}
  end

  # The one place a refusal is recorded, as the code a caller of the refused
  # act receives.
  defp recorded({:error, refusal, record}, action) do
    emit_denial(refusal, action, record)
    {:error, refusal}
  end

  defp recorded(decision, _action), do: decision

  defp emit_denial(refusal, action, record) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :authorization_denied,
        metadata:
          Map.merge(record, %{
            reason: Flezha.Error.from_reason(refusal, action).code,
            action: action
          })
      })
    )
  rescue
    _ -> :ok
  end
end
