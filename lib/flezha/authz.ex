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
  can record refused attempts.

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
          required(:source) => :cli | :ui | :sdk | :http | :internal,
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
  def authorize(caller_context, action, scope) do
    case enforcement_posture() do
      :open ->
        :ok

      :enforced ->
        do_authorize(caller_context, action, scope)

      :bootstrap_pending ->
        authorize_bootstrap_pending(caller_context, action, scope)
    end
  end

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
        emit_denial(Map.get(ctx, :principal), refusal, action, scope)
        {:error, refusal}
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
    if principal != "" do
      :ok
    else
      emit_denial(nil, :principal_not_bound, nil, nil)
      {:error, {:forbidden, :principal_not_bound}}
    end
  end

  def require_bound_principal(_ctx) do
    if enabled?() do
      emit_denial(nil, :principal_not_bound, nil, nil)
      {:error, {:forbidden, :principal_not_bound}}
    else
      :ok
    end
  end

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
    with :ok <- validate_action(action),
         :ok <- Scope.validate(scope),
         :ok <- check_principal_bound(caller_context, action, scope),
         {:ok, held} <- resolve_roles(caller_context, action, scope),
         {:ok, granting} <- check_role_grants(held, action, caller_context, scope) do
      check_namespace_scope(held, granting, action, caller_context, scope)
    end
  end

  defp validate_action(action) do
    if Action.known?(action) do
      :ok
    else
      emit_denial(nil, :unknown_action, action, nil)
      {:error, {:forbidden, :unknown_action}}
    end
  end

  # Internal callers (`source: :internal`) are pre-authorized; the
  # boundary that exposes them to a service surface is responsible
  # for never propagating untrusted input here.
  defp check_principal_bound(%{source: :internal}, _action, _scope), do: :ok

  defp check_principal_bound(%{principal: p} = _ctx, action, scope)
       when is_binary(p) and p != "" do
    :ok
  rescue
    _ ->
      emit_denial(nil, :principal_not_bound, action, scope)
      {:error, {:forbidden, :principal_not_bound}}
  end

  defp check_principal_bound(ctx, action, scope) do
    principal = Map.get(ctx, :principal)
    emit_denial(principal, :principal_not_bound, action, scope)
    {:error, {:forbidden, :principal_not_bound}}
  end

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
  defp resolve_roles(caller_context, action, scope) do
    case held_roles(caller_context) do
      {:ok, roles, bound_ns} ->
        {:ok, %{roles: roles, namespace: bound_ns}}

      :error ->
        principal = Map.get(caller_context, :principal)
        emit_denial(principal, :no_role_binding, action, scope)
        {:error, {:forbidden, :no_role_binding}}
    end
  end

  # The roles that grant, so the namespace rule below reads `admin` from among
  # them rather than from a single role. None granting is the refusal, and the
  # record names the whole held set rather than the part that granted.
  defp check_role_grants(%{roles: roles}, action, caller_context, scope) do
    case Enum.filter(roles, &Policy.grants?(&1, action)) do
      [] ->
        emit_denial(Map.get(caller_context, :principal), :role_insufficient, action, scope, roles)
        {:error, {:forbidden, :role_insufficient}}

      granting ->
        {:ok, granting}
    end
  end

  defp check_namespace_scope(
         %{roles: roles, namespace: bound_ns},
         granting,
         action,
         caller_context,
         %{namespace: target_ns} = scope
       ) do
    admin? = :admin in granting

    cond do
      Policy.cross_namespace_admin_action?(action) ->
        if admin? and Namespace.system?(bound_ns) do
          :ok
        else
          # If the caller is admin on the target namespace, allow
          # mutations on its own auth state (e.g. issuing caller
          # tokens for the namespace the caller administers).
          if admin? and bound_ns == target_ns do
            :ok
          else
            emit_denial(
              Map.get(caller_context, :principal),
              :cross_namespace_admin_denied,
              action,
              scope,
              roles
            )

            {:error, {:forbidden, :cross_namespace_admin_denied}}
          end
        end

      bound_ns == target_ns ->
        :ok

      Namespace.system?(bound_ns) and admin? ->
        # admin@_system can act on any namespace's non-auth-state
        # caller actions (e.g. list operators cross-namespace for the
        # console). Read-only actions are also covered here.
        :ok

      true ->
        emit_denial(
          Map.get(caller_context, :principal),
          :namespace_mismatch,
          action,
          scope,
          roles
        )

        {:error, {:forbidden, :namespace_mismatch}}
    end
  end

  # `caller_roles` is the set the refusal was judged with, and `nil` for a
  # refusal that came before any role was resolved. `reason` (a forbid reason,
  # or the whole refusal) is recorded as the code a caller of the refused act
  # receives.
  defp emit_denial(principal, reason, action, scope, roles \\ nil) do
    refusal = if is_atom(reason), do: {:forbidden, reason}, else: reason

    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :authorization_denied,
        metadata: %{
          principal: principal,
          reason: Flezha.Error.from_reason(refusal, action).code,
          action: action,
          scope: scope,
          caller_roles: roles
        }
      })
    )

    :ok
  rescue
    _ -> :ok
  end
end
