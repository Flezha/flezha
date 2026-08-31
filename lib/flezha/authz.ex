defmodule Flezha.AuthZ do
  @moduledoc """
  Single policy point for the Flezha Controller.

  Every operator-action function in `Flezha.Service.*` invokes
  `Flezha.AuthZ.authorize/3` at the top of its body before any side
  effect. The function returns `:ok` or
  `{:error, {:forbidden, reason}}`.

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
  code path constructs it: `CliManager` stamps `:cli` (identity from a
  verified token only) and the runner connect seam stamps `:sdk`
  (identity from the authenticated connection's namespace handshake).
  The only way to present `:internal` from outside is arbitrary BEAM
  eval over distribution, which the provisioned release cookie gates —
  the same trust boundary as node ownership.
  """

  alias Flezha.AuthZ.Action
  alias Flezha.AuthZ.{AuthState, Config, Namespace, Policy, Principal, RoleStore, Scope}
  alias Flezha.Observability.{Bus, Signal}

  @source :platform_authz

  @type caller_context :: %{
          required(:source) => :cli | :ui | :sdk | :internal,
          optional(:principal) => Principal.t(),
          optional(:namespace) => binary() | nil,
          optional(:role) => atom() | nil,
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
          | :bootstrap_pending

  @doc """
  Authorize an action against a caller context and a scope.

  Returns `:ok` if the call is permitted, or
  `{:error, {:forbidden, reason}}` if denied. Denials emit an
  audit-ready `:authorization_denied` event regardless of whether the
  audit-log subsystem is configured to consume them.
  """
  @spec authorize(caller_context(), action(), Scope.t()) ::
          :ok | {:error, {:forbidden, forbid_reason()}}
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
      bootstrap procedure) while dataflow — internal machinery and
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
  # runner identities pass; every operator verb refuses naming the way in.
  defp authorize_bootstrap_pending(caller_context, action, scope) do
    case caller_context do
      %{source: :internal} ->
        :ok

      %{source: :sdk, role: :runner} ->
        do_authorize(caller_context, action, scope)

      ctx ->
        emit_denial(Map.get(ctx, :principal), :bootstrap_pending, action, scope)
        {:error, {:forbidden, :bootstrap_pending}}
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
         {:ok, role, bound_namespace} <- resolve_role(caller_context, action, scope),
         :ok <- check_role_grants(role, action, caller_context, scope),
         :ok <- check_namespace_scope(role, bound_namespace, action, caller_context, scope) do
      :ok
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

  # Internal source uses :admin@_system as its effective binding;
  # this is safe because :internal can only be supplied by code
  # paths inside the BEAM, not the wire.
  defp resolve_role(%{source: :internal}, _action, _scope) do
    {:ok, :admin, Namespace.system_name()}
  end

  defp resolve_role(ctx, action, scope) do
    # Prefer the role/namespace bound on the caller_context by the
    # authentication layer; fall back to the role store keyed on
    # the principal.
    case {Map.get(ctx, :role), Map.get(ctx, :namespace)} do
      {role, ns} when is_atom(role) and role != nil and is_binary(ns) ->
        {:ok, role, ns}

      _ ->
        principal = Map.get(ctx, :principal)

        case RoleStore.lookup(principal) do
          {:ok, %{role: role, namespace: ns}} ->
            {:ok, role, ns}

          :error ->
            emit_denial(principal, :no_role_binding, action, scope)
            {:error, {:forbidden, :no_role_binding}}
        end
    end
  end

  defp check_role_grants(role, action, caller_context, scope) do
    if Policy.grants?(role, action) do
      :ok
    else
      emit_denial(Map.get(caller_context, :principal), :role_insufficient, action, scope)
      {:error, {:forbidden, :role_insufficient}}
    end
  end

  defp check_namespace_scope(
         role,
         bound_ns,
         action,
         caller_context,
         %{namespace: target_ns} = scope
       ) do
    cond do
      Policy.cross_namespace_admin_action?(action) ->
        if role == :admin and Namespace.system?(bound_ns) do
          :ok
        else
          # If the caller is admin on the target namespace, allow
          # mutations on its own auth state (e.g. issuing operator
          # tokens for the namespace the caller administers).
          if role == :admin and bound_ns == target_ns do
            :ok
          else
            emit_denial(
              Map.get(caller_context, :principal),
              :cross_namespace_admin_denied,
              action,
              scope
            )

            {:error, {:forbidden, :cross_namespace_admin_denied}}
          end
        end

      bound_ns == target_ns ->
        :ok

      Namespace.system?(bound_ns) and role == :admin ->
        # admin@_system can act on any namespace's non-auth-state
        # operator actions (e.g. list operatives cross-namespace for the
        # console). Read-only actions are also covered here.
        :ok

      true ->
        emit_denial(
          Map.get(caller_context, :principal),
          :namespace_mismatch,
          action,
          scope
        )

        {:error, {:forbidden, :namespace_mismatch}}
    end
  end

  defp emit_denial(principal, reason, action, scope) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :authorization_denied,
        metadata: %{
          principal: principal,
          reason: reason,
          action: action,
          scope: scope
        }
      })
    )

    :ok
  rescue
    _ -> :ok
  end
end
