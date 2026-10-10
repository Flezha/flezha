defmodule Flezha.AuthZ.Supervisor do
  @moduledoc """
  Top-level supervisor for the authn/authz subsystem.

  Children, in start order:

    1. `Flezha.AuthZ.Replica` — the stamps that make the stores below one
       state per cluster; first, because every store write stamps.
    2. `Flezha.AuthZ.AuthState` — the durable bootstrap claim.
    3. `Flezha.AuthZ.RoleStore` — principal → {namespace, roles}.
    4. `Flezha.AuthZ.NamespaceStore` — namespace catalog + quotas.
    5. `Flezha.AuthZ.TokenStore` — scoped-token registry.
    6. `Flezha.Runner.DropMarks` — the runner processes refused until a
       different one connects, replicated the way revoked tokens are.
    7. `Flezha.AuthZ.SourceRegistration` — one-shot worker that
       registers the `:platform_authz` source on the observability
       bus so authz signals participate in fan-out.

  Crash-recovery boot policy: if `:authn_authz, :enabled` is `true`
  AND none of the three preconditions hold (an `admin@_system`
  binding exists, a `bootstrap.token` file is present, or
  `FLEZHA_AUTHZ_ENABLED=false` is set), the supervisor refuses to
  start and surfaces an `error` log line listing the recovery
  options. While `:enabled` is `false` the supervisor starts
  unconditionally (the upgrade-default behaviour).
  """

  use Supervisor

  require Logger

  alias Flezha.AuthZ.{AuthState, Config, NamespaceStore, Replica, RoleStore, TokenStore}

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    # Enforcement is the default posture; any disabled boot is an explicit
    # opt-out and stays loud for as long as it remains set.
    unless Config.config_enabled?() do
      Logger.warning(
        "[authz] authn/authz is OFF; every caller is trusted. Remove the opt-out " <>
          "(or unset #{Flezha.Config.env_name(:authz_enabled)}) to restore the enforced default."
      )
    end

    # The strict refusal-to-start policy (refuse when enabled is
    # true AND no admin@_system AND no bootstrap.token) is enforced
    # by `Flezha.AuthZ.Bootstrap.OnBoot` after the stores have come
    # up. Parent-directory mode is checked at `bootstrap.token`
    # write time by `Flezha.AuthZ.Bootstrap.check_and_write/0`.
    children = [
      Replica,
      AuthState,
      RoleStore,
      NamespaceStore,
      TokenStore,
      Flezha.Runner.DropMarks,
      Flezha.AuthZ.QuotaCounters,
      Flezha.AuthZ.SourceRegistration,
      Flezha.AuthZ.Bootstrap.OnBoot
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
