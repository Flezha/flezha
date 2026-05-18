defmodule Flezha.AuthZ.Supervisor do
  @moduledoc """
  Top-level supervisor for the authn/authz subsystem.

  Children, in start order:

    1. `Flezha.AuthZ.AuthState` — durable `:enabled` flag.
    2. `Flezha.AuthZ.RoleStore` — principal → {namespace, role}.
    3. `Flezha.AuthZ.NamespaceStore` — namespace catalog + quotas.
    4. `Flezha.AuthZ.TokenStore` — scoped-token registry.
    5. `Flezha.AuthZ.SourceRegistration` — one-shot worker that
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

  alias Flezha.AuthZ.{AuthState, Config, NamespaceStore, RoleStore, TokenStore}

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    if Config.env_force_disabled?() do
      Logger.warning(
        "[authz] FLEZHA_AUTHZ_ENABLED=false explicit override active; authn/authz is OFF"
      )
    end

    # The strict refusal-to-start policy (refuse when enabled is
    # true AND no admin@_system AND no bootstrap.token) is enforced
    # by `Flezha.AuthZ.Bootstrap.OnBoot` after the stores have come
    # up. Parent-directory mode is checked at `bootstrap.token`
    # write time by `Flezha.AuthZ.Bootstrap.check_and_write/0`.
    children = [
      AuthState,
      RoleStore,
      NamespaceStore,
      TokenStore,
      Flezha.AuthZ.QuotaCounters,
      Flezha.AuthZ.SourceRegistration,
      Flezha.AuthZ.Bootstrap.OnBoot
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end
end
