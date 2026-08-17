defmodule Flezha.AuthZ.Bootstrap.OnBoot do
  @moduledoc """
  One-shot worker run after the auth stores boot.

  Responsibilities:

    1. Detect missing-`admin@_system` state and write
       `bootstrap.token` (delegated to
       `Flezha.AuthZ.Bootstrap.check_and_write/0`).
    2. Refuse to leave the supervision tree healthy when the
       Controller booted with `:authn_authz, :enabled = true` AND
       none of the three preconditions hold:
         a. An `admin@_system` binding exists.
         b. `bootstrap.token` is present.
         c. `FLEZHA_AUTHZ_ENABLED=false` is set in the environment.

  Refusal surfaces as a `{:stop, reason}` from `init/1` so the
  supervisor logs it and the Controller exits cleanly.
  """

  use GenServer

  require Logger

  alias Flezha.AuthZ.{Bootstrap, Config, RoleStore}

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :transient
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    cond do
      Config.env_force_disabled?() ->
        # Operator opted out explicitly; do not write a bootstrap token.
        :ignore

      not Config.config_enabled?() ->
        # Auth subsystem is disabled by config; nothing to write.
        :ignore

      # Break-glass first: a REBOOTSTRAP marker invalidates the claimed
      # state, so the admin_system_exists? check below sees the recovered
      # (unclaimed) truth and a fresh bootstrap token is written.
      consume_marker_then_admin_exists?() ->
        :ignore

      true ->
        case Bootstrap.check_and_write() do
          :ok ->
            :ignore

          {:error, reason} ->
            Logger.error(
              "[authz.bootstrap.on_boot] failed to write bootstrap token: " <>
                inspect(reason) <>
                " — recovery options: (a) restore a valid roles.dets, " <>
                "(b) place a writable bootstrap.token at " <>
                Config.bootstrap_token_path() <>
                ", or (c) set FLEZHA_AUTHZ_ENABLED=false to disable auth"
            )

            {:stop, reason}
        end
    end
  end

  # Consumes the break-glass marker (side-effecting exactly once per boot),
  # then answers whether a claimed admin still exists. After a recovery the
  # bindings are gone, so the caller's `true ->` branch writes a fresh token.
  defp consume_marker_then_admin_exists?() do
    _ = Bootstrap.consume_recovery_marker()
    RoleStore.admin_system_exists?()
  end
end
