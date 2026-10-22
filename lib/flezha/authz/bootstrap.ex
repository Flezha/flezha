defmodule Flezha.AuthZ.Bootstrap do
  @moduledoc """
  One-time bootstrap flow for the first `admin@_system` principal.

  Lifecycle:

    1. **`check_and_write/0`** — invoked at boot when no
       `admin@_system` binding exists. Ensures `<auth_dir>` exists
       with mode `0700`, tightens a umask-inherited group/other-write
       parent directory in place (refusing one it cannot own, such as
       a shared sticky dir), writes a 256-bit token to
       `<auth_dir>/bootstrap.token` with mode `0600`, and emits a
       single `warn` line naming the path.
    2. **`claim/2`** — invoked by `flz auth bootstrap`, which reads the token
       from its stdin.
       Verifies the presented token against `bootstrap.token`,
       resolves a principal (cert SAN > cert DN > `--principal`
       flag > freshly minted admin token), binds `admin@_system`,
       flips `AuthState.enabled` to `true`, unlinks
       `bootstrap.token`, and emits a `:bootstrap_completed` event
       on `:platform_authz`.

  Re-claim after a successful `claim/2` is refused as `:bootstrap_already_completed`: the
  token file is unlinked by the claim and never written again while an admin
  exists, so the refusal names the state rather than the missing file.

  ## Break-glass recovery, with or without a stop

  A lost `admin@_system` is recovered by writing the same `bootstrap.token`
  file, and it does not require stopping the controller. Whoever can write
  inside `<auth_dir>` — mode `0700`, owned by the controller's user — can
  already stop the controller and take the offline path, so admitting the claim
  while it runs grants no authority filesystem access did not already carry;
  what it removes is a forced outage. A claim that finds a previous
  `admin@_system` binding is that recovery: every such binding is unbound and
  every live `_system` admin token revoked before the new principal is bound,
  so exactly one admin identity survives it and the event says so. The offline
  `REBOOTSTRAP` marker path is unchanged and invalidates through the same
  function.
  """

  require Logger

  alias Flezha.AuthZ.{AuthState, Config, Namespace, RoleStore, SourceRegistration, TokenStore}
  alias Flezha.Layout.Table, as: Layout

  @token_bytes 32
  @recovery_marker "REBOOTSTRAP"

  @doc """
  Break-glass recovery (`flz auth rebootstrap`): with the controller
  STOPPED, a caller with filesystem access to `<auth_dir>` writes the
  `REBOOTSTRAP` marker; this consumes it on the next boot — every
  `admin@_system` binding is unbound, every `_system` admin token revoked,
  the durable claimed flag reset — so `check_and_write/0` re-enters the
  ordinary bootstrap flow with a fresh token. Emits an auditable
  `:rebootstrap_recovery` event naming what was invalidated.

  Filesystem access already equals node ownership in this model, so the
  marker adds no new attack surface; a boot without the marker is a no-op.
  """
  @spec consume_recovery_marker() :: :ok | :recovered
  def consume_recovery_marker() do
    path = recovery_marker_path()

    if File.exists?(path) do
      {unbound, revoked} = invalidate_previous_admin("rebootstrap")

      :ok = AuthState.reset()
      _ = File.rm(path)

      Logger.warning(
        "[authz.bootstrap] REBOOTSTRAP marker consumed: unbound #{length(unbound)} " <>
          "admin@_system binding(s), revoked #{length(revoked)} _system admin token(s); " <>
          "re-entering bootstrap"
      )

      emit_recovery(:offline, unbound, length(revoked))
      :recovered
    else
      :ok
    end
  end

  @doc "Path of the break-glass recovery marker inside the auth dir."
  @spec recovery_marker_path() :: String.t()
  def recovery_marker_path(), do: Layout.file(:auth, @recovery_marker)

  @doc """
  Detect missing-bootstrap state and emit a one-time token to disk.
  Returns `:ok` when nothing needed to be written.
  """
  @spec check_and_write() :: :ok | {:error, term()}
  def check_and_write() do
    cond do
      RoleStore.admin_system_exists?() ->
        :ok

      File.exists?(Config.bootstrap_token_path()) ->
        :ok

      true ->
        write_bootstrap_token()
    end
  end

  @doc """
  Claim the bootstrap token, binding `admin@_system` to a principal.

  `opts` may include:

    * `:principal` — explicit principal string (used when no cert
      is presented and the caller wants a stable identity).
    * `:cert_principal` — principal derived from a presented client
      cert by `Flezha.AuthN.Cert.principal_from_peer/1`.

  When neither principal is supplied, a freshly-minted scoped admin
  token is issued and returned to the caller via the `:minted_token`
  field on the success map.

  A claim made while an `admin@_system` binding already exists is a break-glass
  recovery: it needs no stop, and the previous admin bindings and `_system`
  admin tokens are invalidated before the new principal is bound. Whoever wrote
  the token file already owns the box; a caller who did not write it is refused
  naming the file it expected.
  """
  @spec claim(binary(), keyword()) ::
          {:ok, %{principal: binary(), minted_token: binary() | nil}}
          | {:error, term()}
  def claim(presented_token, opts \\ []) when is_binary(presented_token) do
    path = Config.bootstrap_token_path()

    cond do
      File.exists?(path) ->
        with {:ok, stored} <- read_token_file(path),
             :ok <- compare_constant_time(stored, presented_token) do
          recover_or_bind(path, opts)
        end

      RoleStore.admin_system_exists?() ->
        {:error, :bootstrap_already_completed}

      true ->
        {:error, {:bootstrap_token_missing, path}}
    end
  end

  # ----- private -----

  # The one binding step, whichever claim reached it. A claim that finds a
  # previous admin is a recovery, and the previous identity is invalidated
  # first, so a recovered controller holds exactly one admin.
  defp recover_or_bind(path, opts) do
    recovery =
      if RoleStore.admin_system_exists?(), do: invalidate_previous_admin("recovery-claim")

    with {:ok, principal, minted} <- resolve_principal(opts),
         # A claim leaves its principal holding exactly `admin` in `_system`,
         # whatever it held before: a break-glass claim must never be refused
         # because the claimant already holds roles somewhere else. A crash
         # between the two leaves it unbound, and the token file is still there
         # to retry with.
         :ok <- RoleStore.unbind(principal),
         :ok <- RoleStore.bind(principal, Namespace.system_name(), :admin, bound_by: principal),
         :ok <- AuthState.set_enabled(principal) do
      _ = File.rm(path)

      if recovery do
        {unbound, revoked} = recovery

        Logger.warning(
          "[authz.bootstrap] recovery claim accepted against a RUNNING controller: " <>
            "unbound #{length(unbound)} admin@_system binding(s), revoked " <>
            "#{length(revoked)} _system admin token(s); admin@_system is now #{principal}"
        )

        emit_recovery(:online, unbound, length(revoked))
      end

      emit_bootstrap_completed(principal)
      {:ok, %{principal: principal, minted_token: minted}}
    end
  end

  @doc """
  Unbind `principal` and revoke its live `_system` admin tokens, as `actor`.
  What a cluster does to the admin of a claim that lost to another one.
  """
  @spec invalidate_admin(binary(), binary()) :: :ok
  def invalidate_admin(principal, actor) do
    :ok = RoleStore.unbind(principal)
    _ = revoke_admin_tokens(&(&1 == principal), actor)
    :ok
  end

  # Every `admin@_system` binding dropped and every live `_system` admin token
  # revoked. Shared by the offline marker and the online recovery claim, because
  # "the previous admin no longer authorizes" is one rule and both paths owe it.
  defp invalidate_previous_admin(actor) do
    unbound =
      for %{principal: principal, namespace: ns, roles: roles} <- RoleStore.list(),
          :admin in roles,
          Namespace.system?(ns) do
        # The whole identity: a lost admin keeps no other role either.
        :ok = RoleStore.unbind(principal)
        principal
      end

    {unbound, revoke_admin_tokens(fn _principal -> true end, actor)}
  end

  defp revoke_admin_tokens(principal?, actor) do
    for %{token_hash: hash, namespace: ns, role: :admin, revoked_at: nil, principal: principal} <-
          TokenStore.list(:all),
        Namespace.system?(ns),
        principal?.(principal) do
      _ = TokenStore.revoke(String.slice(hash, 0, 12), revoked_by_principal: actor)
      hash
    end
  end

  defp write_bootstrap_token() do
    path = Config.bootstrap_token_path()
    dir = Path.dirname(path)

    case ensure_dir_safe(dir) do
      :ok ->
        raw =
          :crypto.strong_rand_bytes(@token_bytes) |> Base.encode32(case: :lower, padding: false)

        case File.write(path, raw) do
          :ok ->
            _ = File.chmod(path, 0o600)

            Logger.warning(
              "[authz.bootstrap] wrote one-time bootstrap token to #{path}; " <>
                "run `flz auth bootstrap < #{path}` to claim admin@_system"
            )

            :ok

          {:error, reason} ->
            {:error, {:bootstrap_token_write_failed, path, reason}}
        end

      err ->
        err
    end
  end

  defp ensure_dir_safe(dir) do
    parent = Path.dirname(dir)

    with :ok <- check_parent_mode(parent) do
      Layout.claim(:auth, dir)
    end
  end

  defp check_parent_mode(parent) do
    case File.stat(parent) do
      {:ok, %File.Stat{type: :directory, mode: mode}} ->
        cond do
          Bitwise.band(mode, 0o022) == 0 -> :ok
          # A sticky shared dir (e.g. /tmp) is never ours to tighten.
          Bitwise.band(mode, 0o1000) != 0 -> refuse_permissive_parent(parent, mode)
          true -> tighten_parent_mode(parent, mode)
        end

      _ ->
        :ok
    end
  end

  # A umask-inherited 0?75 on the app's own state root would refuse
  # bootstrap on every fresh install; strip the write bits in place
  # instead. A parent we cannot chmod is genuinely foreign — refuse.
  defp tighten_parent_mode(parent, mode) do
    case File.chmod(parent, Bitwise.band(mode, 0o755)) do
      :ok ->
        Logger.warning(
          "[authz.bootstrap] removed group/other-write bits from #{parent} " <>
            "(was #{Integer.to_string(mode, 8)})"
        )

        :ok

      {:error, _} ->
        refuse_permissive_parent(parent, mode)
    end
  end

  defp refuse_permissive_parent(parent, mode) do
    Logger.error(
      "[authz.bootstrap] refusing to create bootstrap.token: parent #{parent} " <>
        "has permissive mode (#{Integer.to_string(mode, 8)}); " <>
        "remove group/other-write bits and retry"
    )

    {:error, {:permissive_auth_parent, parent}}
  end

  defp read_token_file(path) do
    case File.read(path) do
      {:ok, contents} -> {:ok, String.trim(contents)}
      {:error, reason} -> {:error, {:bootstrap_token_read_failed, path, reason}}
    end
  end

  defp compare_constant_time(stored, presented) do
    if byte_size(stored) == byte_size(presented) and :crypto.hash_equals(stored, presented) do
      :ok
    else
      {:error, :bootstrap_token_mismatch}
    end
  end

  defp resolve_principal(opts) do
    cond do
      principal = Keyword.get(opts, :cert_principal) ->
        {:ok, principal, nil}

      principal = Keyword.get(opts, :principal) ->
        {:ok, principal, nil}

      true ->
        synthetic_principal = synthesise_admin_principal()

        case TokenStore.issue(
               synthetic_principal,
               Namespace.system_name(),
               :admin,
               Config.default_token_ttl_seconds(),
               label: "bootstrap-admin"
             ) do
          {:ok, raw, _entry} -> {:ok, synthetic_principal, raw}
          {:error, _} = err -> err
        end
    end
  end

  defp synthesise_admin_principal() do
    suffix = :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
    "admin-" <> suffix
  end

  defp emit_recovery(mode, unbound_principals, revoked_count) do
    SourceRegistration.emit(:rebootstrap_recovery, %{}, %{
      mode: mode,
      unbound_principals: unbound_principals,
      revoked_tokens: revoked_count
    })

    :ok
  rescue
    _ -> :ok
  end

  defp emit_bootstrap_completed(principal) do
    SourceRegistration.emit(:bootstrap_completed, %{}, %{principal: principal})

    :ok
  rescue
    _ -> :ok
  end
end
