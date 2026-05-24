defmodule Flezha.AuthZ.Bootstrap do
  @moduledoc """
  One-time bootstrap flow for the first `admin@_system` principal.

  Lifecycle:

    1. **`check_and_write/0`** — invoked at boot when no
       `admin@_system` binding exists. Ensures `<auth_dir>` exists
       with mode `0700`, refuses to operate against a permissive
       parent directory, writes a 256-bit token to
       `<auth_dir>/bootstrap.token` with mode `0600`, and emits a
       single `warn` line naming the path.
    2. **`claim/2`** — invoked by `flz auth bootstrap <token>`.
       Verifies the presented token against `bootstrap.token`,
       resolves a principal (cert SAN > cert DN > `--principal`
       flag > freshly minted admin token), binds `admin@_system`,
       flips `AuthState.enabled` to `true`, unlinks
       `bootstrap.token`, and emits a `:bootstrap_completed` event
       on `:platform_authz`.

  Re-claim after a successful `claim/2` is rejected with
  `{:error, :bootstrap_already_completed}` because the token file is
  unlinked.
  """

  require Logger

  alias Flezha.AuthZ.{AuthState, Config, Namespace, RoleStore, TokenStore}
  alias Flezha.Observability.{Bus, Signal}

  @source :platform_authz
  @token_bytes 32

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
  """
  @spec claim(binary(), keyword()) ::
          {:ok, %{principal: binary(), minted_token: binary() | nil}}
          | {:error, term()}
  def claim(presented_token, opts \\ []) when is_binary(presented_token) do
    path = Config.bootstrap_token_path()

    cond do
      RoleStore.admin_system_exists?() ->
        {:error, :bootstrap_already_completed}

      not File.exists?(path) ->
        {:error, :bootstrap_already_completed}

      true ->
        with {:ok, stored} <- read_token_file(path),
             :ok <- compare_constant_time(stored, presented_token),
             {:ok, principal, minted} <- resolve_principal(opts),
             :ok <-
               RoleStore.bind(principal, Namespace.system_name(), :admin, bound_by: principal),
             :ok <- AuthState.set_enabled() do
          _ = File.rm(path)
          emit_bootstrap_completed(principal)
          {:ok, %{principal: principal, minted_token: minted}}
        end
    end
  end

  # ----- private -----

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
                "run `flz auth bootstrap <token>` to claim admin@_system"
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

    with :ok <- check_parent_mode(parent),
         :ok <- File.mkdir_p(dir) do
      _ = File.chmod(dir, 0o700)
      :ok
    end
  end

  defp check_parent_mode(parent) do
    case File.stat(parent) do
      {:ok, %File.Stat{type: :directory, mode: mode}} ->
        if Bitwise.band(mode, 0o022) != 0 do
          Logger.error(
            "[authz.bootstrap] refusing to create bootstrap.token: parent #{parent} " <>
              "has permissive mode (#{Integer.to_string(mode, 8)}); " <>
              "remove group/other-write bits and retry"
          )

          {:error, {:permissive_auth_parent, parent}}
        else
          :ok
        end

      _ ->
        :ok
    end
  end

  defp read_token_file(path) do
    case File.read(path) do
      {:ok, contents} -> {:ok, String.trim(contents)}
      {:error, reason} -> {:error, {:bootstrap_token_read_failed, reason}}
    end
  end

  defp compare_constant_time(stored, presented) do
    if byte_size(stored) == byte_size(presented) and :crypto.hash_equals(stored, presented) do
      :ok
    else
      {:error, :bootstrap_token_mismatch}
    end
  rescue
    UndefinedFunctionError ->
      # Pre-OTP 25 fallback; the local OTP is 27/28, but keep a defensive shim.
      if stored == presented, do: :ok, else: {:error, :bootstrap_token_mismatch}
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
               Config.default_token_ttl_seconds(:cli),
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

  defp emit_bootstrap_completed(principal) do
    Bus.emit(
      Signal.new(%{
        kind: :event,
        source: @source,
        name: :bootstrap_completed,
        metadata: %{principal: principal}
      })
    )

    :ok
  rescue
    _ -> :ok
  end
end
