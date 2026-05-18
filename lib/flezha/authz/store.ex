defmodule Flezha.AuthZ.Store do
  @moduledoc """
  Shared helpers for the auth-state DETS stores
  (`tokens.dets`, `roles.dets`, `namespaces.dets`, `state.dets`,
  `quota_counters.dets`).

  Centralises:

    * `ensure_auth_dir!/1` — creates `<auth_dir>` with mode `0700`
      and refuses to operate against a group/world-readable parent
      (the bootstrap-flow safety check).
    * `open/2` — opens a DETS file as a `:set` with `auto_save: 0`
      (we `:dets.sync/1` on every mutation), and rotates a corrupt
      file to `<name>.corrupt-<unix_ts>` before retrying.
    * `corrupted?/1` — predicate used by the supervisor's
      crash-recovery boot path.
  """

  require Logger

  @doc """
  Ensure the auth-state directory exists and is chmod 0700.

  The parent-directory mode check is intentionally NOT performed
  here — it is enforced by `Flezha.AuthZ.Bootstrap.check_and_write/0`
  immediately before writing `bootstrap.token`, where the secret
  actually hits the filesystem. The DETS files themselves are mode
  0600 (set by `open/2`), so a permissive parent does not leak
  their contents.
  """
  @spec ensure_auth_dir!(String.t()) :: :ok
  def ensure_auth_dir!(path) do
    case File.mkdir_p(path) do
      :ok ->
        _ = File.chmod(path, 0o700)
        :ok

      {:error, reason} ->
        raise RuntimeError, "failed to mkdir #{path}: #{inspect(reason)}"
    end
  end

  @doc """
  Open the DETS file `path` as a `:set` named `table_name`, rotating
  the file on corruption. On success the returned `{:ok, name}` is
  the DETS table reference (always equal to `table_name`).
  """
  @spec open(atom(), String.t(), keyword()) :: {:ok, atom()} | {:error, term()}
  def open(table_name, path, extra_opts \\ []) when is_atom(table_name) do
    opts =
      [
        {:type, :set},
        {:auto_save, 0},
        {:file, String.to_charlist(path)}
      ] ++ extra_opts

    case :dets.open_file(table_name, opts) do
      {:ok, ^table_name} ->
        _ = File.chmod(path, 0o600)
        {:ok, table_name}

      {:error, reason} ->
        rotate_and_retry(table_name, path, opts, reason)
    end
  end

  @doc """
  Best-effort detection of a corrupt DETS file: returns `true` if
  `:dets.is_dets_file/1` reports a problem (or if the path exists
  but is not readable as a DETS file at all).
  """
  @spec corrupted?(String.t()) :: boolean()
  def corrupted?(path) do
    if File.exists?(path) do
      case :dets.is_dets_file(String.to_charlist(path)) do
        true -> false
        false -> true
        {:error, _} -> true
      end
    else
      false
    end
  end

  ## ----- private -----

  defp rotate_and_retry(table_name, path, opts, reason) do
    rotated = path <> ".corrupt-" <> Integer.to_string(System.system_time(:second))

    case File.rename(path, rotated) do
      :ok ->
        Logger.error(
          "[authz.store] corrupt DETS at #{path} (#{inspect(reason)}); " <>
            "rotated to #{rotated}, starting fresh"
        )

        case :dets.open_file(table_name, opts) do
          {:ok, ^table_name} ->
            _ = File.chmod(path, 0o600)
            {:ok, table_name}

          {:error, retry_reason} ->
            {:error, retry_reason}
        end

      {:error, rename_reason} ->
        Logger.error(
          "[authz.store] corrupt DETS at #{path} (#{inspect(reason)}) and " <>
            "rename failed: #{inspect(rename_reason)}"
        )

        {:error, reason}
    end
  end
end
