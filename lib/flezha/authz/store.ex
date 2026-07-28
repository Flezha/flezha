defmodule Flezha.AuthZ.Store do
  @moduledoc """
  Shared helpers for the control-plane DETS stores (the auth-state
  files `tokens.dets`, `roles.dets`, `namespaces.dets`, `state.dets`,
  `quota_counters.dets`, plus `schedules.dets`).

  Centralises:

    * `ensure_auth_dir!/1` — creates the store directory with mode
      `0700` (the parent-mode refusal is `Bootstrap.check_and_write/0`'s
      job, not this module's — see the function doc).
    * `open/2` — opens a DETS file as a `:set` with `auto_save: 0`
      (we `:dets.sync/1` on every mutation), and rotates a corrupt
      file to `<name>.corrupt-<unix_ts>` before retrying.
    * `corrupted?/1` — predicate used by the supervisor's
      crash-recovery boot path.
    * `init_table/2` — the shared `init/1` body (ensure dir + open)
      for the uncached stores.
    * the ETS-read-cache skeleton for the cached stores
      (`init_cached/3`, `cache_lookup/2`, `cache_values/1`, `put/4`,
      `put_new/4`, `update/4`, `delete/3`): every mutation writes
      DETS first with `:dets.sync/1`, then updates the cache, so a
      crash mid-write loses the cache update but never an unsynced
      mutation. Existence checks read DETS, never the cache.
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

  @doc """
  Shared `init/1` body for the uncached stores: ensure the parent
  directory and open the DETS table. Returns GenServer init tuples.
  """
  @spec init_table(atom(), String.t()) :: {:ok, %{path: String.t()}} | {:stop, term()}
  def init_table(table, file_path) when is_atom(table) do
    :ok = ensure_auth_dir!(Path.dirname(file_path))

    case open(table, file_path) do
      {:ok, _} -> {:ok, %{path: file_path}}
      {:error, reason} -> {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @doc """
  Shared `init/1` body for the ETS-cached stores: `init_table/2` plus
  (re)creating the named cache and warming it from disk.
  """
  @spec init_cached(atom(), atom(), String.t()) :: {:ok, %{path: String.t()}} | {:stop, term()}
  def init_cached(table, cache, file_path) when is_atom(table) and is_atom(cache) do
    with {:ok, state} <- init_table(table, file_path) do
      ensure_cache!(cache)
      warm_cache(table, cache)
      {:ok, state}
    end
  end

  @doc "Cached read. `:error` when the cache is absent or the key missing."
  @spec cache_lookup(atom(), term()) :: {:ok, term()} | :error
  def cache_lookup(cache, key) do
    case :ets.whereis(cache) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(cache, key) do
          [{^key, value}] -> {:ok, value}
          _ -> :error
        end
    end
  rescue
    _ -> :error
  end

  @doc "Every cached value, unordered. `[]` when the cache is absent."
  @spec cache_values(atom()) :: [term()]
  def cache_values(cache) do
    case :ets.whereis(cache) do
      :undefined -> []
      _ -> :ets.tab2list(cache) |> Enum.map(fn {_key, value} -> value end)
    end
  end

  @doc "Write-through insert: DETS insert + sync, then cache insert."
  @spec put(atom(), atom(), term(), term()) :: :ok
  def put(table, cache, key, value) do
    :ok = :dets.insert(table, {key, value})
    :ok = :dets.sync(table)
    :ets.insert(cache, {key, value})
    :ok
  end

  @doc "`put/4` refusing to overwrite an existing key."
  @spec put_new(atom(), atom(), term(), term()) :: :ok | {:error, :already_exists}
  def put_new(table, cache, key, value) do
    case :dets.lookup(table, key) do
      [_] -> {:error, :already_exists}
      [] -> put(table, cache, key, value)
    end
  end

  @doc "Write-through update of an existing key: `fun` maps the stored value."
  @spec update(atom(), atom(), term(), (term() -> term())) ::
          {:ok, term()} | {:error, :not_found}
  def update(table, cache, key, fun) when is_function(fun, 1) do
    case :dets.lookup(table, key) do
      [{^key, value}] ->
        updated = fun.(value)
        :ok = put(table, cache, key, updated)
        {:ok, updated}

      [] ->
        {:error, :not_found}
    end
  end

  @doc "Write-through delete: DETS delete + sync, then cache delete."
  @spec delete(atom(), atom(), term()) :: :ok
  def delete(table, cache, key) do
    :ok = :dets.delete(table, key)
    :ok = :dets.sync(table)
    :ets.delete(cache, key)
    :ok
  end

  ## ----- private -----

  defp ensure_cache!(cache) do
    case :ets.whereis(cache) do
      :undefined ->
        :ets.new(cache, [:set, :public, :named_table, read_concurrency: true])

      _ ->
        :ets.delete_all_objects(cache)
    end

    :ok
  end

  defp warm_cache(table, cache) do
    :dets.match_object(table, {:_, :_})
    |> Enum.each(fn {key, value} -> :ets.insert(cache, {key, value}) end)

    :ok
  end

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
