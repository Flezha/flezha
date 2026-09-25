defmodule Flezha.DetsStore do
  @moduledoc """
  The DETS half of `Flezha.State.Store`: how a control-plane store's file is
  opened, rotated when corrupt, and read through a cache.

  Which file, which table and which subtree are the store's own declarations,
  and claiming the directory is `Flezha.State.Store.open/1`'s. This module is
  the storage.

  Centralises:

    * `open/3` — opens a DETS file as a `:set` with `auto_save: 0`
      (we `:dets.sync/1` on every mutation), and rotates a corrupt
      file to `<name>.corrupt-<unix_ts>` before retrying.
    * `init_table/3` — the shared `init/1` body (open + init tuples)
      for the uncached stores.
    * the ETS-read-cache skeleton for the cached stores
      (`init_cached/4`, `cache_lookup/2`, `cache_values/1`, `put/4`,
      `put_new/4`, `update/4`, `delete/3`): every mutation writes
      DETS first with `:dets.sync/1`, then updates the cache, so a
      crash mid-write loses the cache update but never an unsynced
      mutation. Existence checks read DETS, never the cache.
  """

  require Logger

  @doc """
  Open the DETS file `path` as a `:set` named `table_name`, rotating
  the file on corruption. On success the returned `{:ok, name}` is
  the DETS table reference (always equal to `table_name`).

  `opts` are the store's own, from `c:Flezha.State.Store.open_opts/0`:

    * `:auto_save` — the DETS auto-save interval, `0` by default because
      every mutation syncs before its call returns.
    * `:corrupt_suffix` — which clock names a rotated file, `:second`
      (`<name>.corrupt-<unix_ts>`) or `:monotonic`. Both are published
      recovery instructions, so a store keeps the one its users read.
  """
  @spec open(atom(), String.t(), keyword()) :: {:ok, atom()} | {:error, term()}
  def open(table_name, path, opts \\ []) when is_atom(table_name) do
    dets_opts = [
      {:type, :set},
      {:auto_save, Keyword.get(opts, :auto_save, 0)},
      {:file, String.to_charlist(path)}
    ]

    case :dets.open_file(table_name, dets_opts) do
      {:ok, ^table_name} ->
        _ = File.chmod(path, 0o600)
        {:ok, table_name}

      {:error, reason} ->
        rotate_and_retry(table_name, path, dets_opts, reason, corrupt_suffix(opts))
    end
  end

  @doc """
  Shared `init/1` body for the uncached stores: open the DETS table and
  answer the GenServer init tuples. The directory is already claimed by
  `Flezha.State.Store.open/1`, which is what calls this.
  """
  @spec init_table(atom(), String.t(), keyword()) ::
          {:ok, %{path: String.t()}} | {:stop, term()}
  def init_table(table, file_path, opts \\ []) when is_atom(table) do
    case open(table, file_path, opts) do
      {:ok, _} -> {:ok, %{path: file_path}}
      {:error, reason} -> {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @doc """
  Shared `init/1` body for the ETS-cached stores: `init_table/3` plus
  (re)creating the named cache and warming it from disk.
  """
  @spec init_cached(atom(), atom(), String.t(), keyword()) ::
          {:ok, %{path: String.t()}} | {:stop, term()}
  def init_cached(table, cache, file_path, opts \\ [])
      when is_atom(table) and is_atom(cache) do
    with {:ok, state} <- init_table(table, file_path, opts) do
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

  @doc """
  Insert and sync: the row is on disk before the call returns.

  Every mutation on these stores syncs, so a crash mid-write never loses one.
  No implementer may drop it, which is why the write is here and not per owner.
  """
  @spec put(atom(), term(), term()) :: :ok
  def put(table, key, value) do
    :ok = :dets.insert(table, {key, value})
    :dets.sync(table)
  end

  @doc "Write-through insert: `put/3`, then cache insert."
  @spec put(atom(), atom(), term(), term()) :: :ok
  def put(table, cache, key, value) do
    :ok = put(table, key, value)
    :ets.insert(cache, {key, value})
    :ok
  end

  @doc "The stored value for `key`, read from DETS rather than a cache."
  @spec fetch(atom(), term()) :: {:ok, term()} | :error
  def fetch(table, key) do
    case :dets.lookup(table, key) do
      [{^key, value}] -> {:ok, value}
      _ -> :error
    end
  rescue
    _ -> :error
  end

  @doc "Every stored `{key, value}`, unordered. `[]` when the table is absent."
  @spec all(atom()) :: [{term(), term()}]
  def all(table), do: :dets.match_object(table, {:_, :_})

  @doc "Row count, `0` when the table is absent."
  @spec count(atom()) :: non_neg_integer()
  def count(table) do
    case :dets.info(table, :size) do
      size when is_integer(size) -> size
      _ -> 0
    end
  end

  @doc "`put/4` refusing to overwrite an existing key."
  @spec put_new(atom(), atom(), term(), term()) :: :ok | {:error, :already_exists}
  def put_new(table, cache, key, value) do
    case fetch(table, key) do
      {:ok, _} -> {:error, :already_exists}
      :error -> put(table, cache, key, value)
    end
  end

  @doc "Write-through update of an existing key: `fun` maps the stored value."
  @spec update(atom(), atom(), term(), (term() -> term())) ::
          {:ok, term()} | {:error, :not_found}
  def update(table, cache, key, fun) when is_function(fun, 1) do
    case fetch(table, key) do
      {:ok, value} ->
        updated = fun.(value)
        :ok = put(table, cache, key, updated)
        {:ok, updated}

      :error ->
        {:error, :not_found}
    end
  end

  @doc "Delete `key` and sync, so the removal is on disk before the return."
  @spec delete(atom(), term()) :: :ok
  def delete(table, key) do
    :ok = :dets.delete(table, key)
    :dets.sync(table)
  end

  @doc "Write-through delete: `delete/2`, then cache delete."
  @spec delete(atom(), atom(), term()) :: :ok
  def delete(table, cache, key) do
    :ok = delete(table, key)
    :ets.delete(cache, key)
    :ok
  end

  ## ----- private -----

  defp corrupt_suffix(opts) do
    case Keyword.get(opts, :corrupt_suffix, :second) do
      :monotonic -> Integer.to_string(System.monotonic_time())
      :second -> Integer.to_string(System.system_time(:second))
    end
  end

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

  defp rotate_and_retry(table_name, path, opts, reason, suffix) do
    rotated = path <> ".corrupt-" <> suffix

    case File.rename(path, rotated) do
      :ok ->
        Logger.error(
          "[flezha.store] corrupt DETS at #{path} (#{inspect(reason)}); " <>
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
          "[flezha.store] corrupt DETS at #{path} (#{inspect(reason)}) and " <>
            "rename failed: #{inspect(rename_reason)}"
        )

        {:error, reason}
    end
  end
end
