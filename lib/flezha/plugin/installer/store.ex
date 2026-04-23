defmodule Flezha.Plugin.Installer.Store do
  @moduledoc """
  DETS-backed catalog of installed plugins, keyed by `plugin_id` (binary).

  Parallel to `Flezha.Recovery.PluginStore` but answering a different question:
  "what packages have been installed on this node and which are enabled?" —
  not "which `%Flezha.Plugin{}` subscriptions are live right now." Rows live
  next to the recovery DETS at `<recovery_data_dir>/installed_plugins.dets`.

  Every mutator ends with `:dets.sync/1` so the row is on disk before the
  call returns — installs must survive an immediate crash. Opening a corrupt
  file rotates it to `installed_plugins.dets.corrupt-<monotonic>` and starts
  fresh; the on-disk `app_data/plugins/installed/` trees remain so operators
  can rehydrate by re-installing.
  """

  use GenServer

  require Logger

  @table :flz_installed_plugins
  @auto_save_ms 5_000
  @dets_filename "installed_plugins.dets"

  @type plugin_id() :: binary()
  @type status() :: :enabled | :disabled | :load_failed | :pending_upgrade
  @type row() :: %{
          required(:version) => binary(),
          required(:status) => status(),
          required(:path) => binary(),
          required(:manifest_digest) => binary(),
          required(:modules) => [atom()],
          required(:installed_at) => integer(),
          required(:source_checksums) => %{optional(binary()) => binary()},
          optional(:description) => binary(),
          optional(:author) => binary(),
          optional(:provides) => [map()],
          optional(:dependencies) => [{binary(), binary()}]
        }

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Insert or overwrite the catalog row for `plugin_id`. Row must include at
  least `:version`, `:status`, `:path`, `:manifest_digest`, `:modules`,
  `:installed_at`, `:source_checksums`. Synchronous: returns only after
  `:dets.sync/1` completes.
  """
  @spec put(plugin_id(), row()) :: :ok | {:error, term()}
  def put(plugin_id, row) when is_binary(plugin_id) and is_map(row) do
    GenServer.call(__MODULE__, {:put, plugin_id, row})
  end

  @doc "Return the row for `plugin_id` or `nil`."
  @spec get(plugin_id()) :: row() | nil
  def get(plugin_id) when is_binary(plugin_id) do
    if table_ready?() do
      case :dets.lookup(@table, plugin_id) do
        [{^plugin_id, row}] -> row
        _ -> nil
      end
    else
      nil
    end
  end

  @doc "Delete the row for `plugin_id`. Synchronous; idempotent."
  @spec delete(plugin_id()) :: :ok
  def delete(plugin_id) when is_binary(plugin_id) do
    GenServer.call(__MODULE__, {:delete, plugin_id})
  end

  @doc "Return all rows as `[{plugin_id, row}]`."
  @spec all() :: [{plugin_id(), row()}]
  def all do
    if table_ready?() do
      :dets.match_object(@table, {:_, :_})
    else
      []
    end
  end

  @doc "Row count."
  @spec count() :: non_neg_integer()
  def count do
    if table_ready?() do
      case :dets.info(@table, :size) do
        n when is_integer(n) -> n
        _ -> 0
      end
    else
      0
    end
  end

  @doc """
  Update only the `:status` field of an existing row; no-op if the row is
  absent. Synchronous: `:dets.sync/1` before returning.
  """
  @spec update_status(plugin_id(), status()) :: :ok | {:error, :not_found}
  def update_status(plugin_id, status)
      when is_binary(plugin_id) and is_atom(status) do
    GenServer.call(__MODULE__, {:update_status, plugin_id, status})
  end

  @doc "Absolute path of the backing DETS file."
  @spec path() :: String.t()
  def path do
    GenServer.call(__MODULE__, :path)
  end

  @doc "Close the DETS table. Primarily for tests."
  @spec close() :: :ok | {:error, term()}
  def close, do: GenServer.call(__MODULE__, :close)

  ## GenServer

  @impl true
  def init(_opts) do
    dir = Application.get_env(:flezha, :recovery_data_dir, "app_data/recovery")
    :ok = File.mkdir_p!(dir)
    file_path = Path.join(dir, @dets_filename)

    case open_dets(file_path) do
      {:ok, _} ->
        rows = count()

        :telemetry.execute(
          [:flezha, :plugin_installer, :store_opened],
          %{rows: rows},
          %{path: file_path}
        )

        {:ok, %{path: file_path}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @impl true
  def handle_call({:put, plugin_id, row}, _from, state) do
    reply =
      case :dets.insert(@table, {plugin_id, row}) do
        :ok ->
          _ = :dets.sync(@table)
          :ok

        {:error, reason} ->
          {:error, reason}
      end

    {:reply, reply, state}
  end

  def handle_call({:delete, plugin_id}, _from, state) do
    _ = :dets.delete(@table, plugin_id)
    _ = :dets.sync(@table)
    {:reply, :ok, state}
  end

  def handle_call({:update_status, plugin_id, status}, _from, state) do
    reply =
      case :dets.lookup(@table, plugin_id) do
        [{^plugin_id, row}] ->
          :ok = :dets.insert(@table, {plugin_id, Map.put(row, :status, status)})
          _ = :dets.sync(@table)
          :ok

        _ ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  def handle_call(:path, _from, %{path: path} = state), do: {:reply, path, state}
  def handle_call(:close, _from, state), do: {:reply, :dets.close(@table), state}

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  ## Private

  defp open_dets(file_path) do
    case :dets.open_file(@table, [
           {:type, :set},
           {:auto_save, @auto_save_ms},
           {:file, String.to_charlist(file_path)}
         ]) do
      {:ok, @table} ->
        {:ok, @table}

      {:error, reason} ->
        rotate_and_retry(file_path, reason)
    end
  end

  # A corrupt file is a deployment hazard, not a fatal error: rename out of
  # the way, log both paths so the operator can recover manually, and start
  # fresh. The on-disk `installed/<id>/<version>/` trees are unaffected.
  defp rotate_and_retry(file_path, reason) do
    rotated = file_path <> ".corrupt-" <> Integer.to_string(System.monotonic_time())

    case File.rename(file_path, rotated) do
      :ok ->
        Logger.error(
          "[plugin_installer] corrupt DETS at #{file_path} (#{inspect(reason)}); " <>
            "rotated to #{rotated}, starting fresh"
        )

        :dets.open_file(@table, [
          {:type, :set},
          {:auto_save, @auto_save_ms},
          {:file, String.to_charlist(file_path)}
        ])

      {:error, rename_reason} ->
        Logger.error(
          "[plugin_installer] corrupt DETS at #{file_path} (#{inspect(reason)}) and " <>
            "rename failed: #{inspect(rename_reason)}"
        )

        {:error, reason}
    end
  end

  defp table_ready? do
    not is_nil(Process.whereis(__MODULE__))
  end
end
