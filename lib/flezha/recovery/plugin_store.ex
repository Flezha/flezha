defmodule Flezha.Recovery.PluginStore do
  @moduledoc """
  Durable local storage for recoverable plugins enabled on this node.

  Parallel to `Flezha.Recovery.Store` (which covers processors) but keyed by
  `plugin_id` and backed by its own DETS file under the same
  `recovery_data_dir`. Rows are the driver struct with transient fields
  (pids, live connection refs) stripped by the caller — see design §D12.

  Opened with `auto_save: :infinity` and sync-on-event: every `put/2` and
  `delete/1` calls `:dets.sync/1` before returning. Plugin lifecycle events
  are operator-driven and infrequent, so an auto_save timer would only add
  a loss window.
  """

  use GenServer

  require Logger

  @table :flz_recovery_plugins
  @dets_filename "recovery_plugins.dets"

  @type plugin_id() :: binary()
  @type row() :: %{
          plugin: struct(),
          stored_node: node(),
          inserted_at: integer()
        }

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Insert or overwrite the row for `plugin_id`. Accepts either a plugin
  struct (wrapped automatically into the row shape) or a full row map
  (merged with `stored_node` / `inserted_at`). No-op if store is down.
  """
  @spec put(plugin_id(), struct() | map()) :: :ok | {:error, term()}
  def put(plugin_id, plugin_or_row) when is_binary(plugin_id) do
    if table_ready?() do
      row = build_row(plugin_or_row)

      case :dets.insert(@table, {plugin_id, row}) do
        :ok ->
          sync_after(:put, plugin_id)
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    else
      :ok
    end
  end

  defp sync_after(op, key) do
    case :dets.sync(@table) do
      :ok ->
        Logger.debug("[recovery] dets sync op=#{op} key=#{inspect(key)} table=#{@table}")
        :ok

      {:error, reason} ->
        Logger.warning(
          "[recovery] dets sync FAILED op=#{op} key=#{inspect(key)} reason=#{inspect(reason)}"
        )

        {:error, reason}
    end
  end

  defp build_row(plugin) when is_struct(plugin) do
    %{
      plugin: plugin,
      stored_node: node(),
      inserted_at: System.system_time(:millisecond)
    }
  end

  defp build_row(%{plugin: _} = row) when is_map(row) do
    row
    |> Map.put_new(:stored_node, node())
    |> Map.put_new(:inserted_at, System.system_time(:millisecond))
  end

  @doc "Delete the row for `plugin_id`. Idempotent; no-op if store is down."
  @spec delete(plugin_id()) :: :ok
  def delete(plugin_id) when is_binary(plugin_id) do
    if table_ready?() do
      :dets.delete(@table, plugin_id)
      sync_after(:delete, plugin_id)
    end

    :ok
  end

  @doc "Return all stored rows as `[{plugin_id, row}]`."
  @spec all() :: [{plugin_id(), row()}]
  def all do
    if table_ready?() do
      :dets.match_object(@table, {:_, :_})
    else
      []
    end
  end

  @doc "Number of rows currently persisted (0 when store is down)."
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

  @doc "Flush & close the table. Primarily used by tests."
  @spec close() :: :ok | {:error, term()}
  def close, do: GenServer.call(__MODULE__, :close)

  @doc "Absolute path of the underlying DETS file."
  @spec path() :: String.t()
  def path, do: GenServer.call(__MODULE__, :path)

  ## GenServer callbacks

  @impl true
  def init(_opts) do
    dir = Application.get_env(:flezha, :recovery_data_dir, "app_data/recovery")
    :ok = File.mkdir_p!(dir)
    file_path = Path.join(dir, @dets_filename)

    case :dets.open_file(@table, [
           {:type, :set},
           {:auto_save, :infinity},
           {:file, String.to_charlist(file_path)}
         ]) do
      {:ok, @table} ->
        rows = count()

        :telemetry.execute(
          [:flezha, :recovery, :plugin_store_opened],
          %{rows: rows},
          %{path: file_path}
        )

        {:ok, %{path: file_path}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  @impl true
  def handle_call(:path, _from, %{path: path} = state), do: {:reply, path, state}

  def handle_call(:close, _from, state) do
    {:reply, :dets.close(@table), state}
  end

  @impl true
  def terminate(_reason, _state) do
    _ = :dets.close(@table)
    :ok
  end

  defp table_ready? do
    not is_nil(Process.whereis(__MODULE__))
  end
end
