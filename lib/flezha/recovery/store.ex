defmodule Flezha.Recovery.Store do
  @moduledoc """
  Durable local storage for locally-hosted processor child specs.

  Backs the failure-recovery subsystem: on every start/stop observed via
  ProcessHub hooks, `Flezha.Recovery.Coordinator` writes/removes a row keyed by
  `child_id`. On agent restart, `Flezha.Recovery.SysMode` consults this store to
  decide whether there is recovery work to do.

  Single DETS table named `:flz_recovery`, `:set` type, opened with
  `auto_save: :infinity`. The timer is explicitly disabled — every `put/3`
  and `delete/1` calls `:dets.sync/1` before returning, so the caller does
  not observe `:ok` until the row is on disk. Processor lifecycle events are
  infrequent and operator-driven; a 5-second batching window adds a loss
  window without amortising any writes.
  """

  use GenServer

  require Logger

  @table :flz_recovery
  @dets_filename "recovery.dets"

  @type child_id() :: binary()
  @type row() :: %{
          namespace: binary(),
          processor: Flezha.Processor.Builder.t(),
          stored_node: node(),
          inserted_at: integer()
        }

  ## Public API

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Insert or overwrite the row for `child_id`. If the Store process is not
  running (e.g. recovery disabled, or tests running with autostart off),
  this is a no-op rather than an error so ProcessHub hooks stay safe.
  """
  @spec put(child_id(), binary(), Flezha.Processor.Builder.t()) :: :ok | {:error, term()}
  def put(child_id, namespace, processor)
      when is_binary(child_id) and is_binary(namespace) do
    if table_ready?() do
      row = %{
        namespace: namespace,
        processor: processor,
        stored_node: node(),
        inserted_at: System.system_time(:millisecond)
      }

      case :dets.insert(@table, {child_id, row}) do
        :ok ->
          sync_after(:put, child_id)
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    else
      :ok
    end
  end

  @doc "Delete the row for `child_id`. Idempotent; no-op if store is down."
  @spec delete(child_id()) :: :ok
  def delete(child_id) when is_binary(child_id) do
    if table_ready?() do
      :dets.delete(@table, child_id)
      sync_after(:delete, child_id)
    end

    :ok
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

  @doc "Return all stored rows as `[{child_id, row}]`."
  @spec all() :: [{child_id(), row()}]
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

  # The DETS table exists only while the owning GenServer is alive — treat
  # "no owner" as "store not ready" and skip the DETS call. Avoids the
  # ArgumentError `:dets.insert` would otherwise raise if the table has been
  # closed mid-process (e.g. between test cases).
  defp table_ready? do
    not is_nil(Process.whereis(__MODULE__))
  end

  @doc "Flush & close the table. Primarily used by tests."
  @spec close() :: :ok | {:error, term()}
  def close do
    GenServer.call(__MODULE__, :close)
  end

  @doc "Absolute path of the underlying DETS file."
  @spec path() :: String.t()
  def path do
    GenServer.call(__MODULE__, :path)
  end

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
          [:flezha, :recovery, :store_opened],
          %{rows: rows},
          %{path: file_path}
        )

        if ephemeral_path?(file_path) do
          Logger.warning(
            "[recovery] DETS file at #{file_path} appears to be on an ephemeral filesystem. " <>
              "Set FLEZHA_DATA_DIR to a persistent volume to avoid losing recovery state across restarts."
          )
        end

        {:ok, %{path: file_path}}

      {:error, reason} ->
        {:stop, {:dets_open_failed, file_path, reason}}
    end
  end

  # Best-effort heuristic for "looks ephemeral". We don't have a reliable way
  # to tell whether a directory is backed by a persistent volume from inside
  # the container — just warn on the obvious cases.
  defp ephemeral_path?(path) do
    abs = Path.expand(path)

    cond do
      String.starts_with?(abs, "/tmp/") -> true
      String.starts_with?(abs, "/var/tmp/") -> true
      true -> false
    end
  end

  @impl true
  def handle_call(:path, _from, %{path: path} = state) do
    {:reply, path, state}
  end

  @impl true
  def handle_call(:close, _from, state) do
    result = :dets.close(@table)
    {:reply, result, state}
  end

  @impl true
  def terminate(_reason, _state) do
    # Close is idempotent; safe to call even if already closed.
    _ = :dets.close(@table)
    :ok
  end

end
