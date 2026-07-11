defmodule Flezha.Plugin.Provider.Postgres.SinkSup do
  @moduledoc """
  Per-`plugin_id` `SinkWorker` supervision on the shared
  `Flezha.Plugin.Provider.WorkerSup` core.

  Sink workers are started on demand from
  `Flezha.Plugin.Provider.Postgres.init_provider/1` via `ensure_worker/1`;
  a re-prepared plugin (Overwatch restart) reuses its live worker so
  buffered records and the connection pool survive the restart.
  """

  alias Flezha.Plugin.Provider.Postgres.SinkWorker
  alias Flezha.Plugin.Provider.WorkerSup

  def child_spec(_opts), do: WorkerSup.child_spec(name: __MODULE__)

  @spec ensure_worker(keyword()) :: {:ok, pid()} | {:error, term()}
  def ensure_worker(opts) when is_list(opts),
    do: WorkerSup.ensure_worker(__MODULE__, Keyword.fetch!(opts, :plugin_id), {SinkWorker, opts})

  @doc "Stop the sink worker for `plugin_id` if running."
  @spec stop_worker(binary()) :: :ok
  def stop_worker(plugin_id) do
    _ = WorkerSup.stop_worker(__MODULE__, plugin_id)
    :ok
  end

  def registry_name, do: WorkerSup.registry_name(__MODULE__)
end
