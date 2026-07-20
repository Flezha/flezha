defmodule Flezha.Plugin.Provider.OperativeForward.ForwardSup do
  @moduledoc """
  Per-`{plugin_id, target_operative_id}` `ForwardWorker` supervision on the
  shared `Flezha.Plugin.Provider.WorkerSup` core.

  Forward workers are started on demand from
  `Flezha.Plugin.Provider.OperativeForward.init_provider/1` via
  `ensure_worker/1`; a re-prepared plugin (Overwatch restart) reuses its live
  worker so queued-but-undelivered payloads survive the restart, exactly as
  buffered Kafka records and Postgres rows do.
  """

  alias Flezha.Plugin.Provider.OperativeForward.ForwardWorker
  alias Flezha.Plugin.Provider.WorkerSup

  def child_spec(_opts), do: WorkerSup.child_spec(name: __MODULE__)

  @spec ensure_worker(keyword()) :: {:ok, pid()} | {:error, term()}
  def ensure_worker(opts) when is_list(opts),
    do: WorkerSup.ensure_worker(__MODULE__, key(opts), {ForwardWorker, opts})

  @doc "Stop the forward worker for `key` if running."
  @spec stop_worker({binary(), binary()}) :: :ok
  def stop_worker(key) do
    _ = WorkerSup.stop_worker(__MODULE__, key)
    :ok
  end

  @spec key(keyword()) :: {binary(), binary()}
  def key(opts),
    do: {Keyword.fetch!(opts, :plugin_id), Keyword.fetch!(opts, :target_operative_id)}

  def registry_name, do: WorkerSup.registry_name(__MODULE__)
end
