defmodule Flezha.Plugin.Provider.Kafka.ProducerSup do
  @moduledoc """
  Per-`(plugin_id, topic, partition)` `ProducerWorker` supervision on the
  shared `Flezha.Plugin.Provider.WorkerSup` core.

  Producer workers are started on demand from
  `Flezha.Plugin.Provider.Kafka.Producer.init_provider/1` via
  `ensure_worker/1`; a registered worker is shared while
  `ProducerWorker.reusable?/1` holds, else it is reaped first so a
  recovered operative starts a fresh, healthy producer rather than
  re-attaching to one that can no longer drain.
  """

  alias Flezha.Plugin.Provider.Kafka.ProducerWorker
  alias Flezha.Plugin.Provider.WorkerSup

  def child_spec(_opts), do: WorkerSup.child_spec(name: __MODULE__)

  @spec ensure_worker(keyword()) :: {:ok, pid()} | {:error, term()}
  def ensure_worker(opts) when is_list(opts) do
    key =
      {Keyword.fetch!(opts, :plugin_id), Keyword.fetch!(opts, :topic),
       Keyword.fetch!(opts, :partition)}

    WorkerSup.ensure_worker(__MODULE__, key, {ProducerWorker, opts}, &ProducerWorker.reusable?/1)
  end

  @doc "Stop the producer worker for `(plugin_id, topic, partition)` if running."
  @spec stop_worker(binary(), binary(), non_neg_integer()) :: :ok | {:error, :not_found}
  def stop_worker(plugin_id, topic, partition),
    do: WorkerSup.stop_worker(__MODULE__, {plugin_id, topic, partition})

  def registry_name, do: WorkerSup.registry_name(__MODULE__)
  def dyn_sup_name, do: WorkerSup.dyn_sup_name(__MODULE__)
end
