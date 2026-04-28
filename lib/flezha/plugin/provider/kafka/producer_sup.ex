defmodule Flezha.Plugin.Provider.Kafka.ProducerSup do
  @moduledoc """
  Supervises the per-`(plugin_id, topic, partition)` `ProducerWorker`
  GenServers and the `Registry` they live under.

  Wired into `Flezha.Application`'s supervision tree alongside
  `Flezha.Plugin.Provider.Kafka.WorkerRegistry`. Producer workers are
  started on demand from `Flezha.Plugin.Provider.Kafka.Producer.init_provider/1`
  via `start_worker/1`.
  """

  use Supervisor

  alias Flezha.Plugin.Provider.Kafka.ProducerWorker

  @registry Flezha.Plugin.Provider.Kafka.ProducerRegistry
  @dyn_sup Flezha.Plugin.Provider.Kafka.ProducerWorkerSup

  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    children = [
      {Registry, keys: :unique, name: @registry, partitions: 1},
      {DynamicSupervisor, name: @dyn_sup, strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @doc """
  Start (or look up) the `ProducerWorker` for `(plugin_id, topic, partition)`.
  Returns `{:ok, pid}` either way; if a worker is already registered the
  caller does NOT increment any refcount (the registered worker is shared).
  """
  @spec ensure_worker(keyword()) :: {:ok, pid()} | {:error, term()}
  def ensure_worker(opts) when is_list(opts) do
    plugin_id = Keyword.fetch!(opts, :plugin_id)
    topic = Keyword.fetch!(opts, :topic)
    partition = Keyword.fetch!(opts, :partition)

    case Registry.lookup(@registry, {plugin_id, topic, partition}) do
      [{pid, _}] ->
        {:ok, pid}

      [] ->
        case DynamicSupervisor.start_child(@dyn_sup, {ProducerWorker, opts}) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = err -> err
        end
    end
  end

  @doc """
  Stop the producer worker for `(plugin_id, topic, partition)` if running.
  """
  @spec stop_worker(binary(), binary(), non_neg_integer()) :: :ok
  def stop_worker(plugin_id, topic, partition) do
    case Registry.lookup(@registry, {plugin_id, topic, partition}) do
      [{pid, _}] ->
        DynamicSupervisor.terminate_child(@dyn_sup, pid)

      [] ->
        :ok
    end
  end

  def registry_name, do: @registry
  def dyn_sup_name, do: @dyn_sup
end
