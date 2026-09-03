defmodule Flezha.Plugin.Provider.WorkerSup do
  @moduledoc """
  Shared supervision core for providers that run one long-lived worker per
  key: a Registry + DynamicSupervisor pair per provider family, with
  ensure / reap / stop semantics.

  Providers wrap it with their key shape and reuse policy —
  `Kafka.ProducerSup` keys by `{plugin_id, topic, partition}` and reaps
  permanently-parked producers, `Postgres.SinkSup` keys by `plugin_id`.
  Each family is its own instance (own Registry, own DynamicSupervisor) so
  a crash-looping worker exhausts only its family's restart budget.

  `ensure_worker/4` names the child: it puts `via/2` into the worker's opts,
  so a worker never has to name the supervisor that started it.
  """

  use Supervisor

  def child_spec(opts) do
    name = Keyword.fetch!(opts, :name)
    %{id: name, start: {__MODULE__, :start_link, [name]}, type: :supervisor}
  end

  def start_link(name), do: Supervisor.start_link(__MODULE__, name, name: name)

  @impl true
  def init(name) do
    children = [
      {Registry, keys: :unique, name: registry_name(name), partitions: 1},
      {DynamicSupervisor, name: dyn_sup_name(name), strategy: :one_for_one}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  def registry_name(family), do: Module.concat(family, Registry)
  def dyn_sup_name(family), do: Module.concat(family, DynSup)

  @doc "The registered name a worker of `family` takes under `key`."
  @spec via(module(), term()) :: {:via, module(), {atom(), term()}}
  def via(family, key), do: {:via, Registry, {registry_name(family), key}}

  @doc """
  Start (or look up) the worker registered under `key`, naming the child
  `via(family, key)`. A registered worker is shared while `reusable?.(pid)`
  holds, else it is reaped first so the caller gets a fresh, healthy one.

  Every arm that answers an *existing* worker asks `reusable?` first, so a
  worker the check has rejected is never handed back: an existing worker the
  reap could not remove answers `{:error, {:not_reusable, pid}}` rather than
  the pid itself.
  """
  @spec ensure_worker(module(), term(), {module(), keyword()}, (pid() -> boolean())) ::
          {:ok, pid()} | {:error, term()}
  def ensure_worker(family, key, {worker, opts}, reusable? \\ &Process.alive?/1) do
    child = {worker, Keyword.put(opts, :name, via(family, key))}

    case Registry.lookup(registry_name(family), key) do
      [{pid, _}] ->
        case gate(pid, reusable?) do
          {:ok, pid} -> {:ok, pid}
          {:error, _} -> reap_and_start(family, pid, child, reusable?)
        end

      [] ->
        start_worker(family, child, reusable?)
    end
  end

  @doc "Stop the worker registered under `key` if running."
  @spec stop_worker(module(), term()) :: :ok | {:error, :not_found}
  def stop_worker(family, key) do
    case Registry.lookup(registry_name(family), key) do
      [{pid, _}] -> DynamicSupervisor.terminate_child(dyn_sup_name(family), pid)
      [] -> :ok
    end
  end

  # `{:ok, pid}` is a worker this call started; `{:already_started, pid}` is an
  # existing one, and an existing one answers only if it passes the same check
  # the lookup arm applies — otherwise the caller is handed the worker
  # `reusable?` has already refused.
  defp start_worker(family, child, reusable?) do
    case DynamicSupervisor.start_child(dyn_sup_name(family), child) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> gate(pid, reusable?)
      {:error, _} = err -> err
    end
  end

  # The one place an existing worker is asked whether it may be answered.
  defp gate(pid, reusable?) do
    if reusable?.(pid), do: {:ok, pid}, else: {:error, {:not_reusable, pid}}
  end

  # Terminate, then start: nothing is waited for in between. `terminate_child/2`
  # monitors the child and returns only after its `:DOWN`, and `Registry`
  # deletes an entry whose pid is dead when the fresh worker registers over it —
  # so a stale entry can neither delay the start nor be handed back as
  # `{:already_started, dead_pid}`. A worker the terminate did not reach — it
  # is not this family's child — survives registered, and `start_worker/3`
  # refuses it rather than answering the pid this arm was reached to replace.
  defp reap_and_start(family, pid, child, reusable?) do
    _ = DynamicSupervisor.terminate_child(dyn_sup_name(family), pid)
    start_worker(family, child, reusable?)
  end
end
