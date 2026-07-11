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

  Workers register themselves under
  `{:via, Registry, {registry_name(family), key}}` in their `start_link`.
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

  @doc """
  Start (or look up) the worker registered under `key`. Returns
  `{:ok, pid}` either way; a registered worker is shared while
  `reusable?.(pid)` holds, else it is reaped first so the caller gets a
  fresh, healthy one.
  """
  @spec ensure_worker(module(), term(), Supervisor.child_spec() | {module(), term()}, (pid() ->
                                                                                         boolean())) ::
          {:ok, pid()} | {:error, term()}
  def ensure_worker(family, key, child, reusable? \\ &Process.alive?/1) do
    case Registry.lookup(registry_name(family), key) do
      [{pid, _}] ->
        if reusable?.(pid), do: {:ok, pid}, else: reap_and_start(family, pid, key, child)

      [] ->
        start_worker(family, child)
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

  defp start_worker(family, child) do
    case DynamicSupervisor.start_child(dyn_sup_name(family), child) do
      {:ok, pid} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      {:error, _} = err -> err
    end
  end

  defp reap_and_start(family, pid, key, child) do
    _ = DynamicSupervisor.terminate_child(dyn_sup_name(family), pid)
    await_unregistered(family, key)
    start_worker(family, child)
  end

  # Bounded wait for the Registry to drop the reaped worker's entry so the
  # fresh start is not handed back the dead pid via `{:already_started, _}`.
  defp await_unregistered(family, key, attempts \\ 50) do
    cond do
      Registry.lookup(registry_name(family), key) == [] ->
        :ok

      attempts <= 0 ->
        :ok

      true ->
        Process.sleep(2)
        await_unregistered(family, key, attempts - 1)
    end
  end
end
