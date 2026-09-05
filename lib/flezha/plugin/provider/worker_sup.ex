defmodule Flezha.Plugin.Provider.WorkerSup do
  @moduledoc """
  The one supervision shape for every provider worker: a Registry +
  DynamicSupervisor pair per provider family, with ensure / reap / stop
  semantics.

  A family is named after the provider module (`family/1`), so each provider's
  workers get their own Registry and DynamicSupervisor and a crash-looping
  worker exhausts only its own family's restart budget. What a provider supplies
  is its keys and the opts each worker takes — `worker_specs/1` — and the
  restart value its declaration names; `Flezha.Plugin.Extension.Table` does the
  starting and the stopping, so no provider wraps this with a supervisor of its
  own.

  `ensure_worker/5` names the child: it puts `via/2` into the worker's opts, so
  a worker never has to name the supervisor that started it.
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

  @doc """
  The family a provider's workers live in. Derived from the provider module, so
  a provider declares a worker and nothing else names where it is supervised.
  """
  @spec family(module()) :: module()
  def family(provider), do: Module.concat(provider, WorkerSup)

  def registry_name(family), do: Module.concat(family, Registry)
  def dyn_sup_name(family), do: Module.concat(family, DynSup)

  @doc """
  Every key this family holds a LIVE worker under.

  A registry entry outlives its process by the moment it takes the registry's
  own monitor to fire, so an entry whose pid is dead is not a worker: nothing
  can be handed it, and `ensure_worker/5` registers over it.
  """
  @spec keys(module()) :: [term()]
  def keys(family) do
    registry_name(family)
    |> Registry.select([{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
    |> Enum.filter(fn {_key, pid} -> Process.alive?(pid) end)
    |> Enum.map(&elem(&1, 0))
  end

  @doc "The registered name a worker of `family` takes under `key`."
  @spec via(module(), term()) :: {:via, module(), {atom(), term()}}
  def via(family, key), do: {:via, Registry, {registry_name(family), key}}

  @doc """
  The worker `provider` registered under `key`, addressed by name.

  A provider's dispatch path reaches its worker through this rather than through
  a pid an option carried: the table started it, a supervisor restart replaces
  it, and a name survives both.
  """
  @spec worker(module(), term()) :: {:via, module(), {atom(), term()}}
  def worker(provider, key), do: via(family(provider), key)

  @doc """
  Start (or look up) the worker registered under `key`, naming the child
  `via(family, key)`. A registered worker is shared while `reusable?.(pid)`
  holds, else it is reaped first so the caller gets a fresh, healthy one.

  Every arm that answers an *existing* worker asks `reusable?` first, so a
  worker the check has rejected is never handed back: an existing worker the
  reap could not remove answers `{:error, {:not_reusable, pid}}` rather than
  the pid itself.
  """
  @spec ensure_worker(
          module(),
          term(),
          {module(), keyword() | map()},
          (pid() -> boolean()),
          :permanent | :transient | :temporary
        ) :: {:ok, pid()} | {:error, term()}
  def ensure_worker(
        family,
        key,
        {worker, opts},
        reusable? \\ &Process.alive?/1,
        restart \\ :temporary
      ) do
    child =
      Supervisor.child_spec({worker, named(opts, via(family, key))}, id: key, restart: restart)

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

  # Both opt shapes a worker takes carry its registered name the same way.
  defp named(opts, via) when is_list(opts), do: Keyword.put(opts, :name, via)
  defp named(opts, via) when is_map(opts), do: Map.put(opts, :name, via)

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
