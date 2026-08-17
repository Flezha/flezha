defmodule Flezha.State.Supervisor do
  @moduledoc """
  `DynamicSupervisor` for per-Entity `WalWriter` GenServers.

  Looks up an existing writer by `entity_id` via a `Registry`, or
  starts one on demand via `find_or_start/1`. The writers run
  `:transient` so a clean exit (`:normal` / `:shutdown`) doesn't trip a
  restart loop.
  """

  use DynamicSupervisor

  alias Flezha.State.WalWriter

  @registry Flezha.State.WriterRegistry

  def child_spec(_arg) do
    %{
      id: __MODULE__,
      type: :supervisor,
      start: {__MODULE__, :start_link, [[]]}
    }
  end

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    # The class→namespace claims table lives with the state subsystem; path
    # resolution degrades to the on-disk layout when it is absent (unit tests
    # that use `Flezha.State.Registry` without this supervisor).
    Flezha.State.Registry.ensure_claims_table()
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Returns the pid of the writer for `entity_id`, starting one if it is
  not yet running. Safe to call from many processes concurrently. A
  3-tuple id resolves a per-Entity `WalWriter`; the 2-tuple
  `{operative_class, :input_log}` resolves the operative's durable
  `InputLog` (same registry, so the shared `FsyncBatcher` drains both).
  """
  @spec find_or_start(tuple()) :: {:ok, pid()} | {:error, term()}
  def find_or_start(entity_id) do
    case lookup(entity_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        spec =
          case entity_id do
            {operative_class, :input_log} -> {Flezha.State.InputLog, operative_class}
            _ -> {WalWriter, entity_id}
          end

        case DynamicSupervisor.start_child(__MODULE__, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = err -> err
        end
    end
  end

  @doc """
  Hard-delete an Entity's persisted state: stop its `WalWriter` (if
  running), drop any pending `FsyncBatcher` dirty mark, and remove the
  on-disk `{key}.wal`, `{key}.snap.{seq}`, and `{key}.outbox.dedup`
  files. Idempotent — deleting a never-seen / already-gone entity_id
  succeeds (`:ok`), which is what the SDK reaper's fire-and-forget send
  and the `flz state delete` CLI both rely on.
  """
  @spec delete_entity(tuple()) :: {:ok, %{entity_id: tuple(), status: :ok}}
  def delete_entity(entity_id) do
    case lookup(entity_id) do
      {:ok, pid} -> WalWriter.close(pid)
      :error -> :ok
    end

    Flezha.State.FsyncBatcher.forget(entity_id)
    Flezha.State.Registry.delete_files(entity_id)

    # A deleted Entity must stop holding the operative's input-log watermark.
    {operative_class, _, _} = entity_id
    Flezha.State.InputLog.forget_entity(operative_class, entity_id)

    {:ok, %{entity_id: entity_id, status: :ok}}
  end

  @doc """
  Look up a running writer pid without starting one.
  """
  @spec lookup(tuple()) :: {:ok, pid()} | :error
  def lookup(entity_id) do
    case Elixir.Registry.lookup(@registry, entity_id) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Pids of every live Entity writer for `operative_class`. The 3-tuple key
  pattern excludes the class's `{class, :input_log}` registration.
  """
  @spec live_writers(String.t()) :: [pid()]
  def live_writers(operative_class) do
    Elixir.Registry.select(@registry, [
      {{{operative_class, :_, :_}, :"$1", :_}, [], [:"$1"]}
    ])
  end

  @doc """
  Process-registry name used by `WalWriter` to register itself by
  `entity_id`. Started alongside this supervisor.
  """
  def registry_name(), do: @registry

  @doc """
  Child specs to add to `Flezha.Application` — the underlying `Registry`
  (a process registry, distinct from `Flezha.State.Registry` which is a
  pure path-resolution module) plus this `DynamicSupervisor`.
  """
  def application_children() do
    [
      {Elixir.Registry, keys: :unique, name: @registry},
      __MODULE__
    ]
  end
end
