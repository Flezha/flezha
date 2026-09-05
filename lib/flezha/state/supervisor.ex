defmodule Flezha.State.Supervisor do
  @moduledoc """
  `DynamicSupervisor` for per-Entity `WalWriter` GenServers.

  Looks up an existing writer by `entity_id` via a `Registry`, or
  starts one on demand via `find_or_start/1`. The writers run
  `:transient` so a clean exit (`:normal` / `:shutdown`) doesn't trip a
  restart loop.
  """

  use DynamicSupervisor

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Process.Cspec
  alias Flezha.State.FsyncBatcher
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.WalWriter
  alias Flezha.State.WriterRegistry

  def child_spec(_arg) do
    Cspec.build(__MODULE__, {__MODULE__, :start_link, [[]]}, :supervisor_permanent)
  end

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    # The class→namespace claims, input-log watermark and class registry
    # tables live with the state subsystem: a named table dies with its owner,
    # and every one of these is read by the data plane, so no process that can
    # crash and restart (a sweeper, a writer) may own them. Path resolution
    # degrades to the on-disk layout when the claims table is absent (unit
    # tests that use `Flezha.State.Paths` without this supervisor).
    Paths.ensure_claims_table()
    InputLog.ensure_watermark_table()
    ClassRegistry.ensure_table()
    # Every per-Entity writer and input log on the node hangs here, and they
    # fail independently — a bad payload, a disk fault, a killed run. The
    # DynamicSupervisor default (3 restarts in 5 s) is a budget for ONE
    # supervised subsystem, not for a fleet: four unrelated writers crashing in
    # the same five seconds would take this supervisor down, and with it every
    # other operative's writer AND the three ETS tables owned above, which the
    # data plane reads. The intensity is therefore sized for independent
    # children; a genuine restart storm still trips it, just not a burst.
    DynamicSupervisor.init([strategy: :one_for_one] ++ Cspec.intensity())
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

    FsyncBatcher.forget(entity_id)
    Paths.delete_files(entity_id)

    # A deleted Entity must stop holding the operative's input-log watermark.
    {operative_class, _, _} = entity_id
    InputLog.forget_entity(operative_class, entity_id)

    {:ok, %{entity_id: entity_id, status: :ok}}
  end

  @doc "Look up a running writer pid without starting one."
  @spec lookup(tuple()) :: {:ok, pid()} | :error
  defdelegate lookup(entity_id), to: WriterRegistry

  @doc "Pids of every live Entity writer for `operative_class`."
  @spec live_writers(String.t()) :: [pid()]
  defdelegate live_writers(operative_class), to: WriterRegistry

  @doc """
  Stop every live writer of `operative_class`; the files stay. What a stopped
  operative owes whether its state is deleted or retained for inspection.
  """
  @spec close_writers(String.t()) :: :ok
  def close_writers(operative_class) do
    Enum.each(live_writers(operative_class), &WalWriter.close/1)
  end

  @doc "Every registered writer id — Entity writers and input logs alike."
  @spec registered_ids() :: [tuple()]
  defdelegate registered_ids(), to: WriterRegistry

  @doc "Process-registry name a writer registers itself under."
  defdelegate registry_name(), to: WriterRegistry, as: :name

  @doc """
  Child specs to add to `Flezha.Application` — the underlying `Registry`
  plus this `DynamicSupervisor`.
  """
  def application_children() do
    [
      WriterRegistry.child_spec_for_application(),
      __MODULE__
    ]
  end
end
