defmodule Flezha.State.Supervisor do
  @moduledoc """
  `DynamicSupervisor` for per-Cell `WalWriter` GenServers.

  Looks up an existing writer by `cell_id` via a `Registry`, or
  starts one on demand via `find_or_start/1`. The writers run
  `:transient` so a clean exit (`:normal` / `:shutdown`) doesn't trip a
  restart loop.
  """

  use DynamicSupervisor

  alias Flezha.Relay.ClassRegistry
  alias Flezha.Process.Cspec
  alias Flezha.State.FsyncBatcher
  alias Flezha.State.InputLog
  alias Flezha.State.Metrics
  alias Flezha.State.Paths
  alias Flezha.State.WalWriter
  alias Flezha.State.WalWriter.RemoteGate
  alias Flezha.State.WriterRegistry

  def child_spec(_arg) do
    Cspec.build(__MODULE__, {__MODULE__, :start_link, [[]]}, :supervisor_permanent)
  end

  def start_link(_opts) do
    DynamicSupervisor.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    # Every per-Cell writer and input log on the node hangs here, and they
    # fail independently — a bad payload, a disk fault, a killed run. The
    # DynamicSupervisor default (3 restarts in 5 s) is a budget for ONE
    # supervised subsystem, not for a fleet: four unrelated writers crashing in
    # the same five seconds would take this supervisor down, and with it every
    # other relay's writer. The intensity is therefore sized for independent
    # children; a genuine restart storm still trips it, just not a burst.
    DynamicSupervisor.init([strategy: :one_for_one] ++ Cspec.intensity())
  end

  @doc """
  Create the class→namespace claims, input-log watermark, class registry and
  owed-credit tables, owned by the caller. The data plane reads every one of
  them, so their owner must outlive every writer and this supervisor too: the
  application creates them at boot. Idempotent.
  """
  @spec ensure_tables() :: :ok
  def ensure_tables() do
    :ok = Paths.ensure_claims_table()
    :ok = InputLog.ensure_watermark_table()
    :ok = ClassRegistry.ensure_table()
    RemoteGate.ensure_owed_table()
  end

  @doc """
  Returns the pid of the writer for `cell_id`, starting one if it is
  not yet running. Safe to call from many processes concurrently. A
  3-tuple id resolves a per-Cell `WalWriter`; the 2-tuple
  `{relay_class, :input_log}` resolves the relay's durable
  `InputLog` (same registry, so the shared `FsyncBatcher` drains both).
  """
  @spec find_or_start(tuple()) :: {:ok, pid()} | {:error, term()}
  def find_or_start(cell_id) do
    case lookup(cell_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        spec =
          case cell_id do
            {relay_class, :input_log} -> {Flezha.State.InputLog.Server, relay_class}
            _ -> {WalWriter, cell_id}
          end

        case DynamicSupervisor.start_child(__MODULE__, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = err -> err
        end
    end
  end

  @doc """
  Hard-delete a Cell's persisted state: stop its `WalWriter` (if
  running), drop any pending `FsyncBatcher` dirty mark, and remove the
  on-disk files (`Flezha.State.Paths.delete_durable/1`). Idempotent — deleting a
  never-seen / already-gone cell_id succeeds (`:ok`), which is what the SDK
  reaper's fire-and-forget send and the `flz state delete` CLI both rely on.
  """
  @spec delete_cell(tuple()) :: {:ok, %{cell_id: tuple(), status: :ok}}
  def delete_cell(cell_id) do
    case lookup(cell_id) do
      {:ok, pid} -> WalWriter.close(pid)
      :error -> :ok
    end

    forget_cell(cell_id)
    Paths.delete_durable({:cell, cell_id})
    {:ok, %{cell_id: cell_id, status: :ok}}
  end

  @doc """
  Delete `relay_class`'s state directory (`Flezha.State.Paths.delete_class_dir/1`)
  and forget every Cell of it this node holds in memory, its writers already
  closed.
  """
  @spec delete_class(String.t()) :: :ok
  def delete_class(relay_class) do
    Enum.each(Metrics.cells_of(relay_class), &forget_cell/1)
    Paths.delete_class_dir(relay_class)
  end

  # What a deleted Cell leaves in memory: its owed remote ships, its fsync
  # mark, its counters (which list it as a Cell with persisted state) and its
  # hold on the relay's input-log watermark.
  defp forget_cell({relay_class, _, _} = cell_id) do
    RemoteGate.release_owed(cell_id)
    FsyncBatcher.forget(cell_id)
    Metrics.drop_cell(cell_id)
    InputLog.forget_cell(relay_class, cell_id)
  end

  @doc "Look up a running writer pid without starting one."
  @spec lookup(tuple()) :: {:ok, pid()} | :error
  defdelegate lookup(cell_id), to: WriterRegistry

  @doc "Pids of every live Cell writer for `relay_class`."
  @spec live_writers(String.t()) :: [pid()]
  defdelegate live_writers(relay_class), to: WriterRegistry

  @doc """
  Stop every live writer of `relay_class` and its input log; the files
  stay. What a stopped relay owes whether its state is deleted, retained
  for inspection or kept for a rescale to move.
  """
  @spec close_writers(String.t()) :: :ok
  def close_writers(relay_class) do
    Enum.each(live_writers(relay_class), &WalWriter.close/1)

    case lookup({relay_class, :input_log}) do
      {:ok, pid} -> _ = DynamicSupervisor.terminate_child(__MODULE__, pid)
      :error -> :ok
    end

    RemoteGate.ship_owed(relay_class)
  end

  @doc "Synchronously flush every dirty writer and input log on this node."
  defdelegate flush_all(), to: FsyncBatcher

  @doc "Confirm an instance's processed prefix in its input log (`InputLog.confirm_prefix/3`)."
  defdelegate confirm_prefix(relay_class, offset, opts), to: InputLog

  @doc "Every registered writer id — Cell writers and input logs alike."
  @spec registered_ids() :: [tuple()]
  defdelegate registered_ids(), to: WriterRegistry

  @doc "Process-registry name a writer registers itself under."
  defdelegate registry_name(), to: WriterRegistry, as: :name

  @doc """
  Child specs to add to the application module — the underlying `Registry`
  plus this `DynamicSupervisor`.
  """
  def application_children() do
    [
      WriterRegistry.child_spec_for_application(),
      __MODULE__
    ]
  end
end
