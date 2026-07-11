defmodule Flezha.State.Supervisor do
  @moduledoc """
  `DynamicSupervisor` for per-Segment `WalWriter` GenServers.

  Looks up an existing writer by `segment_id` via a `Registry`, or
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
    DynamicSupervisor.init(strategy: :one_for_one)
  end

  @doc """
  Returns the pid of the writer for `segment_id`, starting one if it is
  not yet running. Safe to call from many processes concurrently. A
  3-tuple id resolves a per-Segment `WalWriter`; the 2-tuple
  `{operative_class, :input_log}` resolves the operative's durable
  `InputLog` (same registry, so the shared `FsyncBatcher` drains both).
  """
  @spec find_or_start(tuple()) :: {:ok, pid()} | {:error, term()}
  def find_or_start(segment_id) do
    case lookup(segment_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        spec =
          case segment_id do
            {operative_class, :input_log} -> {Flezha.State.InputLog, operative_class}
            _ -> {WalWriter, segment_id}
          end

        case DynamicSupervisor.start_child(__MODULE__, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = err -> err
        end
    end
  end

  @doc """
  Hard-delete a Segment's persisted state: stop its `WalWriter` (if
  running), drop any pending `FsyncBatcher` dirty mark, and remove the
  on-disk `{key}.wal`, `{key}.snap.{seq}`, and `{key}.outbox.dedup`
  files. Idempotent — deleting a never-seen / already-gone segment_id
  succeeds (`:ok`), which is what the SDK reaper's fire-and-forget send
  and the `flz state delete` CLI both rely on.
  """
  @spec delete_segment(tuple()) :: {:ok, %{segment_id: tuple(), status: :ok}}
  def delete_segment(segment_id) do
    case lookup(segment_id) do
      {:ok, pid} -> WalWriter.close(pid)
      :error -> :ok
    end

    Flezha.State.FsyncBatcher.forget(segment_id)
    Flezha.State.Registry.delete_files(segment_id)

    # A deleted Segment must stop holding the operative's input-log watermark.
    case segment_id do
      {operative_class, _, _} ->
        Flezha.State.InputLog.forget_segment(operative_class, segment_id)

      _ ->
        :ok
    end

    {:ok, %{segment_id: segment_id, status: :ok}}
  end

  @doc """
  Look up a running writer pid without starting one.
  """
  @spec lookup(tuple()) :: {:ok, pid()} | :error
  def lookup(segment_id) do
    case Elixir.Registry.lookup(@registry, segment_id) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Process-registry name used by `WalWriter` to register itself by
  `segment_id`. Started alongside this supervisor.
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
