defmodule Flezha.State.Supervisor do
  @moduledoc """
  `DynamicSupervisor` for per-Partition `WalWriter` GenServers.

  Looks up an existing writer by `partition_id` via a `Registry`, or
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
  Returns the pid of the `WalWriter` for `partition_id`, starting one if
  it is not yet running. Safe to call from many processes concurrently.
  """
  @spec find_or_start(tuple()) :: {:ok, pid()} | {:error, term()}
  def find_or_start(partition_id) do
    case lookup(partition_id) do
      {:ok, pid} ->
        {:ok, pid}

      :error ->
        spec = {WalWriter, partition_id}

        case DynamicSupervisor.start_child(__MODULE__, spec) do
          {:ok, pid} -> {:ok, pid}
          {:error, {:already_started, pid}} -> {:ok, pid}
          {:error, _} = err -> err
        end
    end
  end

  @doc """
  Look up a running writer pid without starting one.
  """
  @spec lookup(tuple()) :: {:ok, pid()} | :error
  def lookup(partition_id) do
    case Elixir.Registry.lookup(@registry, partition_id) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Process-registry name used by `WalWriter` to register itself by
  `partition_id`. Started alongside this supervisor.
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
