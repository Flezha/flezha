defmodule Flezha.State.WriterRegistry do
  @moduledoc """
  Where a record writer registers, and how one is found.

  Every per-Cell `WalWriter` and every per-operative `InputLog` registers here
  under its own id — a 3-tuple `cell_id` for a writer, `{operative_class,
  :input_log}` for a log — so the shared `FsyncBatcher` drains both through one
  lookup.

  Reads only, and it names nothing: finding a writer is not the same question as
  starting one, and a module that only needs to find one must not have to name
  the `DynamicSupervisor` that starts them. `Flezha.State.Supervisor` owns the
  starting half and uses this for the finding half.
  """

  @registry __MODULE__

  @doc "The process-registry name a writer registers itself under."
  @spec name() :: atom()
  def name(), do: @registry

  @doc "The pid of the writer registered as `id`, without starting one."
  @spec lookup(tuple()) :: {:ok, pid()} | :error
  def lookup(id) do
    case Elixir.Registry.lookup(@registry, id) do
      [{pid, _}] -> {:ok, pid}
      [] -> :error
    end
  end

  @doc """
  Pids of every live Cell writer for `operative_class`. The 3-tuple key
  pattern excludes the class's `{class, :input_log}` registration.
  """
  @spec live_writers(String.t()) :: [pid()]
  def live_writers(operative_class) do
    Elixir.Registry.select(@registry, [
      {{{operative_class, :_, :_}, :"$1", :_}, [], [:"$1"]}
    ])
  end

  @doc "Every registered writer id — Cell writers and input logs alike."
  @spec registered_ids() :: [tuple()]
  def registered_ids() do
    Elixir.Registry.select(@registry, [{{:"$1", :_, :_}, [], [:"$1"]}])
  end

  @doc "The `Registry` child spec, started alongside `Flezha.State.Supervisor`."
  @spec child_spec_for_application() :: Supervisor.child_spec() | {module(), term()}
  def child_spec_for_application(), do: {Elixir.Registry, keys: :unique, name: @registry}
end
