defmodule Flezha.State.WriterRegistry do
  @moduledoc """
  Where a record writer registers, and how one is found.

  Every per-Cell `WalWriter` and every per-relay `InputLog` registers here
  under its own id — a 3-tuple `cell_id` for a writer, `{relay_class,
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
  Pids of every live Cell writer for `relay_class`. The 3-tuple key
  pattern excludes the class's `{class, :input_log}` registration.
  """
  @spec live_writers(String.t()) :: [pid()]
  def live_writers(relay_class) do
    Elixir.Registry.select(@registry, [
      {{{relay_class, :_, :_}, :"$1", :_}, [], [:"$1"]}
    ])
  end

  @doc "Every registered writer id — Cell writers and input logs alike, no file lock."
  @spec registered_ids() :: [tuple()]
  def registered_ids() do
    lock? = {:==, {:element, 1, :"$1"}, :cell_files}
    Elixir.Registry.select(@registry, [{{:"$1", :_, :_}, [{:not, lock?}], [:"$1"]}])
  end

  @doc """
  Run `fun` in a task holding `cell_id`'s file lock: the writer of the Cell's
  files while it has no running writer (a landing). A booting writer waits for
  the holder to end (`await_files/1`); a holder never waits on a writer.
  """
  @spec with_files(tuple(), (-> result)) :: result when result: term()
  def with_files(cell_id, fun) do
    Task.await(Task.async(fn -> hold_files(cell_id, fun) end), :infinity)
  end

  defp hold_files(cell_id, fun) do
    case Elixir.Registry.register(@registry, {:cell_files, cell_id}, nil) do
      {:ok, _owner} ->
        fun.()

      {:error, {:already_registered, _holder}} ->
        :ok = await_files(cell_id)
        hold_files(cell_id, fun)
    end
  end

  @doc "Return once no process holds `cell_id`'s file lock."
  @spec await_files(tuple()) :: :ok
  def await_files(cell_id) do
    with {:ok, holder} <- lookup({:cell_files, cell_id}) do
      monitor = Process.monitor(holder)
      receive do: ({:DOWN, ^monitor, _, _, _} -> :ok)
    end

    :ok
  end

  @doc "Every registered Cell writer as `{cell_id, pid}`, without the input logs."
  @spec cell_writers() :: [{{binary(), binary(), binary()}, pid()}]
  def cell_writers() do
    Elixir.Registry.select(@registry, [
      {{{:"$1", :"$2", :"$3"}, :"$4", :_}, [], [{{{{:"$1", :"$2", :"$3"}}, :"$4"}}]}
    ])
  end

  @doc "The `Registry` child spec, started alongside `Flezha.State.Supervisor`."
  @spec child_spec_for_application() :: Supervisor.child_spec() | {module(), term()}
  def child_spec_for_application(), do: {Elixir.Registry, keys: :unique, name: @registry}
end
