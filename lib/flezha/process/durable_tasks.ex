defmodule Flezha.Process.DurableTasks do
  @moduledoc """
  Work that must finish once begun (a record, a journal entry, a file delete)
  but must not hold up the process that asks for it. It runs under one task
  supervisor that the controller tree stops before the state side, so a
  stopping controller waits for it, bounded, before the writers close.
  """

  @spec child_spec(term()) :: Supervisor.child_spec()
  def child_spec(_arg), do: Task.Supervisor.child_spec(name: __MODULE__)

  @doc """
  Run `fun` under the supervisor. When it is gone or already stopping, the
  work still runs, unsupervised, and the caller does not exit.
  """
  @spec start((-> any())) :: {:ok, pid()}
  def start(fun) when is_function(fun, 0) do
    {:ok, _pid} = Task.Supervisor.start_child(__MODULE__, fun)
  catch
    :exit, _reason -> Task.start(fun)
  end
end
