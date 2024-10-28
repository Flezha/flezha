defmodule Aflecha.IO.IOSupervisor do
  @moduledoc """
  Supervises computation IO processes.
  """

  use Supervisor

  def start_link() do
    Supervisor.start_link(__MODULE__, [])
  end

  def start_child(server, child_spec) do
    Supervisor.start_child(server, child_spec)
  end

  @impl true
  def init(_init_arg) do
    Supervisor.init([], strategy: :one_for_one)
  end

  def find_pid(server, io_id) do
    child_spec =
      Supervisor.which_children(server)
      |> Enum.find(fn {id, _, _, _} -> id == io_id end)

    case child_spec do
      nil -> {:error, "IO not found"}
      {_, pid, _, _} -> {:ok, pid}
    end
  end
end
