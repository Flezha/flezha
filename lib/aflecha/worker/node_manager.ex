defmodule Aflecha.Worker.NodeManager do
  use GenServer

  def start_link(_) do
    GenServer.start_link(__MODULE__, nil, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{worker_mapping: %{}}}
  end
end
