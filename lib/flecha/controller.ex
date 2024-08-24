defmodule Flecha.Controller do
  use GenServer

  def start_link(arg) do
    GenServer.start_link(__MODULE__, arg, name: __MODULE__)
  end

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_call({:add_computation, computation}, _from, state) do
    child_spec = %{
      id: computation.computation_id,
      start: {Flecha.Engine, :start_link, [computation]}
    }

    ProcessHub.start_child(:flecha_hub, child_spec, async_wait: true) |> ProcessHub.await()

    {:reply, :ok, state}
  end
end
