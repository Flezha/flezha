defmodule Aflecha.IO.InternalProcess do
  alias Aflecha.IO.IOBaseBehaviour, as: IOBaseBehaviour

  use GenServer

  @behaviour IOBaseBehaviour

  @enforce_keys [:output_pid]

  defstruct [
    :output_pid
  ]

  defimpl Aflecha.IO.Base, for: Aflecha.IO.InternalProcess do
    def mfa(struct),
      do: {Aflecha.IO.InternalProcess, :start_link, [[output_pid: struct.output_pid]]}
  end

  def start_link(pipeline_id, opts \\ []) do
    GenServer.start_link(__MODULE__, {pipeline_id, opts}, [])
  end

  @impl true
  def init({pipeline_id, opts}) do
    {:ok, %{pipeline_id: pipeline_id, output_pid: opts[:output_pid]}}
  end

  @impl true
  def handle_cast({:io_in, type, payload}, state) do
    Process.send(state.output_pid, {:io_out, type, payload}, [])

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, type, payload}, state) do
    Process.send(state.output_pid, {:io_out, type, payload}, [])

    {:noreply, state}
  end

  @impl IOBaseBehaviour
  def handle_internal_payload(_server, _payload), do: :ok
end
