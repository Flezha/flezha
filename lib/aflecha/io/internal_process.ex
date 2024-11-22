defmodule Aflecha.IO.InternalProcess do
  alias Aflecha.IO.IOBaseBehaviour, as: IOBaseBehaviour

  use GenServer

  @behaviour IOBaseBehaviour

  @enforce_keys [:output_pid]

  defstruct [
    :output_pid
  ]

  defimpl Aflecha.IO.Base, for: Aflecha.IO.InternalProcess do
    def mfa(io_struct),
      do: {Aflecha.IO.InternalProcess, :start_link, [io_struct]}
  end

  def start_link(pipeline_id, io_struct, opts) do
    GenServer.start_link(__MODULE__, {pipeline_id, io_struct, opts}, [])
  end

  @impl true
  def init({pipeline_id, io_struct, opts}) do
    {:ok,
     %{
       pipeline_id: pipeline_id,
       output_pid: io_struct.output_pid,
       handling_type: Keyword.get(opts, :handling_type)
     }}
  end

  @impl true
  def handle_cast({:io_in, payload, data_type}, %{handling_type: ht, output_pid: op} = state) do
    Process.send(op, {:io_in, payload, data_type, ht}, [])

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, _payload, _data_type} = msg, state) do
    Process.send(state.output_pid, msg, [])

    {:noreply, state}
  end

  @impl IOBaseBehaviour
  def handle_internal_payload(_server, _payload), do: :ok
end
