defmodule Aflecha.IO.InternalProcess do
  alias Aflecha.IO.IOBaseBehaviour, as: IOBaseBehaviour

  use GenServer

  @behaviour IOBaseBehaviour

  @enforce_keys [:output_pid, :handle_as]

  defstruct [
    :output_pid,
    :handle_as
  ]

  defimpl Aflecha.IO.Base, for: Aflecha.IO.InternalProcess do
    def mfa(struct),
      do:
        {Aflecha.IO.InternalProcess, :start_link,
         [
           [output_pid: struct.output_pid, handle_as: struct.handle_as]
         ]}
  end

  def start_link(pipeline_id, opts \\ []) do
    GenServer.start_link(__MODULE__, {pipeline_id, opts}, [])
  end

  @impl true
  def init({pipeline_id, opts}) do
    {:ok,
     %{
       pipeline_id: pipeline_id,
       output_pid: opts[:output_pid],
       handle_as: opts[:handle_as]
     }}
  end

  @impl true
  def handle_cast({:io_in, payload, data_type}, %{handle_as: ha, output_pid: op} = state) do
    Process.send(op, {:io_in, payload, data_type, ha}, [])

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
