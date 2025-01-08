defmodule Aflecha.IO.Handler.StdIO do
  use GenServer
  use Aflecha.IO.Contract

  defstruct []

  def start_link(computation_id, io_struct, opts) do
    GenServer.start_link(__MODULE__, {computation_id, io_struct, opts}, [])
  end

  @impl true
  def init({computation_id, _io_struct, opts}) do
    {:ok,
     %{
       computation_id: computation_id,
       handling_type: Keyword.get(opts, :handling_type)
     }}
  end

  @impl true
  def handle_cast({:io_in, _payload, _data_type}, %{handling_type: _ht} = state) do
    # TODO: Implement the handling of the input

    {:noreply, state}
  end

  @impl true
  def handle_cast({:io_out, payload, data_type}, %{handling_type: ht} = state) do
    IO.puts("Payload: #{payload}, Data Type: #{data_type}, Handling Type: #{ht}")

    {:noreply, state}
  end

  @impl Aflecha.IO.Contract
  def send_payload(_server, msg) do
    IO.puts("Sending payload to the standard output: #{inspect(msg)}")
  end
end
