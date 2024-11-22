defmodule Aflecha.IO.Websocket do
  alias Aflecha.IO.IOBaseBehaviour, as: IOBaseBehaviour

  @behaviour IOBaseBehaviour

  use Fresh

  @enforce_keys [:type, :msg]
  defstruct @enforce_keys

  # TODO: may need to implement this
  # defimpl Aflecha.IO.Base, for: Aflecha.IO.InternalProcess do
  #   def mfa(struct),
  #     do:
  #       {Aflecha.IO.InternalProcess, :start_link,
  #        [
  #          [output_pid: struct.output_pid, handle_as: struct.handle_as]
  #        ]}
  # end

  def start_link(uri, state, opts) do
    Fresh.start_link(uri, __MODULE__, state, opts)
  end

  @impl Fresh
  def handle_connect(_status, _headers, state) do
    {:ok, state}
  end

  @impl Fresh
  def handle_control({_, message}, state) do
    IO.puts("Received ping with content: #{message}!")

    {:ok, state}
  end

  @impl Fresh
  def handle_in({data_type, payload}, state) do
    GenServer.cast(
      {:via, PartitionSupervisor, {Aflecha.ChannelSupervisor, self()}},
      {:io_in, data_type, payload}
    )

    {:ok, state}
  end

  @impl Fresh
  def handle_info(any, state) do
    IO.puts("Received info: #{inspect(any)}")

    {:ok, state}
  end

  @impl IOBaseBehaviour
  def handle_internal_payload(server, %__MODULE__{type: type, msg: msg}) do
    Fresh.send(server, {type, msg})
  end
end
