defmodule Aflecha.IO.Generator do
  use GenServer

  defstruct interval: 1000,
            startup_payload: false,
            message: "Example message"

  defimpl Aflecha.IO.Base, for: Aflecha.IO.Generator do
    def mfa(struct), do: {Aflecha.IO.Generator, :start_link, [struct]}
  end

  def start_link(pipeline_pid, io_struct, opts) do
    GenServer.start_link(__MODULE__, {pipeline_pid, io_struct, opts}, [])
  end

  @impl true
  def init({pipeline_pid, io_struct, opts}) do
    if io_struct.startup_payload do
      send_payload(pipeline_pid, io_struct.message, opts[:handling_type])
    end

    Process.send_after(self(), :generate_data, io_struct.interval)

    {:ok,
     %{
       pipeline_pid: pipeline_pid,
       interval: io_struct.interval,
       message: io_struct.message,
       handling_type: opts[:handling_type]
     }}
  end

  @impl true
  def handle_info(:generate_data, state) do
    Process.send_after(self(), :generate_data, state[:interval])
    send_payload(state.pipeline_pid, state.message, state.handling_type)

    {:noreply, state}
  end

  defp send_payload(pipeline_pid, msg, handling_type) do
    GenServer.cast(pipeline_pid, {:io_in, msg, :text, handling_type})
  end
end
