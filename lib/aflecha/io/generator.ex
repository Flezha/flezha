defmodule Aflecha.IO.Generator do
  use GenServer

  defstruct interval: 1000, startup_payload: false, message: "Example message"

  defimpl Aflecha.IO.Base, for: Aflecha.IO.Generator do
    def mfa(struct), do: {Aflecha.IO.Generator, :start_link, [struct]}
  end

  def start_link(pipeline_pid, struct) do
    GenServer.start_link(__MODULE__, {pipeline_pid, struct}, [])
  end

  @impl true
  def init({pipeline_pid, struct}) do
    if struct.startup_payload do
      send_payload(pipeline_pid, struct.message)
    end

    Process.send_after(self(), :generate_data, struct.interval)

    {:ok, %{pipeline_pid: pipeline_pid, interval: struct.interval, message: struct.message}}
  end

  @impl true
  def handle_info(:generate_data, state) do
    Process.send_after(self(), :generate_data, state[:interval])
    send_payload(state.pipeline_pid, state.message)

    {:noreply, state}
  end

  defp send_payload(pipeline_pid, msg) do
    GenServer.cast(pipeline_pid, {:io_in, :binary, msg})
  end
end
