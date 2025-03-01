defmodule Aflecha.Plugin.Handler.BeamMsgForward do
  alias Aflecha.Plugin.WorkerBinding

  use GenServer

  @enforce_keys [:recv_pid]
  defstruct @enforce_keys

  defimpl WorkerBinding, for: Aflecha.Plugin.Handler.BeamMsgForward do
    alias Aflecha.Plugin.Handler.BeamMsgForward

    @impl true
    def bind(settings) do
      {:ok, settings}
    end

    @impl true
    def exec(settings, data) do
      {:ok, Process.send(settings.recv_pid, data, [])}
    end
  end
end
