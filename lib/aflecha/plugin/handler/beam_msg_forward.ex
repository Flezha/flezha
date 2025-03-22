defmodule Aflecha.Plugin.Handler.BeamMsgForward do
  alias Aflecha.Plugin.PipelineBinding

  use GenServer

  @enforce_keys [:recv_pid]
  defstruct @enforce_keys

  defimpl PipelineBinding, for: Aflecha.Plugin.Handler.BeamMsgForward do
    alias Aflecha.Plugin.Handler.BeamMsgForward

    @impl true
    def bind(settings) do
      {:ok, settings}
    end

    @impl true
    def exec(settings, payload) do
      case Process.send(settings.recv_pid, payload, []) do
        :ok -> {:ok, payload}
        _ -> {:error, :send_failed}
      end
    end
  end
end
