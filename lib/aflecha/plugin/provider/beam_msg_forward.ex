defmodule Aflecha.Plugin.Provider.BeamMsgForward do
  alias Aflecha.Plugin.PipelineBinding
  alias Aflecha.Plugin.PipelinePlugin
  alias Aflecha.Plugin.PluginBase
  alias Aflecha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys PipelinePlugin.default_keys() ++ [:recv_pid]
  defstruct @enforce_keys

  @impl true
  def plugin_id(), do: "pipeline_beam_msg"

  defimpl PluginProtocol, for: Aflecha.Plugin.Provider.BeamMsgForward do
    use PipelinePlugin
  end

  defimpl PipelineBinding, for: Aflecha.Plugin.Provider.BeamMsgForward do
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
