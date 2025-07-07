defmodule Flezha.Plugin.Provider.BeamMsgForward do
  alias Flezha.Plugin.PipelineBinding
  alias Flezha.Plugin.PipelinePlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys PipelinePlugin.required_pipeline_keys() ++ [:recv_pid]
  defstruct @enforce_keys ++ PipelinePlugin.optional_pipeline_keys()

  @impl true
  def plugin_id(), do: "pipeline_beam_msg"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.BeamMsgForward do
    use PipelinePlugin
  end

  defimpl PipelineBinding, for: Flezha.Plugin.Provider.BeamMsgForward do
    @impl true
    def bind(settings) do
      {:ok, settings}
    end

    @impl true
    def exec(settings, payload) do
      case Process.send(settings.recv_pid, {:message_forward, payload}, []) do
        :ok -> {:ok, payload}
        _ -> {:error, :send_failed}
      end
    end
  end
end
