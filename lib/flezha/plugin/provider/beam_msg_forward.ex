defmodule Flezha.Plugin.Provider.BeamMsgForward do
  alias Flezha.Plugin.ComputationBinding
  alias Flezha.Plugin.ComputationPlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys ComputationPlugin.required_computation_keys() ++ [:recv_pid]
  defstruct @enforce_keys ++ ComputationPlugin.optional_computation_keys()

  @impl true
  def plugin_id(), do: "computation_beam_msg"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.BeamMsgForward do
    use ComputationPlugin
  end

  defimpl ComputationBinding, for: Flezha.Plugin.Provider.BeamMsgForward do
    @impl true
    def bind(settings) do
      {:ok, settings}
    end

    @impl true
    def exec(settings, {message, _metadata} = payload) do
      case Process.send(settings.recv_pid, {:message_forward, message}, []) do
        :ok -> {:ok, payload}
        _ -> {:error, :send_failed}
      end
    end
  end
end
