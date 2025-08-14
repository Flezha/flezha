defmodule Flezha.Plugin.Provider.BeamMsgForward do
  alias Flezha.Processor.IOBinding
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.Parser

  use ProviderBehaviour

  @enforce_keys [:recv_pid]
  defstruct @enforce_keys

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:recv_pid, value) when is_binary(value) do
    Parser.string_to_pid(value)
  end

  defimpl IOBinding, for: Flezha.Plugin.Provider.BeamMsgForward do
    @impl true
    def bind(binding) do
      {:ok, {binding, nil}}
    end

    @impl true
    def handle_io(binding, {message, _metadata} = payload) do
      case Process.send(binding.recv_pid, {:message_forward, message}, []) do
        :ok -> {:ok, payload}
        _ -> {:error, "send_failed"}
      end
    end
  end
end
