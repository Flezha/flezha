defmodule Flezha.Plugin.Provider.BeamMsgForward do
  alias Flezha.Computation.IOBinding
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  @enforce_keys [:recv_pid]
  defstruct @enforce_keys

  defp convert_value(:recv_pid, value) when is_binary(value) do
    case String.to_existing_atom(value) do
      pid when is_atom(pid) ->
        case Process.whereis(pid) do
          nil -> {:error, "Process #{value} not found"}
          pid -> pid
        end

      _ ->
        {:error, "Invalid PID format"}
    end
  end

  defimpl IOBinding, for: Flezha.Plugin.Provider.BeamMsgForward do
    @impl true
    def plugin_type(), do: :io_binding

    @impl true
    def bind(binding) do
      {:ok, {binding, nil}}
    end

    @impl true
    def exec(binding, {message, _metadata} = payload) do
      case Process.send(binding.recv_pid, {:message_forward, message}, []) do
        :ok -> {:ok, payload}
        _ -> {:error, "send_failed"}
      end
    end
  end
end
