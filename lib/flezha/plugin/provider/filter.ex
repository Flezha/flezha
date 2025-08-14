defmodule Flezha.Plugin.Provider.Filter do
  alias Flezha.Processor.IOBinding
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  @enforce_keys [:regex_pattern]
  defstruct @enforce_keys ++ [inverted: false]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:inverted, value) when is_binary(value) do
    case String.downcase(value) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, "Invalid value for inverted: #{value}"}
    end
  end

  defimpl IOBinding, for: Flezha.Plugin.Provider.Filter do
    @impl true
    def bind(binding) do
      case Regex.compile(binding.regex_pattern) do
        {:ok, compiled_regex} ->
          {:ok, {%{binding | regex_pattern: compiled_regex}, nil}}

        {:error, reason} ->
          {:error, "Invalid regex pattern: #{reason}"}
      end
    end

    @impl true
    def exec(binding, {message, _metadata} = payload) do
      if Regex.match?(binding.regex_pattern, message) do
        if binding.inverted do
          {:skip, :normal}
        else
          {:ok, payload}
        end
      else
        if binding.inverted do
          {:ok, payload}
        else
          {:skip, :normal}
        end
      end
    end
  end
end
