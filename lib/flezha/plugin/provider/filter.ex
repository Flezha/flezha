defmodule Flezha.Plugin.Provider.Filter do
  alias Flezha.Plugin.ComputationBinding
  alias Flezha.Plugin.ComputationPlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys ComputationPlugin.required_computation_keys() ++
                  [
                    :regex_pattern
                  ]
  defstruct @enforce_keys ++
              [
                inverted: false
              ] ++ ComputationPlugin.optional_computation_keys()

  @impl true
  def plugin_id(), do: "computation_io_filter"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.Filter do
    use ComputationPlugin
  end

  defimpl ComputationBinding, for: Flezha.Plugin.Provider.Filter do
    @impl true
    def bind(settings) do
      case Regex.compile(settings.regex_pattern) do
        {:ok, compiled_regex} ->
          {:ok, %{settings | regex_pattern: compiled_regex}}

        {:error, reason} ->
          {:error, "Invalid regex pattern: #{reason}"}
      end
    end

    @impl true
    def exec(settings, {_type, message} = payload) do
      if Regex.match?(settings.regex_pattern, message) do
        if settings.inverted do
          {:skip, :normal}
        else
          {:ok, payload}
        end
      else
        if settings.inverted do
          {:ok, payload}
        else
          {:skip, :normal}
        end
      end
    end
  end
end
