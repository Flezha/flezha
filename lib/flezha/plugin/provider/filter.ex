defmodule Flezha.Plugin.Provider.Filter do
  alias Flezha.Plugin.PipelineBinding
  alias Flezha.Plugin.PipelinePlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys PipelinePlugin.default_keys() ++
                  [
                    :regex_pattern
                  ]
  defstruct @enforce_keys ++
              [
                inverted: false
              ]

  @impl true
  def plugin_id(), do: "pipeline_io_filter"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.Filter do
    use PipelinePlugin
  end

  defimpl PipelineBinding, for: Flezha.Plugin.Provider.Filter do
    @impl true
    def bind(settings) do
      {:ok, settings}
    end

    @impl true
    def exec(settings, payload) do
      if Regex.match?(payload, settings.regex_pattern) do
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
