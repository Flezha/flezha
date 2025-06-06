defmodule Flezha.Plugin.Provider.StdOut do
  alias Flezha.Plugin.PipelineBinding
  alias Flezha.Plugin.PipelinePlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys PipelinePlugin.default_keys()
  defstruct @enforce_keys

  @impl true
  def plugin_id(), do: "pipeline_stdout"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.StdOut do
    use PipelinePlugin
  end

  defimpl PipelineBinding, for: Flezha.Plugin.Provider.StdOut do
    @impl true
    def bind(settings) do
      {:ok, settings}
    end

    @impl true
    def exec(_settings, payload) do
      IO.puts(inspect(payload))

      {:ok, payload}
    end
  end
end
