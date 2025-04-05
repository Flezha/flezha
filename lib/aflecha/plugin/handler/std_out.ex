defmodule Aflecha.Plugin.Handler.StdOut do
  alias Aflecha.Plugin.PipelineBinding
  alias Aflecha.Plugin.PipelinePlugin
  alias Aflecha.Plugin.PluginBase
  alias Aflecha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys PipelinePlugin.default_keys()
  defstruct @enforce_keys

  @impl true
  def plugin_id(), do: "pipeline_stdout"

  defimpl PluginProtocol, for: Aflecha.Plugin.Handler.StdOut do
    use PipelinePlugin
  end

  defimpl PipelineBinding, for: Aflecha.Plugin.Handler.StdOut do
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
