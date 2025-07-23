defmodule Flezha.Plugin.Provider.StdOut do
  alias Flezha.Plugin.ComputationBinding
  alias Flezha.Plugin.ComputationPlugin
  alias Flezha.Plugin.PluginBase
  alias Flezha.Plugin.PluginProtocol

  @behaviour PluginBase

  @enforce_keys ComputationPlugin.required_computation_keys()
  defstruct @enforce_keys ++ ComputationPlugin.optional_computation_keys()

  @impl true
  def plugin_id(), do: "computation_stdout"

  defimpl PluginProtocol, for: Flezha.Plugin.Provider.StdOut do
    use ComputationPlugin
  end

  defimpl ComputationBinding, for: Flezha.Plugin.Provider.StdOut do
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
