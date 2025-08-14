defmodule Flezha.Plugin.Provider.StdOut do
  alias Flezha.Processor.IOBinding
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  defstruct []

  @impl true
  def plugin_type(), do: :io_binding

  defimpl IOBinding, for: Flezha.Plugin.Provider.StdOut do
    @impl true
    def bind(binding) do
      {:ok, {binding, nil}}
    end

    @impl true
    def handle_io(_plugin, payload) do
      IO.puts(inspect(payload))

      {:ok, payload}
    end
  end
end
