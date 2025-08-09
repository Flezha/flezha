defmodule Flezha.Plugin.Provider.StdOut do
  alias Flezha.Computation.IOBinding
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  defstruct []

  defimpl IOBinding, for: Flezha.Plugin.Provider.StdOut do
    @impl true
    def plugin_type(_), do: :io_binding

    @impl true
    def bind(binding) do
      {:ok, {binding, nil}}
    end

    @impl true
    def exec(_plugin, payload) do
      IO.puts(inspect(payload))

      {:ok, payload}
    end
  end
end
