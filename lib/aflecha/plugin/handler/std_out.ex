defmodule Aflecha.Plugin.Handler.StdOut do
  alias Aflecha.Plugin.PipelineBinding

  defstruct []

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
