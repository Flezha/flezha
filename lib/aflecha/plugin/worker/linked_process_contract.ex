defmodule Aflecha.Plugin.Worker.LinkedProcessContract do
  defmacro __using__(_opts) do
    quote do
      @behaviour Aflecha.Plugin.Worker.LinkedProcessContract
      @behaviour Aflecha.Plugin.PluginContract
    end
  end

  @callback start_link(term()) :: {:ok, pid()} | {:error, term()}
end
