defmodule Aflecha.Plugin.PipelinePlugin do
  alias Aflecha.Service.PipelineManager

  defmacro __using__(_opts) do
    quote do
      def enable(settings) do
        case PipelineManager.get_pipeline_pid(settings.group, settings.pipeline_id) do
          {:ok, pipeline_pid} ->
            GenServer.call(pipeline_pid, {:add_binding, settings})

          err ->
            err
        end
      end

      def disable(settings) do
        case PipelineManager.get_pipeline_pid(settings.group, settings.pipeline_id) do
          {:ok, pipeline_pid} ->
            GenServer.call(pipeline_pid, {:remove_binding, settings})

          err ->
            err
        end
      end

      defoverridable enable: 1, disable: 1
    end
  end

  def default_keys() do
    [:group, :pipeline_id, :trigger]
  end
end
