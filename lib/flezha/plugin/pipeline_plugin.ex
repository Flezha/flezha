defmodule Flezha.Plugin.PipelinePlugin do
  alias Flezha.Service.PipelineManager

  defmacro __using__(_opts) do
    quote do
      def enable(settings) do
        case PipelineManager.get_pipeline_pid(settings.group, settings.pipeline_id) do
          {:ok, pipeline_pid} ->
            GenServer.call(pipeline_pid, {:add_binding, settings})

            # TODO: handle errors too.
            {:ok, "binding added"}

          err ->
            err
        end
      end

      def disable(settings) do
        case PipelineManager.get_pipeline_pid(settings.group, settings.pipeline_id) do
          {:ok, pipeline_pid} ->
            GenServer.call(pipeline_pid, {:remove_binding, settings})

            # TODO: handle errors too.
            {:ok, "binding removed"}

          err ->
            err
        end
      end

      def pre_enable(settings) do
        # This function can be overridden by the plugin to perform any pre-enable actions.
        {:ok, settings}
      end

      defoverridable enable: 1, disable: 1, pre_enable: 1
    end
  end

  def default_keys() do
    [:group, :pipeline_id, :trigger]
  end
end
