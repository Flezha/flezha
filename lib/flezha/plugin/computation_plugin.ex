defmodule Flezha.Plugin.ComputationPlugin do
  alias Flezha.Service.ComputationManager

  defmacro __using__(_opts) do
    quote do
      def enable(settings) do
        case ComputationManager.get_computation_pid(settings.group, settings.computation_id) do
          {:ok, computation_pid} ->
            GenServer.call(computation_pid, {:add_binding, settings})

            # TODO: handle errors too.
            {:ok, "binding added"}

          err ->
            err
        end
      end

      def disable(settings) do
        case ComputationManager.get_computation_pid(settings.group, settings.computation_id) do
          {:ok, computation_pid} ->
            GenServer.call(computation_pid, {:remove_binding, settings})

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

  def required_computation_keys() do
    [:id, :group, :computation_id, :trigger]
  end

  def optional_computation_keys() do
    [plugin_group: "0"]
  end
end
