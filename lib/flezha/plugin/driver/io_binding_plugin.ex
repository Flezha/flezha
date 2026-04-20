defmodule Flezha.Plugin.Driver.IOBindingPlugin do
  alias Flezha.Plugin.PluginBehaviour
  alias Flezha.Plugin.Provider.BeamMsgForward
  alias Flezha.Plugin.Provider.MonitoredIOForward
  alias Flezha.Service.ProcessorManager

  @behaviour PluginBehaviour

  @enforce_keys [:namespace, :processor_id, :trigger, :provider]
  defstruct @enforce_keys ++
              [
                pid: nil,
                monitor_ref: nil,
                plugin_group: "0",
                position: nil
              ]

  # Providers that carry a caller-bound pid and cannot be restored after a
  # restart: the original pid is gone and reopening them would silently drop
  # data. Extend this list consciously when adding a new pid-bound provider.
  @impl true
  def recoverable?(%__MODULE__{provider: %BeamMsgForward{}}), do: false
  def recoverable?(%__MODULE__{provider: %MonitoredIOForward{}}), do: false
  def recoverable?(%__MODULE__{}), do: true

  @impl true
  def enable(plugin_id, plugin) do
    case ProcessorManager.get_processor_pid(plugin.namespace, plugin.processor_id) do
      {:ok, processor_pid} ->
        result = GenServer.call(processor_pid, {:enable_plugin, plugin_id, plugin})

        case result do
          :ok -> {:ok, {__MODULE__, {plugin.namespace, plugin.processor_id}}}
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  @impl true
  def disable(plugin_id, {namespace, processor_id}) do
    case ProcessorManager.get_processor_pid(namespace, processor_id) do
      {:ok, processor_pid} ->
        result = GenServer.call(processor_pid, {:disable_plugin, plugin_id})

        case result do
          :ok -> :ok
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  def build(string_map, provider_struct) do
    struct_fields = [:namespace, :processor_id, :trigger, :plugin_group, :position]

    params =
      Enum.reduce(struct_fields, %{}, fn field, acc ->
        field_str = Atom.to_string(field)

        case Map.get(string_map, field_str) do
          nil -> acc
          value -> Map.put(acc, field, static_convert(field, value))
        end
      end)

    # Set defaults
    params =
      params
      |> Map.put_new(:namespace, "default")
      |> Map.put_new(:plugin_group, "0")
      |> Map.put_new(:position, nil)
      |> Map.put(:provider, provider_struct)

    # Validate required fields
    missing_fields =
      Enum.filter(@enforce_keys, fn key ->
        not Map.has_key?(params, key)
      end)

    if Enum.empty?(missing_fields) do
      try do
        {:ok, struct!(__MODULE__, params)}
      rescue
        e -> {:error, "Failed to create struct: #{Exception.message(e)}"}
      end
    else
      {:error, "Missing required fields: #{inspect(missing_fields)}"}
    end
  end

  defp static_convert(:trigger, value) when is_binary(value) do
    case value do
      "on_input" -> :on_input
      "on_output" -> :on_output
      "proactive" -> :proactive
      "interval" -> :interval
      _ -> value
    end
  end

  defp static_convert(:position, value) when is_binary(value) do
    case Integer.parse(value) do
      {int_val, ""} -> int_val
      _ -> nil
    end
  end

  defp static_convert(_field, value), do: value
end
