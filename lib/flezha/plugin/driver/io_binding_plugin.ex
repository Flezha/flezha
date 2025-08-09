defmodule Flezha.Plugin.Driver.IOBindingPlugin do
  alias Flezha.Plugin.PluginBehaviour
  alias Flezha.Service.ComputationManager

  @behaviour PluginBehaviour

  @enforce_keys [:group, :computation_id, :trigger, :provider]
  defstruct @enforce_keys ++
              [
                pid: nil,
                plugin_group: "0",
                weight: 0
              ]

  @impl true
  def enable(plugin_id, plugin) do
    case ComputationManager.get_computation_pid(plugin.group, plugin.computation_id) do
      {:ok, computation_pid} ->
        result = GenServer.call(computation_pid, {:enable_plugin, plugin_id, plugin})

        case result do
          :ok -> {:ok, {plugin.computation_id}}
          {:error, reason} -> {:error, reason}
        end

      err ->
        err
    end
  end

  alias Flezha.Plugin.PluginProtocol
  alias Flezha.Computation.IOBinding

  def build(string_map, provider_struct) do
    struct_fields = [:group, :computation_id, :trigger, :plugin_group, :weight]

    params =
      Enum.reduce(struct_fields, %{}, fn field, acc ->
        field_str = Atom.to_string(field)

        case Map.get(string_map, field_str) do
          nil -> acc
          value -> Map.put(acc, field, convert_value(field, value))
        end
      end)

    # Set defaults
    params =
      params
      |> Map.put_new(:group, "default")
      |> Map.put_new(:plugin_group, "0")
      |> Map.put_new(:weight, 0)
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

  defp convert_value(:trigger, value) when is_binary(value) do
    case value do
      "on_input" -> :on_input
      "on_output" -> :on_output
      "proactive" -> :proactive
      "interval" -> :interval
      _ -> value
    end
  end

  defp convert_value(:weight, value) when is_binary(value) do
    case Integer.parse(value) do
      {int_val, ""} -> int_val
      _ -> 0
    end
  end

  defp convert_value(_field, value), do: value

  defimpl PluginProtocol, for: Flezha.Plugin.Type.IOBindingPlugin do
    alias Flezha.Plugin.Type.IOBindingPlugin

    def disable(plugin) do
      case ComputationManager.get_computation_pid(plugin.group, plugin.computation_id) do
        {:ok, computation_pid} ->
          GenServer.call(computation_pid, {:disable_plugin, plugin.instance_id})

          # TODO: handle errors too.
          {:ok, "plugin disabled"}

        err ->
          err
      end
    end
  end
end
