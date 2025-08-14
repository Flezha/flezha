defmodule Flezha.Plugin.ProviderBehaviour do
  @moduledoc """
  Behaviour for plugin providers with automatic build/1 implementation.

  Usage:
    defmodule MyProvider do
      use Flezha.Plugin.ProviderBehaviour

      @enforce_keys [:required_field]
      defstruct @enforce_keys ++ [optional_field: "default"]

      # Optionally override convert_value for custom field processing
      defp convert_value(:special_field, value) do
        # custom conversion logic
      end

      # The build/1 function is automatically generated
    end
  """

  @callback plugin_type() :: :io_binding
  @callback build(map()) :: {:ok, struct()} | {:error, String.t()}

  defmacro __using__(_opts) do
    quote do
      @behaviour Flezha.Plugin.ProviderBehaviour
      @before_compile Flezha.Plugin.ProviderBehaviour

      def build(string_map) do
        # Try to determine required fields by attempting to create struct
        enforce_keys = get_required_fields(__MODULE__)

        # Get struct fields at runtime - use a sample with dummy values for enforce_keys
        dummy_params =
          Enum.reduce(enforce_keys, %{}, fn key, acc ->
            Map.put(acc, key, "dummy")
          end)

        struct_sample = struct!(__MODULE__, dummy_params)
        struct_fields = Map.keys(struct_sample) |> Enum.reject(&(&1 == :__struct__))

        {convert_status, params_data} =
          Enum.reduce_while(struct_fields, {:ok, %{}}, fn field, {_status, params} ->
            field_str = Atom.to_string(field)

            case Map.get(string_map, field_str) do
              nil ->
                {:cont, {:ok, params}}

              value ->
                case convert_value(field, value) do
                  {:ok, converted_value} ->
                    params = Map.put(params, field, converted_value)
                    {:cont, {:ok, params}}

                  {:error, reason} ->
                    {:halt, {:error, "Invalid value for #{field}: #{reason}"}}
                end
            end
          end)

        # Validate required fields
        missing_fields =
          Enum.filter(enforce_keys, fn key ->
            not Map.has_key?(params_data, key)
          end)

        cond do
          !Enum.empty?(missing_fields) ->
            {:error, "Missing required fields: #{inspect(missing_fields)}"}

          convert_status == :error ->
            {:error, "Failed to convert fields: #{inspect(params_data)}"}

          true ->
            try do
              {:ok, struct!(__MODULE__, params_data)}
            rescue
              e -> {:error, "Failed to create struct: #{Exception.message(e)}"}
            end
        end
      end

      defp get_required_fields(module) do
        try do
          # Try to create struct with empty map - if it succeeds, no required fields
          struct!(module, %{})
          []
        rescue
          e in ArgumentError ->
            # Parse the error message to extract required field names
            message = Exception.message(e)

            case Regex.run(
                   ~r/the following keys must also be given when building struct (.+): \[(.+)\]/,
                   message
                 ) do
              [_, _struct_name, fields_str] ->
                fields_str
                |> String.split(", ")
                |> Enum.map(&String.trim/1)
                |> Enum.map(fn field_str ->
                  # Remove the colon prefix if present
                  field_str = String.replace_prefix(field_str, ":", "")
                  String.to_existing_atom(field_str)
                end)

              _ ->
                []
            end
        end
      end
    end
  end

  defmacro __before_compile__(_env) do
    quote do
      # Default convert_value catch-all clause - injected at the end
      defp convert_value(nil, value), do: {:error, "Empty value provided for field"}
      defp convert_value(_field, value), do: {:ok, value}
    end
  end
end
