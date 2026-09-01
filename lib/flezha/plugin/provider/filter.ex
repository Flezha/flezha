defmodule Flezha.Plugin.Provider.Filter do
  @moduledoc """
  First-party `:on_input` / `:on_output` transform subscriber that filters
  the payload by regex.

  Opts:
    * `:regex_pattern` (string, required)
    * `:inverted` (boolean, default false)
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  # `:inverted` is total through `parse_bool/2`; no option needs the injected
  # refusal.
  @converted_opts []

  @enforce_keys [:regex_pattern]
  defstruct @enforce_keys ++ [inverted: false]

  @impl true
  def plugin_type(), do: :io_binding

  defp convert_value(:inverted, value) when is_binary(value) do
    case String.downcase(value) do
      "true" -> {:ok, true}
      "false" -> {:ok, false}
      _ -> {:error, "Invalid value for inverted: #{value}"}
    end
  end

  defp convert_value(:inverted, value), do: ProviderBehaviour.parse_bool(value, :inverted)

  def init_provider(opts) do
    pattern = Map.fetch!(opts, :regex_pattern)

    case Regex.compile(pattern) do
      {:ok, compiled} -> {:ok, Map.put(opts, :regex, compiled)}
      {:error, reason} -> {:error, {:invalid_regex, reason}}
    end
  end

  def on_input(%Ctx.IO{} = ctx, opts), do: filter(ctx, opts)
  def on_output(%Ctx.IO{} = ctx, opts), do: filter(ctx, opts)

  defp filter(%Ctx.IO{message: msg} = ctx, opts) do
    regex = Map.fetch!(opts, :regex)
    inverted = Map.get(opts, :inverted, false)

    matches? = Regex.match?(regex, msg)

    cond do
      matches? and inverted -> {:skip, :normal}
      matches? -> {:ok, ctx}
      inverted -> {:ok, ctx}
      true -> {:skip, :normal}
    end
  end
end
