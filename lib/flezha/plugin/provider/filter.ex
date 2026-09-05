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

  use ProviderBehaviour,
    options: [
      regex_pattern: [type: :string, required: true, doc: "Regex the payload is matched against"],
      inverted: [type: :bool, default: false, doc: "Skip what matches instead of passing it"],
      # The compiled pattern this provider commits back onto its own opts.
      regex: [type: :any, internal: true]
    ]

  @impl true
  def init_provider(opts) do
    pattern = Map.fetch!(opts, :regex_pattern)

    case Regex.compile(pattern) do
      {:ok, compiled} -> {:ok, Map.put(opts, :regex, compiled)}
      {:error, reason} -> {:error, {:invalid_regex, reason}}
    end
  end

  @impl true
  def on_input(%Ctx.IO{} = ctx, opts), do: filter(ctx, opts)
  @impl true
  def on_output(%Ctx.IO{} = ctx, opts), do: filter(ctx, opts)

  defp filter(%Ctx.IO{message: msg} = ctx, opts) do
    regex = Map.fetch!(opts, :regex)
    inverted = Map.fetch!(opts, :inverted)

    matches? = Regex.match?(regex, msg)

    cond do
      matches? and inverted -> {:skip, :normal}
      matches? -> {:ok, ctx}
      inverted -> {:ok, ctx}
      true -> {:skip, :normal}
    end
  end
end
