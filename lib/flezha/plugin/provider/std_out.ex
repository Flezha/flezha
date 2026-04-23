defmodule Flezha.Plugin.Provider.StdOut do
  @moduledoc """
  First-party `:on_input` / `:on_output` transform subscriber that prints
  the payload to stdout and passes it through unchanged.
  """

  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Plugin.ProviderBehaviour

  use ProviderBehaviour

  defstruct []

  @impl true
  def plugin_type(), do: :io_binding

  def on_input(%Ctx.IO{} = ctx, _opts), do: print(ctx)
  def on_output(%Ctx.IO{} = ctx, _opts), do: print(ctx)

  defp print(%Ctx.IO{message: msg, metadata: md} = ctx) do
    IO.puts(inspect({msg, md}))
    {:ok, ctx}
  end
end
