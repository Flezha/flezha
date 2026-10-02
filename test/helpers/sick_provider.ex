defmodule FlezhaTest.SickProvider do
  @moduledoc """
  A plugin-health test double: an operator plugin whose health verdict a test
  sets and whose activations it counts, through `:persistent_term`.
  """
  use Flezha.Plugin.ProviderBehaviour

  @impl true
  def on_output(_ctx, _opts), do: :ok

  @impl true
  def activate(_opts) do
    :persistent_term.put({__MODULE__, :activations}, activations() + 1)
    :ok
  end

  @impl true
  def health(_opts), do: :persistent_term.get({__MODULE__, :health}, :healthy)

  def set_health(verdict), do: :persistent_term.put({__MODULE__, :health}, verdict)
  def activations(), do: :persistent_term.get({__MODULE__, :activations}, 0)

  def reset() do
    :persistent_term.put({__MODULE__, :health}, :healthy)
    :persistent_term.put({__MODULE__, :activations}, 0)
  end
end
