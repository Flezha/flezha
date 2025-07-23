defprotocol Flezha.Plugin.PluginProtocol do
  @doc TODO: "Plugins added to computations should also define order"
  @spec enable(t()) :: {:ok, t()} | {:error, String.t()}
  def enable(settings)

  @spec disable(t()) :: {:ok, t()} | {:error, String.t()}
  def disable(settings)

  @spec pre_enable(t()) :: {:ok, t()} | {:error, String.t()}
  def pre_enable(settings)
end
