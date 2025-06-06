defprotocol Flezha.Plugin.PluginProtocol do
  @spec enable(t()) :: {:ok, t()} | {:error, String.t()}
  def enable(settings)

  @spec disable(t()) :: {:ok, t()} | {:error, String.t()}
  def disable(settings)
end
