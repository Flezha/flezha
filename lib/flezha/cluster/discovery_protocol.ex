defprotocol Flezha.Cluster.DiscoveryProtocol do
  @spec get_nodes(__MODULE__.t()) :: {:ok, __MODULE__.t(), [node()]}
  def get_nodes(_struct)

  @spec validate_settings(__MODULE__.t()) :: {:ok, __MODULE__.t()} | {:error, String.t()}
  def validate_settings(_struct)
end
