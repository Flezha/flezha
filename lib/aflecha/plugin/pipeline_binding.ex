defprotocol Aflecha.Plugin.PipelineBinding do
  @spec bind(t()) :: {:ok, t()} | {:error, String.t()}
  def bind(settings)

  @spec exec(t(), term()) :: {:ok, term()} | {:error, String.t()}
  def exec(settings, payload)
end
