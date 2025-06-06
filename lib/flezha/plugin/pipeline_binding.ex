defprotocol Flezha.Plugin.PipelineBinding do
  @spec bind(t()) :: {:ok, t()} | {:error, String.t()}
  def bind(settings)

  @spec exec(
          t(),
          {:binary | :text, binary()}
        ) ::
          {:ok, {:binary | :text, binary()}}
          | {:error, String.t()}
          | {:skip, :normal}
  def exec(settings, payload)
end
