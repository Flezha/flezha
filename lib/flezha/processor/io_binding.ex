defprotocol Flezha.Processor.IOBinding do
  @spec bind(t()) :: {:ok, {t(), pid() | nil}} | {:error, String.t()}
  def bind(settings)

  @spec handle_io(t(), {binary(), map()}) ::
          {:ok, {binary(), map()}}
          | {:error, String.t()}
          | {:skip, :normal}
  def handle_io(settings, payload)
end
