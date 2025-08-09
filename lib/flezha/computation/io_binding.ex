defprotocol Flezha.Computation.IOBinding do
  @spec plugin_type(t()) :: :io_binding
  def plugin_type(settings)

  @spec bind(t()) :: {:ok, {t(), pid() | nil}} | {:error, String.t()}
  def bind(settings)

  @spec exec(t(), {binary() | map()}) ::
          {:ok, {binary(), map()}}
          | {:error, String.t()}
          | {:skip, :normal}
  def exec(settings, payload)
end
