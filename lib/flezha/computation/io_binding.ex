defprotocol Flezha.Computation.IOBinding do
  @spec plugin_type() :: :io_binding
  def plugin_type()

  @spec bind(t()) :: {:ok, {t(), pid() | nil}} | {:error, String.t()}
  def bind(settings)

  @spec exec(t(), {binary() | map()}) ::
          {:ok, {binary(), map()}}
          | {:error, String.t()}
          | {:skip, :normal}
  def exec(settings, payload)
end
