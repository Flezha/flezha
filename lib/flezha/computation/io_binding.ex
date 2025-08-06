defprotocol Flezha.Computation.IOBinding do
  @spec bind(t()) :: {:ok, {t(), pid() | nil}} | {:error, String.t()}
  def bind(settings)

  @spec exec(t(), {binary() | map()}) ::
          {:ok, {binary(), map()}}
          | {:error, String.t()}
          | {:skip, :normal}
  def exec(settings, payload)

  @spec pre_bind(t()) :: {:ok, t()} | {:error, String.t()}
  def pre_bind(settings)
end
