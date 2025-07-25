defprotocol Flezha.Plugin.ComputationBinding do
  @spec bind(t()) :: {:ok, t()} | {:error, String.t()}
  def bind(settings)

  @spec exec(t(), {binary() | map()}) ::
          {:ok, {binary(), map()}}
          | {:error, String.t()}
          | {:skip, :normal}
  def exec(settings, payload)
end
