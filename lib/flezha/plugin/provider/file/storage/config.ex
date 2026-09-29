defmodule Flezha.Plugin.Provider.File.Storage.Config do
  @moduledoc """
  Which `Flezha.Plugin.Provider.File.Storage` the configured root, and a path
  resolved under it, live in.

  Separate from the contract on purpose, as `Flezha.State.DurabilityTier.Backend.Config`
  is: a behaviour that names its implementations is a dependency cycle with
  every module implementing it.
  """

  alias Flezha.Config
  alias Flezha.Plugin.Provider.File.Storage.{Local, S3}

  @doc "The storage a root or a resolved path lives in: S3 for an `s3://` URL, else a folder."
  @spec of(binary()) :: module()
  def of(path), do: if(S3.url?(path), do: S3, else: Local)

  @doc "A plugin's `path` resolved inside `FLEZHA_FILE_PLUGIN_ROOT` by the storage the root names."
  @spec resolve_path(binary()) :: {:ok, binary()} | {:error, term()}
  def resolve_path(path) do
    root = Config.get(:file_plugin_root)
    of(root).resolve(path, root)
  end

  @doc "`FLEZHA_FILE_PLUGIN_ROOT` as printed: a folder as an absolute path, an S3 root as given."
  @spec shown_root() :: binary()
  def shown_root() do
    root = Config.get(:file_plugin_root)
    of(root).shown(root)
  end
end
