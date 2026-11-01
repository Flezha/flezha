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

  @doc """
  A plugin's `path` resolved inside its namespace's folder under
  `FLEZHA_FILE_PLUGIN_ROOT`, by the storage the root names. A path written out
  in full that lies under the root but outside that folder, as one stored
  before namespaces had their own folders does, is refused naming where the
  same file is now.
  """
  @spec resolve_path(binary(), atom() | binary()) :: {:ok, binary()} | {:error, term()}
  def resolve_path(path, namespace) do
    root = Config.get(:file_plugin_root)
    storage = of(root)

    with {:ok, folder} <- storage.namespace_root(root, to_string(namespace)) do
      case storage.resolve(path, folder) do
        {:error, {:file_path_outside_root, ^path, ^folder}} = refused ->
          moved(refused, path, storage.shown(root), folder)

        resolved ->
          resolved
      end
    end
  end

  defp moved(refused, path, root, folder) do
    case String.split(path, root <> "/", parts: 2) do
      ["", rest] -> {:error, {:file_path_outside_namespace, path, folder, folder <> "/" <> rest}}
      _elsewhere -> refused
    end
  end

  @doc "`FLEZHA_FILE_PLUGIN_ROOT` as printed: a folder as an absolute path, an S3 root as given."
  @spec shown_root() :: binary()
  def shown_root() do
    root = Config.get(:file_plugin_root)
    of(root).shown(root)
  end
end
