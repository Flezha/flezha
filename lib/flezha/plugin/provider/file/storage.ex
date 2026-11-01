defmodule Flezha.Plugin.Provider.File.Storage do
  @moduledoc """
  Where a file plugin's bytes live: a folder on this controller
  (`Storage.Local`) or a bucket prefix on an S3-compatible endpoint
  (`Storage.S3`). Each holds the rules its root imposes on a path and the
  operations the two workers perform on what the path names; the workers keep
  the queue, the roll, the reconcile and the health. `Storage.Config` says
  which one a root or a path takes.

  A target is a path the root check resolved: an absolute path or an `s3://`
  URL. A handle is the implementation's own: a descriptor, a URL and its ETag,
  or a sink's open roll.
  """

  @type target :: binary()
  @type handle :: term()
  @type stat :: %{type: atom(), size: non_neg_integer(), identity: term()}

  @doc """
  The folder, or key prefix, of `namespace` under the configured `root`: each
  namespace reads and writes only under its own. Refuses a root that cannot
  hold it, naming the root.
  """
  @callback namespace_root(root :: binary(), namespace :: binary()) ::
              {:ok, binary()} | {:error, term()}

  @doc """
  Resolve a plugin's `path` inside `root` into the one target every log line and
  verdict names, or refuse it naming what stopped it.
  """
  @callback resolve(path :: binary(), root :: binary()) :: {:ok, target()} | {:error, term()}

  @doc """
  Make ready what a sink's layout needs before its worker exists, or refuse a
  layout this storage cannot carry, naming the option.
  """
  @callback prepare_sink(opts :: map()) :: {:ok, map()} | {:error, term()}

  @doc "`root` as a user reads it."
  @callback shown(root :: binary()) :: binary()

  @doc "The type, size and identity of `target`: an inode, or an ETag."
  @callback stat(target()) :: {:ok, stat()} | {:error, term()}

  @doc "A handle reading `target` for as long as it is the one `identity` names."
  @callback open_read(target(), identity :: term()) :: {:ok, handle()} | {:error, term()}

  @doc """
  At most `bytes` bytes from `offset`, `:eof` past the end, and `:replaced` once
  the target is no longer the one the handle was opened on.
  """
  @callback pread(handle(), offset :: non_neg_integer(), bytes :: pos_integer()) ::
              {:ok, binary()} | :eof | {:error, term()}

  @doc "Give up what the handle holds, answering the handle to open again."
  @callback close(handle()) :: handle()

  @doc """
  A handle a batch can be appended to at `target`. With `keep?`, the bytes
  appended through the handle so far must still be at the target.
  """
  @callback open_write(handle() | nil, target(), keep? :: boolean()) ::
              {:ok, handle()} | {:error, term(), handle()}

  @doc "Append a batch durably, or answer the handle with the batch undone."
  @callback append(handle(), iodata()) :: {:ok, handle()} | {:error, term(), handle()}

  @doc "Commit what was appended at `target` as the part at `part`."
  @callback commit_part(handle(), target(), part :: target()) ::
              {:ok, handle()} | {:error, term(), handle()}

  @doc "The names directly under `dir`."
  @callback list(dir :: target()) :: {:ok, [binary()]} | {:error, term()}

  @callback delete(target()) :: :ok | {:error, term()}

  @doc "The whole of `target`, or `{:error, :enoent}`."
  @callback read(target()) :: {:ok, binary()} | {:error, term()}

  @doc """
  Write the progress sidecar at `target` naming `part`, or answer
  `{:progress_ahead, stored}` when the stored one names a later part.
  """
  @callback write_progress(target(), part :: non_neg_integer(), body :: binary()) ::
              :ok | {:error, term()}
end
