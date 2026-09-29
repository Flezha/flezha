defmodule Flezha.Plugin.Provider.File.Storage.Local do
  @moduledoc """
  A file plugin's files in a folder on this controller: the root folder every
  path resolves inside with its symbolic links followed, and the files the
  workers read and write under it.

  A reader's identity is the inode. A sink's handle is its descriptor, the
  inode it was opened on and a cut a failed batch still owes: a batch is
  written in one call and data-synced, and one that fails is cut back to the
  byte it began at, on its own descriptor when that still works, else on the
  next open of the same file.

  A file this storage creates is made `0600`, one that already exists keeps its
  mode, and a path that is a symbolic link when it is opened is refused.
  """

  @behaviour Flezha.Plugin.Provider.File.Storage

  alias Flezha.Config
  alias Flezha.State.{Durable, Paths}

  # Symbolic links followed while resolving one path, the bound Linux applies.
  @max_link_hops 40

  # ---------- the root ----------

  @doc """
  Resolve `path` inside the folder `root`: expanded against the root, so a
  relative path names a file under it and an absolute path is taken as written,
  then walked component by component with every symbolic link followed.

  The file need not exist, but its parent directory must. The answer is the
  resolved absolute path, or a refusal naming the root the path leaves, or the
  directory that is missing.
  """
  @impl true
  def resolve(path, root) do
    # Only the default folder is created. A configured one that is missing may be
    # a share that is not mounted, and writing beside it would go unnoticed.
    if root == Config.default(:file_plugin_root), do: File.mkdir_p(root)

    root = Path.expand(root)
    target = Path.expand(path, root)

    with {:ok, real_root} <- real_root(root),
         {:ok, resolved} <- resolve_target(target, path, root, real_root) do
      if inside?(resolved, real_root), do: {:ok, resolved}, else: refuse_outside(path, root)
    end
  end

  @doc """
  Under `commit: rename` the path is the directory the parts go into, made with
  mode `0700` when it does not exist, so a path that names anything else is
  refused to the user who typed it.
  """
  @impl true
  def prepare_sink(%{commit: "rename", path: dir} = opts) do
    case File.lstat(dir) do
      {:ok, %File.Stat{type: :directory}} ->
        {:ok, opts}

      {:ok, %File.Stat{}} ->
        {:error, {:file_path_not_directory, dir}}

      {:error, :enoent} ->
        with :ok <- File.mkdir(dir), :ok <- File.chmod(dir, 0o700) do
          {:ok, opts}
        else
          {:error, reason} ->
            {:error, {:file_directory_unmakeable, dir, reason}}
        end

      {:error, reason} ->
        {:error, {:file_path_unreadable, dir, reason}}
    end
  end

  def prepare_sink(opts), do: {:ok, opts}

  @impl true
  def shown(root), do: Path.expand(root)

  defp real_root(root) do
    case walk(root) do
      {:ok, real} -> if File.dir?(real), do: {:ok, real}, else: refuse_root(root)
      _missing_or_unreadable -> refuse_root(root)
    end
  end

  defp resolve_target(target, path, root, real_root) do
    case walk(target) do
      {:ok, resolved} ->
        {:ok, resolved}

      {:missing, parent, [name]} ->
        {:ok, Path.join(parent, name)}

      {:missing, _deepest, _rest} ->
        refuse_missing_parent(target, path, root, real_root)

      {:error, :too_many_links} ->
        {:error, {:file_path_too_many_links, path, @max_link_hops}}

      {:error, reason} ->
        {:error, {:file_path_unreadable, path, reason}}
    end
  end

  # From `/`, one component at a time: a link is replaced by its target and the
  # walk starts again from the top, so a link anywhere in the path is judged by
  # where it lands. The kernel follows an intermediate link silently, which is
  # why no single `read_link_all/1` of the whole path can see one.
  defp walk(absolute), do: walk("/", tl(Path.split(absolute)), @max_link_hops)

  defp walk(resolved, [], _hops), do: {:ok, resolved}

  defp walk(resolved, [name | rest] = remaining, hops) do
    candidate = Path.join(resolved, name)

    case :file.read_link_all(candidate) do
      {:error, :einval} ->
        walk(candidate, rest, hops)

      {:ok, _link} when hops == 0 ->
        {:error, :too_many_links}

      {:ok, link} ->
        [_root | components] =
          link |> IO.chardata_to_string() |> Path.expand(resolved) |> Path.split()

        walk("/", components ++ rest, hops - 1)

      {:error, :enoent} ->
        {:missing, resolved, remaining}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp inside?(_path, "/"), do: true
  defp inside?(path, root), do: String.starts_with?(path, root <> "/")

  defp refuse_root(root), do: {:error, {:file_root_missing, root}}

  defp refuse_outside(path, root), do: {:error, {:file_path_outside_root, path, root}}

  # A directory outside the root is not named: whether it exists is nothing an
  # enable is entitled to learn.
  defp refuse_missing_parent(target, path, root, real_root) do
    if inside?(target, root) or inside?(target, real_root),
      do: {:error, {:file_directory_missing, path, Path.dirname(target)}},
      else: refuse_outside(path, root)
  end

  # ---------- reading ----------

  @impl true
  def stat(path) do
    with {:ok, %File.Stat{type: type, size: size, inode: inode}} <- File.stat(path),
         do: {:ok, %{type: type, size: size, identity: inode}}
  end

  @impl true
  def open_read(path, _inode) do
    with {:ok, fd} <- :file.open(path, [:read, :raw, :binary]), do: {:ok, %{fd: fd}}
  end

  @impl true
  def pread(%{fd: fd}, offset, bytes), do: :file.pread(fd, offset, bytes)

  @impl true
  def close(%{fd: nil} = handle), do: handle

  def close(%{fd: fd} = handle) do
    _ = :file.close(fd)
    %{handle | fd: nil}
  end

  # ---------- writing ----------

  @impl true
  def open_write(nil, path, keep?), do: open_write(%{fd: nil, inode: nil, undo: nil}, path, keep?)

  # The path still names the file this handle holds, or it is opened again: a
  # file moved or deleted under an open descriptor would take every later write
  # somewhere no reader of the path looks.
  def open_write(%{fd: fd} = handle, path, keep?) when fd != nil do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular, inode: inode}} when inode == handle.inode -> {:ok, handle}
      _moved -> handle |> close() |> open_write(path, keep?)
    end
  end

  def open_write(handle, path, keep?) do
    case open_file(path) do
      {:ok, fd} -> opened(%{handle | fd: fd}, path, keep?)
      {:error, reason} -> {:error, reason, handle}
    end
  end

  @impl true
  def append(handle, iodata) do
    case :file.position(handle.fd, :eof) do
      {:ok, mark} ->
        with :ok <- :file.write(handle.fd, iodata),
             :ok <- :file.datasync(handle.fd) do
          {:ok, handle}
        else
          {:error, reason} -> {:error, reason, undo(handle, mark)}
        end

      {:error, reason} ->
        {:error, reason, close(handle)}
    end
  end

  @impl true
  def commit_part(handle, path, part) do
    handle = close(handle)

    case File.rename(path, part) do
      :ok -> {:ok, handle}
      {:error, reason} -> {:error, reason, handle}
    end
  end

  @impl true
  def list(dir), do: File.ls(dir)

  @impl true
  def delete(path), do: File.rm(path)

  @impl true
  def read(path), do: File.read(path)

  # The install fills its sibling with the controller's umask, so the sibling is
  # made private before it is renamed into place, as every file the sink makes.
  @impl true
  def write_progress(path, _part, body) do
    in_progress = Durable.in_progress_path(path, :file_sink_progress)

    Paths.atomic_install(
      path,
      fn fd -> with :ok <- :file.write(fd, body), do: :file.change_mode(in_progress, 0o600) end,
      :file_sink_progress
    )
  end

  # ---------- the file a sink opens ----------

  defp open_file(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> :file.open(path, [:append, :raw, :binary])
      {:ok, %File.Stat{type: :symlink}} -> {:error, :symlink}
      {:ok, %File.Stat{type: type}} -> {:error, {:not_a_regular_file, type}}
      {:error, :enoent} -> create(path)
      {:error, reason} -> {:error, reason}
    end
  end

  # Erlang's open takes no creation mode, so a created file is made private
  # straight after. `:exclusive` is what keeps that from ever narrowing a file
  # somebody else created in between.
  defp create(path) do
    with {:ok, fd} <- :file.open(path, [:append, :exclusive, :raw, :binary]) do
      case :file.change_mode(path, 0o600) do
        :ok ->
          {:ok, fd}

        {:error, reason} ->
          _ = :file.close(fd)
          {:error, {:chmod, reason}}
      end
    end
  end

  # A file replaced between the open and this look is caught by the next batch's
  # `open_write/3`, which compares the path's inode with this one. A refusal
  # here closes the descriptor just opened.
  defp opened(handle, path, keep?) do
    with {:ok, %File.Stat{inode: inode}} <- File.lstat(path),
         :ok <- same_file!(handle, inode, path, keep?),
         {:ok, handle} <- finish_undo(%{handle | inode: inode}) do
      {:ok, handle}
    else
      {:error, reason} -> {:error, reason, close(handle)}
    end
  end

  # Bytes that must still be at the path live only in the file they went into,
  # so a file that is not that one has lost them, and whatever they were
  # appended for must not be confirmed. The worker stops; its next start
  # reconciles and the re-drive brings the records back.
  defp same_file!(%{inode: previous}, inode, path, true) when inode != previous,
    do: exit({:open_part_replaced, path})

  defp same_file!(_handle, _inode, _path, _keep?), do: :ok

  # A cut its own descriptor could not make is made here, on the same file. A
  # different file is not that batch's to cut.
  defp finish_undo(%{undo: {inode, mark}, inode: inode} = handle) do
    with :ok <- cut(handle.fd, mark), do: {:ok, %{handle | undo: nil}}
  end

  defp finish_undo(handle), do: {:ok, %{handle | undo: nil}}

  defp undo(handle, mark) do
    case cut(handle.fd, mark) do
      :ok -> close(handle)
      _failed -> %{close(handle) | undo: {handle.inode, mark}}
    end
  end

  # Only ever shorter: truncating to an offset past the end would pad the file.
  defp cut(fd, mark) do
    case :file.position(fd, :eof) do
      {:ok, eof} when eof <= mark -> :ok
      {:ok, _eof} -> with {:ok, _mark} <- :file.position(fd, mark), do: :file.truncate(fd)
      {:error, reason} -> {:error, reason}
    end
  end
end
