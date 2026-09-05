defmodule Flezha.State.WalTransfer do
  @moduledoc """
  Cross-controller pull of one Entity's durable files.

  A controller that loses every runner drains its operatives to a peer. The
  handover carries the SDK's in-memory state — but it sources that from the
  **runner**, dead by construction on that path, so it arrives empty. What
  survives is on the source controller's disk, and nothing else can move it:
  `state_root` is node-local and MUST NOT be assumed shared.

  This module is that mover. The destination pulls and the source only answers
  reads, so a source that dies mid-transfer fails the pull instead of stranding
  a half-written file: `manifest/1` and `read_chunk/3` run on the source,
  `pull_entity/3` on the destination, through a fsynced-then-renamed
  `.transferring` sibling. A failure removes the artefact and leaves the
  destination cold, never half-restored. One-shot per Entity per migration;
  this is NOT ongoing replication.
  """

  alias Flezha.Config.Durations
  alias Flezha.State.Paths

  @chunk_bytes 64 * 1024

  @type entity_id :: {binary(), binary(), binary()}

  # ---------- source side (runs via :erpc on the node that holds the files) ----------

  @doc """
  Entity ids this node holds durable files for under `operative_class`. The
  destination cannot enumerate them locally — it has nothing on disk yet.
  """
  @spec entities(binary()) :: [entity_id()]
  def entities(operative_class), do: Paths.entities_of(operative_class)

  @doc "Basenames of the files `entity_id` actually has on this node."
  @spec manifest(entity_id()) :: [binary()]
  def manifest({_, _, _} = entity_id) do
    entity_id
    |> Paths.entity_files()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.basename/1)
  end

  @doc """
  One slice of `name` under `entity_id`. `name` arrives over the wire, so it is
  resolved through `Paths.entity_file_path/2` — never joined onto the state
  directory, which would let a peer read arbitrary paths off this node.
  """
  @spec read_chunk(entity_id(), binary(), non_neg_integer()) ::
          {:ok, binary()} | :eof | {:error, term()}
  def read_chunk({_, _, _} = entity_id, name, offset)
      when is_binary(name) and is_integer(offset) and offset >= 0 do
    case Paths.entity_file_path(entity_id, name) do
      nil -> {:error, :not_in_manifest}
      path -> pread(path, offset)
    end
  end

  defp pread(path, offset) do
    case :file.open(path, [:raw, :read, :binary]) do
      {:ok, fd} ->
        try do
          :file.pread(fd, offset, @chunk_bytes)
        after
          :file.close(fd)
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  # ---------- destination side ----------

  @doc """
  Pull every Entity of `operative_class` from `source_node`. Returns the
  number of Entities whose files landed; `0` means the source held nothing and
  a cold start is correct. A per-Entity failure is logged via telemetry and
  skipped — one unreadable Entity must not cost the operative the rest.
  """
  @spec pull_operative(node(), binary(), keyword()) :: non_neg_integer()
  def pull_operative(source_node, operative_class, opts \\ [])

  def pull_operative(source_node, _operative_class, _opts) when source_node == node(), do: 0

  def pull_operative(source_node, operative_class, opts) do
    timeout = Keyword.get(opts, :timeout, Durations.get(:wal_transfer_call_ms))

    case call(source_node, :entities, [operative_class], timeout) do
      {:ok, entity_ids} ->
        Enum.count(entity_ids, &landed?(pull_entity(source_node, &1, opts)))

      {:error, reason} ->
        emit(:wal_pull_failed, %{operative_class: operative_class, reason: reason})
        0
    end
  end

  defp landed?({:ok, files}), do: files > 0
  defp landed?(_), do: false

  @doc """
  Pull every file of `entity_id` from `source_node` into this node's state
  tree. `{:ok, 0}` means the source held nothing — a cold start is correct.

  `:as` writes the files under a different Entity id — the same procedure and
  key under another Operative class, which is what a partition rescale's
  cross-controller move is. The entity key is unchanged, so every basename
  resolves in the destination's layout.
  """
  @spec pull_entity(node(), entity_id(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def pull_entity(source_node, entity_id, opts \\ [])

  def pull_entity(source_node, entity_id, opts) when source_node == node() do
    if Keyword.get(opts, :as, entity_id) == entity_id, do: {:ok, 0}, else: {:error, :same_node}
  end

  def pull_entity(source_node, {_, _, _} = entity_id, opts) do
    timeout = Keyword.get(opts, :timeout, Durations.get(:wal_transfer_call_ms))
    destination = Keyword.get(opts, :as, entity_id)

    case call(source_node, :manifest, [entity_id], timeout) do
      {:ok, []} ->
        {:ok, 0}

      {:ok, entries} ->
        pull_all(source_node, entity_id, entries, timeout, destination)

      {:error, _} = err ->
        emit(:wal_pull_failed, %{entity_id: entity_id, reason: elem(err, 1)})
        err
    end
  end

  @doc """
  Move one Entity's whole durable file set to `destination`, wherever it
  currently lives.

  Same node: an atomic rename per file. Another node: the destination-pulled
  chunked copy, and the source's copy is removed only after every file has
  landed. Idempotent — a source that holds nothing is already moved, which is
  what lets a crashed rescale be re-run.
  """
  @spec move_entity(node() | nil, entity_id(), entity_id()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def move_entity(source_node, source, destination)

  def move_entity(_source_node, source, source), do: {:ok, 0}

  def move_entity(source_node, source, destination)
      when source_node in [nil] or source_node == node() do
    files = Enum.filter(Paths.entity_files(source), &File.exists?/1)

    if files == [] do
      {:ok, 0}
    else
      File.mkdir_p!(Paths.state_dir(destination))

      moved =
        Enum.count(files, fn path ->
          case Paths.entity_file_path(destination, Path.basename(path)) do
            nil -> false
            dest -> File.rename(path, dest) == :ok
          end
        end)

      emit(:wal_moved, %{entity_id: source, destination: destination, files: moved})
      {:ok, moved}
    end
  end

  def move_entity(source_node, source, destination) do
    case pull_entity(source_node, source, as: destination) do
      {:ok, 0} ->
        {:ok, 0}

      {:ok, files} ->
        # Only now: the destination copy is on disk and fsynced, so dropping
        # the source cannot lose the Entity.
        _ =
          :erpc.call(
            source_node,
            Paths,
            :delete_files,
            [source],
            Durations.get(:wal_transfer_call_ms)
          )

        emit(:wal_moved, %{entity_id: source, destination: destination, files: files})
        {:ok, files}

      err ->
        err
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp pull_all(source_node, entity_id, names, timeout, destination) do
    Enum.reduce_while(names, {:ok, 0}, fn name, {:ok, done} ->
      case pull_file(source_node, entity_id, name, timeout, destination) do
        :ok ->
          {:cont, {:ok, done + 1}}

        {:error, reason} = err ->
          emit(:wal_pull_failed, %{entity_id: entity_id, file: name, reason: reason})
          {:halt, err}
      end
    end)
    |> case do
      {:ok, count} = ok ->
        emit(:wal_pulled, %{entity_id: entity_id, files: count, source_node: source_node})
        ok

      err ->
        err
    end
  end

  # `Paths.atomic_install/2` owns the `.transferring` → fsync → rename dance;
  # this only fills the handle it is given.
  defp pull_file(source_node, {_, _, _} = entity_id, name, timeout, destination) do
    case Paths.entity_file_path(destination, name) do
      nil ->
        {:error, {:unknown_file, name}}

      dest ->
        File.mkdir_p!(Paths.state_dir(destination))

        Paths.atomic_install(
          dest,
          &copy_chunks(source_node, entity_id, name, &1, 0, timeout)
        )
    end
  end

  defp copy_chunks(source_node, entity_id, name, fd, offset, timeout) do
    case call(source_node, :read_chunk, [entity_id, name, offset], timeout) do
      {:ok, :eof} ->
        :ok

      {:ok, {:ok, data}} ->
        case :file.write(fd, data) do
          :ok -> copy_chunks(source_node, entity_id, name, fd, offset + byte_size(data), timeout)
          {:error, reason} -> {:error, reason}
        end

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, _} = err ->
        err
    end
  end

  defp call(node, fun, args, timeout) do
    {:ok, :erpc.call(node, __MODULE__, fun, args, timeout)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp emit(event, metadata) do
    :telemetry.execute([:flezha, :state, event], %{count: 1}, metadata)
  end
end
