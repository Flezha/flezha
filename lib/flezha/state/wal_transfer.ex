defmodule Flezha.State.WalTransfer do
  @moduledoc """
  Cross-controller pull of one Segment's durable files.

  A controller that loses every runner drains its operatives to a peer
  (`ProcessHub.Service.Migration.drain/2`). The handover carries the SDK's
  in-memory state — but it sources that state from the **runner**, and the
  runner is dead by construction on that path, so the handover arrives empty.
  The state that survives is on the source controller's disk, and nothing else
  can move it: `state_root` is node-local and MUST NOT be assumed shared.

  This module is that mover. The destination pulls; the source only answers
  reads, so a source that dies mid-transfer fails the pull instead of stranding
  a half-written file:

    * `manifest/1` — runs on the source, lists the Segment's existing files
      with sizes (`Flezha.State.Registry.segment_files/1` is the one definition
      of that set).
    * `read_chunk/3` — runs on the source, returns one bounded slice.
    * `pull_segment/3` — runs on the destination, loops the two above per file,
      writing to a `.transferring` sibling that is fsynced and atomically
      renamed. A failure removes the artefact and leaves the destination cold,
      never half-restored.

  One-shot per Segment per migration. This is NOT ongoing replication — a
  controller that dies with its disk takes its state with it.
  """

  alias Flezha.State.Registry

  @chunk_bytes 64 * 1024
  @call_timeout_ms 30_000

  @type segment_id :: {binary(), binary(), binary()}

  # ---------- source side (runs via :erpc on the node that holds the files) ----------

  @doc """
  Segment ids this node holds durable files for under `operative_class`. The
  destination cannot enumerate them locally — it has nothing on disk yet.
  """
  @spec segments(binary()) :: [segment_id()]
  def segments(operative_class), do: Registry.segments_of(operative_class)

  @doc "Basenames of the files `segment_id` actually has on this node."
  @spec manifest(segment_id()) :: [binary()]
  def manifest({_, _, _} = segment_id) do
    segment_id
    |> Registry.segment_files()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.basename/1)
  end

  @doc """
  One slice of `name` under `segment_id`. `name` arrives over the wire, so it is
  resolved through `Registry.segment_file_path/2` — never joined onto the state
  directory, which would let a peer read arbitrary paths off this node.
  """
  @spec read_chunk(segment_id(), binary(), non_neg_integer()) ::
          {:ok, binary()} | :eof | {:error, term()}
  def read_chunk({_, _, _} = segment_id, name, offset)
      when is_binary(name) and is_integer(offset) and offset >= 0 do
    case Registry.segment_file_path(segment_id, name) do
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
  Pull every Segment of `operative_class` from `source_node`. Returns the
  number of Segments whose files landed; `0` means the source held nothing and
  a cold start is correct. A per-Segment failure is logged via telemetry and
  skipped — one unreadable Segment must not cost the operative the rest.
  """
  @spec pull_operative(node(), binary(), keyword()) :: non_neg_integer()
  def pull_operative(source_node, operative_class, opts \\ [])

  def pull_operative(source_node, _operative_class, _opts) when source_node == node(), do: 0

  def pull_operative(source_node, operative_class, opts) do
    timeout = Keyword.get(opts, :timeout, @call_timeout_ms)

    case call(source_node, :segments, [operative_class], timeout) do
      {:ok, segment_ids} ->
        Enum.count(segment_ids, &landed?(pull_segment(source_node, &1, opts)))

      {:error, reason} ->
        emit(:wal_pull_failed, %{operative_class: operative_class, reason: reason})
        0
    end
  end

  defp landed?({:ok, files}), do: files > 0
  defp landed?(_), do: false

  @doc """
  Pull every file of `segment_id` from `source_node` into this node's state
  tree. `{:ok, 0}` means the source held nothing — a cold start is correct.
  """
  @spec pull_segment(node(), segment_id(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def pull_segment(source_node, segment_id, opts \\ [])

  def pull_segment(source_node, _segment_id, _opts) when source_node == node(),
    do: {:ok, 0}

  def pull_segment(source_node, {_, _, _} = segment_id, opts) do
    timeout = Keyword.get(opts, :timeout, @call_timeout_ms)

    case call(source_node, :manifest, [segment_id], timeout) do
      {:ok, []} ->
        {:ok, 0}

      {:ok, entries} ->
        pull_all(source_node, segment_id, entries, timeout)

      {:error, _} = err ->
        emit(:wal_pull_failed, %{segment_id: segment_id, reason: elem(err, 1)})
        err
    end
  end

  defp pull_all(source_node, segment_id, names, timeout) do
    Enum.reduce_while(names, {:ok, 0}, fn name, {:ok, done} ->
      case pull_file(source_node, segment_id, name, timeout) do
        :ok ->
          {:cont, {:ok, done + 1}}

        {:error, reason} = err ->
          emit(:wal_pull_failed, %{segment_id: segment_id, file: name, reason: reason})
          {:halt, err}
      end
    end)
    |> case do
      {:ok, count} = ok ->
        emit(:wal_pulled, %{segment_id: segment_id, files: count, source_node: source_node})
        ok

      err ->
        err
    end
  end

  defp pull_file(source_node, {_, _, _} = segment_id, name, timeout) do
    dest = Registry.segment_file_path(segment_id, name)

    if dest == nil do
      {:error, {:unknown_file, name}}
    else
      transferring = dest <> ".transferring"
      File.mkdir_p!(Path.dirname(dest))

      case :file.open(transferring, [:raw, :write, :binary]) do
        {:ok, fd} ->
          copy_then_commit(source_node, segment_id, name, fd, transferring, dest, timeout)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp copy_then_commit(source_node, segment_id, name, fd, transferring, dest, timeout) do
    result =
      with :ok <- copy_chunks(source_node, segment_id, name, fd, 0, timeout),
           :ok <- :file.datasync(fd) do
        :ok
      end

    _ = :file.close(fd)

    case result do
      :ok ->
        case File.rename(transferring, dest) do
          :ok -> :ok
          {:error, reason} -> discard(transferring, reason)
        end

      {:error, reason} ->
        discard(transferring, reason)
    end
  end

  defp discard(transferring, reason) do
    _ = File.rm(transferring)
    {:error, reason}
  end

  defp copy_chunks(source_node, segment_id, name, fd, offset, timeout) do
    case call(source_node, :read_chunk, [segment_id, name, offset], timeout) do
      {:ok, :eof} ->
        :ok

      {:ok, {:ok, data}} ->
        case :file.write(fd, data) do
          :ok -> copy_chunks(source_node, segment_id, name, fd, offset + byte_size(data), timeout)
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
