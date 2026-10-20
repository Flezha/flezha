defmodule Flezha.State.WalTransfer do
  @moduledoc """
  Cross-controller pull of a relay's durable files, one reference at a
  time: a Cell's file set (`{:cell, cell_id}`, a steps relay's journal among them
  under its reserved identity) or the instance's input log
  (`{:input_log, class}`).

  A controller that loses every runner drains its relays to a peer. The
  handover carries the SDK's in-memory state — but it sources that from the
  **runner**, dead by construction on that path, so it arrives empty. What
  survives is on the source controller's disk, and nothing else can move it:
  `state_root` is node-local and MUST NOT be assumed shared.

  This module is that mover. The destination pulls and the source only answers
  reads, so a source that dies mid-transfer fails the pull instead of stranding
  a half-written file: `manifest/1` and `read_chunk/3` run on the source,
  `pull_cell/3` on the destination, through a fsynced-then-renamed
  `.transferring` sibling. A failure removes the artefact and leaves the
  destination cold, never half-restored. One-shot per reference per migration;
  this is NOT ongoing replication.
  """

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.State.DurabilityTier.Artifact
  alias Flezha.State.{Paths, Ref}

  @chunk_bytes 64 * 1024

  @type ref :: Paths.durable_ref()

  # ---------- source side (runs via :erpc on the node that holds the files) ----------

  @doc "Basenames of the files `ref` actually has on this node."
  @spec manifest(ref()) :: [binary()]
  def manifest(ref) do
    ref
    |> Ref.files()
    |> Enum.filter(&File.regular?/1)
    |> Enum.map(&Path.basename/1)
  end

  @doc """
  One slice of `name` under `ref`. `name` arrives over the wire, so it is
  resolved through `Ref.file_path/2` — never joined onto the state
  directory, which would let a peer read arbitrary paths off this node.
  """
  @spec read_chunk(ref(), binary(), non_neg_integer()) ::
          {:ok, binary()} | :eof | {:error, term()}
  def read_chunk(ref, name, offset)
      when is_binary(name) and is_integer(offset) and offset >= 0 do
    case Ref.file_path(ref, name) do
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
  Pull every durable file of `relay_class` from `source_node`: its Cells,
  a steps relay's journal, and its input log, which travel together because a
  journal pulled without its log seeds the instance's floor in an empty
  offset space. Returns the number of references whose files landed; `0`
  means the source held nothing and a cold start is correct. A per-reference
  failure is logged via telemetry and the pull goes on with the rest;
  `Flezha.State.Adoption` keeps the copy only when it landed whole.
  """
  @spec pull_relay(node(), binary(), keyword()) :: non_neg_integer()
  def pull_relay(source_node, relay_class, opts \\ [])

  def pull_relay(source_node, _relay_class, _opts) when source_node == node(), do: 0

  def pull_relay(source_node, relay_class, opts) do
    timeout = Keyword.get(opts, :timeout, Durations.get(:wal_transfer_call_ms))

    case call(source_node, Paths, :durable_refs, [relay_class], timeout) do
      {:ok, refs} ->
        Enum.count(refs, &landed?(pull_cell(source_node, &1, opts)))

      {:error, reason} ->
        emit(:wal_pull_failed, %{relay_class: relay_class, reason: reason})
        0
    end
  end

  defp landed?({:ok, files}), do: files > 0
  defp landed?(_), do: false

  @doc """
  How new `source_node`'s copy of `relay_class` is (`Artifact.freshness_of/1`),
  `%{}`, logged, when the source cannot be asked.
  """
  @spec freshness(node(), binary()) :: %{ref() => term()}
  def freshness(source_node, relay_class) do
    timeout = Durations.get(:wal_transfer_call_ms)

    case call(source_node, Artifact, :freshness_of, [relay_class], timeout) do
      {:ok, theirs} ->
        theirs

      {:error, reason} ->
        Logger.warning(
          "[state] #{relay_class}: #{source_node} could not be asked for its copy " <>
            "(#{inspect(reason)}); the local copy is bound unchecked"
        )

        emit(:wal_pull_failed, %{relay_class: relay_class, reason: reason})
        %{}
    end
  end

  @doc "`source_node`'s `Artifact.lineage_of/1`, `%{}` when it cannot answer (a build without it)."
  @spec lineage(node(), binary()) :: %{ref() => map() | nil}
  def lineage(source_node, relay_class) do
    timeout = Durations.get(:wal_transfer_call_ms)

    case call(source_node, Artifact, :lineage_of, [relay_class], timeout) do
      {:ok, theirs} -> theirs
      {:error, _older_or_unreachable} -> %{}
    end
  end

  @doc """
  Pull every file of `ref` from `source_node` into this node's state tree.
  `{:ok, 0}` means the source held nothing — a cold start is correct.

  `:as` writes the files under a different reference of the same kind — for a
  Cell, the same tasklet and key under another Relay class, which is what
  a partition rescale's cross-controller move is. The cell key is unchanged,
  so every basename resolves in the destination's layout.
  """
  @spec pull_cell(node(), ref(), keyword()) :: {:ok, non_neg_integer()} | {:error, term()}
  def pull_cell(source_node, ref, opts \\ [])

  def pull_cell(source_node, ref, opts) when source_node == node() do
    if Keyword.get(opts, :as, ref) == ref, do: {:ok, 0}, else: {:error, :same_node}
  end

  def pull_cell(source_node, ref, opts) do
    timeout = Keyword.get(opts, :timeout, Durations.get(:wal_transfer_call_ms))
    destination = Keyword.get(opts, :as, ref)

    case call(source_node, __MODULE__, :manifest, [ref], timeout) do
      {:ok, []} ->
        {:ok, 0}

      {:ok, entries} ->
        # A pull is always a landing of another controller's copy.
        Ref.land(destination, :pulled, fn ->
          pull_all(source_node, ref, entries, timeout, destination)
        end)

      {:error, _} = err ->
        emit(:wal_pull_failed, %{ref: ref, reason: elem(err, 1)})
        err
    end
  end

  @doc """
  Move one reference's whole durable file set to `destination`, wherever it
  currently lives.

  Same node: an atomic rename per file, and nothing to do onto itself.
  Another node: the destination-pulled chunked copy, the same reference
  included, and the source's copy is removed only after every file has
  landed. Idempotent — a source that holds nothing is already moved, which is
  what lets a crashed rescale be re-run.
  """
  @spec move_cell(node() | nil, ref(), ref()) :: {:ok, non_neg_integer()} | {:error, term()}
  def move_cell(source_node, source, destination)

  def move_cell(source_node, source, source) when source_node in [nil] or source_node == node(),
    do: {:ok, 0}

  def move_cell(source_node, source, destination)
      when source_node in [nil] or source_node == node() do
    files = Enum.filter(Ref.files(source), &File.exists?/1)

    if files == [] do
      {:ok, 0}
    else
      moved =
        Enum.count(files, fn path ->
          case Ref.file_path(destination, Path.basename(path)) do
            nil ->
              false

            dest ->
              File.mkdir_p!(Path.dirname(dest))
              File.rename(path, dest) == :ok
          end
        end)

      emit(:wal_moved, %{ref: source, destination: destination, files: moved})
      {:ok, moved}
    end
  end

  def move_cell(source_node, source, destination) do
    case pull_cell(source_node, source, as: destination) do
      {:ok, 0} ->
        {:ok, 0}

      {:ok, files} ->
        # Only now: the destination copy is on disk and fsynced, so dropping
        # the source cannot lose the Cell.
        _ =
          :erpc.call(
            source_node,
            Paths,
            :delete_durable,
            [source],
            Durations.get(:wal_transfer_call_ms)
          )

        emit(:wal_moved, %{ref: source, destination: destination, files: files})
        {:ok, files}

      err ->
        err
    end
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp pull_all(source_node, ref, names, timeout, destination) do
    Enum.reduce_while(names, {:ok, 0}, fn name, {:ok, done} ->
      case pull_file(source_node, ref, name, timeout, destination) do
        :ok ->
          {:cont, {:ok, done + 1}}

        {:error, reason} = err ->
          emit(:wal_pull_failed, %{ref: ref, file: name, reason: reason})
          {:halt, err}
      end
    end)
    |> case do
      {:ok, count} = ok ->
        emit(:wal_pulled, %{ref: ref, files: count, source_node: source_node})
        ok

      err ->
        err
    end
  end

  # `Paths.atomic_install/3` owns the `.transferring` → fsync → rename dance;
  # this only fills the handle it is given.
  defp pull_file(source_node, ref, name, timeout, destination) do
    case Ref.file_path(destination, name) do
      nil ->
        {:error, {:unknown_file, name}}

      dest ->
        File.mkdir_p!(Path.dirname(dest))

        Paths.atomic_install(
          dest,
          &copy_chunks(source_node, ref, name, &1, 0, timeout),
          :wal_transfer
        )
    end
  end

  defp copy_chunks(source_node, ref, name, fd, offset, timeout) do
    case call(source_node, __MODULE__, :read_chunk, [ref, name, offset], timeout) do
      {:ok, :eof} ->
        :ok

      {:ok, {:ok, data}} ->
        case :file.write(fd, data) do
          :ok -> copy_chunks(source_node, ref, name, fd, offset + byte_size(data), timeout)
          {:error, reason} -> {:error, reason}
        end

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, _} = err ->
        err
    end
  end

  defp call(node, module, fun, args, timeout) do
    {:ok, :erpc.call(node, module, fun, args, timeout)}
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp emit(event, metadata) do
    :telemetry.execute([:flezha, :state, event], %{count: 1}, metadata)
  end
end
