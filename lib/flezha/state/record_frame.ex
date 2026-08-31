defmodule Flezha.State.RecordFrame do
  @moduledoc """
  The on-disk record frame every durable log in this subsystem shares, and the
  two byte-range cuts over it.

  A record is `[seq: u64 BE][len: u32 BE][header: 16 B][body][crc32: u32 BE]`,
  self-delimiting and CRC-framed. There is no length at the tail, so a reader
  walks forward from the start and stops at the first record whose CRC does not
  verify — the torn-tail contract the Entity WAL, the input log, the
  proc-journal, the inbox and the retention rewriter all share, and the reason a
  record-aligned byte range is the same log the records would re-encode to.

  This is the lowest layer of the State subsystem: it names `SeqIndex`, whose
  samples decide where a cut starts, and nothing else. Every function takes the
  file it works on; nothing here holds a handle or a process.
  """

  alias Flezha.State.SeqIndex

  @header_size 16

  @doc "Bytes of the record header — the one definition."
  @spec header_size() :: pos_integer()
  def header_size(), do: @header_size

  @doc """
  Frame a record for on-disk storage.
  """
  @spec encode_record(non_neg_integer(), binary(), binary()) :: binary()
  def encode_record(seq, header, change_bytes)
      when is_integer(seq) and seq >= 0 and is_binary(header) and is_binary(change_bytes) do
    if byte_size(header) != @header_size do
      raise ArgumentError, "header must be #{@header_size} bytes, got #{byte_size(header)}"
    end

    len = @header_size + byte_size(change_bytes)
    crc = :erlang.crc32(<<header::binary, change_bytes::binary>>)
    <<seq::64-big, len::32-big, header::binary, change_bytes::binary, crc::32-big>>
  end

  @doc """
  Read one record at `position` from `device`. Returns the decoded
  `{seq, header, change_bytes}` triple plus the byte length consumed,
  or an error tag.
  """
  @spec decode_record(:file.io_device(), non_neg_integer()) ::
          {:ok, non_neg_integer(), binary(), binary(), non_neg_integer()}
          | {:error, :crc_mismatch | :short_read | :bad_len}
  def decode_record(device, position) do
    with {:ok, <<seq::64-big, len::32-big>>} <- pread(device, position, 12),
         true <- len >= @header_size or {:error, :bad_len},
         {:ok, payload_and_crc} <- pread(device, position + 12, len + 4) do
      change_len = len - @header_size

      <<header::binary-size(@header_size), change_bytes::binary-size(^change_len), crc::32-big>> =
        payload_and_crc

      computed = :erlang.crc32(<<header::binary, change_bytes::binary>>)

      if computed == crc do
        {:ok, seq, header, change_bytes, 12 + len + 4}
      else
        {:error, :crc_mismatch}
      end
    else
      # `len >= @header_size or {:error, :bad_len}` yields `true` or the
      # error tuple, so the tuple clause carries :bad_len too.
      :eof -> {:error, :short_read}
      {:error, _} = err -> err
    end
  end

  defp pread(device, position, count) do
    case :file.pread(device, position, count) do
      {:ok, bin} when byte_size(bin) == count -> {:ok, bin}
      {:ok, _} -> :eof
      :eof -> :eof
      {:error, _} = err -> err
    end
  end

  @doc """
  Reduce over a record file's framed records, oldest first.

  `fun.({seq, header, body}, acc)` returns `{:cont, acc}` or `{:halt, acc}`; the
  walk also stops at the first undecodable record.

  The one place the `decode_record/2` recursion lives.
  """
  @spec fold_records(:file.io_device(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                                {:cont, acc} | {:halt, acc})) :: acc
        when acc: term()
  def fold_records(device, acc, fun), do: fold(device, 0, acc, fun)

  @doc """
  `fold_records/3` over a path: open read-only, fold, close. A file that is
  missing or unreadable folds to `acc` unchanged — absent history is no records,
  which is what every reader of this framing already means by it.

  The one definition of the open-fold-close dance, shared by the record-log
  reader, time travel, and the remote durability tier's ship.
  """
  @spec fold_path(binary(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                    {:cont, acc} | {:halt, acc})) :: acc
        when acc: term()
  def fold_path(path, acc, fun) do
    case :file.open(path, [:raw, :read, :binary]) do
      {:ok, device} ->
        try do
          fold_records(device, acc, fun)
        after
          :file.close(device)
        end

      {:error, _} ->
        acc
    end
  end

  defp fold(device, position, acc, fun) do
    case decode_record(device, position) do
      {:ok, seq, header, body, consumed} ->
        case fun.({seq, header, body}, acc) do
          {:cont, acc} -> fold(device, position + consumed, acc, fun)
          {:halt, acc} -> acc
        end

      {:error, _} ->
        acc
    end
  end

  @doc """
  `header => seq` for every record in the file — the set a keyed log dedups
  against.
  """
  @spec header_index(:file.io_device()) :: %{binary() => non_neg_integer()}
  def header_index(device) do
    fold_records(device, %{}, fn {seq, header, _body}, acc ->
      {:cont, Map.put(acc, header, seq)}
    end)
  end

  @doc """
  Walk from offset 0, returning the seq of the last record whose crc validates.
  Truncates any partial-write tail past that point.

  Backward scan would require a second copy of the length field at the record
  tail; we use forward scan and rely on monotonic seqs for the "last valid
  record" answer. The cost is O(records-since-snapshot) at boot; acceptable
  because compaction keeps the files short.
  """
  @spec scan_for_last_valid_seq(:file.io_device()) :: integer()
  def scan_for_last_valid_seq(device) do
    # `-1`, not `0`, for an empty file: the SDK's per-Entity seq is 0-based
    # (`change_seq` starts at 0), so a `0` floor makes the dedup guard
    # (`seq <= last_persisted_seq`) drop the very first change (seq 0) of every
    # operative. `-1` is the same "no records yet" sentinel `realign_epoch` uses.
    do_scan(device, 0, -1, 0)
  end

  defp do_scan(device, position, last_seq, last_end) do
    case decode_record(device, position) do
      {:ok, seq, _header, _bytes, consumed} ->
        do_scan(device, position + consumed, seq, position + consumed)

      {:error, _} ->
        # Truncate any partial-write tail past the last valid record.
        case :file.position(device, last_end) do
          {:ok, _} ->
            _ = :file.truncate(device)
            last_seq

          _ ->
            last_seq
        end
    end
  end

  @doc """
  The file position where the first record with seq >= `floor` starts, found by
  scanning from `index`'s newest sample at or below it; the end of the valid
  records when no such record exists. Leaves `device` positioned at its end,
  where appends go.
  """
  @spec position_of(SeqIndex.t(), :file.io_device(), non_neg_integer()) :: non_neg_integer()
  def position_of(index, device, floor) do
    position = seek(device, SeqIndex.sample_at(index, floor), floor)
    {:ok, _} = :file.position(device, :eof)
    position
  end

  defp seek(device, position, floor) do
    case decode_record(device, position) do
      {:ok, seq, _header, _body, _consumed} when seq >= floor -> position
      {:ok, _seq, _header, _body, consumed} -> seek(device, position + consumed, floor)
      {:error, _} -> position
    end
  end

  @doc """
  Drop the records below `floor` from the record log at `path`, whose live
  handle is `device` and whose index is `index`. The boundary is found within
  one sample span, the kept tail is copied as bytes into `<path>.rewriting`,
  fsynced and renamed into place, and the handle is reopened on the new file
  at its end. Answers `{:ok, cut_bytes, device, index}` with the index
  rebased; `:none` when nothing is below the floor, or fewer than `min_bytes`
  are (the rewrite is not worth its fsync yet); `{:error, reason}` when the
  file is gone (a cut queued behind its directory's removal), with the
  caller's handle untouched. Both record logs cut through this, so a cut costs
  the tail, never the file.
  """
  @spec cut_below(Path.t(), :file.io_device(), SeqIndex.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer(), :file.io_device(), SeqIndex.t()} | :none | {:error, term()}
  def cut_below(path, device, index, floor, min_bytes \\ 0) do
    case position_of(index, device, floor) do
      cut when cut == 0 or cut < min_bytes ->
        :none

      cut ->
        with {:ok, file} <- replace_with_range(path, device, cut, :eof) do
          {:ok, cut, file, SeqIndex.after_cut(index, floor, cut)}
        end
    end
  end

  @doc """
  Drop the records at or above `floor` — the mirror of `cut_below/5`: a
  correction's rewind drops the suffix it invalidated, keeping the prefix as
  bytes. The samples below the boundary stay valid. Answers `{:ok, device,
  index}`.
  """
  @spec cut_above(Path.t(), :file.io_device(), SeqIndex.t(), non_neg_integer()) ::
          {:ok, :file.io_device(), SeqIndex.t()}
  def cut_above(path, device, index, floor) do
    bound = position_of(index, device, floor)
    {:ok, file} = replace_with_range(path, device, 0, bound)
    {:ok, file, SeqIndex.cut_above(index, floor, bound)}
  end

  # Copy the bytes `[from, to)` of `path` into `<path>.rewriting`, fsync it,
  # rename it into place and reopen `device` on it at its end. The two-step is
  # what makes a crash mid-rewrite leave the original intact
  # (`Paths.scan_dir/1` skips `.rewriting` residue); the rename happens
  # before the old handle closes, so a failure before it leaves that handle
  # valid.
  defp replace_with_range(path, device, from, to) do
    new_path = path <> ".rewriting"

    with {:ok, src} <- :file.open(path, [:raw, :read, :binary]),
         {:ok, dst} <- open_or_close(new_path, src),
         :ok <- copy_range(src, dst, from, to),
         :ok <- :file.rename(new_path, path),
         :ok = :file.close(device),
         {:ok, file} <- :file.open(path, [:raw, :read, :write, :binary]),
         {:ok, _} <- :file.position(file, :eof) do
      {:ok, file}
    end
  end

  defp copy_range(src, dst, from, to) do
    try do
      with {:ok, _} <- :file.position(src, from),
           {:ok, _} <-
             if(to == :eof, do: :file.copy(src, dst), else: :file.copy(src, dst, to - from)),
           :ok <- :file.sync(dst) do
        :ok
      end
    after
      _ = :file.close(src)
      _ = :file.close(dst)
    end
  end

  defp open_or_close(path, src) do
    case :file.open(path, [:raw, :write, :binary]) do
      {:ok, dst} ->
        {:ok, dst}

      {:error, _} = error ->
        _ = :file.close(src)
        error
    end
  end
end
