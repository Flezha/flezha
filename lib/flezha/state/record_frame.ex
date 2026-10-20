defmodule Flezha.State.RecordFrame do
  @moduledoc """
  The durable record frame on a device: the walks over a record log on disk and
  the two byte-range cuts over it.

  The frame itself, its one decoder and the proof a walk stops or refuses by
  are `Flezha.State.Frame`'s; every walk here reads a device through them.
  A torn tail, the append protocol's own crash, is repaired; a crc failure with
  complete records behind it is refused by name with the bytes left on disk.
  Records the controller acknowledged as durable are never deleted to make a
  log parse.

  Every walk therefore answers `{:ok, result}` or `{:error, refusal}`.

  This is the lowest layer of the State subsystem: it names `SeqIndex`, whose
  samples decide where a cut starts, and `Durable`, whose table says what a
  record log's rewrite is called while it is being written. Both are data.
  Every function takes the file it works on; nothing here holds a handle or a
  process.
  """

  alias Flezha.State.Frame
  alias Flezha.State.Durable
  alias Flezha.State.SeqIndex

  @header_size Frame.header_size()

  @doc """
  Read one record at `position` from `device`. Returns the decoded
  `{seq, header, change_bytes}` triple plus the byte length consumed, or the
  reason it did not decode and the record's framed extent where that is known.
  """
  @spec decode_record(:file.io_device(), non_neg_integer()) ::
          {:ok, non_neg_integer(), binary(), binary(), non_neg_integer()}
          | {:error, :crc_mismatch | :short_read | :bad_len | term(), non_neg_integer() | nil}
  def decode_record(device, position) do
    # Two preads, not one: the first reads the length, the second the whole
    # framed record so `decode_bytes/1` sees it exactly as a byte walker does.
    with {:ok, <<_seq::64-big, len::32-big>>} <- pread(device, position, 12),
         true <- len >= @header_size or {:error, :bad_len, nil},
         {:ok, framed} <- pread(device, position, 12 + len + 4) do
      case Frame.decode_bytes(framed) do
        {:ok, seq, header, change_bytes, _raw, consumed} ->
          {:ok, seq, header, change_bytes, consumed}

        {:error, _reason, _extent} = err ->
          err
      end
    else
      # `len >= @header_size or {:error, :bad_len, nil}` yields `true` or the
      # error tuple, so the three-element clause carries :bad_len too.
      :eof -> {:error, :short_read, nil}
      {:error, _, _} = err -> err
      {:error, reason} -> {:error, reason, nil}
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

  # A torn tail IS repaired — the append protocol's own crash leaves no complete
  # record behind the failure point — but no longer in silence.
  defp report_repair(path, reason, last_seq, offset, discarded) do
    :telemetry.execute(
      [:flezha, :state, :record_frame, :repaired],
      %{bytes: discarded},
      %{path: path, reason: reason, last_seq: last_seq, offset: offset}
    )
  end

  @doc """
  Reduce over a record file's framed records, oldest first.

  `fun.({seq, header, body}, acc)` returns `{:cont, acc}` or `{:halt, acc}`; the
  walk also stops at a torn tail. A record that fails its crc with complete
  records behind it refuses instead — the accumulator so far is a partial
  history, and answering with it would ship it as a whole one.

  The one place the `decode_record/2` recursion lives.
  """
  @spec fold_records(:file.io_device(), Path.t(), acc, ({non_neg_integer(), binary(), binary()},
                                                        acc ->
                                                          {:cont, acc} | {:halt, acc})) ::
          {:ok, acc} | {:error, Frame.refusal()}
        when acc: term()
  def fold_records(device, path, acc, fun) do
    step = fn seq, header, body, _position, acc -> fun.({seq, header, body}, acc) end
    fold(device, path, 0, -1, acc, {step, fn acc, _tail -> acc end})
  end

  @doc """
  `fold_records/4` over a path: open read-only, fold, close. A file that is
  missing or unreadable folds to `acc` unchanged — absent history is no records,
  which is what every reader of this framing already means by it.

  The one definition of the open-fold-close dance, shared by the record-log
  reader, time travel, and the remote durability tier's ship.
  """
  @spec fold_path(binary(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                    {:cont, acc} | {:halt, acc})) ::
          {:ok, acc} | {:error, Frame.refusal()}
        when acc: term()
  def fold_path(path, acc, fun) do
    case :file.open(path, [:raw, :read, :binary]) do
      {:ok, device} ->
        try do
          fold_records(device, path, acc, fun)
        after
          :file.close(device)
        end

      {:error, _} ->
        {:ok, acc}
    end
  end

  @doc """
  `fold_path/3` for a reader that has no channel to answer a refusal on: a
  refused record raises rather than being handed back as a shorter history.

  The readers that use it — the record log's decode, time travel and the
  durability tier's ship — all answer a list or a tuple of bytes, so a refusal
  returned to them would be indistinguishable from a log that ended there.
  """
  @spec fold_path!(binary(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                     {:cont, acc} | {:halt, acc})) :: acc
        when acc: term()
  def fold_path!(path, acc, fun) do
    case fold_path(path, acc, fun) do
      {:ok, acc} -> acc
      {:error, refusal} -> raise Frame.refusal_message(path, refusal)
    end
  end

  # The one walk. `step.(seq, header, body, position, acc)` answers `{:cont, acc}`
  # or `{:halt, result}` per record; where the records end, `stop.(acc, tail)` with
  # `tail` `{last_seq, position, reason, size}` gives the result. A walk never
  # repairs by itself: a tail it may not read is where the records end, and
  # anything else is a refusal.
  defp fold(device, path, position, last_seq, acc, {step, stop} = rule) do
    case decode_record(device, position) do
      {:ok, seq, header, body, consumed} ->
        case step.(seq, header, body, position, acc) do
          {:cont, acc} -> fold(device, path, position + consumed, seq, acc, rule)
          {:halt, result} -> {:ok, result}
        end

      {:error, reason, extent} ->
        case Frame.stop_or_refuse(
               {:log, path},
               position,
               last_seq,
               reason,
               extent,
               device_size(device)
             ) do
          {:stop, size} -> {:ok, stop.(acc, {last_seq, position, reason, size})}
          refused -> refused
        end
    end
  end

  defp device_size(device) do
    {:ok, size} = :file.position(device, :eof)
    size
  end

  @doc """
  `key => seq` for every record in the file that `key_of` gives a key, `nil`
  leaving a record out — the set a keyed log dedups against.
  """
  @spec index_by(
          :file.io_device(),
          Path.t(),
          ({non_neg_integer(), binary(), binary()} -> term() | nil)
        ) ::
          {:ok, %{term() => non_neg_integer()}} | {:error, Frame.refusal()}
  def index_by(device, path, key_of) do
    fold_records(device, path, %{}, fn {seq, _header, _body} = record, acc ->
      case key_of.(record) do
        nil -> {:cont, acc}
        key -> {:cont, Map.put(acc, key, seq)}
      end
    end)
  end

  @doc """
  Walk from offset 0, returning the seq of the last record whose crc validates.
  Repairs a partial-write tail past that point, and reports the repair; refuses
  anything `repairable?/3` cannot prove safe, leaving the file as it found it.

  Backward scan would require a second copy of the length field at the record
  tail; we use forward scan and rely on monotonic seqs for the "last valid
  record" answer. The cost is O(records-since-snapshot) at boot; acceptable
  because compaction keeps the files short.
  """
  @spec scan_for_last_valid_seq(:file.io_device(), Path.t()) ::
          {:ok, integer()} | {:error, Frame.refusal()}
  def scan_for_last_valid_seq(device, path) do
    # `-1`, not `0`, for an empty file: the SDK's per-Cell seq is 0-based
    # (`change_seq` starts at 0), so a `0` floor makes the dedup guard
    # (`seq <= last_persisted_seq`) drop the very first change (seq 0) of every
    # relay. `-1` is the same "no records yet" sentinel `realign_epoch` uses.
    # The step answers a literal, so the walk allocates nothing per record.
    each = fn _, _, _, _, nil -> {:cont, nil} end
    fold(device, path, 0, -1, nil, {each, fn nil, tail -> repair(device, path, tail) end})
  end

  # The tail's `position` is the end of the last record that verified, which is
  # why a repair truncates to it.
  defp repair(device, path, {last_seq, position, reason, size}) do
    case :file.position(device, position) do
      {:ok, _} ->
        _ = :file.truncate(device)
        if size > position, do: report_repair(path, reason, last_seq, position, size - position)
        last_seq

      _ ->
        last_seq
    end
  end

  @doc """
  The file position where the first record with seq >= `floor` starts, found by
  scanning from `index`'s newest sample at or below it; the end of the valid
  records when no such record exists. Leaves `device` positioned at its end,
  where appends go.

  Refuses rather than answering the offset of a record that failed its crc: a
  cut taken there would delete the valid records between it and the boundary
  the caller asked for.
  """
  @spec position_of(SeqIndex.t(), :file.io_device(), Path.t(), non_neg_integer()) ::
          {:ok, non_neg_integer()} | {:error, Frame.refusal()}
  def position_of(index, device, path, floor) do
    {sample_seq, start} = SeqIndex.sample_at(index, floor)
    start = if start > 0 and not record_at?(device, start, sample_seq), do: 0, else: start
    at_floor = fn seq, _, _, at, nil -> if seq >= floor, do: {:halt, at}, else: {:cont, nil} end
    result = fold(device, path, start, -1, nil, {at_floor, fn nil, {_, at, _, _} -> at end})
    {:ok, _} = :file.position(device, :eof)
    result
  end

  # A sample is trusted only while the record at its position still carries its
  # seq: from a stale one a seek would cut mid-record or past the end.
  defp record_at?(device, position, seq),
    do: match?({:ok, ^seq, _header, _body, _consumed}, decode_record(device, position))

  @doc """
  Drop the records below `floor` from the record log at `path`, whose live
  handle is `device` and whose index is `index`. The boundary is found within
  one sample span, the kept tail is copied as bytes into `<path>.rewriting`,
  fsynced and renamed into place, and the handle is reopened on the new file
  at its end. Answers `{:ok, cut_bytes, device, index}` with the index
  rebased; `:none` when nothing is below the floor, or fewer than `min_bytes`
  are (the rewrite is not worth its fsync yet); `{:error, reason}` when the
  file is gone (a cut queued behind its directory's removal) or a record on the
  way to the boundary is refused, with the caller's handle untouched. Both
  record logs cut through this, so a cut costs the tail, never the file.
  """
  @spec cut_below(Path.t(), :file.io_device(), SeqIndex.t(), non_neg_integer(), non_neg_integer()) ::
          {:ok, non_neg_integer(), :file.io_device(), SeqIndex.t()} | :none | {:error, term()}
  def cut_below(path, device, index, floor, min_bytes \\ 0) do
    with {:ok, cut} <- position_of(index, device, path, floor) do
      if cut == 0 or cut < min_bytes do
        :none
      else
        with {:ok, file} <- replace_with_range(path, device, cut, :eof) do
          {:ok, cut, file, SeqIndex.after_cut(index, floor, cut)}
        end
      end
    end
  end

  @doc """
  Drop the records at or above `floor` — the mirror of `cut_below/5`: a
  correction's rewind drops the suffix it invalidated, keeping the prefix as
  bytes. The samples below the boundary stay valid. Answers `{:ok, device,
  index}`, or the refusal when a record below the boundary does not decode —
  the one cut that deletes what it did not read.
  """
  @spec cut_above(Path.t(), :file.io_device(), SeqIndex.t(), non_neg_integer()) ::
          {:ok, :file.io_device(), SeqIndex.t()} | {:error, term()}
  def cut_above(path, device, index, floor) do
    with {:ok, bound} <- position_of(index, device, path, floor),
         {:ok, file} <- replace_with_range(path, device, 0, bound) do
      {:ok, file, SeqIndex.cut_above(index, floor, bound)}
    end
  end

  # Copy the bytes `[from, to)` of `path` into `<path>.rewriting`, fsync it,
  # rename it into place and reopen `device` on it at its end. The two-step is
  # what makes a crash mid-rewrite leave the original intact
  # (`Paths.scan_dir/1` skips `.rewriting` residue); the rename happens
  # before the old handle closes, so a failure before it leaves that handle
  # valid.
  defp replace_with_range(path, device, from, to) do
    new_path = path <> Durable.record_rewrite_suffix()

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

  @doc "Byte size of the record file at `path`; `0` when it is not there."
  @spec file_size(Path.t()) :: non_neg_integer()
  def file_size(path) do
    case :file.read_file_info(path) do
      {:ok, info} -> elem(info, 1)
      _ -> 0
    end
  end
end
