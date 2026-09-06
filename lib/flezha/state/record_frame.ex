defmodule Flezha.State.RecordFrame do
  @moduledoc """
  The on-disk record frame every durable log in this subsystem shares, and the
  two byte-range cuts over it.

  A record is `[seq: u64 BE][len: u32 BE][header: 16 B][body][crc32: u32 BE]`,
  self-delimiting and CRC-framed. There is no length at the tail, so a reader
  walks forward from the start and stops at the first record that does not
  decode. What it does then is decided by one rule, `repairable?/3`: a record
  log is truncated only where no complete record can begin after the failure
  point. A torn tail — the append protocol's own crash — satisfies that proof
  and is still repaired; a crc failure with complete records behind it does
  not, and is refused by name with the bytes left on disk. Records the
  controller acknowledged as durable are never deleted to make a log parse.

  Every walk therefore answers `{:ok, result}` or `{:error, refusal}`, and
  `refuse/4` is the one place a refusal is logged and counted.

  This is the lowest layer of the State subsystem: it names `SeqIndex`, whose
  samples decide where a cut starts, and `Durable`, whose table says what a
  record log's rewrite is called while it is being written. Both are data.
  Every function takes the file it works on; nothing here holds a handle or a
  process.
  """

  alias Flezha.State.Durable
  alias Flezha.State.SeqIndex

  require Logger

  @header_size 16

  # The Elixir mirror of `flz_core/src/header.rs`'s allocation table: the
  # writing SDK at byte 0 and the body codec at byte 9. One row per allocated
  # value, checked against the kernel's own by `test/fixtures/durable_frame.txt`.
  @sdk_langs %{
    0 => "unspecified",
    1 => "rust",
    2 => "java",
    3 => "elixir",
    4 => "node",
    5 => "ruby",
    6 => "php",
    7 => "python"
  }

  @format_ids %{
    postcard: 1,
    effect_record: 2,
    timer_ops: 3,
    kryo: 4,
    msgpack: 5,
    json: 6,
    etf: 7
  }

  @typedoc """
  A refused record: the decoder's own reason tag, the last seq that verified
  before it, and the byte offset the failure was read at.
  """
  @type refusal :: {reason :: atom(), last_seq :: integer(), offset :: non_neg_integer()}

  @typedoc """
  Where a fold's bytes came from: a record log at a path, which an append may
  have torn, or a change log shipped whole under a name.
  """
  @type source :: {:log, Path.t()} | {:payload, binary()}

  @typedoc "The 16 header bytes, decomposed."
  @type header :: %{
          sdk_lang: byte(),
          sdk_version: non_neg_integer(),
          schema_version: non_neg_integer(),
          format_id: byte(),
          crc32: non_neg_integer()
        }

  @doc "Bytes of the record header — the one definition."
  @spec header_size() :: pos_integer()
  def header_size(), do: @header_size

  @doc "The allocated name of an `sdk_lang` byte; `\"unallocated\"` for a value no SDK holds."
  @spec sdk_lang_name(byte()) :: binary()
  def sdk_lang_name(lang), do: Map.get(@sdk_langs, lang, "unallocated")

  @doc "The `format_id` byte allocated to a body codec."
  @spec format_id(atom()) :: byte()
  def format_id(codec), do: Map.fetch!(@format_ids, codec)

  @doc """
  Stamp the 16-byte header for `body`: language, SDK version, schema version,
  format, two reserved zero bytes, and the crc32 over the body.

  The write side of `decode_header/1`; the byte layout exists here and nowhere
  else on this side of the wire.
  """
  @spec encode_header(byte(), non_neg_integer(), non_neg_integer(), byte(), binary()) ::
          <<_::128>>
  def encode_header(sdk_lang, sdk_version, schema_version, format_id, body)
      when is_binary(body) do
    <<sdk_lang, sdk_version::32-big, schema_version::32-big, format_id, 0, 0,
      :erlang.crc32(body)::32-big>>
  end

  @doc """
  Decompose the 16 header bytes. `:error` when there are not 16 of them —
  every caller answers a malformed header its own way, and none of them
  re-reads the layout to do it.
  """
  @spec decode_header(binary()) :: {:ok, header()} | :error
  def decode_header(<<lang, version::32-big, schema::32-big, format, _reserved::16, crc::32-big>>) do
    {:ok,
     %{
       sdk_lang: lang,
       sdk_version: version,
       schema_version: schema,
       format_id: format,
       crc32: crc
     }}
  end

  def decode_header(_bytes), do: :error

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

    <<seq::64-big, len::32-big, header::binary, change_bytes::binary,
      crc32(header, change_bytes)::32-big>>
  end

  @doc """
  The crc32 over `header <> body` — the one expression in the tree.

  Public because the snapshot file, whose own format is not this frame, covers
  exactly the same two ranges with exactly the same crc; a second spelling of
  one value is a second thing to keep in step.
  """
  @spec crc32(binary(), binary()) :: non_neg_integer()
  def crc32(header, body), do: :erlang.crc32(<<header::binary, body::binary>>)

  @doc """
  Decode one record from the head of `bytes`. Returns the decoded
  `{seq, header, change_bytes}` triple, the record's own bytes verbatim and the
  byte length consumed, or the reason it did not decode and its framed extent
  where that is known.

  The one decoder: `decode_record/2` reads a device into it, and the two
  byte-oriented walks fold through it.
  """
  @spec decode_bytes(binary()) ::
          {:ok, non_neg_integer(), binary(), binary(), binary(), non_neg_integer()}
          | {:error, :crc_mismatch | :short_read | :bad_len, non_neg_integer() | nil}
  def decode_bytes(<<_seq::64-big, len::32-big, _tail::binary>>) when len < @header_size,
    do: {:error, :bad_len, nil}

  def decode_bytes(<<seq::64-big, len::32-big, tail::binary>> = bytes) do
    change_len = len - @header_size
    consumed = 12 + len + 4

    case tail do
      <<header::binary-size(@header_size), change_bytes::binary-size(^change_len), stored::32-big,
        _rest::binary>> ->
        if crc32(header, change_bytes) == stored do
          <<raw::binary-size(^consumed), _rest::binary>> = bytes
          {:ok, seq, header, change_bytes, raw, consumed}
        else
          # The extent is exact even though the content is not: the record's
          # `len` framed it and the bytes are all here. Carrying it is what
          # lets `repairable?/3` decide without a second read.
          {:error, :crc_mismatch, consumed}
        end

      _short ->
        {:error, :short_read, nil}
    end
  end

  def decode_bytes(_short), do: {:error, :short_read, nil}

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
      case decode_bytes(framed) do
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

  @doc """
  Whether a walk stopped by `reason`, with `remaining` bytes left from the
  failure point to the end of the file, may be repaired by truncating there.

  The one proof in the subsystem, stated once and applied at every entry point:
  a record log is truncated only where **no complete record can begin after the
  failure point**.

    * a read that ran off the end of the file has nothing behind it by
      construction;
    * a crc failure has an exact framed extent, so it is a torn tail exactly
      when that extent is the rest of the file — anything more and the records
      behind it are complete records the controller acknowledged;
    * a length field below the header size leaves the next record unlocatable,
      so it can only be judged by what remains: less than one whole record, and
      nothing can follow it.
  """
  @spec repairable?(atom(), non_neg_integer() | nil, non_neg_integer()) :: boolean()
  def repairable?(:short_read, _extent, _remaining), do: true
  def repairable?(:crc_mismatch, extent, remaining), do: extent == remaining
  def repairable?(:bad_len, _extent, remaining), do: remaining < 12 + @header_size + 4
  def repairable?(_reason, _extent, _remaining), do: false

  @doc """
  Report a refused record and shape the refusal every reader of this framing
  answers with.

  The one place this subsystem writes a refusal's telemetry event and its error
  line, so a further reader of the framing reports by calling this rather than
  by writing a second log line of its own.
  """
  @spec refuse(Path.t(), atom(), integer(), non_neg_integer()) :: {:error, refusal()}
  def refuse(path, reason, last_seq, offset) do
    refusal = {reason, last_seq, offset}

    :telemetry.execute(
      [:flezha, :state, :record_frame, :refused],
      %{count: 1},
      %{path: path, reason: reason, last_seq: last_seq, offset: offset}
    )

    Logger.error(refusal_message(path, refusal))
    {:error, refusal}
  end

  @doc """
  The one sentence a refusal is logged with, and reported to callers as.

  `source` is the log the records came from: a path for a file, the payload's
  own name for a change log read from bytes.
  """
  @spec refusal_message(Path.t(), refusal()) :: binary()
  def refusal_message(source, {reason, last_seq, offset}) do
    "[State] durable record refused (#{reason}) in #{source} at byte offset #{offset}, " <>
      "after seq #{last_seq}: the failure is corruption and not a torn tail, so the " <>
      "log is left as it stands and refuses to serve"
  end

  # A torn tail IS repaired — the append protocol's own crash leaves no complete
  # record behind the failure point — but no longer in silence.
  defp report_repair(path, reason, last_seq, offset, discarded) do
    :telemetry.execute(
      [:flezha, :state, :record_frame, :repaired],
      %{bytes: discarded},
      %{path: path, reason: reason, last_seq: last_seq, offset: offset}
    )

    Logger.warning(
      "[State] durable record log repaired (#{reason}) in #{path}: torn tail of " <>
        "#{discarded} byte(s) at offset #{offset} discarded, last seq #{last_seq}"
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
          {:ok, acc} | {:error, refusal()}
        when acc: term()
  def fold_records(device, path, acc, fun), do: fold(device, path, 0, -1, acc, fun)

  @doc """
  `fold_records/4` over a path: open read-only, fold, close. A file that is
  missing or unreadable folds to `acc` unchanged — absent history is no records,
  which is what every reader of this framing already means by it.

  The one definition of the open-fold-close dance, shared by the record-log
  reader, time travel, and the remote durability tier's ship.
  """
  @spec fold_path(binary(), acc, ({non_neg_integer(), binary(), binary()}, acc ->
                                    {:cont, acc} | {:halt, acc})) ::
          {:ok, acc} | {:error, refusal()}
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
      {:error, refusal} -> raise refusal_message(path, refusal)
    end
  end

  @doc """
  Reduce over records already in memory, oldest first — the byte-oriented twin
  of `fold_records/4`, for a reader holding a whole log or a change log shipped
  over the wire.

  `fun.({seq, header, body, raw}, acc)` returns `{:cont, acc}` or `{:halt, acc}`;
  `raw` is the record's own bytes verbatim, so a caller that ships them on does
  not re-frame what it just decoded.

  `source` says both where the bytes came from and what they are — the one
  thing a byte walk cannot read off them — and names them in a refusal:

    * `{:log, path}` — a record log an append may have torn, so `repairable?/3`
      decides as it does for the device walk;
    * `{:payload, label}` — a change log shipped whole, where nothing was
      mid-append: an incomplete trailing fragment still ends the walk, but a
      record whose crc fails is corruption and is refused however few bytes
      follow it.
  """
  @spec fold_bytes(
          binary(),
          source(),
          acc,
          ({non_neg_integer(), binary(), binary(), binary()}, acc ->
             {:cont, acc} | {:halt, acc})
        ) ::
          {:ok, acc} | {:error, refusal()}
        when acc: term()
  def fold_bytes(bytes, {kind, _name} = source, acc, fun)
      when is_binary(bytes) and kind in [:log, :payload],
      do: walk_bytes(bytes, source, 0, -1, acc, fun)

  defp walk_bytes(<<>>, _source, _position, _last_seq, acc, _fun), do: {:ok, acc}

  defp walk_bytes(bytes, source, position, last_seq, acc, fun) do
    case decode_bytes(bytes) do
      {:ok, seq, header, body, raw, consumed} ->
        case fun.({seq, header, body, raw}, acc) do
          {:cont, acc} ->
            <<_consumed::binary-size(^consumed), rest::binary>> = bytes
            walk_bytes(rest, source, position + consumed, seq, acc, fun)

          {:halt, acc} ->
            {:ok, acc}
        end

      {:error, reason, extent} ->
        case stop_or_refuse(
               source,
               position,
               last_seq,
               reason,
               extent,
               position + byte_size(bytes)
             ) do
          {:stop, _size} -> {:ok, acc}
          refused -> refused
        end
    end
  end

  defp fold(device, path, position, last_seq, acc, fun) do
    case decode_record(device, position) do
      {:ok, seq, header, body, consumed} ->
        case fun.({seq, header, body}, acc) do
          {:cont, acc} -> fold(device, path, position + consumed, seq, acc, fun)
          {:halt, acc} -> {:ok, acc}
        end

      {:error, reason, extent} ->
        # A fold reads; it never repairs. A tail it may not read is where the
        # records end, and anything else is a refusal.
        case stop_or_refuse(
               {:log, path},
               position,
               last_seq,
               reason,
               extent,
               device_size(device)
             ) do
          {:stop, _size} -> {:ok, acc}
          refused -> refused
        end
    end
  end

  # What every walk does with a record that did not decode: `{:stop, size}` when
  # the proof holds and the walk may end — or truncate — where it stands, the
  # refusal otherwise. One expression, so the four walkers cannot drift apart
  # again.
  #
  # The one exception a source carries: a payload was shipped whole, so it has
  # no torn tail to repair and the crc branch of `repairable?/3` does not apply
  # to it.
  defp stop_or_refuse({:payload, name}, position, last_seq, :crc_mismatch, _extent, _size),
    do: refuse(name, :crc_mismatch, last_seq, position)

  defp stop_or_refuse({_kind, name}, position, last_seq, reason, extent, size) do
    if repairable?(reason, extent, size - position),
      do: {:stop, size},
      else: refuse(name, reason, last_seq, position)
  end

  defp device_size(device) do
    {:ok, size} = :file.position(device, :eof)
    size
  end

  @doc """
  `header => seq` for every record in the file — the set a keyed log dedups
  against.
  """
  @spec header_index(:file.io_device(), Path.t()) ::
          {:ok, %{binary() => non_neg_integer()}} | {:error, refusal()}
  def header_index(device, path) do
    fold_records(device, path, %{}, fn {seq, header, _body}, acc ->
      {:cont, Map.put(acc, header, seq)}
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
          {:ok, integer()} | {:error, refusal()}
  def scan_for_last_valid_seq(device, path) do
    # `-1`, not `0`, for an empty file: the SDK's per-Entity seq is 0-based
    # (`change_seq` starts at 0), so a `0` floor makes the dedup guard
    # (`seq <= last_persisted_seq`) drop the very first change (seq 0) of every
    # operative. `-1` is the same "no records yet" sentinel `realign_epoch` uses.
    do_scan(device, path, 0, -1)
  end

  # `position` is both the next record's start and the end of the last one that
  # verified, which is why a repair truncates to it.
  defp do_scan(device, path, position, last_seq) do
    case decode_record(device, position) do
      {:ok, seq, _header, _bytes, consumed} ->
        do_scan(device, path, position + consumed, seq)

      {:error, reason, extent} ->
        case stop_or_refuse(
               {:log, path},
               position,
               last_seq,
               reason,
               extent,
               device_size(device)
             ) do
          {:stop, size} -> {:ok, repair(device, path, reason, last_seq, position, size)}
          refused -> refused
        end
    end
  end

  defp repair(device, path, reason, last_seq, position, size) do
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
          {:ok, non_neg_integer()} | {:error, refusal()}
  def position_of(index, device, path, floor) do
    result = seek(device, path, SeqIndex.sample_at(index, floor), -1, floor)
    {:ok, _} = :file.position(device, :eof)
    result
  end

  defp seek(device, path, position, last_seq, floor) do
    case decode_record(device, position) do
      {:ok, seq, _header, _body, _consumed} when seq >= floor ->
        {:ok, position}

      {:ok, seq, _header, _body, consumed} ->
        seek(device, path, position + consumed, seq, floor)

      {:error, reason, extent} ->
        case stop_or_refuse(
               {:log, path},
               position,
               last_seq,
               reason,
               extent,
               device_size(device)
             ) do
          {:stop, _size} -> {:ok, position}
          refused -> refused
        end
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
