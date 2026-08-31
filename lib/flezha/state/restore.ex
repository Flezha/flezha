defmodule Flezha.State.Restore do
  @moduledoc """
  Assembles the byte payload shipped with `RestoreInitState`.

  The controller reads:
    * The latest snapshot (if any) — supplies `snapshot_bytes` and the
      16-byte `snapshot_header` it was stored with.
    * The tail-WAL records strictly newer than the snapshot's `seq` —
      supplies `change_log_bytes` (records concatenated in their on-disk
      `[seq][len][header][bytes][crc32]` framing).

  When no snapshot exists, `snapshot_bytes = <<>>` and `change_log_bytes`
  contains the entire WAL. When neither snapshot nor WAL exist (true
  cold start), `terminal_seq_hint = 0` and both bodies are empty.
  """

  alias Flezha.State.{Paths, Snapshot}

  @header_size 16

  @type payload :: %{
          snapshot_bytes: binary(),
          snapshot_header: binary(),
          change_log_bytes: binary(),
          terminal_seq_hint: non_neg_integer()
        }

  @spec assemble_payload(Paths.entity_id()) :: {:ok, payload()}
  def assemble_payload(entity_id) do
    # `-1` (not `0`) is the "no snapshot" sentinel: the WAL is 0-based, so a
    # `0` sentinel with the `seq > snapshot_seq` tail filter would silently
    # drop the very first change record. `-1` includes seq 0.
    # The header travels with the body: it carries the schema_version the
    # snapshot was written at, and stripping it left every compacted Entity
    # unverifiable on the SDK side.
    {snapshot_bytes, snapshot_header, snapshot_seq} =
      case Snapshot.latest_snapshot(entity_id) do
        nil ->
          {<<>>, <<>>, -1}

        {seq, path} ->
          case Snapshot.read_snapshot(path) do
            {:ok, header, bytes} -> {bytes, header, seq}
            {:error, _} -> {<<>>, <<>>, -1}
          end
      end

    {change_log_bytes, terminal_seq} =
      read_tail_wal(entity_id, snapshot_seq)

    terminal_seq_hint = max(snapshot_seq, terminal_seq)

    {:ok,
     %{
       snapshot_bytes: snapshot_bytes,
       snapshot_header: snapshot_header,
       change_log_bytes: change_log_bytes,
       terminal_seq_hint: terminal_seq_hint
     }}
  end

  @doc """
  Whether the Entity has any persisted state to restore. A zero-byte WAL
  (lazily created by the writer before any record landed) is no state: an
  assembled payload from it would be empty and rebuild a live Entity to
  `init_state`.
  """
  @spec has_persisted_state?(Paths.entity_id()) :: boolean()
  def has_persisted_state?(entity_id) do
    Snapshot.latest_snapshot(entity_id) != nil or
      match?(
        {:ok, %{size: size}} when size > 0,
        File.stat(Paths.wal_path(entity_id))
      )
  end

  @doc """
  The SDK language byte stamped in this Entity's durable state, or `nil` when
  it has none.

  Byte 0 of the newest snapshot header, else of the first WAL record's. The
  authority on which SDK wrote an Entity's state, and what the placement guard
  resolves a factory's established language from. A routing decision, not a
  validation: the body is never touched and an unknown value is reported as
  found.
  """
  @spec sdk_lang(Paths.entity_id()) :: byte() | nil
  def sdk_lang(entity_id) do
    case Snapshot.latest_snapshot(entity_id) do
      {_seq, path} ->
        case Snapshot.read_snapshot(path) do
          {:ok, <<lang, _::binary>>, _bytes} -> lang
          _ -> first_wal_lang(entity_id)
        end

      nil ->
        first_wal_lang(entity_id)
    end
  end

  @doc """
  The SDK language stamped across `operative_class`'s durable state, or `nil`
  when the class has none on disk.

  The first Entity that answers wins: a class is written by one SDK, so a
  second read would only confirm the first.
  """
  @spec class_sdk_lang(binary()) :: byte() | nil
  def class_sdk_lang(operative_class) when is_binary(operative_class) do
    operative_class
    |> Paths.entities_of()
    |> Enum.find_value(&sdk_lang/1)
  end

  # The WAL's first record carries the same header shape the snapshot does, at
  # a fixed offset in the `[seq: 8][len: 4][header: 16]...` record framing.
  defp first_wal_lang(entity_id) do
    case :file.read_file(Paths.wal_path(entity_id)) do
      {:ok, <<_seq::64-big, len::32-big, lang, _::binary>>} when len >= @header_size -> lang
      _ -> nil
    end
  end

  # Read the WAL once and parse records in memory; a per-record `:file.pread`
  # churns small transient refc binaries that fragment the binary allocator.
  defp read_tail_wal(entity_id, snapshot_seq) do
    case :file.read_file(Paths.wal_path(entity_id)) do
      {:ok, data} ->
        {iodata, terminal_seq} = gather_records(data, snapshot_seq, [], snapshot_seq)
        {IO.iodata_to_binary(iodata), terminal_seq}

      {:error, _} ->
        {<<>>, snapshot_seq}
    end
  end

  defp gather_records(
         <<seq::64-big, len::32-big, tail::binary>> = data,
         snapshot_seq,
         acc,
         term_seq
       )
       when len >= @header_size and byte_size(tail) >= len + 4 do
    change_len = len - @header_size

    <<header::binary-size(@header_size), change::binary-size(^change_len), crc::32-big,
      _::binary>> = tail

    if :erlang.crc32(<<header::binary, change::binary>>) == crc do
      # Keep the on-disk bytes verbatim rather than re-framing them.
      record_len = 12 + len + 4
      <<record::binary-size(^record_len), rest::binary>> = data
      acc = if seq > snapshot_seq, do: [record | acc], else: acc
      gather_records(rest, snapshot_seq, acc, max(term_seq, seq))
    else
      # Stop at the first corrupt record, mirroring the device-scan contract.
      {Enum.reverse(acc), term_seq}
    end
  end

  defp gather_records(_remaining, _snapshot_seq, acc, term_seq) do
    {Enum.reverse(acc), term_seq}
  end
end
