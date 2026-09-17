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

  alias Flezha.Operative.ClassRegistry
  alias Flezha.SDK.RecordFrame
  alias Flezha.State.{Paths, Snapshot}

  @type payload :: %{
          snapshot_bytes: binary(),
          snapshot_header: binary(),
          change_log_bytes: binary(),
          terminal_seq_hint: non_neg_integer()
        }

  @spec assemble_payload(Paths.entity_id()) :: {:ok, payload()} | {:error, binary()}
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

    with {:ok, {change_log_bytes, terminal_seq}} <- read_tail_wal(entity_id, snapshot_seq) do
      {:ok,
       %{
         snapshot_bytes: snapshot_bytes,
         snapshot_header: snapshot_header,
         change_log_bytes: change_log_bytes,
         terminal_seq_hint: max(snapshot_seq, terminal_seq)
       }}
    end
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
    with {_seq, path} <- Snapshot.latest_snapshot(entity_id),
         {:ok, header, _bytes} <- Snapshot.read_snapshot(path),
         {:ok, %{sdk_lang: lang}} <- RecordFrame.decode_header(header) do
      lang
    else
      _ -> first_wal_lang(entity_id)
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

  # The WAL's first record carries the same header the snapshot does; the frame
  # seam cuts both, so neither layout is restated here.
  defp first_wal_lang(entity_id) do
    with {:ok, data} <- :file.read_file(Paths.wal_path(entity_id)),
         {:ok, _seq, header, _body, _raw, _consumed} <- RecordFrame.decode_bytes(data),
         {:ok, %{sdk_lang: lang}} <- RecordFrame.decode_header(header) do
      lang
    else
      _ -> nil
    end
  end

  # Read the WAL once and fold the records in memory; a per-record
  # `:file.pread` churns small transient refc binaries that fragment the binary
  # allocator.
  defp read_tail_wal(entity_id, snapshot_seq) do
    path = Paths.wal_path(entity_id)

    case :file.read_file(path) do
      {:ok, data} ->
        data
        |> RecordFrame.fold_bytes(
          {:log, path},
          {[], snapshot_seq},
          &tail_record(&1, &2, snapshot_seq)
        )
        |> case do
          {:ok, {acc, term_seq}} -> {:ok, {change_log(acc), term_seq}}
          {:error, refusal} -> {:error, RecordFrame.refusal_message(path, refusal)}
        end

      {:error, _} ->
        {:ok, {<<>>, snapshot_seq}}
    end
  end

  # Keep the on-disk bytes verbatim rather than re-framing them: they are
  # shipped to the SDK as they lie.
  defp tail_record({seq, _header, _body, raw}, {acc, term_seq}, snapshot_seq) do
    acc = if seq > snapshot_seq, do: [raw | acc], else: acc
    {:cont, {acc, max(term_seq, seq)}}
  end

  defp change_log(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  @doc """
  Establish the language of `factory_id` in `namespace` from the bytes
  `operative_class` actually wrote, correcting a value some runner established
  by advertising first.

  The header is the authority wherever durable state exists. A class with
  nothing on disk records nothing. Called from the bind, the one seam holding
  both the factory id and the operative's on-disk class.
  """
  @spec establish_sdk_lang_from_state(binary(), binary(), binary()) :: :ok
  def establish_sdk_lang_from_state(namespace, factory_id, operative_class)
      when is_binary(namespace) and is_binary(factory_id) and is_binary(operative_class) do
    case class_sdk_lang(operative_class) do
      nil ->
        :ok

      lang ->
        if ClassRegistry.sdk_lang(namespace, factory_id) == lang,
          do: :ok,
          else: ClassRegistry.put_sdk_lang(namespace, factory_id, lang)
    end
  end
end
