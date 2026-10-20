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
  cold start), `terminal_seq = -1` and both bodies are empty. `flz state at`
  reads the same payload as of an earlier seq (`Flezha.State.TimeTravel`).
  """

  alias Flezha.Relay.ClassRegistry
  alias Flezha.State.Frame
  alias Flezha.State.{Paths, Snapshot}

  @type payload :: %{
          snapshot_bytes: binary(),
          snapshot_header: binary(),
          change_log_bytes: binary(),
          terminal_seq: integer()
        }

  @doc """
  The newest snapshot at or before `upto` (`:latest` or a seq) with the header it
  was stored with, the WAL records above it through `upto` as they lie on disk,
  and the highest seq the two reach (`-1` for none).
  """
  @spec payload(Paths.cell_id(), :latest | non_neg_integer()) ::
          {:ok, payload()} | {:error, binary()}
  def payload(cell_id, upto) do
    # `-1` (not `0`) is the "no snapshot" sentinel: the WAL is 0-based, so a
    # `0` sentinel with the `seq > snapshot_seq` tail filter would silently
    # drop the very first change record. An atom sorts above every integer,
    # so `:latest` passes every snapshot and record.
    snaps = for {seq, _path} = snap <- Paths.snapshot_paths(cell_id), seq <= upto, do: snap

    {snapshot_bytes, snapshot_header, snapshot_seq} =
      with {seq, path} <- List.last(snaps),
           {:ok, header, bytes} <- Snapshot.read_snapshot(path) do
        {bytes, header, seq}
      else
        _none_or_unreadable -> {<<>>, <<>>, -1}
      end

    with {:ok, {change_log_bytes, terminal_seq}} <- read_tail_wal(cell_id, snapshot_seq, upto) do
      {:ok,
       %{
         snapshot_bytes: snapshot_bytes,
         snapshot_header: snapshot_header,
         change_log_bytes: change_log_bytes,
         terminal_seq: max(snapshot_seq, terminal_seq)
       }}
    end
  end

  @doc """
  Whether the Cell has any persisted state to restore. A zero-byte WAL
  (lazily created by the writer before any record landed) is no state: an
  assembled payload from it would be empty and rebuild a live Cell to
  `init_state`.
  """
  @spec has_persisted_state?(Paths.cell_id()) :: boolean()
  def has_persisted_state?(cell_id) do
    Snapshot.latest_snapshot(cell_id) != nil or
      match?(
        {:ok, %{size: size}} when size > 0,
        File.stat(Paths.wal_path(cell_id))
      )
  end

  @doc """
  The SDK language byte stamped in this Cell's durable state, or `nil` when
  it has none.

  Byte 0 of the newest snapshot header, else of the first WAL record's. The
  authority on which SDK wrote a Cell's state, and what the placement guard
  resolves a factory's established language from. A routing decision, not a
  validation: the body is never touched and an unknown value is reported as
  found.
  """
  @spec sdk_lang(Paths.cell_id()) :: byte() | nil
  def sdk_lang(cell_id) do
    with {_seq, path} <- Snapshot.latest_snapshot(cell_id),
         {:ok, header, _bytes} <- Snapshot.read_snapshot(path),
         {:ok, %{sdk_lang: lang}} <- Frame.decode_header(header) do
      lang
    else
      _ -> first_wal_lang(cell_id)
    end
  end

  @doc """
  The SDK language stamped across `relay_class`'s durable state, or `nil`
  when the class has none on disk.

  The first Cell that answers wins: a class is written by one SDK, so a
  second read would only confirm the first.
  """
  @spec class_sdk_lang(binary()) :: byte() | nil
  def class_sdk_lang(relay_class) when is_binary(relay_class) do
    relay_class
    |> Paths.cells_of()
    |> Enum.find_value(&sdk_lang/1)
  end

  # The WAL's first record carries the same header the snapshot does; the frame
  # seam cuts both, so neither layout is restated here.
  defp first_wal_lang(cell_id) do
    with {:ok, data} <- :file.read_file(Paths.wal_path(cell_id)),
         {:ok, _seq, header, _body, _raw, _consumed} <- Frame.decode_bytes(data),
         {:ok, %{sdk_lang: lang}} <- Frame.decode_header(header) do
      lang
    else
      _ -> nil
    end
  end

  # Read the WAL once and fold the records in memory; a per-record
  # `:file.pread` churns small transient refc binaries that fragment the binary
  # allocator.
  defp read_tail_wal(cell_id, snapshot_seq, upto) do
    path = Paths.wal_path(cell_id)

    case :file.read_file(path) do
      {:ok, data} ->
        data
        |> Frame.fold_bytes(
          {:log, path},
          {[], snapshot_seq},
          &tail_record(&1, &2, snapshot_seq, upto)
        )
        |> case do
          {:ok, {acc, term_seq}} -> {:ok, {change_log(acc), term_seq}}
          {:error, refusal} -> {:error, Frame.refusal_message(path, refusal)}
        end

      {:error, _} ->
        {:ok, {<<>>, snapshot_seq}}
    end
  end

  # Keep the on-disk bytes verbatim rather than re-framing them: they are
  # shipped to the SDK as they lie.
  defp tail_record({seq, _header, _body, _raw}, acc, _snapshot_seq, upto) when seq > upto,
    do: {:halt, acc}

  defp tail_record({seq, _header, _body, raw}, {acc, term_seq}, snapshot_seq, _upto) do
    acc = if seq > snapshot_seq, do: [raw | acc], else: acc
    {:cont, {acc, max(term_seq, seq)}}
  end

  defp change_log(acc), do: acc |> Enum.reverse() |> IO.iodata_to_binary()

  @doc """
  Establish the language of `factory_id` in `namespace` from the bytes
  `relay_class` actually wrote, correcting a value some runner established
  by advertising first.

  The header is the authority wherever durable state exists. A class with
  nothing on disk records nothing. Called from the bind, the one seam holding
  both the factory id and the relay's on-disk class.
  """
  @spec establish_sdk_lang_from_state(binary(), binary(), binary()) :: :ok
  def establish_sdk_lang_from_state(namespace, factory_id, relay_class)
      when is_binary(namespace) and is_binary(factory_id) and is_binary(relay_class) do
    case class_sdk_lang(relay_class) do
      nil ->
        :ok

      lang ->
        if ClassRegistry.established(namespace, factory_id, :sdk_lang) == lang,
          do: :ok,
          else: ClassRegistry.put_established(namespace, factory_id, :sdk_lang, lang)
    end
  end
end
