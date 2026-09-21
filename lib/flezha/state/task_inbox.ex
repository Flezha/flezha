defmodule Flezha.State.TaskInbox do
  @moduledoc """
  Per-instance durable inbox for a durable workflow operative.

  One `inbox.wal` per instance, written through the same `WalWriter` framing +
  `FsyncBatcher` path as the sibling `task.journal` under the reserved identity
  `Paths.inbox_id/1`. Every post-start input is appended here **before** it is
  forwarded to the runner, so one arriving while the instance is suspended,
  crashed or migrating is delivered on rehydrate rather than dropped. The record
  layout is in `docs/internals/state_persistence.md`.

  Offsets are the record's WAL seq **plus one**, so `0` is free to mean "nothing
  consumed yet" and "not journaled"; they are monotonic and survive truncation.
  A message re-sent under the same `flz.msg_id` is absorbed rather than
  delivered twice, the record header being a digest of that id; a message
  without one is always appended, as a stream processes every frame it is
  sent. Truncation is advisory cleanup — a lost truncate costs disk and a wider
  dedup window, never correctness, because delivery is filtered by the
  journal's consumed offset.
  """

  alias Flezha.State.{
    Paths,
    PersistenceResolver,
    TaskJournal,
    RecordLog,
    Supervisor,
    WalWriter
  }

  @append_opts [sync: :batched]

  @msg_id_key "flz.msg_id"

  @type entry :: %{
          offset: non_neg_integer(),
          dedup_id: binary(),
          message: binary(),
          metadata: map()
        }

  @doc """
  Append one post-start input, durable before returning, and answer its offset.
  A duplicate `flz.msg_id` in the undelivered window is dropped and answers
  `{:duplicate, existing_offset}` — exactly one entry exists and the operative
  receives the message once.
  """
  @spec append(binary(), binary(), map()) ::
          {:ok, non_neg_integer()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def append(instance_id, message, metadata)
      when is_binary(instance_id) and is_binary(message) and is_map(metadata) do
    {header, body, opts} = encode(message, metadata)

    case RecordLog.append(Paths.inbox_id(instance_id), header, body, opts) do
      {:ok, seq} -> {:ok, offset(seq)}
      {:duplicate, seq} -> {:duplicate, offset(seq)}
      {:error, _} = err -> err
    end
  end

  @doc """
  `append/3` without waiting: issued with `:gen_server.send_request/4` under
  `label` into `reqids`. The answer — `{:ok, seq}` or `{:duplicate, seq}` in the
  writer's seq space (`offset/1` maps it), or the writer's exit — comes back as
  a message for `:gen_server.check_response/3`, once the batch holding the
  record has synced.
  """
  @spec append_async(binary(), binary(), map(), :gen_server.request_id_collection(), term()) ::
          :gen_server.request_id_collection()
  def append_async(instance_id, message, metadata, reqids, label)
      when is_binary(instance_id) and is_binary(message) and is_map(metadata) do
    {header, body, opts} = encode(message, metadata)

    RecordLog.append_async(Paths.inbox_id(instance_id), header, body, opts, reqids, label)
  end

  # The writer's dedup window is consulted only for a message that carries an
  # id: one without is appended whatever came before it.
  defp encode(message, metadata) do
    dedup_id = dedup_id(metadata)
    opts = Keyword.put(@append_opts, :dedup, dedup_id != "")
    {header(dedup_id), encode_entry(dedup_id, message, metadata), opts}
  end

  # The record header is the dedup id's digest, so the writer refuses a
  # duplicate — and rebuilds its window — from headers alone.
  defp header(dedup_id), do: binary_part(:crypto.hash(:sha256, dedup_id), 0, 16)

  @doc """
  The entries the instance has not consumed yet, in offset order: everything
  above the offset the task-journal has released (`TaskJournal.released_offset/1`
  — the highest any iteration ever consumed, which a rewind does not lower and
  which a deferred file rewrite does not move). The delivery set a rehydrate
  re-forwards.
  """
  @spec undelivered(binary()) :: [entry()]
  def undelivered(instance_id) when is_binary(instance_id) do
    floor = TaskJournal.released_offset(instance_id)
    Enum.filter(read(instance_id), &(&1.offset > floor))
  end

  @doc """
  Decode every complete entry, in append order. `[]` when the instance has no
  inbox. Reads the file directly (no writer needed); stops at the first invalid
  record, mirroring the WAL torn-tail contract.
  """
  @spec read(binary()) :: [entry()]
  def read(instance_id) when is_binary(instance_id) do
    RecordLog.read(Paths.inbox_path(instance_id), fn seq, _header, body ->
      decode_entry(offset(seq), body)
    end)
  end

  @doc "Highest appended offset, `0` when the inbox is empty or absent."
  @spec last_offset(binary()) :: non_neg_integer()
  def last_offset(instance_id) when is_binary(instance_id) do
    case read(instance_id) do
      [] -> 0
      entries -> List.last(entries).offset
    end
  end

  @doc """
  Release every entry at or below `offset` (iteration commit): the dedup window
  drops them at once, the file once
  `PersistenceResolver.record_log_cut_after_bytes/0` of released entries have
  accumulated. The cut is the inbox writer's to run — this answers once it is
  asked. Idempotent, and a no-op when the instance has no inbox: an instance
  that never received post-start input never starts an inbox writer.
  """
  @spec truncate(binary(), non_neg_integer()) :: :ok
  def truncate(instance_id, offset) when is_binary(instance_id) and offset >= 0 do
    if File.exists?(Paths.inbox_path(instance_id)) do
      with {:ok, pid} <- Supervisor.find_or_start(Paths.inbox_id(instance_id)) do
        min_bytes = PersistenceResolver.record_log_cut_after_bytes()
        WalWriter.truncate_up_to_async(pid, seq(offset), min_bytes: min_bytes)
      end
    end

    :ok
  end

  @doc """
  Offset ⇄ WAL seq. The substrate requires the first record to carry seq 0
  (an empty WAL receiving any other seq sheds the frame and pulls a
  baseline), while the offset space must reserve 0 as its "none" value.
  """
  @spec offset(non_neg_integer()) :: pos_integer()
  def offset(seq), do: seq + 1

  defp seq(offset), do: offset - 1

  # ---------- Entry codec ----------

  @doc false
  @spec encode_entry(binary(), binary(), map()) :: binary()
  def encode_entry(dedup_id, message, metadata) do
    meta = :erlang.term_to_binary(metadata)

    IO.iodata_to_binary([
      <<byte_size(dedup_id)::16-big>>,
      dedup_id,
      <<byte_size(meta)::32-big>>,
      meta,
      message
    ])
  end

  @doc false
  @spec decode_entry(non_neg_integer(), binary()) :: {:ok, entry()} | {:error, term()}
  def decode_entry(
        offset,
        <<dedup_len::16-big, dedup_id::binary-size(dedup_len), meta_len::32-big,
          meta::binary-size(meta_len), message::binary>>
      ) do
    {:ok,
     %{
       offset: offset,
       dedup_id: dedup_id,
       message: message,
       metadata: :erlang.binary_to_term(meta)
     }}
  rescue
    ArgumentError -> {:error, :bad_metadata}
  end

  def decode_entry(_offset, _short), do: {:error, :short_entry}

  # The sender's message id when it supplied one, else empty: two identical
  # payloads without ids are two messages.
  defp dedup_id(metadata) do
    case Map.get(metadata, @msg_id_key) do
      id when is_binary(id) -> id
      _ -> ""
    end
  end
end
