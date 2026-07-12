defmodule Flezha.State.OpInbox do
  @moduledoc """
  Per-instance durable inbox for a durable bounded operative
  (`bounded-durable-suspension`).

  One `inbox.wal` per instance at `<state_root>/<instance_id>/<instance_id>/
  inbox.wal`, written through the same `WalWriter` framing + `FsyncBatcher`
  path as the sibling `op.journal` under the reserved identity
  `Registry.inbox_id/1` — no parallel persistence machinery. Every post-start
  input is appended here **before** it is forwarded to the executor, so an
  input that arrives while the instance is suspended, crashed, or migrating is
  delivered on rehydrate instead of being dropped.

  ```
  header: [0: 128 bits]
  body:   [dedup_len: u16 BE][dedup_id][meta_len: u32 BE][meta][payload]
  ```

  Offsets are the record's WAL seq **plus one**, so they run from 1 and `0` is
  free to mean "nothing consumed yet" (the journal's floor before the first
  iteration commit) and "not journaled" (an opted-out class's memory-only
  inbox). They are monotonic and survive truncation. `meta` is the forwarded
  frame's metadata map, so a rehydrated delivery is byte-identical to the
  original.

  ## Dedup and truncation

  `dedup_id` is the sender-supplied message id (`flz.msg_id` metadata) when
  present, else a SHA-256 of the payload. An append whose dedup id already
  exists in the **undelivered window** — the entries above the consumed offset
  the op-journal's latest `iter_committed` recorded — is dropped and answers
  with the existing entry's offset, so a re-sent message is absorbed rather
  than delivered twice.

  Iteration commit truncates everything at or below the consumed offset
  (`truncate/2` → `WalWriter.truncate_up_to/2`, which preserves the surviving
  records' seqs). Truncation is advisory cleanup: a lost truncate costs disk,
  never correctness, because delivery is filtered by the same consumed offset
  (`undelivered/1`).
  """

  alias Flezha.State.{OpJournal, RecordLog, Registry, Supervisor, WalWriter}

  # Inbox records carry no wire header; the 16 zero bytes keep the WalWriter
  # record framing (and its scan/rewrite helpers) reusable as-is.
  @zero_header <<0::unit(8)-size(16)>>

  @msg_id_key "flz.msg_id"

  @type entry :: %{
          offset: non_neg_integer(),
          dedup_id: binary(),
          message: binary(),
          metadata: map()
        }

  @doc """
  Append one post-start input, durable (fsync) before returning, and answer its
  offset. A duplicate dedup id in the undelivered window is dropped and answers
  `{:duplicate, existing_offset}` — exactly one entry exists and the operative
  receives the message once.
  """
  @spec append(binary(), binary(), map()) ::
          {:ok, non_neg_integer()} | {:duplicate, non_neg_integer()} | {:error, term()}
  def append(instance_id, message, metadata)
      when is_binary(instance_id) and is_binary(message) and is_map(metadata) do
    dedup_id = dedup_id(message, metadata)

    case Enum.find(undelivered(instance_id), &(&1.dedup_id == dedup_id)) do
      %{offset: offset} ->
        {:duplicate, offset}

      nil ->
        body = encode_entry(dedup_id, message, metadata)

        case RecordLog.append(Registry.inbox_id(instance_id), @zero_header, body) do
          {:ok, seq} -> {:ok, offset(seq)}
          {:error, _} = err -> err
        end
    end
  end

  @doc """
  The entries the instance has not consumed yet, in offset order: everything
  above the consumed offset the op-journal's latest `iter_committed` recorded.
  This is both the dedup window and the delivery set a rehydrate re-forwards.
  """
  @spec undelivered(binary()) :: [entry()]
  def undelivered(instance_id) when is_binary(instance_id) do
    floor = OpJournal.consumed_offset(instance_id)
    Enum.filter(read(instance_id), &(&1.offset > floor))
  end

  @doc """
  Decode every complete entry, in append order. `[]` when the instance has no
  inbox. Reads the file directly (no writer needed); stops at the first invalid
  record, mirroring the WAL torn-tail contract.
  """
  @spec read(binary()) :: [entry()]
  def read(instance_id) when is_binary(instance_id) do
    RecordLog.read(Registry.inbox_path(instance_id), fn seq, _header, body ->
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
  Release every entry at or below `offset` (iteration commit). Idempotent, and
  a no-op when the instance has no inbox — an instance that never received
  post-start input never starts an inbox writer.
  """
  @spec truncate(binary(), non_neg_integer()) :: :ok
  def truncate(instance_id, offset) when is_binary(instance_id) and offset >= 0 do
    if File.exists?(Registry.inbox_path(instance_id)) do
      with {:ok, pid} <- Supervisor.find_or_start(Registry.inbox_id(instance_id)) do
        WalWriter.truncate_up_to(pid, seq(offset))
      end
    end

    :ok
  end

  # Offset ⇄ WAL seq. The substrate requires the first record to carry seq 0
  # (an empty WAL receiving any other seq sheds the frame and pulls a
  # baseline), while the offset space must reserve 0 as its "none" value.
  defp offset(seq), do: seq + 1
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

  # The sender's message id when it supplied one; otherwise a payload hash, which
  # absorbs an accidental re-send but cannot tell two legitimately identical
  # payloads apart (documented — such a sender must stamp `flz.msg_id`).
  defp dedup_id(message, metadata) do
    case Map.get(metadata, @msg_id_key) do
      id when is_binary(id) and id != "" -> id
      _ -> :crypto.hash(:sha256, message)
    end
  end
end
