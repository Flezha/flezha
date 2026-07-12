defmodule Flezha.State.OpJournal do
  @moduledoc """
  Per-instance append-only op-progress journal for durable structured
  bounded operatives (`bounded-op-journal`).

  One `op.journal` per instance at
  `<state_root>/<instance_id>/<instance_id>/op.journal`, written through the
  existing `WalWriter` framing + `FsyncBatcher` path under the reserved
  identity `Registry.journal_id/1` — no parallel persistence machinery. Each
  WAL record's change-bytes carry one typed journal entry:

  ```
  [type: u8][op_idx: u16 BE][iter: u32 BE][payload]
  ```

  Shipped entry type: `1 = op_committed` (payload = wire segment identities
  that ran + the serialized inter-op handoff batch). Types `2..5`
  (`outbox_flushed`, `iter_committed`, `parked`, `compensated`) are reserved
  for the staged-commit / durable-loop follow-ups: the decoder surfaces them
  with a raw payload instead of crashing. Torn tails are truncated by the
  `WalWriter` open scan; the read path stops at the first invalid record.

  `op_committed` payload:

  ```
  [n_segments: u16 BE] n × ([len: u16 BE][segment_identity_bytes])
  [n_items: u32 BE]    n × ([channel_id: u32 BE][len: u32 BE][payload_bytes])
  ```
  """

  alias Flezha.State.{Registry, Supervisor, WalWriter}

  @type_op_committed 1
  @reserved_types %{
    2 => :outbox_flushed,
    3 => :iter_committed,
    4 => :parked,
    5 => :compensated
  }

  @zero_header <<0::128>>

  @type handoff_item :: {channel_id :: non_neg_integer(), payload :: binary()}

  @type entry :: %{
          seq: non_neg_integer(),
          type: :op_committed | :outbox_flushed | :iter_committed | :parked | :compensated,
          op_idx: non_neg_integer(),
          iter: non_neg_integer(),
          payload:
            %{segments_ran: [binary()], handoff: [handoff_item()]}
            | binary()
        }

  @doc """
  Append one `op_committed` entry and make it durable (fsync) before
  returning — the caller replies `OpCommitResponse` only on `:ok`. An entry
  whose encoded body exceeds `max_entry_bytes/0` is refused with
  `{:error, {:oversized_handoff, size, max}}` (keep bulk data in Segment
  state, which is already WAL-durable, and hand off references).
  """
  @spec append_op_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [binary()],
          [handoff_item()]
        ) :: :ok | {:error, term()}
  def append_op_committed(instance_id, op_idx, iter, segments_ran, handoff)
      when is_binary(instance_id) and is_integer(op_idx) and op_idx >= 0 do
    body = encode_entry(@type_op_committed, op_idx, iter, encode_payload(segments_ran, handoff))
    max = max_entry_bytes()

    if byte_size(body) > max do
      {:error, {:oversized_handoff, byte_size(body), max}}
    else
      with {:ok, pid} <- Supervisor.find_or_start(Registry.journal_id(instance_id)),
           seq = WalWriter.last_persisted_seq(pid) + 1,
           :ok <- WalWriter.append(pid, seq, @zero_header, body) do
        WalWriter.fsync(pid)
      else
        {:error, _} = err -> err
        other -> {:error, {:journal_append_failed, other}}
      end
    end
  end

  @doc """
  Decode every complete entry, in append order. `{:ok, []}` when the
  instance has no journal. Reads the file directly (no writer needed);
  stops at the first invalid record, mirroring the WAL torn-tail contract.
  """
  @spec read(binary()) :: {:ok, [entry()]}
  def read(instance_id) when is_binary(instance_id) do
    case :file.open(Registry.journal_path(instance_id), [:raw, :read, :binary]) do
      {:ok, device} ->
        try do
          {:ok, read_entries(device, 0, [])}
        after
          :file.close(device)
        end

      {:error, _} ->
        {:ok, []}
    end
  end

  @doc """
  Fold the journal into the crash-resume state shipped on
  `RestoreInitState.op_journal`: the committed op indexes plus the latest
  committed entry's handoff (`nil` when no `op_committed` entry exists).
  """
  @spec restore_state(binary()) ::
          %{
            committed_op_idxs: [non_neg_integer()],
            iter: non_neg_integer(),
            handoff: [handoff_item()]
          }
          | nil
  def restore_state(instance_id) do
    {:ok, entries} = read(instance_id)

    entries
    |> Enum.filter(&(&1.type == :op_committed))
    |> case do
      [] ->
        nil

      committed ->
        latest = Enum.max_by(committed, & &1.seq)

        %{
          committed_op_idxs: committed |> Enum.map(& &1.op_idx) |> Enum.uniq() |> Enum.sort(),
          iter: latest.iter,
          handoff: latest.payload.handoff
        }
    end
  end

  @doc """
  Journal entry size bound — the WAL soft watermark scale
  (`:wal_high_watermark_bytes`, default 16 MiB).
  """
  @spec max_entry_bytes() :: pos_integer()
  def max_entry_bytes() do
    Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)
  end

  # ---------- Entry codec ----------

  @doc false
  @spec encode_entry(pos_integer(), non_neg_integer(), non_neg_integer(), iodata()) :: binary()
  def encode_entry(type, op_idx, iter, payload)
      when type in 1..255 and op_idx in 0..0xFFFF and iter in 0..0xFFFF_FFFF do
    IO.iodata_to_binary([<<type::8, op_idx::16-big, iter::32-big>>, payload])
  end

  @doc false
  @spec decode_entry(non_neg_integer(), binary()) :: {:ok, entry()} | {:error, term()}
  def decode_entry(seq, <<type::8, op_idx::16-big, iter::32-big, payload::binary>>) do
    entry = %{seq: seq, op_idx: op_idx, iter: iter}

    cond do
      type == @type_op_committed ->
        with {:ok, decoded} <- decode_payload(payload) do
          {:ok, Map.merge(entry, %{type: :op_committed, payload: decoded})}
        end

      Map.has_key?(@reserved_types, type) ->
        {:ok, Map.merge(entry, %{type: @reserved_types[type], payload: payload})}

      true ->
        {:error, {:unknown_entry_type, type}}
    end
  end

  def decode_entry(_seq, _short), do: {:error, :short_entry}

  defp encode_payload(segments_ran, handoff) do
    [
      <<length(segments_ran)::16-big>>,
      Enum.map(segments_ran, fn id -> [<<byte_size(id)::16-big>>, id] end),
      <<length(handoff)::32-big>>,
      Enum.map(handoff, fn {channel_id, bytes} ->
        [<<channel_id::32-big, byte_size(bytes)::32-big>>, bytes]
      end)
    ]
  end

  defp decode_payload(<<n_segments::16-big, rest::binary>>) do
    with {:ok, segments, rest} <- decode_segments(rest, n_segments, []),
         <<n_items::32-big, rest::binary>> <- rest,
         {:ok, handoff, <<>>} <- decode_handoff(rest, n_items, []) do
      {:ok, %{segments_ran: segments, handoff: handoff}}
    else
      {:error, _} = err -> err
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(_), do: {:error, :bad_payload}

  defp decode_segments(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_segments(<<len::16-big, id::binary-size(len), rest::binary>>, n, acc),
    do: decode_segments(rest, n - 1, [id | acc])

  defp decode_segments(_, _, _), do: {:error, :bad_payload}

  defp decode_handoff(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_handoff(
         <<channel_id::32-big, len::32-big, bytes::binary-size(len), rest::binary>>,
         n,
         acc
       ),
       do: decode_handoff(rest, n - 1, [{channel_id, bytes} | acc])

  defp decode_handoff(_, _, _), do: {:error, :bad_payload}

  # ---------- Read scan ----------

  defp read_entries(device, position, acc) do
    case WalWriter.decode_record(device, position) do
      {:ok, seq, _header, body, consumed} ->
        case decode_entry(seq, body) do
          {:ok, entry} -> read_entries(device, position + consumed, [entry | acc])
          # An undecodable body is skipped, not fatal: forward-compatibility
          # with entry shapes this build does not know.
          {:error, _} -> read_entries(device, position + consumed, acc)
        end

      {:error, _} ->
        Enum.reverse(acc)
    end
  end
end
