defmodule Flezha.State.OpJournal do
  @moduledoc """
  Per-instance append-only op-progress journal for durable structured
  bounded operatives (`bounded-op-journal`) and their transactional outbox
  (`bounded-transactional-outbox`).

  One `op.journal` per instance at
  `<state_root>/<instance_id>/<instance_id>/op.journal`, written through the
  existing `WalWriter` framing + `FsyncBatcher` path under the reserved
  identity `Registry.journal_id/1` — no parallel persistence machinery. Each
  WAL record's change-bytes carry one typed journal entry, and its 16-byte
  record header carries the instance epoch:

  ```
  header: [epoch: u32 BE][0: 96 bits]
  body:   [type: u8][op_idx: u16 BE][iter: u32 BE][payload]
  ```

  Entry types: `1 = op_committed` (the segments that ran, the serialized
  inter-op handoff, and the operation's staged outbox — ONE durable write),
  `2 = outbox_flushed` (that outbox reached the plugins),
  `3 = iter_committed` (a loop iteration finished: the inbox offset it
  consumed plus its terminal handoff — `bounded-durable-loop`),
  `4 = parked` (the loop is suspended at a committed boundary, with an
  optional wake deadline — `bounded-durable-suspension`), `5 = compensated`
  (a compensation hook's undo outbox). Torn tails are truncated by the
  `WalWriter` open scan; the read path stops at the first invalid record.

  Payloads:

  ```
  op_committed:   [n_segments: u16 BE] n × ([len: u16 BE][segment_identity])
                  [handoff]
                  [outbox]
  iter_committed: [consumed_offset: u64 BE][handoff]
  parked:         [wake_deadline_ms: u64 BE]   # 0 = no deadline
  compensated:    [outbox]
  outbox_flushed: [for_type: u8]   # which entry type's outbox was flushed
  handoff:        [n: u32 BE] n × ([channel_id: u32 BE][len: u32 BE][payload])
  outbox:         [n: u32 BE] n × ([len: u16 BE][segment_identity][seq_in_op: u32 BE]
                                   [len: u16 BE][channel][len: u32 BE][payload])
  ```

  The `iter` field of every entry is the enclosing loop iteration (0 for a
  non-looping chain), so redo-vs-skip works at both the operation and the
  iteration granularity from one scan.

  ## Flush

  An operation's plugin-bound output is staged in the executor and journaled
  WITH its commit, so progress and outbox cannot tear apart. Delivery happens
  only after that fsync, through the same `:on_output` fan-out an unbounded
  operative's effectively-once outbox uses, stamped with the deterministic key
  `(segment, op_idx, seq_in_op, epoch)`. `flush_pending/1` is the single flush
  path — driven post-ack by the `ControlChannel` and again on restore, it reads
  the journal, delivers every entry with no matching `outbox_flushed`, and
  records the flush. Journal state (not memory) decides what is owed, so a
  crash between commit and flush re-drives with identical keys.
  """

  require Logger

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{RecordLog, Registry, Wire}

  @type_op_committed 1
  @type_outbox_flushed 2
  @type_iter_committed 3
  @type_parked 4
  @type_compensated 5
  @entry_types %{
    @type_op_committed => :op_committed,
    @type_outbox_flushed => :outbox_flushed,
    @type_iter_committed => :iter_committed,
    @type_parked => :parked,
    @type_compensated => :compensated
  }

  @type outbox_entry :: %{
          segment_id: binary(),
          seq_in_op: non_neg_integer(),
          channel: binary(),
          payload: binary()
        }

  @type handoff_item :: {channel_id :: non_neg_integer(), payload :: binary()}

  @type entry :: %{
          seq: non_neg_integer(),
          epoch: non_neg_integer(),
          type: :op_committed | :outbox_flushed | :iter_committed | :parked | :compensated,
          op_idx: non_neg_integer(),
          iter: non_neg_integer(),
          payload:
            %{segments_ran: [binary()], handoff: [handoff_item()], outbox: [outbox_entry()]}
            | %{consumed_offset: non_neg_integer(), handoff: [handoff_item()]}
            | %{wake_deadline_ms: non_neg_integer() | nil}
            | %{outbox: [outbox_entry()]}
            | %{for_type: non_neg_integer()}
        }

  @doc """
  Append one `op_committed` entry — segments ran, inter-op handoff, and the
  operation's staged outbox in ONE record — and make it durable (fsync) before
  returning; the caller replies `OpCommitResponse` only on `:ok`. An entry whose
  encoded body exceeds `max_entry_bytes/0` is refused with
  `{:error, {:oversized_handoff, size, max}}` (keep bulk data in Segment state,
  which is already WAL-durable, and hand off references).
  """
  @spec append_op_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [binary()],
          [handoff_item()],
          [outbox_entry()]
        ) :: :ok | {:error, term()}
  def append_op_committed(instance_id, op_idx, iter, segments_ran, handoff, outbox \\ [])
      when is_binary(instance_id) and is_integer(op_idx) and op_idx >= 0 do
    append(
      instance_id,
      @type_op_committed,
      op_idx,
      iter,
      [encode_committed_payload(segments_ran, handoff), encode_outbox(outbox)]
    )
  end

  @doc """
  Append one `iter_committed` entry — loop iteration `iter` finished, having
  consumed the inbox up to `consumed_offset` and released `handoff` as its
  terminal batch — durable before the reply. The commit boundary a crash-resume
  restarts after: iterations at or below `iter` never re-run.
  """
  @spec append_iter_committed(binary(), non_neg_integer(), non_neg_integer(), [handoff_item()]) ::
          :ok | {:error, term()}
  def append_iter_committed(instance_id, iter, consumed_offset, handoff)
      when is_binary(instance_id) and is_integer(iter) and iter >= 0 do
    append(instance_id, @type_iter_committed, 0, iter, [
      <<consumed_offset::64-big>>,
      encode_handoff(handoff)
    ])
  end

  @doc """
  Append one `parked` entry — the loop is suspended at the committed boundary of
  iteration `iter`, to be rehydrated by a new inbox append or, when
  `wake_deadline_ms` is non-nil, by its durable timer. Durable before the
  instance is evicted, so suspension survives a controller redeploy.
  """
  @spec append_parked(binary(), non_neg_integer(), non_neg_integer() | nil) ::
          :ok | {:error, term()}
  def append_parked(instance_id, iter, wake_deadline_ms \\ nil)
      when is_binary(instance_id) and is_integer(iter) and iter >= 0 do
    append(instance_id, @type_parked, 0, iter, <<wake_deadline_ms || 0::64-big>>)
  end

  @doc """
  Append one `compensated` entry carrying a compensation hook's undo outbox,
  durable before the reply. Its outputs are flushed with comp-marked keys, so
  an undo can never collide with the forward output it reverses.
  """
  @spec append_compensated(binary(), non_neg_integer(), non_neg_integer(), [outbox_entry()]) ::
          :ok | {:error, term()}
  def append_compensated(instance_id, op_idx, iter, outbox) when is_binary(instance_id) do
    append(instance_id, @type_compensated, op_idx, iter, encode_outbox(outbox))
  end

  @doc """
  Deliver every journaled outbox entry this instance still owes, in journal
  order, and record each delivery with an `outbox_flushed` entry.

  Idempotent and driven purely by journal state: an entry whose flush is already
  recorded is never re-delivered, and one whose flush is not is re-driven with
  the original keys. Returns `{:error, :no_overwatch}` when the instance is not
  (yet) bound — the caller retries; nothing is dropped. `overwatch` may be
  supplied by a caller that already holds the pid (the restore re-drive, which
  runs off the Overwatch itself).
  """
  @spec flush_pending(binary(), pid() | nil) :: :ok | {:error, term()}
  def flush_pending(instance_id, overwatch \\ nil) when is_binary(instance_id) do
    case pending_entries(instance_id) do
      [] ->
        :ok

      pending ->
        with {:ok, pid} <- resolve_overwatch(instance_id, overwatch) do
          Enum.each(pending, &flush_entry(instance_id, pid, &1))
        end
    end
  end

  defp resolve_overwatch(_instance_id, pid) when is_pid(pid), do: {:ok, pid}

  defp resolve_overwatch(instance_id, nil) do
    case ClassRegistry.overwatch_pid(instance_id) do
      {:ok, pid} -> {:ok, pid}
      :error -> {:error, :no_overwatch}
    end
  end

  @doc """
  The entries whose outbox is journaled but not yet recorded flushed, in
  journal order. An entry carrying no output needs no delivery and is not
  pending.
  """
  @spec pending_entries(binary()) :: [entry()]
  def pending_entries(instance_id) when is_binary(instance_id) do
    {:ok, entries} = read(instance_id)

    flushed =
      for %{type: :outbox_flushed, op_idx: op_idx, iter: iter, payload: %{for_type: for_type}} <-
            entries,
          into: MapSet.new(),
          do: {for_type, op_idx, iter}

    Enum.filter(entries, fn entry ->
      entry.type in [:op_committed, :compensated] and
        entry.payload.outbox != [] and
        not MapSet.member?(
          flushed,
          {entry_type_code(entry.type), entry.op_idx, entry.iter}
        )
    end)
  end

  @doc """
  Decode every complete entry, in append order. `{:ok, []}` when the
  instance has no journal. Reads the file directly (no writer needed);
  stops at the first invalid record, mirroring the WAL torn-tail contract.
  """
  @spec read(binary()) :: {:ok, [entry()]}
  def read(instance_id) when is_binary(instance_id) do
    entries =
      RecordLog.read(Registry.journal_path(instance_id), fn seq, <<epoch::32-big, _::96>>, body ->
        decode_entry(seq, body, epoch)
      end)

    {:ok, entries}
  end

  @doc """
  Fold the journal into the crash-resume state shipped on
  `RestoreInitState.op_journal` (`nil` when the instance has no journal).

  The resume point is the iteration **after** the highest `iter_committed`:
  `committed_op_idxs` are the operations already committed inside that
  still-in-flight iteration (skipped on resume) and `handoff` seeds the first
  uncommitted one — falling back, when the iteration has not committed an
  operation yet, to the previous iteration's terminal handoff (what a
  handoff-chained loop feeds its next iteration). `committed_ops` carries every
  forward op commit with its iteration, which is what the driver folds into
  `plan_compensation`'s per-iteration `committed` input.
  """
  @spec restore_state(binary()) ::
          %{
            committed_op_idxs: [non_neg_integer()],
            iter: non_neg_integer(),
            handoff: [handoff_item()],
            committed_ops: [
              %{op_idx: non_neg_integer(), iter: non_neg_integer(), segments_ran: [binary()]}
            ],
            parked: boolean(),
            wake_deadline_ms: non_neg_integer() | nil,
            consumed_offset: non_neg_integer()
          }
          | nil
  def restore_state(instance_id) do
    {:ok, entries} = read(instance_id)

    case entries do
      [] ->
        nil

      _ ->
        last_iter = latest_iter_committed(entries)
        resume_iter = if last_iter, do: last_iter.iter + 1, else: 0

        in_flight =
          Enum.filter(entries, &(&1.type == :op_committed and &1.iter == resume_iter))

        %{
          committed_op_idxs: in_flight |> Enum.map(& &1.op_idx) |> Enum.uniq() |> Enum.sort(),
          iter: resume_iter,
          handoff: resume_handoff(in_flight, last_iter),
          committed_ops:
            for %{type: :op_committed} = e <- entries do
              %{op_idx: e.op_idx, iter: e.iter, segments_ran: e.payload.segments_ran}
            end,
          parked: match?(%{type: :parked}, List.last(entries)),
          wake_deadline_ms: wake_deadline(List.last(entries)),
          consumed_offset: consumed_offset_of(last_iter)
        }
    end
  end

  @doc """
  The inbox offset this instance has durably consumed — the one recorded by the
  latest `iter_committed`, `0` before the first iteration commits. The floor for
  both inbox truncation and rehydrate delivery.
  """
  @spec consumed_offset(binary()) :: non_neg_integer()
  def consumed_offset(instance_id) when is_binary(instance_id) do
    {:ok, entries} = read(instance_id)
    entries |> latest_iter_committed() |> consumed_offset_of()
  end

  defp latest_iter_committed(entries) do
    entries
    |> Enum.filter(&(&1.type == :iter_committed))
    |> case do
      [] -> nil
      committed -> Enum.max_by(committed, & &1.seq)
    end
  end

  defp consumed_offset_of(nil), do: 0
  defp consumed_offset_of(%{payload: %{consumed_offset: offset}}), do: offset

  defp wake_deadline(%{type: :parked, payload: %{wake_deadline_ms: ms}}), do: ms
  defp wake_deadline(_), do: nil

  # Seed for the resumed operation: the latest op commit of the in-flight
  # iteration, else the previous iteration's terminal handoff (a fresh
  # iteration's input for a handoff-chained loop; ignored by an inbox-driven
  # one, which re-seeds from the undelivered inbox tail).
  defp resume_handoff([], nil), do: []
  defp resume_handoff([], %{payload: %{handoff: handoff}}), do: handoff

  defp resume_handoff(in_flight, _last_iter),
    do: in_flight |> Enum.max_by(& &1.seq) |> Map.fetch!(:payload) |> Map.fetch!(:handoff)

  @doc """
  The instance's current epoch — part of every flushed output's dedup key.
  `0` until a rewind bumps it; carried verbatim across crash re-drives, so a
  re-driven output keys identically to its original delivery.
  """
  @spec current_epoch(binary()) :: non_neg_integer()
  def current_epoch(instance_id) when is_binary(instance_id) do
    {:ok, entries} = read(instance_id)

    case entries do
      [] -> 0
      _ -> List.last(entries).epoch
    end
  end

  @doc """
  Journal entry size bound — the WAL soft watermark scale
  (`:wal_high_watermark_bytes`, default 16 MiB). Covers the combined
  handoff + outbox payload.
  """
  @spec max_entry_bytes() :: pos_integer()
  def max_entry_bytes() do
    Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024)
  end

  # ---------- Append ----------

  # `epoch` defaults to the instance's current one (a journal read); a caller
  # already holding the entry it is marking passes that entry's epoch instead,
  # which both saves the read and pins the marker to the epoch it flushed.
  defp append(instance_id, type, op_idx, iter, payload, epoch \\ nil) do
    body = encode_entry(type, op_idx, iter, payload)
    max = max_entry_bytes()

    if byte_size(body) > max do
      {:error, {:oversized_handoff, byte_size(body), max}}
    else
      # The epoch rides every record's header, so it survives compaction of any
      # single entry and is readable without interpreting a payload.
      header = <<epoch || current_epoch(instance_id)::32-big, 0::96>>

      case RecordLog.append(Registry.journal_id(instance_id), header, body) do
        {:ok, _seq} -> :ok
        {:error, _} = err -> err
      end
    end
  end

  # ---------- Flush ----------

  # Deliver one journaled entry's outbox through the operative's `:on_output`
  # chain — the same fan-out `StreamChannel` uses for live bounded output and
  # `WalWriter` uses for the unbounded effectively-once outbox — then record the
  # flush. The marker is written only after delivery, so a crash in between
  # re-drives (at-least-once with identical keys) rather than losing output.
  defp flush_entry(instance_id, pid, entry) do
    comp? = entry.type == :compensated

    Enum.each(entry.payload.outbox, fn item ->
      # An undo output's seq carries a trailing `.1` — the compensation marker —
      # so it can never collide with the forward output it reverses while staying
      # a numeric dotted sequence a keyed sink can order.
      seq = "#{entry.op_idx}.#{item.seq_in_op}"

      stamps =
        Wire.output_stamps(%{
          segment: item.segment_id,
          seq: if(comp?, do: seq <> ".1", else: seq),
          epoch: entry.epoch,
          channel: item.channel
        })

      GenServer.cast(pid, {:io_out, item.payload, stamps})
    end)

    :telemetry.execute(
      [:flezha, :state, :op_outbox_flush],
      %{count: 1, entries: length(entry.payload.outbox)},
      %{
        operative_id: instance_id,
        op_idx: entry.op_idx,
        epoch: entry.epoch,
        compensating: comp?
      }
    )

    case append(
           instance_id,
           @type_outbox_flushed,
           entry.op_idx,
           entry.iter,
           <<entry_type_code(entry.type)::8>>,
           entry.epoch
         ) do
      :ok ->
        :ok

      {:error, reason} ->
        # The outbox reached the plugins but the marker did not land: a restore
        # re-drives this entry. Duplicate delivery with an identical key is the
        # documented at-least-once posture; losing the marker write silently is
        # not.
        Logger.warning(
          "[op_journal] outbox_flushed append failed instance=#{inspect(instance_id)} " <>
            "op_idx=#{entry.op_idx}: #{inspect(reason)} — entry will re-drive on restore"
        )
    end
  end

  # ---------- Entry codec ----------

  @doc false
  @spec encode_entry(pos_integer(), non_neg_integer(), non_neg_integer(), iodata()) :: binary()
  def encode_entry(type, op_idx, iter, payload)
      when type in 1..255 and op_idx in 0..0xFFFF and iter in 0..0xFFFF_FFFF do
    IO.iodata_to_binary([<<type::8, op_idx::16-big, iter::32-big>>, payload])
  end

  @doc false
  @spec decode_entry(non_neg_integer(), binary(), non_neg_integer()) ::
          {:ok, entry()} | {:error, term()}
  def decode_entry(seq, body, epoch \\ 0)

  def decode_entry(seq, <<type::8, op_idx::16-big, iter::32-big, payload::binary>>, epoch) do
    entry = %{seq: seq, epoch: epoch, op_idx: op_idx, iter: iter}

    with {:ok, name} <- entry_type(type),
         {:ok, decoded} <- decode_payload(name, payload) do
      {:ok, Map.merge(entry, %{type: name, payload: decoded})}
    end
  end

  def decode_entry(_seq, _short, _epoch), do: {:error, :short_entry}

  defp entry_type(type) do
    case Map.fetch(@entry_types, type) do
      {:ok, name} -> {:ok, name}
      :error -> {:error, {:unknown_entry_type, type}}
    end
  end

  defp entry_type_code(:op_committed), do: @type_op_committed
  defp entry_type_code(:compensated), do: @type_compensated

  defp decode_payload(:op_committed, <<n_segments::16-big, rest::binary>>) do
    with {:ok, segments, rest} <- decode_segments(rest, n_segments, []),
         {:ok, handoff, rest} <- decode_handoff(rest),
         {:ok, outbox, <<>>} <- decode_outbox(rest) do
      {:ok, %{segments_ran: segments, handoff: handoff, outbox: outbox}}
    else
      {:error, _} = err -> err
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(:iter_committed, <<consumed_offset::64-big, rest::binary>>) do
    case decode_handoff(rest) do
      {:ok, handoff, <<>>} -> {:ok, %{consumed_offset: consumed_offset, handoff: handoff}}
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(:parked, <<deadline::64-big>>),
    do: {:ok, %{wake_deadline_ms: if(deadline == 0, do: nil, else: deadline)}}

  defp decode_payload(:compensated, payload) do
    case decode_outbox(payload) do
      {:ok, outbox, <<>>} -> {:ok, %{outbox: outbox}}
      _ -> {:error, :bad_payload}
    end
  end

  defp decode_payload(:outbox_flushed, <<for_type::8>>), do: {:ok, %{for_type: for_type}}

  defp decode_payload(_name, _payload), do: {:error, :bad_payload}

  defp encode_committed_payload(segments_ran, handoff) do
    [
      <<length(segments_ran)::16-big>>,
      Enum.map(segments_ran, fn id -> [<<byte_size(id)::16-big>>, id] end),
      encode_handoff(handoff)
    ]
  end

  defp encode_handoff(handoff) do
    [
      <<length(handoff)::32-big>>,
      Enum.map(handoff, fn {channel_id, bytes} ->
        [<<channel_id::32-big, byte_size(bytes)::32-big>>, bytes]
      end)
    ]
  end

  defp encode_outbox(outbox) do
    [
      <<length(outbox)::32-big>>,
      Enum.map(outbox, fn item ->
        [
          <<byte_size(item.segment_id)::16-big>>,
          item.segment_id,
          <<item.seq_in_op::32-big, byte_size(item.channel)::16-big>>,
          item.channel,
          <<byte_size(item.payload)::32-big>>,
          item.payload
        ]
      end)
    ]
  end

  defp decode_segments(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_segments(<<len::16-big, id::binary-size(len), rest::binary>>, n, acc),
    do: decode_segments(rest, n - 1, [id | acc])

  defp decode_segments(_, _, _), do: {:error, :bad_payload}

  defp decode_handoff(<<n::32-big, rest::binary>>), do: decode_handoff_items(rest, n, [])
  defp decode_handoff(_), do: {:error, :bad_payload}

  defp decode_handoff_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_handoff_items(
         <<channel_id::32-big, len::32-big, bytes::binary-size(len), rest::binary>>,
         n,
         acc
       ),
       do: decode_handoff_items(rest, n - 1, [{channel_id, bytes} | acc])

  defp decode_handoff_items(_, _, _), do: {:error, :bad_payload}

  defp decode_outbox(<<n::32-big, rest::binary>>), do: decode_outbox_items(rest, n, [])
  defp decode_outbox(_), do: {:error, :bad_payload}

  defp decode_outbox_items(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_outbox_items(
         <<seg_len::16-big, segment_id::binary-size(seg_len), seq_in_op::32-big, chan_len::16-big,
           channel::binary-size(chan_len), payload_len::32-big, payload::binary-size(payload_len),
           rest::binary>>,
         n,
         acc
       ) do
    item = %{
      segment_id: segment_id,
      seq_in_op: seq_in_op,
      channel: channel,
      payload: payload
    }

    decode_outbox_items(rest, n - 1, [item | acc])
  end

  defp decode_outbox_items(_, _, _), do: {:error, :bad_payload}
end
