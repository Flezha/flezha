defmodule Flezha.State.ProcJournal do
  @moduledoc """
  Per-instance append-only proc-progress journal for durable structured
  bounded operatives (`bounded-proc-journal`) and their transactional outbox
  (`bounded-transactional-outbox`).

  One `proc.journal` per instance at
  `<state_root>/<instance_id>/<instance_id>/proc.journal`, written through the
  existing `WalWriter` framing + `FsyncBatcher` path under the reserved
  identity `Registry.journal_id/1` — no parallel persistence machinery. Each
  WAL record's change-bytes carry one typed journal entry, and its 16-byte
  record header carries the instance epoch:

  ```
  header: [epoch: u32 BE][0: 96 bits]
  body:   [type: u8][proc_idx: u16 BE][iter: u32 BE][payload]
  ```

  Entry types: `1 = proc_committed` (the entities that ran, the serialized
  inter-proc handoff, and the procedure's staged outbox — ONE durable write),
  `2 = outbox_flushed` (that outbox reached the plugins),
  `3 = iter_committed` (a loop iteration finished: the inbox offset it
  consumed plus its terminal handoff — `bounded-durable-loop`),
  `4 = parked` (the loop is suspended at a committed boundary, with an
  optional wake deadline — `bounded-durable-suspension`), `5 = compensated`
  (a compensation hook's undo outbox), `6 = rewound` (the loop was rewound to
  the iteration in this entry's `iter` field — `bounded-loop-rewind`). Torn
  tails are truncated by the `WalWriter` open scan; the read path stops at the
  first invalid record.

  Payloads:

  ```
  proc_committed:   [n_entities: u16 BE] n × ([len: u16 BE][entity_identity])
                  [handoff]
                  [outbox]
  iter_committed: [consumed_offset: u64 BE][handoff]
  parked:         [wake_deadline_ms: u64 BE]   # 0 = no deadline
  compensated:    [outbox]
  outbox_flushed: [for_type: u8]   # which entry type's outbox was flushed
  rewound:        <empty>          # the target iteration is the `iter` field
  handoff:        [n: u32 BE] n × ([channel_id: u32 BE][len: u32 BE][payload])
  outbox:         [n: u32 BE] n × ([len: u16 BE][entity_identity][seq_in_proc: u32 BE]
                                   [len: u16 BE][channel][len: u32 BE][payload])
  ```

  The `iter` field of every entry is the enclosing loop iteration (0 for a
  non-looping chain), so redo-vs-skip works at both the procedure and the
  iteration granularity from one scan.

  ## Rewind

  The journal is append-only, so a rewind to iteration N is a `rewound` marker,
  not a truncation. Every fold runs over `live_entries/1`, which replays the
  markers: on `rewound(N)`, entries accumulated so far with `iter > N` are
  purged. Entries appended *after* a marker (the undo records, and the re-run of
  the iterations that were unwound) accumulate normally — "purge what came
  before the marker, keep what came after" is exact, because a pre-rewind
  iteration is journaled before it and its re-run after it.

  So a rewound iteration is not merely skipped on resume: its unflushed outbox
  ceases to be owed (`pending_entries/1` folds the same way) and the inbox items
  it consumed become undelivered again (`consumed_offset/1` folds the same way,
  and `ProcInbox.undelivered/1` is a pure function of it). The marker carries
  `current_epoch + 1`, and since `current_epoch/1` reads the last record's
  header, every post-rewind append inherits the bumped epoch — which is what
  makes a re-run's output supersede the unwound run's at a guarded sink.

  ## Flush

  A procedure's plugin-bound output is staged in the runner and journaled
  WITH its commit, so progress and outbox cannot tear apart. Delivery happens
  only after that fsync, through the same `:on_output` fan-out an unbounded
  operative's effectively-once outbox uses, stamped with the deterministic key
  `(entity, proc_idx, seq_in_proc, epoch)`. `flush_pending/1` is the single flush
  path — driven post-ack by the `ControlChannel` and again on restore, it reads
  the journal, delivers every entry with no matching `outbox_flushed`, and
  records the flush. Journal state (not memory) decides what is owed, so a
  crash between commit and flush re-drives with identical keys.
  """

  require Logger

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.{RecordLog, Registry, Wire}

  @type_proc_committed 1
  @type_outbox_flushed 2
  @type_iter_committed 3
  @type_parked 4
  @type_compensated 5
  @type_rewound 6
  @entry_types %{
    @type_proc_committed => :proc_committed,
    @type_outbox_flushed => :outbox_flushed,
    @type_iter_committed => :iter_committed,
    @type_parked => :parked,
    @type_compensated => :compensated,
    @type_rewound => :rewound
  }

  @type outbox_entry :: %{
          entity_id: binary(),
          seq_in_proc: non_neg_integer(),
          channel: binary(),
          payload: binary()
        }

  @type handoff_item :: {channel_id :: non_neg_integer(), payload :: binary()}

  @type entry_type_name ::
          :proc_committed
          | :outbox_flushed
          | :iter_committed
          | :parked
          | :compensated
          | :rewound

  @type entry :: %{
          seq: non_neg_integer(),
          epoch: non_neg_integer(),
          type: entry_type_name(),
          proc_idx: non_neg_integer(),
          iter: non_neg_integer(),
          payload:
            %{entities_ran: [binary()], handoff: [handoff_item()], outbox: [outbox_entry()]}
            | %{consumed_offset: non_neg_integer(), handoff: [handoff_item()]}
            | %{wake_deadline_ms: non_neg_integer() | nil}
            | %{outbox: [outbox_entry()]}
            | %{for_type: entry_type_name()}
            | %{}
        }

  @type committed_proc :: %{
          proc_idx: non_neg_integer(),
          iter: non_neg_integer(),
          entities_ran: [binary()]
        }

  @doc """
  Append one `proc_committed` entry — entities ran, inter-proc handoff, and the
  procedure's staged outbox in ONE record — and make it durable (fsync) before
  returning; the caller replies `ProcCommitResponse` only on `:ok`. An entry whose
  encoded body exceeds `max_entry_bytes/0` is refused with
  `{:error, {:oversized_handoff, size, max}}` (keep bulk data in Entity state,
  which is already WAL-durable, and hand off references).
  """
  @spec append_proc_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [binary()],
          [handoff_item()],
          [outbox_entry()]
        ) :: :ok | {:error, term()}
  def append_proc_committed(instance_id, proc_idx, iter, entities_ran, handoff, outbox \\ [])
      when is_binary(instance_id) and is_integer(proc_idx) and proc_idx >= 0 do
    append(
      instance_id,
      @type_proc_committed,
      proc_idx,
      iter,
      [encode_committed_payload(entities_ran, handoff), encode_outbox(outbox)]
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
  def append_compensated(instance_id, proc_idx, iter, outbox) when is_binary(instance_id) do
    append(instance_id, @type_compensated, proc_idx, iter, encode_outbox(outbox))
  end

  @doc """
  Append one `rewound` entry — the loop is rewound to the committed boundary of
  iteration `to_iter` — bumping the epoch. Durable *before* any compensation
  runs, so a crash mid-rewind resumes as a rewind rather than as a partially
  unwound loop.

  Every fold honours the marker (see `live_entries/1`), so this one append both
  un-commits the iterations above `to_iter` and re-keys everything the instance
  emits from here on.
  """
  @spec append_rewound(binary(), non_neg_integer()) :: :ok | {:error, term()}
  def append_rewound(instance_id, to_iter)
      when is_binary(instance_id) and is_integer(to_iter) and to_iter >= 0 do
    append(instance_id, @type_rewound, 0, to_iter, <<>>, current_epoch(instance_id) + 1)
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
  pending, and a rewound entry's outbox is no longer owed at all.
  """
  @spec pending_entries(binary()) :: [entry()]
  def pending_entries(instance_id) when is_binary(instance_id) do
    entries = live(instance_id)

    flushed =
      for %{type: :outbox_flushed, proc_idx: proc_idx, iter: iter, payload: %{for_type: for_type}} <-
            entries,
          into: MapSet.new(),
          do: {for_type, proc_idx, iter}

    Enum.filter(entries, fn entry ->
      entry.type in [:proc_committed, :compensated] and
        entry.payload.outbox != [] and
        not MapSet.member?(flushed, {entry.type, entry.proc_idx, entry.iter})
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
  `AddOperative.proc_journal` (`nil` when the instance has no journal).

  The resume point is the iteration **after** the highest `iter_committed`:
  `committed_proc_idxs` are the procedures already committed inside that
  still-in-flight iteration (skipped on resume) and `proc_handoffs` what each of
  them released, so every uncommitted child branch re-seeds from exactly its own
  parent's edge (`bounded-branching`). `handoff` is the previous iteration's
  terminal handoff — what a handoff-chained loop feeds its next iteration.
  `committed_procs` carries every forward proc commit with its iteration, which is
  what the driver folds into `plan_compensation`'s per-iteration `committed`
  input.

  A rewind folds into the same shape — the resume point simply lands back at the
  rewound-to iteration — plus `rewind_undo`: the procedures the rewind unwound
  that have not been compensated yet. The driver runs those through the ordinary
  compensation planner before resuming forward. Each undo journals its own
  `compensated` entry, which shrinks `rewind_undo` on the next fold, so a crash
  mid-rewind resumes with exactly the outstanding work and no double
  compensation. An empty list means no rewind is pending.
  """
  @spec restore_state(binary()) ::
          %{
            committed_proc_idxs: [non_neg_integer()],
            iter: non_neg_integer(),
            handoff: [handoff_item()],
            proc_handoffs: [%{proc_idx: non_neg_integer(), handoff: [handoff_item()]}],
            committed_procs: [committed_proc()],
            rewind_undo: [committed_proc()],
            parked: boolean(),
            wake_deadline_ms: non_neg_integer() | nil,
            consumed_offset: non_neg_integer()
          }
          | nil
  def restore_state(instance_id) do
    # The one fold that needs BOTH views: the forward resume state comes from the
    # live entries, the outstanding undo from what the marker purged.
    {:ok, raw} = read(instance_id)

    case raw do
      [] ->
        nil

      _ ->
        entries = live_entries(raw)
        last_iter = latest_iter_committed(entries)
        # The iteration in flight: the one after the highest committed, 0 before any.
        resume_iter = if last_iter, do: last_iter.iter + 1, else: 0

        in_flight =
          Enum.filter(entries, &(&1.type == :proc_committed and &1.iter == resume_iter))

        %{
          committed_proc_idxs: in_flight |> Enum.map(& &1.proc_idx) |> Enum.uniq() |> Enum.sort(),
          iter: resume_iter,
          handoff: iter_handoff(last_iter),
          proc_handoffs: proc_handoffs(in_flight),
          committed_procs: for(%{type: :proc_committed} = e <- entries, do: committed_proc(e)),
          rewind_undo: rewind_undo(raw),
          parked: match?(%{type: :parked}, List.last(entries)),
          wake_deadline_ms: wake_deadline(List.last(entries)),
          consumed_offset: consumed_offset_of(last_iter)
        }
    end
  end

  @doc """
  The inbox offset this instance has durably consumed — the one recorded by the
  latest live `iter_committed`, `0` before the first iteration commits. The floor
  for both inbox truncation and rehydrate delivery.

  A rewind moves it back with the rest of the fold, but it does NOT resurrect the
  rewound iterations' inputs: the inbox was truncated to this offset when they
  committed, so those entries are already gone. A rewind is undo-and-continue —
  the loop resumes waiting, and the next input becomes iteration N+1.
  """
  @spec consumed_offset(binary()) :: non_neg_integer()
  def consumed_offset(instance_id) when is_binary(instance_id) do
    instance_id |> live() |> latest_iter_committed() |> consumed_offset_of()
  end

  @doc """
  Whether the instance's in-flight iteration has committed every **leaf**
  procedure of its tree — the completion rule of a branching bounded operative
  (`bounded-branching`), derived from the journal alone.

  `leaf_procs` is the class's declared leaf set (`ClassRegistry.leaf_procs/1`, from
  the SDK's `AddOperative` response). `nil` — a class that declared none
  (run-sugar, unbounded, an SDK without the declaration) — is not derivable, so
  the answer is `nil` rather than a guess. An empty journal is not complete.
  """
  @spec complete?(binary(), [non_neg_integer()] | nil) :: boolean() | nil
  def complete?(_instance_id, nil), do: nil

  def complete?(instance_id, leaf_procs) when is_binary(instance_id) and is_list(leaf_procs) do
    # `restore_state/1` already folds the journal down to the procedures the
    # in-flight iteration has committed — which is exactly the question.
    case restore_state(instance_id) do
      nil -> false
      %{committed_proc_idxs: committed} -> Enum.all?(leaf_procs, &(&1 in committed))
    end
  end

  @doc """
  The loop iterations this instance has committed, ascending — the valid targets
  of a rewind, and the range the CLI reports when refusing one.
  """
  @spec committed_iterations(binary()) :: [non_neg_integer()]
  def committed_iterations(instance_id) when is_binary(instance_id) do
    for %{type: :iter_committed, iter: iter} <- live(instance_id),
        uniq: true,
        do: iter
  end

  @doc """
  The plugin channels the procedures above `to_iter` staged output on, each with
  the procedure that staged it — everything a rewind to `to_iter` would have to
  undo. The rewind guard refuses when one of these procedures cannot compensate.
  """
  @spec staged_channels_above(binary(), non_neg_integer()) ::
          [{non_neg_integer(), binary()}]
  def staged_channels_above(instance_id, to_iter) when is_binary(instance_id) do
    for %{type: :proc_committed, iter: iter, proc_idx: proc_idx, payload: payload} <-
          live(instance_id),
        iter > to_iter,
        item <- payload.outbox,
        uniq: true,
        do: {proc_idx, item.channel}
  end

  # The journal as every fold sees it: read, with the rewind markers applied.
  defp live(instance_id) do
    {:ok, entries} = read(instance_id)
    live_entries(entries)
  end

  # The journal with its rewind markers applied: on `rewound(N)`, every entry
  # accumulated so far whose `iter` exceeds N is dropped. Entries appended after a
  # marker — the undo records, and the re-run of the unwound iterations — are
  # kept, which is what makes "purge before, keep after" exact.
  #
  # Every fold that answers "what does this instance still owe / where does it
  # resume" runs over this, so a rewind is a single append rather than a
  # truncation plus a set of special cases.
  defp live_entries(entries) do
    entries
    |> Enum.reduce([], fn
      %{type: :rewound, iter: to_iter} = marker, acc ->
        [marker | Enum.reject(acc, &(&1.iter > to_iter))]

      entry, acc ->
        [entry | acc]
    end)
    |> Enum.reverse()
  end

  # The procedures the latest rewind unwound (journaled before its marker, above
  # its target) that no `compensated` entry has undone yet. A `compensated` entry
  # always follows the proc it undoes, so "undone" is "some compensation for this
  # (proc_idx, iter) appended after this commit" — which stays right across a
  # re-run (the re-run's commit is newer than the old undo) and across a second
  # rewind (the first rewind's undo records are not re-planned).
  defp rewind_undo(raw) do
    case raw |> Enum.filter(&(&1.type == :rewound)) |> List.last() do
      nil ->
        []

      %{seq: marker_seq, iter: to_iter} ->
        undone = for %{type: :compensated} = c <- raw, do: {c.proc_idx, c.iter, c.seq}

        for %{type: :proc_committed} = e <- raw,
            e.seq < marker_seq and e.iter > to_iter,
            not Enum.any?(undone, fn {proc, iter, seq} ->
              proc == e.proc_idx and iter == e.iter and seq > e.seq
            end),
            do: committed_proc(e)
    end
  end

  defp committed_proc(%{proc_idx: proc_idx, iter: iter, payload: %{entities_ran: entities_ran}}),
    do: %{proc_idx: proc_idx, iter: iter, entities_ran: entities_ran}

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

  # Seed for a fresh iteration: the previous iteration's terminal handoff (a
  # handoff-chained loop's input; ignored by an inbox-driven one, which re-seeds
  # from the undelivered inbox tail).
  defp iter_handoff(nil), do: []
  defp iter_handoff(%{payload: %{handoff: handoff}}), do: handoff

  # Seed for each resumed branch: what its parent released. Keyed by the
  # committing procedure, so a child takes its own parent's edge and never a
  # sibling's. The last commit of a proc_idx wins — a re-run supersedes the
  # commit it replaced.
  defp proc_handoffs(in_flight) do
    in_flight
    |> Enum.sort_by(& &1.seq)
    |> Map.new(&{&1.proc_idx, &1.payload.handoff})
    |> Enum.map(fn {proc_idx, handoff} -> %{proc_idx: proc_idx, handoff: handoff} end)
  end

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
  defp append(instance_id, type, proc_idx, iter, payload, epoch \\ nil) do
    body = encode_entry(type, proc_idx, iter, payload)
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
      seq = "#{entry.proc_idx}.#{item.seq_in_proc}"

      stamps =
        Wire.output_stamps(%{
          entity: item.entity_id,
          seq: if(comp?, do: seq <> ".1", else: seq),
          epoch: entry.epoch,
          channel: item.channel
        })

      GenServer.cast(pid, {:io_out, item.payload, stamps})
    end)

    :telemetry.execute(
      [:flezha, :state, :proc_outbox_flush],
      %{count: 1, entries: length(entry.payload.outbox)},
      %{
        operative_id: instance_id,
        proc_idx: entry.proc_idx,
        epoch: entry.epoch,
        compensating: comp?
      }
    )

    case append(
           instance_id,
           @type_outbox_flushed,
           entry.proc_idx,
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
          "[proc_journal] outbox_flushed append failed instance=#{inspect(instance_id)} " <>
            "proc_idx=#{entry.proc_idx}: #{inspect(reason)} — entry will re-drive on restore"
        )
    end
  end

  # ---------- Entry codec ----------

  @doc false
  @spec encode_entry(pos_integer(), non_neg_integer(), non_neg_integer(), iodata()) :: binary()
  def encode_entry(type, proc_idx, iter, payload)
      when type in 1..255 and proc_idx in 0..0xFFFF and iter in 0..0xFFFF_FFFF do
    IO.iodata_to_binary([<<type::8, proc_idx::16-big, iter::32-big>>, payload])
  end

  @doc false
  @spec decode_entry(non_neg_integer(), binary(), non_neg_integer()) ::
          {:ok, entry()} | {:error, term()}
  def decode_entry(seq, body, epoch \\ 0)

  def decode_entry(seq, <<type::8, proc_idx::16-big, iter::32-big, payload::binary>>, epoch) do
    entry = %{seq: seq, epoch: epoch, proc_idx: proc_idx, iter: iter}

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

  defp entry_type_code(:proc_committed), do: @type_proc_committed
  defp entry_type_code(:compensated), do: @type_compensated

  defp decode_payload(:proc_committed, <<n_entities::16-big, rest::binary>>) do
    with {:ok, entities, rest} <- decode_entities(rest, n_entities, []),
         {:ok, handoff, rest} <- decode_handoff(rest),
         {:ok, outbox, <<>>} <- decode_outbox(rest) do
      {:ok, %{entities_ran: entities, handoff: handoff, outbox: outbox}}
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

  defp decode_payload(:outbox_flushed, <<for_type::8>>) do
    with {:ok, name} <- entry_type(for_type), do: {:ok, %{for_type: name}}
  end

  # The rewind target is the entry's own `iter` field — no payload to carry.
  defp decode_payload(:rewound, <<>>), do: {:ok, %{}}

  defp decode_payload(_name, _payload), do: {:error, :bad_payload}

  defp encode_committed_payload(entities_ran, handoff) do
    [
      <<length(entities_ran)::16-big>>,
      Enum.map(entities_ran, fn id -> [<<byte_size(id)::16-big>>, id] end),
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
          <<byte_size(item.entity_id)::16-big>>,
          item.entity_id,
          <<item.seq_in_proc::32-big, byte_size(item.channel)::16-big>>,
          item.channel,
          <<byte_size(item.payload)::32-big>>,
          item.payload
        ]
      end)
    ]
  end

  defp decode_entities(rest, 0, acc), do: {:ok, Enum.reverse(acc), rest}

  defp decode_entities(<<len::16-big, id::binary-size(len), rest::binary>>, n, acc),
    do: decode_entities(rest, n - 1, [id | acc])

  defp decode_entities(_, _, _), do: {:error, :bad_payload}

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
         <<seg_len::16-big, entity_id::binary-size(seg_len), seq_in_proc::32-big,
           chan_len::16-big, channel::binary-size(chan_len), payload_len::32-big,
           payload::binary-size(payload_len), rest::binary>>,
         n,
         acc
       ) do
    item = %{
      entity_id: entity_id,
      seq_in_proc: seq_in_proc,
      channel: channel,
      payload: payload
    }

    decode_outbox_items(rest, n - 1, [item | acc])
  end

  defp decode_outbox_items(_, _, _), do: {:error, :bad_payload}
end
