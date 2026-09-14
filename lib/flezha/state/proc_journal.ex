defmodule Flezha.State.ProcJournal do
  @moduledoc """
  Per-instance append-only proc-progress journal for a durable structured
  workflow operative, and its transactional outbox.

  One `proc.journal` per instance, written through the `WalWriter` framing +
  `FsyncBatcher` path under the reserved identity `Paths.journal_id/1`. Each
  record's change-bytes carry one typed journal entry and its header the
  instance epoch and the entry's identity, which a commit barrier dedups on. The
  entry types and payload layouts are in `docs/internals/state_persistence.md`.

  **Journal state, not memory, decides what is owed.** Every fold runs over
  `live_entries/1`, which replays `rewound` markers — a rewind is a marker, not
  a truncation, so a rewound iteration's outbox stops being owed and
  `consumed_offset/1` moves back while `released_offset/1`, which no rewind
  lowers, holds the inbox floor. The marker bumps the epoch, so a re-run's
  output supersedes the unwound run's at a guarded sink.
  """

  require Logger

  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Lookup

  alias Flezha.State.{
    Outbox,
    Paths,
    PersistenceResolver,
    RecordLog,
    Supervisor,
    WalWriter
  }

  alias Flezha.State.ProcJournal.Codec

  @type_proc_committed Codec.type_code(:proc_committed)
  @type_outbox_flushed Codec.type_code(:outbox_flushed)
  @type_iter_committed Codec.type_code(:iter_committed)
  @type_parked Codec.type_code(:parked)
  @type_compensated Codec.type_code(:compensated)
  @type_rewound Codec.type_code(:rewound)

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

  @typedoc """
  A commit barrier as the control channel receives it — what `append_async/4`
  journals and what the synchronous `append_*` forms wrap.
  """
  @type barrier ::
          {:proc_committed, non_neg_integer(), non_neg_integer(), [binary()], [handoff_item()],
           [outbox_entry()]}
          | {:compensated, non_neg_integer(), non_neg_integer(), [outbox_entry()]}
          | {:iter_committed, non_neg_integer(), non_neg_integer(), [handoff_item()]}

  @doc """
  Append one `proc_committed` entry — entities ran, inter-proc handoff, and the
  procedure's staged outbox in ONE record — durable before returning; the caller
  replies `ProcCommitResponse` only on `:ok`. An entry whose encoded body
  exceeds `max_entry_bytes/0` is refused with
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
    append_barrier(instance_id, {:proc_committed, proc_idx, iter, entities_ran, handoff, outbox})
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
    append_barrier(instance_id, {:iter_committed, iter, consumed_offset, handoff})
  end

  @doc """
  Append one `parked` entry — the loop is suspended at the committed boundary of
  iteration `iter`, to be rehydrated by a new inbox append or, when
  `wake_deadline_ms` is non-nil, by its durable timer. Durable before the
  instance is evicted, so suspension survives a controller redeploy.

  The park is named from `namespace` by an instance id alone, so one for an
  instance whose durable state another namespace owns is refused.
  """
  @spec append_parked(binary(), binary(), non_neg_integer(), non_neg_integer() | nil) ::
          :ok | {:error, term()}
  def append_parked(namespace, instance_id, iter, wake_deadline_ms \\ nil)
      when is_binary(namespace) and is_binary(instance_id) and is_integer(iter) and iter >= 0 do
    owner = Paths.namespace_of_class(instance_id)

    if owner in [namespace, "_system"],
      do: append(instance_id, @type_parked, 0, iter, <<wake_deadline_ms || 0::64-big>>),
      else: {:error, {:namespace_conflict, instance_id, owner}}
  end

  @doc """
  Append one `compensated` entry carrying a compensation hook's undo outbox,
  durable before the reply. Its outputs are flushed with comp-marked keys, so
  an undo can never collide with the forward output it reverses.
  """
  @spec append_compensated(binary(), non_neg_integer(), non_neg_integer(), [outbox_entry()]) ::
          :ok | {:error, term()}
  def append_compensated(instance_id, proc_idx, iter, outbox) when is_binary(instance_id) do
    append_barrier(instance_id, {:compensated, proc_idx, iter, outbox})
  end

  @doc """
  Journal one commit barrier without waiting: the append is issued with
  `:gen_server.send_request/4` under `label` into `reqids`, and its answer —
  `{:ok, seq}` or `{:duplicate, seq}` once the batch holding the record has
  synced, or the writer's exit — comes back as a message for
  `:gen_server.check_response/3`. A barrier the journal already holds under the
  same identity is answered from the existing entry, never appended twice. An
  entry over `max_entry_bytes/0` is refused here, before anything is issued.
  """
  @spec append_async(binary(), barrier(), :gen_server.request_id_collection(), term()) ::
          {:ok, :gen_server.request_id_collection()} | {:error, term()}
  def append_async(instance_id, barrier, reqids, label) when is_binary(instance_id) do
    {type, proc_idx, iter, payload} = barrier_record(barrier)

    with {:ok, header, body} <- encode(instance_id, type, proc_idx, iter, payload, nil) do
      log_id = Paths.journal_id(instance_id)
      {:ok, RecordLog.append_async(log_id, header, body, append_opts(type), reqids, label)}
    end
  end

  @doc """
  Drop the journal's settled prefix: everything below the oldest entry the fold
  still needs — the latest live `iter_committed` (the resume point, its consumed
  offset and the epoch), every entry whose outbox is still owed, and the latest
  `rewound` marker with the unwound commits it still names. Driven once an
  iteration commit is durable, so a loop's journal is bounded by one iteration
  plus a settled prefix below `PersistenceResolver.record_log_cut_after_bytes/0`
  — never by its history; `restore_state/1` folds the cut journal to the state
  the full one gave. The cut itself is the writer's to run: this answers once
  it is asked.

  Only a class with no compensable procedure is cut: the saga's all-or-nothing
  rollback and a rewind both replan from the committed history, which a cut
  would silently shorten. A class that declared nothing is not known to be safe
  and keeps its history too.
  """
  @spec cut(binary()) :: :ok
  def cut(instance_id) when is_binary(instance_id) do
    with [] <- ClassRegistry.compensable_procs(instance_id),
         {:ok, raw} <- read(instance_id),
         floor when is_integer(floor) and floor > 0 <- cut_floor(raw),
         {:ok, writer} <- Supervisor.find_or_start(Paths.journal_id(instance_id)) do
      min_bytes = PersistenceResolver.record_log_cut_after_bytes()
      WalWriter.truncate_up_to_async(writer, floor - 1, min_bytes: min_bytes)
    end

    :ok
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
    case Lookup.overwatch(instance_id) do
      {:ok, pid} -> {:ok, pid}
      {:error, _reason} -> {:error, :no_overwatch}
    end
  end

  @doc """
  The entries whose outbox is journaled but not yet recorded flushed, in
  journal order. An entry carrying no output needs no delivery and is not
  pending, and a rewound entry's outbox stops being owed at all.
  """
  @spec pending_entries(binary()) :: [entry()]
  def pending_entries(instance_id) when is_binary(instance_id),
    do: instance_id |> live() |> pending_of()

  defp pending_of(entries) do
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
      RecordLog.read(Paths.journal_path(instance_id), fn seq, <<epoch::32-big, _::96>>, body ->
        Codec.decode_entry(seq, body, epoch)
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
  parent's edge (`workflow-branching`). `handoff` is the previous iteration's
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
  The highest inbox offset any iteration ever consumed — what the inbox has
  released, whether or not its file has been rewritten yet. Unlike
  `consumed_offset/1` a rewind does not move it back: the rewound iterations'
  inputs were released when they committed, and a rewind is undo-and-continue,
  never a replay. The delivery set (`ProcInbox.undelivered/1`) is everything
  above it.
  """
  @spec released_offset(binary()) :: non_neg_integer()
  def released_offset(instance_id) when is_binary(instance_id) do
    {:ok, raw} = read(instance_id)

    raw
    |> Enum.filter(&(&1.type == :iter_committed))
    |> Enum.map(&consumed_offset_of/1)
    |> Enum.max(fn -> 0 end)
  end

  @doc """
  Whether the instance's in-flight iteration has committed every **leaf**
  procedure of its tree — the completion rule of a branching workflow operative
  (`workflow-branching`), derived from the journal alone.

  `leaf_procs` is the class's declared leaf set (`ClassRegistry.leaf_procs/1`, from
  the SDK's `AddOperative` response). `nil` — a class that declared none
  (run-sugar, stream, an SDK without the declaration) — is not derivable, so
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
  defp rewind_undo(raw), do: Enum.map(rewind_undo_entries(raw), &committed_proc/1)

  defp rewind_undo_entries(raw) do
    case latest_rewound(raw) do
      nil ->
        []

      %{seq: marker_seq, iter: to_iter} ->
        undone = for %{type: :compensated} = c <- raw, do: {c.proc_idx, c.iter, c.seq}

        for %{type: :proc_committed} = e <- raw,
            e.seq < marker_seq and e.iter > to_iter,
            not Enum.any?(undone, fn {proc, iter, seq} ->
              proc == e.proc_idx and iter == e.iter and seq > e.seq
            end),
            do: e
    end
  end

  defp latest_rewound(raw), do: raw |> Enum.filter(&(&1.type == :rewound)) |> List.last()

  # The seq below which nothing is live, or `nil` before the first iteration
  # commit. The head and the owed entries come from the fold's view; the rewind
  # marker — needed only while a commit it unwound is still uncompensated — and
  # those commits from the raw one.
  defp cut_floor(raw) do
    entries = live_entries(raw)

    with %{seq: head} <- latest_iter_committed(entries) do
      owed = for %{seq: seq} <- pending_of(entries), do: seq

      unwound =
        case rewind_undo_entries(raw) do
          [] -> []
          undo -> [latest_rewound(raw).seq | Enum.map(undo, & &1.seq)]
        end

      Enum.min([head | owed ++ unwound])
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
  def max_entry_bytes(), do: PersistenceResolver.wal_high_watermark_bytes()

  # ---------- Append ----------

  @barrier_types [@type_proc_committed, @type_iter_committed, @type_compensated]

  defp append_barrier(instance_id, barrier) do
    {type, proc_idx, iter, payload} = barrier_record(barrier)
    append(instance_id, type, proc_idx, iter, payload)
  end

  defp barrier_record({:proc_committed, proc_idx, iter, entities_ran, handoff, outbox}),
    do:
      {@type_proc_committed, proc_idx, iter,
       [Codec.encode_committed_payload(entities_ran, handoff), Codec.encode_outbox(outbox)]}

  defp barrier_record({:compensated, proc_idx, iter, outbox}),
    do: {@type_compensated, proc_idx, iter, Codec.encode_outbox(outbox)}

  defp barrier_record({:iter_committed, iter, consumed_offset, handoff}),
    do:
      {@type_iter_committed, 0, iter,
       [<<consumed_offset::64-big>>, Codec.encode_handoff(handoff)]}

  # `epoch` defaults to the instance's current one (a journal read); a caller
  # already holding the entry it is marking passes that entry's epoch instead,
  # which both saves the read and pins the marker to the epoch it flushed. An
  # entry the journal already holds (a re-sent barrier) is `:ok` — it is durable.
  defp append(instance_id, type, proc_idx, iter, payload, epoch \\ nil) do
    with {:ok, header, body} <- encode(instance_id, type, proc_idx, iter, payload, epoch),
         {:ok, _seq} <-
           RecordLog.append(Paths.journal_id(instance_id), header, body, append_opts(type)) do
      :ok
    else
      {:duplicate, _seq} -> :ok
      {:error, _} = err -> err
    end
  end

  defp encode(instance_id, type, proc_idx, iter, payload, epoch) do
    body = Codec.encode_entry(type, proc_idx, iter, payload)
    max = max_entry_bytes()

    if byte_size(body) > max do
      {:error, {:oversized_handoff, byte_size(body), max}}
    else
      {:ok, encode_header(epoch || current_epoch(instance_id), type, proc_idx, iter), body}
    end
  end

  # The epoch rides every record's header, so it survives compaction of any
  # single entry and is readable without interpreting a payload; the entry's
  # identity rides beside it, which is what lets the writer refuse a re-send.
  defp encode_header(epoch, type, proc_idx, iter),
    do: <<epoch::32-big, type::8, proc_idx::16-big, iter::32-big, 0::40>>

  # A barrier is batched and idempotent by identity; a marker and the park are
  # batched; a rewind syncs inline — rare, and durable before the eviction.
  defp append_opts(@type_rewound), do: [sync: :sync]
  defp append_opts(type) when type in @barrier_types, do: [sync: :batched, dedup: true]
  defp append_opts(_type), do: [sync: :batched]

  # ---------- Flush ----------

  # Deliver one journaled entry's outbox through the operative's `:on_output`
  # chain — the same fan-out `StreamChannel` uses for live workflow output and
  # `WalWriter` uses for the stream effectively-once outbox. The instance's
  # journal writer delivers: it owns the entry until the sinks confirm (then
  # appends the `outbox_flushed` marker), and it is what keeps a re-entered
  # flush from handing the chain an entry it already has. A writer that cannot
  # start delivers nothing; the journal still owes the entry to the next flush.
  defp flush_entry(instance_id, pid, entry) do
    comp? = entry.type == :compensated
    for_type = Codec.entry_type_code(entry.type)
    key = {:marker, for_type, entry.proc_idx, entry.iter, entry.epoch}
    owner = Paths.journal_id(instance_id)

    outputs =
      Enum.map(entry.payload.outbox, fn item ->
        # An undo output's seq carries a trailing `.1` — the compensation
        # marker — so it can never collide with the forward output it
        # reverses while staying a numeric dotted sequence a keyed sink can
        # order.
        seq = "#{entry.proc_idx}.#{item.seq_in_proc}"

        Outbox.stamp(item.payload, %{
          owner: owner,
          entry: key,
          entity: item.entity_id,
          seq: if(comp?, do: seq <> ".1", else: seq),
          epoch: entry.epoch,
          channel: item.channel
        })
      end)

    telemetry =
      {[:flezha, :state, :proc_outbox_flush], %{count: 1, entries: length(outputs)},
       %{
         operative_id: instance_id,
         proc_idx: entry.proc_idx,
         epoch: entry.epoch,
         compensating: comp?
       }}

    with {:ok, writer} <- Supervisor.find_or_start(owner) do
      WalWriter.flush_outbox(writer, key, outputs, pid, telemetry)
    end

    :ok
  end

  @doc """
  Record one entry's outbox as delivered — the sinks confirmed it. Called by the
  instance's journal writer through `Flezha.State.Outbox`, never by the flush:
  the marker is what a restore reads to know an entry stopped being owed, so it
  may only follow a confirmed delivery.
  """
  @spec mark_flushed(
          binary(),
          byte(),
          non_neg_integer(),
          non_neg_integer(),
          non_neg_integer()
        ) :: :ok
  def mark_flushed(instance_id, for_type, proc_idx, iter, epoch) do
    case append(instance_id, @type_outbox_flushed, proc_idx, iter, <<for_type::8>>, epoch) do
      :ok ->
        :ok

      {:error, reason} ->
        # The outbox reached the sinks but the marker did not land: a restore
        # re-drives this entry. Duplicate delivery with an identical key is the
        # documented at-least-once posture; losing the marker write silently is
        # not.
        Logger.warning(
          "[proc_journal] outbox_flushed append failed instance=#{inspect(instance_id)} " <>
            "proc_idx=#{proc_idx}: #{inspect(reason)} — entry will re-drive on restore"
        )

        :ok
    end
  end
end
