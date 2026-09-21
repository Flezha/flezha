defmodule Flezha.State.TaskJournal do
  @moduledoc """
  Per-instance append-only task-progress journal for a durable structured
  workflow operative, and its transactional outbox.

  One `task.journal` per instance, written through the `WalWriter` framing +
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

  alias Flezha.State.TaskJournal.Codec

  @type_task_committed Codec.type_code(:task_committed)
  @type_outbox_flushed Codec.type_code(:outbox_flushed)
  @type_iter_committed Codec.type_code(:iter_committed)
  @type_parked Codec.type_code(:parked)
  @type_compensated Codec.type_code(:compensated)
  @type_rewound Codec.type_code(:rewound)

  @type outbox_entry :: %{
          cell_id: binary(),
          seq_in_task: non_neg_integer(),
          channel: binary(),
          payload: binary()
        }

  @type handoff_item :: {channel_id :: non_neg_integer(), payload :: binary()}

  @type entry_type_name ::
          :task_committed
          | :outbox_flushed
          | :iter_committed
          | :parked
          | :compensated
          | :rewound

  @type entry :: %{
          seq: non_neg_integer(),
          epoch: non_neg_integer(),
          type: entry_type_name(),
          task_idx: non_neg_integer(),
          iter: non_neg_integer(),
          payload:
            %{cells_ran: [binary()], handoff: [handoff_item()], outbox: [outbox_entry()]}
            | %{
                consumed_offset: non_neg_integer(),
                entry_channel: String.t(),
                handoff: [handoff_item()]
              }
            | %{wake_deadline_ms: non_neg_integer() | nil, consumed_offset: non_neg_integer()}
            | %{outbox: [outbox_entry()]}
            | %{for_type: entry_type_name()}
            | %{}
        }

  @type committed_task :: %{
          task_idx: non_neg_integer(),
          iter: non_neg_integer(),
          cells_ran: [binary()]
        }

  @typedoc """
  A commit barrier as the control channel receives it — what `append_async/4`
  journals and what the synchronous `append_*` forms wrap.
  """
  @type barrier ::
          {:task_committed, non_neg_integer(), non_neg_integer(), [binary()], [handoff_item()],
           [outbox_entry()], completion()}
          | {:compensated, non_neg_integer(), non_neg_integer(), [outbox_entry()]}
          | {:iter_committed, non_neg_integer(), non_neg_integer(), String.t(), [handoff_item()],
             completion()}

  @typedoc "The result a tasklet's `complete` asked for, or `nil` when none did."
  @type completion :: binary() | nil

  @doc """
  Append one `task_committed` entry — cells ran, inter-task handoff, the
  tasklet's staged outbox and the completion it asked for in ONE record —
  durable before returning; the caller replies `TaskCommitResponse` only on
  `:ok`. An entry whose encoded body
  exceeds `max_entry_bytes/0` is refused with
  `{:error, {:oversized_handoff, size, max}}` (keep bulk data in Cell state,
  which is already WAL-durable, and hand off references).
  """
  @spec append_task_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [binary()],
          [handoff_item()],
          [outbox_entry()],
          completion()
        ) :: :ok | {:error, term()}
  def append_task_committed(
        instance_id,
        task_idx,
        iter,
        cells_ran,
        handoff,
        outbox \\ [],
        completion \\ nil
      )
      when is_binary(instance_id) and is_integer(task_idx) and task_idx >= 0 do
    append_barrier(
      instance_id,
      {:task_committed, task_idx, iter, cells_ran, handoff, outbox, completion}
    )
  end

  @doc """
  Append one `iter_committed` entry — loop iteration `iter` finished, having
  run the chain of entry channel `entry_channel`, consumed the inbox up to
  `consumed_offset` and released `handoff` as its terminal batch, and ended
  the instance when `completion` is not `nil` — durable before the reply. The
  commit boundary a crash-resume restarts after: iterations at or below
  `iter` never re-run, and a chained loop re-enters the named chain.
  """
  @spec append_iter_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [handoff_item()],
          completion(),
          String.t()
        ) :: :ok | {:error, term()}
  def append_iter_committed(
        instance_id,
        iter,
        consumed_offset,
        handoff,
        completion \\ nil,
        entry_channel \\ ""
      )
      when is_binary(instance_id) and is_integer(iter) and iter >= 0 and
             is_binary(entry_channel) do
    append_barrier(
      instance_id,
      {:iter_committed, iter, consumed_offset, entry_channel, handoff, completion}
    )
  end

  @doc """
  Append one `parked` entry — the loop is suspended at the committed boundary of
  iteration `iter`, to be rehydrated by a new inbox append or, when
  `wake_deadline_ms` is non-nil, by its durable timer. `consumed_offset` is the
  highest inbox offset the run parked to the dead-letter queue unread (`0` when
  none), which the inbox releases as an iteration's consumed input. Durable
  before the instance is evicted, so suspension survives a controller redeploy.

  The park is named from `namespace` by an instance id alone, so one for an
  instance whose durable state another namespace owns is refused.
  """
  @spec append_parked(
          binary(),
          binary(),
          non_neg_integer(),
          non_neg_integer() | nil,
          non_neg_integer()
        ) :: :ok | {:error, term()}
  def append_parked(namespace, instance_id, iter, wake_deadline_ms \\ nil, consumed_offset \\ 0)
      when is_binary(namespace) and is_binary(instance_id) and is_integer(iter) and iter >= 0 and
             is_integer(consumed_offset) and consumed_offset >= 0 do
    owner = Paths.namespace_of_class(instance_id)
    payload = <<wake_deadline_ms || 0::64-big, consumed_offset::64-big>>

    if owner in [namespace, "_system"],
      do: append(instance_id, @type_parked, 0, iter, payload),
      else: {:error, {:namespace_conflict, instance_id, owner}}
  end

  @doc """
  Append one `compensated` entry carrying a compensation hook's undo outbox,
  durable before the reply. Its outputs are flushed with comp-marked keys, so
  an undo can never collide with the forward output it reverses.
  """
  @spec append_compensated(binary(), non_neg_integer(), non_neg_integer(), [outbox_entry()]) ::
          :ok | {:error, term()}
  def append_compensated(instance_id, task_idx, iter, outbox) when is_binary(instance_id) do
    append_barrier(instance_id, {:compensated, task_idx, iter, outbox})
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
    {type, task_idx, iter, payload} = barrier_record(barrier)

    with {:ok, header, body} <- encode(instance_id, type, task_idx, iter, payload, nil) do
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

  Only a class with no compensable tasklet is cut: the saga's all-or-nothing
  rollback and a rewind both replan from the committed history, which a cut
  would silently shorten. A class that declared nothing is not known to be safe
  and keeps its history too.
  """
  @spec cut(binary()) :: :ok
  def cut(instance_id) when is_binary(instance_id) do
    with [] <- ClassRegistry.compensable_tasks(instance_id),
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
      for %{type: :outbox_flushed, task_idx: task_idx, iter: iter, payload: %{for_type: for_type}} <-
            entries,
          into: MapSet.new(),
          do: {for_type, task_idx, iter}

    Enum.filter(entries, fn entry ->
      entry.type in [:task_committed, :compensated] and
        entry.payload.outbox != [] and
        not MapSet.member?(flushed, {entry.type, entry.task_idx, entry.iter})
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
  `AddOperative.task_journal` (`nil` when the instance has no journal).

  The resume point is the iteration **after** the highest `iter_committed`:
  `committed_task_idxs` are the tasklets already committed inside that
  still-in-flight iteration (skipped on resume) and `task_handoffs` what each of
  them released, so every uncommitted child branch re-seeds from exactly its own
  parent's edge (`workflow-branching`). `handoff` is the previous iteration's
  terminal handoff — what a handoff-chained loop feeds its next iteration.
  `committed_tasks` carries every forward task commit with its iteration and the
  completion it asked for, which is what the driver folds into
  `plan_compensation`'s per-iteration `committed` input and what ends a resumed
  iteration whose completing tasklet committed before the crash.
  `completion` is the result the last committed iteration ended the instance
  with, `nil` when it did not.

  A rewind folds into the same shape — the resume point simply lands back at the
  rewound-to iteration — plus `rewind_undo`: the tasklets the rewind unwound
  that have not been compensated yet. The driver runs those through the ordinary
  compensation planner before resuming forward. Each undo journals its own
  `compensated` entry, which shrinks `rewind_undo` on the next fold, so a crash
  mid-rewind resumes with exactly the outstanding work and no double
  compensation. An empty list means no rewind is pending.
  """
  @spec restore_state(binary()) ::
          %{
            committed_task_idxs: [non_neg_integer()],
            iter: non_neg_integer(),
            handoff: [handoff_item()],
            task_handoffs: [%{task_idx: non_neg_integer(), handoff: [handoff_item()]}],
            committed_tasks: [committed_task()],
            rewind_undo: [committed_task()],
            parked: boolean(),
            wake_deadline_ms: non_neg_integer() | nil,
            consumed_offset: non_neg_integer(),
            completion: completion()
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
          Enum.filter(entries, &(&1.type == :task_committed and &1.iter == resume_iter))

        %{
          committed_task_idxs: in_flight |> Enum.map(& &1.task_idx) |> Enum.uniq() |> Enum.sort(),
          iter: resume_iter,
          entry_channel: entry_channel_of(last_iter),
          handoff: iter_handoff(last_iter),
          task_handoffs: task_handoffs(in_flight),
          committed_tasks: for(%{type: :task_committed} = e <- entries, do: committed_task(e)),
          rewind_undo: rewind_undo(raw),
          parked: match?(%{type: :parked}, List.last(entries)),
          wake_deadline_ms: wake_deadline(List.last(entries)),
          consumed_offset: consumed_offset_of(last_iter),
          completion: completion_of(last_iter)
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
  The highest inbox offset any iteration ever consumed, or a park recorded as
  parked to the dead-letter queue unread: what the inbox has released, whether
  or not its file has been rewritten yet. Unlike `consumed_offset/1` a rewind
  does not move it back: the rewound iterations' inputs were released when they
  committed, and a rewind is undo-and-continue, never a replay. The delivery set
  (`TaskInbox.undelivered/1`) is everything above it.
  """
  @spec released_offset(binary()) :: non_neg_integer()
  def released_offset(instance_id) when is_binary(instance_id) do
    {:ok, raw} = read(instance_id)

    raw
    |> Enum.filter(&(&1.type in [:iter_committed, :parked]))
    |> Enum.map(&consumed_offset_of/1)
    |> Enum.max(fn -> 0 end)
  end

  @doc """
  Whether the instance's in-flight iteration has committed every **leaf**
  tasklet of its tree — the completion rule of a branching workflow operative
  (`workflow-branching`), derived from the journal alone.

  `leaf_tasks` is the class's declared leaf set (`ClassRegistry.leaf_tasks/1`, from
  the SDK's `AddOperative` response). `nil` — a class that declared none (a
  stream, an SDK without the declaration) — is not derivable, so the answer is
  `nil` rather than a guess. An empty journal is not complete.
  """
  @spec complete?(binary(), [non_neg_integer()] | nil) :: boolean() | nil
  def complete?(_instance_id, nil), do: nil

  def complete?(instance_id, leaf_tasks) when is_binary(instance_id) and is_list(leaf_tasks) do
    # `restore_state/1` already folds the journal down to the tasklets the
    # in-flight iteration has committed — which is exactly the question.
    case restore_state(instance_id) do
      nil -> false
      %{committed_task_idxs: committed} -> Enum.all?(leaf_tasks, &(&1 in committed))
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
  The plugin channels the tasklets above `to_iter` staged output on, each with
  the tasklet that staged it — everything a rewind to `to_iter` would have to
  undo. The rewind guard refuses when one of these tasklets cannot compensate.
  """
  @spec staged_channels_above(binary(), non_neg_integer()) ::
          [{non_neg_integer(), binary()}]
  def staged_channels_above(instance_id, to_iter) when is_binary(instance_id) do
    for %{type: :task_committed, iter: iter, task_idx: task_idx, payload: payload} <-
          live(instance_id),
        iter > to_iter,
        item <- payload.outbox,
        uniq: true,
        do: {task_idx, item.channel}
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

  # The tasklets the latest rewind unwound (journaled before its marker, above
  # its target) that no `compensated` entry has undone yet. A `compensated` entry
  # always follows the task it undoes, so "undone" is "some compensation for this
  # (task_idx, iter) appended after this commit" — which stays right across a
  # re-run (the re-run's commit is newer than the old undo) and across a second
  # rewind (the first rewind's undo records are not re-planned).
  defp rewind_undo(raw), do: Enum.map(rewind_undo_entries(raw), &committed_task/1)

  defp rewind_undo_entries(raw) do
    case latest_rewound(raw) do
      nil ->
        []

      %{seq: marker_seq, iter: to_iter} ->
        undone = for %{type: :compensated} = c <- raw, do: {c.task_idx, c.iter, c.seq}

        for %{type: :task_committed} = e <- raw,
            e.seq < marker_seq and e.iter > to_iter,
            not Enum.any?(undone, fn {task, iter, seq} ->
              task == e.task_idx and iter == e.iter and seq > e.seq
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

  defp committed_task(%{task_idx: task_idx, iter: iter, payload: payload}),
    do: %{
      task_idx: task_idx,
      iter: iter,
      cells_ran: payload.cells_ran,
      completion: payload.completion
    }

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

  defp entry_channel_of(nil), do: ""
  defp entry_channel_of(%{payload: %{entry_channel: channel}}), do: channel

  defp completion_of(nil), do: nil
  defp completion_of(%{payload: %{completion: completion}}), do: completion

  defp wake_deadline(%{type: :parked, payload: %{wake_deadline_ms: ms}}), do: ms
  defp wake_deadline(_), do: nil

  # Seed for a fresh iteration: the previous iteration's terminal handoff (a
  # handoff-chained loop's input; ignored by an inbox-driven one, which re-seeds
  # from the undelivered inbox tail).
  defp iter_handoff(nil), do: []
  defp iter_handoff(%{payload: %{handoff: handoff}}), do: handoff

  # Seed for each resumed branch: what its parent released. Keyed by the
  # committing tasklet, so a child takes its own parent's edge and never a
  # sibling's. The last commit of a task_idx wins — a re-run supersedes the
  # commit it replaced.
  defp task_handoffs(in_flight) do
    in_flight
    |> Enum.sort_by(& &1.seq)
    |> Map.new(&{&1.task_idx, &1.payload.handoff})
    |> Enum.map(fn {task_idx, handoff} -> %{task_idx: task_idx, handoff: handoff} end)
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

  @barrier_types [@type_task_committed, @type_iter_committed, @type_compensated]

  defp append_barrier(instance_id, barrier) do
    {type, task_idx, iter, payload} = barrier_record(barrier)
    append(instance_id, type, task_idx, iter, payload)
  end

  defp barrier_record({:task_committed, task_idx, iter, cells_ran, handoff, outbox, completion}),
    do:
      {@type_task_committed, task_idx, iter,
       [
         Codec.encode_committed_payload(cells_ran, handoff),
         Codec.encode_outbox(outbox),
         Codec.encode_completion(completion)
       ]}

  defp barrier_record({:compensated, task_idx, iter, outbox}),
    do: {@type_compensated, task_idx, iter, Codec.encode_outbox(outbox)}

  defp barrier_record(
         {:iter_committed, iter, consumed_offset, entry_channel, handoff, completion}
       ),
       do:
         {@type_iter_committed, 0, iter,
          [
            <<consumed_offset::64-big, byte_size(entry_channel)::16-big>>,
            entry_channel,
            Codec.encode_handoff(handoff),
            Codec.encode_completion(completion)
          ]}

  # `epoch` defaults to the instance's current one (a journal read); a caller
  # already holding the entry it is marking passes that entry's epoch instead,
  # which both saves the read and pins the marker to the epoch it flushed. An
  # entry the journal already holds (a re-sent barrier) is `:ok` — it is durable.
  defp append(instance_id, type, task_idx, iter, payload, epoch \\ nil) do
    with {:ok, header, body} <- encode(instance_id, type, task_idx, iter, payload, epoch),
         {:ok, _seq} <-
           RecordLog.append(Paths.journal_id(instance_id), header, body, append_opts(type)) do
      :ok
    else
      {:duplicate, _seq} -> :ok
      {:error, _} = err -> err
    end
  end

  defp encode(instance_id, type, task_idx, iter, payload, epoch) do
    body = Codec.encode_entry(type, task_idx, iter, payload)
    max = max_entry_bytes()

    if byte_size(body) > max do
      {:error, {:oversized_handoff, byte_size(body), max}}
    else
      {:ok, encode_header(epoch || current_epoch(instance_id), type, task_idx, iter), body}
    end
  end

  # The epoch rides every record's header, so it survives compaction of any
  # single entry and is readable without interpreting a payload; the entry's
  # identity rides beside it, which is what lets the writer refuse a re-send.
  defp encode_header(epoch, type, task_idx, iter),
    do: <<epoch::32-big, type::8, task_idx::16-big, iter::32-big, 0::40>>

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
    key = {:marker, for_type, entry.task_idx, entry.iter, entry.epoch}
    owner = Paths.journal_id(instance_id)

    outputs =
      Enum.map(entry.payload.outbox, fn item ->
        # An undo output's seq carries a trailing `.1` — the compensation
        # marker — so it can never collide with the forward output it
        # reverses while staying a numeric dotted sequence a keyed sink can
        # order.
        seq = "#{entry.task_idx}.#{item.seq_in_task}"

        Outbox.stamp(item.payload, %{
          owner: owner,
          entry: key,
          cell: item.cell_id,
          seq: if(comp?, do: seq <> ".1", else: seq),
          epoch: entry.epoch,
          channel: item.channel
        })
      end)

    telemetry =
      {[:flezha, :state, :task_outbox_flush], %{count: 1, entries: length(outputs)},
       %{
         operative_id: instance_id,
         task_idx: entry.task_idx,
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
  def mark_flushed(instance_id, for_type, task_idx, iter, epoch) do
    case append(instance_id, @type_outbox_flushed, task_idx, iter, <<for_type::8>>, epoch) do
      :ok ->
        :ok

      {:error, reason} ->
        # The outbox reached the sinks but the marker did not land: a restore
        # re-drives this entry. Duplicate delivery with an identical key is the
        # documented at-least-once posture; losing the marker write silently is
        # not.
        Logger.warning(
          "[task_journal] outbox_flushed append failed instance=#{inspect(instance_id)} " <>
            "task_idx=#{task_idx}: #{inspect(reason)} — entry will re-drive on restore"
        )

        :ok
    end
  end
end
