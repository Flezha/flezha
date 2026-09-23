defmodule Flezha.State.TaskJournal do
  @moduledoc """
  Per-instance append-only task-progress journal for a durable structured
  workflow operative: the markers of its progress, never its Cells' state or
  output.

  One `task.journal` per instance, written through the `WalWriter` framing +
  `FsyncBatcher` path under the reserved identity `Paths.journal_id/1`. Each
  record's change-bytes carry one typed journal entry and its header the
  instance epoch and the entry's identity, which a commit barrier dedups on. The
  entry types and payload layouts are in `docs/internals/state_persistence.md`.

  A stage entry names, for each Cell it ran, the effect records the Cell wrote
  in the stage; those records carry the stage's state and output, and their
  Cell's writer releases the output once the entry naming them is durable
  (`committed_records/1`).

  **Journal state, not memory, decides what is committed.** Every fold runs
  over `live_entries/1`, which replays `rewound` markers: a rewind is a
  marker, not a truncation, so a rewound iteration's records stop being
  committed and `consumed_offset/1` moves back while `released_offset/1`,
  which no rewind lowers, holds the instance's floor in its input log. The
  marker bumps the epoch, which keys every later barrier's identity.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.SDK.Wire

  alias Flezha.State.{
    InputLog,
    Outbox,
    Paths,
    PersistenceResolver,
    RecordLog,
    Supervisor,
    WalWriter
  }

  alias Flezha.State.TaskJournal.Codec

  @type_task_committed Codec.type_code(:task_committed)
  @type_iter_committed Codec.type_code(:iter_committed)
  @type_compensated Codec.type_code(:compensated)
  @type_rewound Codec.type_code(:rewound)

  @typedoc """
  One Cell a stage or an undo ran: its identity, the effect records it wrote
  there (`first_seq..seq` on its WAL, both `nil` when it wrote none) and the
  plugin channels those records wrote on.
  """
  @type committed_cell :: %{
          cell_id: binary(),
          first_seq: non_neg_integer() | nil,
          seq: non_neg_integer() | nil,
          channels: [binary()]
        }

  @type handoff_item :: {channel_id :: non_neg_integer(), payload :: binary()}

  @type entry_type_name ::
          :task_committed
          | :iter_committed
          | :compensated
          | :rewound

  @type entry :: %{
          seq: non_neg_integer(),
          epoch: non_neg_integer(),
          type: entry_type_name(),
          task_idx: non_neg_integer(),
          iter: non_neg_integer(),
          payload:
            %{
              cells_ran: [committed_cell()],
              handoff: [handoff_item()],
              completion: completion(),
              trigger: trigger()
            }
            | %{
                consumed_offset: non_neg_integer() | nil,
                entry_channel: String.t(),
                handoff: [handoff_item()],
                completion: completion()
              }
            | %{cells_ran: [committed_cell()]}
            | %{}
        }

  @type committed_task :: %{
          task_idx: non_neg_integer(),
          iter: non_neg_integer(),
          cells_ran: [binary()],
          completion: completion(),
          trigger: trigger()
        }

  @typedoc """
  A commit barrier as the control channel receives it — what `append_async/4`
  journals and what the synchronous `append_*` forms wrap.
  """
  @type barrier ::
          {:task_committed, non_neg_integer(), non_neg_integer(), [committed_cell()],
           [handoff_item()], completion(), trigger()}
          | {:compensated, non_neg_integer(), non_neg_integer(), [committed_cell()]}
          | {:iter_committed, non_neg_integer(), non_neg_integer() | nil, String.t(),
             [handoff_item()], completion()}

  @typedoc "The result a tasklet's `complete` asked for, or `nil` when none did."
  @type completion :: binary() | nil

  @typedoc """
  The timer firing that opened a pass, carried by its root tasklet's
  `task_committed` entry; `nil` on every other.
  """
  @type trigger ::
          %{
            cell_key: binary(),
            kind: :processing_time | :event_time,
            scheduled_at: non_neg_integer()
          }
          | nil

  @doc """
  Append one `task_committed` entry (the Cells the stage ran with the records
  each wrote, the inter-task handoff, the completion it asked for and, on a
  timer-fed pass's root, the firing that opened it, in ONE record), durable
  before returning; the caller replies `TaskCommitResponse` only on `:ok`. An
  entry whose encoded body exceeds `max_entry_bytes/0` is refused with
  `{:error, {:oversized_handoff, size, max}}` (keep bulk data in Cell state,
  which is already WAL-durable, and hand off references).
  """
  @spec append_task_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          [committed_cell()],
          [handoff_item()],
          completion(),
          trigger()
        ) :: :ok | {:error, term()}
  def append_task_committed(
        instance_id,
        task_idx,
        iter,
        cells_ran,
        handoff,
        completion \\ nil,
        trigger \\ nil
      )
      when is_binary(instance_id) and is_integer(task_idx) and task_idx >= 0 do
    append_barrier(
      instance_id,
      {:task_committed, task_idx, iter, cells_ran, handoff, completion, trigger}
    )
  end

  @doc """
  Append one `iter_committed` entry — loop iteration `iter` finished, having
  run the chain of entry channel `entry_channel`, consumed the input log up to
  `consumed_offset` (`nil` when it consumed no journaled input) and released
  `handoff` as its terminal batch, and ended the instance when `completion` is
  not `nil` — durable before the reply. The commit boundary a crash-resume
  restarts after: iterations at or below `iter` never re-run, and a chained
  loop re-enters the named chain.
  """
  @spec append_iter_committed(
          binary(),
          non_neg_integer(),
          non_neg_integer() | nil,
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
  Append one `compensated` entry naming the undo records the tasklet's hooks
  wrote, durable before the reply.
  """
  @spec append_compensated(binary(), non_neg_integer(), non_neg_integer(), [committed_cell()]) ::
          :ok | {:error, term()}
  def append_compensated(instance_id, task_idx, iter, cells_ran) when is_binary(instance_id) do
    append_barrier(instance_id, {:compensated, task_idx, iter, cells_ran})
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
  offset and the epoch), every stage entry naming a record its Cell's flush
  cursor has not passed (a writer that restarts releases a record only once a
  live entry names it), and the latest `rewound` marker with the unwound
  commits it still names. Driven once an
  iteration commit is durable, so a loop's journal is bounded by one iteration
  plus a settled prefix below `PersistenceResolver.record_log_cut_after_bytes/0`
  — never by its history; `restore_state/1` folds the cut journal to the state
  the full one gave. The cut itself is the writer's to run: this answers once
  it is asked.

  Only a class with no compensable tasklet is cut: the saga's all-or-nothing
  rollback and a rewind both replan from the committed history, which a cut
  would silently shorten. A class that declared nothing is not known to be safe
  and keeps its history too.

  `seal: true` is a partition rescale's history boundary: the prefix is cut
  whatever the class declares and however small it is, and the cut has landed
  when this returns, because the member restarts on it next. A moved Cell's
  earlier passes belong to a member that may no longer own it, so no rewind or
  rollback may reach below the rescale.
  """
  @spec cut(binary(), keyword()) :: :ok
  def cut(instance_id, opts \\ []) when is_binary(instance_id) do
    seal? = Keyword.get(opts, :seal, false)

    with true <- seal? or ClassRegistry.compensable_tasks(instance_id) == [],
         {:ok, raw} <- read(instance_id),
         floor when is_integer(floor) and floor > 0 <- cut_floor(raw),
         {:ok, writer} <- Supervisor.find_or_start(Paths.journal_id(instance_id)) do
      if seal? do
        WalWriter.truncate_up_to(writer, floor - 1)
      else
        min_bytes = PersistenceResolver.record_log_cut_after_bytes()
        WalWriter.truncate_up_to_async(writer, floor - 1, min_bytes: min_bytes)
      end
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
  `committed_tasks` carries every forward task commit with its iteration, the
  identities of the Cells it ran and the completion it asked for, which is what
  the driver folds into
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
            consumed_offset: non_neg_integer() | nil,
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
          consumed_offset: consumed_offset_of(last_iter),
          completion: completion_of(last_iter)
        }
    end
  end

  @doc """
  The input-log offset this instance has durably consumed — the one recorded by
  the latest live `iter_committed`, `nil` while none has consumed any.

  A rewind moves it back with the rest of the fold, but it does NOT resurrect the
  rewound iterations' inputs: those were released when they committed, and the
  instance's floor (`released_offset/1`) does not move back. A rewind is
  undo-and-continue — the loop resumes waiting, and the next input becomes
  iteration N+1.
  """
  @spec consumed_offset(binary()) :: non_neg_integer() | nil
  def consumed_offset(instance_id) when is_binary(instance_id) do
    instance_id |> live() |> latest_iter_committed() |> consumed_offset_of()
  end

  @doc """
  The highest input-log offset any iteration ever consumed, `nil` when none
  has: the instance's floor in its input log. Unlike `consumed_offset/1` a rewind does
  not move it back: the rewound iterations' inputs were released when they
  committed, and a rewind is undo-and-continue, never a replay.
  """
  @spec released_offset(binary()) :: non_neg_integer() | nil
  def released_offset(instance_id) when is_binary(instance_id) do
    {:ok, raw} = read(instance_id)

    raw
    |> Enum.filter(&(&1.type == :iter_committed))
    |> Enum.map(&consumed_offset_of/1)
    |> Enum.reject(&is_nil/1)
    |> Enum.max(fn -> nil end)
  end

  @doc """
  Report input this instance has consumed into its input log's watermark,
  under the journal's identity — the instance's one reporter, so its row is the
  instance's floor. `offset` is a durable iteration commit's or park's, `nil`
  when it consumed none. `release_input/1` reports `released_offset/1`, which
  is how every bind seeds the row before the log is asked anything: the row is
  in memory, and `record_consumed/3` only moves forward, so seeding again is
  harmless.
  """
  @spec release_input(binary(), non_neg_integer() | nil) :: :ok
  def release_input(instance_id), do: release_input(instance_id, released_offset(instance_id))

  def release_input(_instance_id, nil), do: :ok

  def release_input(instance_id, offset),
    do: InputLog.record_consumed(instance_id, Paths.journal_id(instance_id), offset)

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
  The plugin channels the tasklets above `to_iter` wrote output on, each with
  the tasklet that wrote it: everything a rewind to `to_iter` would have to
  undo, read from the stage entries alone, since compaction may have truncated
  the records themselves. The rewind guard refuses when one of these tasklets
  cannot compensate.
  """
  @spec staged_channels_above(binary(), non_neg_integer()) ::
          [{non_neg_integer(), binary()}]
  def staged_channels_above(instance_id, to_iter) when is_binary(instance_id) do
    for %{type: :task_committed, iter: iter, task_idx: task_idx, payload: payload} <-
          live(instance_id),
        iter > to_iter,
        cell <- payload.cells_ran,
        channel <- cell.channels,
        uniq: true,
        do: {task_idx, channel}
  end

  @doc """
  The effect records the live `task_committed` and `compensated` entries name,
  as `first..last` ranges per Cell identity, in journal order. A Cell writer
  holds its workflow Cell's outputs until a range names them; one its Cell
  wrote outside every range, between two ranges or above the last, belongs to
  an attempt that never committed (a crash, a failed stage, an unwound
  iteration). The cut never drops an entry whose records are still owed, so
  every record above a Cell's flush cursor that did commit is named here.
  """
  @spec committed_records(binary()) ::
          %{optional(binary()) => [{non_neg_integer(), non_neg_integer()}]}
  def committed_records(instance_id) when is_binary(instance_id) do
    for %{type: type, payload: %{cells_ran: cells}} <- live(instance_id),
        type in [:task_committed, :compensated],
        {id, first, last} <- record_ranges(cells),
        reduce: %{} do
      acc -> Map.update(acc, id, [{first, last}], &(&1 ++ [{first, last}]))
    end
  end

  @doc """
  The records a stage entry's Cells wrote, `{cell_id, first_seq, seq}` for
  each Cell that wrote any.
  """
  @spec record_ranges([committed_cell()]) :: [{binary(), non_neg_integer(), non_neg_integer()}]
  def record_ranges(cells),
    do:
      for(
        %{cell_id: id, first_seq: first, seq: last} <- cells,
        is_integer(last),
        do: {id, first, last}
      )

  @doc """
  Ask the writer of the Cell `cell_id` names to answer once its record at
  `seq` is durable (`WalWriter.await_durable_async/4`, under `label` into
  `reqids`). A Cell whose writer cannot be reached is refused with the reason
  a barrier naming it fails with.
  """
  @spec await_record_async(binary(), non_neg_integer(), term(), term()) ::
          {:ok, term()} | {:error, String.t()}
  def await_record_async(cell_id, seq, reqids, label) do
    case record_writer(cell_id) do
      {:ok, writer} -> {:ok, WalWriter.await_durable_async(writer, seq, reqids, label)}
      {:error, _reason} -> {:error, record_not_durable(cell_id, seq)}
    end
  end

  @doc "Why a barrier naming the record at `seq` of `cell_id` fails."
  @spec record_not_durable(binary(), non_neg_integer()) :: String.t()
  def record_not_durable(cell_id, seq),
    do: "record_not_durable: #{Outbox.cell_label(cell_id)} seq=#{seq}"

  @doc """
  The entry naming `records` (`record_ranges/1`) is durable: each Cell's
  writer releases their outputs.
  """
  @spec release_records([{binary(), non_neg_integer(), non_neg_integer()}]) :: :ok
  def release_records(records) do
    for {id, first, last} <- records,
        {:ok, writer} <- [record_writer(id)],
        do: WalWriter.commit_records(writer, first, last)

    :ok
  end

  @doc """
  The outbox entries the instance's live Cell writers still owe the sinks:
  dispatchable, or in the chain awaiting confirmation. An entry held for a
  stage marker is owed nothing, and a writer that is not running, or exits
  before it answers, holds its debt on disk for its next boot.
  """
  @spec owed_outputs(binary()) :: non_neg_integer()
  def owed_outputs(instance_id) do
    for writer <- Supervisor.live_writers(instance_id),
        %{pending: pending, awaiting: awaiting} <- [WalWriter.outbox_status(writer)],
        reduce: 0,
        do: (owed -> owed + pending + awaiting)
  end

  defp record_writer(cell_id) do
    with {:ok, cell} <- Wire.decode_cell_id(cell_id), do: Supervisor.find_or_start(cell)
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
      owed = for %{seq: seq} = entry <- entries, seq < head, owes_output?(entry), do: seq

      unwound =
        case rewind_undo_entries(raw) do
          [] -> []
          undo -> [latest_rewound(raw).seq | Enum.map(undo, & &1.seq)]
        end

      Enum.min([head | owed ++ unwound])
    end
  end

  # A stage entry naming a record above its Cell's persisted flush cursor. The
  # cursor lags what the sinks confirmed, never leads it, so this keeps an entry
  # a little longer than needed and never too briefly.
  defp owes_output?(%{type: type, payload: %{cells_ran: cells}})
       when type in [:task_committed, :compensated] do
    Enum.any?(record_ranges(cells), fn {id, _first, seq} ->
      case Wire.decode_cell_id(id) do
        {:ok, cell} -> seq > Outbox.read(cell).outbox_flushed_seq
        {:error, _} -> false
      end
    end)
  end

  defp owes_output?(_entry), do: false

  defp committed_task(%{task_idx: task_idx, iter: iter, payload: payload}),
    do: %{
      task_idx: task_idx,
      iter: iter,
      cells_ran: Enum.map(payload.cells_ran, & &1.cell_id),
      completion: payload.completion,
      trigger: payload.trigger
    }

  defp latest_iter_committed(entries) do
    entries
    |> Enum.filter(&(&1.type == :iter_committed))
    |> case do
      [] -> nil
      committed -> Enum.max_by(committed, & &1.seq)
    end
  end

  defp consumed_offset_of(nil), do: nil
  defp consumed_offset_of(%{payload: %{consumed_offset: offset}}), do: offset

  defp entry_channel_of(nil), do: ""
  defp entry_channel_of(%{payload: %{entry_channel: channel}}), do: channel

  defp completion_of(nil), do: nil
  defp completion_of(%{payload: %{completion: completion}}), do: completion

  # Seed for a fresh iteration: the previous iteration's terminal handoff (a
  # handoff-chained loop's input; ignored by an input-driven one, which re-seeds
  # from its input log's undelivered tail).
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
  The instance's current epoch, part of every barrier's identity, so a re-run
  of a stage a rewind unwound is a new entry rather than a re-send of the old
  one. `0` until a rewind bumps it.
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
  (`:wal_high_watermark_bytes`, default 16 MiB). Covers the entry's handoff
  and the Cells it names.
  """
  @spec max_entry_bytes() :: pos_integer()
  def max_entry_bytes(), do: PersistenceResolver.wal_high_watermark_bytes()

  # ---------- Append ----------

  @barrier_types [@type_task_committed, @type_iter_committed, @type_compensated]

  defp append_barrier(instance_id, barrier) do
    {type, task_idx, iter, payload} = barrier_record(barrier)
    append(instance_id, type, task_idx, iter, payload)
  end

  defp barrier_record({:task_committed, task_idx, iter, cells_ran, handoff, completion, trigger}),
    do:
      {@type_task_committed, task_idx, iter,
       [
         Codec.encode_cells(cells_ran),
         Codec.encode_handoff(handoff),
         Codec.encode_completion(completion),
         Codec.encode_trigger(trigger)
       ]}

  defp barrier_record({:compensated, task_idx, iter, cells_ran}),
    do: {@type_compensated, task_idx, iter, Codec.encode_cells(cells_ran)}

  defp barrier_record(
         {:iter_committed, iter, consumed_offset, entry_channel, handoff, completion}
       ),
       do:
         {@type_iter_committed, 0, iter,
          [
            Codec.encode_offset(consumed_offset),
            <<byte_size(entry_channel)::16-big>>,
            entry_channel,
            Codec.encode_handoff(handoff),
            Codec.encode_completion(completion)
          ]}

  # `epoch` defaults to the instance's current one (a journal read); a rewind
  # passes the bumped one. An entry the journal already holds (a re-sent
  # barrier) is `:ok`: it is durable.
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

  # A barrier is batched and idempotent by identity; a rewind syncs inline:
  # rare, and durable before the eviction.
  defp append_opts(@type_rewound), do: [sync: :sync]
  defp append_opts(type) when type in @barrier_types, do: [sync: :batched, dedup: true]
end
