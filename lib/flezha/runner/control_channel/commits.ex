defmodule Flezha.Runner.ControlChannel.Commits do
  @moduledoc """
  The durable-barrier half of a runner's control channel: one process per
  runner that journals every `TaskCommit` / `IterCommit` the channel's reader
  decodes and acks it once the batch holding the record has synced.

  The reader forwards and returns, so a commit storm never queues a ping, a
  stop, a park or a compaction behind disk I/O. Every wait is issued without
  blocking: the in-flight request collection IS the set of answers owed.

  Invariant: records, then journal, then ack. A stage barrier names the effect
  records its Cells wrote; the entry is appended only once each is durable on
  its Cell's writer, and the ack carries the append's outcome in its `error`
  field (empty = durable). The ack gates the post-ack work: releasing the
  named records' outputs, and for an iteration commit the input-log release
  and the journal cut. The ack itself is written by the channel, the one
  process that writes control frames.
  """

  use GenServer

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.State.{DurabilityTier, TaskJournal}

  def start_link({namespace, runner_id}) do
    GenServer.start_link(__MODULE__, {namespace, runner_id},
      name: Identificator.commits(runner_id)
    )
  end

  @doc "Hand a decoded barrier frame over; the reader returns at once."
  @spec commit(binary(), binary(), RunnerCommands.TaskCommit.t() | RunnerCommands.IterCommit.t()) ::
          :ok
  def commit(runner_id, reference_id, frame),
    do: GenServer.cast(Identificator.commits(runner_id), {:commit, reference_id, frame})

  @doc """
  Hand over a barrier from a runner that does not own its instance: it is
  answered with an error and journals nothing.
  """
  @spec refuse(binary(), binary(), RunnerCommands.TaskCommit.t() | RunnerCommands.IterCommit.t()) ::
          :ok
  def refuse(runner_id, reference_id, frame),
    do: GenServer.cast(Identificator.commits(runner_id), {:refuse, reference_id, frame})

  @impl true
  def init({namespace, runner_id}) do
    {:ok,
     %{
       namespace: namespace,
       runner_id: runner_id,
       reqids: :gen_server.reqids_new(),
       waiting: %{}
     }}
  end

  # A barrier whose first copy is still waiting on its records is a re-send:
  # the one wait answers both. One naming records waits for each to be durable
  # before its entry is appended; any other is appended at once.
  @impl true
  def handle_cast({:commit, reference_id, frame}, state) do
    if Map.has_key?(state.waiting, reference_id) do
      {:noreply, state}
    else
      commit = decode(frame, state)

      case commit.records do
        [] -> {:noreply, append(state, reference_id, commit)}
        records -> {:noreply, await_records(state, reference_id, commit, records)}
      end
    end
  end

  def handle_cast({:refuse, reference_id, frame}, state) do
    commit = decode(frame, state)
    error = {:error, {:not_owner, commit.operator_id}}
    {:noreply, ack(state, reference_id, commit, error)}
  end

  # A synchronous-durability barrier finished its remote ship: ack with its
  # outcome. A failed ship rides back in the ack's `error` and the SDK aborts
  # the run — the class asked for RPO 0, so a barrier that is not remotely
  # durable must not be reported as committed.
  @impl true
  def handle_info({:commit_shipped, reference_id, commit, ship}, state),
    do: {:noreply, ack(state, reference_id, commit, ship)}

  # The ack deadline elapsed with a named record still not durable.
  def handle_info({:records_overdue, reference_id}, state) do
    case Map.fetch(state.waiting, reference_id) do
      {:ok, %{owed: owed}} ->
        [{cell_id, seq} | _] = Enum.sort(owed)

        {:noreply,
         records_failed(state, reference_id, TaskJournal.record_not_durable(cell_id, seq))}

      :error ->
        {:noreply, state}
    end
  end

  # A Cell writer's answer to a record wait, or a journal writer's to an
  # append (`{:ok, seq}` or `{:duplicate, seq}` once the record is synced),
  # or either writer's exit, which the monitored request reports the same way.
  def handle_info(msg, state) do
    case :gen_server.check_response(msg, state.reqids, true) do
      {answer, {:record, reference_id, cell_id, seq}, reqids} ->
        {:noreply, record_answered(%{state | reqids: reqids}, reference_id, cell_id, seq, answer)}

      {{:reply, _synced}, {reference_id, commit}, reqids} ->
        {:noreply, settle(%{state | reqids: reqids}, reference_id, commit, :ok)}

      {{:error, {reason, _writer}}, {reference_id, commit}, reqids} ->
        {:noreply, settle(%{state | reqids: reqids}, reference_id, commit, {:error, reason})}

      _no_reply_or_no_request ->
        Logger.warning(
          "[control_channel] unexpected message at the commit worker: #{inspect(msg)}"
        )

        {:noreply, state}
    end
  end

  # One wait per named record, on its Cell's writer; the deadline is the one
  # the SDK re-sends on. A Cell whose writer cannot be reached fails the
  # barrier at once.
  defp await_records(state, reference_id, commit, records) do
    Enum.reduce_while(records, {:ok, state.reqids}, fn {cell_id, _first, seq}, {:ok, reqids} ->
      label = {:record, reference_id, cell_id, seq}

      case TaskJournal.await_record_async(cell_id, seq, reqids, label) do
        {:ok, reqids} -> {:cont, {:ok, reqids}}
        {:error, reason} -> {:halt, reason}
      end
    end)
    |> case do
      {:ok, reqids} ->
        timer =
          Process.send_after(
            self(),
            {:records_overdue, reference_id},
            Durations.get(:sdk_commit_ack_ms)
          )

        owed = MapSet.new(records, fn {cell_id, _first, seq} -> {cell_id, seq} end)
        wait = %{commit: commit, owed: owed, timer: timer}
        %{state | reqids: reqids, waiting: Map.put(state.waiting, reference_id, wait)}

      reason ->
        settle(state, reference_id, commit, {:error, reason})
    end
  end

  defp record_answered(state, reference_id, cell_id, seq, answer) do
    case {Map.fetch(state.waiting, reference_id), answer} do
      # A wait the barrier no longer holds: it failed, or its deadline passed.
      {:error, _answer} ->
        state

      {{:ok, wait}, {:reply, {:ok, _seq}}} ->
        owed = MapSet.delete(wait.owed, {cell_id, seq})

        if MapSet.size(owed) == 0 do
          Process.cancel_timer(wait.timer)
          state = %{state | waiting: Map.delete(state.waiting, reference_id)}
          append(state, reference_id, wait.commit)
        else
          put_in(state.waiting[reference_id].owed, owed)
        end

      {{:ok, _wait}, _refused_or_gone} ->
        records_failed(state, reference_id, TaskJournal.record_not_durable(cell_id, seq))
    end
  end

  defp records_failed(state, reference_id, reason) do
    {%{commit: commit, timer: timer}, waiting} = Map.pop(state.waiting, reference_id)
    Process.cancel_timer(timer)
    settle(%{state | waiting: waiting}, reference_id, commit, {:error, reason})
  end

  # The append is issued, not awaited; an entry refused before the write (an
  # oversized payload) is acked with its error at once.
  defp append(state, reference_id, commit) do
    case TaskJournal.append_async(
           commit.operator_id,
           commit.barrier,
           state.reqids,
           {reference_id, commit}
         ) do
      {:ok, reqids} -> %{state | reqids: reqids}
      {:error, _} = error -> settle(state, reference_id, commit, error)
    end
  end

  # The append's outcome is known: report it, then the durability gate around
  # the ack. A class that declared synchronous remote durability must have its
  # barrier on the remote store BEFORE the ack, so the ack the SDK observes
  # implies remote durability (RPO 0); the ship runs in a task and replies
  # through this process. Every other class acks on the local sync and reaches
  # the remote store asynchronously, off this path.
  defp settle(state, reference_id, commit, result) do
    {event, measurements, metadata} = commit.telemetry
    :telemetry.execute(event, measurements, Map.put(metadata, :status, status(result)))

    if result == :ok and DurabilityTier.sync_class?(commit.operator_id) do
      worker = self()

      Task.start(fn ->
        send(
          worker,
          {:commit_shipped, reference_id, commit, DurabilityTier.commit_sync(commit.operator_id)}
        )
      end)

      state
    else
      ack(state, reference_id, commit, result)
    end
  end

  # The protocol's core invariant, in one place: the reply to a durability
  # handshake is emitted only AFTER the journal answered, carries the append's
  # outcome in its `error` field (absent = durable), and gates whatever post-ack
  # work the barrier owes.
  defp ack(state, reference_id, commit, result) do
    error =
      case result do
        :ok -> nil
        {:error, reason} -> Flezha.Error.from_reason(reason, :"wire.commit")
      end

    if error,
      do:
        Logger.warning(
          "[control_channel] #{commit.label} failed: #{error.message} (#{error.code})"
        )

    {tag, response} = commit.response

    ControlChannel.send_control_async(
      state.runner_id,
      {tag, %{response | error: error && Flezha.Error.to_wire(error)}},
      reference_id
    )

    if is_nil(error) do
      commit.post.()
      DurabilityTier.on_bounded_commit(commit.operator_id)
    end

    state
  end

  defp status(:ok), do: :ok
  defp status({:error, _}), do: :error

  # Task-boundary commit (bounded-task-journal): ONE `task_committed` entry
  # (the Cells the stage ran, each with the records it wrote, and the handoff),
  # appended once those records are durable and acked once the entry is. The
  # ack IS the durable point the driver gates the next tasklet on, and the
  # point past which the named records' outputs may be delivered: their Cells'
  # writers release them after it. `compensating` marks the undo twin, the
  # records a compensation hook wrote, journaled as a `compensated` entry.
  defp decode(%RunnerCommands.TaskCommit{} = commit, state) do
    %RunnerCommands.TaskCommit{
      operator_id: operator_id,
      task_idx: task_idx,
      iter: iter,
      cells_ran: cells_ran,
      handoff: handoff,
      compensating: compensating,
      completes: completes,
      result: result,
      trigger: trigger
    } = commit

    cells = Enum.map(cells_ran, &committed_cell/1)
    records = TaskJournal.record_ranges(cells)

    barrier =
      if compensating,
        do: {:compensated, task_idx, iter, cells},
        else:
          {:task_committed, task_idx, iter, cells, handoff_items(handoff),
           completion(completes, result), trigger(trigger)}

    %{
      operator_id: operator_id,
      barrier: barrier,
      records: records,
      telemetry:
        {[:flezha, :state, :task_commit],
         %{count: 1, handoff_items: length(handoff), cells: length(cells)},
         %{
           namespace: state.namespace,
           operator_id: operator_id,
           task_idx: task_idx,
           compensating: compensating
         }},
      label: "task-journal commit operator=#{inspect(operator_id)} task_idx=#{task_idx}",
      response:
        {:task_commit_response,
         %RunnerCommands.TaskCommitResponse{operator_id: operator_id, task_idx: task_idx}},
      post: fn -> TaskJournal.release_records(records) end
    }
  end

  # Iteration boundary of a durable structured loop (`bounded-durable-loop`):
  # ONE `iter_committed(iter, consumed_offset)` record — with the iteration's
  # terminal handoff, so a crash can re-seed the next iteration — then the ack.
  # Everything after the ack is cleanup the commit does not depend on: the
  # report of the input it consumed into the input log's watermark (whose
  # retention sweep then reclaims the prefix) and the cut of the journal prefix
  # the fold no longer needs. A failure in either costs work, never
  # correctness: the iteration is already committed. The cuts are the
  # writers' to run, so this process never waits on a file rewrite.
  defp decode(%RunnerCommands.IterCommit{} = commit, state) do
    %RunnerCommands.IterCommit{
      operator_id: operator_id,
      iter: iter,
      consumed_offset: consumed_offset,
      handoff: handoff,
      completes: completes,
      result: result,
      entry_channel: entry_channel
    } = commit

    %{
      operator_id: operator_id,
      barrier:
        {:iter_committed, iter, consumed_offset, entry_channel, handoff_items(handoff),
         completion(completes, result)},
      records: [],
      telemetry:
        {[:flezha, :state, :iter_commit], %{count: 1},
         %{
           namespace: state.namespace,
           operator_id: operator_id,
           iter: iter,
           consumed_offset: consumed_offset
         }},
      label: "task-journal iteration commit operator=#{inspect(operator_id)} iter=#{iter}",
      response:
        {:iter_commit_response,
         %RunnerCommands.IterCommitResponse{operator_id: operator_id, iter: iter}},
      post: fn ->
        TaskJournal.release_input(operator_id, consumed_offset)
        TaskJournal.cut(operator_id)
      end
    }
  end

  defp committed_cell(%RunnerCommands.CommittedCell{} = cell),
    do: %{
      cell_id: cell.cell_id,
      first_seq: cell.first_seq || cell.seq,
      seq: cell.seq,
      channels: cell.channels
    }

  defp handoff_items(handoff) do
    Enum.map(handoff, fn %RunnerCommands.TaskHandoffItem{channel_id: cid, payload: bytes} ->
      {cid, bytes}
    end)
  end

  defp completion(true, result), do: result
  defp completion(false, _result), do: nil

  defp trigger(nil), do: nil

  defp trigger(%RunnerCommands.TimerTrigger{} = trigger),
    do: %{
      cell_key: trigger.cell_key,
      kind: if(trigger.event_time, do: :event_time, else: :processing_time),
      scheduled_at: trigger.scheduled_at
    }
end
