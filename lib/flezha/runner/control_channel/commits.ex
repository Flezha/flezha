defmodule Flezha.Runner.ControlChannel.Commits do
  @moduledoc """
  The durable-barrier half of a runner's control channel: one process per
  runner that journals every `TaskCommit` / `IterCommit` the channel's reader
  decodes and acks it once the batch holding the record has synced.

  The reader forwards and returns, so a commit storm never queues a ping, a
  stop, a park or a compaction behind disk I/O. Appends are issued without
  waiting: the in-flight request collection IS the set of acks owed.

  Invariant: journal-then-ack. The ack carries the append's outcome in its
  `error` field (empty = durable) and gates the post-ack work — the outbox
  flush, and for an iteration commit the boundary snapshot, the inbox release
  and the journal cut. The ack itself is written by the channel, the one
  process that writes control frames.
  """

  use GenServer

  require Logger

  alias Flezha.Config.Backoff
  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.State.Commands
  alias Flezha.State.{DurabilityTier, TaskInbox, TaskJournal}
  alias Flezha.State.Paths

  # Re-attempt cadence for a journaled outbox whose operative is not bound yet
  # (mirrors the stream outbox's `:outbox_retry` in `Flezha.State.WalWriter`).

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
    {:ok, %{namespace: namespace, runner_id: runner_id, reqids: :gen_server.reqids_new()}}
  end

  # The append is issued, not awaited; an entry refused before the write (an
  # oversized payload) is acked with its error at once.
  @impl true
  def handle_cast({:commit, reference_id, frame}, state) do
    commit = decode(frame, state)

    case TaskJournal.append_async(
           commit.operative_id,
           commit.barrier,
           state.reqids,
           {reference_id, commit}
         ) do
      {:ok, reqids} -> {:noreply, %{state | reqids: reqids}}
      {:error, _} = error -> {:noreply, settle(state, reference_id, commit, error)}
    end
  end

  def handle_cast({:refuse, reference_id, frame}, state) do
    commit = decode(frame, state)
    error = {:error, "not the owner of #{commit.operative_id}"}
    {:noreply, ack(state, reference_id, commit, error)}
  end

  # A synchronous-durability barrier finished its remote ship: ack with its
  # outcome. A failed ship rides back in the ack's `error` and the SDK aborts
  # the run — the class asked for RPO 0, so a barrier that is not remotely
  # durable must not be reported as committed.
  @impl true
  def handle_info({:commit_shipped, reference_id, commit, ship}, state),
    do: {:noreply, ack(state, reference_id, commit, ship)}

  # Retry a journaled outbox whose operative was not bound when the flush was
  # first attempted. The journal is the source of truth, so the retry simply
  # re-runs the same flush; it stops when nothing is owed.
  def handle_info({:task_outbox_retry, operative_id}, state) do
    flush_task_outbox(operative_id)
    {:noreply, state}
  end

  # A journal writer's answer — `{:ok, seq}` or `{:duplicate, seq}` once the
  # record is synced, or the writer's exit, which the monitored request reports
  # the same way.
  def handle_info(msg, state) do
    case :gen_server.check_response(msg, state.reqids, true) do
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

  # The append's outcome is known: report it, then the durability gate around
  # the ack. A class that declared synchronous remote durability must have its
  # barrier on the remote store BEFORE the ack, so the ack the SDK observes
  # implies remote durability (RPO 0); the ship runs in a task and replies
  # through this process. Every other class acks on the local sync and reaches
  # the remote store asynchronously, off this path.
  defp settle(state, reference_id, commit, result) do
    {event, measurements, metadata} = commit.telemetry
    :telemetry.execute(event, measurements, Map.put(metadata, :status, status(result)))

    if result == :ok and DurabilityTier.sync_class?(commit.operative_id) do
      worker = self()

      Task.start(fn ->
        send(
          worker,
          {:commit_shipped, reference_id, commit, DurabilityTier.commit_sync(commit.operative_id)}
        )
      end)

      state
    else
      ack(state, reference_id, commit, result)
    end
  end

  # The protocol's core invariant, in one place: the reply to a durability
  # handshake is emitted only AFTER the journal answered, carries the append's
  # outcome in its `error` field (empty = durable), and gates whatever post-ack
  # work the barrier owes.
  defp ack(state, reference_id, commit, result) do
    error =
      case result do
        :ok -> ""
        {:error, reason} when is_binary(reason) -> reason
        {:error, reason} -> inspect(reason)
      end

    if error != "", do: Logger.warning("[control_channel] #{commit.label} failed: #{error}")

    {tag, response} = commit.response

    ControlChannel.send_control_async(
      state.runner_id,
      {tag, %{response | error: error}},
      reference_id
    )

    if error == "" do
      commit.post.()
      DurabilityTier.on_workflow_commit(commit.operative_id)
    end

    state
  end

  defp status(:ok), do: :ok
  defp status({:error, _}), do: :error

  # Task-boundary commit (workflow-task-journal + workflow-transactional-outbox):
  # ONE `task_committed` record — cells ran, handoff, AND the tasklet's
  # staged outbox — acked once durable. The ack IS the durable point the driver
  # gates the next tasklet on, and the point past which the staged output may
  # finally be delivered: the flush runs AFTER it, and the next tasklet is
  # free to run while the flush lands (the chain hands data forward through the
  # handoff, never through the sink). `compensating` marks the undo twin: the
  # same handshake carrying a compensation hook's staged undo output, journaled
  # as a `compensated` entry.
  defp decode(%RunnerCommands.TaskCommit{} = commit, state) do
    %RunnerCommands.TaskCommit{
      operative_id: operative_id,
      task_idx: task_idx,
      iter: iter,
      cells_ran: cells_ran,
      handoff: handoff,
      outbox: outbox,
      compensating: compensating,
      completes: completes,
      result: result
    } = commit

    outbox =
      Enum.map(outbox, fn %RunnerCommands.TaskOutboxItem{} = item ->
        %{
          cell_id: item.cell_id,
          seq_in_task: item.seq_in_task,
          channel: item.channel,
          payload: item.payload
        }
      end)

    barrier =
      if compensating,
        do: {:compensated, task_idx, iter, outbox},
        else:
          {:task_committed, task_idx, iter, cells_ran, handoff_items(handoff), outbox,
           completion(completes, result)}

    %{
      operative_id: operative_id,
      barrier: barrier,
      telemetry:
        {[:flezha, :state, :task_commit],
         %{count: 1, handoff_items: length(handoff), outbox_items: length(outbox)},
         %{
           namespace: state.namespace,
           operative_id: operative_id,
           task_idx: task_idx,
           compensating: compensating
         }},
      label: "task-journal commit operative=#{inspect(operative_id)} task_idx=#{task_idx}",
      response:
        {:task_commit_response,
         %RunnerCommands.TaskCommitResponse{operative_id: operative_id, task_idx: task_idx}},
      post: fn -> flush_task_outbox(operative_id) end
    }
  end

  # Iteration boundary of a durable structured loop (`workflow-durable-loop`):
  # ONE `iter_committed(iter, consumed_offset)` record — with the iteration's
  # terminal handoff, so a crash can re-seed the next iteration — then the ack.
  # Everything after the ack is cleanup the commit does not depend on: the
  # boundary snapshot (so a rehydrate loads iteration-N state instead of
  # replaying the WAL tail), the iteration's staged outbox, the release of the
  # inbox prefix it consumed, and the cut of the journal prefix the fold no
  # longer needs. A failure in any of them costs work, never correctness — the
  # iteration is already committed. The flush runs here, in journal order; the
  # two cuts are the writers' to run, so this process never waits on a file
  # rewrite.
  defp decode(%RunnerCommands.IterCommit{} = commit, state) do
    %RunnerCommands.IterCommit{
      operative_id: operative_id,
      iter: iter,
      consumed_offset: consumed_offset,
      handoff: handoff,
      completes: completes,
      result: result
    } = commit

    runner_id = state.runner_id

    %{
      operative_id: operative_id,
      barrier:
        {:iter_committed, iter, consumed_offset, handoff_items(handoff),
         completion(completes, result)},
      telemetry:
        {[:flezha, :state, :iter_commit], %{count: 1, consumed_offset: consumed_offset},
         %{namespace: state.namespace, operative_id: operative_id, iter: iter}},
      label: "task-journal iteration commit operative=#{inspect(operative_id)} iter=#{iter}",
      response:
        {:iter_commit_response,
         %RunnerCommands.IterCommitResponse{operative_id: operative_id, iter: iter}},
      post: fn ->
        snapshot_iteration_boundary(runner_id, operative_id)
        flush_task_outbox(operative_id)
        if consumed_offset > 0, do: TaskInbox.truncate(operative_id, consumed_offset)
        TaskJournal.cut(operative_id)
      end
    }
  end

  defp handoff_items(handoff) do
    Enum.map(handoff, fn %RunnerCommands.TaskHandoffItem{channel_id: cid, payload: bytes} ->
      {cid, bytes}
    end)
  end

  defp completion(true, result), do: result
  defp completion(false, _result), do: nil

  # Boundary snapshot: ask the SDK to compact every Cell of the instance, so a
  # rehydrate restores iteration-N state from a snapshot instead of replaying the
  # whole WAL. Off this process: `CompactCell` is dispatched through
  # `send_control`, a call into the channel whose reader's frames feed this
  # process; the reply must not wait on an ack this process owes. Best-effort
  # besides: a failed or dropped compaction only means the restore falls back to
  # WAL replay, which the Watchdog's own size/age triggers will re-attempt.
  defp snapshot_iteration_boundary(runner_id, operative_id) do
    cells = Paths.cells_of(operative_id)

    Task.start(fn -> Enum.each(cells, &compact_cell(runner_id, &1)) end)
  end

  defp compact_cell(runner_id, cell_id) do
    case Commands.dispatch_compact_cell(runner_id, cell_id) do
      {:ok, _ref} ->
        :ok

      {:error, reason} ->
        Logger.debug(
          "[control_channel] boundary snapshot skipped for #{inspect(cell_id)}: " <>
            "#{inspect(reason)} — rehydrate falls back to WAL replay"
        )
    end
  end

  # Journal-driven flush: everything this instance owes, in journal order, with
  # the `outbox_flushed` marker written per entry after delivery. An unbound
  # operative (cold-boot re-drive before the bind lands) retries on a timer;
  # the journal still owes the entries, so nothing is dropped either way.
  defp flush_task_outbox(operative_id) do
    case TaskJournal.flush_pending(operative_id) do
      :ok ->
        :ok

      {:error, :no_overwatch} ->
        Process.send_after(
          self(),
          {:task_outbox_retry, operative_id},
          Backoff.arm(:task_outbox_retry, 1, operative_id)
        )

        :ok
    end
  end
end
