defmodule Flezha.State.Commands do
  @moduledoc """
  The controller->SDK control commands that carry durable state.

  Restoring a Cell, pulling a snapshot, threading a steps instance's
  task-journal onto its bind, and telling a Tasklet its target Cell's
  mailbox is full are all one thing: a control command the State subsystem
  issues on behalf of a relay it does not own a process for. Its callers —
  `Watchdog`, `Recovery`, `WalWriter`, `Runner.ControlChannel`,
  `Service.Steps` — are none of them an Overwatch and none of them holding
  relay state.

  Each cell-scoped function takes the controller-side `runner_id` and a
  structured `cell_id` triple `{relay_class, tasklet_id, cell_key}`.
  `task_journal_state/1` and `effect_results/1` are the instance-scoped
  siblings: a steps instance's journal and an instance's kept effect results
  are not per-Cell, which is why they ride the bind rather than a per-Cell
  restore.
  """

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.InputLog
  alias Flezha.State.Paths
  alias Flezha.State.TaskJournal
  alias Flezha.State.Restore
  alias Flezha.State.Watchdog
  alias Flezha.State.Wire
  require Logger

  # Bound on the CompactCellResponse round-trip. Generous because the SDK
  # serializes the whole Cell State under its lock before replying.

  # ---------- restore ----------

  @doc """
  Ship a `RestoreInitState` command to the SDK and return the controller's
  freshly assembled payload. The caller is expected to await the
  matching `RestoreInitStateResponse` over the control channel before
  unblocking input streaming for the Cell.
  """
  @spec dispatch_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_restore_init_state(runner_id, cell_id) do
    case restore_command(cell_id) do
      {:ok, command} -> ControlChannel.send_control(runner_id, command)
      {:error, _refused} = refused -> refused
    end
  end

  @doc """
  Ship a `RestoreInitState` and block until the SDK acknowledges it — the SDK
  has applied state and the consumed floor when the reply lands.

  The awaited form is what the effectively-once bind and the correction
  transaction need: the restore rides the control channel while the replay
  rides the data channel, and without this fence they race.
  """
  @spec await_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, binary()} | {:error, term()}
  def await_restore_init_state(runner_id, cell_id) do
    with {:ok, command} <- restore_command(cell_id) do
      # The ONE awaited-control-round-trip seam; it owns the reply-ref
      # bookkeeping and the timeout, so a caller must never hand the
      # ControlUnit a raw `{:send_control, _}` of its own shape.
      case ControlUnit.send_control_await(runner_id, command, timeout: restore_await_timeout_ms()) do
        {:error, reason} = err ->
          Logger.warning(
            "[Overwatch] awaited restore failed cell=#{inspect(cell_id)}: #{inspect(reason)}"
          )

          err

        {:ok, raw} = ok ->
          compact_after_migration(runner_id, cell_id, raw)
          ok
      end
    end
  end

  @doc """
  The effect results the bind ships: the ones the instance's input log keeps
  for its inputs above its floor. Their count rides
  `AddRelay.effect_result_count`, so a steps driver holds its first Tasklet
  until they have landed; `[]` for an instance that keeps none.
  """
  @spec effect_results(binary()) :: [%RunnerCommands.EffectResult{}]
  def effect_results(relay_class) do
    case InputLog.effect_results(relay_class, InputLog.watermark(relay_class) || -1) do
      results when is_list(results) ->
        for {offset, identity, cell, payload} <- results,
            do: %RunnerCommands.EffectResult{
              identity: identity,
              offset: offset,
              cell_identity: cell,
              payload: payload
            }

      {:error, _refused} ->
        []
    end
  end

  @doc """
  Ship one batch of an instance's kept effect results to the runner it is
  bound to and await the runner's answer, through the same awaited round trip
  a restore takes. A failure is logged and answered: the runner's gate stops
  waiting for the batch on its own deadline.
  """
  @spec ship_effect_results(binary(), binary(), [%RunnerCommands.EffectResult{}]) ::
          :ok | {:error, term()}
  def ship_effect_results(runner_id, relay_id, results) do
    command =
      {:effect_results, %RunnerCommands.EffectResults{relay_id: relay_id, results: results}}

    case ControlUnit.send_control_await(runner_id, command, timeout: restore_await_timeout_ms()) do
      {:ok, _raw} ->
        :ok

      {:error, reason} = err ->
        Logger.warning(
          "[Overwatch] effect results unshipped relay=#{inspect(relay_id)}: #{inspect(reason)}"
        )

        err
    end
  end

  # A restore that migrated records leaves the Cell current in memory and
  # stale on disk: its baseline is still the bytes an older schema version
  # wrote, so every future restore would migrate them again. Compaction is
  # what rewrites the baseline at the running version, and the controller
  # already knows how to drive it — the migration is then paid once.
  defp compact_after_migration(runner_id, {rc, sh, pk} = cell_id, raw) do
    case RunnerCommands.RestoreInitStateResponse.decode(raw) do
      {:ok, %RunnerCommands.RestoreInitStateResponse{migrated: true}} ->
        :telemetry.execute(
          [:flezha, :state, :schema_migration],
          %{count: 1},
          %{relay: rc, tasklet: sh, key: pk, runner: runner_id}
        )

        Logger.info(
          "[State] restore migrated records from an older schema version " <>
            "cell=#{inspect(cell_id)}; compacting to rebase the snapshot"
        )

        _ = dispatch_compact_cell(runner_id, cell_id)
        :ok

      _ ->
        :ok
    end
  end

  defp restore_command({rc, sh, pk}) do
    case Restore.payload({rc, sh, pk}, :latest) do
      {:ok, payload} ->
        cell_id_bytes = Wire.encode_cell_id(rc, sh, pk)
        markers = Paths.compensation_markers({rc, sh, pk})

        {:ok,
         {:restore_init_state,
          %RunnerCommands.RestoreInitState{
            cell_id: cell_id_bytes,
            snapshot_bytes: payload.snapshot_bytes,
            snapshot_header: payload.snapshot_header,
            change_log_bytes: payload.change_log_bytes,
            has_compensation_intent: markers.intent,
            has_compensation_done: markers.done,
            # A snapshot-only restore has no change-log record to re-arm
            # the SDK's seq from; the hint covers the snapshot.
            terminal_seq: max(payload.terminal_seq, 0)
          }}}

      # The Cell's own WAL refused a record. Shipping the records before it
      # would restore the Cell from a change log that was cut short and call
      # it whole; the reason is text, so the awaited path parks the relay
      # the way an SDK-reported refusal does.
      {:error, _reason} = refused ->
        refused
    end
  end

  defp restore_await_timeout_ms(),
    do: Durations.get(:restore_await_timeout_ms)

  # ---------- task journal ----------

  @doc """
  Steps relay task-journal crash-resume: the instance's committed task state, threaded
  onto `AddRelay.task_journal` so the resumed driver skips the journaled
  tasklets, seeds the next one from the journaled handoff, and enters the
  iteration after the last committed one.

  It rides the bind rather than a per-Cell `RestoreInitState` because it is
  instance-scoped: a durable steps class with no per-Cell state ships no
  restore at all, and would otherwise resume at iteration 0 on every wake.

  `nil` when the instance has no journal — a fresh spawn, a flow relay
  relay, an opted-out class.
  """
  @spec task_journal_state(binary()) :: %RunnerCommands.TaskJournalState{} | nil
  def task_journal_state(instance_id) do
    case TaskJournal.restore_state(instance_id) do
      nil ->
        nil

      %{} = resume ->
        %RunnerCommands.TaskJournalState{
          committed_task_idxs: resume.committed_task_idxs,
          iter: resume.iter,
          # The chain the last committed iteration ran, which a resumed
          # chained loop feeds its carry back into.
          entry_channel: resume.entry_channel,
          # The previous iteration's terminal batch — what re-seeds iteration
          # N+1 of a handoff-chained loop.
          handoff: handoff_items(resume.handoff),
          # What each tasklet of the in-flight iteration released. A branching
          # tree re-seeds every uncommitted child from its own parent's edge, so
          # each in-flight commit's handoff travels, not just the latest one's.
          task_handoffs:
            Enum.map(resume.task_handoffs, fn %{task_idx: task_idx, handoff: handoff} ->
              %RunnerCommands.TaskEdgeHandoff{
                task_idx: task_idx,
                handoff: handoff_items(handoff)
              }
            end),
          # Every forward task commit with its iteration: the driver folds these
          # back into the compensation planner's per-iteration `committed` input,
          # which is what makes a rollback span journaled iterations (and what
          # rebuilds the in-flight iteration's completed stages on resume).
          committed_tasks: Enum.map(resume.committed_tasks, &committed_task/1),
          # The tasklets a rewind unwound and nothing has compensated yet. The
          # driver plans these through the ordinary compensation planner before
          # resuming forward.
          rewind_undo: Enum.map(resume.rewind_undo, &committed_task/1),
          consumed_offset: resume.consumed_offset,
          released_offset: resume.released_offset,
          # The last committed iteration ended the instance: a resume ends it at
          # once with this result instead of waiting for input.
          completed: resume.completion != nil,
          result: resume.completion || <<>>
        }
    end
  end

  defp committed_task(task) do
    %RunnerCommands.TaskCommittedTask{
      task_idx: task.task_idx,
      iter: task.iter,
      cells_ran: task.cells_ran,
      completes: task.completion != nil,
      result: task.completion || <<>>,
      trigger: timer_trigger(task[:trigger])
    }
  end

  defp timer_trigger(nil), do: nil

  defp timer_trigger(trigger),
    do: %RunnerCommands.TimerTrigger{
      cell_key: trigger.cell_key,
      event_time: trigger.kind == :event_time,
      scheduled_at: trigger.scheduled_at
    }

  defp handoff_items(handoff) do
    Enum.map(handoff, fn {cid, bytes} ->
      %RunnerCommands.TaskHandoffItem{channel_id: cid, payload: bytes}
    end)
  end

  # ---------- compaction ----------

  @doc """
  Ship a `CompactCell` command to the SDK. The SDK responds with a
  `cell_state_snapshot` data frame (handled by
  `Flezha.Runner.StreamChannel`) followed by a
  `CompactCellResponse` control reply. The reply is inspected off-process:
  a `{snapshot_seq: 0, snapshot_bytes_size: 0}` answer against a non-empty WAL
  is recorded as a failed handshake rather than a completion, and a
  `state.cell_busy` refusal is left to the Watchdog to ask again, whoever
  dispatched it.
  """
  @spec dispatch_compact_cell(binary(), {binary(), binary(), binary()}) ::
          {:ok, term()} | {:error, term()}
  def dispatch_compact_cell(runner_id, {rc, sh, pk} = cell_id) do
    cell_id_bytes = Wire.encode_cell_id(rc, sh, pk)

    command =
      {:compact_cell, %RunnerCommands.CompactCell{cell_id: cell_id_bytes}}

    case GenServer.whereis(Identificator.control_unit(runner_id)) do
      nil ->
        # No request/reply layer available (isolated unit tests): fire the
        # command directly. Stub detection still rides the timeout → backoff →
        # give-up path, so the missing inspection is not a coverage gap.
        case ControlChannel.send_control(runner_id, command) do
          {:ok, _ref} = ok ->
            Watchdog.compaction_started(cell_id, runner_id)
            ok

          other ->
            other
        end

      _ ->
        # Bound the handshake: a snapshot that never arrives is timed out by the
        # Watchdog rather than left to wedge persistence.
        Watchdog.compaction_started(cell_id, runner_id)

        # Inspect the confirming CompactCellResponse off the caller's process:
        # serializing a large State under the cell lock can be slow, and
        # blocking the Watchdog sweep / WalWriter baseline pull on the round-trip
        # would stall unrelated cells. The `ControlUnit` request/reply sends
        # ONE command (via `send_control` internally) and awaits the reply. The
        # snapshot itself rides the data stream (driving `compaction_completed`);
        # this only catches a stub reply — a snapshot the SDK never ships —
        # sooner than the timeout sweep would.
        spawn(fn -> inspect_compact_response(runner_id, cell_id, command) end)
        {:ok, :dispatched}
    end
  end

  # A `CompactCellResponse{snapshot_seq: 0, snapshot_bytes_size: 0}` means the
  # SDK produced no snapshot. Against a non-empty WAL that is a FAILED handshake
  # (a stub build, or a Cell the runner cannot address), never a completion —
  # record it so the strike/backoff path runs instead of waiting out the timeout.
  # Against an empty WAL there was genuinely nothing to snapshot, so it is not an
  # error.
  defp inspect_compact_response(runner_id, {rc, sh, pk} = cell_id, command) do
    with {:ok, raw_data} <-
           ControlUnit.send_control_await(runner_id, command,
             timeout: Durations.get(:compaction_timeout_ms)
           ),
         {:ok, %RunnerCommands.CompactCellResponse{snapshot_seq: 0, snapshot_bytes_size: 0}} <-
           RunnerCommands.CompactCellResponse.decode(raw_data),
         true <- Restore.has_persisted_state?(cell_id) do
      :telemetry.execute(
        [:flezha, :state, :compaction_failed],
        %{},
        %{relay: rc, tasklet: sh, key: pk, runner: runner_id, reason: :no_snapshot}
      )

      Watchdog.compaction_failed(cell_id)
    else
      # The Cell holds changes no record carries yet (an open checkpoint
      # interval, a retried attempt): no snapshot now, and no strike either.
      {:error, %Flezha.Error{code: "state.cell_busy"}} ->
        Watchdog.compaction_deferred(cell_id)

      {:error, reason} ->
        Logger.warning(
          "[Overwatch] compact_cell response inspection failed " <>
            "cell=#{inspect(cell_id)}: #{inspect(reason)}"
        )

      _ ->
        # A real snapshot (non-zero seq/size) or an empty-WAL no-op: the
        # data-stream frame path handles completion.
        :ok
    end
  end

  # ---------- mailbox back-pressure ----------

  @doc """
  Ship a `CellMailboxStatus` notification to the SDK so the
  producing Tasklet's `OverflowPolicy` machinery can park or unpark
  emits to this Cell. `status` is `:full` or `:open`.
  """
  @spec notify_cell_backpressure({binary(), binary(), binary(), binary()}, :full | :open) ::
          :ok | {:error, term()}
  def notify_cell_backpressure({runner_id, rc, sh, pk}, status)
      when status in [:full, :open] do
    cell_id_bytes = Wire.encode_cell_id(rc, sh, pk)

    command =
      {:cell_mailbox_status,
       %RunnerCommands.CellMailboxStatus{
         cell_id: cell_id_bytes,
         status: Atom.to_string(status)
       }}

    ControlChannel.send_control_async(runner_id, command)
  end
end
