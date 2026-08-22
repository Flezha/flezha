defmodule Flezha.State.Commands do
  @moduledoc """
  The controller→SDK control commands that carry durable state.

  Restoring an Entity, pulling a snapshot, threading a workflow instance's
  proc-journal onto its bind, and telling a Procedure its target Entity's
  mailbox is full are all one thing: a control command the State subsystem
  issues on behalf of an operative it does not own a process for. The callers
  are `Flezha.State.Watchdog`, `Flezha.State.Recovery`,
  `Flezha.State.WalWriter`, `Flezha.Runner.ControlChannel` and
  `Flezha.Service.CliManager` — none of them an Overwatch, none of them holding
  operative state.

  Each entity-scoped function takes the controller-side `runner_id` and a
  structured `entity_id` triple `{operative_class, procedure_id, entity_key}`.
  `proc_journal_state/1` is the instance-scoped sibling: a workflow instance's
  journal is not per-Entity, which is why it rides the bind rather than a
  per-Entity restore.
  """

  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.ProcJournal
  alias Flezha.State.Registry
  alias Flezha.State.Restore
  alias Flezha.State.Wire

  require Logger

  # Bound on the CompactEntityResponse round-trip. Generous because the SDK
  # serializes the whole Entity State under its lock before replying.
  @compact_response_timeout_ms 30_000

  # ---------- restore ----------

  @doc """
  Ship a `RestoreInitState` command to the SDK and return the controller's
  freshly assembled payload. The caller is expected to await the
  matching `RestoreInitStateResponse` over the control channel before
  unblocking input streaming for the Entity.
  """
  @spec dispatch_restore_init_state(binary(), {binary(), binary(), binary()}) ::
          {:ok, reference()} | {:error, term()}
  def dispatch_restore_init_state(runner_id, entity_id) do
    case restore_command(entity_id) do
      {:ok, command} -> ControlChannel.send_control(runner_id, command)
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
  def await_restore_init_state(runner_id, entity_id) do
    with {:ok, command} <- restore_command(entity_id) do
      # The ONE awaited-control-round-trip seam; it owns the reply-ref
      # bookkeeping and the timeout, so a caller must never hand the
      # ControlUnit a raw `{:send_control, _}` of its own shape.
      case ControlUnit.send_control_await(runner_id, command, timeout: restore_await_timeout_ms()) do
        {:error, reason} = err ->
          Logger.warning(
            "[Overwatch] awaited restore failed entity=#{inspect(entity_id)}: #{inspect(reason)}"
          )

          err

        ok ->
          ok
      end
    end
  end

  defp restore_command({rc, sh, pk}) do
    case Restore.assemble_payload({rc, sh, pk}) do
      {:ok, payload} ->
        entity_id_bytes = Wire.encode_entity_id(rc, sh, pk)
        markers = Registry.compensation_markers({rc, sh, pk})

        {:ok,
         {:restore_init_state,
          %RunnerCommands.RestoreInitState{
            entity_id: entity_id_bytes,
            snapshot_bytes: payload.snapshot_bytes,
            change_log_bytes: payload.change_log_bytes,
            has_compensation_intent: markers.intent,
            has_compensation_done: markers.done,
            # A snapshot-only restore has no change-log record to re-arm
            # the SDK's seq from; the hint covers the snapshot.
            terminal_seq: max(payload.terminal_seq_hint, 0)
          }}}
    end
  end

  defp restore_await_timeout_ms(),
    do: Application.get_env(:flezha, :restore_await_timeout_ms, 15_000)

  # ---------- proc journal ----------

  @doc """
  Workflow proc-journal crash-resume: the instance's committed proc state, threaded
  onto `AddOperative.proc_journal` so the resumed driver skips the journaled
  procedures, seeds the next one from the journaled handoff, and enters the
  iteration after the last committed one.

  It rides the bind rather than a per-Entity `RestoreInitState` because it is
  instance-scoped: a durable workflow class with no per-Entity state ships no
  restore at all, and would otherwise resume at iteration 0 on every wake.

  `nil` when the instance has no journal — a fresh spawn, a stream
  operative, an opted-out class.
  """
  @spec proc_journal_state(binary()) :: RunnerCommands.ProcJournalState.t() | nil
  def proc_journal_state(instance_id) do
    case ProcJournal.restore_state(instance_id) do
      nil ->
        nil

      %{} = resume ->
        %RunnerCommands.ProcJournalState{
          committed_proc_idxs: resume.committed_proc_idxs,
          iter: resume.iter,
          # The previous iteration's terminal batch — what re-seeds iteration
          # N+1 of a handoff-chained loop.
          handoff: handoff_items(resume.handoff),
          # What each procedure of the in-flight iteration released. A branching
          # tree re-seeds every uncommitted child from its own parent's edge, so
          # each in-flight commit's handoff travels, not just the latest one's.
          proc_handoffs:
            Enum.map(resume.proc_handoffs, fn %{proc_idx: proc_idx, handoff: handoff} ->
              %RunnerCommands.ProcEdgeHandoff{
                proc_idx: proc_idx,
                handoff: handoff_items(handoff)
              }
            end),
          # Every forward proc commit with its iteration: the driver folds these
          # back into the compensation planner's per-iteration `committed` input,
          # which is what makes a rollback span journaled iterations (and what
          # rebuilds the in-flight iteration's completed stages on resume).
          committed_procs: Enum.map(resume.committed_procs, &committed_proc/1),
          # The procedures a rewind unwound and nothing has compensated yet. The
          # driver plans these through the ordinary compensation planner before
          # resuming forward.
          rewind_undo: Enum.map(resume.rewind_undo, &committed_proc/1),
          consumed_offset: resume.consumed_offset,
          parked: resume.parked,
          wake_deadline_ms: resume.wake_deadline_ms || 0
        }
    end
  end

  defp committed_proc(proc) do
    %RunnerCommands.ProcCommittedProc{
      proc_idx: proc.proc_idx,
      iter: proc.iter,
      entities_ran: proc.entities_ran
    }
  end

  defp handoff_items(handoff) do
    Enum.map(handoff, fn {cid, bytes} ->
      %RunnerCommands.ProcHandoffItem{channel_id: cid, payload: bytes}
    end)
  end

  # ---------- compaction ----------

  @doc """
  Ship a `CompactEntity` command to the SDK. The SDK responds with a
  `entity_state_snapshot` data frame (handled by
  `Flezha.Runner.StreamChannel`) followed by a
  `CompactEntityResponse` control reply. The reply is inspected off-process:
  a `{snapshot_seq: 0, snapshot_bytes_size: 0}` answer against a non-empty WAL
  is recorded as a failed handshake rather than a completion.
  """
  @spec dispatch_compact_entity(binary(), {binary(), binary(), binary()}) ::
          {:ok, term()} | {:error, term()}
  def dispatch_compact_entity(runner_id, {rc, sh, pk} = entity_id) do
    entity_id_bytes = Wire.encode_entity_id(rc, sh, pk)

    command =
      {:compact_entity, %RunnerCommands.CompactEntity{entity_id: entity_id_bytes}}

    case GenServer.whereis(Flezha.Service.Identificator.control_unit(runner_id)) do
      nil ->
        # No request/reply layer available (isolated unit tests): fire the
        # command directly. Stub detection still rides the timeout → backoff →
        # give-up path, so the missing inspection is not a coverage gap.
        case ControlChannel.send_control(runner_id, command) do
          {:ok, _ref} = ok ->
            Flezha.State.Watchdog.compaction_started(entity_id, runner_id)
            ok

          other ->
            other
        end

      _ ->
        # Bound the handshake: a snapshot that never arrives is timed out by the
        # Watchdog rather than left to wedge persistence.
        Flezha.State.Watchdog.compaction_started(entity_id, runner_id)

        # Inspect the confirming CompactEntityResponse off the caller's process:
        # serializing a large State under the entity lock can be slow, and
        # blocking the Watchdog sweep / WalWriter baseline pull on the round-trip
        # would stall unrelated entities. The `ControlUnit` request/reply sends
        # ONE command (via `send_control` internally) and awaits the reply. The
        # snapshot itself rides the data stream (driving `compaction_completed`);
        # this only catches a stub reply — a snapshot the SDK never ships —
        # sooner than the timeout sweep would.
        spawn(fn -> inspect_compact_response(runner_id, entity_id, command) end)
        {:ok, :dispatched}
    end
  end

  # A `CompactEntityResponse{snapshot_seq: 0, snapshot_bytes_size: 0}` means the
  # SDK produced no snapshot. Against a non-empty WAL that is a FAILED handshake
  # (a stub build, or an Entity the runner cannot address), never a completion —
  # record it so the strike/backoff path runs instead of waiting out the timeout.
  # Against an empty WAL there was genuinely nothing to snapshot, so it is not an
  # error.
  defp inspect_compact_response(runner_id, {rc, sh, pk} = entity_id, command) do
    with {:ok, raw_data} <-
           ControlUnit.send_control_await(runner_id, command,
             timeout: @compact_response_timeout_ms
           ),
         {:ok, %RunnerCommands.CompactEntityResponse{snapshot_seq: 0, snapshot_bytes_size: 0}} <-
           RunnerCommands.CompactEntityResponse.decode(raw_data),
         true <- Restore.has_persisted_state?(entity_id) do
      :telemetry.execute(
        [:flezha, :state, :compaction_failed],
        %{},
        %{operative: rc, procedure: sh, key: pk, runner: runner_id, reason: :no_snapshot}
      )

      Flezha.State.Watchdog.compaction_failed(entity_id)
    else
      {:error, reason} ->
        Logger.warning(
          "[Overwatch] compact_entity response inspection failed " <>
            "entity=#{inspect(entity_id)}: #{inspect(reason)}"
        )

      _ ->
        # A real snapshot (non-zero seq/size) or an empty-WAL no-op: the
        # data-stream frame path handles completion.
        :ok
    end
  end

  # ---------- mailbox back-pressure ----------

  @doc """
  Ship an `EntityMailboxStatus` notification to the SDK so the
  producing Procedure's `OverflowPolicy` machinery can park or unpark
  emits to this Entity. `status` is `:full` or `:open`.
  """
  @spec notify_entity_backpressure({binary(), binary(), binary(), binary()}, :full | :open) ::
          :ok | {:error, term()}
  def notify_entity_backpressure({runner_id, rc, sh, pk}, status)
      when status in [:full, :open] do
    entity_id_bytes = Wire.encode_entity_id(rc, sh, pk)

    command =
      {:entity_mailbox_status,
       %RunnerCommands.EntityMailboxStatus{
         entity_id: entity_id_bytes,
         status: Atom.to_string(status)
       }}

    ControlChannel.send_control_async(runner_id, command)
  end
end
