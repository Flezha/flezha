defmodule Flezha.Runner.StreamChannel.Records do
  @moduledoc """
  The operative route cache, the ownership check, the durable-record fan-out
  and the credit handshake.

  Every frame a runner sends on a Cell's write path arrives here: state
  changes, snapshots, deletions, compensation records, corrections and parks.
  Each is routed to the Cell's own writer. A frame from a session the
  takeover fence marked superseded is refused rather than written, and so is
  one from a runner the operative's ownership record does not name
  (`admit/3`), the check the control channel's requests are judged by too.

  Invariant: credit is granted per window and debited per record, so a runner
  that outruns its grant is reported once and throttled, never silently
  dropped.

  Invariant: every pid the route cache holds is monitored, so a cached
  Overwatch that dies cannot keep winning against the liveness-checked
  registry behind it.
  """

  alias Flezha.Config

  alias Flezha.Config.Durations
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Lookup
  alias Flezha.Operative.Replay
  alias Flezha.Process.Watch
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.State.CreditLedger
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Snapshot, as: StateSnapshot
  alias Flezha.State.WalWriter
  alias Flezha.State.Watchdog
  alias Flezha.SDK.Wire
  alias Flezha.Transport.LivenessCounters

  require Logger

  @doc """
  Write one operative's route into the cache: `operative_mapping` is what
  `route/2` reads first, in front of `ClassRegistry` — which refuses a dead
  pid. A cache that is not invalidated by the death of the process it caches
  always wins that race, so the frame is cast into a dead process and lost
  with no drop counted. Every writer goes through here or `drop_route/2`,
  which monitor what the cache gains and demonitor what it loses.
  """
  def put_route(state, operative_id, pids) do
    state = drop_route(state, operative_id)

    %{
      state
      | operative_mapping: Map.put(state.operative_mapping, operative_id, pids),
        route_monitors:
          Enum.reduce(pids, state.route_monitors, &Watch.watch(&2, &1, operative_id))
    }
  end

  @doc "Forget one operative's route, releasing the monitors only it held."
  def drop_route(state, operative_id) do
    {pids, mapping} = Map.pop(state.operative_mapping, operative_id, [])

    %{
      state
      | operative_mapping: mapping,
        route_monitors:
          Enum.reduce(pids, state.route_monitors, &Watch.unwatch(&2, &1, operative_id))
    }
  end

  @doc """
  A cached Overwatch died: forget every operative routing to it, so the next
  frame resolves through `ClassRegistry` and reaches the live replacement.

  Frames enqueued after the death sit behind the `:DOWN` in the same mailbox,
  so they see a clear cache; one enqueued before it was routed while the
  target was still alive, which no design can save.
  """
  def forget_pid(state, pid) do
    {operative_ids, monitors} = Watch.forget(state.route_monitors, pid)
    Enum.reduce(operative_ids, %{state | route_monitors: monitors}, &drop_route(&2, &1))
  end

  @doc """
  The Overwatch an output frame is for, and the channel state after the
  lookup: `{{:ok, pid} | :drop, state}`.

  A route exists only in the owner session's channels, so a hit in the stream's
  mapping is the ownership check and costs nothing more. A miss is judged by
  the operative's ownership record: the owner's or the pending runner's first
  output, which can beat the publish that seeds the route, is routed through
  the record's Overwatch and the route cached; a frame from any other runner
  is refused (`admit/3`). A frame for an operative nobody records, or whose
  recorded Overwatch is gone, is the only true drop, and it is counted.
  """
  def route(state, operative_id) do
    case Map.get(state.operative_mapping, operative_id, []) do
      [] ->
        warm_route(state, operative_id)

      [pid] ->
        {{:ok, pid}, state}

      [first | _] = pids ->
        # Defensive: the publish seeds one local pid, so the list is single by
        # construction. If a regression
        # slips a longer one through we must NOT fan out frames to
        # every pid — that is the EVIDENCE1 §2 data-plane drain that
        # produced 22 frames/s on unchosen runners. Route to the
        # first pid only and surface the misuse via telemetry.
        Logger.error(
          "[stream_channel] fan-out warning for operative=#{inspect(operative_id)}: " <>
            "expected one pid, got #{length(pids)}: #{inspect(pids)}"
        )

        :telemetry.execute(
          [:flezha, :stream, :fanout_warning],
          %{count: 1, pid_count: length(pids)},
          %{operative_id: operative_id, runner_id: state.runner_id}
        )

        {{:ok, first}, state}
    end
  end

  # The liveness check runs on a miss only: per frame it costs a measurable
  # share of the output path.
  defp warm_route(state, operative_id) do
    case ownership(state, operative_id, :output) do
      {:owner, pid} ->
        if Process.alive?(pid),
          do: {{:ok, pid}, put_route(state, operative_id, [pid])},
          else: {unrouted(state, operative_id), state}

      :unrecorded ->
        {unrouted(state, operative_id), state}

      :refused ->
        {:drop, state}
    end
  end

  defp unrouted(state, operative_id) do
    :telemetry.execute(
      [:flezha, :stream, :drop],
      %{count: 1},
      %{operative_id: operative_id, runner_id: state.runner_id, reason: :unrouted}
    )

    :drop
  end

  @doc """
  Whether the session behind `state` may act on the operative whose class is
  `operative_class` (its id, unless it declared a class): the session's stable
  identity is the owner or the pending identity the ownership record names, or
  no record exists on this node. `kind` names the frame in the report.

  A refusal is counted on the channel every time. The event, the warning and
  the `RemOperative` that stops the sender's copy go out once per session and
  operative, however many of the session's channels the copy speaks on.
  """
  @spec admit(map(), binary(), atom()) :: boolean()
  def admit(state, operative_class, kind),
    do: ownership(state, operative_class, kind) != :refused

  # The one read of the ownership record: the session owns the operative (as
  # owner or pending) and the record names its Overwatch, no record exists on
  # this node, or the frame is refused.
  defp ownership(state, operative_class, kind) do
    case ClassRegistry.owner(operative_class) do
      {:ok, pid, owner, pending} ->
        if state.identity in [owner, pending],
          do: {:owner, pid},
          else: refuse(state, operative_class, owner, kind)

      :error ->
        :unrecorded
    end
  end

  defp refuse(state, operative_class, owner, kind) do
    LivenessCounters.bump_channel(state.liveness_ref, :foreign_frame_refused)

    if Marks.first_refusal?(state.runner_id, operative_class) do
      Logger.warning(
        "[runner] #{state.runner_id} (identity #{state.identity}) sent #{kind} for " <>
          "#{inspect(operative_class)}, which is bound to #{inspect(owner)}; refused, and the " <>
          "runner is told to remove its copy"
      )

      :telemetry.execute(
        [:flezha, :runner, :foreign_frame_refused],
        %{count: 1},
        %{
          runner_id: state.runner_id,
          identity: state.identity,
          owner: owner,
          operative_id: operative_class,
          kind: kind
        }
      )

      ControlUnit.stop_operative_async(state.runner_id, operative_class)
    end

    :refused
  end

  # Emit `output_stall` for each mapped operative whose kind runs the output-stall
  # watchdog, naming the runner whose stream stalled: the stall is about that
  # binding, and the loop drops it once the operative is bound elsewhere. A kind
  # without the watchdog (a durable workflow operative) legitimately goes
  # silent, so flagging it would spuriously rebind.
  def emit_output_stall(runner_id, operative_mapping) do
    for {operative_id, _} <- operative_mapping,
        ClassRegistry.capable?(operative_id, :output_stall_watch?) do
      :telemetry.execute(
        [:flezha, :state, :output_stall],
        %{count: 1},
        %{operative_instance: operative_id, runner_id: runner_id}
      )
    end
  end

  # Persistence frames are handed to the per-cell WalWriter via a
  # non-blocking cast: the writer appends, drives seq-gap resync, and writes
  # snapshots (an fsync) in its own process, so disk/recovery latency never
  # stalls this channel — which also carries the operative's output frames.
  # The per-Cell queue that keeps a slow Cell from stalling siblings on
  # this stream is each WalWriter's own mailbox: the cast demuxes here and
  # queues there, so only the shared credit window couples Cells.
  def handle_cell_state_change(state, frame) do
    case Wire.decode_frame(frame) do
      {:ok, %{cell_id: {rc, _sh, _pk} = cell_id} = record} ->
        if admit(state, rc, :cell_state_change),
          do: append_to_cell(state, cell_id, rc, record, byte_size(frame)),
          else: state

      {:error, reason} ->
        Logger.warning("[stream] cell_state_change decode failed: #{inspect(reason)}")
        state
    end
  end

  defp append_to_cell(state, cell_id, rc, record, frame_bytes) do
    case Flezha.State.Supervisor.find_or_start(cell_id) do
      {:ok, pid} ->
        admit_and_append(state, pid, rc, record, frame_bytes)

      {:error, reason} ->
        Logger.error(
          "[stream] cell_state_change WalWriter start failed " <>
            "cell=#{inspect(cell_id)}: #{inspect(reason)}"
        )

        state
    end
  end

  defp admit_and_append(state, pid, rc, record, frame_bytes) do
    case credit_admit(state, rc, frame_bytes) do
      {:shed, state} ->
        state

      {credit_opts, state} ->
        WalWriter.append_async(
          pid,
          record.seq,
          record.header,
          record.body,
          state.runner_id,
          credit_opts
        )

        state
    end
  end

  # The DLQ append (an fsync) runs in the store process, never here — this
  # channel also carries the operative's output frames. The ack closure
  # runs in the store process after the fsync; `send_control_async` is a
  # cast, so it cannot block the store either.
  def handle_correction_request(state, frame) do
    with {:ok, %{cell_id: {operative_class, _, _} = cell_id, body: body}} <-
           Wire.decode_frame(frame),
         {:ok, target} <- Wire.decode_correction_request(body),
         true <- admit(state, operative_class, :correction_request),
         {:ok, overwatch} <- Lookup.overwatch(operative_class) do
      Replay.request_correction(overwatch, cell_id, target)
    else
      false ->
        :ok

      other ->
        Logger.warning("[stream] undecodable correction_request: #{inspect(other)}")
    end
  end

  def handle_park_payload(state, frame) do
    with {:ok, %{cell_id: {rc, sh, pk}, body: body}} <- Wire.decode_frame(frame),
         {:ok, park} <- Wire.decode_park_body(body),
         true <- admit(state, rc, :park_payload) do
      runner_id = state.runner_id

      DlqStore.park_async(
        {rc, sh, pk},
        park,
        fn _entry_ref ->
          ControlChannel.send_control_async(
            runner_id,
            {:park_payload_ack, %RunnerCommands.ParkPayloadAck{park_id: park.park_id}}
          )
        end
      )
    else
      false -> :ok
      err -> Logger.warning("[stream] park_payload decode failed: #{inspect(err)}")
    end
  end

  # Count every refused forward; emit `stale_writer_refused` once per
  # (Cell, superseded session) episode so a chatty zombie cannot storm the
  # bus. A frame whose Cell cannot be decoded is still counted.
  def refuse_stale_forward(state, payload_id, frame) do
    LivenessCounters.bump_channel(state.liveness_ref, :stale_writer_refused)

    with {:ok, {rc, sh, pk} = cell_id} <- frame_cell_id(payload_id, frame),
         true <- Marks.first_refusal?(state.runner_id, cell_id) do
      :telemetry.execute(
        [:flezha, :state, :stale_writer_refused],
        %{count: 1},
        %{operative: rc, tasklet: sh, key: pk, session: state.runner_id}
      )
    end

    :ok
  end

  def frame_cell_id("delete_cell_state", frame), do: Wire.decode_cell_id(frame)

  def frame_cell_id(_payload_id, frame) do
    with {:ok, %{cell_id: cell_id}} <- Wire.decode_frame(frame),
         do: {:ok, cell_id}
  end

  # ---------- Credit-grant flow control ----------

  # Admit one state-change frame against the stream's credit ledger.
  # Returns `{credit_opts_for_append, state}` or `{:shed, state}` for a
  # frame overrunning an exhausted ledger (protocol violation — the seq gap
  # it leaves drives the existing resync repair).
  def credit_admit(%{credit: nil} = state, operative_class, _size) do
    if credit_enabled?() do
      {%{}, offer_credit_window(state, operative_class)}
    else
      {%{}, state}
    end
  end

  def credit_admit(%{credit: %CreditLedger{} = ledger} = state, _class, size) do
    case CreditLedger.admit(ledger, size) do
      {:ok, ledger} ->
        put_ledger_level(state, ledger)

        {%{channel: self(), session: ledger.session, wire_size: size}, %{state | credit: ledger}}

      {:overrun, overrun_ledger} ->
        report_overrun(state, ledger, size)
        {:shed, %{state | credit: overrun_ledger}}
    end
  end

  # `:offered` (handshake in flight) and `:legacy` (nack / kill switch at
  # offer time) both admit un-metered — byte-for-byte the pre-credit path.
  def credit_admit(state, _class, _size), do: {%{}, state}

  # Offer `CreditWindowInit` for this attach. Runs on the FIRST state-change
  # frame rather than at stream bind so the offer is guaranteed to originate
  # from the channel on the actual state path, and so the window can resolve
  # per-Operative from that frame's class. The offer rides the ControlUnit's
  # request/reply machinery in a Task: the SDK's ack comes back as the
  # CommandResponse raw_data; a pre-credit SDK never replies and the call
  # timeout degrades the session to `:legacy`.
  def offer_credit_window(state, operative_class) do
    session = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    window = PersistenceResolver.credit_window_bytes(operative_class)
    # Unset means half the window, and the window is resolved per operative
    # class, so the halving is the site's rather than a row's.
    low = Config.get(:credit_low_watermark_bytes) || div(window, 2)

    parent = self()
    runner_id = state.runner_id

    Task.start(fn ->
      init = %RunnerCommands.CreditWindowInit{
        stream_id: session,
        window_bytes: window,
        low_watermark_bytes: low
      }

      outcome =
        ControlUnit.send_control_await(
          runner_id,
          {:credit_window_init, init},
          timeout: Durations.get(:credit_handshake_timeout_ms)
        )

      send(parent, {credit_verdict(outcome, session), session})
    end)

    %{state | credit: {:offered, CreditLedger.new(session, window, low)}}
  end

  # Only an ack naming THIS session admits the window; anything else — a decode
  # failure, another session's ack, a refused round-trip — is a nack.
  defp credit_verdict({:ok, raw_data}, session) do
    case RunnerCommands.CreditWindowAck.decode(raw_data) do
      {:ok, %RunnerCommands.CreditWindowAck{stream_id: ^session}} -> :credit_ack
      _ -> :credit_nack
    end
  end

  defp credit_verdict(_outcome, _session), do: :credit_nack

  # Overrun observability: counter always, anomaly event always, warning once
  # per episode (`pre.overrun?` is the pre-admit flag).
  def report_overrun(state, pre, size) do
    LivenessCounters.bump_channel(state.liveness_ref, :credit_overrun_total)

    :telemetry.execute(
      [:flezha, :state, :credit_anomaly],
      %{count: 1, un_fsynced: CreditLedger.outstanding(pre), frame_bytes: size},
      %{kind: :overrun, runner: state.runner_id, session: pre.session}
    )

    unless pre.overrun? do
      Logger.warning(
        "[stream_channel] credit overrun: runner=#{state.runner_id} " <>
          "session=#{pre.session} level=#{CreditLedger.level(pre)} frame=#{size} " <>
          "window=#{pre.window} — shedding (protocol violation, resync repairs)"
      )
    end
  end

  def put_ledger_level(state, ledger) do
    LivenessCounters.set_channel(
      state.liveness_ref,
      :credit_ledger_level,
      CreditLedger.level(ledger)
    )
  end

  # Stream re-attach voids the session: the next state frame re-runs the
  # handshake with a fresh session id, so grants in flight for the dead
  # session can never apply (the SDK discards a session mismatch).
  def reset_credit(state) do
    LivenessCounters.set_channel(state.liveness_ref, :credit_active, 0)
    LivenessCounters.set_channel(state.liveness_ref, :credit_ledger_level, 0)
    %{state | credit: nil}
  end

  def credit_enabled?() do
    Config.get(:credit_flow_control_enabled)
  end

  # Writing a snapshot fsyncs and truncates the per-cell WAL (a call back
  # into its writer), so run it in a separate process — off this channel's loop
  # and outside the writer (which would self-call).
  def handle_cell_state_snapshot(state, frame) do
    case Wire.decode_frame(frame) do
      {:ok, %{cell_id: {rc, _sh, _pk} = cell_id, seq: seq, header: header, body: body}} ->
        if admit(state, rc, :cell_state_snapshot),
          do: spawn(fn -> write_snapshot(cell_id, seq, header, body) end)

      {:error, reason} ->
        Logger.warning("[stream] cell_state_snapshot decode failed: #{inspect(reason)}")
    end
  end

  # Persist one saga compensation marker for the frame's Cell (presence is the
  # signal; `Flezha.State.Paths.compensation_markers/1` reads it back onto
  # `RestoreInitState`). Write off this channel's loop so a slow disk never stalls
  # the output-carrying channel.
  def handle_compensation_record(state, payload_id, frame) do
    case Wire.decode_frame(frame) do
      {:ok, %{cell_id: {rc, _sh, _pk} = cell_id}} ->
        kind = if payload_id == "compensation_intent", do: :intent, else: :done

        if admit(state, rc, :compensation_record),
          do: spawn(fn -> write_compensation_marker(cell_id, kind) end)

      {:error, reason} ->
        Logger.warning("[stream] #{payload_id} decode failed: #{inspect(reason)}")
    end
  end

  defp write_compensation_marker(cell_id, kind) do
    with {:error, reason} <- Paths.write_compensation_marker(cell_id, kind) do
      Logger.error(
        "[stream] compensation marker write failed " <>
          "cell=#{inspect(cell_id)} kind=#{kind}: #{inspect(reason)}"
      )
    end
  end

  # Run the file teardown off this channel's loop: it stops the writer and
  # unlinks files, which we never want to gate the output-carrying channel on.
  # Destructive idle-eviction is a dynamic-cell mode behaviour; ignore it for a
  # kind without dynamic cells so a durable workflow operative's state is
  # never dropped out from under it.
  defp delete_if_dynamic({rc, _sh, _pk} = cell_id) do
    if ClassRegistry.capable?(rc, :dynamic_cells?) do
      spawn(fn -> Flezha.State.Supervisor.delete_cell(cell_id) end)
    else
      Logger.warning("[stream] delete_cell_state ignored for non-dynamic class #{inspect(rc)}")
    end
  end

  def handle_delete_cell_state(state, frame) do
    case Wire.decode_cell_id(frame) do
      {:ok, {rc, _sh, _pk} = cell_id} ->
        if admit(state, rc, :delete_cell_state), do: delete_if_dynamic(cell_id)

      {:error, reason} ->
        Logger.warning("[stream] delete_cell_state decode failed: #{inspect(reason)}")
    end
  end

  def write_snapshot(cell_id, seq, header, body) do
    Watchdog.compaction_completed(cell_id)

    case StateSnapshot.write_snapshot(cell_id, seq, header, body) do
      {:ok, _path} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[stream] cell_state_snapshot write failed " <>
            "cell=#{inspect(cell_id)}: #{inspect(reason)}"
        )
    end
  end
end
