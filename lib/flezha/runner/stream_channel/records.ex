defmodule Flezha.Runner.StreamChannel.Records do
  @moduledoc """
  The operative route cache, the durable-record fan-out and the credit
  handshake.

  Every frame a runner sends on an Entity's write path arrives here: state
  changes, snapshots, deletions, compensation records, corrections and parks.
  Each is routed to the Entity's own writer, and a frame from a session the
  takeover fence marked superseded is refused rather than written.

  Invariant: credit is granted per window and debited per record, so a runner
  that outruns its grant is reported once and throttled, never silently
  dropped.

  Invariant: every pid the route cache holds is monitored, so a cached
  Overwatch that dies cannot keep winning against the liveness-checked
  registry behind it.
  """

  alias Flezha.Config.Durations
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Operative.Replay
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.State.CreditLedger
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Snapshot, as: StateSnapshot
  alias Flezha.State.WalWriter
  alias Flezha.State.Watchdog
  alias Flezha.State.Wire, as: StateWire
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
        route_monitors: Enum.reduce(pids, state.route_monitors, &watch(&2, &1, operative_id))
    }
  end

  @doc "Forget one operative's route, releasing the monitors only it held."
  def drop_route(state, operative_id) do
    {pids, mapping} = Map.pop(state.operative_mapping, operative_id, [])

    %{
      state
      | operative_mapping: mapping,
        route_monitors: Enum.reduce(pids, state.route_monitors, &unwatch(&2, &1, operative_id))
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
    case Map.pop(state.route_monitors, pid) do
      {nil, _monitors} ->
        state

      {{_ref, ids}, monitors} ->
        Enum.reduce(ids, %{state | route_monitors: monitors}, &drop_route(&2, &1))
    end
  end

  # One monitor per pid however many operatives name it, so a `:DOWN` arrives
  # once and the demonitor waits for the last of them to leave. A route that is
  # not a pid raises here rather than entering the cache unmonitored.
  defp watch(monitors, pid, operative_id) do
    case monitors do
      %{^pid => {ref, ids}} -> Map.put(monitors, pid, {ref, MapSet.put(ids, operative_id)})
      _ -> Map.put(monitors, pid, {Process.monitor(pid), MapSet.new([operative_id])})
    end
  end

  defp unwatch(monitors, pid, operative_id) do
    case monitors do
      %{^pid => {ref, ids}} ->
        ids = MapSet.delete(ids, operative_id)

        if MapSet.size(ids) == 0 do
          Process.demonitor(ref, [:flush])
          Map.delete(monitors, pid)
        else
          Map.put(monitors, pid, {ref, ids})
        end

      _ ->
        monitors
    end
  end

  # Emit `output_stall` for each mapped operative whose kind runs the output-stall
  # watchdog. A kind without it (a durable workflow operative) legitimately goes
  # silent, so flagging it would spuriously rebind.
  # The Overwatch a frame is for: the stream's mapping first; on a miss, the
  # class registry — the mapping is seeded by the registry hook after the
  # bind publishes, and a chain that emits within its first milliseconds
  # beats it, while the Overwatch registered itself for its class before the
  # SDK started. A frame neither knows is the only true drop, and it is counted.
  def route(state, operative_id) do
    case Map.get(state.operative_mapping, operative_id, []) do
      [] ->
        case ClassRegistry.overwatch_pid(operative_id) do
          {:ok, pid} ->
            {:ok, pid}

          :error ->
            :telemetry.execute(
              [:flezha, :stream, :drop],
              %{count: 1},
              %{operative_id: operative_id, runner_id: state.runner_id, reason: :unrouted}
            )

            :drop
        end

      [pid] ->
        {:ok, pid}

      [first | _] = pids ->
        # Defensive: `hook_insert_pid/2` seeds one registry row's local-node
        # pids, so the list is single by construction — but if a regression
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

        {:ok, first}
    end
  end

  def emit_output_stall(operative_mapping) do
    for {operative_id, _} <- operative_mapping,
        ClassRegistry.capable?(operative_id, :output_stall_watch?) do
      :telemetry.execute(
        [:flezha, :state, :output_stall],
        %{count: 1},
        %{operative_instance: operative_id}
      )
    end
  end

  # Persistence frames are handed to the per-entity WalWriter via a
  # non-blocking cast: the writer appends, drives seq-gap resync, and writes
  # snapshots (an fsync) in its own process, so disk/recovery latency never
  # stalls this channel — which also carries the operative's output frames.
  # The per-Entity queue that keeps a slow Entity from stalling siblings on
  # this stream is each WalWriter's own mailbox: the cast demuxes here and
  # queues there, so only the shared credit window couples Entities.
  def handle_entity_state_change(state, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: {rc, _sh, _pk} = entity_id} = record} ->
        append_to_entity(state, entity_id, rc, record, byte_size(frame))

      {:error, reason} ->
        Logger.warning("[stream] entity_state_change decode failed: #{inspect(reason)}")
        state
    end
  end

  defp append_to_entity(state, entity_id, rc, record, frame_bytes) do
    case Flezha.State.Supervisor.find_or_start(entity_id) do
      {:ok, pid} ->
        admit_and_append(state, pid, rc, record, frame_bytes)

      {:error, reason} ->
        Logger.error(
          "[stream] entity_state_change WalWriter start failed " <>
            "entity=#{inspect(entity_id)}: #{inspect(reason)}"
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
  def handle_correction_request(frame) do
    with {:ok, %{entity_id: {operative_class, _, _} = entity_id, body: body}} <-
           StateWire.decode_frame(frame),
         {:ok, target} <- StateWire.decode_correction_request(body),
         {:ok, overwatch} <- ClassRegistry.overwatch_pid(operative_class) do
      Replay.request_correction(overwatch, entity_id, target)
    else
      other ->
        Logger.warning("[stream] undecodable correction_request: #{inspect(other)}")
    end
  end

  def handle_park_payload(state, frame) do
    with {:ok, %{entity_id: {rc, sh, pk}, body: body}} <- StateWire.decode_frame(frame),
         {:ok, park} <- StateWire.decode_park_body(body) do
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
      err -> Logger.warning("[stream] park_payload decode failed: #{inspect(err)}")
    end
  end

  # Count every refused forward; emit `stale_writer_refused` once per
  # (Entity, superseded session) episode so a chatty zombie cannot storm the
  # bus. A frame whose Entity cannot be decoded is still counted.
  def refuse_stale_forward(state, payload_id, frame) do
    LivenessCounters.bump_channel(state.liveness_ref, :stale_writer_refused)

    case frame_entity_id(payload_id, frame) do
      {:ok, {rc, sh, pk} = entity_id} ->
        if MapSet.member?(state.refused_entities, entity_id) do
          state
        else
          :telemetry.execute(
            [:flezha, :state, :stale_writer_refused],
            %{count: 1},
            %{operative: rc, procedure: sh, key: pk, session: state.runner_id}
          )

          %{state | refused_entities: MapSet.put(state.refused_entities, entity_id)}
        end

      {:error, _} ->
        state
    end
  end

  def frame_entity_id("delete_entity_state", frame), do: StateWire.decode_entity_id(frame)

  def frame_entity_id(_payload_id, frame) do
    with {:ok, %{entity_id: entity_id}} <- StateWire.decode_frame(frame),
         do: {:ok, entity_id}
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
    low = Application.get_env(:flezha, :credit_low_watermark_bytes, div(window, 2))

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
    Application.get_env(:flezha, :credit_flow_control_enabled, true)
  end

  # Writing a snapshot fsyncs and truncates the per-entity WAL (a call back
  # into its writer), so run it in a separate process — off this channel's loop
  # and outside the writer (which would self-call).
  def handle_entity_state_snapshot(_runner_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        entity_id = {rc, sh, pk}
        spawn(fn -> write_snapshot(entity_id, seq, header, body) end)

      {:error, reason} ->
        Logger.warning("[stream] entity_state_snapshot decode failed: #{inspect(reason)}")
    end
  end

  # Persist one saga compensation marker for the frame's Entity (presence is the
  # signal; `Flezha.State.Paths.compensation_markers/1` reads it back onto
  # `RestoreInitState`). Write off this channel's loop so a slow disk never stalls
  # the output-carrying channel.
  def handle_compensation_record(payload_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: entity_id}} ->
        kind = if payload_id == "compensation_intent", do: :intent, else: :done

        spawn(fn -> write_compensation_marker(entity_id, kind) end)

      {:error, reason} ->
        Logger.warning("[stream] #{payload_id} decode failed: #{inspect(reason)}")
    end
  end

  defp write_compensation_marker(entity_id, kind) do
    with {:error, reason} <- Paths.write_compensation_marker(entity_id, kind) do
      Logger.error(
        "[stream] compensation marker write failed " <>
          "entity=#{inspect(entity_id)} kind=#{kind}: #{inspect(reason)}"
      )
    end
  end

  # Run the file teardown off this channel's loop: it stops the writer and
  # unlinks files, which we never want to gate the output-carrying channel on.
  # Destructive idle-eviction is a dynamic-entity mode behaviour; ignore it for a
  # kind without dynamic entities so a durable workflow operative's state is
  # never dropped out from under it.
  defp delete_if_dynamic({rc, _sh, _pk} = entity_id) do
    if ClassRegistry.capable?(rc, :dynamic_entities?) do
      spawn(fn -> Flezha.State.Supervisor.delete_entity(entity_id) end)
    else
      Logger.warning("[stream] delete_entity_state ignored for non-dynamic class #{inspect(rc)}")
    end
  end

  def handle_delete_entity_state(frame) do
    case StateWire.decode_entity_id(frame) do
      {:ok, {rc, sh, pk}} ->
        delete_if_dynamic({rc, sh, pk})

      {:error, reason} ->
        Logger.warning("[stream] delete_entity_state decode failed: #{inspect(reason)}")
    end
  end

  def write_snapshot(entity_id, seq, header, body) do
    Watchdog.compaction_completed(entity_id)

    case StateSnapshot.write_snapshot(entity_id, seq, header, body) do
      {:ok, _path} ->
        :ok

      {:error, reason} ->
        Logger.error(
          "[stream] entity_state_snapshot write failed " <>
            "entity=#{inspect(entity_id)}: #{inspect(reason)}"
        )
    end
  end
end
