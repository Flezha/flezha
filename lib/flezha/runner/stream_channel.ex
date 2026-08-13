defmodule Flezha.Runner.StreamChannel do
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.CreditLedger
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Wire, as: StateWire
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Native

  require Logger

  use GenServer

  def start_link({runner_id, name, type}) do
    GenServer.start_link(__MODULE__, {runner_id, type, name}, name: name)
  end

  @io_liveness_interval_ms 10_000
  @io_liveness_threshold_ms 30_000
  @credit_handshake_timeout_ms 10_000

  # Every payload-id this channel forwards onto an Entity's durable write path;
  # the takeover fence (`:fence_write_path`) refuses exactly this set.
  @entity_write_payloads [
    "entity_state_change",
    "entity_effect_record",
    "entity_state_snapshot",
    "delete_entity_state",
    "compensation_intent",
    "compensation_done",
    "park_payload"
  ]

  @impl true
  def init({runner_id, type, name}) do
    # The runner heartbeats every SECONDARY stream every ~10s; a bound
    # secondary whose last frame (heartbeat or data) is older than the
    # threshold is stalled — a silent data-plane wedge that raises no
    # `{:flz_closed}` write-error. Primary channels are send-only (the
    # runner never writes them), so the check would flag them forever.
    if type == :secondary, do: schedule_io_liveness_check()

    {
      :ok,
      %{
        runner_id: runner_id,
        type: type,
        stream_ref: nil,
        operative_mapping: %{},
        name: name,
        last_seen: System.monotonic_time(:millisecond),
        liveness_ref: LivenessCounters.register_channel(runner_id, name),
        # Credit-grant session for this state stream, encoded in the shape:
        # `nil` (no offer yet) -> `{:offered, %CreditLedger{}}` (handshake
        # in flight) -> `%CreditLedger{}` (credit-speaking) or `:legacy`
        # (pre-credit SDK / nack — terminal for the attach).
        credit: nil,
        # Takeover fence: once true (a newest-wins takeover unbound this
        # session), durable-write forwards are refused at this binding.
        # Terminal — the session is evicted right after, never re-bound.
        fenced?: false,
        refused_entities: MapSet.new()
      }
    }
  end

  defp schedule_io_liveness_check do
    interval =
      Application.get_env(:flezha, :io_stream_liveness_interval_ms, @io_liveness_interval_ms)

    if interval > 0, do: Process.send_after(self(), :io_liveness_check, interval)
  end

  defp mark_seen(state) do
    LivenessCounters.bump_channel(state[:liveness_ref], :flz_data_received)
    %{state | last_seen: System.monotonic_time(:millisecond)}
  end

  # Count an outbound io_in: one frame received and one send_frame issued, with
  # the send timestamp. A stall shows whether the channel is still being fed
  # (io_in_received) and still calling send_frame (send_frame_calls).
  defp bump_send(state) do
    ref = state[:liveness_ref]
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :send_frame_calls)
    LivenessCounters.set_channel(ref, :last_send_ms, System.monotonic_time(:millisecond))
  end

  # Count an io_in that hit an unbound stream: fed, but dropped (not sent).
  defp bump_drop(state) do
    ref = state[:liveness_ref]
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :nil_drops)
  end

  @impl true
  def handle_cast(
        {:io_in, operative_id, {_message, _metadata}},
        %{stream_ref: nil} = state
      ) do
    # Unbound channel: emit `[:flezha, :stream, :drop]` so the seam
    # is observable. The convergence Loop subscribes to `:output_stall`
    # for rebind decisions; drop telemetry remains a useful operator
    # signal independent of that.
    bump_drop(state)
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: operative_id})
    {:noreply, state}
  end

  @impl true
  def handle_cast(
        {:io_in, operative_id, {message, metadata}},
        %{stream_ref: stream_ref} = state
      ) do
    bump_send(state)
    Native.send_frame(stream_ref, operative_id, message, metadata)
    {:noreply, state}
  end

  @impl true
  def handle_call(
        {:io_in, _operative_id, {_message, _metadata}},
        _from,
        %{stream_ref: nil} = state
      ) do
    bump_drop(state)
    {:reply, {:error, :no_stream}, state}
  end

  @impl true
  def handle_call(
        {:io_in, operative_id, {message, metadata}},
        _from,
        %{stream_ref: stream_ref} = state
      ) do
    bump_send(state)
    Native.send_frame(stream_ref, operative_id, message, metadata)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:operative_pid_update, operative_id, updated_pids}, _from, state) do
    operative_mapping = Map.put(state[:operative_mapping], operative_id, updated_pids)

    {:reply, :ok, Map.put(state, :operative_mapping, operative_mapping)}
  end

  @impl true
  def handle_call({:operative_pid_remove, operative_id}, _from, state) do
    operative_mapping = Map.delete(state[:operative_mapping], operative_id)

    {:reply, :ok, Map.put(state, :operative_mapping, operative_mapping)}
  end

  @impl true
  def handle_call({:operative_bulk_insert, operatives}, _from, state) do
    operative_mapping =
      Enum.reduce(operatives, state.operative_mapping, fn {operative_id, updated_pids}, acc ->
        Map.put(acc, operative_id, updated_pids)
      end)

    {:reply, :ok, Map.put(state, :operative_mapping, operative_mapping)}
  end

  # Probe used by acceptors to find rebindable channels. Returns the current
  # stream_ref (or nil if unbound). Snapshot only — the acceptor must then
  # call {:bind, ref} to atomically claim a nil-ref channel.
  @impl true
  def handle_call(:stream_ref_status, _from, state) do
    {:reply, state.stream_ref, state}
  end

  # Atomic bind. Succeeds only if stream_ref is currently nil. The acceptor
  # uses :already_bound as a signal to try the next candidate, avoiding a
  # snapshot-vs-bind race that could otherwise overwrite a live ref.
  @impl true
  def handle_call({:bind, new_ref}, _from, %{stream_ref: nil} = state) do
    {:reply, :ok, %{state | stream_ref: new_ref, last_seen: System.monotonic_time(:millisecond)}}
  end

  @impl true
  def handle_call({:bind, _new_ref}, _from, state) do
    {:reply, :already_bound, state}
  end

  # Takeover fence: unbind this channel from the Entity write path.
  # Synchronous, so the takeover transaction knows that once this returns no
  # further durable-write forward can leave this channel — the single state
  # flip that stops a superseded session, with zero added work anywhere else.
  @impl true
  def handle_call(:fence_write_path, _from, state) do
    {:reply, :ok, %{state | fenced?: true}}
  end

  # Unified NIF transport message — complete parsed frame.
  #
  # Two reserved payload-ids carry persistence-bound data (see
  # `Flezha.State.Wire`). All other payload-ids are operative outputs
  # routed via the per-operative mapping.
  #
  # Fenced (superseded-session) channel: refuse every durable-write forward
  # at the binding. In-flight frames from the old session die here instead of
  # reaching an Entity's WAL/snapshot files after the takeover unbind.
  @impl true
  def handle_info(
        {:flz_data, _stream_id, payload_id, message, _metadata},
        %{fenced?: true} = state
      )
      when payload_id in @entity_write_payloads do
    {:noreply, refuse_stale_forward(state, payload_id, message)}
  end

  # Effect records (effectively-once tier) share the state-change frame
  # layout and append path; the `format_id = 2` header is the durable
  # marker the WalWriter keys the fsync-gated outbox flush on.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id in ["entity_state_change", "entity_effect_record"] do
    state = handle_entity_state_change(state, message)
    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "entity_state_snapshot" do
    handle_entity_state_snapshot(state.runner_id, message)
    {:noreply, mark_seen(state)}
  end

  # Dynamic-mode hard-delete: the runner evicted a persistence-enabled
  # Entity. Drop its WAL/snapshot/outbox files so a later recreate
  # cold-starts. Fire-and-forget — no response frame is written back.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "delete_entity_state" do
    handle_delete_entity_state(message)
    {:noreply, mark_seen(state)}
  end

  # Workflow-operative saga compensation markers (SDK → controller). A durable
  # workflow records each procedure's forward completion (`compensation_intent`)
  # and each compensation's completion (`compensation_done`); persisted per
  # Entity and shipped back on the next `RestoreInitState` so a respawn finishes
  # the remaining hooks and skips already-undone ones. Fire-and-forget.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id in ["compensation_intent", "compensation_done"] do
    handle_compensation_record(payload_id, message)
    {:noreply, mark_seen(state)}
  end

  # Dead-letter park (SDK → controller, tier-off sessions): append the
  # poison payload to the durable DLQ store. The ParkPayloadAck control
  # command fires only after the entry is fsynced — park-then-advance, so
  # the parked entity never advances past a payload the controller could
  # still lose.
  def handle_info({:flz_data, _stream_id, "park_payload", message, _metadata}, state) do
    handle_park_payload(state, message)
    {:noreply, mark_seen(state)}
  end

  # Late-input correction (SDK → controller): an Entity saw an input behind its
  # event-time high-water but inside the declared window. Only the SDK can know
  # that — the event time comes from user code — so detection is its half and
  # orchestration is the Overwatch's.
  def handle_info({:flz_data, _stream_id, "correction_request", message, _metadata}, state) do
    handle_correction_request(message)
    {:noreply, mark_seen(state)}
  end

  # Data-channel heartbeat: the runner emits this every ~10s while connected,
  # so an idle-but-healthy operative keeps the channel live without producing
  # output. Metadata carries the runner's dispatch-progress self-report; the
  # stall classifier compares it across ticks to catch an alive-but-wedged
  # dispatch pool (a state every heartbeat-level signal is blind to).
  def handle_info({:flz_data, _stream_id, "data_heartbeat", _message, metadata}, state) do
    ref = state[:liveness_ref]
    LivenessCounters.bump_channel(ref, :hb_count)

    case Integer.parse(Map.get(metadata, "dispatches", "")) do
      {dispatches, _} -> LivenessCounters.set_channel(ref, :hb_dispatches, dispatches)
      :error -> :ok
    end

    # Credit-park self-report: advancing while dispatches freeze classifies
    # the runner as fed-but-parked on credit, not silently frozen.
    case Integer.parse(Map.get(metadata, "credit_parked", "")) do
      {parked, _} -> LivenessCounters.set_channel(ref, :hb_credit_parked, parked)
      :error -> :ok
    end

    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, operative_id, message, metadata}, state) do
    case Map.get(state.operative_mapping, operative_id, []) do
      [] ->
        :ok

      [pid] ->
        GenServer.cast(pid, {:io_out, message, metadata})

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

        GenServer.cast(first, {:io_out, message, metadata})
    end

    {:noreply, mark_seen(state)}
  end

  # Periodic liveness check: a bound SECONDARY stream whose last frame
  # (heartbeat or data) is older than the threshold has missed several
  # heartbeats and is stalled. Emit `output_stall` for each mapped operative (the
  # signal the convergence Loop rebinds on) — but keep `stream_ref`, since the
  # rebind re-establishes it. Primary streams are send-only and never receive
  # heartbeats, so the check is meaningless there (it flagged a false
  # output_stall every 10s for 21h straight on 2026-06-09/10).
  @impl true
  def handle_info(:io_liveness_check, %{type: type} = state) when type != :secondary do
    {:noreply, state}
  end

  def handle_info(:io_liveness_check, state) do
    schedule_io_liveness_check()

    threshold =
      Application.get_env(:flezha, :io_stream_liveness_threshold_ms, @io_liveness_threshold_ms)

    stale? = System.monotonic_time(:millisecond) - state.last_seen >= threshold

    if not is_nil(state.stream_ref) and stale? do
      Logger.warning(
        "[stream_channel] IO stream silent past liveness threshold, flagging output_stall: " <>
          "runner=#{inspect(state.runner_id)} operatives=#{inspect(Map.keys(state.operative_mapping))}"
      )

      emit_output_stall(state.operative_mapping)
    end

    {:noreply, state}
  end

  # Credit handshake outcome, sent by the offer Task. The ack activates the
  # ledger; a nack (pre-credit SDK, timeout, transport error) pins the attach
  # to the legacy EntityMailboxStatus path. Session mismatches are stale
  # outcomes from a superseded attach and are dropped.
  def handle_info(
        {:credit_ack, session},
        %{credit: {:offered, %CreditLedger{session: session} = ledger}} = state
      ) do
    Logger.info(
      "[stream_channel] credit session established for #{state.runner_id}: " <>
        "session=#{session} window=#{ledger.window} low=#{ledger.low}"
    )

    LivenessCounters.set_channel(state.liveness_ref, :credit_active, 1)
    put_ledger_level(state, ledger)
    {:noreply, %{state | credit: ledger}}
  end

  def handle_info({:credit_ack, _session}, state), do: {:noreply, state}

  def handle_info(
        {:credit_nack, session},
        %{credit: {:offered, %CreditLedger{session: session}}} = state
      ) do
    Logger.debug(
      "[stream_channel] credit handshake declined for #{state.runner_id} " <>
        "(pre-credit SDK or timeout); staying on the legacy path"
    )

    {:noreply, %{state | credit: :legacy}}
  end

  def handle_info({:credit_nack, _session}, state), do: {:noreply, state}

  # Durable-drain report from a WalWriter fed by this stream: `bytes` of
  # previously-debited credit are freed (fsynced, or dropped without ever
  # occupying the un-fsynced buffer). The ledger decides the batched grant —
  # one CreditGrant when the un-fsynced buffer falls below the low watermark,
  # never per record; a stalled fsync reports nothing so nothing is minted.
  def handle_info(
        {:credit_drained, session, bytes},
        %{credit: %CreditLedger{session: session} = ledger} = state
      ) do
    {grant, ledger} = CreditLedger.drain(ledger, bytes)

    if grant > 0 do
      ControlChannel.send_control_async(
        state.runner_id,
        {:credit_grant, %RunnerCommands.CreditGrant{stream_id: session, bytes: grant}}
      )
    end

    put_ledger_level(state, ledger)
    {:noreply, %{state | credit: ledger}}
  end

  # Drain report tagged with a superseded session (reconnect voided it) — stale.
  def handle_info({:credit_drained, _session, _bytes}, state), do: {:noreply, state}

  # NIF transport: stream closed. Synchronous rebindability contract — once
  # this clause runs, state.stream_ref is nil and no outbound send may use
  # the dead ref. The acceptor will pick this channel up via :stream_ref_status
  # on the next incoming data stream for the same runner_id.
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    Logger.warning("Connection closed for IO socket.")

    # Emit output_stall immediately for every operative in the mapping so the
    # convergence Loop triggers a rebind tick rather than waiting for the
    # Watchdog's 60s grace period to detect the dead socket.
    emit_output_stall(state.operative_mapping)

    {:noreply, %{state | stream_ref: nil} |> reset_credit()}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  # Emit `output_stall` for each mapped operative whose kind runs the output-stall
  # watchdog. A kind without it (a durable workflow operative) legitimately goes
  # silent, so flagging it would spuriously rebind.
  defp emit_output_stall(operative_mapping) do
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
  defp handle_entity_state_change(state, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        entity_id = {rc, sh, pk}

        case Flezha.State.Supervisor.find_or_start(entity_id) do
          {:ok, pid} ->
            case credit_admit(state, rc, byte_size(frame)) do
              {:shed, state} ->
                state

              {credit_opts, state} ->
                Flezha.State.WalWriter.append_async(
                  pid,
                  seq,
                  header,
                  body,
                  state.runner_id,
                  credit_opts
                )

                state
            end

          {:error, reason} ->
            Logger.error(
              "[stream] entity_state_change WalWriter start failed " <>
                "entity=#{inspect(entity_id)}: #{inspect(reason)}"
            )

            state
        end

      {:error, reason} ->
        Logger.warning("[stream] entity_state_change decode failed: #{inspect(reason)}")
        state
    end
  end

  # The DLQ append (an fsync) runs in the store process, never here — this
  # channel also carries the operative's output frames. The ack closure
  # runs in the store process after the fsync; `send_control_async` is a
  # cast, so it cannot block the store either.
  defp handle_correction_request(frame) do
    with {:ok, %{entity_id: {operative_class, _, _} = entity_id, body: body}} <-
           StateWire.decode_frame(frame),
         {:ok, target} <- StateWire.decode_correction_request(body),
         {:ok, overwatch} <- ClassRegistry.overwatch_pid(operative_class) do
      Flezha.Operative.Replay.request_correction(overwatch, entity_id, target)
    else
      other ->
        Logger.warning("[stream] undecodable correction_request: #{inspect(other)}")
    end
  end

  defp handle_park_payload(state, frame) do
    with {:ok, %{entity_id: {rc, sh, pk}, body: body}} <- StateWire.decode_frame(frame),
         {:ok, park} <- StateWire.decode_park_body(body) do
      runner_id = state.runner_id

      Flezha.Dlq.Store.append_async(
        %{
          operative_class: rc,
          procedure_id: sh,
          entity_key: pk,
          channel: park.channel,
          origin_offset: park.origin_offset,
          error: park.error,
          attempts: park.attempts,
          payload: park.payload,
          park_id: park.park_id
        },
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
  defp refuse_stale_forward(state, payload_id, frame) do
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

  defp frame_entity_id("delete_entity_state", frame), do: StateWire.decode_entity_id(frame)

  defp frame_entity_id(_payload_id, frame) do
    with {:ok, %{entity_id: entity_id}} <- StateWire.decode_frame(frame),
         do: {:ok, entity_id}
  end

  # ---------- Credit-grant flow control ----------

  # Admit one state-change frame against the stream's credit ledger.
  # Returns `{credit_opts_for_append, state}` or `{:shed, state}` for a
  # frame overrunning an exhausted ledger (protocol violation — the seq gap
  # it leaves drives the existing resync repair).
  defp credit_admit(%{credit: nil} = state, operative_class, _size) do
    if credit_enabled?() do
      {%{}, offer_credit_window(state, operative_class)}
    else
      {%{}, state}
    end
  end

  defp credit_admit(%{credit: %CreditLedger{} = ledger} = state, _class, size) do
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
  defp credit_admit(state, _class, _size), do: {%{}, state}

  # Offer `CreditWindowInit` for this attach. Runs on the FIRST state-change
  # frame rather than at stream bind so the offer is guaranteed to originate
  # from the channel on the actual state path, and so the window can resolve
  # per-Operative from that frame's class. The offer rides the ControlUnit's
  # request/reply machinery in a Task: the SDK's ack comes back as the
  # CommandResponse raw_data; a pre-credit SDK never replies and the call
  # timeout degrades the session to `:legacy`.
  defp offer_credit_window(state, operative_class) do
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
          timeout: @credit_handshake_timeout_ms
        )

      case outcome do
        {:ok, raw_data} ->
          case RunnerCommands.CreditWindowAck.decode(raw_data) do
            {:ok, %RunnerCommands.CreditWindowAck{stream_id: ^session}} ->
              send(parent, {:credit_ack, session})

            _ ->
              send(parent, {:credit_nack, session})
          end

        _ ->
          send(parent, {:credit_nack, session})
      end
    end)

    %{state | credit: {:offered, CreditLedger.new(session, window, low)}}
  end

  # Overrun observability: counter always, anomaly event always, warning once
  # per episode (`pre.overrun?` is the pre-admit flag).
  defp report_overrun(state, pre, size) do
    LivenessCounters.bump_channel(state.liveness_ref, :credit_overrun_total)

    :telemetry.execute(
      [:flezha, :state, :credit_anomaly],
      %{count: 1, un_fsynced: pre.debited - pre.drained, frame_bytes: size},
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

  defp put_ledger_level(state, ledger) do
    LivenessCounters.set_channel(
      state.liveness_ref,
      :credit_ledger_level,
      CreditLedger.level(ledger)
    )
  end

  # Stream re-attach voids the session: the next state frame re-runs the
  # handshake with a fresh session id, so grants in flight for the dead
  # session can never apply (the SDK discards a session mismatch).
  defp reset_credit(state) do
    LivenessCounters.set_channel(state.liveness_ref, :credit_active, 0)
    LivenessCounters.set_channel(state.liveness_ref, :credit_ledger_level, 0)
    %{state | credit: nil}
  end

  defp credit_enabled?() do
    Application.get_env(:flezha, :credit_flow_control_enabled, true)
  end

  # Writing a snapshot fsyncs and truncates the per-entity WAL (a call back
  # into its writer), so run it in a separate process — off this channel's loop
  # and outside the writer (which would self-call).
  defp handle_entity_state_snapshot(_runner_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: {rc, sh, pk}, seq: seq, header: header, body: body}} ->
        entity_id = {rc, sh, pk}
        spawn(fn -> write_snapshot(entity_id, seq, header, body) end)

      {:error, reason} ->
        Logger.warning("[stream] entity_state_snapshot decode failed: #{inspect(reason)}")
    end
  end

  # Persist one saga compensation marker for the frame's Entity (presence is the
  # signal; `Flezha.State.Registry.compensation_markers/1` reads it back onto
  # `RestoreInitState`). Write off this channel's loop so a slow disk never stalls
  # the output-carrying channel.
  defp handle_compensation_record(payload_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: entity_id}} ->
        kind = if payload_id == "compensation_intent", do: :intent, else: :done

        spawn(fn ->
          with {:error, reason} <-
                 Flezha.State.Registry.write_compensation_marker(entity_id, kind) do
            Logger.error(
              "[stream] compensation marker write failed " <>
                "entity=#{inspect(entity_id)} kind=#{kind}: #{inspect(reason)}"
            )
          end
        end)

      {:error, reason} ->
        Logger.warning("[stream] #{payload_id} decode failed: #{inspect(reason)}")
    end
  end

  # Run the file teardown off this channel's loop: it stops the writer and
  # unlinks files, which we never want to gate the output-carrying channel on.
  defp handle_delete_entity_state(frame) do
    case StateWire.decode_entity_id(frame) do
      {:ok, {rc, sh, pk}} ->
        # Destructive idle-eviction is a dynamic-entity mode behaviour; ignore
        # it for a kind without dynamic entities so a durable workflow operative's
        # state is never dropped out from under it.
        if ClassRegistry.capable?(rc, :dynamic_entities?) do
          spawn(fn -> Flezha.State.Supervisor.delete_entity({rc, sh, pk}) end)
        else
          Logger.warning(
            "[stream] delete_entity_state ignored for non-dynamic class #{inspect(rc)}"
          )
        end

      {:error, reason} ->
        Logger.warning("[stream] delete_entity_state decode failed: #{inspect(reason)}")
    end
  end

  defp write_snapshot(entity_id, seq, header, body) do
    Flezha.State.Watchdog.compaction_completed(entity_id)

    case Flezha.State.Snapshot.write_snapshot(entity_id, seq, header, body) do
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
