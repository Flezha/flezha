defmodule Flezha.Runner.StreamChannel do
  @moduledoc """
  One data stream between the controller and a runner: a GenServer that owns
  the stream handle and is the per-input entry point (`io_in/3`).

  The two mechanisms it drives live beside it —
  `Flezha.Runner.StreamChannel.Gate` (the send gate and the bind-waiter set)
  and `Flezha.Runner.StreamChannel.Records` (the durable-record fan-out and
  the credit handshake).

  Invariant: the published stream ref lives in a `:persistent_term` under this
  module's name, written at init, bind and close only — `io_in/3` reads it
  without a call, so a frame never queues behind this process.
  """

  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamChannel.Gate
  alias Flezha.Runner.StreamChannel.Records
  alias Flezha.State.CreditLedger
  alias Flezha.Transport.LivenessCounters

  require Logger

  use GenServer

  def start_link({runner_id, name, type}) do
    GenServer.start_link(__MODULE__, {runner_id, type, name}, name: name)
  end

  @io_liveness_interval_ms 10_000
  @io_liveness_threshold_ms 30_000

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

  @doc """
  Send one input frame on `channel`'s stream from the calling process — the
  Overwatch's, on the ingest path. The channel publishes its bound
  `stream_ref` (and liveness counters) under its registered name at bind and
  clears it at close; a sender reads that instead of asking the channel, so an
  ingest pays no round trip through the one process every operative on the
  runner shares. Unbound, unknown or dead channels answer
  `{:error, :no_stream}`, counted and emitted as `[:flezha, :stream, :drop]`.

  A stream whose send buffer is over its watermark refuses the frame rather
  than buffering it, and this holds the CALLING process until the driver
  reports the buffer drained — never a scheduler thread, and never the
  channel's own. `{:error, :send_buffer_full}` means the hold ran out of
  patience with the frame still unsent; nothing was discarded on the way.
  """
  @spec io_in(GenServer.name() | nil, binary(), {binary(), map()}) ::
          :ok | {:error, :no_stream | :send_buffer_full}
  def io_in(channel, operative_id, {message, metadata}) do
    case :persistent_term.get({__MODULE__, channel}, nil) do
      {stream_ref, liveness_ref} when stream_ref != nil ->
        bump_send(liveness_ref)
        Gate.send_held(channel, stream_ref, liveness_ref, operative_id, message, metadata)

      published ->
        if published, do: LivenessCounters.bump_channel(elem(published, 1), :io_in_received)
        Gate.undeliverable(published, operative_id)
    end
  end

  # them and backpressure would otherwise look like a runner doing nothing.
  # Each reported key mirrors into the `hb_<key>` liveness slot, so the two
  # sides cannot be spelled apart.
  @heartbeat_keys ~w(dispatches credit_parked blocked_parks inflight inflight_oldest_ms
                     drops_newest drops_oldest pending_pushes pending_push_oldest_ms)
  @heartbeat_slots for key <- @heartbeat_keys, do: {key, :"hb_#{key}"}

  @impl true
  def init({runner_id, type, name}) do
    # The runner heartbeats every SECONDARY stream every ~10s; a bound
    # secondary whose last frame (heartbeat or data) is older than the
    # threshold is stalled — a silent data-plane wedge that raises no
    # `{:flz_closed}` write-error. Primary channels are send-only (the
    # runner never writes them), so the check would flag them forever.
    if type == :secondary, do: schedule_io_liveness_check()

    liveness_ref = LivenessCounters.register_channel(runner_id, name)
    publish_stream(name, nil, liveness_ref)

    {
      :ok,
      %{
        runner_id: runner_id,
        type: type,
        stream_ref: nil,
        operative_mapping: %{},
        name: name,
        last_seen: System.monotonic_time(:millisecond),
        liveness_ref: liveness_ref,
        # Credit-grant session for this state stream, encoded in the shape:
        # `nil` (no offer yet) -> `{:offered, %CreditLedger{}}` (handshake
        # in flight) -> `%CreditLedger{}` (credit-speaking) or `:legacy`
        # (pre-credit SDK / nack — terminal for the attach).
        credit: nil,
        # Takeover fence: once true (a newest-wins takeover unbound this
        # session), durable-write forwards are refused at this binding.
        # Terminal — the session is evicted right after, never re-bound.
        fenced?: false,
        refused_entities: MapSet.new(),
        # Processes owed word of an event they are held on: `:bind` (this
        # channel has no stream yet) and `:send_gate` (its send buffer is over
        # the mark). Each set is cleared by the event that serves it, so
        # neither outlives the window it exists for.
        waiters: %{bind: MapSet.new(), send_gate: MapSet.new()}
      }
    }
  end

  # Mirror one heartbeat-metadata integer into its liveness slot. A key the
  # runner did not send leaves the slot at zero, which is what an SDK without
  # the self-report must read as — never as a stall.
  defp set_reported(ref, metadata, key, slot) do
    case Integer.parse(Map.get(metadata, key, "")) do
      {value, _} -> LivenessCounters.set_channel(ref, slot, value)
      :error -> :ok
    end
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
  # What `io_in/3` reads, under the channel's registered name — the identity an
  # Overwatch holds. Written at init, bind and close only: a persistent_term
  # write is a global operation, a read is free.
  defp publish_stream(name, stream_ref, liveness_ref),
    do: :persistent_term.put({__MODULE__, name}, {stream_ref, liveness_ref})

  defp bump_send(ref) do
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :send_frame_calls)
    LivenessCounters.set_channel(ref, :last_send_ms, System.monotonic_time(:millisecond))
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
    publish_stream(state.name, new_ref, state.liveness_ref)

    # After the publish: a waiter woken before `io_in/3` could read the new ref
    # would forward onto the channel it was told is up and be dropped again.
    state = Gate.wake_waiters(state, :bind, {:stream_bound, state.name})

    {:reply, :ok, %{state | stream_ref: new_ref, last_seen: System.monotonic_time(:millisecond)}}
  end

  @impl true
  def handle_call({:bind, _new_ref}, _from, state) do
    {:reply, :already_bound, state}
  end

  # Bind interest, decided and recorded in one turn (see `notify_on_bind/2`).
  @impl true
  def handle_call({:notify_on_bind, _pid}, _from, %{stream_ref: ref} = state) when ref != nil do
    {:reply, :bound, state}
  end

  @impl true
  def handle_call({:notify_on_bind, pid}, _from, state) do
    {:reply, :unbound, Gate.record_waiter(state, :bind, pid)}
  end

  # Send-gate interest, decided and recorded in this process's own turn — the
  # same contract `notify_on_bind/2` has, against the same race.
  #
  # The caller is never argued with. It reached here by being refused on an
  # exact read of the gate bit, which is fresher than anything this channel
  # could hold: the driver's report of a crossing travels a queue, and the
  # sender raises the bit itself precisely because that queue is what it
  # outruns. So interest is recorded whenever it is asked for, and the drain
  # report is what releases it.
  @impl true
  def handle_call({:await_send_gate, pid}, _from, state) do
    {:reply, :held, Gate.record_waiter(state, :send_gate, pid)}
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
    {:noreply, Records.refuse_stale_forward(state, payload_id, message)}
  end

  # Effect records (effectively-once tier) share the state-change frame
  # layout and append path; the `format_id = 2` header is the durable
  # marker the WalWriter keys the fsync-gated outbox flush on.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id in ["entity_state_change", "entity_effect_record"] do
    state = Records.handle_entity_state_change(state, message)
    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "entity_state_snapshot" do
    Records.handle_entity_state_snapshot(state.runner_id, message)
    {:noreply, mark_seen(state)}
  end

  # Dynamic-mode hard-delete: the runner evicted a persistence-enabled
  # Entity. Drop its WAL/snapshot/outbox files so a later recreate
  # cold-starts. Fire-and-forget — no response frame is written back.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id == "delete_entity_state" do
    Records.handle_delete_entity_state(message)
    {:noreply, mark_seen(state)}
  end

  # Workflow-operative saga compensation markers (SDK → controller). A durable
  # workflow records each procedure's forward completion (`compensation_intent`)
  # and each compensation's completion (`compensation_done`); persisted per
  # Entity and shipped back on the next `RestoreInitState` so a respawn finishes
  # the remaining hooks and skips already-undone ones. Fire-and-forget.
  def handle_info({:flz_data, _stream_id, payload_id, message, _metadata}, state)
      when payload_id in ["compensation_intent", "compensation_done"] do
    Records.handle_compensation_record(payload_id, message)
    {:noreply, mark_seen(state)}
  end

  # Dead-letter park (SDK → controller, tier-off sessions): append the
  # poison payload to the durable DLQ store. The ParkPayloadAck control
  # command fires only after the entry is fsynced — park-then-advance, so
  # the parked entity never advances past a payload the controller could
  # still lose.
  def handle_info({:flz_data, _stream_id, "park_payload", message, _metadata}, state) do
    Records.handle_park_payload(state, message)
    {:noreply, mark_seen(state)}
  end

  # Late-input correction (SDK → controller): an Entity saw an input behind its
  # event-time high-water but inside the declared window. Only the SDK can know
  # that — the event time comes from user code — so detection is its half and
  # orchestration is the Overwatch's.
  def handle_info({:flz_data, _stream_id, "correction_request", message, _metadata}, state) do
    Records.handle_correction_request(message)
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
    for {key, slot} <- @heartbeat_slots, do: set_reported(ref, metadata, key, slot)
    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, operative_id, message, metadata}, state) do
    case Records.route(state, operative_id) do
      {:ok, pid} -> GenServer.cast(pid, {:io_out, message, metadata})
      :drop -> :ok
    end

    {:noreply, mark_seen(state)}
  end

  # Periodic liveness check: a bound SECONDARY stream whose last frame
  # (heartbeat or data) is older than the threshold has missed several
  # heartbeats and is stalled. Emit `output_stall` for each mapped operative (the
  # signal the convergence Loop rebinds on) — but keep `stream_ref`, since the
  # rebind re-establishes it. Primary streams are send-only and never receive
  # heartbeats, so the check is meaningless there and would flag every
  # primary stream stalled for as long as it stayed open.
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

      Records.emit_output_stall(state.operative_mapping)
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
    Records.put_ledger_level(state, ledger)
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
  # credit already debited are freed (fsynced, or dropped without ever
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

    Records.put_ledger_level(state, ledger)
    {:noreply, %{state | credit: ledger}}
  end

  # Drain report tagged with a superseded session (reconnect voided it) — stale.
  def handle_info({:credit_drained, _session, _bytes}, state), do: {:noreply, state}

  # The driver crossed a watermark on this stream's send buffer. A raise is
  # recorded nowhere: a sender discovers it from `send_frame/4`'s own answer,
  # which is the same bit read without a queue in between, so keeping a copy
  # here would only be a staler one. The drain is what this channel is for —
  # it releases every sender holding on that stream.
  def handle_info({:flz_send_gate, _stream_id, true}, state), do: {:noreply, state}

  def handle_info({:flz_send_gate, _stream_id, false}, state) do
    {:noreply, Gate.wake_waiters(state, :send_gate, {:send_gate, state.name, :open})}
  end

  # NIF transport: stream closed. Synchronous rebindability contract — once
  # this clause runs, state.stream_ref is nil and no outbound send may use
  # the dead ref. The acceptor will pick this channel up via :stream_ref_status
  # on the next incoming data stream for the same runner_id.
  def handle_info({:flz_closed, _stream_id, _reason}, state) do
    # A close the controller itself asked for is not an incident: it is
    # reported, not diagnosed. Only the REPORTING sits inside this branch —
    # the binding clear and the credit reset below are unconditional, because
    # a real socket close drops the runner either way.
    if Marks.runner_retiring?(state.runner_id) do
      Logger.info("IO socket closed for retiring runner #{state.runner_id}.")
    else
      Logger.warning("Connection closed for IO socket.")

      # Emit output_stall immediately for every operative in the mapping so the
      # convergence Loop triggers a rebind tick rather than waiting for the
      # Watchdog's 60s grace period to detect the dead socket.
      Records.emit_output_stall(state.operative_mapping)
    end

    publish_stream(state.name, nil, state.liveness_ref)

    # A sender held on this stream's gate is waiting for a buffer that will
    # never drain now. It is told the stream went, so it reports the frame
    # undelivered instead of waiting out its hold on a dead binding.
    state = Gate.wake_waiters(state, :send_gate, {:send_gate, state.name, :closed})

    {:noreply, %{state | stream_ref: nil} |> Records.reset_credit()}
  end

  def handle_info(msg, state) do
    Logger.warning("Unexpected message at io channel: #{inspect(msg)}")

    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    _ = :persistent_term.erase({__MODULE__, state.name})
    :ok
  end
end
