defmodule Flezha.Runner.StreamChannel do
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Operative.Replay
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.CreditLedger
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Snapshot, as: StateSnapshot
  alias Flezha.State.WalWriter
  alias Flezha.State.Watchdog
  alias Flezha.State.Wire, as: StateWire
  alias Flezha.Transport.LivenessCounters

  require Logger

  use GenServer

  def start_link({runner_id, name, type}) do
    GenServer.start_link(__MODULE__, {runner_id, type, name}, name: name)
  end

  @io_liveness_interval_ms 10_000
  @io_liveness_threshold_ms 30_000
  @credit_handshake_timeout_ms 10_000
  @send_gate_hold_ms 5_000

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
        send_held(channel, stream_ref, liveness_ref, operative_id, message, metadata)

      published ->
        if published, do: LivenessCounters.bump_channel(elem(published, 1), :io_in_received)
        undeliverable(published, operative_id)
    end
  end

  # The send, and the hold the send gate asks for. `send_frame/4` answers
  # `{:error, :send_buffer_full}` from one relaxed atomic load, on a crossing
  # this caller already pays — the answer is a congestion signal, not a loss,
  # so the frame is offered again once the buffer has drained. Any other error
  # is a driver that is gone: the frame IS lost, and it is reported through the
  # same counter and event an unbound stream uses rather than a second one.
  defp send_held(channel, stream_ref, liveness_ref, operative_id, message, metadata) do
    case Flezha.Transport.Stream.send_frame(stream_ref, operative_id, message, metadata) do
      :ok ->
        :ok

      {:error, :send_buffer_full} ->
        LivenessCounters.bump_channel(liveness_ref, :send_gate_held)

        case await_send_gate(channel, send_gate_hold_ms()) do
          :open -> send_held(channel, stream_ref, liveness_ref, operative_id, message, metadata)
          :closed -> undeliverable({nil, liveness_ref}, operative_id)
          :timeout -> {:error, :send_buffer_full}
        end

      {:error, _driver_gone} ->
        undeliverable({nil, liveness_ref}, operative_id)
    end
  end

  # One statement of "this channel could not carry the frame", for the three
  # ways that happens: no binding, a stream that closed under a held sender,
  # and a driver thread that is gone.
  defp undeliverable(published, operative_id) do
    if published, do: LivenessCounters.bump_channel(elem(published, 1), :nil_drops)
    :telemetry.execute([:flezha, :stream, :drop], %{count: 1}, %{operative_id: operative_id})
    {:error, :no_stream}
  end

  # How long a held sender waits for the driver to report the buffer drained.
  # It is a lost-wake fallback, not a pacing dial: the wake arrives on the
  # crossing, and this only bounds a wake that never came.
  defp send_gate_hold_ms,
    do: Application.get_env(:flezha, :send_gate_hold_ms, @send_gate_hold_ms)

  # Register interest and wait, in the calling process. Deciding and recording
  # happen in the channel's own turn, so a drain landing between the two cannot
  # be missed; the wait is a selective receive, so everything else the caller
  # is holding stays queued.
  defp await_send_gate(channel, timeout_ms) do
    case GenServer.whereis(channel) && GenServer.call(channel, {:await_send_gate, self()}) do
      :held ->
        receive do
          {:send_gate, ^channel, result} -> result
        after
          timeout_ms -> :timeout
        end

      _open_or_no_channel ->
        :open
    end
  end

  @doc """
  Ask this channel to tell `pid` about its next stream bind, as
  `{:stream_bound, name}` under the channel's registered name.

  Answers `:bound` when a stream is already up — the caller has nothing to wait
  for — `:unbound` once the interest is recorded, and `:no_channel` when no such
  channel is running. Deciding and recording happen in the channel process, so a
  bind landing between the two cannot be missed.

  One-shot: every bind notifies the recorded pids and forgets them, so a caller
  that still cares after a later close asks again.
  """
  @spec notify_on_bind(GenServer.name() | nil, pid()) :: :bound | :unbound | :no_channel
  def notify_on_bind(nil, _pid), do: :no_channel

  def notify_on_bind(channel, pid) when is_pid(pid) do
    case GenServer.whereis(channel) do
      nil -> :no_channel
      _ -> GenServer.call(channel, {:notify_on_bind, pid})
    end
  end

  @doc """
  Block the CALLING process until `channel` has a bound stream, at most
  `timeout_ms`.

  Used by the operative bind so nothing downstream of it — the durable restore,
  the journal re-forward, the first ingest queued behind it — is issued onto a
  stream that cannot carry it. The wait is a selective receive, so every other
  message the caller is holding stays queued, which is what makes the caller's
  mailbox the queue an unbound stream needs.

  `:ok` also answers a channel that is not running: an operative whose runner has
  no channel pool has nothing to wait for and its delivery failure is reported by
  `io_in/3` as before.
  """
  @spec await_bind(GenServer.name() | nil, non_neg_integer()) ::
          :ok | {:error, :stream_unbound}
  def await_bind(channel, timeout_ms) do
    case notify_on_bind(channel, self()) do
      :unbound ->
        receive do
          {:stream_bound, ^channel} -> :ok
        after
          timeout_ms -> {:error, :stream_unbound}
        end

      _bound_or_absent ->
        :ok
    end
  end

  # The runner's self-report, mirrored slot for slot. `dispatches` frozen while
  # the runner is alive is a wedged pool; `credit_parked` advancing while it
  # freezes is fed-but-parked rather than frozen; `blocked_parks` says the same
  # for the receive side (a producer held on a full entity mailbox);
  # `inflight` and its oldest age are what make ONE wedged Entity visible while
  # the rest keeps completing; the two drop counters are what a shedding
  # operative loses, which nothing outside the runner could see before;
  # `pending_pushes` and its oldest age are the producers a full consumer is
  # withholding work from — they hold no handout, so `inflight` is blind to
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
  defp bump_send(ref) do
    LivenessCounters.bump_channel(ref, :io_in_received)
    LivenessCounters.bump_channel(ref, :send_frame_calls)
    LivenessCounters.set_channel(ref, :last_send_ms, System.monotonic_time(:millisecond))
  end

  # What `io_in/3` reads, under the channel's registered name — the identity an
  # Overwatch holds. Written at init, bind and close only: a persistent_term
  # write is a global operation, a read is free.
  defp publish_stream(name, stream_ref, liveness_ref),
    do: :persistent_term.put({__MODULE__, name}, {stream_ref, liveness_ref})

  # One waiter set per event, so the bind hold and the send-gate hold are one
  # mechanism rather than two that can drift.
  defp record_waiter(state, event, pid),
    do: %{state | waiters: Map.update!(state.waiters, event, &MapSet.put(&1, pid))}

  defp wake_waiters(state, event, message) do
    Enum.each(state.waiters[event], &send(&1, message))
    %{state | waiters: Map.put(state.waiters, event, MapSet.new())}
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
    state = wake_waiters(state, :bind, {:stream_bound, state.name})

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
    {:reply, :unbound, record_waiter(state, :bind, pid)}
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
    {:reply, :held, record_waiter(state, :send_gate, pid)}
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
    for {key, slot} <- @heartbeat_slots, do: set_reported(ref, metadata, key, slot)
    {:noreply, mark_seen(state)}
  end

  def handle_info({:flz_data, _stream_id, operative_id, message, metadata}, state) do
    case route(state, operative_id) do
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

  # The driver crossed a watermark on this stream's send buffer. A raise is
  # recorded nowhere: a sender discovers it from `send_frame/4`'s own answer,
  # which is the same bit read without a queue in between, so keeping a copy
  # here would only be a staler one. The drain is what this channel is for —
  # it releases every sender holding on that stream.
  def handle_info({:flz_send_gate, _stream_id, true}, state), do: {:noreply, state}

  def handle_info({:flz_send_gate, _stream_id, false}, state) do
    {:noreply, wake_waiters(state, :send_gate, {:send_gate, state.name, :open})}
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
    if FleetManager.runner_retiring?(state.runner_id) do
      Logger.info("IO socket closed for retiring runner #{state.runner_id}.")
    else
      Logger.warning("Connection closed for IO socket.")

      # Emit output_stall immediately for every operative in the mapping so the
      # convergence Loop triggers a rebind tick rather than waiting for the
      # Watchdog's 60s grace period to detect the dead socket.
      emit_output_stall(state.operative_mapping)
    end

    publish_stream(state.name, nil, state.liveness_ref)

    # A sender held on this stream's gate is waiting for a buffer that will
    # never drain now. It is told the stream went, so it reports the frame
    # undelivered instead of waiting out its hold on a dead binding.
    state = wake_waiters(state, :send_gate, {:send_gate, state.name, :closed})

    {:noreply, %{state | stream_ref: nil} |> reset_credit()}
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

  # Emit `output_stall` for each mapped operative whose kind runs the output-stall
  # watchdog. A kind without it (a durable workflow operative) legitimately goes
  # silent, so flagging it would spuriously rebind.
  # The Overwatch a frame is for: the stream's mapping first; on a miss, the
  # class registry — the mapping is seeded by the registry hook after the
  # bind publishes, and a chain that emits within its first milliseconds
  # beats it, while the Overwatch registered itself for its class before the
  # SDK started. A frame neither knows is the only true drop, and it is counted.
  defp route(state, operative_id) do
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
                WalWriter.append_async(
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
      Replay.request_correction(overwatch, entity_id, target)
    else
      other ->
        Logger.warning("[stream] undecodable correction_request: #{inspect(other)}")
    end
  end

  defp handle_park_payload(state, frame) do
    with {:ok, %{entity_id: {rc, sh, pk}, body: body}} <- StateWire.decode_frame(frame),
         {:ok, park} <- StateWire.decode_park_body(body) do
      runner_id = state.runner_id

      DlqStore.append_async(
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
  # signal; `Flezha.State.Paths.compensation_markers/1` reads it back onto
  # `RestoreInitState`). Write off this channel's loop so a slow disk never stalls
  # the output-carrying channel.
  defp handle_compensation_record(payload_id, frame) do
    case StateWire.decode_frame(frame) do
      {:ok, %{entity_id: entity_id}} ->
        kind = if payload_id == "compensation_intent", do: :intent, else: :done

        spawn(fn ->
          with {:error, reason} <-
                 Paths.write_compensation_marker(entity_id, kind) do
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
