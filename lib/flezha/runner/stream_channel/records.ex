defmodule Flezha.Runner.StreamChannel.Records do
  @moduledoc """
  The relay route cache, the ownership check, the durable-record fan-out
  and the credit handshake.

  Every frame a runner sends on a Cell's write path arrives here: state
  changes, snapshots, deletions, compensation records, corrections and parks.
  Each is routed to the Cell's own writer. A frame from a session the
  takeover fence marked superseded is refused rather than written, and so is
  one from a runner the relay's ownership record does not name
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
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Relay.Lookup
  alias Flezha.Relay.Replay
  alias Flezha.Process.{DurableTasks, Watch}
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.StreamChannel.Prefix
  alias Flezha.State.CreditLedger
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.WalWriter
  alias Flezha.State.Wire
  alias Flezha.Transport.LivenessCounters

  require Logger

  @typedoc """
  What an ownership check reads of the session it judges, held alike by a
  stream channel's and a control channel's state.
  """
  @type session :: %{
          required(:runner_id) => binary(),
          required(:identity) => binary(),
          required(:namespace) => binary(),
          required(:liveness_ref) => term(),
          optional(atom()) => term()
        }

  @doc """
  Write one relay's route into the cache: `relay_mapping` is what
  `route/2` reads first, in front of `ClassRegistry` — which refuses a dead
  pid. A cache that is not invalidated by the death of the process it caches
  always wins that race, so the frame is cast into a dead process and lost
  with no drop counted. Every writer goes through here or `drop_route/2`,
  which monitor what the cache gains and demonitor what it loses.
  """
  def put_route(state, relay_id, pids) do
    state = drop_route(state, relay_id)

    %{
      state
      | relay_mapping: Map.put(state.relay_mapping, relay_id, pids),
        route_monitors: Enum.reduce(pids, state.route_monitors, &Watch.watch(&2, &1, relay_id))
    }
  end

  @doc "Forget one relay's route, releasing the monitors only it held."
  def drop_route(state, relay_id) do
    {pids, mapping} = Map.pop(state.relay_mapping, relay_id, [])

    %{
      state
      | relay_mapping: mapping,
        route_monitors: Enum.reduce(pids, state.route_monitors, &Watch.unwatch(&2, &1, relay_id))
    }
  end

  @doc """
  A cached Overwatch died: forget every relay routing to it, so the next
  frame resolves through `ClassRegistry` and reaches the live replacement.

  Frames enqueued after the death sit behind the `:DOWN` in the same mailbox,
  so they see a clear cache; one enqueued before it was routed while the
  target was still alive, which no design can save.
  """
  def forget_pid(state, pid) do
    {relay_ids, monitors} = Watch.forget(state.route_monitors, pid)
    Enum.reduce(relay_ids, %{state | route_monitors: monitors}, &drop_route(&2, &1))
  end

  @doc """
  The Overwatch an output frame is for, and the channel state after the
  lookup: `{{:ok, pid} | :drop, state}`.

  A route exists only in the owner session's channels, so a hit in the stream's
  mapping is the ownership check and costs nothing more. A miss is judged by
  the relay's ownership record: the owner's first output, which can beat
  the publish that seeds the route, is routed through
  the record's Overwatch and the route cached; a frame from any other runner
  is refused (`admit/3`). A frame for a relay of the session's namespace
  nobody records, or whose recorded Overwatch is gone, is the only true drop,
  and it is counted.
  """
  def route(state, relay_id) do
    case Map.get(state.relay_mapping, relay_id, []) do
      [] ->
        warm_route(state, relay_id)

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
          "[stream_channel] fan-out warning for relay=#{inspect(relay_id)}: " <>
            "expected one pid, got #{length(pids)}: #{inspect(pids)}"
        )

        :telemetry.execute(
          [:flezha, :stream, :fanout_warning],
          %{count: 1, pid_count: length(pids)},
          %{relay_id: relay_id, runner_id: state.runner_id}
        )

        {{:ok, first}, state}
    end
  end

  # The liveness check runs on a miss only: per frame it costs a measurable
  # share of the output path.
  defp warm_route(state, relay_id) do
    case ownership(state, relay_id, :output) do
      {:owner, pid} ->
        if Process.alive?(pid),
          do: {{:ok, pid}, put_route(state, relay_id, [pid])},
          else: {unrouted(state, relay_id), state}

      :unrecorded ->
        {unrouted(state, relay_id), state}

      :refused ->
        {:drop, state}
    end
  end

  defp unrouted(state, relay_id) do
    :telemetry.execute(
      [:flezha, :stream, :drop],
      %{count: 1},
      %{relay_id: relay_id, runner_id: state.runner_id, reason: :unrouted}
    )

    :drop
  end

  @doc """
  Whether the session behind `state` may act on the relay whose class is
  `relay_class` (its id, unless it declared a class): the session's stable
  identity is the owner the ownership record names, or no record exists on
  this node and the relay's durable state belongs to the session's namespace
  (`Flezha.State.Paths.namespace_of_class/1`). `kind` names the frame in the
  report.

  A refusal is counted on the channel every time. The event, the warning and
  the `RemRelay` that stops the sender's copy go out once per session and
  relay, however many of the session's channels the copy speaks on.
  """
  @spec admit(session(), binary(), atom()) :: boolean()
  def admit(state, relay_class, kind),
    do: ownership(state, relay_class, kind) != :refused

  @doc """
  `admit/3`, answering where an admitted frame goes: the Overwatch the
  ownership record names, alive or not, the one the frame was admitted
  against, else `nil` (no record on this node) for the caller to resolve
  through the hub. During a hub merge's hand-over the hub can still list the
  source node's departing Overwatch, which would lose the frame.
  """
  @spec admit_to(session(), binary(), atom()) :: {:ok, pid() | nil} | :refused
  def admit_to(state, relay_class, kind) do
    case ownership(state, relay_class, kind) do
      {:owner, pid} -> {:ok, pid}
      :unrecorded -> {:ok, nil}
      :refused -> :refused
    end
  end

  @doc """
  Whether the session behind `state` hosts `relay_class`: the owner its
  ownership record names. Unlike `admit/3` it refuses nothing and reports
  nothing, so a claim a runner merely names is judged without acting on it.
  """
  @spec hosts?(session(), binary()) :: boolean()
  def hosts?(state, relay_class) do
    case ClassRegistry.owner(relay_class) do
      {:ok, _pid, owner} -> owner == state.identity
      :error -> false
    end
  end

  @doc """
  Whether the session behind `state` is the owner the relay's ownership record
  names: unlike `admit/3`, a relay with no record on this node is not its.
  """
  @spec owned?(session(), binary(), atom()) :: boolean()
  def owned?(state, relay_class, kind),
    do: match?({:owner, _pid}, ownership(state, relay_class, kind))

  # The one read of the ownership record: the session owns the relay and
  # the record names its Overwatch, no record exists on this node and the
  # relay's state belongs to the session's namespace, or the frame is refused.
  defp ownership(state, relay_class, kind) do
    case ClassRegistry.owner(relay_class) do
      {:ok, pid, owner} ->
        if state.identity == owner,
          do: {:owner, pid},
          else: refuse(state, relay_class, owner, kind)

      :error ->
        unrecorded(state, relay_class, kind)
    end
  end

  # No record here means a relay whose stop is waiting for its runner's last
  # frames (this channel still routes it), a hub hand-over's old copy still
  # speaking, a steps Cell after its relay ended, or a lost ownership table, so
  # past the route the namespace the relay's state belongs to on this node is
  # what is judged. A control request's relay id passed no
  # `Wire.decode_cell_id/1`, so it is held to the same path rule before it
  # names a directory.
  defp unrecorded(state, relay_class, kind) do
    if Map.has_key?(Map.get(state, :relay_mapping, %{}), relay_class) do
      :unrecorded
    else
      namespace = if Wire.path_segment?(relay_class), do: Paths.namespace_of_class(relay_class)

      if namespace == state.namespace,
        do: :unrecorded,
        else: refuse(state, relay_class, namespace, kind)
    end
  end

  defp refuse(state, relay_class, owner, kind) do
    LivenessCounters.bump_channel(state.liveness_ref, :foreign_frame_refused)

    if Marks.first_refusal?(state.runner_id, relay_class) do
      Logger.warning(
        "[runner] #{state.runner_id} (identity #{state.identity}) sent #{kind} for " <>
          "#{inspect(relay_class)}, which belongs to #{inspect(owner)}; refused, and the " <>
          "runner is told to remove its copy"
      )

      :telemetry.execute(
        [:flezha, :runner, :foreign_frame_refused],
        %{count: 1},
        %{
          runner_id: state.runner_id,
          identity: state.identity,
          owner: owner,
          relay_id: relay_class,
          kind: kind
        }
      )

      ControlUnit.stop_relay_async(state.runner_id, relay_class)
    end

    :refused
  end

  # Emit `output_stall` for each relay a silent stream carried, naming
  # the runner whose stream stalled: the stall is about that binding, and the
  # loop drops it once the relay is bound elsewhere. A sleeping instance is not
  # mapped to any runner, so it is never flagged.
  def emit_output_stall(runner_id, relay_ids) do
    for relay_id <- relay_ids do
      :telemetry.execute(
        [:flezha, :state, :output_stall],
        %{count: 1},
        %{relay_instance: relay_id, runner_id: runner_id}
      )
    end
  end

  # Persistence frames are handed to the per-cell WalWriter via a
  # non-blocking cast: the writer appends, drives seq-gap resync, and writes
  # snapshots (an fsync) in its own process, so disk/recovery latency never
  # stalls this channel — which also carries the relay's output frames.
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
        undecodable(state, "cell_state_change", reason)
        state
    end
  end

  defp undecodable(state, payload_id, reason) do
    report_once(state, {:undecodable, payload_id}, :warning, fn ->
      "[stream] #{payload_id} decode failed: #{inspect(reason)}"
    end)
  end

  # A frame refused one way is refused alike every time a runner sends it again,
  # so it is reported once per session and subject.
  defp report_once(state, subject, level, message) do
    if Marks.first_refusal?(state.runner_id, subject), do: Logger.log(level, message)
  end

  defp append_to_cell(state, cell_id, rc, record, frame_bytes) do
    case Flezha.State.Supervisor.find_or_start(cell_id) do
      {:ok, pid} ->
        admit_and_append(state, pid, rc, record, frame_bytes)

      {:error, reason} ->
        report_once(state, {:writer_start_failed, cell_id}, :error, fn ->
          "[stream] cell_state_change WalWriter start failed " <>
            "cell=#{inspect(cell_id)}: #{inspect(reason)}"
        end)

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

        %{state | prefix: Prefix.received(state.prefix, record.cell_id, frame_bytes)}
    end
  end

  @doc """
  The instance and offset a processed-prefix marker names, and on how many
  streams it was sent, when the session may speak for that instance. The
  marker is a Cell frame whose identity names the instance and whose seq
  carries the offset; a runner that spreads an instance's records over several
  streams sends it on each and names their count in the body, an empty body
  meaning one. A one-byte body `1` is the final marker of a stop that drained
  the instance: it names no offset (`:final`) and confirms no prefix.
  """
  @spec processed_prefix(map(), binary()) ::
          {:ok, binary(), non_neg_integer() | :final, pos_integer()} | :refused
  def processed_prefix(state, frame) do
    with {:ok, %{cell_id: {rc, _, _}, seq: offset, body: body}} <- Wire.decode_frame(frame),
         {:ok, at, streams} <- marker_body(offset, body),
         true <- admit(state, rc, :processed_prefix) do
      {:ok, rc, at, streams}
    else
      false ->
        :refused

      {:error, reason} ->
        undecodable(state, "processed_prefix", reason)
        :refused
    end
  end

  defp marker_body(offset, <<>>), do: {:ok, offset, 1}
  defp marker_body(offset, <<streams::16-big>>) when streams > 0, do: {:ok, offset, streams}
  defp marker_body(_offset, <<1>>), do: {:ok, :final, 1}
  defp marker_body(_offset, body), do: {:error, {:bad_marker_body, body}}

  @doc """
  Records an earlier stream handed to the writers are not counted by this
  stream's markers; one batcher flush and the ships of the runner's gated
  writers, asked for by the stream's first marker, acknowledge them before the
  channel hears `{:prefix_fenced, stream_ref}`. A stream that never carries a
  marker never forces a flush.
  """
  @spec fence_once(map()) :: map()
  def fence_once(state) do
    case Prefix.ask_fence(state.prefix) do
      {true, prefix} ->
        channel = self()
        %{stream_ref: stream_ref, identity: identity} = state

        Task.start(fn ->
          :ok = Flezha.State.Supervisor.flush_all()
          :ok = WalWriter.RemoteGate.await_held(identity)
          send(channel, {:prefix_fenced, stream_ref})
        end)

        %{state | prefix: prefix}

      {false, _prefix} ->
        state
    end
  end

  @doc """
  Step the stream's markers and confirm, in each instance's input log, what
  they release. A final marker released tells the relay's routed Overwatch
  `{:relay_drained, relay_id}`: its writers hold every record the runner sent
  ahead of it. With no route, it is dropped.
  """
  @spec confirm_prefix(map(), (Prefix.t() -> {[tuple()], Prefix.t()})) :: map()
  def confirm_prefix(state, step) do
    {confirmed, prefix} = step.(state.prefix)

    Enum.each(confirmed, fn
      {rc, :final, _streams} ->
        with [pid | _] <- Map.get(state.relay_mapping, rc, []),
             do: send(pid, {:relay_drained, rc})

      {rc, offset, streams} ->
        Flezha.State.Supervisor.confirm_prefix(rc, offset, streams: streams, stream: self())
    end)

    %{state | prefix: prefix}
  end

  # The DLQ append (an fsync) runs in the store process, never here — this
  # channel also carries the relay's output frames. The ack closure
  # runs in the store process after the fsync; `send_control_async` is a
  # cast, so it cannot block the store either.
  def handle_correction_request(state, frame) do
    with {:ok, %{cell_id: {relay_class, _, _} = cell_id, body: body}} <-
           Wire.decode_frame(frame),
         {:ok, target} <- Wire.decode_correction_request(body),
         true <- admit(state, relay_class, :correction_request),
         {:ok, overwatch} <- Lookup.overwatch(relay_class) do
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
        fn parked ->
          ControlChannel.send_control_async(
            runner_id,
            {:park_payload_ack, %RunnerCommands.ParkPayloadAck{park_id: park.park_id}}
          )

          DlqStore.announce(rc, parked, %{
            token: park.reply_to,
            error: park.error,
            channel: park.channel,
            offset: park.origin_offset,
            tasklet: sh,
            attempts: park.attempts
          })
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
        %{relay: rc, tasklet: sh, key: pk, session: state.runner_id}
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
  # it leaves drives the existing resync repair). Every admitted frame names
  # this channel, so its writer reports the drain the markers confirm on.
  def credit_admit(%{credit: nil} = state, relay_class, size) do
    if credit_enabled?() do
      {unmetered(size), offer_credit_window(state, relay_class)}
    else
      {unmetered(size), state}
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
  # offer time) both admit un-metered.
  def credit_admit(state, _class, size), do: {unmetered(size), state}

  defp unmetered(size), do: %{channel: self(), session: nil, wire_size: size}

  # Offer `CreditWindowInit` for this attach. Runs on the FIRST state-change
  # frame rather than at stream bind so the offer is guaranteed to originate
  # from the channel on the actual state path, and so the window can resolve
  # per-Relay from that frame's class. The offer rides the ControlUnit's
  # request/reply machinery in a Task: the SDK's ack comes back as the
  # CommandResponse raw_data; a pre-credit SDK never replies and the call
  # timeout degrades the session to `:legacy`.
  def offer_credit_window(state, relay_class) do
    session = Base.encode16(:crypto.strong_rand_bytes(8), case: :lower)
    window = PersistenceResolver.credit_window_bytes(relay_class)
    # Unset means half the window, and the window is resolved per relay
    # class, so the halving is the site's rather than a row's.
    low = Config.get(:credit_low_watermark_bytes) || div(window, 2)

    parent = self()
    runner_id = state.runner_id

    Task.start(fn ->
      init = %RunnerCommands.CreditWindowInit{stream_id: session, window_bytes: window}

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

  # Overrun observability: the counter always, the anomaly event and the
  # warning once per episode (`pre.overrun?` is the pre-admit flag).
  def report_overrun(state, pre, size) do
    LivenessCounters.bump_channel(state.liveness_ref, :credit_overrun_total)

    unless pre.overrun? do
      :telemetry.execute(
        [:flezha, :state, :credit_anomaly],
        %{count: 1, un_fsynced: CreditLedger.outstanding(pre), frame_bytes: size},
        %{kind: :overrun, runner: state.runner_id, session: pre.session}
      )

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

  # A snapshot is cast to its Cell's writer as its records are, so the writer
  # installs it in the order the runner shipped them.
  def handle_cell_state_snapshot(state, frame) do
    case Wire.decode_frame(frame) do
      {:ok, %{cell_id: {rc, _sh, _pk} = cell_id, seq: seq, header: header, body: body}} ->
        with true <- admit(state, rc, :cell_state_snapshot),
             {:ok, pid} <- Flezha.State.Supervisor.find_or_start(cell_id),
             do: WalWriter.install_snapshot(pid, state.runner_id, seq, header, body)

      {:error, reason} ->
        undecodable(state, "cell_state_snapshot", reason)
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
          do: DurableTasks.start(fn -> write_compensation_marker(cell_id, kind) end)

      {:error, reason} ->
        undecodable(state, payload_id, reason)
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

  # The teardown stops the writer and unlinks files, so it runs off this
  # channel's loop, never gating the output-carrying channel.
  def handle_delete_cell_state(state, frame) do
    case Wire.decode_cell_id(frame) do
      {:ok, {rc, _sh, _pk} = cell_id} ->
        if admit(state, rc, :delete_cell_state),
          do: DurableTasks.start(fn -> Flezha.State.Supervisor.delete_cell(cell_id) end)

      {:error, reason} ->
        undecodable(state, "delete_cell_state", reason)
    end
  end
end
