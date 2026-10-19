defmodule Flezha.Transport.LivenessCounters do
  @moduledoc """
  Always-on, lock-free liveness counters for the controller data plane.

  Each `StreamChannel` and `Relay.Overwatch` owns an `:atomics`-backed
  `:counters` array and bumps slots locally (single-process, no contention).
  The ref is registered in a public ETS table so the `:flezha_transport`
  observability source and the convergence stall classifier can read every
  channel/relay's counters in one lock-free scan, and collapse a runner's
  channel rows to one with `by_runner/1`.

  Cardinality is bounded: one entry per channel (runner + stream name) and
  one per relay instance — never per frame. Entries whose owner process has
  died are pruned on the next `snapshot_all/0`.

  The counters add **zero** data-plane behaviour — they are pure
  instrumentation.

  ## One row per counted fact

  `@channel_rows` and `@relay_rows` are the single declaration this
  subsystem derives from. A row says everything about its slot:

    * `name:` — the slot, and its key in `snapshot_all/0`.
    * `kind:` — `:count` (monotonic), `:set` (a gauge written whole) or
      `:instant` (a monotonic-ms stamp).
    * `scope:` — whose fact it is. `:channel` is about this stream;
      `:runner` is one fact about the runner that every channel row of that
      runner repeats, because the runner sends the same heartbeat on every
      secondary stream it holds; `:relay` is about a relay instance.
    * `aggregate:` — how a reader reduces the slot ACROSS runners. A count
      sums; an age gauge takes the larger, because summing two ages means
      nothing.
    * `heartbeat:` — the wire key this slot mirrors, or absent.
    * `surface:` — who reads it. `{:metric, name}` is emitted by the
      `:flezha_transport` source, `{:verdict, name}` is consumed by a
      convergence probe or classifier verdict, `{:telemetry, event}` reaches a
      sink through the bridge, and `{:internal, reason}` states in words why a
      slot reaches no user at all. A row must name at least one, so a slot
      that reaches nothing is a test failure rather than a silent loss.

  Everything else is derived: the `:counters` index of each slot, the array
  size, the snapshot keys, the per-runner collapse, the cross-runner
  reduction, the mirrored heartbeat key list and the source's metric rows. A
  new liveness metric is one row plus the line that bumps it.
  """

  alias Flezha.Config
  alias Flezha.Observability.CounterTable

  @table :flezha_liveness_counters

  # `hb_count` counts heartbeat FRAMES, so it is a per-channel count rather
  # than a mirrored runner fact — which is why it alone among the `hb_` slots
  # is scoped `:channel`.
  #
  # `hb_dispatches` and `hb_handouts` mirror the runner's pool progress, work
  # settled and work handed out: heartbeats advancing while both are frozen
  # means the runner process is alive and its dispatch pool is wedged, and
  # handouts moving alone is a handler still running or a retry waiting out its
  # backoff. `hb_inflight` and `hb_inflight_oldest_ms` mirror the handout
  # self-report — the two progress counts freezing say the WHOLE pool stopped,
  # these two are what make ONE wedged Cell visible while the runner keeps
  # completing work. `hb_pending_pushes` and `hb_pending_push_oldest_ms`
  # mirror the deferred-push self-report: a gated producer holds no handout, so
  # `hb_inflight` is blind to it and backpressure that withholds work would
  # otherwise read as a runner doing nothing. `hb_blocked_parks` mirrors the
  # DropNothing block counter and `hb_drops_newest`/`hb_drops_oldest` the shed
  # counts — dispatches frozen with blocked parks climbing is healthy
  # backpressure, frozen with it flat is a wedge, and the two drop counters are
  # the only outside handle on a shedding relay's loss.
  # `hb_restore_discarded_inputs` mirrors what a rebind threw away, which is
  # correct only where the controller re-forwards from a durable log, so a
  # climbing value is the user's one handle on the size of that loss.
  # `hb_timers_armed`/`hb_timers_fired` are the wake list's occupancy and its
  # firings: armed climbing while fired stays flat is promotion wedged rather
  # than an idle Relay. `hb_parts_sent` and the three `hb_parts_dropped_*`
  # slots mirror the parts a runner sent and the ones it dropped by reason: a
  # part is a lossy preview, and these are where its loss shows.
  #
  # `credit_overrun_total` counts frames a credit-speaking sender pushed past
  # an exhausted ledger — protocol violations, so any non-zero value is an
  # anomaly and never load response. `send_gate_held` counts senders held on
  # this stream's send-buffer gate, the send-side mirror of `hb_credit_parked`:
  # advancing while `nil_drops` stays flat is congestion being absorbed, not
  # data being lost.
  # Nine rows carry `measurement: :count` on a `:set` slot. The measurement key
  # is what decides the Prometheus TYPE, and those nine ship as counters today;
  # the slot itself is a gauge the runner writes whole. The mis-typing is filed
  # rather than corrected here, because changing the key renames a live series —
  # stating it on the row is what makes it a decision instead of an accident.
  use Flezha.Observability.CounterTable,
    table: :channel,
    defaults: [scope: :channel],
    slots: [
      [
        name: :last_control_frame_ms,
        kind: :instant,
        # The control half of the session row: `Probe.Readers.session/1` reads
        # it as `control_silent_ms`, beside the data half below, so "the
        # session is open" and "the data plane is draining" are two fields of
        # one sample.
        surface: [{:verdict, :session}]
      ],
      [name: :io_in_received, kind: :count, surface: [{:metric, :channel_io_in_received}]],
      [name: :send_frame_calls, kind: :count, surface: [{:metric, :channel_send_frame_calls}]],
      [name: :nil_drops, kind: :count, surface: [{:metric, :channel_nil_drops}]],
      [name: :flz_data_received, kind: :count, surface: [{:metric, :channel_flz_data_received}]],
      [
        name: :last_send_ms,
        kind: :instant,
        # The data half of the same row, read as `data_silent_ms`. It is
        # written on the hot send path and this is the only thing that reads
        # it back: a live control session over a silent data plane is exactly
        # the pair the session row exists to show.
        surface: [{:verdict, :session}]
      ],
      [
        name: :last_heartbeat_ms,
        kind: :instant,
        # When the latest data heartbeat landed, which the stall classifier
        # orders against `last_send_ms`. Declared before the slots a heartbeat
        # mirrors and written after them: a snapshot reads slots in layout
        # order, so a sample holding a new stamp holds that heartbeat's report.
        surface: [{:verdict, :runner_pool_wedged}]
      ],
      [
        name: :hb_count,
        kind: :count,
        surface: [{:verdict, :session}, {:metric, :heartbeat_frames}]
      ],
      [
        name: :hb_dispatches,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "dispatches",
        surface: [{:verdict, :runner_pool_wedged}, {:metric, :dispatches}]
      ],
      [
        name: :hb_handouts,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        heartbeat: "handouts",
        surface: [{:verdict, :runner_pool_wedged}]
      ],
      [
        name: :hb_credit_parked,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "credit_parked",
        surface: [{:metric, :credit_parked}]
      ],
      [
        name: :hb_inflight,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        heartbeat: "inflight",
        surface: [{:verdict, :cell_dispatch_stalled}, {:metric, :inflight}]
      ],
      [
        name: :hb_inflight_oldest_ms,
        kind: :set,
        scope: :runner,
        # An age, not a count: two runners' oldest handouts do not add up.
        aggregate: :max,
        heartbeat: "inflight_oldest_ms",
        surface: [{:verdict, :cell_dispatch_stalled}, {:metric, :inflight_oldest_ms}]
      ],
      [
        name: :hb_blocked_parks,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "blocked_parks",
        surface: [{:verdict, :runner_pool_wedged}, {:metric, :mailbox_blocked}]
      ],
      [
        name: :hb_drops_newest,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "drops_newest",
        surface: [{:metric, :mailbox_drops_newest}]
      ],
      [
        name: :hb_drops_oldest,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "drops_oldest",
        surface: [{:metric, :mailbox_drops_oldest}]
      ],
      [
        name: :hb_pending_pushes,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "pending_pushes",
        surface: [{:verdict, :cell_dispatch_stalled}, {:metric, :mailbox_pending_pushes}]
      ],
      [
        name: :hb_pending_push_oldest_ms,
        kind: :set,
        scope: :runner,
        aggregate: :max,
        heartbeat: "pending_push_oldest_ms",
        surface: [{:verdict, :cell_dispatch_stalled}, {:metric, :pending_push_oldest_ms}]
      ],
      [
        name: :hb_restore_discarded_inputs,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "restore_discarded_inputs",
        surface: [{:metric, :restore_discarded_inputs}]
      ],
      [
        name: :hb_timers_armed,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "timers_armed",
        surface: [{:metric, :timers_armed}]
      ],
      [
        name: :hb_timers_fired,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "timers_fired",
        surface: [{:metric, :timers_fired}]
      ],
      [
        name: :hb_parts_sent,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "parts_sent",
        surface: [{:metric, :parts_sent}]
      ],
      [
        name: :hb_parts_dropped_no_stream,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "parts_dropped_no_stream",
        surface: [{:metric, :parts_dropped_no_stream}]
      ],
      [
        name: :hb_parts_dropped_refused,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "parts_dropped_refused",
        surface: [{:metric, :parts_dropped_refused}]
      ],
      [
        name: :hb_parts_dropped_too_large,
        kind: :set,
        scope: :runner,
        aggregate: :sum,
        measurement: :count,
        heartbeat: "parts_dropped_too_large",
        surface: [{:metric, :parts_dropped_too_large}]
      ],
      [
        name: :credit_active,
        kind: :set,
        aggregate: :sum,
        measurement: :count,
        surface: [{:metric, :credit_active_sessions}]
      ],
      [
        name: :credit_ledger_level,
        kind: :set,
        aggregate: :sum,
        surface: [{:metric, :credit_ledger_level}]
      ],
      [name: :credit_overrun_total, kind: :count, surface: [{:metric, :credit_overrun_total}]],
      [
        name: :stale_writer_refused,
        kind: :count,
        # Counted here per channel AND in `Flezha.State.Metrics` per Cell, and
        # exported from the second: different keys, so the two are not a
        # duplicate. The bridged event is the surface this slot reaches.
        surface: [{:telemetry, [:flezha, :state, :stale_writer_refused]}]
      ],
      [
        name: :foreign_frame_refused,
        kind: :count,
        # Every frame refused because its runner does not own the relay it
        # names. The bridged event goes out once per session and relay, so
        # a copy that keeps speaking is counted here.
        surface: [{:telemetry, [:flezha, :runner, :foreign_frame_refused]}]
      ],
      [name: :send_gate_held, kind: :count, surface: [{:metric, :send_gate_held}]]
    ]

  use Flezha.Observability.CounterTable,
    table: :relay,
    defaults: [kind: :count, scope: :relay],
    slots: [
      [name: :io_in_received, surface: [{:metric, :relay_io_in_received}]],
      [name: :filtered_nil, surface: [{:metric, :relay_filtered_nil}]],
      # The payload bytes behind `io_in_received` and `filtered_nil`, so a
      # relay's traffic reads in bytes as well as messages.
      [name: :bytes_in, surface: [{:metric, :relay_bytes_in}]],
      [name: :bytes_filtered, surface: [{:metric, :relay_bytes_filtered}]],
      # Every message received is either filtered or forwarded, so these two
      # are never written: `snapshot_all/0` fills them in from the four above,
      # which keeps a forwarded message at two writes on the hot path.
      [name: :forwarded, surface: [{:metric, :relay_forwarded}]],
      [name: :bytes_forwarded, surface: [{:metric, :relay_bytes_forwarded}]],
      # Ingress casts a halted relay dropped; the confirming entry point
      # refuses instead, so this counts only what a cast-based source lost.
      [name: :halted_drops, surface: [{:metric, :relay_halted_drops}]],
      # Plain outputs to a binding recorded not started: it makes no delivery
      # promise, so it drops them.
      [name: :not_started_drops, surface: [{:metric, :relay_not_started_drops}]]
    ]

  # `channel_slots/0`, `relay_slots/0`, `channel_rows/0`,
  # `relay_rows/0`, the index clauses, `channel_reduce/3` and the snapshot
  # readers are all generated from the two row lists above.

  @heartbeat_pairs for row <- @channel_rows,
                       key = Keyword.get(row, :heartbeat),
                       key != nil,
                       do: {key, Keyword.fetch!(row, :name)}

  @heartbeat_keys Enum.map(@heartbeat_pairs, &elem(&1, 0))

  @doc """
  The heartbeat keys this controller mirrors, derived from the rows that
  declare one.

  Read by `Flezha.Runner.StreamChannel` and by the cross-SDK gate, which
  compares them with the key sets the shared core's heartbeat and the Elixir
  SDK build. Deriving the keys FROM the slots is what makes "a key with no
  slot" unconstructible: there is no second list to fall out of step.
  """
  @spec heartbeat_keys() :: [String.t()]
  def heartbeat_keys(), do: @heartbeat_keys

  @doc """
  The heartbeat keys with the slots they mirror into, in layout order — what a
  mirror walks to write an inbound self-report.
  """
  @spec heartbeat_pairs() :: [{String.t(), atom()}]
  def heartbeat_pairs(), do: @heartbeat_pairs

  @doc """
  The rows a surface of `kind` reads, as `{group, row, surface_payload}`.

  The `:flezha_transport` source iterates `surfaces(:metric)` rather than
  keeping a row list of its own, so a slot cannot reach an exporter in one
  release and not the next because an emit line was forgotten.
  """
  @spec surfaces(:metric | :verdict | :telemetry | :internal) ::
          [{:channels | :relays, CounterTable.row(), term()}]
  def surfaces(kind) do
    for {group, rows} <- [channels: @channel_rows, relays: @relay_rows],
        row <- rows,
        {^kind, payload} <- Keyword.fetch!(row, :surface),
        do: {group, row, payload}
  end

  # What each surfaced metric says, as a running total the poller reads every
  # tick: a channel row is summed over every runner, a relay row over every relay.
  @metric_words %{
    channel_io_in_received:
      {"Messages received", "Messages the controller has received from runners."},
    channel_send_frame_calls: {"Messages sent", "Messages the controller has sent to runners."},
    channel_nil_drops:
      {"Lost: stream gone",
       "Messages dropped because the runner's stream was gone; above zero is a loss."},
    channel_flz_data_received:
      {"Data received", "Data messages the controller has received from runners."},
    heartbeat_frames: {"Heartbeats", "Heartbeats runners have sent to show they are alive."},
    dispatches: {"Work finished", "Pieces of work runners report they have finished."},
    credit_parked:
      {"Waiting for allowance",
       "Sends runners are holding back until the controller allows more."},
    inflight: {"Work in progress", "Pieces of work runners hold that are not finished yet."},
    inflight_oldest_ms:
      {"Oldest unfinished work",
       "How long the oldest unfinished piece of work has waited, in milliseconds."},
    mailbox_blocked:
      {"Waited for room", "Times a sender waited because a relay's inbox was full."},
    mailbox_drops_newest:
      {"Dropped on arrival",
       "Messages a full relay inbox dropped as they arrived; above zero is a loss."},
    mailbox_drops_oldest:
      {"Dropped to make room",
       "Older messages a full relay inbox dropped to make room; above zero is a loss."},
    mailbox_pending_pushes:
      {"Held back", "Messages runners are holding back until there is room to send them."},
    pending_push_oldest_ms:
      {"Oldest held-back message",
       "How long the oldest held-back message has waited, in milliseconds."},
    restore_discarded_inputs:
      {"Thrown away on takeover",
       "Input runners threw away when they took over a relay; above zero is a loss."},
    timers_armed: {"Timers set", "Timers relays have set and are waiting on."},
    timers_fired: {"Timers fired", "Timers that have gone off."},
    parts_sent: {"Previews sent", "Partial results runners have sent ahead of a full result."},
    parts_dropped_no_stream:
      {"Previews lost: no stream",
       "Partial results dropped because there was no stream to send them on."},
    parts_dropped_refused: {"Previews refused", "Partial results the receiver refused."},
    parts_dropped_too_large:
      {"Previews too large", "Partial results dropped for being too large."},
    credit_active_sessions:
      {"Paced connections", "Runner connections that pace their sending by an allowance."},
    credit_ledger_level:
      {"Allowance left", "How much more runners may send before they must wait."},
    credit_overrun_total:
      {"Sent past allowance",
       "Messages runners sent beyond their allowance; above zero is a runner fault."},
    send_gate_held:
      {"Senders held", "Times a sender waited because a stream's send buffer was full."},
    relay_io_in_received: {"Messages in", "Messages relays have received."},
    relay_filtered_nil:
      {"Filtered out", "Messages relays have filtered out without passing them on."},
    relay_bytes_in: {"Bytes in", "Bytes of messages relays have received."},
    relay_bytes_filtered: {"Bytes filtered out", "Bytes of messages relays have filtered out."},
    relay_forwarded: {"Messages passed on", "Messages relays have passed on."},
    relay_bytes_forwarded: {"Bytes passed on", "Bytes of messages relays have passed on."},
    relay_halted_drops:
      {"Lost: relay halted",
       "Messages dropped because their relay was halted; above zero is a loss."},
    relay_not_started_drops:
      {"Lost: not started",
       "Messages dropped because their target was not started; above zero is a loss."}
  }

  @doc """
  Each metric this layout surfaces, as a signal declaration for its source. A
  surfaced metric without words here fails `metric_signals/0`, so its source
  cannot register it unexplained.
  """
  @spec metric_signals() :: [{atom(), {:metric, keyword()}}]
  def metric_signals() do
    for {_group, _row, name} <- surfaces(:metric) do
      {label, meaning} = Map.fetch!(@metric_words, name)
      {name, {:metric, label: label, meaning: meaning}}
    end
  end

  @doc """
  The metric readings a snapshot carries: `{name, measurements}` per row whose
  declared surface is a metric.

  The reduction lives here rather than in the source that emits it, because it
  is the rows that say how a slot reduces. A channel slot is read from the
  per-runner collapse first, so a runner-global counter is counted once per
  runner however many secondary streams mirror it; across runners the row's
  `aggregate:` decides, and its `measurement:` names the key the reading is
  emitted under.
  """
  @spec metric_readings(%{channels: [map()], relays: [map()]}) :: [{atom(), map()}]
  def metric_readings(%{channels: channels, relays: relays}) do
    rows = %{channels: Map.values(by_runner(channels)), relays: relays}

    for {group, row, name} <- surfaces(:metric) do
      slot = Keyword.fetch!(row, :name)

      value =
        Enum.reduce(Map.fetch!(rows, group), 0, &reduce(group, slot, &2, Map.fetch!(&1, slot)))

      {name, %{CounterTable.measurement(row) => value}}
    end
  end

  defp reduce(:channels, slot, a, b), do: channel_reduce(slot, a, b)
  defp reduce(:relays, slot, a, b), do: relay_reduce(slot, a, b)

  defp derive_relay(row) do
    %{
      row
      | forwarded: row.io_in_received - row.filtered_nil,
        bytes_forwarded: row.bytes_in - row.bytes_filtered
    }
  end

  @doc """
  Whether the ~1 Hz driver poll + stall classifier should run. The counters
  themselves are always-on; only the polling/classification is gated.
  """
  @spec poll_enabled?() :: boolean()
  def poll_enabled?() do
    Config.get(:observability_data_plane_liveness)
  end

  # ---------- table lifecycle ----------

  @doc """
  Create the registry table if absent. Idempotent and race-tolerant.

  The calling process owns a table it creates, so the controller creates it
  from `Flezha.Observability.Supervisor` at boot; the calls from the register
  functions only cover a node that runs without that supervisor.
  """
  @spec ensure_table() :: :ok
  def ensure_table() do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [
            :set,
            :public,
            :named_table,
            read_concurrency: true,
            write_concurrency: true
          ])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  # ---------- registration ----------

  @doc """
  Register a channel's counter array, keyed by `{runner_id, name}`. The
  caller (the channel process) owns the entry; pass `self()`. Returns the
  `:counters` ref to bump on the hot path.
  """
  @spec register_channel(term(), term()) :: :counters.counters_ref()
  def register_channel(runner_id, name) do
    ensure_table()
    ref = channel_new()
    :ets.insert(@table, {{:channel, runner_id, name}, ref, self()})
    ref
  end

  @doc """
  Register a relay's counter array, keyed by `relay_id`. Pass `self()` as the
  owner. Returns the `:counters` ref.
  """
  @spec register_relay(term()) :: :counters.counters_ref()
  def register_relay(relay_id) do
    ensure_table()
    ref = relay_new()
    :ets.insert(@table, {{:relay, relay_id}, ref, self()})
    ref
  end

  @doc """
  Register a per-runner data-stream NIF server so the stall classifier can
  poll its driver self-report. Without this the classifier only sees the
  control-channel driver, and the data-plane drivers are the blind spot that
  leaves. Pass `self()` (the owning `StreamAcceptor`).
  """
  @spec register_driver(term(), atom(), reference()) :: :ok
  def register_driver(runner_id, type, server_ref) do
    ensure_table()
    :ets.insert(@table, {{:driver, runner_id, type}, server_ref, self()})
    :ok
  end

  @doc """
  Live data-stream driver refs as `{{runner_id, type}, server_ref}` pairs,
  pruning entries whose owner has died.
  """
  @spec driver_refs() :: [{{term(), atom()}, reference()}]
  def driver_refs() do
    case :ets.whereis(@table) do
      :undefined ->
        []

      _ ->
        :ets.match_object(@table, {{:driver, :_, :_}, :_, :_})
        |> Enum.flat_map(fn {{:driver, runner_id, type} = key, ref, owner} ->
          if Process.alive?(owner) do
            [{{runner_id, type}, ref}]
          else
            :ets.delete(@table, key)
            []
          end
        end)
    end
  end

  # ---------- hot-path mutation ----------

  @doc "Increment a channel counter slot by one."
  @spec bump_channel(:counters.counters_ref() | nil, atom()) :: :ok
  def bump_channel(nil, _slot), do: :ok
  def bump_channel(ref, slot), do: channel_bump(ref, slot)

  @doc "Set a channel gauge or stamp slot (e.g. `:last_send_ms`)."
  @spec set_channel(:counters.counters_ref() | nil, atom(), integer()) :: :ok
  def set_channel(nil, _slot, _value), do: :ok
  def set_channel(ref, slot, value), do: channel_set(ref, slot, value)

  @doc "Increment a relay counter slot by one."
  @spec bump_relay(:counters.counters_ref() | nil, atom()) :: :ok
  def bump_relay(nil, _slot), do: :ok
  def bump_relay(ref, slot), do: relay_bump(ref, slot)

  @doc "Add `amount` to a relay counter slot, such as the bytes behind one message."
  @spec add_relay(:counters.counters_ref() | nil, atom(), non_neg_integer()) :: :ok
  def add_relay(nil, _slot, _amount), do: :ok
  def add_relay(ref, slot, amount), do: relay_add(ref, slot, amount)

  # ---------- read side ----------

  @doc """
  Snapshot every live channel and relay counter array, pruning entries whose
  owner has died. Returns `%{channels: [...], relays: [...]}`.
  """
  @spec snapshot_all() :: %{channels: [map()], relays: [map()]}
  def snapshot_all() do
    case :ets.whereis(@table) do
      :undefined ->
        %{channels: [], relays: []}

      _ ->
        entries = :ets.tab2list(@table)

        {channels, relays} =
          Enum.reduce(entries, {[], []}, fn {key, ref, owner}, {chans, rels} ->
            if Process.alive?(owner) do
              case key do
                {:channel, runner_id, name} ->
                  {[channel_snapshot(ref, %{runner_id: runner_id, name: name}) | chans], rels}

                {:relay, relay_id} ->
                  {chans, [ref |> relay_snapshot(%{relay_id: relay_id}) |> derive_relay() | rels]}

                # Driver server refs are polled via `driver_refs/0`, not here.
                {:driver, _runner_id, _type} ->
                  {chans, rels}
              end
            else
              :ets.delete(@table, key)
              {chans, rels}
            end
          end)

        %{channels: channels, relays: relays}
    end
  end

  @doc """
  The five facts about one runner, side by side: how long its control session
  has been silent, how long its data plane has, and whether its heartbeats and
  its dispatches are still advancing.

  Read from the same two liveness samples the stall classifier compares, so the
  session-open question and the data-flowing question are answered from one
  sample and never disagree about which tick they describe. A stamp that was
  never written reads `nil` — "not measured", never "silent forever".
  """
  @spec runner_liveness([map()], [map()] | nil, integer()) :: %{term() => map()}
  def runner_liveness(channels, previous_channels, now) do
    previous = by_runner(previous_channels || [])

    Map.new(by_runner(channels), fn {runner_id, row} ->
      prev = Map.get(previous, runner_id)

      {runner_id,
       %{
         control_silent_ms: silence(now, row.last_control_frame_ms),
         data_silent_ms: silence(now, row.last_send_ms),
         heartbeats_advancing?: advancing?(row, prev, :hb_count),
         dispatches_advancing?: advancing?(row, prev, :hb_dispatches)
       }}
    end)
  end

  @doc """
  How long `runner_id`'s control channel on this node has gone without a
  frame, in ms against this node's monotonic `now`; `nil` when the channel is
  not registered here or never stamped one.
  """
  @spec control_silent_ms(term(), integer()) :: non_neg_integer() | nil
  def control_silent_ms(runner_id, now) do
    with true <- :ets.whereis(@table) != :undefined,
         [{_key, ref, _owner}] <- :ets.lookup(@table, {:channel, runner_id, :control}) do
      silence(now, channel_get(ref, :last_control_frame_ms))
    else
      _ -> nil
    end
  end

  defp silence(_now, 0), do: nil
  defp silence(now, at), do: max(now - at, 0)

  defp advancing?(_row, nil, _slot), do: nil
  defp advancing?(row, prev, slot), do: Map.fetch!(row, slot) > Map.fetch!(prev, slot)

  @doc """
  Collapse channel rows to one row per runner, keyed by runner id.

  A `scope: :runner` slot is one fact about the runner that every channel row
  of that runner repeats, so it keeps its value rather than being added up;
  read per channel it would be multiplied by the size of the runner's stream
  pool. An `:instant` keeps the latest stamp among the channels that took one,
  because summing two clocks means nothing and a channel that never wrote the
  slot has no reading to offer. Every genuinely per-channel counter sums. An
  SDK that does not send the self-report never advances a mirrored slot, so it
  reads 0.
  """
  @spec by_runner([map()]) :: %{term() => map()}
  def by_runner(channels) do
    Enum.reduce(channels, %{}, fn c, acc ->
      row = Map.new(channel_slots(), &{&1, Map.fetch!(c, &1)})
      Map.update(acc, c.runner_id, row, fn seen -> Map.merge(seen, row, &collapse/3) end)
    end)
  end

  # The within-runner collapse, one clause per row, and the row is what says
  # which rule applies. A mirrored runner fact is read once however many
  # channels carry it; a stamp keeps the latest of the readings that exist;
  # everything else is genuinely per channel and sums. Generated from the rows,
  # so a slot cannot be collapsed one way here and another way in the
  # classifier.
  for row <- @channel_rows do
    name = Keyword.fetch!(row, :name)

    cond do
      Keyword.fetch!(row, :kind) == :instant ->
        defp collapse(unquote(name), a, b), do: latest(a, b)

      Keyword.fetch!(row, :scope) == :runner ->
        defp collapse(unquote(name), a, b), do: max(a, b)

      true ->
        defp collapse(unquote(name), a, b), do: a + b
    end
  end

  # A stamp is not a quantity to maximise. Only the control channel writes
  # `last_control_frame_ms` and only the data channels write `last_send_ms`, so
  # every collapse mixes a real reading with the 0 of a channel that never
  # took one, and a monotonic reading is negative on the BEAM. 0 is therefore
  # absent, which is the reading `silence/2` already takes of it.
  defp latest(0, b), do: b
  defp latest(a, 0), do: a
  defp latest(a, b), do: max(a, b)
end
