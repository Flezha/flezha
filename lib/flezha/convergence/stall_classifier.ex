defmodule Flezha.Convergence.StallClassifier do
  @moduledoc """
  Pure liveness classifiers for the convergence Loop: per-layer localization
  of a silent data-plane stall (`classify/3`) and per-operative
  producing-liveness from durable progress markers (`classify_producing/3`).
  Both are detection-only and share the deepest-verdict-wins convention.

  `classify/3`: given a current and previous liveness snapshot (driver
  self-report + `Flezha.Transport.LivenessCounters`), it walks the
  controller→runner path top-down and returns the single most-fundamental
  stalled layer, or `:ok`.
  Detection only — it changes no behaviour and triggers no remediation; the
  convergence Loop emits its verdict as a `[:flezha, :data_plane,
  :stall_localized]` event so the *next* silent freeze self-localises instead
  of needing a live post-mortem.

  Verdicts, in priority order (a lower layer being wedged explains every layer
  above it, so the deepest wins):

    * `:driver_loop_frozen`     — the flz-uds-driver thread stopped looping.
      Judged only against a baseline that shows an established driver (a
      non-zero iteration count and a stamped loop clock) whose counter has
      not gone backwards, so a thread that has not started yet — and a fresh
      thread re-registered at the same transport key — is not a frozen one
    * `:nif_queue_backing_up`   — commands enqueued but the driver isn't
      draining them. Judged only once the driver has demonstrably written,
      so "nothing written since" means it stopped rather than that there was
      nothing to drain
    * `:send_buffer_dropping`   — a stream's send buffer discarded a frame at
      its ceiling since the last tick. Under the send gate a congested sender
      is held rather than dropped, so this counter is an invariant and any
      increment is transport data loss. Detection only: it declares NO scope,
      so it mints no heal op — a rebind does not bring the frame back, and the
      condition is the peer being behind, which `:socket_write_blocked` below
      already localises
    * `:socket_write_blocked`   — driver writing into a buffer the peer won't drain
    * `:runner_not_consuming` — controller sending fine, runner reads nothing back
    * `:runner_pool_wedged`   — runner alive (heartbeats flowing) and fed, but its
      self-reported dispatch counter is frozen AND its block counter is flat —
      the SDK pool stopped processing. A frozen dispatch count with the block
      counter climbing is healthy backpressure, not a wedge: producers are held
      on full mailboxes and a rebind would only move the congestion. Judged
      only across a heartbeat that landed after a frame was sent, because a
      heartbeat counts only what reached the runner before it
    * `:channel_not_sending`    — channel fed but not calling send_frame
    * `:operative_not_forwarding`   — operative fed but not forwarding to its channel
    * `:cell_dispatch_stalled` — the runner is completing work, but one
      activation it took has been outstanding past the threshold: a handout its
      host lost, which per-cell exclusivity would otherwise retire silently
    * `:producer_push_stalled` — a Cell has been withheld work past the
      threshold because a consumer will not take its output. Detection only:
      it declares NO scope, so it mints no heal op, because the thing that is
      behind is the CONSUMER and the remedy for a wedged producer (a rebind)
      would move the congestion rather than clear it. The runner names both
      ends in its own log; the heartbeat carries counters, not identities

  Each check has a built-in "is it actually being fed?" precondition (a count
  that must be *advancing* between ticks), so an idle operative never reads as a
  stall.

  **The driver verdicts need a NIF driver.** `:driver_loop_frozen`,
  `:nif_queue_backing_up`, `:socket_write_blocked`, `:send_buffer_dropping` and
  `:runner_not_consuming` all read the NIF driver self-report. Both the UDS and
  the QUIC server driver produce it, in one shape; the `:native` in-node
  transport has no driver at all, so on a native-only controller `drivers` is
  empty and those five clauses do not fire.
  """

  alias Flezha.Convergence.Verdict
  alias Flezha.Transport.LivenessCounters

  @type snapshot :: %{drivers: [map()], channels: [map()], operatives: [map()]}
  @type verdict :: {atom(), map()} | :ok

  # `cell_stall_ms` is deliberately far above the longest activation any
  # bench-fleet fixture produces (a chained workflow turn is milliseconds; the
  # slowest e2e Tasklet sleeps seconds), because from the runner an activation
  # that is merely slow and one whose handout was lost look identical. Override
  # with `FLEZHA_CONVERGENCE_CELL_STALL_MS` where real Tasklets run longer.
  @defaults %{
    driver_freeze_ms: 5_000,
    stall_ms: 30_000,
    queue_high: 256,
    cell_stall_ms: 300_000
  }

  # What KIND of thing each layer says is wedged. This is the taxonomy half of
  # a verdict and belongs beside the checks that mint them: the Diff resolves a
  # scope to the live cell that can be healed, the Planner picks the remedy,
  # and `/healthz` owns `:fabric`. Adding a verdict therefore declares its scope
  # here and nowhere else.
  #
  # `:fabric` is the controller's own NIF driver thread. No BEAM-level remedy
  # reaches it — every heal the convergence pipeline could issue travels through
  # that thread — which is why it is the one scope that becomes a `503` instead
  # of an op.
  @layer_scopes %{
    driver_loop_frozen: :fabric,
    nif_queue_backing_up: :fabric,
    # Declared with no scope on purpose: the frame is already gone, and the
    # congestion this reports is what `:socket_write_blocked` heals.
    send_buffer_dropping: nil,
    socket_write_blocked: :connection,
    runner_not_consuming: :runner,
    runner_pool_wedged: :runner,
    channel_not_sending: :connection,
    operative_not_forwarding: :operative,
    cell_dispatch_stalled: :operative,
    # Declared with no scope on purpose: a producer withheld work is evidence
    # about the consumer, and every scope here resolves to a heal op.
    producer_push_stalled: nil
  }

  @doc "The classifier's own threshold defaults, by name — read by the probe table."
  @spec defaults() :: map()
  def defaults(), do: @defaults

  @doc "The layer a verdict names, or `nil` for `:ok`."
  @spec layer(verdict()) :: atom() | nil
  def layer(:ok), do: nil
  def layer({layer, _ctx}), do: layer

  @doc """
  The runner a verdict's context names, directly or through its data transport
  key; `nil` when it names none. The one reading of that context — the probe
  table resolves a verdict to its subject with it, and the Diff resolves the
  same verdict to a heal target.
  """
  @spec runner_of(map()) :: binary() | nil
  def runner_of(%{runner_id: id}) when is_binary(id), do: id
  def runner_of(%{transport: {:uds_data, id, _type}}) when is_binary(id), do: id
  def runner_of(_ctx), do: nil

  @doc """
  Every operative the probe saw bound to the runner a verdict's context names,
  sorted; `[]` when the context names no runner or nothing is bound to it. The
  one reading of "who does this wedge reach": the probe table mints one fact
  per operative from it, and the Diff plans one heal per operative from the
  same list.
  """
  @spec operatives_of(map(), map()) :: [term()]
  def operatives_of(observed, ctx) do
    case runner_of(ctx) do
      nil -> []
      id -> observed |> Enum.filter(&bound_to?(&1, id)) |> Enum.map(&elem(&1, 0)) |> Enum.sort()
    end
  end

  defp bound_to?({_ref, obs}, id), do: is_map(obs) and Map.get(obs, :bound_runner) == id

  @doc "Which kind of thing `layer` says is wedged; `nil` for no/unknown layer."
  @spec layer_scope(atom() | nil) :: :fabric | :runner | :connection | :operative | nil
  def layer_scope(layer), do: Map.get(@layer_scopes, layer)

  @doc """
  Classify the stall, if any. `previous` is `nil` on the first tick (no deltas
  yet → always `:ok`). `opts` overrides `#{inspect(@defaults)}`.
  """
  @spec classify(snapshot(), snapshot() | nil, map()) :: verdict()
  def classify(current, previous, opts \\ %{})

  def classify(_current, nil, _opts), do: :ok

  def classify(current, previous, opts) do
    o = Map.merge(@defaults, opts)

    transport_verdict(current, previous, o) || participant_verdict(current, previous, o) || :ok
  end

  # The order across both is the rule — deepest wedged layer wins — so the
  # transport is asked in full before anything above it is asked at all.
  defp transport_verdict(current, previous, o) do
    driver_loop_frozen(current, previous, o) ||
      nif_queue_backing_up(current, previous, o) ||
      send_buffer_dropping(current, previous) ||
      socket_write_blocked(current, previous, o)
  end

  defp participant_verdict(current, previous, o) do
    runner_not_consuming(current, previous, o) ||
      runner_pool_wedged(current, previous) ||
      channel_not_sending(current, previous) ||
      operative_not_forwarding(current, previous) ||
      cell_dispatch_stalled(current, previous, o) ||
      producer_push_stalled(current, previous, o)
  end

  # ---------- driver-global checks ----------

  defp driver_loop_frozen(current, previous, o) do
    find_driver(current, previous, fn d, prev ->
      age = age(d.now_ms, d.last_loop_ms)

      if established?(d, prev) and d.loop_iterations == prev.loop_iterations and
           age > o.driver_freeze_ms do
        {:driver_loop_frozen, %{transport: d.transport, loop_age_ms: age}}
      end
    end)
  end

  defp nif_queue_backing_up(current, previous, o) do
    find_driver(current, previous, fn d, prev ->
      written = total_written(d.streams)
      prev_written = total_written(prev.streams)

      # `prev_written > 0` is this check's form of the fed precondition: a
      # driver that has never written has nothing to have stopped draining,
      # so "written flat" would be true of every driver in its first seconds.
      if established?(d, prev) and prev_written > 0 and
           d.cmd_queue_depth > o.queue_high and d.cmd_queue_depth > prev.cmd_queue_depth and
           written == prev_written do
        {:nif_queue_backing_up, %{transport: d.transport, cmd_queue_depth: d.cmd_queue_depth}}
      end
    end)
  end

  # The fed precondition in its driver-global form. A verdict says something
  # that WAS working stopped, so the baseline must show it working: a driver
  # that has never completed an iteration reports `last_loop_ms == 0`, whose
  # age is `:infinity`, and a stream set that did not exist reports zero
  # progress — both satisfy "no progress" vacuously.
  #
  # The monotonic clause guards the pairing itself: `find_driver` pairs by
  # transport key, and a reconnect registers a NEW driver under the same key
  # whose counters restart. A count that went backwards is a different thread,
  # not a frozen one.
  #
  # The rule holds on BOTH samples, because the age a verdict reports is taken
  # from the current one: judge a stamp only where there is a stamp. That is
  # also what makes an infinite loop age unreportable — the one value that
  # identified this family of false positive.
  defp established?(d, prev) do
    prev.loop_iterations > 0 and prev.last_loop_ms > 0 and
      d.last_loop_ms > 0 and d.loop_iterations >= prev.loop_iterations
  end

  # ---------- driver per-stream checks ----------

  # The one verdict with no threshold and no fed precondition: a counter that
  # moved is itself the evidence. It says a frame was discarded between these
  # two ticks, which under the send gate cannot happen to a sender that was
  # told — so it is reported the moment it is seen, rather than waiting for a
  # window to establish that it kept happening.
  defp send_buffer_dropping(current, previous) do
    find_stream(current, previous, fn d, s, prev_s ->
      if s.write_buffer_dropped > prev_s.write_buffer_dropped do
        {:send_buffer_dropping,
         %{
           transport: d.transport,
           stream_id: s.stream_id,
           dropped: s.write_buffer_dropped - prev_s.write_buffer_dropped
         }}
      end
    end)
  end

  defp socket_write_blocked(current, previous, o) do
    find_stream(current, previous, fn d, s, prev_s ->
      if s.write_buffer_len > 0 and s.write_buffer_len > prev_s.write_buffer_len and
           s.frames_written > prev_s.frames_written and
           age(d.now_ms, s.last_write_ms) > o.stall_ms do
        {:socket_write_blocked,
         %{transport: d.transport, stream_id: s.stream_id, write_buffer_len: s.write_buffer_len}}
      end
    end)
  end

  defp runner_not_consuming(current, previous, o) do
    find_stream(current, previous, fn d, s, prev_s ->
      # `prev_s.frames_read > 0` exempts one-directional streams: a primary
      # data stream is write-only on the controller side (the runner never
      # answers on it), so "reads flat" is its normal state, not a stall.
      if s.frames_written > prev_s.frames_written and s.frames_read == prev_s.frames_read and
           prev_s.frames_read > 0 and
           age(d.now_ms, s.last_read_ms) > o.stall_ms do
        {:runner_not_consuming, %{transport: d.transport, stream_id: s.stream_id}}
      end
    end)
  end

  # ---------- runner self-report checks ----------

  # The runner's heartbeat thread outlives a wedged dispatch pool, so a runner
  # can look perfectly alive while processing nothing.
  # Heartbeat metadata carries a dispatch-progress counter; flag the runner when
  # that counter is frozen.
  #
  # Unless its block counter is climbing. A producer held on a full cell
  # mailbox freezes `dispatches` exactly as a wedge does — the two pictures were
  # byte-for-byte identical — but it is the system working: the consumer is
  # behind, backpressure is propagating, and the remedy for a wedge (a rebind)
  # would only move the congestion. The runner counts a block per wait
  # iteration precisely so this reads as motion.
  #
  # And only across a heartbeat that landed after a send. A heartbeat reports
  # the count as it stood when the runner sent it, so no heartbeat has counted
  # a frame sent after the latest one, and a flat count says nothing about it.
  defp runner_pool_wedged(current, previous) do
    find_runner(current, previous, fn runner_id, cur, prev ->
      if cur.hb_dispatches > 0 and cur.hb_dispatches == prev.hb_dispatches and
           cur.hb_blocked_parks == prev.hb_blocked_parks and answered_send?(cur, prev) do
        {:runner_pool_wedged, %{runner_id: runner_id, dispatches: cur.hb_dispatches}}
      end
    end)
  end

  # A send the baseline heartbeat had not seen and a later one has: either
  # sample's latest send falls between the two samples' latest heartbeats. Only
  # the latest send is stamped, so a burst that straddles a heartbeat waits for
  # one that lands after its last frame.
  defp answered_send?(cur, prev) do
    Enum.any?([prev.last_send_ms, cur.last_send_ms], fn sent ->
      sent > prev.last_heartbeat_ms and sent < cur.last_heartbeat_ms
    end)
  end

  # One activation the runner took and never answered. Invisible to
  # `runner_pool_wedged` by construction: the runner-wide dispatch counter keeps
  # climbing on every other Cell, and per-cell exclusivity means the stalled
  # one simply never appears in a poll result again.
  #
  # Ranked shallower than every runner-scoped verdict — a wedged runner explains
  # a wedged Cell and not the reverse — and it carries the reported age, so a
  # false positive (a legitimately long Tasklet) is diagnosable from the log
  # line alone. The verdict names the runner; resolving it to the operatives
  # that can actually be rebound is the Diff's job.
  defp cell_dispatch_stalled(current, previous, o) do
    find_runner(current, previous, fn runner_id, cur, _prev ->
      if cur.hb_inflight > 0 and cur.hb_inflight_oldest_ms > o.cell_stall_ms do
        {:cell_dispatch_stalled,
         %{
           runner_id: runner_id,
           inflight: cur.hb_inflight,
           oldest_ms: cur.hb_inflight_oldest_ms
         }}
      end
    end)
  end

  # A producer that cannot deliver holds no thread — it holds its own
  # round and is handed no further work — so it is invisible to every check
  # above: it owns no handout for `cell_dispatch_stalled` to age, and the
  # runner's dispatch counter keeps climbing on every other Cell.
  #
  # Ranked last, and scope-less, because it is a report rather than a fault:
  # the Cell named here is WAITING, and the one to look at is whatever is
  # downstream of it. Shares `cell_stall_ms` with the handout check — a push
  # held that long is the same order of pathology.
  defp producer_push_stalled(current, previous, o) do
    find_runner(current, previous, fn runner_id, cur, _prev ->
      if cur.hb_pending_pushes > 0 and cur.hb_pending_push_oldest_ms > o.cell_stall_ms do
        {:producer_push_stalled,
         %{
           runner_id: runner_id,
           pending_pushes: cur.hb_pending_pushes,
           oldest_ms: cur.hb_pending_push_oldest_ms
         }}
      end
    end)
  end

  # ---------- BEAM checks ----------

  defp channel_not_sending(current, previous) do
    prev_index = index_by(previous.channels, fn c -> {c.runner_id, c.name} end)

    find_first(current.channels, fn c ->
      case Map.get(prev_index, {c.runner_id, c.name}) do
        nil ->
          nil

        prev ->
          # Fed but not sending, and not merely an unbound channel dropping
          # (nil_drops flat) — that is a binding problem, not a send wedge.
          if c.io_in_received > prev.io_in_received and
               c.send_frame_calls == prev.send_frame_calls and
               c.nil_drops == prev.nil_drops do
            {:channel_not_sending, %{runner_id: c.runner_id, channel: c.name}}
          end
      end
    end)
  end

  defp operative_not_forwarding(current, previous) do
    prev_index = index_by(previous.operatives, fn r -> r.operative_id end)

    find_first(current.operatives, fn r ->
      case Map.get(prev_index, r.operative_id) do
        nil ->
          nil

        prev ->
          io_delta = r.io_in_received - prev.io_in_received
          filtered_delta = r.filtered_nil - prev.filtered_nil

          # Fed, not forwarding, and the gap isn't explained by the on_input
          # filter dropping everything.
          if io_delta > 0 and r.forwarded == prev.forwarded and filtered_delta < io_delta do
            {:operative_not_forwarding, %{operative_id: r.operative_id}}
          end
      end
    end)
  end

  # ---------- producing-liveness (durable progress markers) ----------

  @producing_defaults %{grace_ms: 60_000}

  @doc """
  Per-operative producing-liveness from durable progress markers, compared at
  the convergence tick cadence.

  `current` is one map per live Cell (built by the Loop from
  `Flezha.State.Watchdog.cell_statuses/0`): always `:operative_id`,
  `:cell`, `:last_change_seen_at`, `:last_advance_at` (the WAL
  fed/advance evidence), `:out_pending_seq`, `:outbox_flushed_seq` and
  `:outbox_unconfirmed_ms` (how long the oldest dispatched output has gone
  unconfirmed by its sink), plus —
  when the effectively-once tier runs — `:input_log_tail` and this Cell's
  durable `:consumed_offset`. `baseline` is a snapshot taken at least one grace
  window earlier (`nil` before one has aged enough); cursor checks compare
  against it and are skipped when it or the cursor fields are absent, so
  classification degrades to WAL-seq-only.

  Every verdict carries the fed precondition: input-side evidence advancing
  while the progress marker is flat past the grace window is `{:stuck,
  detail, ctx}` (detail ∈ `:wal_frozen | :watermark_frozen | :outbox_flat`,
  ctx naming the lagging Cell); flat markers with no input is `:idle`
  (producing-healthy). Per operative the single deepest verdict wins.
  """
  @spec classify_producing([map()], [map()] | nil, map()) :: %{
          binary() => :advancing | :idle | {:stuck, atom(), map()}
        }
  def classify_producing(current, baseline, opts \\ %{}) do
    o = Map.merge(@producing_defaults, opts)
    now = Map.get_lazy(o, :now, fn -> System.monotonic_time(:millisecond) end)
    base = index_by(baseline || [], fn m -> {m.operative_id, m.cell} end)

    current
    |> Enum.group_by(& &1.operative_id)
    |> Map.new(fn {operative_id, markers} ->
      verdicts =
        Enum.map(markers, fn m ->
          marker_verdict(m, Map.get(base, {m.operative_id, m.cell}), now, o.grace_ms)
        end)

      {operative_id, Enum.max_by(verdicts, &Verdict.producing_rank/1)}
    end)
  end

  defp marker_verdict(m, base, now, grace_ms) do
    fed? = now - m.last_change_seen_at < grace_ms
    advanced? = now - m.last_advance_at < grace_ms

    cond do
      fed? and not advanced? -> {:stuck, :wal_frozen, %{cell: m.cell}}
      watermark_frozen?(m, base) -> {:stuck, :watermark_frozen, %{cell: m.cell}}
      outbox_flat?(m, base, grace_ms) -> {:stuck, :outbox_flat, %{cell: m.cell}}
      advanced? -> :advancing
      true -> :idle
    end
  end

  # Tier-on: the input-log tail grew over the grace window while THIS Cell's
  # durable consumed offset did not advance. Read per Cell, not as the
  # operative's min-watermark: the minimum across Cells IS that watermark, so
  # any frozen Cell still freezes the operative's verdict — and the verdict
  # names the Cell actually responsible, which a class-wide scalar cannot.
  defp watermark_frozen?(m, base) when is_map(base) do
    is_integer(m[:input_log_tail]) and is_integer(base[:input_log_tail]) and
      is_integer(m[:consumed_offset]) and is_integer(base[:consumed_offset]) and
      m.input_log_tail > base.input_log_tail and m.consumed_offset <= base.consumed_offset
  end

  defp watermark_frozen?(_m, _base), do: false

  # Tier-on: output-bearing WAL records accumulated while the outbox flush
  # cursor stayed flat — or output already handed to the plugin chain has gone
  # unconfirmed past the grace window. The second half is what catches a sink
  # that stopped answering on a Cell quiet enough to emit nothing new, which
  # no comparison against a baseline can see.
  defp outbox_flat?(m, base, grace_ms) do
    unconfirmed_stalled?(m, grace_ms) or accumulating?(m, base)
  end

  defp unconfirmed_stalled?(m, grace_ms),
    do: is_integer(m[:outbox_unconfirmed_ms]) and m.outbox_unconfirmed_ms >= grace_ms

  defp accumulating?(m, base) when is_map(base) do
    is_integer(m[:out_pending_seq]) and is_integer(base[:out_pending_seq]) and
      is_integer(m[:outbox_flushed_seq]) and is_integer(base[:outbox_flushed_seq]) and
      m.out_pending_seq > base.out_pending_seq and
      m.outbox_flushed_seq == base.outbox_flushed_seq
  end

  defp accumulating?(_m, _base), do: false

  # ---------- helpers ----------

  defp find_driver(current, previous, fun) do
    prev_index = index_by(previous.drivers, fn d -> d.transport end)

    find_first(current.drivers, fn d ->
      case Map.get(prev_index, d.transport) do
        nil -> nil
        prev -> fun.(d, prev)
      end
    end)
  end

  # Pair each runner's collapsed row with its previous one and judge only a
  # runner that is demonstrably still being fed AND still answering — the
  # "is it actually being fed?" precondition in its runner-scoped form, shared
  # by every self-report check. A mirrored slot an SDK never sends reads 0
  # there — nothing outstanding, nothing blocked, a dispatch count the wedge
  # check does not judge — so it never stalls.
  defp find_runner(current, previous, fun) do
    prev_index = LivenessCounters.by_runner(previous.channels)

    current.channels
    |> LivenessCounters.by_runner()
    |> find_first(fn {runner_id, cur} ->
      case Map.get(prev_index, runner_id) do
        nil ->
          nil

        prev ->
          if cur.send_frame_calls > prev.send_frame_calls and cur.hb_count > prev.hb_count,
            do: fun.(runner_id, cur, prev)
      end
    end)
  end

  defp find_stream(current, previous, fun) do
    prev_index = index_by(previous.drivers, fn d -> d.transport end)

    find_first(current.drivers, fn d ->
      case Map.get(prev_index, d.transport) do
        nil ->
          nil

        prev ->
          prev_streams = index_by(prev.streams, fn s -> s.stream_id end)

          find_first(d.streams, fn s ->
            case Map.get(prev_streams, s.stream_id) do
              nil -> nil
              prev_s -> fun.(d, s, prev_s)
            end
          end)
      end
    end)
  end

  defp find_first(list, fun) do
    Enum.find_value(list, fun)
  end

  defp index_by(list, key_fun) do
    Map.new(list, fn item -> {key_fun.(item), item} end)
  end

  defp total_written(streams) do
    Enum.reduce(streams, 0, fn s, acc -> acc + s.frames_written end)
  end

  # Driver-elapsed age; `last_ms == 0` means "never", reported as a large age.
  defp age(_now_ms, 0), do: :infinity
  defp age(now_ms, last_ms), do: max(now_ms - last_ms, 0)
end
