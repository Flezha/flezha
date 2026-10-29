defmodule Flezha.Observability.OtpBackend.TransportSource do
  @moduledoc """
  Polled source emitting per-transport (`:uds`, `:quic`) metrics.

  Reads `Flezha.Transport.Server.list_active_transports/0` + `get_stats/1`
  for `:open_connections`, and — under `:data_plane_liveness` — the NIF
  driver self-report per transport plus the global BEAM aggregates from
  `Flezha.Transport.LivenessCounters`, a runner's self-report counted once
  per runner.

  Cardinality is bounded by construction: driver metrics are per
  transport (at most two) and the BEAM aggregates carry no per-cell
  label, so nothing here grows with runner or relay churn.

  Both server drivers produce the self-report, so the same metrics are
  emitted whichever transport carries the traffic.
  """

  alias Flezha.Transport.LivenessCounters

  # What each liveness metric says, as a running total the poller reads every
  # tick: a channel row is summed over every runner, a relay row over every relay.
  # A metric `LivenessCounters.surfaces(:metric)` names without words here fails
  # the build, so it cannot register unexplained.
  @liveness_words %{
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

  use Flezha.Observability.OtpBackend.Poller,
    source: :flezha_transport,
    cardinality_budget: 40,
    default_poll_ms: 1_000,
    area: :machine,
    title: "Message flow (low-level)",
    description:
      "Runner connections and the running totals of messages moving between runners and relays.",
    signals:
      [
        open_connections:
          {:metric,
           label: "Open connections",
           meaning: "Runner connections open on one transport.",
           sentence: "{value:connection} open over {transport}"},
        driver_loop_iterations:
          {:metric,
           label: "Driver turns",
           meaning: "Times the transport's driver went round its loop since it started."},
        driver_loop_age_ms:
          {:metric,
           label: "Driver idle time",
           meaning:
             "Milliseconds since the transport's driver last went round its loop; a high value means it is stuck."},
        driver_cmd_queue_depth:
          {:metric,
           label: "Driver backlog", meaning: "Commands waiting for the transport's driver."},
        driver_write_buffer_bytes:
          {:metric,
           label: "Bytes waiting to send",
           meaning: "Bytes waiting in the transport's send buffers."},
        driver_write_buffer_dropped:
          {:metric,
           label: "Messages lost",
           meaning: "Messages the transport has lost; anything above zero is a defect."},
        driver_streams:
          {:metric, label: "Open streams", meaning: "Streams the transport's driver holds open."}
      ] ++
        Enum.map(LivenessCounters.surfaces(:metric), fn {_group, _row, name} ->
          {label, meaning} = Map.fetch!(@liveness_words, name)
          {name, {:metric, label: label, meaning: meaning}}
        end)

  alias Flezha.Transport.Server, as: TransportServer

  @impl Flezha.Observability.OtpBackend.Poller
  def poll(state) do
    poll_transports()
    if LivenessCounters.poll_enabled?(), do: poll_beam_counters()
    state
  end

  # ---------- Internals ----------

  defp poll_transports() do
    transports =
      try do
        TransportServer.list_active_transports()
      catch
        _, _ -> []
      end

    Enum.each(transports, &emit_for_transport/1)
  end

  defp emit_for_transport(transport) do
    case TransportServer.get_stats(transport) do
      {:ok, %{pending_streams: pending} = stats} ->
        emit(:open_connections, %{value: pending}, %{transport: transport})
        if LivenessCounters.poll_enabled?(), do: emit_driver_stats(transport, stats[:driver])

      _ ->
        :ok
    end
  end

  # Driver self-report; a transport with no driver has no `:driver` key.
  # `loop_age_ms` is the headline liveness signal — a value far above the poll
  # interval means the driver thread is wedged.
  #
  # `write_buffer_dropped` is not a metric to trend: the send gate holds a
  # sender rather than discarding its frame, so any non-zero value is a frame
  # the transport lost and is a defect to report.
  defp emit_driver_stats(_transport, nil), do: :ok

  defp emit_driver_stats(transport, driver) do
    meta = %{transport: transport}
    age = max(driver.now_ms - driver.last_loop_ms, 0)

    emit(:driver_loop_iterations, %{count: driver.loop_iterations}, meta)
    emit(:driver_loop_age_ms, %{value: age}, meta)
    emit(:driver_cmd_queue_depth, %{value: driver.cmd_queue_depth}, meta)
    emit(:driver_write_buffer_bytes, %{value: sum(driver.streams, :write_buffer_len)}, meta)
    emit(:driver_write_buffer_dropped, %{count: sum(driver.streams, :write_buffer_dropped)}, meta)
    emit(:driver_streams, %{value: length(driver.streams)}, meta)
  end

  # Global BEAM aggregates — no per-cell label, so cardinality is flat.
  #
  # Nothing here names a slot, and nothing here decides how one reduces. The
  # layout declares which slots are metrics, how each is collapsed per runner
  # and reduced across them, and which measurement key it is emitted under;
  # this source asks for the readings and emits them. A slot therefore cannot
  # reach an exporter in one release and not the next because a line was
  # forgotten, and adding a liveness metric is one row in
  # `Flezha.Transport.LivenessCounters`.
  defp poll_beam_counters() do
    snapshot =
      try do
        LivenessCounters.snapshot_all()
      catch
        _, _ -> %{channels: [], relays: []}
      end

    scope = %{scope: :data_plane}

    for {name, measurements} <- LivenessCounters.metric_readings(snapshot),
        do: emit(name, measurements, scope)
  end

  defp sum(rows, key), do: Enum.reduce(rows, 0, fn row, acc -> acc + Map.fetch!(row, key) end)
end
