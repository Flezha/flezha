defmodule Flezha.Observability.OtpBackend.TransportSource do
  @moduledoc """
  Polled source emitting per-transport (`:uds`, `:quic`) metrics.

  Reads `Flezha.Transport.Server.list_active_transports/0` + `get_stats/1`
  for `:open_connections`, and — under `:data_plane_liveness` — the NIF
  driver self-report per transport plus the global BEAM aggregates from
  `Flezha.Transport.LivenessCounters`.

  Cardinality is bounded by construction: driver metrics are per
  transport (at most two) and the BEAM aggregates carry no per-entity
  label, so nothing here grows with runner or operative churn.

  Both server drivers produce the self-report, so the same metrics are
  emitted whichever transport carries the traffic; the `:native` in-node
  transport has no driver and emits none. The metric list is in
  `docs/operations/observability.md` → Shipped sources.
  """

  use Flezha.Observability.OtpBackend.Poller,
    source: :flezha_transport,
    kinds: [:metric],
    cardinality_budget: 40,
    default_poll_ms: 1_000

  alias Flezha.Observability.{Bus, Signal}
  alias Flezha.Transport.LivenessCounters
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
    emit(:driver_write_buffer_bytes, %{value: sum_streams(driver, :write_buffer_len)}, meta)
    emit(:driver_write_buffer_dropped, %{count: sum_streams(driver, :write_buffer_dropped)}, meta)
    emit(:driver_streams, %{value: length(driver.streams)}, meta)
  end

  defp sum_streams(driver, key),
    do: Enum.reduce(driver.streams, 0, fn s, acc -> acc + Map.fetch!(s, key) end)

  # Global BEAM aggregates — no per-entity label, so cardinality is flat.
  defp poll_beam_counters() do
    %{channels: channels, operatives: operatives} =
      try do
        LivenessCounters.snapshot_all()
      catch
        _, _ -> %{channels: [], operatives: []}
      end

    emit_sum(:channel_io_in_received, channels, :io_in_received)
    emit_sum(:channel_send_frame_calls, channels, :send_frame_calls)
    emit_sum(:channel_nil_drops, channels, :nil_drops)
    emit_sum(:channel_flz_data_received, channels, :flz_data_received)
    emit_sum(:operative_io_in_received, operatives, :io_in_received)
    emit_sum(:operative_forwarded, operatives, :forwarded)
    emit_sum(:operative_filtered_nil, operatives, :filtered_nil)

    # Credit-grant flow control. Any non-zero overrun is an anomaly (a
    # credit-speaking sender violated its window), never expected load
    # response; `credit_ledger_level` is the summed remaining window and
    # `credit_parked` the SDKs' park self-report.
    emit_sum(:credit_active_sessions, channels, :credit_active)
    emit_sum(:credit_overrun_total, channels, :credit_overrun_total)
    emit_sum(:credit_parked, channels, :hb_credit_parked)

    # Receive-side overflow, the mirror of `credit_parked`: `mailbox_blocked`
    # climbing is producers held on full entity mailboxes (backpressure
    # working), and either drop counter climbing is data an operative shed —
    # the only handle on that loss from outside the runner.
    emit_sum(:mailbox_blocked, channels, :hb_blocked_parks)
    emit_sum(:mailbox_drops_newest, channels, :hb_drops_newest)
    emit_sum(:mailbox_drops_oldest, channels, :hb_drops_oldest)
    emit_sum(:mailbox_pending_pushes, channels, :hb_pending_pushes)

    # Send-side backpressure, the mirror of `credit_parked`: senders held on a
    # stream's send-buffer gate. Climbing while `channel_nil_drops` stays flat
    # is congestion being absorbed rather than data being lost.
    emit_sum(:send_gate_held, channels, :send_gate_held)

    ledger =
      Enum.reduce(channels, 0, fn row, acc -> acc + Map.get(row, :credit_ledger_level, 0) end)

    emit(:credit_ledger_level, %{value: ledger}, %{scope: :data_plane})
  end

  defp emit_sum(name, rows, key) do
    total = Enum.reduce(rows, 0, fn row, acc -> acc + Map.get(row, key, 0) end)
    emit(name, %{count: total}, %{scope: :data_plane})
  end

  defp emit(name, measurements, metadata) do
    Bus.emit(
      Signal.new(%{
        kind: :metric,
        source: @source,
        name: name,
        measurements: measurements,
        metadata: metadata
      })
    )
  end
end
