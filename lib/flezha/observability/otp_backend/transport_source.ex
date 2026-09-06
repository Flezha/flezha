defmodule Flezha.Observability.OtpBackend.TransportSource do
  @moduledoc """
  Polled source emitting per-transport (`:uds`, `:quic`) metrics.

  Reads `Flezha.Transport.Server.list_active_transports/0` + `get_stats/1`
  for `:open_connections`, and — under `:data_plane_liveness` — the NIF
  driver self-report per transport plus the global BEAM aggregates from
  `Flezha.Transport.LivenessCounters`, a runner's self-report counted once
  per runner.

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
    emit(:driver_write_buffer_bytes, %{value: sum(driver.streams, :write_buffer_len)}, meta)
    emit(:driver_write_buffer_dropped, %{count: sum(driver.streams, :write_buffer_dropped)}, meta)
    emit(:driver_streams, %{value: length(driver.streams)}, meta)
  end

  # Global BEAM aggregates — no per-entity label, so cardinality is flat.
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
        _, _ -> %{channels: [], operatives: []}
      end

    scope = %{scope: :data_plane}

    for {name, measurements} <- LivenessCounters.metric_readings(snapshot),
        do: emit(name, measurements, scope)
  end

  defp sum(rows, key), do: Enum.reduce(rows, 0, fn row, acc -> acc + Map.fetch!(row, key) end)

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
