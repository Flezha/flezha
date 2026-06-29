defmodule Flezha.Observability.OtpBackend.TransportSource do
  @moduledoc """
  Polled source emitting per-transport (`:uds`, `:quic`) metrics.

  Reads the public `Flezha.Transport.Server.list_active_transports/0`
  + `get_stats/1` surface. Emits `:open_connections` per transport (a
  gauge sourced from the server's pending-stream count).

  Data-plane liveness (always-on, gated by `:data_plane_liveness`):

    * per transport, from the NIF driver self-report —
      `:driver_loop_iterations` (heartbeat counter), `:driver_loop_age_ms`
      (gauge; a large value means the driver thread is wedged),
      `:driver_cmd_queue_depth`, `:driver_write_buffer_bytes`,
      `:driver_streams`.
    * global BEAM aggregates from
      `Flezha.Transport.LivenessCounters` — `:channel_io_in_received`,
      `:channel_send_frame_calls`, `:channel_nil_drops`,
      `:channel_flz_data_received`, `:operative_io_in_received`,
      `:operative_forwarded`, `:operative_filtered_nil`.

  Cardinality stays bounded: driver metrics are per transport (≤ 2);
  BEAM aggregates carry no per-entity label (the stall classifier, not
  this source, does the per-entity localisation), so the label-value set
  never grows with executor/operative churn.

  Default poll interval `1_000` ms; overridable via
  `:flezha, :observability, :otp_backend, :poll_intervals, :flezha_transport`.
  """

  use Flezha.Observability.OtpBackend.Source, source: :flezha_transport, default_poll_ms: 1_000

  alias Flezha.Observability.{Bus, Registry, Signal}
  alias Flezha.Transport.LivenessCounters
  alias Flezha.Transport.Server, as: TransportServer

  @impl GenServer
  def init(_opts) do
    Registry.register_source(%{
      name: @source,
      kinds: [:metric],
      flavour: :process_backed,
      cardinality_budget: 40
    })

    interval = poll_interval_ms()
    Process.send_after(self(), :poll, interval)
    {:ok, %{interval: interval}}
  end

  @impl GenServer
  def handle_info(:poll, state) do
    poll_transports()
    if LivenessCounters.poll_enabled?(), do: poll_beam_counters()
    Process.send_after(self(), :poll, state.interval)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

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

  # Driver self-report (UDS only; QUIC has no `:driver` key). `loop_age_ms`
  # is the headline liveness signal — a value far above the poll interval
  # means the flz-uds-driver thread is wedged.
  defp emit_driver_stats(_transport, nil), do: :ok

  defp emit_driver_stats(transport, driver) do
    meta = %{transport: transport}
    age = max(driver.now_ms - driver.last_loop_ms, 0)
    write_buffer = Enum.reduce(driver.streams, 0, fn s, acc -> acc + s.write_buffer_len end)

    emit(:driver_loop_iterations, %{count: driver.loop_iterations}, meta)
    emit(:driver_loop_age_ms, %{value: age}, meta)
    emit(:driver_cmd_queue_depth, %{value: driver.cmd_queue_depth}, meta)
    emit(:driver_write_buffer_bytes, %{value: write_buffer}, meta)
    emit(:driver_streams, %{value: length(driver.streams)}, meta)
  end

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
