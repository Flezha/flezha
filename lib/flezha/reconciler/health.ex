defmodule Flezha.Reconciler.Health do
  @moduledoc """
  Computes a relay's health from its `Flezha.Reconciler.Probe` snapshot.

    * `:failed`   — desired state cannot currently be achieved (the relay is not
      runnable and no executor advertises its factory anywhere), so the loop must
      not thrash; the bounded sweep governs re-attempts and it converges back to
      `:healthy` once an executor appears.
    * `:degraded` — the relay is diverging but an executor exists to converge onto
      (dead pid to re-place, parked Overwatch to un-park, unbound stream to
      rebind, a throughput stall to clear, stale dead-node entries to purge, or
      an assigned executor that has gone away).
    * `:healthy`  — pid alive on a live node, not parked, data stream bound, not
      throughput-stalled, and the assigned executor reachable.

  Throughput-stall (`throughput_stalled?`) is gated on "ingest live (recent
  unbound-stream drop) AND output stalled" (Phase 6.2) so a legitimately idle
  relay is never flagged. The persistence-path output-staleness is owned by
  `Flezha.State.Watchdog` (`[:flezha, :state, :output_stall]`) and consumed,
  not re-detected, here.
  """

  alias Flezha.Reconciler.Probe

  @type t() :: :healthy | :degraded | :failed

  @spec classify(Probe.t()) :: t()
  def classify(%Probe{} = p) do
    cond do
      # Not runnable (dead or parked) and nowhere to place it → terminal until an
      # executor for the factory connects.
      (not p.relay_alive? or p.parked?) and not p.executor_available? -> :failed
      not p.relay_alive? -> :degraded
      p.parked? -> :degraded
      p.stream_bound? == false -> :degraded
      # Data in but nothing out (catches a wedged channel whose stream probe is
      # :unknown yet frames are being dropped).
      p.throughput_stalled? -> :degraded
      # A running plugin reporting unhealthy (e.g. parked Kafka producer, idle
      # WebSocket) — surfaced now; the loop escalates after it persists.
      p.unhealthy_plugins != [] -> :degraded
      p.dead_node_entries != [] -> :degraded
      not p.executor_reachable? -> :degraded
      true -> :healthy
    end
  end
end
