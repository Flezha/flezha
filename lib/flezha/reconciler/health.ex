defmodule Flezha.Reconciler.Health do
  @moduledoc """
  Computes a relay's health from its `Flezha.Reconciler.Probe` snapshot.

    * `:failed`   — desired state cannot currently be achieved (the relay is not
      runnable and no executor advertises its factory anywhere), so the loop must
      not thrash; the bounded sweep governs re-attempts and it converges back to
      `:healthy` once an executor appears.
    * `:degraded` — the relay is diverging but an executor exists to converge onto
      (dead pid to re-place, parked Overwatch to un-park, unbound stream to
      rebind, stale dead-node entries to purge, or an assigned executor that has
      gone away).
    * `:healthy`  — pid alive on a live node, not parked, data stream bound, and
      the assigned executor reachable.

  Throughput-stall (`last_output_age_ms`) is deliberately NOT classified here in
  Phase 5: stall must be gated on "ingest live AND output stalled" to avoid
  flagging legitimately idle relays, which lands with the Phase 6 ingest signal.
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
      p.dead_node_entries != [] -> :degraded
      not p.executor_reachable? -> :degraded
      true -> :healthy
    end
  end
end
