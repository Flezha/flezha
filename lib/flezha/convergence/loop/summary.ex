defmodule Flezha.Convergence.Loop.Summary do
  @moduledoc """
  The tick's per-tick summary and its publication to `Convergence.Snapshot` —
  the one source `/healthz`, the CLI and telemetry all read.

  The verdict records the judge minted ARE the summary: every reader derives
  its own counts from them with `Verdict.counts/1` — the published map carries
  no second copy — so a dead binding can never be counted as bound on one
  surface and not on another. This is the one site that writes
  `relay_health` into the published snapshot.

  `bound < intended` is a recovery window, not a fault: after a restart the
  controller is up while its relays are not yet rebound. The unmet-intent
  clock runs only while that holds and nothing is recovering.
  """

  alias Flezha.Convergence.{Intent, RecoveryGate, Snapshot, StallClassifier, Verdict}
  alias Flezha.Convergence.Loop.State
  alias Flezha.Service.HubManager

  def publish(%Intent{}, reality, %State{} = state) do
    counts = Verdict.counts(state.verdicts)

    # After a restart the durable registry may hold relays that are not yet
    # bound to a live pid — either the hub has not started (lazy on runner
    # reconnect) or it has replayed its DETS rows but the stale pids are dead.
    # In that window `Intent` is short of the durable set, so `bound < intended`
    # cannot catch it (it compares two already-shrunken counts). Surfacing the
    # registry-vs-loaded signal lets `/healthz` report `recovering` instead of
    # vacuously `healthy` until every durable relay is reloaded and bound.
    pending_restore = HubManager.durable_registry_pending?()

    # Unmet-intent clock: runs while a judged relay is not bound and nothing is
    # recovering. An `:unknown` relay (hosted by another controller, or unread
    # this tick) is not judged here, so it neither starts nor holds the clock.
    now = System.monotonic_time(:millisecond)
    recovering? = RecoveryGate.any_recovering?(Map.get(reality, :recovery, %{}))

    unmet_intent_since =
      cond do
        counts.bound >= counts.intended - counts.unknown or recovering? -> nil
        is_integer(state.unmet_intent_since) -> state.unmet_intent_since
        true -> now
      end

    Snapshot.publish(%{
      last_tick_at: now,
      stuck_relays: state.stuck_relays,
      intent_mismatch: state.intent_mismatch,
      pending_restore: pending_restore,
      unmet_intent_ms: unmet_intent_since && now - unmet_intent_since,
      relay_health: state.verdicts,
      data_plane_layer: StallClassifier.layer(state.stall_verdict),
      data_plane_streak: state.stall_streak
    })

    %{state | unmet_intent_since: unmet_intent_since}
  end
end
