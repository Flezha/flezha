defmodule Flezha.Convergence.Loop.Summary do
  @moduledoc """
  The tick's per-tick summary and its publication to `Convergence.Snapshot` —
  the one source `/healthz`, the CLI and telemetry all read.

  `bound < intended` is a recovery window, not a fault: after a restart the
  controller is up while its operatives are not yet rebound. The unmet-intent
  clock runs only while that holds and nothing is recovering.
  """

  alias Flezha.Convergence.{Intent, Snapshot, StallClassifier}
  alias Flezha.Convergence.Loop.State
  alias Flezha.Service.HubManager

  def publish(%Intent{operatives: operatives}, reality, %State{} = state) do
    # `intended` = declared operatives (what should be bound); `bound` = operatives
    # actually observed running on runners. After a restart the controller is
    # up but its operatives are not yet rebound (bound < intended) — that is a
    # recovery window, NOT healthy, even though nothing is "stalled".
    intended = length(operatives)
    bound = map_size(Map.get(reality, :operatives, %{}))
    stalled = Map.keys(state.stalled_operatives)
    stuck = Map.keys(state.stuck_operatives)
    non_producing = Enum.uniq(stalled ++ stuck ++ state.plugin_degraded)
    producing = max(bound - length(non_producing), 0)

    # After a restart the durable registry may hold operatives that are not yet
    # bound to a live pid — either the hub has not started (lazy on runner
    # reconnect) or it has replayed its DETS rows but the stale pids are dead.
    # In that window `Intent` is short of the durable set, so `bound < intended`
    # cannot catch it (it compares two already-shrunken counts). Surfacing the
    # registry-vs-loaded signal lets `/healthz` report `recovering` instead of
    # vacuously `healthy` until every durable operative is reloaded and bound.
    pending_restore = HubManager.durable_registry_pending?()

    # Unmet-intent clock: runs while `bound < intended` and not recovering.
    now = System.monotonic_time(:millisecond)

    unmet_intent_since =
      cond do
        bound >= intended or any_recovering?(reality) -> nil
        is_integer(state.unmet_intent_since) -> state.unmet_intent_since
        true -> now
      end

    Snapshot.publish(%{
      last_tick_at: now,
      stalled_operatives: stalled,
      stuck_operatives: state.stuck_operatives,
      plugin_degraded_operatives: state.plugin_degraded,
      intent_mismatch: state.intent_mismatch,
      intended_operatives: intended,
      bound_operatives: bound,
      producing_operatives: producing,
      pending_restore: pending_restore,
      unmet_intent_ms: unmet_intent_since && now - unmet_intent_since,
      operative_health: operative_health_map(operatives, reality, state),
      data_plane_layer: StallClassifier.layer(state.stall_verdict),
      data_plane_streak: state.stall_streak
    })

    %{state | unmet_intent_since: unmet_intent_since}
  end

  # Per-operative health for the operator surfaces (`flz operative list`,
  # `flz cluster status`), keyed `{namespace, operative_id}`. Derives the same
  # authoritative reality the Diff classifies against, so the CLI sees exactly
  # what the loop sees. A operative only appears `:unknown` here while a tick has
  # not yet observed it or its read failed — never permanently. Mirrors the
  # bound/misbound cond in `Diff.compute/2`, extended with the produce signals.
  @doc false
  @spec operative_health_map([Intent.operative()], map(), State.t()) :: %{
          {atom(), binary()} => atom()
        }
  def operative_health_map(operatives, reality, %State{} = state) do
    observed = Map.get(reality, :operatives, %{})
    active = Map.get(reality, :active_runners, %{})
    plugin_degraded = MapSet.new(state.plugin_degraded)

    Map.new(operatives, fn %{namespace: ns, operative_id: rid} ->
      obs = Map.get(observed, {ns, rid}, %{})
      bound = Map.get(obs, :bound_runner)
      live = Map.get(active, ns, MapSet.new())

      health =
        cond do
          # An observation failed this tick (operative state or live-runner read);
          # no authoritative signal, so report `:unknown` for this tick only.
          bound == :unknown -> :unknown
          # Evicted at a committed iteration boundary, not broken.
          Map.get(obs, :suspended?, false) -> :suspended
          is_nil(bound) -> :unbound
          live == :unknown -> :unknown
          # Bound to a runner that is not live — the loop will rebind.
          not MapSet.member?(live, bound) -> :failed
          Map.get(obs, :parked?, false) -> :degraded
          Map.has_key?(state.stalled_operatives, rid) -> :degraded
          Map.has_key?(state.stuck_operatives, rid) -> :degraded
          MapSet.member?(plugin_degraded, rid) -> :degraded
          true -> :healthy
        end

      {{ns, rid}, health}
    end)
  end

  defp any_recovering?(reality) do
    reality
    |> Map.get(:recovery, %{})
    |> Map.values()
    |> Enum.any?(&(&1 in [:recovery_pending, :recovering, :timeout]))
  end
end
