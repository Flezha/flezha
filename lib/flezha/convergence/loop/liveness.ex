defmodule Flezha.Convergence.Loop.Liveness do
  @moduledoc """
  The tick's data-plane liveness pass: sample every control and data driver,
  hand the sample to `StallClassifier`, and report the layer transition.

  The streak counts consecutive polls reporting the SAME layer, so the
  persistence window `/healthz` reads can never be satisfied by a run of
  unrelated one-poll verdicts. A failure here returns the state untouched —
  the classifier must never cost the tick its heals or its publish.
  """

  alias Flezha.Convergence.Loop.State
  alias Flezha.Convergence.StallClassifier
  alias Flezha.Transport.{LivenessCounters, Native}
  alias Flezha.Transport.Server, as: TransportServer

  require Logger

  def classify(%State{} = state) do
    if LivenessCounters.poll_enabled?() do
      current = build_liveness_snapshot()
      verdict = StallClassifier.classify(current, state.prev_liveness, classifier_opts())
      maybe_emit_stall(verdict, StallClassifier.layer(state.stall_verdict))

      %{
        state
        | prev_liveness: current,
          runner_facts: runner_facts(current, state.prev_liveness),
          stall_verdict: verdict,
          stall_streak: stall_streak(state, verdict)
      }
    else
      state
    end
  rescue
    e ->
      Logger.warning("[convergence] data-plane classifier skipped: #{Exception.message(e)}")
      state
  catch
    _, _ -> state
  end

  # The runner facts the judge reads, taken from the SAME two samples the
  # classifier compares — so "the session is open" and "the data plane is
  # draining" are two fields of one sample rather than two detectors that
  # disagree about which tick they describe.
  defp runner_facts(current, previous) do
    LivenessCounters.runner_liveness(
      current.channels,
      previous && previous.channels,
      System.monotonic_time(:millisecond)
    )
  end

  # Consecutive polls reporting the SAME layer. A different layer (or `:ok`)
  # restarts the count, so the persistence window /healthz reads can never be
  # satisfied by a run of unrelated one-poll verdicts.
  defp stall_streak(_state, :ok), do: 0

  defp stall_streak(%State{} = state, verdict) do
    if StallClassifier.layer(verdict) == StallClassifier.layer(state.stall_verdict),
      do: state.stall_streak + 1,
      else: 1
  end

  # The classifier's own defaults, with the one knob an operator whose
  # Procedures legitimately run long has to raise.
  defp classifier_opts() do
    case Application.get_env(:flezha, :convergence_entity_stall_ms) do
      ms when is_integer(ms) and ms > 0 -> %{entity_stall_ms: ms}
      _ -> %{}
    end
  end

  defp build_liveness_snapshot() do
    control_drivers =
      TransportServer.list_active_transports()
      |> Enum.flat_map(fn t ->
        case TransportServer.get_stats(t) do
          {:ok, %{driver: d}} when is_map(d) -> [Map.put(d, :transport, t)]
          _ -> []
        end
      end)

    # Per-runner data-stream servers (StreamAcceptor-owned) — the layer
    # that actually carries operative traffic, invisible to get_stats/1.
    data_drivers =
      LivenessCounters.driver_refs()
      |> Enum.flat_map(fn {{runner_id, type}, ref} ->
        case Native.driver_stats(ref) do
          d when is_map(d) -> [Map.put(d, :transport, {:uds_data, runner_id, type})]
          _ -> []
        end
      end)

    %{channels: channels, operatives: operatives} = LivenessCounters.snapshot_all()
    %{drivers: control_drivers ++ data_drivers, channels: channels, operatives: operatives}
  end

  defp maybe_emit_stall(:ok, last) when last != nil do
    Logger.info("[convergence] data-plane stall cleared (was #{inspect(last)})")
  end

  defp maybe_emit_stall(:ok, nil), do: :ok

  defp maybe_emit_stall({layer, ctx}, last) when layer != last do
    # Telemetry feeds the bus; the log line is what an operator greps during
    # an incident. A data-plane freeze that reaches the logs only at recovery
    # is the case this line exists for.
    Logger.warning(
      "[convergence] data-plane stall localized: layer=#{inspect(layer)} #{inspect(ctx)}"
    )

    :telemetry.execute(
      [:flezha, :data_plane, :stall_localized],
      %{count: 1},
      Map.put(ctx, :layer, layer)
    )
  end

  defp maybe_emit_stall(_verdict, _last), do: :ok

  # Producing-liveness from durable progress markers (report-only; the
  # output-stall ladder stays the heal). Reads the
  # existing WalWriter state at tick cadence; a failure here must never
  # skip the tick's heals or publish.
end
