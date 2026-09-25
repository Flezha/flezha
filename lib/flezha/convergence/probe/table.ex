defmodule Flezha.Convergence.Probe.Table do
  @moduledoc """
  Every health detector, declared once: the fact reader it runs, the subject
  kind it speaks about, the threshold names it cites and the `[:flezha, …]`
  events it executes.

  The table is the extension point. A new detector is one entry here — its
  subjects are judged, published and rendered on every surface, and its events
  are bridged by `Flezha.Observability.TelemetryBridge`'s derived half, with no
  edit to the Diff, the snapshot, `/healthz`, the CLI or the bridge.

  Three entries declare events and read nothing (`facts: nil`): the judge's own
  transition event, the runner-side SDK hold whose fact belongs to the runner,
  and the bounded call whose two facts are fields of the binding row. They exist
  so those events reach a sink by declaration rather than by a hand-written
  bridge line.
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Probe.Readers
  alias Flezha.Convergence.StallClassifier
  alias Flezha.Plugin.ProviderBehaviour

  @detectors [
    %{id: :binding, kind: :operator, facts: &Readers.binding/1, events: []},
    %{
      id: :park,
      kind: :operator,
      facts: &Readers.park/1,
      events: [[:flezha, :operator, :parked]]
    },
    %{
      id: :suspension,
      kind: :operator,
      facts: &Readers.suspension/1,
      events: [
        [:flezha, :bounded, :parked],
        [:flezha, :bounded, :suspended],
        [:flezha, :bounded, :failed]
      ]
    },
    %{
      id: :output_stall,
      kind: :operator,
      facts: &Readers.output_stall/1,
      events: [
        [:flezha, :state, :output_stall],
        [:flezha, :state, :input_silence]
      ],
      thresholds: [:output_stall_timeout_ms, :input_silence_timeout_ms]
    },
    %{
      id: :progress,
      kind: :operator,
      facts: &Readers.progress/1,
      events: [[:flezha, :convergence, :operator_stuck]],
      thresholds: [:convergence_output_stall_grace_ms]
    },
    %{
      id: :plugin_health,
      kind: :plugin,
      facts: &Readers.plugin_health/1,
      events: [[:flezha, :plugin, :health, :fail_open]],
      thresholds: [:probe_budget_ms]
    },
    %{
      id: :session,
      kind: :runner,
      facts: &Readers.session/1,
      events: [
        [:flezha, :runner, :liveness_timeout],
        [:flezha, :runner, :restarted],
        [:flezha, :runner, :reconnected],
        [:flezha, :runner, :reconnected_reassigned],
        [:flezha, :runner, :evicted],
        [:flezha, :transport, :connect_rejected]
      ],
      thresholds: [:control_liveness_window_ms, :runner_keepalive]
    },
    # Two entries, because "what exists" and "what is wedged" are two reads: a
    # verdict the classifier could not compute leaves the population standing
    # and every driver and channel in it `unknown`, rather than reporting none.
    %{
      id: :data_plane_population,
      kind: :by_scope,
      facts: &Readers.data_plane_population/1,
      events: [],
      # A channel row carries its runner's control silence, and the judge reads
      # it against this window when no roster the tick read holds that runner.
      thresholds: [:control_liveness_window_ms]
    },
    %{
      id: :data_plane,
      kind: :by_scope,
      facts: &Readers.data_plane/1,
      events: [[:flezha, :data_plane, :stall_localized]],
      thresholds: [:driver_freeze_ms, :stall_ms, :queue_high, :cell_stall_ms]
    },
    # Declaration-only: whether the bounded call answered is `observed?` and
    # `bound_runner: :unknown` on the binding row, so a second reader would
    # only re-derive it.
    %{
      id: :bounded_call,
      kind: :operator,
      facts: nil,
      events: [
        [:flezha, :convergence, :safe_call],
        [:flezha, :convergence, :heal],
        [:flezha, :hub, :unavailable]
      ]
    },
    # Declaration-only: the hold is the runner's own fact and reaches the
    # controller as nothing else, so the entry exists to give the event a sink.
    %{id: :sdk_hold, kind: :operator, facts: nil, events: [[:flezha, :sdk, :parked]]},
    # The judge's own entry. Every state transition executes it, and the
    # derived bridge half maps it from here rather than from a bridge edit.
    %{id: :verdict, kind: :any, facts: nil, events: [[:flezha, :health, :verdict]]}
  ]

  @doc "Every declared detector, in judge order."
  @spec detectors() :: [map()]
  def detectors(), do: @detectors

  @doc "Every `[:flezha, …]` event any detector declares."
  @spec events() :: [[atom()]]
  def events(), do: @detectors |> Enum.flat_map(& &1.events) |> Enum.uniq()

  @doc """
  The live value behind a declared threshold name. Every name a detector cites
  must answer here — the conformance test asserts it, so a detector cannot cite
  a knob nothing configures.
  """
  @spec threshold(atom()) :: non_neg_integer() | nil
  def threshold(:output_stall_timeout_ms), do: Durations.get(:output_stall_timeout_ms)

  def threshold(:input_silence_timeout_ms),
    do: Durations.get(:input_silence_timeout_ms)

  def threshold(:convergence_output_stall_grace_ms),
    do: Durations.get(:convergence_output_stall_grace_ms)

  def threshold(:probe_budget_ms), do: ProviderBehaviour.probe_budget_ms()
  def threshold(:control_liveness_window_ms), do: Durations.get(:control_liveness_window_ms)
  def threshold(:runner_keepalive), do: Durations.get(:runner_keepalive)

  def threshold(name) do
    case Map.fetch(StallClassifier.defaults(), name) do
      {:ok, value} -> value
      :error -> nil
    end
  end
end
