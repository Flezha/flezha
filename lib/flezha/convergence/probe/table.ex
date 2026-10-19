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
    %{id: :binding, kind: :relay, facts: &Readers.binding/1, events: []},
    %{
      id: :halt,
      kind: :relay,
      facts: &Readers.halt/1,
      events: [
        {[:flezha, :relay, :halted],
         label: "Halted",
         outcome: :failure,
         meaning: "A relay was halted after a failure and stays stopped until someone acts."}
      ]
    },
    %{
      id: :suspension,
      kind: :relay,
      facts: &Readers.suspension/1,
      events: [
        {[:flezha, :steps, :suspend_requested],
         label: "Pause asked",
         meaning: "A steps relay's runner asked to pause it at the next safe point."},
        {[:flezha, :steps, :suspended],
         label: "Paused", meaning: "A steps relay paused at a safe point."},
        {[:flezha, :steps, :failed],
         label: "Steps relay failed", outcome: :failure, meaning: "A steps relay failed."}
      ]
    },
    %{
      id: :output_stall,
      kind: :relay,
      facts: &Readers.output_stall/1,
      events: [
        {[:flezha, :state, :output_stall],
         label: "No output",
         outcome: :failure,
         meaning: "A relay has produced no output for longer than allowed."},
        {[:flezha, :state, :input_silence],
         label: "No input",
         outcome: :failure,
         meaning: "A relay has received no input for longer than allowed."}
      ],
      thresholds: [:output_stall_timeout_ms, :input_silence_timeout_ms]
    },
    %{
      id: :progress,
      kind: :relay,
      facts: &Readers.progress/1,
      events: [
        {[:flezha, :convergence, :relay_stuck],
         label: "Stuck", outcome: :failure, meaning: "A relay is making no progress."}
      ],
      thresholds: [:convergence_output_stall_grace_ms]
    },
    %{
      id: :plugin_health,
      kind: :plugin,
      facts: &Readers.plugin_health/1,
      events: [
        {[:flezha, :plugin, :health, :fail_open],
         label: "Health check failed",
         outcome: :failure,
         meaning:
           "A plugin's health check failed or timed out, so it is treated as healthy for now."}
      ],
      thresholds: [:probe_budget_ms]
    },
    %{
      id: :session,
      kind: :runner,
      facts: &Readers.session/1,
      events: [
        {[:flezha, :runner, :liveness_timeout],
         label: "Stopped answering",
         outcome: :failure,
         meaning: "A runner stopped answering the controller's keepalive."},
        {[:flezha, :runner, :restarted],
         label: "Restarted", outcome: :failure, meaning: "A runner restarted."},
        {[:flezha, :runner, :reconnected],
         label: "Reconnected", meaning: "A runner reconnected and kept its relays."},
        {[:flezha, :runner, :reconnected_reassigned],
         label: "Reconnected too late",
         outcome: :failure,
         meaning: "A runner reconnected after its relays had already moved elsewhere."},
        {[:flezha, :runner, :evicted],
         label: "Evicted", outcome: :failure, meaning: "The controller removed a runner."},
        {[:flezha, :transport, :connect_rejected],
         label: "Connection rejected",
         outcome: :failure,
         meaning: "A connection to the controller was rejected."}
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
      events: [
        {[:flezha, :data_plane, :stall_localized],
         label: "Stall traced",
         outcome: :failure,
         meaning: "A stalled message flow was traced to one connection, stream or relay."}
      ],
      thresholds: [:driver_freeze_ms, :stall_ms, :queue_high, :cell_stall_ms]
    },
    # Declaration-only: whether the bounded call answered is `observed?` and
    # `bound_runner: :unknown` on the binding row, so a second reader would
    # only re-derive it.
    %{
      id: :bounded_call,
      kind: :relay,
      facts: nil,
      events: [
        {[:flezha, :convergence, :safe_call],
         label: "Probed", meaning: "A short call checked whether a relay answers."},
        {[:flezha, :convergence, :heal],
         label: "Repaired", meaning: "The controller ran a repair step."},
        {[:flezha, :hub, :unavailable],
         label: "Namespace down",
         outcome: :failure,
         meaning: "A namespace's hub was down when something asked for it."}
      ]
    },
    # The judge's own entry. Every state transition executes it, and the
    # derived bridge half maps it from here rather than from a bridge edit.
    %{
      id: :verdict,
      kind: :any,
      facts: nil,
      events: [
        {[:flezha, :health, :verdict],
         label: "Health changed",
         meaning: "The health of a relay, runner or plugin changed.",
         sentence: "{id} is now {state}"}
      ]
    }
  ]

  @doc "Every declared detector, in judge order."
  @spec detectors() :: [map()]
  def detectors(), do: @detectors

  @doc "Every `[:flezha, …]` event any detector declares."
  @spec events() :: [[atom()]]
  def events(), do: Enum.map(event_descriptions(), &elem(&1, 0))

  @doc """
  Every declared event with what its signal says, once each: the words
  `Flezha.Observability.Source.signals/1` takes beside a signal's kind.
  """
  @spec event_descriptions() :: [{[atom()], keyword()}]
  def event_descriptions(),
    do: @detectors |> Enum.flat_map(& &1.events) |> Enum.uniq_by(&elem(&1, 0))

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
