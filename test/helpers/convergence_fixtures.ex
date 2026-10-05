defmodule FlezhaTest.Helper.ConvergenceFixtures do
  @moduledoc """
  Drive one convergence tick's judging half the way the Loop does.

  A Diff is computed from verdict records, not from raw reality, so a test that
  wants a divergence has to judge first. Doing that here — through
  `Probe.context/4` and `Probe.judge/2`, the same two calls the Loop makes —
  keeps every test on the production path instead of hand-building records that
  the judge would never mint.
  """

  alias Flezha.Convergence.{Diff, Intent, Probe, Snapshot, Verdict}
  alias Flezha.Service.HubManager

  @doc """
  A verdict-record map from `{subject, state}` or `{subject, state, opts}`
  entries — the published snapshot's `relay_health` shape, for a test that
  wants to drive a READER rather than the judge.
  """
  @spec verdicts([{tuple(), atom()} | {tuple(), atom(), keyword()}]) ::
          %{term() => Verdict.t()}
  def verdicts(entries) do
    Map.new(entries, fn entry ->
      {subject, state, opts} =
        case entry do
          {subject, state} -> {subject, state, []}
          {subject, state, opts} -> {subject, state, opts}
        end

      {subject, Verdict.new(subject, state, Keyword.put_new(opts, :now, 0))}
    end)
  end

  @doc """
  Publish the Loop's verdict for each of `rows` (their `health` is the record
  state) beside its unmet-intent clock, and stub `HubManager.relay_rows/1` to
  answer `rows` with each health read back through `Snapshot.relay_health/2`,
  as the registry rows read it. The calling test must `use Mimic`.
  """
  @spec stub_registry_rows(String.t(), [map()], non_neg_integer() | nil) :: :ok
  def stub_registry_rows(namespace, rows, unmet_intent_ms) do
    Snapshot.publish(%{
      relay_health: verdicts(for r <- rows, do: {{:relay, {namespace, r.id}}, r.health}),
      unmet_intent_ms: unmet_intent_ms
    })

    Mimic.stub(HubManager, :relay_rows, fn ns ->
      Enum.map(rows, &%{&1 | health: Snapshot.relay_health(ns, &1.id)})
    end)

    :ok
  end

  @doc """
  The six-key map `Probe.snapshot/0` answers, for a test that stubs the probe.

  Only the relays and the active set ever differ between tests; the other
  four are what an idle tick reads, so a key added to the real snapshot is
  added here once instead of in every module that stubs one.
  """
  @spec snapshot(map(), map()) :: map()
  def snapshot(relays, active_runners) do
    %{
      relays: relays,
      active_runners: active_runners,
      placement: %{},
      recovery: %{},
      writers: MapSet.new(),
      tick_at: 0
    }
  end

  @doc "The verdict records one tick would mint from `reality` and `signals`."
  @spec judge(Intent.t(), map(), map()) :: %{term() => Flezha.Convergence.Verdict.t()}
  def judge(%Intent{} = intent, reality, signals \\ %{}) do
    Probe.judge(
      Probe.context(intent, reality, signals, System.monotonic_time(:millisecond)),
      %{}
    )
  end

  @doc "One tick's Diff: judge, then diff the records — the Loop's own order."
  @spec diff(Intent.t(), map(), map()) :: Diff.t()
  def diff(%Intent{} = intent, reality, signals \\ %{}) do
    Diff.compute(intent, reality, judge(intent, reality, signals), Map.get(signals, :facts, %{}))
  end
end
