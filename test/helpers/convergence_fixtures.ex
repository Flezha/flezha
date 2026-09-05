defmodule FlezhaTest.Helper.ConvergenceFixtures do
  @moduledoc """
  Drive one convergence tick's judging half the way the Loop does.

  A Diff is computed from verdict records, not from raw reality, so a test that
  wants a divergence has to judge first. Doing that here — through
  `Probe.context/4` and `Probe.judge/2`, the same two calls the Loop makes —
  keeps every test on the production path instead of hand-building records that
  the judge would never mint.
  """

  alias Flezha.Convergence.{Diff, Intent, Probe, Verdict}

  @doc """
  A verdict-record map from `{subject, state}` or `{subject, state, opts}`
  entries — the published snapshot's `operative_health` shape, for a test that
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
