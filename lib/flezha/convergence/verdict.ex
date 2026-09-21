defmodule Flezha.Convergence.Verdict do
  @moduledoc """
  One health record per subject, and the only vocabulary the controller judges
  health in.

  `subject` names what the record is about (`{kind, id}`), `state` is one of
  seven closed words, `phase` is the subject's own lifecycle word — a FACT read
  from the subject, never derived from `state` — and `facts` carries the raw
  fields the judge read, so two detectors that disagree about one subject are
  two fields of one row rather than two answers on two surfaces.

  Each state names ONE owner of the next step, and no two states share an owner:

    * `:healthy` — nobody
    * `:degraded` — the loop, in place (restart the plugin, resync the runner)
    * `:failed` — the loop, by rebinding: the runner is gone
    * `:unbound` — the loop, by binding
    * `:parked` — an operator or a plugin fix; the loop does not touch it
    * `:suspended` — nobody until the committed boundary; planned, not a fault
    * `:unknown` — the next tick; an uncertain probe escalates nothing

  `rank/1` is the one order every listing sorts by (`healthy < unknown <
  suspended < degraded < unbound < failed < parked`), and `fold/2` is the one
  table `/healthz` folds records into an endpoint word — so no surface keeps a
  second vocabulary or a second rank.
  """

  @states [:healthy, :degraded, :failed, :unbound, :parked, :suspended, :unknown]
  @kinds [:runner, :operative, :plugin, :channel, :driver]

  # The one order. `:parked` is worst because it is the only state whose remedy
  # nobody in the system owns, and `:unknown` sits just above `:healthy`
  # because an uncertain probe must never outrank a fault somebody read.
  @rank %{healthy: 0, unknown: 1, suspended: 2, degraded: 3, unbound: 4, failed: 5, parked: 6}

  # Which states a kind can reach. Declared here and conformance-tested, so a
  # surface never has to guard against a word its subject cannot produce.
  @reachable %{
    plugin: [:healthy, :degraded, :unknown],
    driver: [:healthy, :degraded, :unknown],
    channel: [:healthy, :degraded, :failed, :unknown],
    runner: [:healthy, :degraded, :failed, :unknown],
    operative: @states
  }

  # Deepest first: a frozen WAL freezes the min-watermark and the outbox cursor
  # as a consequence, so one root cause yields one verdict. Lives here with the
  # state rank because both are "which of these answers wins".
  @stuck_rank %{wal_frozen: 3, watermark_frozen: 2, outbox_flat: 1}

  @event [:flezha, :health, :verdict]

  @type kind :: :runner | :operative | :plugin | :channel | :driver
  @type state ::
          :healthy | :degraded | :failed | :unbound | :parked | :suspended | :unknown
  @type phase ::
          :running
          | :suspended
          | :unbound
          | :parked
          | :finished
          | :prepared
          | :active
          | :inactive
          | nil

  @type t :: %__MODULE__{
          subject: {kind(), term()},
          state: state(),
          reason: term(),
          phase: phase(),
          since_ms: integer() | nil,
          threshold: {atom(), non_neg_integer()} | nil,
          facts: map()
        }

  defstruct subject: nil,
            state: :unknown,
            reason: nil,
            phase: nil,
            since_ms: nil,
            threshold: nil,
            facts: %{}

  @doc "Every state the vocabulary holds."
  @spec states() :: [state()]
  def states(), do: @states

  @doc "Every subject kind a record can be about."
  @spec kinds() :: [kind()]
  def kinds(), do: @kinds

  @doc "The states `kind` can reach. Raises for a kind the vocabulary does not hold."
  @spec reachable(kind()) :: [state()]
  def reachable(kind), do: Map.fetch!(@reachable, kind)

  @doc "The one order: worse states rank higher."
  @spec rank(state()) :: non_neg_integer()
  def rank(state), do: Map.fetch!(@rank, state)

  @doc "The transition event every state change executes."
  @spec event() :: [atom()]
  def event(), do: @event

  @doc """
  The deepest producing-liveness verdict of a set, ranked by root cause. The
  classifier folds its per-Cell verdicts with it and the judge reads the
  winner as a `{:stuck, detail}` reason, so "which stall wins" is answered
  once, here, beside the state rank.
  """
  @spec producing_rank(:idle | :advancing | {:stuck, atom(), map()}) :: {integer(), integer()}
  def producing_rank({:stuck, detail, _ctx}), do: {2, Map.get(@stuck_rank, detail, 0)}
  def producing_rank(:advancing), do: {1, 0}
  def producing_rank(:idle), do: {0, 0}

  @doc """
  Build a record, carrying `since_ms` forward from `previous` while the state
  holds. A state that changed is stamped `now`, so `since_ms` always answers
  "how long has it been THIS".
  """
  @spec new({kind(), term()}, state(), keyword()) :: t()
  def new(subject, state, opts \\ []) do
    now = Keyword.get(opts, :now)
    previous = Keyword.get(opts, :previous)

    since =
      case previous do
        %__MODULE__{state: ^state, since_ms: since} when is_integer(since) -> since
        _ -> now
      end

    %__MODULE__{
      subject: subject,
      state: state,
      reason: Keyword.get(opts, :reason),
      phase: Keyword.get(opts, :phase),
      since_ms: since,
      threshold: Keyword.get(opts, :threshold),
      facts: Keyword.get(opts, :facts, %{})
    }
  end

  @doc """
  The operative's own lifecycle word, read from what the probe observed. The
  one derivation of a phase in the tree: `info` reads it for an operative the
  loop has not judged yet, and the judge reads it for every one it has. An
  observation that names no operative at all (`observed?: false`, which only
  the judge's merged facts carry) has no phase to report.
  """
  @spec phase_of(map()) :: phase()
  def phase_of(observed) do
    observed? = Map.get(observed, :observed?, true)

    cond do
      suspended?(observed) -> :suspended
      observed? and unbound?(observed) -> :unbound
      parked?(observed) -> :parked
      observed? -> :running
      true -> nil
    end
  end

  # The order `Probe.Judge` derives the state in, so the two answers cannot
  # describe one operative differently.
  #
  # Each fact has two spellings, because two callers reach this: the probe's
  # merged facts, and an Overwatch's own state as `info` holds it. A caller
  # carrying neither spelling of the binding is reporting no binding at all,
  # which is not a claim that there is none.
  defp suspended?(o), do: Map.get(o, :suspended?, false) or Map.get(o, :suspended, false)

  defp unbound?(o), do: is_nil(Map.get(o, :bound_runner, Map.get(o, :runner_id, :absent)))

  defp parked?(o), do: Map.get(o, :parked?, false) or not is_nil(Map.get(o, :init_error))

  @doc """
  Execute `[:flezha, :health, :verdict]` for every subject whose state changed
  between two tick maps. Steady state emits nothing — a transition is the
  event, and a record that holds is what `/healthz` is for.
  """
  @spec emit_transitions(%{term() => t()}, %{term() => t()}) :: :ok
  def emit_transitions(previous, current) do
    Enum.each(current, fn {subject, %__MODULE__{} = record} ->
      case Map.get(previous, subject) do
        %__MODULE__{state: same} when same == record.state -> :ok
        _ -> execute_transition(record)
      end
    end)
  end

  defp execute_transition(%__MODULE__{subject: {kind, id}} = record) do
    :telemetry.execute(
      @event,
      %{count: 1},
      %{kind: kind, id: id, state: record.state, reason: record.reason}
    )
  catch
    _, _ -> :ok
  end

  @doc """
  The counts every operator surface reports, derived from the records so no
  reader re-derives them: `bound` is every operative record that is neither
  `unbound`, `failed` nor `unknown` and is not suspended — a dead binding is
  never counted as bound — `producing` is every `healthy` one (an IDLE
  operative counts, a STUCK one does not), and `unknown` is the population the
  loop could not read.
  """
  @spec counts(%{term() => t()}) :: %{
          intended: non_neg_integer(),
          bound: non_neg_integer(),
          producing: non_neg_integer(),
          unknown: non_neg_integer()
        }
  def counts(records) do
    operatives = for {_s, %__MODULE__{subject: {:operative, _}} = r} <- records, do: r

    %{
      intended: length(operatives),
      bound: Enum.count(operatives, &bound?/1),
      producing: Enum.count(operatives, &(&1.state == :healthy)),
      unknown: Enum.count(operatives, &(&1.state == :unknown))
    }
  end

  defp bound?(%__MODULE__{state: state, phase: phase}),
    do: state not in [:unbound, :failed, :unknown] and phase != :suspended

  @doc "The HTTP code an endpoint word carries. `503` is the restart signal and nothing else is."
  @spec http_status(:healthy | :degraded | :recovering | :unhealthy) :: 200 | 503
  def http_status(:unhealthy), do: 503
  def http_status(_status), do: 200

  # ---------- the /healthz fold ----------

  @doc """
  Fold the tick's records and its NON-verdict facts into one endpoint word and
  the body that explains it.

  The second argument carries the rows that are not judgements about a subject
  — recovery states, tick age, pending restore, RPO breaches, intent mismatch,
  stale sensors and the fabric streak — because six of the nine rows below are
  about the node, not about a subject.

  The order is the ranking: an authoritative dead loop and a wedged controller
  fabric outrank every degraded case, a planned recovery outranks a fault, and
  nothing below the first two rows can reach `503`. `:unknown` degrades
  nothing — an uncertain probe never escalates — and is reported as a count.
  """
  @spec fold(%{term() => t()}, map()) ::
          {:healthy | :degraded | :recovering | :unhealthy, map()}
  def fold(records, facts) do
    c = counts(records)
    reasons = degraded_reasons(records, facts)

    cond do
      Map.get(facts, :recovering?, false) ->
        {:recovering, %{status: "recovering", recovery_state: pick_recovery_state(facts)}}

      Map.get(facts, :tick_stale?, false) ->
        {:unhealthy,
         %{status: "unhealthy", reason: "tick_stale", tick_age_ms: facts[:tick_age_ms]}}

      match?({:fabric, _layer, _polls}, facts[:data_plane]) ->
        fabric_body(facts)

      Map.get(facts, :tick_gapped?, false) ->
        {:recovering,
         %{status: "recovering", reason: "tick_resuming", tick_age_ms: facts[:tick_age_ms]}}

      rebinding?(records, facts) ->
        {:recovering,
         Map.merge(%{status: "recovering", reason: "operatives_rebinding"}, sizes(c))}

      Map.get(facts, :pending_restore, false) ->
        {:recovering, Map.merge(%{status: "recovering", reason: "restoring_registry"}, sizes(c))}

      Map.get(facts, :rpo_breaches, []) != [] ->
        {:degraded,
         Map.merge(
           %{
             status: "degraded",
             reason: "durability_rpo_exceeded",
             rpo_breached_refs: length(facts[:rpo_breaches])
           },
           Map.take(sizes(c), [:bound_operatives, :producing_operatives])
         )}

      reasons != [] ->
        {:degraded, degraded_body(records, c, facts, reasons)}

      true ->
        {:healthy,
         Map.merge(%{status: "healthy", tick_age_ms: facts[:tick_age_ms]}, healthy_sizes(c))}
    end
  end

  # The controller's own driver thread is wedged and has stayed wedged. Ranked
  # with `tick_stale`: a heal issued from this controller would travel through
  # the very thread that is not moving, so only a restart clears it.
  defp fabric_body(facts) do
    {:fabric, layer, polls} = facts[:data_plane]

    {:unhealthy,
     %{
       status: "unhealthy",
       reason: "data_plane_fabric_wedged",
       layer: to_string(layer),
       polls: polls
     }}
  end

  # Declared but not all bound, inside the grace: a restart window is a planned
  # recovery, not a fault, and a restart cannot re-bind a runner-orphaned
  # operative. `:unbound` and `:failed` share the window — past it both fall
  # through to `degraded` with their own reason among the joined ones.
  #
  # OPERATIVE records only. The grace is measured from unmet intent, and no
  # other kind can make intent unmet, so a failed runner or channel record
  # would hold the endpoint at `recovering` with no way out of it.
  defp rebinding?(records, facts) do
    Enum.any?(records, fn
      {{:operative, _}, %__MODULE__{state: state}} -> state in [:unbound, :failed]
      _ -> false
    end) and not past_grace?(facts)
  end

  defp degraded_body(records, c, facts, reasons) do
    %{
      status: "degraded",
      reason: Enum.join(reasons, "+"),
      tick_age_ms: facts[:tick_age_ms],
      unmet_intent_ms: facts[:unmet_intent_ms],
      stalled_operatives: subjects(records, &(reason_word(&1) == "output_stalled")),
      stuck_operatives: Map.get(facts, :stuck, %{}),
      plugin_degraded_operatives: plugin_degraded(records),
      intent_mismatch: Map.get(facts, :intent_mismatch, []),
      stale_sensors: Map.get(facts, :stale_sensors, []),
      data_plane_layer: pipeline_layer(facts[:data_plane])
    }
    |> Map.merge(sizes(c))
  end

  # Every reason this tick is degraded, joined. The list IS the branch
  # condition, so the condition and the reported reason cannot drift apart.
  defp degraded_reasons(records, facts) do
    record_reasons =
      records
      |> Map.values()
      |> Enum.flat_map(&List.wrap(reason_word(&1)))
      |> Enum.uniq()

    node_reasons =
      [
        if(Map.get(facts, :intent_mismatch, []) != [], do: "intent_mismatch"),
        if(Map.get(facts, :stale_sensors, []) != [], do: "sensor_stale"),
        if(match?({:pipeline, _layer}, facts[:data_plane]), do: "data_plane_wedged")
      ]
      |> Enum.reject(&is_nil/1)

    Enum.sort(Enum.uniq(record_reasons ++ node_reasons))
  end

  # A record's contribution to the endpoint's reason, or `nil` when it
  # contributes none. `:unknown` and `:suspended` contribute nothing — the
  # first is uncertainty, the second is planned — and a `:driver` record is
  # read by the fabric arm alone.
  defp reason_word(%__MODULE__{subject: {:driver, _}}), do: nil

  defp reason_word(%__MODULE__{state: state}) when state in [:healthy, :unknown, :suspended],
    do: nil

  defp reason_word(%__MODULE__{subject: {:plugin, _}, state: :degraded}), do: "plugin_unhealthy"
  defp reason_word(%__MODULE__{state: :parked}), do: "parked"
  defp reason_word(%__MODULE__{state: :failed}), do: "dead_runner"
  defp reason_word(%__MODULE__{state: :unbound}), do: "operatives_unbound"

  defp reason_word(%__MODULE__{state: :degraded, reason: reason}) do
    case reason do
      {:plugin_unhealthy, _id, _why} -> "plugin_unhealthy"
      {:wedged, _layer} -> "data_plane_wedged"
      _ -> "output_stalled"
    end
  end

  defp past_grace?(facts) do
    ms = Map.get(facts, :unmet_intent_ms)
    is_integer(ms) and ms > Map.get(facts, :unmet_intent_grace_ms, 120_000)
  end

  defp subjects(records, pred) do
    for {{:operative, {_ns, id}}, record} <- records, pred.(record), do: id
  end

  @doc """
  The operatives whose declared plugins are missing, crashed or self-reported
  unhealthy — derived from the records rather than kept as a second list.
  """
  @spec plugin_degraded(%{term() => t()}) :: [binary()]
  def plugin_degraded(records) do
    for(
      {{:operative, {_ns, id}}, %__MODULE__{reason: {:plugin_unhealthy, _, _}}} <- records,
      do: id
    )
    |> Enum.uniq()
    |> Enum.sort()
  end

  defp sizes(c) do
    %{
      intended_operatives: c.intended,
      bound_operatives: c.bound,
      producing_operatives: c.producing,
      unknown_operatives: c.unknown
    }
  end

  defp healthy_sizes(c), do: Map.drop(sizes(c), [:intended_operatives])

  defp pick_recovery_state(facts) do
    states = Map.get(facts, :recovery_states, [])

    cond do
      :recovering in states -> "recovering"
      :recovery_pending in states -> "recovery_pending"
      :timeout in states -> "timeout"
      states == [] -> "unknown"
      true -> Atom.to_string(List.first(states))
    end
  end

  defp pipeline_layer({:pipeline, layer}), do: to_string(layer)
  defp pipeline_layer(_verdict), do: nil
end
