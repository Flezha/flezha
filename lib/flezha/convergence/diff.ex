defmodule Flezha.Convergence.Diff do
  @moduledoc """
  One pure function — `compute/3` — turning declared `Intent`, probed
  reality, and this tick's verdict records into a structured `Delta`
  enumerating every invariant violation. The records are the health input in
  place of an open signals map: the binding classification reads a record's
  `state` and `reason`, never `bound_runner`/`parked?`/`suspended?` of its own,
  and a `driver` record is planned for by nobody — `/healthz` owns it. This is NOT a per-failure-mode dispatch: each `Delta` field
  corresponds to one invariant, every declared kind is consumed by exactly one
  field, and all catalogue rows reduce to populating these fields.
  """

  alias Flezha.Convergence.{Facts, StallClassifier, Verdict}

  @type operative_ref :: {atom(), binary()}

  @typedoc """
  The tick's data-plane stall verdict resolved to the entity a heal primitive
  can act on, or `nil` when the verdict is `:ok`, fabric-scoped (only a
  process restart reaches those — `/healthz` owns them), or names nothing the
  probe observed.
  """
  @type wedge ::
          nil
          | {:runner, atom(), binary()}
          | {:connection, [operative_ref()]}
          | {:operative, [operative_ref()]}

  @typedoc "One judged record per subject, keyed by subject — the tick's health input."
  @type verdicts :: %{term() => Verdict.t()}

  @typedoc "Why an operative is misbound: the probe saw its runner gone, or the fleet declared the disconnect."
  @type misbound_reason :: :probe | :runner_disconnected

  @type t :: %__MODULE__{
          unbound_operatives: [operative_ref()],
          misbound_operatives: [{operative_ref(), binary(), misbound_reason()}],
          parked_operatives: [{operative_ref(), map(), binary() | nil}],
          replace_operatives: [{operative_ref(), binary() | nil}],
          stalled_operatives: [operative_ref()],
          resync_entities: [{tuple(), binary(), atom()}],
          missing_plugins: [{operative_ref(), binary(), map()}],
          extra_plugins: [{operative_ref(), binary(), pid()}],
          unhealthy_plugins: [{operative_ref(), binary(), map(), term()}],
          stale_tags: [{operative_ref(), binary(), binary()}],
          data_plane_wedge: wedge(),
          data_plane_layer: atom() | nil,
          draining: boolean()
        }

  defstruct unbound_operatives: [],
            misbound_operatives: [],
            parked_operatives: [],
            replace_operatives: [],
            stalled_operatives: [],
            resync_entities: [],
            missing_plugins: [],
            extra_plugins: [],
            unhealthy_plugins: [],
            stale_tags: [],
            data_plane_wedge: nil,
            data_plane_layer: nil,
            draining: false

  # Every field that carries a divergence, as opposed to the two that describe
  # one (`data_plane_layer`) or the node (`draining`). Listed once, so a field
  # added to the struct and forgotten here cannot make a non-empty delta read
  # as nothing to do.
  @healable_fields [
    :unbound_operatives,
    :misbound_operatives,
    :parked_operatives,
    :replace_operatives,
    :stalled_operatives,
    :resync_entities,
    :missing_plugins,
    :extra_plugins,
    :unhealthy_plugins,
    :stale_tags,
    :data_plane_wedge
  ]

  # The fields that mean "this operative needs a runner": what one arriving in
  # the namespace is news for, and what `Planner.waiting_keys/2` plans to derive
  # the cooldown keys that arrival releases.
  @waiting_fields [
    :unbound_operatives,
    :misbound_operatives,
    :parked_operatives,
    :replace_operatives
  ]

  # The verdict states `classify/6` answers with a binding: never bound, bound
  # to a runner that is gone, or left behind by a bind that failed. Declared
  # beside the classification that produces them so the two cannot drift, and
  # held to it by the seam test.
  @awaiting_binding [:unbound, :failed, :parked]

  # The fields a binding op is planned from — the waiting ones, and the row
  # re-publish that follows a placement. While this node drains they are
  # emptied: every binding the local loop can plan places onto this node, and
  # the drain owns the node.
  @placing_fields @waiting_fields ++ [:stale_tags]

  @doc "The fields whose ops place a waiting operative on a runner."
  @spec waiting_fields() :: [atom()]
  def waiting_fields(), do: @waiting_fields

  @doc "The verdict states whose next step is a binding, so a runner arriving is news for them."
  @spec awaiting_binding() :: [Flezha.Convergence.Verdict.state()]
  def awaiting_binding(), do: @awaiting_binding

  @spec compute(Flezha.Convergence.Intent.t(), map(), verdicts(), Facts.t()) :: t()
  def compute(intent, reality, verdicts \\ %{}, facts \\ %{})

  def compute(%Flezha.Convergence.Intent{operatives: declared}, reality, verdicts, facts) do
    observed = Map.get(reality, :operatives, %{})
    placement = Map.get(reality, :placement, %{})
    verdict = wedge_verdict(verdicts)

    {unbound, misbound, parked, replace} =
      binding_divergence(declared, observed, verdicts, placement, facts)

    {missing, extra, unhealthy} = plugin_divergence(declared, observed)

    %__MODULE__{
      unbound_operatives: Enum.reverse(unbound),
      misbound_operatives: Enum.reverse(misbound),
      parked_operatives: Enum.reverse(parked),
      replace_operatives: Enum.reverse(replace),
      stalled_operatives: stalled_refs(declared, verdicts),
      resync_entities: resync_entities(facts),
      missing_plugins: missing,
      extra_plugins: extra,
      unhealthy_plugins: unhealthy,
      stale_tags: stale_tags(declared, observed),
      data_plane_wedge: resolve_wedge(verdict, Map.get(reality, :active_runners, %{}), observed),
      data_plane_layer: StallClassifier.layer(verdict),
      draining: Facts.targets(facts, :node_draining) != []
    }
    |> suppress_placing()
  end

  defp suppress_placing(%__MODULE__{draining: false} = d), do: d

  defp suppress_placing(%__MODULE__{} = d) do
    Enum.reduce(@placing_fields, d, &Map.put(&2, &1, []))
  end

  # The per-operative binding classification, the sibling of `plugin_divergence/2`:
  # one pass over the declared set answering `{unbound, misbound, parked, replace}`.
  defp binding_divergence(declared, observed, verdicts, placement, facts) do
    disconnected = disconnected_runners(facts)

    Enum.reduce(declared, {[], [], [], []}, fn %{namespace: ns, operative_id: rid},
                                               {unb, mis, park, repl} ->
      ref = {ns, rid}

      case classify(
             ref,
             Map.get(observed, ref),
             Map.get(verdicts, {:operative, ref}),
             placement,
             facts,
             disconnected
           ) do
        :skip -> {unb, mis, park, repl}
        {:unbound, entry} -> {[entry | unb], mis, park, repl}
        {:misbound, entry} -> {unb, [entry | mis], park, repl}
        {:parked, entry} -> {unb, mis, [entry | park], repl}
        {:replace, entry} -> {unb, mis, park, [entry | repl]}
      end
    end)
  end

  # What one declared operative's binding diverges into, or `:skip` when this
  # tick has no authoritative answer for it. The judged record IS the answer:
  # every state below names one owner of the next step, and the ones the loop
  # does not own (`:unknown`, `:suspended`, `:healthy`, `:degraded`) plan
  # nothing here.
  defp classify(ref, observed_operative, verdict, placement, facts, disconnected) do
    bound = observed_operative && Map.get(observed_operative, :bound_runner)

    case verdict do
      # Declared but not observed running at all, or an observation that
      # failed: starting it is the hub reconcile's job, and acting on a read
      # that failed mass-migrates healthy operatives on a transient race.
      nil ->
        :skip

      %Verdict{state: :unknown} ->
        :skip

      # Suspended at a committed iteration boundary: unbound BY DESIGN
      # (`workflow-durable-suspension`). Rebinding it would undo the very
      # eviction it asked for; its own wake sources bring it back.
      %Verdict{state: :suspended} ->
        :skip

      # Parked: a failed repair's outcome, reported apart from unbound so the
      # Planner keys it, carrying what the park declared — its reason and
      # what that reason says about the runner's transport.
      %Verdict{state: :parked} ->
        {:parked, {ref, park_args(facts, ref, observed_operative), normalize_runner(bound)}}

      %Verdict{state: :unbound} ->
        classify_bindable(ref, nil, observed_operative, placement)

      %Verdict{state: :failed} ->
        ref
        |> classify_bindable(bound, observed_operative, placement)
        |> with_misbound_reason(ref, disconnected)

      %Verdict{} ->
        :skip
    end
  end

  # The tick's data-plane verdict, read back off the records the judge minted
  # from it. `/healthz` owns a `driver` record and the Diff plans nothing for
  # one, which is the "exactly one consumer" rule expressed as the subject.
  defp wedge_verdict(verdicts) do
    Enum.find_value(verdicts, :ok, fn
      {{:driver, _id}, _record} ->
        nil

      {_subject, %Verdict{reason: {:wedged, layer}, facts: facts}} ->
        {layer, Map.get(facts, :wedge_ctx, %{})}

      _ ->
        nil
    end)
  end

  # A misbound entry carries WHY: the probe saw its runner gone, or the fleet
  # declared the disconnect — and only the first is streak-gated.
  defp with_misbound_reason({:misbound, {ref, dead}}, {ns, _rid}, disconnected) do
    reason = if MapSet.member?(disconnected, {ns, dead}), do: :runner_disconnected, else: :probe
    {:misbound, {ref, dead, reason}}
  end

  defp with_misbound_reason(other, _ref, _disconnected), do: other

  defp disconnected_runners(facts) do
    MapSet.new(Facts.targets(facts, :runner_disconnected))
  end

  # What the park declared, or — for one the loop was never told about (a park
  # from before this loop started) — the reason the probe read, with no
  # transport verdict: nothing may be cut on a reason nobody classified.
  defp park_args(facts, ref, observed_operative) do
    case Facts.args(facts, :operative_parked, ref) do
      %{} = args -> args
      nil -> %{reason: Map.get(observed_operative, :park_reason) || :observed, transport: nil}
    end
  end

  defp normalize_runner(runner) when is_binary(runner) and runner != "", do: runner
  defp normalize_runner(_), do: nil

  # An operative needing a binding whose factory no LOCAL runner advertises
  # while another node's fleet does is a re-place, not a rebind — a local bind
  # is doomed, the operative must move. The local read must be a definite `[]`
  # (`:unknown` classifies as an ordinary rebind, which the heal then skips or
  # fails safely) and the factory must be known.
  defp classify_bindable({ns, _rid} = ref, dead_runner, observed_operative, placement) do
    factory = Map.get(observed_operative, :factory_id)

    case is_binary(factory) && Map.get(placement, {ns, factory}) do
      %{local: [], remote_nodes: [_ | _]} -> {:replace, {ref, dead_runner}}
      _ when is_nil(dead_runner) -> {:unbound, ref}
      _ -> {:misbound, {ref, dead_runner}}
    end
  end

  # I3: declared to produce, observed not producing. The Watchdog names the
  # instance; resolving it against `declared` is what supplies the namespace and
  # what drops a flag for an operative this tick is not healing (an id absent
  # from the declared set, or one in a namespace still mid-recovery — the caller passes only
  # recovery-ready operatives).
  defp stalled_refs(declared, verdicts) do
    for %{namespace: ns, operative_id: rid} <- declared,
        match?(%Verdict{reason: {:stalled, _}}, Map.get(verdicts, {:operative, {ns, rid}})),
        do: {ns, rid}
  end

  # An Entity whose durable log stopped agreeing with its runner: a seq gap the
  # WAL admission declared, or a compaction the Watchdog gave up on. The fact
  # names the runner to resync against; the loop's table paces the attempts.
  defp resync_entities(facts) do
    for {{kind, entity_id}, %{args: %{runner_id: runner_id}}} <- facts,
        kind in [:seq_gap, :compaction_gave_up],
        do: {entity_id, runner_id, kind}
  end

  # A durable row naming a runner other than the one the operative is bound to:
  # a start's late registry write clobbered a rebind's tag. Both sides known,
  # both binaries, and they differ.
  defp stale_tags(declared, observed) do
    for %{namespace: ns, operative_id: rid} <- declared,
        %{bound_runner: bound, registry_tag: tag} <- [Map.get(observed, {ns, rid})],
        is_binary(bound) and is_binary(tag) and bound != tag,
        do: {{ns, rid}, tag, bound}
  end

  # I4: the classifier's single deepest verdict, resolved against the SAME
  # probed reality every other invariant is classified against — a verdict
  # naming a runner or operative the probe did not observe heals nothing and is
  # dropped rather than turned into a no-op op every tick.
  defp resolve_wedge(:ok, _active, _observed), do: nil

  defp resolve_wedge({layer, ctx}, active, observed) do
    case StallClassifier.layer_scope(layer) do
      :runner -> wedged_runner(ctx, active)
      :connection -> wedged_connection(ctx, observed)
      :operative -> wedged_operative(ctx, observed)
      # `:fabric` is /healthz's to report; `nil` is a layer with no scope.
      _ -> nil
    end
  end

  defp wedged_runner(ctx, active) do
    case StallClassifier.runner_of(ctx) do
      nil ->
        nil

      id ->
        # The namespace the runner is live in rides along: a runner-scoped op
        # has no operative to take one from, and the terminal-restart rung has
        # to name it in the telemetry an operator reads during a flap.
        case namespace_of_runner(id, active) do
          nil -> nil
          ns -> {:runner, ns, id}
        end
    end
  end

  defp wedged_connection(ctx, observed) do
    case operatives_on_runner(ctx, observed) do
      nil -> nil
      refs -> {:connection, refs}
    end
  end

  # The verdict names an operative id; the namespace comes from the probe, so a
  # verdict for an operative this controller does not hold resolves to nothing.
  defp wedged_operative(%{operative_id: rid}, observed) do
    case for {{_ns, ^rid} = ref, _obs} <- observed, do: ref do
      [ref | _] -> {:operative, [ref]}
      [] -> nil
    end
  end

  defp wedged_operative(%{runner_id: id} = ctx, observed) when is_binary(id) do
    case operatives_on_runner(ctx, observed) do
      nil -> nil
      refs -> {:operative, refs}
    end
  end

  defp wedged_operative(_ctx, _observed), do: nil

  # Every operative bound to the runner the verdict names. Two verdicts resolve
  # through it: a wedged connection carries all of them, and a runner's handout
  # self-report is process-global — it proves one activation is stuck, not
  # which operative owns it — so the runner's operatives are the healable set
  # either way. A rebind resumes each from durable state, which is safe whether
  # or not that operative was the stuck one.
  defp operatives_on_runner(ctx, observed) do
    case StallClassifier.operatives_of(observed, ctx) do
      [] -> nil
      refs -> refs
    end
  end

  # A driver verdict names its transport; only the per-runner data transports
  # (`{:uds_data, runner_id, type}`) identify a runner. A control-transport
  # verdict names no runner and heals nothing here.
  defp namespace_of_runner(id, active) do
    Enum.find_value(active, fn
      {_ns, :unknown} -> nil
      {ns, set} -> if MapSet.member?(set, id), do: ns
    end)
  end

  # A live pid that is not running: a suspended workflow instance released
  # its plugins with its runner and the rehydrating bind replays them; a
  # parked one (init failed) activates them on its un-park. Neither has
  # anything to heal until then.
  defp plugins_diffable?(observed_operative) do
    is_pid(Map.get(observed_operative, :pid)) and
      not Map.get(observed_operative, :suspended?, false) and
      not Map.get(observed_operative, :parked?, false)
  end

  # I2: a operative's declared plugin set (durable cspec) vs the set actually
  # subscribed in its running extension table, plus each subscribed declared
  # plugin's provider self-reported health. Only diffed for a RUNNING
  # operative (`plugins_diffable?/1`) — a plugin can only be (un)subscribed on
  # one; unbound operatives are healed by binding first, which replays the full
  # cspec plugin list.
  defp plugin_divergence(declared, observed) do
    Enum.reduce(declared, {[], [], []}, fn %{namespace: ns, operative_id: rid, plugins: plugins},
                                           {missing_acc, extra_acc, unhealthy_acc} ->
      ref = {ns, rid}
      {missing, extra, unhealthy} = operative_plugins(ref, plugins, Map.get(observed, ref, %{}))
      {missing_acc ++ missing, extra_acc ++ extra, unhealthy_acc ++ unhealthy}
    end)
  end

  # One operative's plugin divergence: `{missing, extra, unhealthy}`, or three
  # empty lists when its plugin sets are not diffable this tick.
  defp operative_plugins(ref, plugins, observed_operative) do
    if plugins_diffable?(observed_operative) do
      # Two sets, deliberately. `running` is the `:running`-phase set — a
      # declared plugin outside it is missing, whether it was never
      # subscribed or subscribed and failed to activate. `subscribed` is
      # every plugin on the table whatever its phase, so an UNDECLARED one
      # is still torn down while it is `:prepared`. Reading one set for both
      # checks is what let a failed activation read as running.
      running = MapSet.new(Map.get(observed_operative, :running_plugins, []))
      # A subscriber whose owned pid crashed: its provider must be re-run,
      # not merely re-activated, so it is unhealthy rather than missing.
      down = Map.get(observed_operative, :down_plugins, %{})
      health = Map.get(observed_operative, :plugin_health, %{})

      {missing_plugins(ref, plugins, running, down),
       extra_plugins(ref, plugins, observed_operative),
       unhealthy_plugins(ref, plugins, running, down, health)}
    else
      {[], [], []}
    end
  end

  defp missing_plugins(ref, plugins, running, down) do
    for {id, plugin} <- plugins,
        not MapSet.member?(running, id),
        not Map.has_key?(down, id),
        do: {ref, id, plugin}
  end

  # Disjoint from `missing`: a subscribed declared plugin whose provider
  # self-reports unhealthy (the authoritative production signal; an absent or
  # uncertain probe is healthy by contract), or whose subscriber crashed.
  defp unhealthy_plugins(ref, plugins, running, down, health) do
    for {id, plugin} <- plugins,
        verdict = plugin_verdict(id, running, down, health),
        verdict != nil,
        do: {ref, id, plugin, verdict}
  end

  # An empty declared set is ambiguous: a genuinely pluginless operative vs a
  # cspec read that returned nothing (`Intent.declared/0` falls back to [] on
  # any read failure, and the durable cspec can lag a running operative's
  # actual plugin set). Tearing down EVERY running plugin on that basis caused
  # a per-tick stop/re-subscribe storm, so "extra" is reconciled only against a
  # non-empty declaration.
  defp extra_plugins(_ref, [], _observed_operative), do: []

  defp extra_plugins(ref, plugins, observed_operative) do
    declared_ids = MapSet.new(plugins, fn {id, _plugin} -> id end)
    pid = Map.get(observed_operative, :pid)

    for id <- Map.get(observed_operative, :subscribed_plugins, []),
        not MapSet.member?(declared_ids, id),
        do: {ref, id, pid}
  end

  defp plugin_verdict(id, running, down, health) do
    cond do
      Map.has_key?(down, id) -> {:down, Map.get(down, id)}
      not MapSet.member?(running, id) -> nil
      match?({:unhealthy, _}, Map.get(health, id)) -> elem(Map.get(health, id), 1)
      true -> nil
    end
  end

  @doc "True when the delta has no divergence to heal."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = d) do
    Enum.all?(@healable_fields, fn field -> Map.fetch!(d, field) in [[], nil] end)
  end
end
