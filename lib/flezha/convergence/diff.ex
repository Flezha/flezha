defmodule Flezha.Convergence.Diff do
  @moduledoc """
  One pure function — `compute/3` — turning declared `Intent`, probed
  reality, and this tick's sensor `signals` into a structured `Delta`
  enumerating every invariant violation. This is NOT a per-failure-mode
  dispatch: each `Delta` field corresponds to one invariant, and all
  catalogue rows reduce to populating these fields.
  """

  alias Flezha.Convergence.StallClassifier

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

  @typedoc """
  Facts the sensors produced this tick, as opposed to what the Probe observed:
  the data-plane classifier `:verdict` and the `:stalled` operative ids the
  Watchdog has flagged. Every entry is optional — a Diff computed without them
  simply reports no divergence in those fields.
  """
  @type signals :: %{
          optional(:verdict) => StallClassifier.verdict(),
          optional(:stalled) => [binary()]
        }

  @type t :: %__MODULE__{
          unbound_operatives: [operative_ref()],
          misbound_operatives: [{operative_ref(), binary()}],
          replace_operatives: [{operative_ref(), binary() | nil}],
          stalled_operatives: [operative_ref()],
          missing_plugins: [{operative_ref(), binary(), map()}],
          extra_plugins: [{operative_ref(), binary(), pid()}],
          unhealthy_plugins: [{operative_ref(), binary(), map(), term()}],
          data_plane_wedge: wedge(),
          data_plane_layer: atom() | nil
        }

  defstruct unbound_operatives: [],
            misbound_operatives: [],
            replace_operatives: [],
            stalled_operatives: [],
            missing_plugins: [],
            extra_plugins: [],
            unhealthy_plugins: [],
            data_plane_wedge: nil,
            data_plane_layer: nil

  @spec compute(Flezha.Convergence.Intent.t(), map(), signals()) :: t()
  def compute(intent, reality, signals \\ %{})

  def compute(%Flezha.Convergence.Intent{operatives: declared}, reality, signals) do
    active = Map.get(reality, :active_runners, %{})
    observed = Map.get(reality, :operatives, %{})
    placement = Map.get(reality, :placement, %{})
    verdict = Map.get(signals, :verdict, :ok)

    {unbound, misbound, replace} =
      Enum.reduce(declared, {[], [], []}, fn %{namespace: ns, operative_id: rid},
                                             {unb, mis, repl} ->
        ref = {ns, rid}
        observed_operative = Map.get(observed, ref)
        live = Map.get(active, ns, MapSet.new())
        bound = observed_operative && Map.get(observed_operative, :bound_runner)

        cond do
          # Declared but not observed running at all: starting it is the hub
          # reconcile's job through the idempotent start authority — a heal op
          # can only act on a live Overwatch.
          observed_operative == nil ->
            {unb, mis, repl}

          # `:unknown` = an observation failed this tick — either the operative's
          # own state read (`bound`) or the namespace's live-runner read
          # (`live`, Probe could not reach FleetManager). With no authoritative
          # signal we must NOT classify it: acting on an empty/failed read
          # mass-migrates healthy operatives on a transient read race.
          bound == :unknown ->
            {unb, mis, repl}

          # Suspended at a committed iteration boundary: unbound BY DESIGN
          # (`workflow-durable-suspension`). Rebinding it would undo the very
          # eviction it asked for; its own wake sources bring it back.
          Map.get(observed_operative, :suspended?, false) ->
            {unb, mis, repl}

          is_nil(bound) ->
            case classify_bindable(ref, nil, observed_operative, placement) do
              {:replace, entry} -> {unb, mis, [entry | repl]}
              {:unbound, ref} -> {[ref | unb], mis, repl}
            end

          live == :unknown ->
            {unb, mis, repl}

          not MapSet.member?(live, bound) ->
            case classify_bindable(ref, bound, observed_operative, placement) do
              {:replace, entry} -> {unb, mis, [entry | repl]}
              {:misbound, entry} -> {unb, [entry | mis], repl}
            end

          true ->
            {unb, mis, repl}
        end
      end)

    {missing, extra, unhealthy} = plugin_divergence(declared, observed)

    %__MODULE__{
      unbound_operatives: Enum.reverse(unbound),
      misbound_operatives: Enum.reverse(misbound),
      replace_operatives: Enum.reverse(replace),
      stalled_operatives: stalled_refs(declared, Map.get(signals, :stalled, [])),
      missing_plugins: missing,
      extra_plugins: extra,
      unhealthy_plugins: unhealthy,
      data_plane_wedge: resolve_wedge(verdict, active, observed),
      data_plane_layer: StallClassifier.layer(verdict)
    }
  end

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
  # what drops a flag for an operative this tick is not healing (an id no longer
  # declared, or one in a namespace still mid-recovery — the caller passes only
  # recovery-ready operatives).
  defp stalled_refs(_declared, []), do: []

  defp stalled_refs(declared, stalled) do
    ids = MapSet.new(stalled)

    for %{namespace: ns, operative_id: rid} <- declared,
        MapSet.member?(ids, rid),
        do: {ns, rid}
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
    case runner_of(ctx) do
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

  # Every operative bound to the runner the verdict names, sorted. Two verdicts
  # resolve through it: a wedged connection carries all of them, and a runner's
  # handout self-report is process-global — it proves one activation is stuck,
  # not which operative owns it — so the runner's operatives are the healable
  # set either way. A rebind resumes each from durable state, which is safe
  # whether or not that operative was the stuck one.
  defp operatives_on_runner(ctx, observed) do
    with id when is_binary(id) <- runner_of(ctx),
         [_ | _] = refs <- for({ref, %{bound_runner: ^id}} <- observed, do: ref) do
      Enum.sort(refs)
    else
      _ -> nil
    end
  end

  # A driver verdict names its transport; only the per-runner data transports
  # (`{:uds_data, runner_id, type}`) identify a runner. A control-transport
  # verdict names no runner and heals nothing here.
  defp runner_of(%{runner_id: id}) when is_binary(id), do: id
  defp runner_of(%{transport: {:uds_data, id, _type}}) when is_binary(id), do: id
  defp runner_of(_ctx), do: nil

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
      observed_operative = Map.get(observed, ref, %{})
      pid = Map.get(observed_operative, :pid)

      if plugins_diffable?(observed_operative) do
        running = MapSet.new(Map.get(observed_operative, :running_plugins, []))
        health = Map.get(observed_operative, :plugin_health, %{})
        declared_ids = MapSet.new(plugins, fn {id, _plugin} -> id end)

        missing =
          for {id, plugin} <- plugins, not MapSet.member?(running, id), do: {ref, id, plugin}

        # Disjoint from `missing`: a subscribed declared plugin whose provider
        # self-reports unhealthy (the authoritative production signal; an
        # absent or uncertain probe is healthy by contract).
        unhealthy =
          for {id, plugin} <- plugins,
              MapSet.member?(running, id),
              match?({:unhealthy, _}, Map.get(health, id)),
              do: {ref, id, plugin, elem(Map.get(health, id), 1)}

        # An empty declared set is ambiguous: a genuinely pluginless operative
        # vs a cspec read that returned nothing (Intent.declared/0 falls back
        # to [] on any read failure, and the durable cspec can lag a running
        # operative's actual plugin set). Tearing down EVERY running plugin on
        # that basis caused a per-tick stop/re-subscribe storm. Reconcile
        # "extra" plugins only against a non-empty declaration.
        extra =
          if MapSet.size(declared_ids) == 0 do
            []
          else
            for id <- MapSet.to_list(running),
                not MapSet.member?(declared_ids, id),
                do: {ref, id, pid}
          end

        {missing_acc ++ missing, extra_acc ++ extra, unhealthy_acc ++ unhealthy}
      else
        {missing_acc, extra_acc, unhealthy_acc}
      end
    end)
  end

  @doc "True when the delta has no divergence to heal."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = d) do
    d.unbound_operatives == [] and d.misbound_operatives == [] and
      d.replace_operatives == [] and
      d.stalled_operatives == [] and d.missing_plugins == [] and d.extra_plugins == [] and
      d.unhealthy_plugins == [] and d.data_plane_wedge == nil
  end
end
