defmodule Flezha.Convergence.Planner do
  @moduledoc """
  Pure function `plan/3` turning a `Delta` into an ordered,
  backoff-filtered list of primitive ops. Each op is a map
  `%{primitive: atom, namespace: atom, target: term, args: map}`.

  Ordering rules: a runner is evicted (`evict_runner`) and a dead channel cut
  (`reset_control_channel`) before another binding claims their operatives; a
  operative is bound (`rebind_operative`) before its plugins start; a row is
  re-published (`publish_binding`) after every move. Ops still inside their
  `Backoff` cooldown are dropped, and the per-tick op count is capped at
  `max_ops`.

  This is the ONE place a divergence becomes an op. Adding a failure mode does
  NOT touch the Planner — it surfaces through an existing `Delta` field healed
  by one of the existing primitives — and adding a remedy is one primitive
  entry plus the one rule in `rules/0` that plans it.
  """

  alias Flezha.Convergence.{Backoff, Diff, Facts}

  @type op :: %{primitive: atom(), namespace: atom() | nil, target: term(), args: map()}

  @default_max_ops 20

  # The `args.reason` that marks an op as a rung of the output-stall ladder.
  @ladder_reason :output_stall
  # ...and the reason stamped on an op the data-plane classifier drove.
  @wedge_reason :data_plane_wedge

  # Reasons whose evidence is "this target is demonstrably not producing", as
  # opposed to a structural divergence (misbound, unbound, plugin drift).
  @proved_not_producing [@ladder_reason, @wedge_reason]

  # The plugin primitives key on `{primitive, {operative_id, plugin_id}}`: one
  # crashing plugin must never throttle its siblings' starts.
  @plugin_primitives [:start_plugin, :stop_plugin, :restart_plugin]

  # A parked operative whose bind's ping merely timed out is a busy runner:
  # its channel is live and carries every other instance on it, so the
  # channel is cut only once the parked ladder has walked this many rungs.
  @busy_runner_cut_at 2

  # Every `%Diff{}` field a rule reads, and the primitives that rule can plan.
  # The seam test holds this table to the Diff, the primitive table and the
  # declared kinds: a kind nobody plans, a primitive no rule names, or a field
  # planned twice fails there by name.
  @rules [
    misbound_operatives: [:evict_runner, :rebind_operative],
    replace_operatives: [:evict_runner, :replace_operative],
    parked_operatives: [:reset_control_channel, :rebind_operative],
    data_plane_wedge: [:restart_runner, :evict_runner, :resync_runner, :rebind_operative],
    unbound_operatives: [:rebind_operative],
    resync_entities: [:resync_runner],
    missing_plugins: [:start_plugin],
    extra_plugins: [:stop_plugin],
    unhealthy_plugins: [:restart_plugin],
    stale_tags: [:publish_binding],
    stalled_operatives: [:resync_runner, :rebind_operative]
  ]

  @spec plan(Diff.t(), Backoff.t(), keyword()) :: [op()]
  def plan(%Diff{} = delta, %Backoff{} = backoff, opts \\ []) do
    max_ops = Keyword.get(opts, :max_ops, @default_max_ops)

    planned =
      delta
      |> ordered_ops(backoff)
      |> Backoff.filter(backoff, &key/1)
      |> Enum.take(max_ops)

    # The ladder is appended past `max_ops` on purpose: a operative that has
    # stopped producing must not be starved by a large structural divergence
    # filling the tick's budget.
    planned ++ stall_ops(delta, backoff, planned)
  end

  @doc "The per-tick op cap when the caller names none."
  @spec max_ops() :: pos_integer()
  def max_ops(), do: @default_max_ops

  @doc "The Diff fields a rule reads, in prerequisite order, with the primitives it plans."
  @spec rules() :: [{atom(), [atom()]}]
  def rules(), do: @rules

  @doc """
  Backoff key for an op: `{primitive, target}`; for a plugin op
  `{primitive, {operative_id, plugin_id}}`; for an op carrying a declared
  reason `{reason, target}`, so every rung of that reason's ladder walks ONE
  cooldown instead of restarting the throttle under a fresh key — which is
  also what makes the attempt count the rung.
  """
  @spec key(op()) :: Backoff.key()
  def key(%{primitive: primitive, target: target, args: %{plugin_id: plugin_id}})
      when primitive in @plugin_primitives,
      do: {primitive, {target, plugin_id}}

  def key(%{target: target, args: %{reason: reason}}) when is_atom(reason),
    do: ladder_key(reason, target)

  def key(%{primitive: primitive, target: target}), do: {primitive, target}

  @doc "The one cooldown key every rung of `target`'s `reason` ladder shares."
  @spec ladder_key(atom(), term()) :: Backoff.key()
  def ladder_key(reason, target), do: {reason, target}

  @doc "The output-stall ladder's cooldown key for `target`."
  @spec ladder_key(term()) :: Backoff.key()
  def ladder_key(target), do: ladder_key(@ladder_reason, target)

  @doc """
  Whether an op is a ladder rung: its `:ok` means the remedy was APPLIED, not
  that the fault cleared, so the Loop advances the cooldown on either outcome
  and resets it only on the declared evidence. Read from the declared kinds —
  the op's reason, or its primitive — never from a case here.
  """
  @spec ladder?(op()) :: boolean()
  def ladder?(%{args: %{reason: reason}}) when is_atom(reason), do: Facts.ladder_reason?(reason)
  def ladder?(%{primitive: primitive}), do: Facts.ladder_primitive?(primitive)

  @doc """
  Whether this op's reason PROVES its target is not producing.

  `Heal` reads it to waive the guards that exist to spare a *healthy* target —
  the active-runner rebind check and the "never evict a live runner" rule.
  Neither an output-stalled operative nor a classifier-wedged runner is
  healthy, and in the wedge case the target still being connected is exactly
  what stops anything else from dislodging it.
  """
  @spec proved_wedged?(op()) :: boolean()
  def proved_wedged?(%{args: %{reason: reason}}), do: reason in @proved_not_producing
  def proved_wedged?(_op), do: false

  # One op per stalled operative the plan does not already cover, laddered:
  # resync the existing binding first (cheap, no migration), migrate only once
  # that failed to restore production. The rung is read off the ladder's own
  # cooldown attempt count — the counter that already throttles it — so there
  # is no second piece of per-operative bookkeeping.
  defp stall_ops(%Diff{stalled_operatives: []}, _backoff, _planned), do: []

  defp stall_ops(%Diff{stalled_operatives: stalled}, backoff, planned) do
    covered = MapSet.new(planned, & &1.target)

    stalled
    |> Enum.reject(fn {_ns, rid} -> MapSet.member?(covered, rid) end)
    |> Enum.map(fn {ns, rid} ->
      op(rung(backoff, @ladder_reason, rid, :resync_runner, :rebind_operative), ns, rid, %{
        reason: @ladder_reason
      })
    end)
    |> Backoff.filter(backoff, &key/1)
  end

  # Which rung of `reason`'s ladder `target` is on, read off the ladder's own
  # cooldown attempt count — the counter that already throttles it, so there is
  # no second piece of per-target bookkeeping. `first` until the cooldown has
  # recorded an attempt, `next` after.
  defp rung(backoff, reason, target, first, next) do
    case Backoff.attempt(backoff, ladder_key(reason, target)) do
      0 -> first
      _ -> next
    end
  end

  # Prerequisite order: evictions and channel resets, then the data-plane
  # wedge (at most one op, of either kind), then (re)binds, re-placements,
  # Entity resyncs, plugin starts, and the row re-publish last. Each group
  # preserves Delta order within it.
  defp ordered_ops(%Diff{} = d, backoff) do
    eviction_ops(d) ++
      reset_ops(d, backoff) ++
      wedge_ops(d, backoff) ++
      bind_ops(d) ++
      replace_ops(d) ++
      resync_ops(d) ++
      plugin_ops(d) ++
      publish_ops(d)
  end

  # The tick's classifier verdict, already resolved to the entity that can be
  # healed. Remedy by scope: a wedged runner is evicted (its operatives rebind
  # through the existing ordering), a wedged connection is resynced in place,
  # a wedged operative is rebound.
  defp wedge_ops(%Diff{data_plane_wedge: nil}, _backoff), do: []

  # A runner rides its own two-rung ladder. The terminal restart first, on the
  # session the verdict named — eviction would end that session, the same
  # process would reconnect under a fresh id no rung keyed on the session could
  # reach, and it would be placeable again; eviction only once the restart has
  # been applied and the classifier STILL proves the same session wedged, which
  # is a process that did not exit.
  defp wedge_ops(
         %Diff{data_plane_wedge: {:runner, ns, runner_id}, data_plane_layer: layer},
         backoff
       ) do
    [
      op(
        rung(backoff, @wedge_reason, runner_id, :restart_runner, :evict_runner),
        ns,
        runner_id,
        wedge_args(layer)
      )
    ]
  end

  defp wedge_ops(%Diff{data_plane_wedge: {:connection, refs}, data_plane_layer: layer}, _backoff) do
    Enum.map(refs, fn {ns, rid} -> op(:resync_runner, ns, rid, wedge_args(layer)) end)
  end

  defp wedge_ops(%Diff{data_plane_wedge: {:operative, refs}, data_plane_layer: layer}, _backoff) do
    Enum.map(refs, fn {ns, rid} -> op(:rebind_operative, ns, rid, wedge_args(layer)) end)
  end

  defp wedge_args(layer), do: %{reason: @wedge_reason, layer: layer}

  defp eviction_ops(%Diff{misbound_operatives: misbound, replace_operatives: replace}) do
    dead_from_replace = for {_ref, dead} <- replace, is_binary(dead), do: dead

    misbound
    |> Enum.map(fn {_ref, dead_runner, _reason} -> dead_runner end)
    |> Enum.concat(dead_from_replace)
    |> Enum.uniq()
    |> Enum.map(&op(:evict_runner, nil, &1, %{}))
  end

  # A parked operative's bind reported its runner's transport dead: cut the
  # channel before anything is placed on it. A ping that merely timed out says
  # the runner is busy — its channel is live and carries every other instance
  # on it — so it is retried, and cut only once the parked ladder has walked
  # `@busy_runner_cut_at` rungs. The verdict rides the fact: the bind that
  # failed is what knows what its own failure meant.
  defp reset_ops(%Diff{parked_operatives: parked}, backoff) do
    parked
    |> Enum.filter(fn {{_ns, id}, args, runner} ->
      is_binary(runner) and
        cut_channel?(args[:transport], Backoff.attempt(backoff, {:parked, id}))
    end)
    |> Enum.map(fn {{ns, id}, _args, runner} ->
      op(:reset_control_channel, ns, runner, %{operative_id: id})
    end)
    |> Enum.uniq_by(& &1.target)
  end

  defp cut_channel?(:dead, _attempt), do: true
  defp cut_channel?(:busy, attempt), do: attempt >= @busy_runner_cut_at
  defp cut_channel?(_unclassified, _attempt), do: false

  # An operative whose factory only another node's fleet serves: the local
  # bind is doomed, so the op moves the operative through the existing
  # stop/start handover with state transfer. Its own backoff key bounds churn.
  defp replace_ops(%Diff{replace_operatives: replace}) do
    Enum.map(replace, fn {{ns, rid}, _dead} -> op(:replace_operative, ns, rid, %{}) end)
  end

  defp bind_ops(%Diff{} = d) do
    rebind =
      Enum.map(d.misbound_operatives, fn {{ns, rid}, _dead, reason} ->
        op(:rebind_operative, ns, rid, reasoned(reason))
      end)

    # A parked operative binds via the same rebind primitive on the
    # `{:parked, id}` ladder: it resolves a runner and sends `:runner_migrate`,
    # which un-parks it; the outcome is only visible later, when the probe sees
    # it bound.
    unpark =
      Enum.map(d.parked_operatives, fn {{ns, rid}, args, _runner} ->
        op(:rebind_operative, ns, rid, %{reason: :parked, park: args[:reason]})
      end)

    # Unbound operatives bind via the same rebind primitive too.
    start =
      Enum.map(d.unbound_operatives, fn {ns, rid} ->
        op(:rebind_operative, ns, rid, %{})
      end)

    rebind ++ unpark ++ start
  end

  # A probe-observed misbound keys on its primitive; a declared disconnect keys
  # on the fact, so the streak gate and the key both read the same reason.
  defp reasoned(:probe), do: %{}
  defp reasoned(reason), do: %{reason: reason}

  # An Entity resync against the runner the fact named, keyed on the fact's
  # reason and the Entity — the same primitive the output-stall ladder starts
  # with, aimed one level down.
  defp resync_ops(%Diff{resync_entities: entities}) do
    Enum.map(entities, fn {entity_id, runner_id, reason} ->
      op(:resync_runner, nil, entity_id, %{
        reason: reason,
        entity_id: entity_id,
        runner_id: runner_id
      })
    end)
  end

  defp plugin_ops(%Diff{} = d) do
    missing =
      Enum.map(d.missing_plugins, fn {{ns, rid}, plugin_id, plugin} ->
        op(:start_plugin, ns, rid, %{plugin_id: plugin_id, plugin: plugin})
      end)

    extra =
      Enum.map(d.extra_plugins, fn {{ns, rid}, plugin_id, _pid} ->
        op(:stop_plugin, ns, rid, %{plugin_id: plugin_id})
      end)

    unhealthy =
      Enum.map(d.unhealthy_plugins, fn {{ns, rid}, plugin_id, plugin, verdict} ->
        op(:restart_plugin, ns, rid, %{plugin_id: plugin_id, plugin: plugin, verdict: verdict})
      end)

    missing ++ extra ++ unhealthy
  end

  defp publish_ops(%Diff{stale_tags: stale}) do
    Enum.map(stale, fn {{ns, rid}, tag, bound} ->
      op(:publish_binding, ns, rid, %{tag: tag, bound: bound})
    end)
  end

  defp op(primitive, namespace, target, args) do
    %{primitive: primitive, namespace: namespace, target: target, args: args}
  end
end
