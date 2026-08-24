defmodule Flezha.Convergence.Planner do
  @moduledoc """
  Pure function `plan/3` turning a `Delta` into an ordered,
  backoff-filtered list of primitive ops. Each op is a map
  `%{primitive: atom, namespace: atom, target: term, args: map}`.

  Ordering rules: a operative is bound (`rebind_operative`) before its plugins
  start; a runner is evicted (`evict_runner`) before another binding
  claims its operatives. Ops still inside their `Backoff` cooldown are dropped,
  and the per-tick op count is capped at `max_ops`.

  This is the ONE place a divergence becomes an op. Adding a failure mode does
  NOT touch the Planner — it surfaces through an existing `Delta` field healed
  by one of the existing primitives.
  """

  alias Flezha.Convergence.{Backoff, Diff}

  @type op :: %{primitive: atom(), namespace: atom() | nil, target: term(), args: map()}

  @default_max_ops 20

  # The `args.reason` that marks an op as a rung of the output-stall ladder.
  @ladder_reason :output_stall
  # ...and the reason stamped on an op the data-plane classifier drove.
  @wedge_reason :data_plane_wedge

  # Reasons whose evidence is "this target is demonstrably not producing", as
  # opposed to a structural divergence (misbound, unbound, plugin drift).
  @proved_not_producing [@ladder_reason, @wedge_reason]

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

  @doc """
  Backoff key for an op: `{primitive, target}`.

  The two ladders are the exception: every rung of one keys on
  `ladder_key/2`, so escalating (`resync_runner` → `rebind_operative`;
  `evict_runner` → `restart_runner`) walks ONE cooldown instead of restarting
  the throttle under a fresh key — which is also what makes the attempt count
  the rung.
  """
  @spec key(op()) :: Backoff.key()
  def key(%{target: target, args: %{reason: reason}}) when reason in @proved_not_producing,
    do: ladder_key(reason, target)

  def key(%{primitive: primitive, target: target}), do: {primitive, target}

  @doc "The one cooldown key every rung of `target`'s `reason` ladder shares."
  @spec ladder_key(atom(), term()) :: Backoff.key()
  def ladder_key(reason, target), do: {reason, target}

  @doc "The output-stall ladder's cooldown key for `target`."
  @spec ladder_key(term()) :: Backoff.key()
  def ladder_key(target), do: ladder_key(@ladder_reason, target)

  @doc """
  Whether an op is a ladder rung. Its `:ok` means the remedy was APPLIED, not
  that the stall cleared, so the Loop advances the cooldown on either outcome
  — that is what reaches the next rung instead of re-firing this one. Both
  proved-not-producing ladders qualify: only fresh evidence on a later tick
  proves the target still needs healing.
  """
  @spec ladder?(op()) :: boolean()
  def ladder?(%{args: %{reason: reason}}), do: reason in @proved_not_producing
  def ladder?(_op), do: false

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

  # Prerequisite order: evictions, then (re)binds, then re-placements, then
  # plugin starts. Each group preserves Delta order within it. The data-plane
  # wedge sits between them because it is at most one op and may be either kind.
  defp ordered_ops(%Diff{} = d, backoff) do
    eviction_ops(d) ++
      wedge_ops(d, backoff) ++
      bind_ops(d) ++
      replace_ops(d) ++
      plugin_ops(d)
  end

  # The tick's classifier verdict, already resolved to the entity that can be
  # healed. Remedy by scope: a wedged runner is evicted (its operatives rebind
  # through the existing ordering), a wedged connection is resynced in place,
  # a wedged operative is rebound.
  defp wedge_ops(%Diff{data_plane_wedge: nil}, _backoff), do: []

  # A runner rides its own two-rung ladder. Eviction first — cheap, moves the
  # work, leaves the process alone; the terminal restart only once eviction has
  # been applied and the classifier STILL proves the same runner wedged, since
  # an evicted-but-connected runner is placeable again and would otherwise flap.
  defp wedge_ops(
         %Diff{data_plane_wedge: {:runner, ns, runner_id}, data_plane_layer: layer},
         backoff
       ) do
    [
      op(
        rung(backoff, @wedge_reason, runner_id, :evict_runner, :restart_runner),
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
    |> Enum.map(fn {_ref, dead_runner} -> dead_runner end)
    |> Enum.concat(dead_from_replace)
    |> Enum.uniq()
    |> Enum.map(&op(:evict_runner, nil, &1, %{}))
  end

  # An operative whose factory only another node's fleet serves: the local
  # bind is doomed, so the op moves the operative through the existing
  # stop/start handover with state transfer. Its own backoff key bounds churn.
  defp replace_ops(%Diff{replace_operatives: replace}) do
    Enum.map(replace, fn {{ns, rid}, _dead} -> op(:replace_operative, ns, rid, %{}) end)
  end

  defp bind_ops(%Diff{} = d) do
    rebind =
      Enum.map(d.misbound_operatives, fn {{ns, rid}, _dead} ->
        op(:rebind_operative, ns, rid, %{})
      end)

    # Unbound (incl. parked) operatives bind via the same rebind primitive: it
    # resolves a runner and sends `:runner_migrate`, which un-parks them.
    start =
      Enum.map(d.unbound_operatives, fn {ns, rid} ->
        op(:rebind_operative, ns, rid, %{})
      end)

    rebind ++ start
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
      Enum.map(d.unhealthy_plugins, fn {{ns, rid}, plugin_id, plugin, reason} ->
        op(:restart_plugin, ns, rid, %{plugin_id: plugin_id, plugin: plugin, reason: reason})
      end)

    missing ++ extra ++ unhealthy
  end

  defp op(primitive, namespace, target, args) do
    %{primitive: primitive, namespace: namespace, target: target, args: args}
  end
end
