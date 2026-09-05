defmodule Flezha.Convergence.Facts do
  @moduledoc """
  The one intake of the convergence loop, and the table of what can be
  declared through it.

  A site that observes something — a runner closed its session, a bind
  parked, a compaction gave up, a seq gap opened, a subscriber crashed, a
  registry row was written — calls `declare/1` and decides nothing. The loop
  holds every declared fact in one map keyed `{kind, target}` with the time of
  its last declaration; the Diff consumes each kind through exactly one field
  and the Planner chooses the remedy. A fact leaves the map when its
  counter-fact arrives, when reality shows it repaired, or when its grace
  passes.

  This module is deliberately a leaf: it names no other module, so a site
  depends on an intake that names no remedy, and the loop that consumes the
  facts stays outside the dependency cycle every site sits in.
  """

  alias Flezha.Config.Durations

  # The loop's registered name, spelled as the atom it is: writing the alias
  # would be a compile-time edge back into the loop, and an intake that
  # names the loop drags every declaring site into the loop's cycle —
  # which is the one thing this module exists not to do.
  @loop :"Elixir.Flezha.Convergence.Loop"

  # A restarted plugin counts as repaired only once it has stayed `:running`
  # this long: a provider that reaches `:running` and crashes again a second
  # later keeps its rung, which is what stops a fast-failing upstream (a WS
  # that keeps 429-ing) from retrying at the base delay for ever.

  @typedoc "What a site declares: the kind, the target it is about, and its arguments."
  @type fact :: {kind :: atom(), target :: term(), args :: map()}

  @typedoc "The held facts: `{kind, target} => %{at: monotonic_ms, args: map}`."
  @type t :: %{optional({atom(), term()}) => %{at: integer(), args: map()}}

  # Every kind a site may declare. A holding kind is kept in the map; a transient
  # one only ticks the loop; a clearing kind drops the fact it counters.
  #
  #   diff     — the one `%Diff{}` field that consumes the kind
  #   reason   — the `args.reason` the planned op carries; its backoff key is
  #              `{reason, target}` (`nil`: the op keys on its primitive)
  #   reset    — `:inline` resets the key on the op's own `:ok`; `:evidence`
  #              records `:error` on every application and resets only when
  #              the repair is observed (a ladder)
  #   grace    — whether the fact ages out when nothing re-declares it
  #   tick     — `:now` ticks the loop on a new key, `:next` waits for the
  #              periodic tick
  @kinds %{
    output_stall: %{
      holds: true,
      diff: :stalled_operatives,
      reason: :output_stall,
      reset: :evidence,
      grace: true,
      tick: :now
    },
    runner_disconnected: %{
      holds: true,
      diff: :misbound_operatives,
      reason: :runner_disconnected,
      reset: :inline,
      grace: true,
      tick: :now
    },
    operative_parked: %{
      holds: true,
      diff: :parked_operatives,
      reason: :parked,
      reset: :evidence,
      grace: false,
      tick: :now
    },
    compaction_gave_up: %{
      holds: true,
      diff: :resync_entities,
      reason: :compaction_gave_up,
      reset: :evidence,
      grace: false,
      tick: :now,
      cleared_by: :compaction_completed
    },
    seq_gap: %{
      holds: true,
      diff: :resync_entities,
      reason: :seq_gap,
      reset: :evidence,
      grace: false,
      tick: :now,
      cleared_by: :seq_gap_closed
    },
    plugin_down: %{
      holds: true,
      diff: :unhealthy_plugins,
      reason: nil,
      primitive: :restart_plugin,
      reset: :evidence,
      grace: true,
      tick: :now
    },
    registry_written: %{
      holds: true,
      diff: :stale_tags,
      reason: nil,
      reset: :inline,
      grace: true,
      tick: :next
    },
    node_draining: %{
      holds: true,
      diff: :draining,
      reason: nil,
      reset: :inline,
      grace: false,
      tick: :now,
      cleared_by: :node_drained
    },
    # The data-plane classifier's verdict: a signal the loop computes itself,
    # listed here so its ladder is declared beside the others.
    data_plane_wedge: %{
      holds: false,
      diff: :data_plane_wedge,
      reason: :data_plane_wedge,
      reset: :evidence,
      grace: false,
      tick: :next
    },
    runner_connected: %{holds: false, tick: :now},
    reconcile_started: %{holds: false, tick: :now},
    seq_gap_closed: %{clears: :seq_gap},
    compaction_completed: %{clears: :compaction_gave_up},
    node_drained: %{clears: :node_draining}
  }

  @doc """
  Tell the loop a fact. A `send` to the loop's registered name; a controller
  running with the loop off (the test default) drops it.
  """
  @spec declare(fact()) :: :ok
  def declare({kind, target, args}) when is_map_key(@kinds, kind) and is_map(args) do
    case Process.whereis(@loop) do
      nil -> :ok
      pid -> send(pid, {:declare, kind, target, args})
    end

    :ok
  end

  @doc "The kinds table."
  @spec kinds() :: %{atom() => map()}
  def kinds(), do: @kinds

  @doc "How long a restarted plugin must stay `:running` before its key resets."
  @spec stable_uptime_ms() :: pos_integer()
  def stable_uptime_ms(), do: Durations.get(:plugin_stable_uptime_ms)

  @doc "The kinds the loop holds in its map, with their entries."
  @spec holding() :: %{atom() => map()}
  def holding(), do: Map.filter(@kinds, fn {_, e} -> Map.get(e, :holds, false) end)

  @doc "Whether an op carrying `reason` is a ladder rung: its key resets on evidence only."
  @spec ladder_reason?(atom()) :: boolean()
  def ladder_reason?(reason) when is_atom(reason) do
    Enum.any?(@kinds, fn {_, e} -> e[:reason] == reason and e[:reset] == :evidence end)
  end

  @doc "Whether `primitive` resets on evidence whatever the op's reason."
  @spec ladder_primitive?(atom()) :: boolean()
  def ladder_primitive?(primitive) when is_atom(primitive) do
    Enum.any?(@kinds, fn {_, e} -> e[:primitive] == primitive and e[:reset] == :evidence end)
  end

  @doc """
  Apply one declaration to the held map. Answers the map and what the
  declaration was: `:new` (a key the loop did not hold — tick at once when the
  kind says so), `:known` (refreshed, no tick), `:cleared` (a counter-fact
  dropped its fact), `:transient` (never held; ticks).
  """
  @spec put(t(), atom(), term(), map(), integer()) :: {t(), :new | :known | :cleared | :transient}
  def put(facts, kind, target, args, now) do
    case Map.fetch!(@kinds, kind) do
      %{clears: cleared_kind} ->
        {Map.delete(facts, {cleared_kind, target}), :cleared}

      %{holds: false} ->
        {facts, :transient}

      _ ->
        key = {kind, target}
        known? = Map.has_key?(facts, key)
        {Map.put(facts, key, %{at: now, args: args}), if(known?, do: :known, else: :new)}
    end
  end

  @doc "Whether a declaration answered `what` by `put/5` should tick the loop at once."
  @spec ticks?(atom(), :new | :known | :cleared | :transient) :: boolean()
  def ticks?(_kind, :known), do: false
  def ticks?(_kind, :cleared), do: false
  def ticks?(kind, _what), do: Map.get(Map.fetch!(@kinds, kind), :tick) == :now

  @doc "The targets held under `kind`."
  @spec targets(t(), atom()) :: [term()]
  def targets(facts, kind), do: for({{^kind, target}, _} <- facts, do: target)

  @doc "The args held for `{kind, target}`, or `nil`."
  @spec args(t(), atom(), term()) :: map() | nil
  def args(facts, kind, target) do
    case Map.get(facts, {kind, target}) do
      %{args: args} -> args
      nil -> nil
    end
  end

  @doc """
  The backoff key a held fact's remedy walks, or `nil` for a fact whose remedy
  resets inline and so has no ladder to release.

  It must answer exactly what `Planner.key/1` builds for the op that fact
  plans, or a repaired target would release a key nothing walked. Two kinds
  therefore spell it out rather than take the default: a park is held per
  `{namespace, operative}` while its rebind targets the operative, and a
  crashed plugin is held per `{namespace, operative, plugin}` while its restart
  keys per `{operative, plugin}`.
  """
  @spec key({atom(), term()}) :: {atom(), term()} | nil
  def key({:operative_parked, {_ns, id}}), do: {:parked, id}
  def key({:plugin_down, {_ns, id, plugin_id}}), do: {:restart_plugin, {id, plugin_id}}

  def key({kind, target}) do
    case Map.get(@kinds, kind) do
      %{reason: reason, reset: :evidence} when is_atom(reason) and not is_nil(reason) ->
        {reason, target}

      _ ->
        nil
    end
  end

  @doc """
  The backoff key a counter-fact releases: the key of the fact it clears, so
  the evidence that ends an episode also ends the ladder that episode walked.
  """
  @spec cleared_key(atom(), term()) :: {atom(), term()} | nil
  def cleared_key(kind, target) do
    case Map.get(@kinds, kind) do
      %{clears: cleared_kind} -> key({cleared_kind, target})
      _ -> nil
    end
  end

  @doc """
  Reconcile the held facts with observed reality: drop the ones whose repair
  the probe shows, the ones nothing still names, and the ones past their grace;
  hold a park the probe observes but nobody declared, so the loop's map is the
  whole parked set. Answers the map and the keys of the dropped facts, which
  the caller resets.
  """
  @spec settle(t(), map(), pos_integer(), integer()) :: {t(), [{atom(), term()}]}
  def settle(facts, reality, grace_ms, now) do
    observed = Map.get(reality, :operatives, %{})

    {kept, dropped} =
      Enum.split_with(facts, fn {{kind, target}, %{at: at, args: args}} ->
        entry = Map.fetch!(@kinds, kind)
        not (repaired?(kind, target, args, reality) or aged_out?(entry, at, grace_ms, now))
      end)

    facts =
      Enum.reduce(observed, Map.new(kept), fn
        {ref, %{park_reason: reason}}, acc when not is_nil(reason) ->
          Map.put_new(acc, {:operative_parked, ref}, %{at: now, args: %{reason: reason}})

        _, acc ->
          acc
      end)

    {facts, dropped |> Enum.map(fn {key, _} -> key(key) end) |> Enum.reject(&is_nil/1)}
  end

  defp aged_out?(%{grace: true}, at, grace_ms, now), do: now - at >= grace_ms
  defp aged_out?(_entry, _at, _grace_ms, _now), do: false

  # A disconnected runner is repaired once it is back in the active set, or once
  # no operative still names it. An unreadable set (`:unknown`) is evidence of
  # nothing.
  defp repaired?(:runner_disconnected, {ns, runner_id}, _args, reality) do
    case get_in(reality, [:active_runners, ns]) do
      :unknown -> false
      nil -> nobody_names?(reality, runner_id)
      live -> MapSet.member?(live, runner_id) or nobody_names?(reality, runner_id)
    end
  end

  # A park is repaired once the probe sees the operative unparked, and gone
  # once the probe sees no operative at all.
  defp repaired?(:operative_parked, ref, _args, reality) do
    case get_in(reality, [:operatives, ref]) do
      %{bound_runner: :unknown} -> false
      %{park_reason: reason} -> is_nil(reason)
      _ -> true
    end
  end

  # A crashed subscriber is repaired once the probe sees it running AND up for
  # the stable window — the fact and the rung its restart walks clear together,
  # so a plugin that reaches `:running` and crashes again keeps both.
  defp repaired?(:plugin_down, {ns, id, plugin_id}, _args, reality) do
    case get_in(reality, [:operatives, {ns, id}]) do
      %{bound_runner: :unknown} ->
        false

      %{plugin_uptimes: uptimes} ->
        Map.get(uptimes, plugin_id, -1) >= Durations.get(:plugin_stable_uptime_ms)

      _ ->
        true
    end
  end

  # A stale tag is repaired once the row names the bound runner.
  defp repaired?(:registry_written, ref, _args, reality) do
    case get_in(reality, [:operatives, ref]) do
      %{bound_runner: :unknown} -> false
      %{bound_runner: bound, registry_tag: tag} -> is_nil(bound) or tag == bound
      _ -> true
    end
  end

  # An Entity fact is cleared by its counter-fact; a writer that is gone has no
  # gap left to close.
  defp repaired?(kind, entity_id, _args, reality) when kind in [:seq_gap, :compaction_gave_up] do
    case Map.get(reality, :writers, :unknown) do
      :unknown -> false
      writers -> not MapSet.member?(writers, entity_id)
    end
  end

  defp repaired?(_kind, _target, _args, _reality), do: false

  defp nobody_names?(reality, runner_id) do
    reality
    |> Map.get(:operatives, %{})
    |> Enum.all?(fn {_ref, observed} -> Map.get(observed, :bound_runner) != runner_id end)
  end
end
