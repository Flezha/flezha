defmodule Flezha.Convergence.Planner do
  @moduledoc """
  Pure function `plan/3` turning a `Delta` into an ordered,
  backoff-filtered list of primitive ops. Each op is a map
  `%{primitive: atom, namespace: atom, target: term, args: map}`.

  Ordering rules: a relay is bound (`rebind_relay`) before its plugins
  start; an executor is evicted (`evict_executor`) before another binding
  claims its relays. Ops still inside their `Backoff` cooldown are dropped,
  and the per-tick op count is capped at `max_ops`.

  Adding a failure mode does NOT touch the Planner — it surfaces through
  an existing `Delta` field healed by one of the four primitives.
  """

  alias Flezha.Convergence.{Backoff, Diff}

  @type op :: %{primitive: atom(), namespace: atom() | nil, target: term(), args: map()}

  @default_max_ops 20

  @spec plan(Diff.t(), Backoff.t(), keyword()) :: [op()]
  def plan(%Diff{} = delta, %Backoff{} = backoff, opts \\ []) do
    max_ops = Keyword.get(opts, :max_ops, @default_max_ops)

    delta
    |> ordered_ops()
    |> Backoff.filter(backoff, &key/1)
    |> Enum.take(max_ops)
  end

  @doc "Backoff key for an op: `{primitive, target}`."
  @spec key(op()) :: Backoff.key()
  def key(%{primitive: primitive, target: target}), do: {primitive, target}

  # Prerequisite order: evictions, then (re)binds, then plugin starts.
  # Each group preserves Delta order within it.
  defp ordered_ops(%Diff{} = d) do
    eviction_ops(d) ++
      bind_ops(d) ++
      plugin_ops(d)
  end

  defp eviction_ops(%Diff{misbound_relays: misbound}) do
    misbound
    |> Enum.map(fn {_ref, dead_executor} -> dead_executor end)
    |> Enum.uniq()
    |> Enum.map(&op(:evict_executor, nil, &1, %{}))
  end

  defp bind_ops(%Diff{} = d) do
    rebind =
      Enum.map(d.misbound_relays, fn {{ns, rid}, _dead} ->
        op(:rebind_relay, ns, rid, %{})
      end)

    # Unbound (incl. parked) relays bind via the same rebind primitive: it
    # resolves an executor and sends `:executor_migrate`, which un-parks them.
    start =
      Enum.map(d.unbound_relays, fn {ns, rid} ->
        op(:rebind_relay, ns, rid, %{})
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
