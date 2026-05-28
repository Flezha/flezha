defmodule Flezha.Convergence.Planner do
  @moduledoc """
  Pure function `plan/3` turning a `Delta` into an ordered,
  backoff-filtered list of primitive ops (design.md §5). Each op is a
  map `%{primitive: atom, namespace: atom, target: term, args: map}`.

  Ordering rules: a relay is bound (`rebind_relay`/`start_relay`) before
  its plugins start; an executor is evicted (`evict_executor`) before
  another binding claims its relays; a `wake_event` triggers
  `reconnect_stream` first. Ops still inside their `Backoff` cooldown are
  dropped, and the per-tick op count is capped at `max_ops`.

  Adding a failure mode does NOT touch the Planner — it surfaces through
  an existing `Delta` field healed by one of the ten primitives.
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

  # Prerequisite order: wake reconnects, then wedged-channel kills, then
  # evictions, then (re)binds, then plugin starts. Each group preserves
  # Delta order within it.
  defp ordered_ops(%Diff{} = d) do
    wake_ops(d) ++
      wedge_ops(d) ++
      eviction_ops(d) ++
      bind_ops(d) ++
      plugin_ops(d)
  end

  defp wake_ops(%Diff{wake_event: true}) do
    [op(:reconnect_stream, nil, :all, %{reason: :wake_event})]
  end

  defp wake_ops(%Diff{}), do: []

  # A wedged channel is healed by reconnecting its stream (primitive 8):
  # Heal kills the pid; its supervisor restarts it and the StreamAcceptor
  # re-binds the fresh channel.
  defp wedge_ops(%Diff{wedged_channels: wedged}) do
    Enum.map(wedged, fn {pid, kind, _relay} ->
      op(:reconnect_stream, nil, pid, %{kind: kind})
    end)
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

    start =
      Enum.map(d.unbound_relays, fn {ns, rid} ->
        op(:start_relay, ns, rid, %{})
      end)

    stall_rebind =
      Enum.map(d.stalled_relays, fn {{ns, rid}, age} ->
        op(:rebind_relay, ns, rid, %{reason: :output_stall, output_age_ms: age})
      end)

    rebind ++ start ++ stall_rebind
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

    missing ++ extra
  end

  defp op(primitive, namespace, target, args) do
    %{primitive: primitive, namespace: namespace, target: target, args: args}
  end
end
