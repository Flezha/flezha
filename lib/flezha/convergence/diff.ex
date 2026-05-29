defmodule Flezha.Convergence.Diff do
  @moduledoc """
  One pure function — `compute/2` — turning declared `Intent` and probed
  reality into a structured `Delta` enumerating every invariant
  violation (design.md §2-§3). This is NOT a per-failure-mode dispatch:
  each `Delta` field corresponds to one of the seven invariants, and all
  catalogue rows reduce to populating these fields.

  Sessions 3-4 enrich the population logic (plugins, output advance,
  stream health, wake detection, resources). This session populates the
  binding fields (I1) and the stalled set (I3) so the pipeline is real
  while heals stay stubbed.
  """

  @type relay_ref :: {atom(), binary()}

  @type t :: %__MODULE__{
          unbound_relays: [relay_ref()],
          misbound_relays: [{relay_ref(), binary()}],
          missing_plugins: [{relay_ref(), binary(), map()}],
          extra_plugins: [{relay_ref(), binary(), pid()}],
          stalled_relays: [{relay_ref(), non_neg_integer()}],
          degraded_external: [{relay_ref(), term()}],
          wedged_channels: [{pid(), atom(), relay_ref()}],
          broken_streams: [{atom(), term()}],
          silent_streams: [{atom(), term()}],
          wake_event: boolean(),
          resource_violations: [{atom(), term(), term()}],
          observability_drift: [{atom(), atom(), atom()}]
        }

  defstruct unbound_relays: [],
            misbound_relays: [],
            missing_plugins: [],
            extra_plugins: [],
            stalled_relays: [],
            degraded_external: [],
            wedged_channels: [],
            broken_streams: [],
            silent_streams: [],
            wake_event: false,
            resource_violations: [],
            observability_drift: []

  @spec compute(Flezha.Convergence.Intent.t(), map()) :: t()
  def compute(%Flezha.Convergence.Intent{relays: declared}, reality) do
    active = Map.get(reality, :active_executors, %{})
    observed = Map.get(reality, :relays, %{})

    {unbound, misbound} =
      Enum.reduce(declared, {[], []}, fn %{namespace: ns, relay_id: rid}, {unb, mis} ->
        ref = {ns, rid}
        observed_relay = Map.get(observed, ref, %{})
        bound = Map.get(observed_relay, :bound_executor)
        live = Map.get(active, ns, MapSet.new())

        cond do
          is_nil(bound) -> {[ref | unb], mis}
          not MapSet.member?(live, bound) -> {unb, [{ref, bound} | mis]}
          true -> {unb, mis}
        end
      end)

    {missing, extra} = plugin_divergence(declared, observed)

    %__MODULE__{
      unbound_relays: Enum.reverse(unbound),
      misbound_relays: Enum.reverse(misbound),
      missing_plugins: missing,
      extra_plugins: extra,
      stalled_relays: stalled_relays(reality, observed),
      wedged_channels: Map.get(reality, :wedged_channels, [])
    }
  end

  # I3: resolve each stalled relay-instance id (emitted by the Watchdog,
  # keyed `relay_instance`) against the relays we observed running, so the
  # Planner rebinds the exact `{namespace, relay_id}` instance. A stalled
  # id with no live relay is dropped — there is nothing to rebind. Age is
  # `tick_at - stalled_since` when both are known.
  defp stalled_relays(reality, observed) do
    tick_at = Map.get(reality, :tick_at)
    index = Map.new(Map.keys(observed), fn {_ns, rid} = ref -> {rid, ref} end)

    reality
    |> Map.get(:stalled_relays, %{})
    |> normalize_stalled()
    |> Enum.flat_map(fn {relay_id, since} ->
      case Map.get(index, relay_id) do
        nil -> []
        ref -> [{ref, stall_age(tick_at, since)}]
      end
    end)
  end

  defp normalize_stalled(map) when is_map(map) and not is_struct(map), do: Map.to_list(map)
  defp normalize_stalled(set) when is_struct(set, MapSet), do: Enum.map(set, &{&1, nil})
  defp normalize_stalled(list) when is_list(list), do: Enum.map(list, &{&1, nil})

  defp stall_age(tick_at, since) when is_integer(tick_at) and is_integer(since),
    do: max(tick_at - since, 0)

  defp stall_age(_tick_at, _since), do: 0

  # I2: a relay's declared plugin set (durable cspec) vs the set actually
  # subscribed in its running extension table. Only diffed for relays we
  # observed with a live pid — a plugin can only be (un)subscribed on a
  # running relay; unbound relays are healed by binding first, which
  # replays the full cspec plugin list.
  defp plugin_divergence(declared, observed) do
    Enum.reduce(declared, {[], []}, fn %{namespace: ns, relay_id: rid, plugins: plugins},
                                       {missing_acc, extra_acc} ->
      ref = {ns, rid}
      observed_relay = Map.get(observed, ref, %{})
      pid = Map.get(observed_relay, :pid)

      if is_pid(pid) do
        running = MapSet.new(Map.get(observed_relay, :running_plugins, []))
        declared_ids = MapSet.new(plugins, fn {id, _plugin} -> id end)

        missing =
          for {id, plugin} <- plugins, not MapSet.member?(running, id), do: {ref, id, plugin}

        extra =
          for id <- MapSet.to_list(running),
              not MapSet.member?(declared_ids, id),
              do: {ref, id, pid}

        {missing_acc ++ missing, extra_acc ++ extra}
      else
        {missing_acc, extra_acc}
      end
    end)
  end

  @doc "True when the delta has no divergence to heal."
  @spec empty?(t()) :: boolean()
  def empty?(%__MODULE__{} = d) do
    d.unbound_relays == [] and d.misbound_relays == [] and d.missing_plugins == [] and
      d.extra_plugins == [] and d.stalled_relays == [] and d.wedged_channels == [] and
      d.broken_streams == [] and d.silent_streams == [] and d.wake_event == false and
      d.resource_violations == [] and d.observability_drift == []
  end
end
