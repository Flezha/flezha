defmodule Flezha.Convergence.Diff do
  @moduledoc """
  One pure function — `compute/2` — turning declared `Intent` and probed
  reality into a structured `Delta` enumerating every invariant violation.
  This is NOT a per-failure-mode dispatch: each `Delta` field corresponds
  to one invariant, and all catalogue rows reduce to populating these
  fields.
  """

  @type operative_ref :: {atom(), binary()}

  @type t :: %__MODULE__{
          unbound_operatives: [operative_ref()],
          misbound_operatives: [{operative_ref(), binary()}],
          missing_plugins: [{operative_ref(), binary(), map()}],
          extra_plugins: [{operative_ref(), binary(), pid()}],
          unhealthy_plugins: [{operative_ref(), binary(), map(), term()}]
        }

  defstruct unbound_operatives: [],
            misbound_operatives: [],
            missing_plugins: [],
            extra_plugins: [],
            unhealthy_plugins: []

  @spec compute(Flezha.Convergence.Intent.t(), map()) :: t()
  def compute(%Flezha.Convergence.Intent{operatives: declared}, reality) do
    active = Map.get(reality, :active_executors, %{})
    observed = Map.get(reality, :operatives, %{})

    {unbound, misbound} =
      Enum.reduce(declared, {[], []}, fn %{namespace: ns, operative_id: rid}, {unb, mis} ->
        ref = {ns, rid}
        observed_operative = Map.get(observed, ref, %{})
        bound = Map.get(observed_operative, :bound_executor)
        live = Map.get(active, ns, MapSet.new())

        cond do
          # `:unknown` = an observation failed this tick — either the operative's
          # own state read (`bound`) or the namespace's live-executor read
          # (`live`, Probe could not reach FleetManager). With no authoritative
          # signal we must NOT classify it: acting on an empty/failed read
          # mass-migrates healthy operatives on a transient read race.
          bound == :unknown -> {unb, mis}
          is_nil(bound) -> {[ref | unb], mis}
          live == :unknown -> {unb, mis}
          not MapSet.member?(live, bound) -> {unb, [{ref, bound} | mis]}
          true -> {unb, mis}
        end
      end)

    {missing, extra, unhealthy} = plugin_divergence(declared, observed)

    %__MODULE__{
      unbound_operatives: Enum.reverse(unbound),
      misbound_operatives: Enum.reverse(misbound),
      missing_plugins: missing,
      extra_plugins: extra,
      unhealthy_plugins: unhealthy
    }
  end

  # I2: a operative's declared plugin set (durable cspec) vs the set actually
  # subscribed in its running extension table, plus each subscribed declared
  # plugin's provider self-reported health. Only diffed for operatives we
  # observed with a live pid — a plugin can only be (un)subscribed on a
  # running operative; unbound operatives are healed by binding first, which
  # replays the full cspec plugin list.
  defp plugin_divergence(declared, observed) do
    Enum.reduce(declared, {[], [], []}, fn %{namespace: ns, operative_id: rid, plugins: plugins},
                                           {missing_acc, extra_acc, unhealthy_acc} ->
      ref = {ns, rid}
      observed_operative = Map.get(observed, ref, %{})
      pid = Map.get(observed_operative, :pid)

      if is_pid(pid) do
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
      d.missing_plugins == [] and d.extra_plugins == [] and d.unhealthy_plugins == []
  end
end
