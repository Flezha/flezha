defmodule Flezha.Convergence.Probe.Readers do
  @moduledoc """
  The fact readers `Flezha.Convergence.Probe.Table` declares — one function per
  detector entry.

  A reader takes the tick's context (`operatives` from the Intent, the probed
  `reality`, the loop's `signals` and `now`) and returns
  `[%Flezha.Convergence.Probe.Fact{}]`. It reads; it never decides. Every
  verdict word in this file's neighbourhood belongs to the judge, which is why
  none appears here.

  Named `Readers` rather than the design's `Probe.Facts`: `Flezha.Convergence.Facts`
  is the loop's declared-fact store, and the judge needs both in one scope.
  """

  alias Flezha.Convergence.Facts
  alias Flezha.Convergence.Probe.Fact
  alias Flezha.Convergence.StallClassifier

  @type context :: %{
          operatives: [map()],
          reality: map(),
          signals: map(),
          now: integer()
        }

  @doc """
  Which runner an operative is bound to, whether that runner is live, and
  whether the bounded call into the Overwatch answered at all. `observed?`
  false and `bound_runner: :unknown` are the two ways a `SafeCall` that timed
  out or found no process reaches the judge — the facts that must never be
  diffed as a divergence.
  """
  @spec binding(context()) :: [Fact.t()]
  def binding(ctx) do
    per_operative(ctx, fn ref, {ns, _rid}, observed ->
      obs = observed || %{}

      %{
        bound_runner: Map.get(obs, :bound_runner),
        factory_id: Map.get(obs, :factory_id),
        live_runners: live_runners(ctx, ns),
        registry_tag: Map.get(obs, :registry_tag),
        observed?: observed != nil
      }
      |> fact(ref)
    end)
  end

  @doc "Whether `Lifecycle.park/2` left a reason on the Overwatch, and which."
  @spec park(context()) :: [Fact.t()]
  def park(ctx) do
    per_operative(ctx, fn ref, key, observed ->
      fact(
        %{
          parked?: Map.get(observed || %{}, :parked?, false),
          park_reason: Map.get(observed || %{}, :park_reason),
          park_args: Facts.args(ctx.signals[:facts] || %{}, :operative_parked, key)
        },
        ref
      )
    end)
  end

  @doc "Whether the operative was evicted at a committed iteration boundary."
  @spec suspension(context()) :: [Fact.t()]
  def suspension(ctx) do
    per_operative(ctx, fn ref, _key, observed ->
      fact(%{suspended?: Map.get(observed || %{}, :suspended?, false)}, ref)
    end)
  end

  @doc "Whether a site declared this operative's output stalled."
  @spec output_stall(context()) :: [Fact.t()]
  def output_stall(ctx) do
    stalled = MapSet.new(Facts.targets(ctx.signals[:facts] || %{}, :output_stall))

    per_operative(ctx, fn ref, {_ns, rid}, _observed ->
      fact(%{output_stalled?: MapSet.member?(stalled, rid)}, ref)
    end)
  end

  @doc "The producing-liveness marker: fed but flat past the grace window, or not."
  @spec progress(context()) :: [Fact.t()]
  def progress(ctx) do
    stuck = ctx.signals[:stuck] || %{}

    per_operative(ctx, fn ref, {_ns, rid}, _observed ->
      fact(%{stuck: Map.get(stuck, rid)}, ref)
    end)
  end

  @doc """
  Per declared plugin: where the operative's extension table actually has it,
  and what its provider said. The provider's answer arrives unjudged — the
  `{:fail_open, reason}` of an uncertain probe reaches the judge as itself.
  """
  @spec plugin_health(context()) :: [Fact.t()]
  def plugin_health(ctx) do
    observed_map = Map.get(ctx.reality, :operatives, %{})

    Enum.flat_map(ctx.operatives, fn %{namespace: ns, operative_id: rid} = declared ->
      observed = Map.get(observed_map, {ns, rid})

      if plugin_readable?(observed),
        do: declared_plugin_facts({ns, rid}, declared, observed),
        else: []
    end)
  end

  defp declared_plugin_facts({ns, rid}, declared, observed) do
    running = MapSet.new(Map.get(observed, :running_plugins, []))
    subscribed = MapSet.new(Map.get(observed, :subscribed_plugins, []))
    down = Map.get(observed, :down_plugins, %{})
    health = Map.get(observed, :plugin_health, %{})

    for {plugin_id, _plugin} <- Map.get(declared, :plugins, []) do
      fact(
        %{
          running?: MapSet.member?(running, plugin_id),
          subscribed?: MapSet.member?(subscribed, plugin_id),
          down: Map.get(down, plugin_id),
          probe: Map.get(health, plugin_id, :unprobed)
        },
        {:plugin, {ns, rid, plugin_id}}
      )
    end
  end

  # A plugin set is readable only on an operative the probe actually reached
  # and that is neither suspended nor parked: a suspended instance released its
  # plugins with its runner, and there is nothing there to be unhealthy.
  defp plugin_readable?(nil), do: false

  defp plugin_readable?(observed) do
    Map.get(observed, :bound_runner) not in [nil, :unknown] and
      not Map.get(observed, :suspended?, false) and
      not Map.get(observed, :parked?, false)
  end

  @doc """
  Per runner: roster membership and the two silences side by side — the age of
  the last inbound control frame and of the last data-plane frame — so a live
  session over a dead data plane is two fields of one row rather than two
  detectors that disagree.
  """
  @spec session(context()) :: [Fact.t()]
  def session(ctx) do
    liveness = ctx.signals[:runner_facts] || %{}

    Enum.map(runner_roster(ctx), fn {runner_id, {namespace, roster}} ->
      fact(
        Map.merge(
          %{
            control_silent_ms: nil,
            data_silent_ms: nil,
            heartbeats_advancing?: nil,
            dispatches_advancing?: nil
          },
          Map.get(liveness, runner_id, %{})
        )
        |> Map.merge(%{roster: roster, namespace: namespace}),
        {:runner, runner_id}
      )
    end)
  end

  @doc """
  Every driver and channel the tick sampled — the population, so a healthy one
  is a row an operator can read rather than the absence of one. Read apart from
  the verdict: a classifier that could not answer leaves these standing and
  `unknown`, instead of reporting no data plane at all.
  """
  @spec data_plane_population(context()) :: [Fact.t()]
  def data_plane_population(ctx), do: base_subjects(ctx, ctx.signals[:liveness])

  @doc """
  The data-plane classifier's single deepest verdict, resolved to the subject
  kind its layer names. `:fabric` becomes a `driver` subject — the one kind the
  Diff plans nothing for — and every other scope becomes the runner, channel or
  operative the heal can reach.
  """
  @spec data_plane(context()) :: [Fact.t()]
  def data_plane(ctx) do
    case ctx.signals[:stall_verdict] do
      {layer, wedge_ctx} -> wedge_facts(ctx, layer, wedge_ctx)
      _ -> []
    end
  end

  defp base_subjects(_ctx, nil), do: []

  defp base_subjects(ctx, sample) do
    roster = runner_roster(ctx)
    liveness = ctx.signals[:runner_facts] || %{}

    drivers =
      for d <- Map.get(sample, :drivers, []),
          do: fact(%{wedged_layer: nil}, {:driver, d.transport})

    # The runner's control silence rides along with its membership: the judge
    # weighs the two together, and a channel row is the only place a runner no
    # roster holds is seen at all.
    channels =
      for c <- sample_channels(ctx) do
        {_ns, membership} = Map.fetch!(roster, c.runner_id)
        silent_ms = get_in(liveness, [c.runner_id, :control_silent_ms])

        fact(
          %{wedged_layer: nil, roster: membership, control_silent_ms: silent_ms},
          {:channel, {c.runner_id, c.name}}
        )
      end

    drivers ++ channels
  end

  # One verdict, one subject kind — the "exactly one consumer" rule expressed
  # as the subject. `:fabric` is a driver (read by `/healthz` alone), and every
  # other scope resolves to the runner, channel or operatives a heal can reach.
  defp wedge_facts(ctx, layer, wedge_ctx) do
    observed = %{wedged_layer: layer, wedge_ctx: wedge_ctx}
    scope = StallClassifier.layer_scope(layer)

    for subject <- wedge_subjects(ctx, scope, wedge_ctx), do: fact(observed, subject)
  end

  defp wedge_subjects(_ctx, :fabric, %{transport: transport}), do: [{:driver, transport}]

  defp wedge_subjects(_ctx, :runner, wedge_ctx) do
    case StallClassifier.runner_of(wedge_ctx) do
      nil -> []
      id -> [{:runner, id}]
    end
  end

  defp wedge_subjects(_ctx, :connection, wedge_ctx) do
    case StallClassifier.runner_of(wedge_ctx) do
      nil -> []
      id -> [{:channel, {id, Map.get(wedge_ctx, :channel) || Map.get(wedge_ctx, :stream_id)}}]
    end
  end

  # The layer names a bare operative id; the namespace comes from the probed
  # reality, which is the only place the pair exists.
  defp wedge_subjects(ctx, :operative, %{operative_id: id}) when is_binary(id) do
    for {{_ns, rid} = ref, _obs} <- Map.get(ctx.reality, :operatives, %{}),
        rid == id,
        do: {:operative, ref}
  end

  # The layer names the runner; the operatives it wedged are the ones bound to
  # it, resolved against the SAME probed reality the Diff resolves the same
  # verdict against.
  defp wedge_subjects(ctx, :operative, wedge_ctx) do
    for ref <- StallClassifier.operatives_of(Map.get(ctx.reality, :operatives, %{}), wedge_ctx),
        do: {:operative, ref}
  end

  # A layer declared scope-less names nothing to heal: the frame is already
  # gone, or the report is about the consumer downstream.
  defp wedge_subjects(_ctx, nil, _wedge_ctx), do: []

  # A layer that DOES declare a scope and whose context names no subject is a
  # verdict this tick could not resolve. Raising is what makes it `unknown`
  # rather than silence: dropping it would report the data plane healthy on a
  # verdict nobody could read.
  defp wedge_subjects(_ctx, scope, wedge_ctx) do
    raise ArgumentError,
          "data-plane verdict scoped #{inspect(scope)} names no subject: #{inspect(wedge_ctx)}"
  end

  # ---------- helpers ----------

  defp per_operative(ctx, fun) do
    observed_map = Map.get(ctx.reality, :operatives, %{})

    Enum.map(ctx.operatives, fn %{namespace: ns, operative_id: rid} ->
      key = {ns, rid}
      fun.({:operative, key}, key, Map.get(observed_map, key))
    end)
  end

  defp live_runners(ctx, namespace) do
    ctx.reality
    |> Map.get(:active_runners, %{})
    |> Map.get(namespace, MapSet.new())
  end

  # Every runner the tick knows about: the live roster of each namespace, plus
  # every runner a declared operative names as its binding, plus every runner
  # the liveness sample holds a live channel for. A binding to a runner no
  # roster holds is exactly what the judge has to be able to see, and so is a
  # session the rosters say nothing about at all.
  defp runner_roster(ctx) do
    rosters = Map.get(ctx.reality, :active_runners, %{})

    active =
      for {ns, runners} <- rosters,
          runners != :unknown,
          runner_id <- runners,
          into: %{},
          do: {runner_id, {ns, :active}}

    # A binding naming a runner no roster holds is `:absent` — unless the
    # namespace's roster read itself failed, which is `:unknown`: a read that
    # failed proves nothing, and evicting on it is what mass-migrates a healthy
    # fleet on a transient race.
    bound =
      Enum.reduce(Map.get(ctx.reality, :operatives, %{}), active, fn {{ns, _rid}, obs}, acc ->
        case Map.get(obs || %{}, :bound_runner) do
          runner_id when is_binary(runner_id) ->
            Map.put_new(acc, runner_id, {ns, off_roster(rosters, ns)})

          _ ->
            acc
        end
      end)

    # The rosters cover the running namespaces only, so a runner whose namespace
    # hub is not running is in none of them and no declared operative names it.
    # It is `:absent` on the same evidence the binding half has, none, and the
    # judge is what weighs that against the session's own silence.
    Enum.reduce(sample_channels(ctx), bound, &Map.put_new(&2, &1.runner_id, {nil, :absent}))
  end

  defp sample_channels(ctx), do: Map.get(ctx.signals[:liveness] || %{}, :channels, [])

  defp off_roster(rosters, namespace) do
    if Map.get(rosters, namespace) == :unknown, do: :unknown, else: :absent
  end

  defp fact(observed, subject), do: Fact.new(subject, observed)
end
