defmodule Flezha.Convergence.Probe do
  @moduledoc """
  Pure observer of live, ephemeral reality — "what the system actually
  looks like right now". No decisions, no side effects.

  `snapshot/0` builds a `Reality` map the Loop diffs against
  `Intent.declared/0`. It observes only the relays whose Overwatch runs on
  this controller, so a declared relay hosted elsewhere is unobserved here and
  its own controller's loop places it.
  """

  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Probe.Judge
  alias Flezha.Convergence.Snapshot
  alias Flezha.Identificator
  alias Flezha.Parallel
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Relay.Overwatch
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.State.WriterRegistry

  @type placement :: %{
          {binary(), binary()} => %{local: [binary()] | :unknown, remote_nodes: [node()]}
        }

  @typedoc "The node of each relay read, with the plugin ids it has running."
  @type plugins_running :: [{node(), [binary()]}]

  @type reality :: %{
          relays: %{{binary(), binary()} => map()},
          active_runners: %{binary() => MapSet.t() | :unknown},
          placement: placement(),
          recovery: %{atom() => atom()},
          writers: MapSet.t() | :unknown,
          tick_at: integer()
        }

  @doc """
  Fold the tick's facts into one verdict record per subject. The Probe observes
  and the judge decides; `Flezha.Convergence.Probe.Judge` holds the rules.
  """
  @spec judge(Flezha.Convergence.Probe.Readers.context(), %{
          term() => Flezha.Convergence.Verdict.t()
        }) ::
          %{term() => Flezha.Convergence.Verdict.t()}
  defdelegate judge(context, previous), to: Judge

  @doc """
  The tick's judging context: the declared relays, the probed reality, the
  loop signals the readers name (`facts`, `stuck`, `stall_verdict`,
  `runner_facts`, `liveness`) and the tick instant.

  A plain map rather than the Loop's struct, so a fact reader depends on the
  signals it names and on nothing else — and so a test can judge one tick
  through exactly the path the Loop takes.
  """
  @spec context(Flezha.Convergence.Intent.t(), reality(), map(), integer()) ::
          Flezha.Convergence.Probe.Readers.context()
  def context(%Flezha.Convergence.Intent{relays: relays}, reality, signals, now) do
    %{relays: relays, reality: reality, signals: signals, now: now}
  end

  # ---------- reality ----------

  @spec snapshot() :: reality()
  def snapshot() do
    namespaces = running_namespaces()
    relays = relays(namespaces)

    %{
      relays: relays,
      active_runners: active_runners(namespaces),
      placement: placement(relays),
      recovery: Snapshot.recovery_states(),
      writers: live_writers(),
      tick_at: System.monotonic_time(:millisecond)
    }
  end

  @doc """
  The node of each of `relay_ids` in `namespace` that answers, with the plugin
  ids its Overwatch has running: read as the snapshot reads a relay, in one
  bounded fan-out. A relay that is not registered, or does not answer within
  `relay_call_timeout_ms`, is left out.
  """
  @spec plugins_running(binary(), [binary()]) :: plugins_running()
  def plugins_running(namespace, relay_ids) do
    pairs = for id <- relay_ids, {:ok, pid} <- [HubManager.get_pid(namespace, id)], do: {id, pid}

    for {{_id, pid}, {:ok, {:ok, ow}}} <- each_relay(pairs, &overwatch/1),
        do: {node(pid), running_plugins(ow)}
  end

  @doc """
  The relay's Overwatch state, read under `relay_call_timeout_ms`; `:error`
  when it does not answer. A relay already gone is not logged: one stopped
  after it was listed is routine, and the next listing leaves it out.
  """
  @spec overwatch(pid()) :: {:ok, map()} | :error
  def overwatch(pid) do
    timeout = Durations.get(:relay_call_timeout_ms)

    case SafeCall.bounded(pid, :get_overwatch_state, timeout,
           on_error: :error,
           log: %{noproc: false}
         ) do
      ow when is_map(ow) -> {:ok, ow}
      _ -> :error
    end
  end

  # The Cells with a live writer — the evidence a declared gap or give-up
  # still has a log to close. `:unknown` on a failed read, never empty: a
  # missing writer clears a fact, and a read that failed proves nothing.
  defp live_writers() do
    MapSet.new(WriterRegistry.cell_writers(), &elem(&1, 0))
  catch
    :exit, _ -> :unknown
  end

  # Probe every relay this controller hosts, off the Loop process. A relay hosted
  # elsewhere is its own controller's loop's: this one holds neither the runners
  # nor the streams it would be judged and bound against. A relay we cannot read
  # is recorded as `bound_runner: :unknown` so the Diff skips it — a failed
  # observation must never drive a mutating heal.
  defp relays(namespaces) do
    pairs =
      for ns <- namespaces,
          {relay_id, pid} <- HubManager.local_children(ns, :relays),
          do: {{ns, relay_id}, pid}

    pairs
    |> each_relay(&observe_relay/1)
    |> Enum.reduce(%{}, fn
      {{ref, _pid}, {:ok, observed}}, acc -> Map.put(acc, ref, observed)
      {{ref, _pid}, {:exit, _reason}}, acc -> Map.put(acc, ref, unreadable())
    end)
  end

  # Run `fun` on every `{key, pid}` concurrently: one wedged Overwatch costs
  # ~one timeout, not N×timeout serial.
  defp each_relay(pairs, fun) do
    Parallel.results(pairs, fn {_key, pid} -> fun.(pid) end,
      max_concurrency: System.schedulers_online() * 4,
      timeout: Durations.get(:relay_call_timeout_ms) + Durations.get(:health_probe_budget_ms)
    )
  end

  defp observe_relay(pid) do
    case overwatch(pid) do
      {:ok, ow} ->
        observed(%{
          pid: pid,
          bound_runner: Map.get(ow, :runner_id),
          registry_tag: registry_tag(ow),
          factory_id: Map.get(ow, :factory_id),
          phase: Overwatch.phase(ow),
          halt_reason: Map.get(ow, :halt_reason),
          running_plugins: running_plugins(ow),
          subscribed_plugins: subscribed_plugins(ow),
          down_plugins: phase_plugins(ow, :down),
          not_started_plugins: phase_plugins(ow, :not_started),
          plugin_uptimes: plugin_uptimes(ow),
          plugin_health: with_table(ow, %{}, &Table.health/1)
        })

      :error ->
        unreadable()
    end
  end

  # A relay whose state could not be read at all. `:unknown` rather than
  # `nil` for the binding: "we could not tell" must not be diffed as "unbound".
  defp unreadable(), do: observed(%{bound_runner: :unknown})

  # The one shape an observed relay has. `Diff` reads every key through a
  # default, so a field added to one constructor and forgotten in another would
  # not fail — it would silently read as "nothing diverged". There is therefore
  # exactly one place the keys are listed.
  defp observed(fields) do
    Map.merge(
      %{
        pid: nil,
        bound_runner: nil,
        registry_tag: nil,
        factory_id: nil,
        phase: nil,
        halt_reason: nil,
        running_plugins: [],
        subscribed_plugins: [],
        down_plugins: %{},
        not_started_plugins: %{},
        plugin_uptimes: %{},
        plugin_health: %{}
      },
      fields
    )
  end

  # The plugin ids that reached `:running` — the ones actually holding their
  # provider's upstream resources. A plugin whose `activate/1` failed is
  # subscribed but `:prepared`, and reporting it as running is what let a
  # source that never connected sit undetected forever.
  defp running_plugins(ow), do: with_table(ow, [], &Table.running_ids/1)

  # The full subscribed set, whatever the phase — what the `extra_plugins`
  # check reads, so an undeclared plugin is torn down in either phase.
  defp subscribed_plugins(ow), do: with_table(ow, [], &Table.subscribed_ids/1)

  # The subscribers whose owned pid crashed, with the reason, which the loop
  # restarts rather than re-activates; and the bindings that cannot come up
  # here, whose start waits on something outside the loop.
  defp phase_plugins(ow, tag), do: with_table(ow, %{}, &Table.phase_reasons(&1, tag))

  # How long each running plugin's owned pid has been up: the evidence that
  # resets a restarted plugin's key.
  defp plugin_uptimes(ow), do: with_table(ow, %{}, &Table.uptimes/1)

  # The runner the relay's durable row names. A rebind that raced a start's
  # late registry write leaves it naming the old runner; the Diff reports the
  # difference and `publish_binding` re-writes it.
  defp registry_tag(ow) do
    HubManager.relay_runner_tag(Map.get(ow, :namespace), Map.get(ow, :id))
  catch
    _, _ -> nil
  end

  # The one read of a relay's extension table. A snapshot that carries no
  # table, or a read that fails, answers `default` — which for every caller is
  # the value that plans nothing.
  defp with_table(ow, default, fun) do
    case Map.get(ow, :extensions) do
      %Table{} = table -> fun.(table)
      _ -> default
    end
  rescue
    _ -> default
  catch
    _, _ -> default
  end

  # For every observed `{namespace, factory}` pair: this node's runners
  # advertising the factory, and the OTHER nodes the hub places it on, the ones
  # serving its group. The facts that tell a doomed local rebind — the hub is
  # already moving the relay to a controller that can run it — from one worth
  # planning. A peer that advertises the factory without serving it is no such
  # controller, and a hub that cannot be read names none. A failed local read
  # is `:unknown`, never `[]`, so the Diff never stops planning for a relay on
  # the strength of a read that failed.
  defp placement(relays) do
    relays
    |> Enum.flat_map(fn {{ns, _rid}, observed} ->
      case Map.get(observed, :factory_id) do
        factory when is_binary(factory) -> [{ns, factory}]
        _ -> []
      end
    end)
    |> Enum.uniq()
    |> Enum.into(%{}, fn {ns, factory} ->
      {{ns, factory},
       %{
         local: local_factory_runners(ns, factory),
         remote_nodes:
           List.delete(GroupedHashing.group_nodes(Identificator.hub(ns), factory), node())
       }}
    end)
  end

  defp local_factory_runners(namespace, factory) do
    FleetManager.runners_for_factory(namespace, factory)
  catch
    :exit, _ -> :unknown
  end

  # On a failed FleetManager read we record `:unknown`, NOT an empty set:
  # the Diff must skip misbound classification for the namespace rather than
  # mass-migrate every relay on a transient read failure.
  defp active_runners(namespaces) do
    Enum.into(namespaces, %{}, fn ns ->
      runners =
        try do
          FleetManager.active_runners(ns) |> MapSet.new()
        catch
          :exit, _ -> :unknown
        end

      {ns, runners}
    end)
  end

  # ---------- helpers ----------

  defp running_namespaces() do
    HubManager.get_running_namespaces()
  catch
    :exit, _ -> []
  end
end
