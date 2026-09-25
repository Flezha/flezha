defmodule Flezha.Convergence.Probe do
  @moduledoc """
  Pure observer of live, ephemeral reality — "what the system actually
  looks like right now". No decisions, no side effects.

  `snapshot/0` builds a `Reality` map the Loop diffs against
  `Intent.declared/0`.
  """

  alias Flezha.Cluster.Directory
  alias Flezha.Config.Durations
  alias Flezha.Convergence.Probe.Judge
  alias Flezha.Convergence.Snapshot
  alias Flezha.Parallel
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.FleetManager
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.HubManager
  alias Flezha.State.WriterRegistry

  @type placement :: %{
          {binary(), binary()} => %{local: [binary()] | :unknown, remote_nodes: [node()]}
        }

  @type reality :: %{
          operators: %{{binary(), binary()} => map()},
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
  The tick's judging context: the declared operators, the probed reality, the
  loop signals the readers name (`facts`, `stuck`, `stall_verdict`,
  `runner_facts`, `liveness`) and the tick instant.

  A plain map rather than the Loop's struct, so a fact reader depends on the
  signals it names and on nothing else — and so a test can judge one tick
  through exactly the path the Loop takes.
  """
  @spec context(Flezha.Convergence.Intent.t(), reality(), map(), integer()) ::
          Flezha.Convergence.Probe.Readers.context()
  def context(%Flezha.Convergence.Intent{operators: operators}, reality, signals, now) do
    %{operators: operators, reality: reality, signals: signals, now: now}
  end

  # ---------- reality ----------

  @spec snapshot() :: reality()
  def snapshot() do
    namespaces = running_namespaces()
    operators = operators(namespaces)

    %{
      operators: operators,
      active_runners: active_runners(namespaces),
      placement: placement(operators),
      recovery: Snapshot.recovery_states(),
      writers: live_writers(),
      tick_at: System.monotonic_time(:millisecond)
    }
  end

  # The Cells with a live writer — the evidence a declared gap or give-up
  # still has a log to close. `:unknown` on a failed read, never empty: a
  # missing writer clears a fact, and a read that failed proves nothing.
  defp live_writers() do
    WriterRegistry.registered_ids()
    |> Enum.filter(&(is_tuple(&1) and tuple_size(&1) == 3))
    |> MapSet.new()
  catch
    :exit, _ -> :unknown
  end

  # Probe every operator concurrently: one wedged Overwatch costs ~one timeout,
  # not N×timeout serial in the Loop process. An operator we cannot read is
  # recorded as `bound_runner: :unknown` so the Diff skips it — a failed
  # observation must never drive a mutating heal.
  defp operators(namespaces) do
    pairs =
      Enum.flat_map(namespaces, fn ns ->
        Enum.map(safe_operators(ns), fn {operator_id, pid} -> {{ns, operator_id}, pid} end)
      end)

    pairs
    |> Parallel.results(fn {_ref, pid} -> observe_operator(pid) end,
      max_concurrency: System.schedulers_online() * 4,
      timeout: Durations.get(:operator_call_timeout_ms) + Durations.get(:health_probe_budget_ms)
    )
    |> Enum.reduce(%{}, fn
      {{ref, _pid}, {:ok, observed}}, acc -> Map.put(acc, ref, observed)
      {{ref, _pid}, {:exit, _reason}}, acc -> Map.put(acc, ref, unreadable())
    end)
  end

  defp observe_operator(pid) when is_pid(pid) do
    case SafeCall.bounded(pid, :get_overwatch_state, Durations.get(:operator_call_timeout_ms)) do
      {:ok, ow} when is_map(ow) ->
        observed(%{
          pid: pid,
          bound_runner: Map.get(ow, :runner_id),
          registry_tag: registry_tag(ow),
          factory_id: Map.get(ow, :factory_id),
          parked?: Map.get(ow, :init_error) != nil,
          park_reason: Map.get(ow, :init_error),
          suspended?: Map.get(ow, :suspended, false),
          running_plugins: running_plugins(ow),
          subscribed_plugins: subscribed_plugins(ow),
          down_plugins: down_plugins(ow),
          plugin_uptimes: plugin_uptimes(ow),
          plugin_health: plugin_health(pid, ow)
        })

      _ ->
        unreadable()
    end
  end

  defp observe_operator(_), do: observed()

  # An operator whose state could not be read at all. `:unknown` rather than
  # `nil` for the binding: "we could not tell" must not be diffed as "unbound".
  defp unreadable(), do: observed(%{bound_runner: :unknown})

  # The one shape an observed operator has. `Diff` reads every key through a
  # default, so a field added to one constructor and forgotten in another would
  # not fail — it would silently read as "nothing diverged". There is therefore
  # exactly one place the keys are listed.
  defp observed(fields \\ %{}) do
    Map.merge(
      %{
        pid: nil,
        bound_runner: nil,
        registry_tag: nil,
        factory_id: nil,
        parked?: false,
        park_reason: nil,
        suspended?: false,
        running_plugins: [],
        subscribed_plugins: [],
        down_plugins: %{},
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

  # The subscribers whose owned pid crashed, with the reason — a plugin the
  # loop restarts rather than re-activates.
  defp down_plugins(ow), do: with_table(ow, %{}, &Table.down_ids/1)

  # How long each running plugin's owned pid has been up: the evidence that
  # resets a restarted plugin's key.
  defp plugin_uptimes(ow), do: with_table(ow, %{}, &Table.uptimes/1)

  # The runner the operator's durable row names. A rebind that raced a start's
  # late registry write leaves it naming the old runner; the Diff reports the
  # difference and `publish_binding` re-writes it.
  defp registry_tag(ow) do
    HubManager.operator_runner_tag(Map.get(ow, :namespace), Map.get(ow, :id))
  catch
    _, _ -> nil
  end

  # Provider self-reported health (`Extension.Table.health/1`). Owned plugin
  # pids live on the Overwatch's node, so only the loop running there can probe
  # them; elsewhere, report `%{}` — an uncertain probe is healthy and must never
  # drive a heal.
  defp plugin_health(pid, ow) do
    if node(pid) == node(), do: with_table(ow, %{}, &Table.health/1), else: %{}
  end

  # The one read of an operator's extension table. A snapshot that carries no
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
  # advertising the factory, and the OTHER nodes whose fleets do. The facts
  # that tell a doomed local rebind — the hub is already moving the operator
  # to a controller that can run it — from one worth planning. A failed local
  # read is `:unknown`, never `[]`, so the Diff never stops planning for an
  # operator on the strength of a read that failed.
  defp placement(operators) do
    operators
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
         remote_nodes: List.delete(Directory.factory_nodes(ns, factory), node())
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
  # mass-migrate every operator on a transient read failure.
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

  defp safe_operators(namespace) do
    HubManager.get_all_operators(namespace)
  catch
    :exit, _ -> []
  end
end
