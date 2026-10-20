defmodule Flezha.Convergence.Heal do
  @moduledoc """
  The entire heal surface: nine idempotent, bounded primitives. Every
  catalogue row reduces to a composition of these — adding a failure mode
  never adds a primitive, and adding a remedy is one entry in the table.

      rebind_relay  resync_runner
      start_plugin  stop_plugin  restart_plugin
      evict_runner  restart_runner
      reset_control_channel  publish_binding

  None of them moves a relay between controllers. Which controller hosts
  a relay is the namespace hub's decision alone
  (`Flezha.Cluster.GroupedHashing`), and it only ever names a controller whose
  fleet can run the relay's factory.

  `execute/1` dispatches one planned op to its primitive and returns
  `:ok | :error` so the Loop can drive `Backoff`.

    * `rebind_relay` — migrate a relay tagged to a dead/wrong runner (or
      not yet bound, or halted) onto an available one through the full bind:
      resume state, the durable Cells restored, the input log's tail
      re-forwarded, then the registry tag and the route published.
    * `resync_runner` — re-run the `RestoreInitState` handshake for a
      relay against the runner it is already bound to, or for ONE Cell
      against the runner a declared gap named. The rung below
      `rebind_relay`: no placement decision, no migration, no handover —
      the remedy for a binding that is intact but has stopped agreeing.
    * `start_plugin` / `stop_plugin` — reconcile a relay's running plugin
      set against its declared cspec manifest.
    * `restart_plugin` — re-run a declared plugin's provider, whether it
      self-reported unhealthy or its subscriber crashed, so `activate/1`
      reopens its upstream resources.
    * `evict_runner` — forcibly drop a dead runner binding; a wedge the
      classifier proved also waives the "never evict a live runner" guard,
      since a still-connected runner processing nothing is precisely the
      case that guard would otherwise protect forever.
    * `restart_runner` — the rung ABOVE `evict_runner`, and the only remedy
      that clears a wedged process: a terminal disconnect the SDK answers by
      running its graceful shutdown and then exiting non-zero, so the
      orchestrator that owns process lifecycle restarts it. Eviction alone
      leaves the wedged runner connected and placeable, which is the flap —
      evict, reconnect, take work, wedge.
    * `reset_control_channel` — the controller-side reconnect request: clear a
      runner's `stream_ref` so its next data stream claims the channel, planned
      from a halt whose bind reported the transport dead.
    * `publish_binding` — re-write a relay's durable row and route from
      the runner its Overwatch is bound to, planned from a registry write that
      named a stale runner.
  """

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Planner
  alias Flezha.Convergence.Probe
  alias Flezha.Relay.Overwatch
  alias Flezha.Runner.{ControlUnit, FleetManager}
  alias Flezha.SDK.SafeCall
  alias Flezha.Service.{HubManager, RelayManager}
  alias Flezha.State.Recovery

  require Logger

  # The primitive table: the name, the executor the op reaches, and the
  # evidence that resets its key — `:ok` (the executor's own answer) or the
  # observation named. An op carrying a declared reason resets on that
  # reason's evidence instead (`Flezha.Convergence.Facts`).
  @table [
    %{name: :rebind_relay, executor: "Overwatch {:rebind, target}", evidence: :ok},
    %{
      name: :resync_runner,
      executor: "State.Recovery.resync_for_overwatch/2 | resync/2",
      evidence: :ok
    },
    %{
      name: :start_plugin,
      executor: "Overwatch.plugin_heal/4 → Plugins.subscribe/4",
      evidence: :ok
    },
    %{name: :stop_plugin, executor: "Overwatch.plugin_unsubscribe/2", evidence: :ok},
    %{
      name: :restart_plugin,
      executor: "Overwatch.plugin_heal/4 → Plugins.restart/3",
      evidence: :running_stable
    },
    %{name: :evict_runner, executor: "FleetManager.evict_runner/2", evidence: :ok},
    %{name: :restart_runner, executor: "ControlUnit.disconnect/2 terminal", evidence: :ok},
    %{
      name: :reset_control_channel,
      executor: "FleetManager.reset_control_channel/1",
      evidence: :ok
    },
    %{name: :publish_binding, executor: "Overwatch :publish_binding", evidence: :ok}
  ]

  @primitives Enum.map(@table, & &1.name)

  @doc "The primitive names, in table order."
  @spec primitives() :: [atom()]
  def primitives(), do: @primitives

  @doc "The primitive table: name, executor and the evidence that resets the key."
  @spec table() :: [%{name: atom(), executor: String.t(), evidence: atom()}]
  def table(), do: @table

  @doc """
  Apply one planned op. `:ok` and `:error` drive the ladder; `:unmet` is the
  third answer and drives nothing — the op's precondition did not hold, so
  nothing was attempted and the target's rung is neither advanced nor reset.
  """
  @spec execute(Flezha.Convergence.Planner.op()) :: :ok | :error | :unmet
  def execute(%{primitive: primitive} = op) when primitive in @primitives do
    :telemetry.execute(
      [:flezha, :convergence, :heal],
      %{count: 1},
      %{primitive: primitive, namespace: op.namespace, target: op.target}
    )

    apply_primitive(primitive, op)
  end

  def execute(%{primitive: other}) do
    Logger.warning("[convergence] unknown heal primitive #{inspect(other)}")
    :error
  end

  defp apply_primitive(:rebind_relay, %{namespace: namespace, target: relay_id} = op) do
    on_relay(namespace, relay_id, &rebind_relay(op, &1))
  end

  # One Cell, against the runner the declared gap or give-up named: the
  # dispatch reads `{:ok, ref}` when the handshake left; the repair is proved
  # later, by the gap closing.
  defp apply_primitive(:resync_runner, %{
         args: %{cell_id: cell_id, runner_id: runner_id} = args
       })
       when is_tuple(cell_id) and is_binary(runner_id) do
    case Recovery.resync(cell_id, runner_id: runner_id, reason: Map.get(args, :reason)) do
      {:ok, _} -> :ok
      _ -> :error
    end
  end

  # Re-issue `RestoreInitState` for every persisted Cell of a relay against
  # the runner it is ALREADY bound to — the same replay a rebind performs after
  # migrating, without the migration. A relay with no live pid or no binding
  # has nothing to resync; that is a failed heal, and the next tick reaches it
  # through `rebind_relay` instead.
  defp apply_primitive(:resync_runner, %{namespace: namespace, target: relay_id}) do
    on_relay(namespace, relay_id, fn pid ->
      case Probe.overwatch(pid) do
        {:ok, %{runner_id: runner_id}} when is_binary(runner_id) ->
          Recovery.resync_for_overwatch(pid, runner_id)

        _ ->
          :error
      end
    end)
  end

  # (re)subscribe a declared-but-missing plugin into the running relay through
  # `Plugins.subscribe/4`, the path `flz plugin enable` takes (which activates
  # immediately since the SDK is already up).
  defp apply_primitive(:start_plugin, %{namespace: ns, target: rid, args: args}) do
    plugin_call(ns, rid, &Overwatch.plugin_heal(&1, :start_plugin, args.plugin_id, args.plugin))
  end

  # Unsubscribe a plugin that is running but not declared in the cspec.
  defp apply_primitive(:stop_plugin, %{namespace: ns, target: rid, args: args}) do
    plugin_call(ns, rid, &Overwatch.plugin_unsubscribe(&1, args.plugin_id))
  end

  # Re-run a declared plugin's provider, whether it self-reported unhealthy or
  # its subscriber crashed. On failure the plugin keeps its phase and the next
  # eligible rung retries it.
  defp apply_primitive(:restart_plugin, %{namespace: ns, target: rid, args: args}) do
    plugin_call(ns, rid, &Overwatch.plugin_heal(&1, :restart_plugin, args.plugin_id, args.plugin))
  end

  # Forcibly drop a dead/orphaned runner binding so its relays become
  # misbound and rebind onto a live runner next tick. For a proved wedge this is
  # the rung above the restart: a process that did not exit.
  defp apply_primitive(:evict_runner, %{target: runner_id} = op) when is_binary(runner_id) do
    case FleetManager.evict_runner(runner_id, proved_wedged: Planner.proved_wedged?(op)) do
      :ok -> :ok
      _ -> :error
    end
  end

  # Terminal disconnect: the SDK shuts down gracefully and exits non-zero so its
  # orchestrator restarts the process. The first rung for a runner the
  # classifier proves wedged, gated on `proved_wedged?/1`: its session closes
  # with the process, and the declared disconnect moves its relays.
  defp apply_primitive(:restart_runner, %{target: runner_id, args: args} = op)
       when is_binary(runner_id) do
    if Planner.proved_wedged?(op) do
      layer = Map.get(args, :layer)
      why = "convergence:#{layer}"

      case ControlUnit.disconnect(runner_id,
             terminal: true,
             reason: why,
             timeout: Durations.get(:migrate_timeout_ms)
           ) do
        {:ok, _} ->
          emit_restart(runner_id, layer)
          :ok

        _ ->
          :error
      end
    else
      # Never restart a runner on anything but proved-wedged evidence; a merely
      # slow or idle runner must be left alone.
      Logger.warning("[convergence] refused restart_runner for #{runner_id}: wedge not proved")
      :error
    end
  end

  # The controller-side reconnect request. Sends nothing: it clears the
  # channel's stale `stream_ref` so the SDK's next data stream claims it.
  defp apply_primitive(:reset_control_channel, %{target: runner_id}) when is_binary(runner_id) do
    case FleetManager.reset_control_channel(runner_id) do
      :ok -> :ok
      _ -> :error
    end
  end

  # The Overwatch publishes from its own process — the route it seeds points at
  # itself — so the primitive asks it to, under a call timeout.
  defp apply_primitive(:publish_binding, %{namespace: namespace, target: relay_id}) do
    on_relay(namespace, relay_id, fn pid ->
      case SafeCall.bounded(pid, :publish_binding, Durations.get(:relay_call_timeout_ms)) do
        {:ok, :ok} -> :ok
        _ -> :error
      end
    end)
  end

  # Malformed op (no live target) — never produced by the Planner, but a
  # crash here would take down the whole tick. Treat as a failed heal.
  defp apply_primitive(prim, _op) when prim in @primitives, do: :error

  # Migrate a relay bound to a dead/wrong runner onto an available one.
  # The Overwatch runs the bind in its own task and publishes the binding, the
  # registry tag and the target runner's route once it lands; a bind that
  # fails halts the relay for the next tick under Backoff.
  defp rebind_relay(%{namespace: namespace, target: relay_id} = op, pid) do
    with {:ok, %{factory_id: factory, runner_id: prev}} <- Probe.overwatch(pid),
         true <- factory != nil,
         false <- skip_rebind?(namespace, factory, prev, op),
         {:ok, target} <- placed(RelayManager.assign_runner(namespace, factory, relay_id)),
         {:ok, :ok} <-
           SafeCall.bounded(pid, {:rebind, target}, Durations.get(:migrate_timeout_ms)) do
      emit_reconnected(namespace, relay_id, prev, target)
      :ok
    else
      # `skip_rebind?` returned true — a MISBOUND relay whose bound runner
      # is already active and owns the factory, so a stale-snapshot rebind
      # would only disrupt a healthy relay. Safe no-op.
      true ->
        :ok

      # No runner in the namespace can serve this relay's factory. That is
      # an unmet precondition, not a repair that failed: the ladder spaces out
      # attempts that could have worked, and charging a rung for one that could
      # not is what turns a wake with nothing to bind to into a longer wait.
      :unplaced ->
        :unmet

      # The previous rebind's bind is still running: `rebind` answers
      # before its bind lands, so the next tick plans the same rebind. Every
      # step of that bind is a bounded call and a failed one halts the relay,
      # so a bind that never lands comes back as a `:halted` rebind.
      {:ok, {:error, :busy}} ->
        :unmet

      _ ->
        :error
    end
  end

  # A misbound rebind is a safe no-op once the bound runner is back in the
  # live, factory-owning active set (a reconnect between snapshot and heal
  # time): re-reading the authoritative set here stops a detection slip from
  # churning a healthy relay. An op whose reason PROVED the target is not
  # producing is never skipped — that guard protects the healthy, and this
  # target is not — and neither is a halted one, whose runner is exactly the
  # one its bind failed on.
  defp skip_rebind?(_namespace, _factory, prev, _op) when prev in [nil, ""], do: false
  defp skip_rebind?(_namespace, _factory, _prev, %{args: %{reason: :halted}}), do: false

  defp skip_rebind?(namespace, factory, prev, op) do
    not Planner.proved_wedged?(op) and
      prev in FleetManager.runners_for_factory(namespace, factory)
  end

  # Resolve the relay's live pid or fail the heal — the prologue every
  # relay-scoped primitive shares.
  defp on_relay(namespace, relay_id, fun) do
    case HubManager.get_pid(namespace, relay_id) do
      {:ok, pid} when is_pid(pid) -> fun.(pid)
      _ -> :error
    end
  end

  # The three plugin primitives differ only in the verb they send; the call and
  # its bound belong to `Overwatch`. A start or restart is judged by the phase it
  # left the binding in: one that cannot start until its plugin is enabled or its
  # provider loaded is an unmet precondition, which no rung of the ladder fixes.
  # The reason of a failure is already logged by the bounded call.
  defp plugin_call(namespace, relay_id, verb) do
    on_relay(namespace, relay_id, fn pid ->
      case verb.(pid) do
        :ok -> :ok
        {:ok, :running} -> :ok
        {:ok, {:not_started, _reason}} -> :unmet
        _ -> :error
      end
    end)
  end

  # The placement decision, told apart from the reads around it: `nil` is "no
  # runner or node can serve this relay", the one failure that is an unmet
  # precondition rather than a repair that failed.
  defp placed(nil), do: :unplaced
  defp placed(target), do: {:ok, target}

  # A runner restarting on a schedule must be visible rather than mysterious:
  # the verdict that proved the wedge and the ladder rung both ride the event.
  defp emit_restart(runner_id, layer) do
    :telemetry.execute(
      [:flezha, :runner, :restarted],
      %{count: 1},
      %{runner_id: runner_id, verdict: layer, rung: :restart_runner}
    )
  end

  defp emit_reconnected(namespace, relay_id, prev, target) do
    meta = %{namespace: namespace, relay_id: relay_id, runner_id: target}

    :telemetry.execute(
      [:flezha, :runner, :reconnected_reassigned],
      %{count: 1},
      Map.merge(meta, %{previous_id: prev, new_id: target, reason: :convergence_rebind})
    )

    :telemetry.execute(
      [:flezha, :runner, :reconnected],
      %{count: 1},
      Map.merge(meta, %{reason: :convergence_rebind, mode: :reassigned})
    )
  end
end
