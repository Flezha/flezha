defmodule Flezha.Service.Controller do
  @moduledoc """
  Published service-API surface for the controller itself: its readiness, the
  stop, its durability posture, its memory snapshot, the connected controllers
  and the cluster view of a namespace, each node's recovery state in it.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.
  `local_recovery_snapshot/1` and `local_controller/0` are the exceptions and
  are not on the published surface: they are the remote halves of
  `cluster_status/2`'s and `controllers/1`'s fan-out, which every peer calls by
  name. `stop_progress/4` is the telemetry handler a stop attaches.

  `stop/2` reports its progress as it goes, to the `:emit` the caller's context
  carries (a function, or a pid that receives `{:flezha_emit, event}`), from
  whichever process the progress happens in: `{:progress, event, measurements,
  metadata}` for each shutdown phase, then `{:stopped, answer, duration_ms}`
  once, before the node stops.

  The controller's event bus is deliberately NOT here: it registers local
  processes only. A consumer that needs runner sessions tails the
  `:flezha_runner` observability source, which carries `runner_connected` and
  `runner_disconnected`, through `Flezha.Service.Observability.tail/2`.
  """

  use Flezha.AuthZ.Verb

  alias Flezha.AuthZ.Namespace
  alias Flezha.Boot.Readiness
  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.FleetManager
  alias Flezha.Plugin.Provider.File.Storage
  alias Flezha.Process.Poll
  alias Flezha.Service.CallerContext
  alias Flezha.Service.Controller.MemTop
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier
  alias Flezha.State.Paths
  alias Flezha.Transport.LivenessCounters
  alias ProcessHub.Service.Recovery

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc """
  The readiness verdict and every precondition behind it. It also answers the
  recovery state and persistent-registry row count of each running namespace the
  caller may see (`Flezha.AuthZ.Namespace.visible/3`). The verb is public, so a
  caller with no identity gets the verdict and only `_system`'s row. Answers at
  once; `boot_await/2` waits.
  """
  @spec boot_status(caller_context()) :: {:ok, map()} | {:error, term()}
  verb boot_status(caller_context) do
    {verdict, rows} = Readiness.verdict()

    namespaces =
      for namespace <- Namespace.visible(HubManager.get_running_namespaces(), caller_context) do
        hub_id = Identificator.hub(namespace)

        %{
          namespace: namespace,
          recovery_state: safe(fn -> Recovery.recovery_state(hub_id) end, :unknown),
          registry_rows: safe(fn -> hub_id |> ProcessHub.registry_dump() |> Enum.count() end, 0)
        }
      end

    {:ok, %{readiness: verdict, preconditions: rows, namespaces: namespaces}}
  end

  @doc """
  Wait until the controller is ready, or the timeout passes. The timeout is
  `timeout_ms`. A timeout of zero evaluates the verdict once, which is what the
  image `HEALTHCHECK` runs.
  """
  @spec boot_await(non_neg_integer(), caller_context()) :: {:ok, atom()} | {:error, term()}
  verb boot_await(timeout_ms, caller_context),
    stream: true,
    args: [
      timeout_ms: [type: :int, min: 0, default: 60_000, doc: "How long to wait, in milliseconds."]
    ] do
    case poll_ready(System.monotonic_time(:millisecond) + timeout_ms) do
      {:ready, _rows} ->
        {:ok, :ready}

      {verdict, rows} ->
        {:error, {:not_ready, verdict, for(%{key: key, met: false} <- rows, do: key)}}
    end
  end

  @doc """
  The controller's status: node, clock, shutdown and durability. In full: its
  node and its monotonic clock, its shutdown state and settings, its durability,
  its state root and the folder file plugins may use.

  `monotonic_us` is the node's `System.monotonic_time(:microsecond)`, the clock
  observability signals are stamped with, so a consumer on another node can age
  a signal against it.
  """
  @spec status(caller_context()) :: {:ok, map()} | {:error, term()}
  verb status(caller_context), verb: :controller_status do
    {:ok,
     shutdown_snapshot()
     |> Map.merge(durability_snapshot())
     |> Map.merge(%{
       node: node(),
       monotonic_us: System.monotonic_time(:microsecond),
       state_root: Paths.state_root(),
       file_plugin_root: Storage.Config.shown_root()
     })}
  end

  @doc "One snapshot of the controller's memory hot spots."
  @spec mem_top(caller_context()) :: {:ok, map()} | {:error, term()}
  verb mem_top(caller_context), verb: :controller_mem_top do
    {:ok, MemTop.snapshot()}
  end

  @doc """
  The cluster as one namespace sees it: nodes, relays and health. Each node
  comes with its relay count and recovery state, beside the unplaced relays and
  the relays' health.
  """
  @spec cluster_status(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb cluster_status(namespace, caller_context),
    args: [namespace: [type: :string, doc: "The namespace."]] do
    namespace = Namespace.or_default(namespace)
    nodes = HubManager.get_hub_nodes(namespace, true)
    rows = HubManager.relay_rows(namespace)
    counts = Enum.frequencies_by(rows, & &1.node)
    recovery = recovery_map(namespace, nodes)

    # There is no leader election: the convergence loop uses local reads and
    # process_hub's own coordinator for cluster-wide work, so the local node is
    # the de-facto leader.
    {:ok,
     %{
       namespace: namespace,
       leader: node(),
       local_node: node(),
       is_leader: true,
       nodes:
         Enum.map(nodes, fn n ->
           %{
             node: n,
             relay_count: Map.get(counts, n, 0),
             leader: n == node(),
             recovery_state: Map.get(recovery, n, :unknown)
           }
         end),
       unplaced_relays: Map.get(counts, nil, 0),
       relay_health: Enum.frequencies_by(rows, & &1.health)
     }}
  end

  @doc """
  Every connected controller, the one that answered first. Each row gives its
  node, its `host` address, the machine it runs on (`host_node`, `nil` while
  `FLEZHA_HOST_NODE` is unset), its `version`, `uptime_ms`, how many `runners`
  are connected to it, and `local?` for the one that answered. A controller
  that does not answer in time is listed with its node and `reachable?: false`.
  """
  @spec controllers(caller_context()) :: {:ok, [map()]} | {:error, term()}
  verb controllers(caller_context), verb: :controller_status do
    nodes = [node() | Enum.sort(Node.list())]
    facts = fan_out(nodes, :local_controller, [])

    {:ok,
     Enum.map(nodes, fn n ->
       case Map.fetch!(facts, n) do
         :unreachable -> %{node: n, reachable?: false, local?: false}
         %{} = row -> Map.merge(row, %{reachable?: true, local?: n == node()})
       end
     end)}
  end

  @doc "The durability breaches recorded since the controller started."
  @spec durability_breaches(caller_context()) :: {:ok, term()} | {:error, term()}
  verb durability_breaches(caller_context), verb: :controller_status do
    {:ok, DurabilityTier.breaches()}
  end

  @doc "The durability shipper's own status."
  @spec durability_status(caller_context()) :: {:ok, term()} | {:error, term()}
  verb durability_status(caller_context), verb: :controller_status do
    {:ok, DurabilityTier.status()}
  end

  @doc "Every data-plane liveness counter the connected controllers hold."
  @spec liveness_counters(caller_context()) :: {:ok, term()} | {:error, term()}
  verb liveness_counters(caller_context), verb: :controller_status do
    {:ok, LivenessCounters.snapshot_cluster()}
  end

  # ---------- Mutations ----------

  @doc """
  Drain this controller and stop it. `opts` carries `timeout_ms`, `force` (skip
  the quiesce and the drain, finalise only) and `no_wait` (dispatch the stop
  and answer at once). Progress goes to the context's `:emit`.
  """
  @spec stop(keyword(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb stop(opts, caller_context),
    verb: :controller_stop,
    stream: true,
    args: [opts: [type: :keyword, default: [], doc: "`timeout_ms`, `force` and `no_wait`."]] do
    {emit, _watch} = CallerContext.emitter(caller_context)
    force = Keyword.get(opts, :force) == true

    shutdown_opts =
      Enum.reject([timeout_ms: Keyword.get(opts, :timeout_ms), force: force], fn {_key, value} ->
        value in [nil, false]
      end)

    if Keyword.get(opts, :no_wait) == true do
      _ = Task.start(fn -> Coordinator.shutdown_and_stop(:cli, shutdown_opts) end)
      {:ok, %{outcome: :dispatched}}
    else
      with_progress(emit, fn ->
        started = System.monotonic_time(:millisecond)

        Coordinator.shutdown_and_stop(:cli, shutdown_opts, fn outcome ->
          answer = stopped(outcome, force)
          emit.({:stopped, answer, System.monotonic_time(:millisecond) - started})
          answer
        end)
      end)
    end
  end

  @doc false
  # The handler a stop attaches; it runs in whichever process emitted the event.
  def stop_progress(event, measurements, metadata, %{emit: emit}) do
    emit.({:progress, event, measurements, metadata})
  rescue
    _ -> :ok
  end

  @doc false
  # The remote half of `cluster_status/2`'s recovery fan-out. Public because the
  # fan-out calls it by name on every peer.
  @spec local_recovery_snapshot(atom()) :: atom()
  def local_recovery_snapshot(hub_id) when is_atom(hub_id) do
    case HubManager.recovery_state(hub_id) do
      {:ok, state} -> state
      {:error, :unknown_hub} -> :unknown
    end
  end

  @doc false
  # The remote half of `controllers/1`.
  @spec local_controller() :: map()
  def local_controller do
    {uptime_ms, _since_last} = :erlang.statistics(:wall_clock)
    [_name, host] = node() |> Atom.to_string() |> String.split("@", parts: 2)

    runners =
      for namespace <- HubManager.get_running_namespaces(),
          row <- FleetManager.Cluster.runner_rows(namespace),
          uniq: true,
          do: row.id

    %{
      node: node(),
      host: host,
      host_node: Config.get(:host_node),
      version: to_string(Application.spec(:flezha, :vsn)),
      uptime_ms: uptime_ms,
      runners: length(runners)
    }
  end

  defp recovery_map(namespace, nodes),
    do: fan_out(nodes, :local_recovery_snapshot, [Identificator.hub(namespace)])

  # Each of `nodes`' answer to `function`, this node's read in place and the
  # peers' at once; a peer that does not answer in time answers `:unreachable`.
  defp fan_out(nodes, function, args) do
    peers = List.delete(nodes, node())
    answers = :erpc.multicall(peers, __MODULE__, function, args, Durations.get(:peer_read_ms))

    remote =
      Enum.zip_with(peers, answers, fn
        peer, {:ok, answer} -> {peer, answer}
        peer, _failed -> {peer, :unreachable}
      end)

    local = if node() in nodes, do: [{node(), apply(__MODULE__, function, args)}], else: []
    Map.new(local ++ remote)
  end

  # The phase lines come from an attached handler, so the detach must survive
  # every exit from the drive, including a coordinator that is already gone.
  defp with_progress(emit, drive) do
    handler_id = {__MODULE__, :stop_progress, make_ref()}

    :ok =
      :telemetry.attach_many(
        handler_id,
        [
          [:flezha, :shutdown, :phase_started],
          [:flezha, :shutdown, :phase_completed],
          [:flezha, :shutdown, :drain_progress],
          [:flezha, :shutdown, :timeout]
        ],
        &__MODULE__.stop_progress/4,
        %{emit: emit}
      )

    try do
      drive.()
    after
      :telemetry.detach(handler_id)
    end
  end

  defp stopped(:ok, force), do: {:ok, %{outcome: if(force, do: :forced, else: :clean)}}
  defp stopped({:partial, still_local}, _force), do: {:error, {:partial_drain, still_local}}

  # The coordinator is gone because the node is already stopping: the stop
  # that began it is the outcome, and there is nothing left to drain.
  defp stopped({:exit, {:noproc, _}}, _force),
    do: {:ok, %{outcome: :clean, already_stopping: true}}

  defp stopped({:exit, reason}, _force), do: {:error, {:coordinator_exited, reason}}
  defp stopped(other, _force), do: {:error, {:coordinator_exited, other}}

  # The last verdict, ready or not.
  defp poll_ready(deadline) do
    ready = fn ->
      case Readiness.verdict() do
        {:ready, _rows} = ready -> {:ok, ready}
        unready -> {:error, unready}
      end
    end

    {_ready?, verdict} = Poll.until(deadline, Durations.get(:boot_await_poll_ms), ready)
    verdict
  end

  # A hub can be torn down between the listing and the read; a status table
  # reports what it could not read rather than failing.
  defp safe(read, fallback) do
    read.()
  catch
    _, _ -> fallback
  end

  defp shutdown_snapshot do
    Coordinator.status()
  catch
    :exit, _ ->
      %{
        shutdown_in_progress: false,
        shutdown_phase: nil,
        signal_handler_status: :unknown,
        graceful_shutdown_enabled: Config.get(:graceful_shutdown_enabled),
        graceful_shutdown_timeout_ms: Durations.get(:graceful_shutdown_timeout_ms)
      }
  end

  # The remote durability tier as a caller reads it: which backend is
  # configured, how many references are past their RPO target, how many are
  # now shipped by another controller, and each class's level. A superseded
  # count that keeps climbing on a node that should still own its relays is
  # how a placement problem shows up here.
  defp durability_snapshot do
    %{
      durability_backend: DurabilityTier.configured_backend(),
      durability_posture: DurabilityTier.posture(),
      durability_rpo_breaches: length(DurabilityTier.breaches()),
      durability_levels: Map.new(ClassRegistry.running(), &{&1, ClassRegistry.durable_level(&1)}),
      durability_superseded: Enum.count(DurabilityTier.status(), &(&1.superseded_by != nil))
    }
  end
end
