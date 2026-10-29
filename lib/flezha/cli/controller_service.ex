defmodule Flezha.Service.Controller do
  @moduledoc """
  Published service-API surface for the controller itself: its readiness, the
  stop, its durability posture, its memory snapshot, the connected controllers
  and the cluster view of a namespace, each node's recovery state in it.

  Every public function takes a `Flezha.Service.CallerContext` as its last
  argument, binds a row of `Flezha.AuthZ.Verbs`, and answers data.
  `local_recovery_snapshot/1` and `local_controller/0` are the exceptions and
  are not on the published surface: they are the local halves of
  `cluster_status/2`'s and `controllers/1`'s cluster reads, which every peer
  calls by name. `stop_progress/4` is the telemetry handler a stop attaches.

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
  alias Flezha.Cluster.Directory
  alias Flezha.Cluster.Page
  alias Flezha.Cluster.Reads
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
  alias Flezha.Service.Relay.Info
  alias Flezha.Shutdown.Coordinator
  alias Flezha.State.DurabilityTier
  alias Flezha.State.Paths
  alias Flezha.Transport.LivenessCounters
  alias ProcessHub.Service.Recovery

  @type caller_context :: CallerContext.t()

  # ---------- Reads ----------

  @doc """
  Each controller's readiness verdict and every precondition behind it, one
  row per controller. Each also answers the recovery state and
  persistent-registry row count of each namespace running there that the
  caller may see (`Flezha.AuthZ.Namespace.visible/3`). The verb is public, so a
  caller with no identity gets the verdicts and only `_system`'s rows. Answers
  at once; `boot_await/2` waits, for this controller.
  """
  @spec boot_status(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb boot_status(caller_context) do
    %{rows: rows} = answer = Reads.rows(Reads.controllers(), __MODULE__, :local_boot_status, [])

    visible =
      rows
      |> Enum.flat_map(fn row -> Enum.map(row.namespaces, & &1.namespace) end)
      |> Namespace.visible(caller_context)

    rows =
      Enum.map(rows, fn row ->
        %{row | namespaces: Enum.filter(row.namespaces, &(&1.namespace in visible))}
      end)

    {:ok, %{answer | rows: rows}}
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
  Each controller's status, one row per controller: node, clock, shutdown and
  durability. In full: its node and its monotonic clock, its shutdown state and
  settings, its durability, its state root and the folder file plugins may use.

  `monotonic_us` is the node's `System.monotonic_time(:microsecond)`, the clock
  observability signals are stamped with.
  """
  @spec status(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb status(caller_context), verb: :controller_status do
    {:ok, Reads.rows(Reads.controllers(), __MODULE__, :local_status, [])}
  end

  @doc "One snapshot of each controller's memory hot spots, one row per controller."
  @spec mem_top(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb mem_top(caller_context), verb: :controller_mem_top do
    {:ok, Reads.rows(Reads.controllers(), MemTop, :snapshot, [])}
  end

  @doc """
  The cluster as one namespace sees it: nodes, relays and health. Each of the
  hub's nodes comes with its relay count and recovery state, beside the
  unplaced relays and the relays' health, each relay's as the controller
  hosting it judges it. `answered_by` names the
  controller that answered; the cluster elects no leader. `unreachable` names
  the controllers that did not answer.
  """
  @spec cluster_status(String.t(), caller_context()) :: {:ok, map()} | {:error, term()}
  verb cluster_status(namespace, caller_context),
    args: [namespace: [type: :string, doc: "The namespace."]] do
    namespace = Namespace.or_default(namespace)
    nodes = namespace |> Directory.hub_nodes() |> Enum.sort()
    %{rows: rows, unreachable: rows_unreachable} = Info.rows(namespace)
    counts = Enum.frequencies_by(rows, & &1.node)

    %{answers: recovery, unreachable: recovery_unreachable} =
      Reads.ask(nodes, __MODULE__, :local_recovery_snapshot, [Identificator.hub(namespace)])

    recovery = Map.new(recovery)

    {:ok,
     %{
       namespace: namespace,
       answered_by: node(),
       nodes:
         Enum.map(nodes, fn n ->
           %{
             node: n,
             relay_count: Map.get(counts, n, 0),
             recovery_state: Map.get(recovery, n, :unknown)
           }
         end),
       unplaced_relays: Map.get(counts, nil, 0),
       relay_health: Enum.frequencies_by(rows, & &1.health),
       unreachable: Enum.uniq(rows_unreachable ++ recovery_unreachable)
     }}
  end

  @doc """
  Every controller the cluster directory lists, the one that answered first.
  Each row gives its node, its `host` address, the machine it runs on
  (`host_node`, `nil` while `FLEZHA_HOST_NODE` is unset), its `version`,
  `uptime_ms`, how many `runners` are connected to it, and `local?` for the one
  that answered. A controller that does not answer in time is listed with its
  node and `reachable?: false`, and named in `unreachable`.
  """
  @spec controllers(caller_context()) :: {:ok, map()} | {:error, term()}
  verb controllers(caller_context), verb: :controller_status do
    nodes = Reads.controllers()

    %{answers: answers, unreachable: unreachable} =
      Reads.ask(nodes, __MODULE__, :local_controller, [])

    facts = Map.new(answers)

    {:ok,
     %{
       rows:
         Enum.map(nodes, fn n ->
           case Map.fetch(facts, n) do
             :error -> %{node: n, reachable?: false, local?: false}
             {:ok, row} -> Map.merge(row, %{reachable?: true, local?: n == node()})
           end
         end),
       unreachable: unreachable
     }}
  end

  @doc """
  The durability breaches every controller recorded since it started, each
  with the `node` that recorded it.
  """
  @spec durability_breaches(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb durability_breaches(caller_context), verb: :controller_status do
    {:ok, Reads.rows(Reads.controllers(), DurabilityTier, :breaches, [])}
  end

  @doc "Every controller's durability shipper status, each row with its `node`."
  @spec durability_status(caller_context()) :: {:ok, Reads.rows()} | {:error, term()}
  verb durability_status(caller_context), verb: :controller_status do
    {:ok, Reads.rows(Reads.controllers(), DurabilityTier, :status, [])}
  end

  @doc """
  Every data-plane liveness counter the cluster's controllers hold. Each row
  carries the `node` that holds it and a relay row its `namespace`;
  `unreachable` names the controllers that did not answer.

  `opts` takes `filter` with one scope, each controller reading only its keys:
  `relays: [{namespace, relay_id}]` (at most 500) for those relays, or
  `namespace: ns` for every relay of that namespace, each answered with the
  channel counters of the runners those relays are bound to. `limit: 0`
  answers no rows, only `totals`: each counted relay slot summed over the
  relays in scope on every controller.
  """
  @spec liveness_counters(keyword(), caller_context()) :: {:ok, term()} | {:error, term()}
  verb liveness_counters(opts \\ [], caller_context),
    verb: :controller_status,
    args: [opts: Page.arg()] do
    with {:ok, only, answer} <- liveness_keys(opts),
         do: {:ok, LivenessCounters.snapshot_cluster(only, answer)}
  end

  defp liveness_keys(opts) do
    declared = [
      sorts: [relay: :asc],
      filters: [:relays, :namespace],
      keys: [:filter, :limit],
      limit: {nil, 0}
    ]

    case Page.parse(opts, :liveness_counters, declared) do
      {:ok, nil} ->
        {:ok, nil, :rows}

      {:ok, %Page{filter: filter, limit: limit}} ->
        answer = if limit == 0, do: :totals, else: :rows
        with {:ok, only} <- liveness_scope(filter, answer), do: {:ok, only, answer}

      refused ->
        refused
    end
  end

  defp liveness_scope(%{relays: _, namespace: _} = filter, _answer),
    do: Page.invalid("filter", Map.to_list(filter))

  defp liveness_scope(%{namespace: namespace}, answer) when is_binary(namespace) do
    runners = bound_runners(namespace, {HubManager, :relay_rows, [namespace]}, answer)
    {:ok, {{:namespace, namespace}, runners}}
  end

  defp liveness_scope(%{namespace: written}, _answer),
    do: Page.invalid("filter.namespace", written)

  defp liveness_scope(%{relays: written}, answer) do
    with {:ok, relays} <- named_relays(written) do
      runners =
        for {namespace, ids} <- Enum.group_by(relays, &elem(&1, 0), &elem(&1, 1)),
            runner <-
              bound_runners(namespace, {HubManager, :relay_rows_of, [namespace, ids]}, answer),
            uniq: true,
            do: runner

      {:ok, {Enum.uniq(relays), runners}}
    end
  end

  defp liveness_scope(_filter, _answer), do: {:ok, nil}

  # Read from the namespace's hub, which knows each relay's runner; a totals
  # read answers no channel rows, so it skips the read.
  defp bound_runners(_namespace, _call, :totals), do: []

  defp bound_runners(namespace, call, :rows) do
    for row <- elem(Info.hub_read(namespace, call), 0),
        row.runner != nil,
        uniq: true,
        do: row.runner
  end

  defp named_relays(written) do
    with {:ok, written} <- Page.bounded(written, "filter.relays") do
      pairs = Enum.map(written, &pair/1)

      if Enum.all?(pairs, &is_tuple/1),
        do: {:ok, pairs},
        else: Page.invalid("filter.relays", written)
    end
  end

  defp pair({namespace, relay_id}) when is_binary(namespace) and is_binary(relay_id),
    do: {namespace, relay_id}

  defp pair([namespace, relay_id]), do: pair({namespace, relay_id})
  defp pair(_other), do: nil

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
  # The local half of `boot_status/1`: every namespace running here.
  @spec local_boot_status() :: map()
  def local_boot_status do
    {verdict, rows} = Readiness.verdict()

    namespaces =
      for namespace <- HubManager.get_running_namespaces() do
        hub_id = Identificator.hub(namespace)

        %{
          namespace: namespace,
          recovery_state: safe(fn -> Recovery.recovery_state(hub_id) end, :unknown),
          registry_rows: safe(fn -> hub_id |> ProcessHub.registry_dump() |> Enum.count() end, 0)
        }
      end

    %{readiness: verdict, preconditions: rows, namespaces: namespaces}
  end

  @doc false
  # The local half of `status/1`.
  @spec local_status() :: map()
  def local_status do
    shutdown_snapshot()
    |> Map.merge(durability_snapshot())
    |> Map.merge(%{
      node: node(),
      monotonic_us: System.monotonic_time(:microsecond),
      state_root: Paths.state_root(),
      file_plugin_root: Storage.Config.shown_root()
    })
  end

  @doc false
  # The local half of `cluster_status/2`'s recovery read, called by name on
  # every hub node.
  @spec local_recovery_snapshot(atom()) :: atom()
  def local_recovery_snapshot(hub_id) when is_atom(hub_id) do
    case HubManager.recovery_state(hub_id) do
      {:ok, state} -> state
      {:error, :unknown_hub} -> :unknown
    end
  end

  @doc false
  # The local half of `controllers/1`.
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
