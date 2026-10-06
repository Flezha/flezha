defmodule Flezha.Service.HubManager do
  @moduledoc """
  The supervisor every hub starts under, and the RPC surface `flz` and the
  controller call. The bodies live in `Flezha.Service.HubManager.*`; what stays
  here is what names this module's own supervisor identity.
  """

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Cluster.GroupedHashing
  alias Flezha.Config.Durations
  alias Flezha.Convergence.RecoveryGate
  alias Flezha.Convergence.Snapshot
  alias Flezha.Identificator
  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Process.Hub
  alias Flezha.Service.HubManager.Lifecycle
  alias Flezha.Service.HubManager.Placement
  alias Flezha.Service.HubManager.Rows
  alias ProcessHub.Constant.Hook
  alias ProcessHub.Service.HookManager

  use Supervisor

  defdelegate alter_child_data(arg1, arg2), to: Placement
  defdelegate attempt_start(arg1, arg2), to: Lifecycle
  defdelegate auto_recovery_config(), to: Lifecycle
  defdelegate await_cluster_gate(), to: Lifecycle
  defdelegate ensure_hub(arg1), to: Lifecycle
  defdelegate consent_settings(), to: Lifecycle
  defdelegate cspec_partition(arg1), to: Rows
  defdelegate declared_relays(arg1, arg2 \\ []), to: Rows
  defdelegate drain_hub(arg1, arg2), to: Lifecycle
  defdelegate durable_starts?(), to: Rows
  defdelegate get_all_relays(arg1), to: Rows
  defdelegate get_hub_nodes(arg1, arg2 \\ false), to: Rows
  defdelegate get_pid(arg1, arg2), to: Rows
  defdelegate get_registry(arg1), to: Rows
  defdelegate hook_registry_written(arg1, arg2), to: Placement
  defdelegate hub_alive?(arg1), to: Rows
  defdelegate live_node(arg1), to: Rows
  defdelegate live_pid(arg1), to: Rows
  defdelegate migration_strategy(), to: Lifecycle
  defdelegate relay_bindings(arg1), to: Rows
  defdelegate declared_bindings(arg1), to: Rows
  defdelegate relay_bindings(arg1, arg2), to: Rows
  defdelegate plugin_entries(arg1), to: Rows
  defdelegate plugin_entry(arg1, arg2), to: Rows
  defdelegate relay_rows(arg1), to: Rows
  defdelegate relay_spec(arg1, arg2), to: Rows
  defdelegate relay_runner_tag(arg1, arg2), to: Placement
  defdelegate relay_set_runner(arg1, arg2, arg3), to: Placement
  defdelegate partition_members(arg1, arg2), to: Placement
  defdelegate recovery_state(arg1), to: Lifecycle
  defdelegate registered_relays?(arg1), to: Rows
  defdelegate registry_dump(arg1), to: Rows
  defdelegate registry_backend_config(arg1), to: Lifecycle
  defdelegate rewrite_cspec_plugins(arg1, arg2), to: Rows
  defdelegate runner_relays(arg1, arg2), to: Placement
  defdelegate with_live_hub_id(arg1, arg2, arg3), to: Rows

  # Bounds one whole start request: WorkerQueue kills it at this deadline and
  # `Stream.run/1` discards the exit, so an overrun silently drops the batch's
  # registry update. Every wait on the child-start path must fit inside it.

  # Deliberately off the `<namespace>_hub` convention: `Identificator.hub_id?/1` filters it
  # out, so every relay-oriented walker (Convergence, RecoveryGate,
  # ShutdownCoordinator, the CLI, metrics) ignores it.
  @schedule_hub_id :flz_schedules

  def start_link(_) do
    Supervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  def stop_hub_manager() do
    Supervisor.stop(__MODULE__)
  end

  def start_hub(namespace) do
    hub_id = Identificator.hub(namespace)

    case Supervisor.start_child(__MODULE__, process_hub(namespace)) do
      {:ok, pid} = ok ->
        Lifecycle.on_hub_started(hub_id, pid, namespace)
        ok

      {:ok, pid, _info} = ok ->
        Lifecycle.on_hub_started(hub_id, pid, namespace)
        ok

      other ->
        other
    end
  end

  @doc """
  Block until every per-namespace hub reaches a terminal recovery state
  (`:normal | :skipped`) or `timeout_ms` (default 60s) elapses.
  Returns `:ok` on success or
  `{:error, {:recovery_timeout, [hub_id]}}` carrying the offending
  hub_ids that did not finish.
  """
  @spec await_recovery_complete(non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_recovery_complete(timeout_ms \\ Durations.get(:hub_await_recovery_ms))
      when is_integer(timeout_ms) and timeout_ms >= 0 do
    RecoveryGate.await_recovery_complete(nil, timeout_ms)
  end

  @doc """
  Wait for the listed hubs to reach `:normal | :skipped`, with a custom
  timeout. Returns the same shape as `await_recovery_complete/1`.
  """
  @spec await_recovery_complete([atom()], non_neg_integer()) ::
          :ok | {:error, {:recovery_timeout, [atom()]}}
  def await_recovery_complete(hub_ids, timeout_ms)
      when is_list(hub_ids) and is_integer(timeout_ms) do
    RecoveryGate.await_recovery_complete(hub_ids, timeout_ms)
  end

  def stop_hub(namespace) do
    hub_id = Identificator.hub(namespace)
    Supervisor.terminate_child(__MODULE__, hub_id)
    Supervisor.delete_child(__MODULE__, hub_id)

    # `stop_hub/1` is the explicit "tear this namespace down" path
    # (user-initiated, or last-runner cleanup deciding the
    # namespace has gone away). Clean the persistent registry so a
    # later `start_hub/1` for the same namespace starts empty rather
    # than resurrecting children that were intentionally torn down.
    # Cold-boot recovery is unaffected — that path never calls
    # `stop_hub/1`.
    _ = Lifecycle.wipe_registry_dir(hub_id)
    :ok
  end

  @doc "Hub id of the cron-schedule hub."
  @spec schedule_hub_id() :: atom()
  def schedule_hub_id(), do: @schedule_hub_id

  @doc """
  Start the cron-schedule hub. Idempotent: an already-started hub answers
  `{:ok, pid}` too, so a restarted owner just re-asserts.
  """
  @spec start_schedule_hub() :: {:ok, pid()} | {:error, term()}
  def start_schedule_hub() do
    case Supervisor.start_child(__MODULE__, schedule_hub()) do
      {:ok, pid} -> schedule_hub_started(pid)
      {:ok, pid, _info} -> schedule_hub_started(pid)
      {:error, {:already_started, pid}} -> schedule_hub_started(pid)
      other -> other
    end
  end

  defp schedule_hub_started(pid) do
    Lifecycle.wait_until_hub_alive(@schedule_hub_id)
    {:ok, pid}
  end

  @doc """
  Stop the cron-schedule hub. No registry wipe: its backend is `:ets`, so it
  leaves nothing on disk.
  """
  @spec stop_schedule_hub() :: :ok
  def stop_schedule_hub() do
    Supervisor.terminate_child(__MODULE__, @schedule_hub_id)
    Supervisor.delete_child(__MODULE__, @schedule_hub_id)
    :ok
  end

  @doc """
  Reclaim a namespace hub on THIS node, but only when the namespace is neither
  declared here nor holding any of this node's relays.

  Tearing down a hub that still holds durably-suspended relays would kill
  their Overwatches and wipe the durable registry, stranding a parked fleet
  across a runner restart — so runner cleanup and the loop's vacancy reclaim go
  through this guard. A peer's relays are that peer's business: the reclaim
  is node-local, which is what lets a controller whose fleet does not serve a
  namespace leave that namespace's cluster while the relays the hub moved
  off it keep running elsewhere.

  **What it was declared to hold counts too, and so does a recovery that has
  not settled.** The reclaim runs on every convergence tick over each hub with
  no active runners, and `stop_hub/1` wipes the registry directory. Boot-time
  creation is what makes that reachable: it puts hubs exactly where runners are
  not.

  Two windows, and a live-placement read is blind in both. A hub does not load
  its durable registry into the live one at start — the rows sit on disk until
  the first reconcile round runs, and every live read answers "empty" until
  then, so an unsettled recovery is what says "wait".
  After the reconcile the rows are back but nothing has placed them, because
  placing a relay needs a runner and there is none; the declared set is
  what says "this node was told to hold these", whether or not anything
  currently runs them.

  A settled hub declared to hold nothing IS vacant and is still reclaimed, so
  the rule does not pin every hub the moment auto-restore writes it a registry
  directory, and a node whose fleet does not serve a namespace still leaves
  that namespace's cluster.

  The explicit `stop_hub/1` teardown (user `stop_namespace`, the shutdown
  plan's namespace step) is intentionally unguarded.
  """
  def stop_hub_if_vacant(namespace) do
    unless holds_anything?(namespace), do: stop_hub(namespace)
    :ok
  end

  defp holds_anything?(namespace) do
    Rows.local_relays?(namespace) or
      Rows.declares_anything?(namespace) or
      unsettled?(namespace)
  end

  # `:timeout` counts as unsettled: a hub that gave up may still hold rows
  # nothing has replayed.
  defp unsettled?(namespace) do
    case Lifecycle.recovery_state(Identificator.hub(namespace)) do
      {:ok, state} -> RecoveryGate.recovering?(state)
      {:error, :unknown_hub} -> false
    end
  end

  def get_running_namespaces() do
    list_running_hub_ids()
    |> Enum.map(&Identificator.namespace_from_hub/1)
    |> Enum.reject(&is_nil/1)
  end

  @doc """
  All hub_ids currently registered with this supervisor that have a live
  coordinator. Filters non-hub children by the
  `Identificator.hub/1`-shaped atom suffix and gating on
  `ProcessHub.is_alive?/1`. Used by `RecoveryGate.await_recovery_complete/2`
  when the caller does not pass an explicit hub-id list.
  """
  @spec list_running_hub_ids() :: [atom()]
  def list_running_hub_ids() do
    Supervisor.which_children(__MODULE__)
    |> Enum.map(fn {id, _pid, _type, _modules} -> id end)
    |> Enum.filter(&Identificator.hub_id?/1)
    |> Enum.filter(fn id -> Hub.alive?(id) end)
  end

  @impl true
  def init(_init_arg) do
    # Dynamic supervisor for every hub: the per-namespace ones plus the
    # schedule hub. Recovery-state caching moved to
    # `Flezha.Convergence.Snapshot` (started earlier in the application tree).
    Supervisor.init([], strategy: :one_for_one)
  end

  defp process_hub(namespace) do
    hub_id = Identificator.hub(namespace)

    ProcessHub.child_spec(%ProcessHub{
      hub_id: hub_id,
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
      # A relay's group is its factory id, and a controller serves the
      # factories its fleet can run, so a relay is only ever assigned to a
      # controller that can run it. A relay with no factory has no group
      # and is hashed over every controller in the hub.
      distribution_strategy: %GroupedHashing{},
      migration_strategy: Lifecycle.migration_strategy(),
      registry_backend: Lifecycle.registry_backend_config(hub_id),
      auto_recovery: Lifecycle.auto_recovery_config(),
      # Relay startup involves multiple GenServer hops and socket communication
      # to the compute engine (Rust), so the default 5s is too tight.
      cross_node_request_timeout: Durations.get(:hub_cross_node_request_ms),
      hooks:
        Map.merge(
          %{
            Hook.child_data_alter() => [
              %HookManager{
                id: Hook.child_data_alter(),
                m: __MODULE__,
                f: :alter_child_data,
                a: [hub_id, :_]
              }
            ],
            Hook.child_registered() => [
              %HookManager{
                id: :flezha_registry_written,
                m: __MODULE__,
                f: :hook_registry_written,
                a: [hub_id, :_]
              }
            ]
          },
          Snapshot.hook_handlers(hub_id)
        )
    })
  end

  # Transient uniqueness arbiter for cron schedules: `Singularity` gives one
  # schedule process cluster-wide per name. Durability belongs to
  # `Flezha.Schedule.Store`, so the registry is `:ets` and `auto_recovery` is
  # off — nothing lands under `:process_hub_registry_root`, which keeps
  # `unstarted_hub_has_registry?/0` (and thus `/healthz`) clean.
  #
  # No `child_data_alter` hook: a schedule's start args are not a relay's,
  # so the hook would log every schedule start as unrecognised. No
  # `child_registered` hook either — schedules have no runner binding to
  # reassert.
  #
  # `ColdSwap` over the namespace hubs' `HotSwap`: HotSwap keeps the old
  # process alive until the new one registers, which for a cron timer means
  # two armed timers for one schedule. Terminate-then-start makes "at most one
  # armed timer cluster-wide" structural; the resulting gap is what the job's
  # misfire-grace look-back on init repairs.
  defp schedule_hub() do
    ProcessHub.child_spec(%ProcessHub{
      hub_id: @schedule_hub_id,
      synchronization_strategy: %ProcessHub.Strategy.Synchronization.PubSub{},
      redundancy_strategy: %ProcessHub.Strategy.Redundancy.Singularity{},
      partition_tolerance_strategy: %ProcessHub.Strategy.PartitionTolerance.Divergence{},
      distribution_strategy: %ProcessHub.Strategy.Distribution.ConsistentHashing{},
      migration_strategy: %ProcessHub.Strategy.Migration.ColdSwap{},
      registry_backend: :ets,
      auto_recovery: false,
      hooks: %{}
    })
  end

  @doc """
  True when auto-restore is enabled and the durable registry holds relays
  this (re)started controller has not loaded back into live, bound
  processes yet — closing the post-restart "vacuously healthy" hole.
  Covers both an on-disk `registry.dets` for a hub that has not started
  (`unstarted_hub_has_registry?/0`) and a running hub whose replayed DETS
  rows outnumber the live-bound relays (`running_hub_relays_unbound?/0`).
  Used by `/healthz` to report `recovering` instead of `healthy` until the
  registry is fully reloaded and bound.
  """
  @spec durable_registry_pending?() :: boolean()
  def durable_registry_pending?() do
    Rows.durable_starts?() and
      (running_hub_relays_unbound?() or unstarted_hub_has_registry?())
  end

  # Running hub whose durable registry declares more relays than are
  # currently bound to a live pid — relays replayed from DETS but not yet
  # reseated onto a reconnected runner.
  defp running_hub_relays_unbound?() do
    Enum.any?(list_running_hub_ids(), fn hub_id ->
      namespace = Identificator.namespace_from_hub(hub_id)

      namespace != nil and
        Rows.durable_relay_count(hub_id) > length(Rows.get_all_relays(namespace))
    end)
  end

  @doc """
  Relay ids present in the durable registries of running hubs — durable
  evidence of intent consumed by the convergence intent-mismatch check.
  """
  @spec durable_relay_ids() :: MapSet.t()
  def durable_relay_ids() do
    list_running_hub_ids()
    |> Enum.flat_map(&Rows.durable_relay_keys/1)
    |> MapSet.new()
  end

  @doc """
  Every namespace this node already holds evidence for, from two sources and
  no namespace name written into the derivation: a registry directory under
  the process-hub root, which is durable state it must recover, and an entry
  in the durable namespace catalog, which is user intent.

  The auto-restore kill-switch governs it. A node told not to bring things
  back by itself has an empty declared set, which is what makes both readers —
  the boot-time starter and the readiness row — agree without either of them
  reading the switch again.

  The catalog read is soft by construction: `NamespaceStore.list/0` answers
  `[]` when its cache table is absent, which is the deployment running with
  the authz supervisor out of the application tree.
  """
  @spec declared_namespaces() :: [binary()]
  def declared_namespaces() do
    if Rows.durable_starts?() do
      registry_hub_dirs()
      |> Enum.map(&Identificator.namespace_from_hub/1)
      |> Enum.concat(Enum.map(NamespaceStore.list(), & &1.name))
      |> Enum.uniq()
    else
      []
    end
  end

  # On-disk registry for a hub that has not started yet.
  defp unstarted_hub_has_registry?() do
    running = MapSet.new(list_running_hub_ids(), &Atom.to_string/1)

    Enum.any?(registry_hub_dirs(), &(not MapSet.member?(running, &1)))
  end

  # Every hub directory under the process-hub root that holds a registry file.
  # The one walk of that root: the declared set and the unstarted-hub
  # predicate are two readings of this list. Directory names, never atoms — a
  # directory nobody created through `Identificator.hub/1` must not reach the
  # atom table just by existing there.
  defp registry_hub_dirs() do
    case File.ls(Layout.dir(:process_hub)) do
      {:ok, entries} ->
        Enum.filter(entries, fn entry ->
          Identificator.hub_id?(entry) and
            File.regular?(Layout.file(:process_hub, [entry, Layout.hub_registry_file()]))
        end)

      _ ->
        []
    end
  end
end
