defmodule Flezha.Service.HubManager.Lifecycle do
  @moduledoc """
  What a hub does around its own start and stop: the post-start hook wiring,
  the aliveness wait, the drain, and the registry directory a stopped hub
  leaves behind.
  """

  alias Flezha.Boot.RecoveryHooks
  alias Flezha.Cluster.Directory
  alias Flezha.Convergence.Snapshot
  alias Flezha.Service.HubManager.Rows
  alias Flezha.Shutdown.Coordinator
  alias ProcessHub.Service.Migration
  alias ProcessHub.Service.Recovery, as: HubRecovery

  # Default ceiling on one `drain_hub/2`. The graceful-shutdown path passes its
  # own per-hub slice of the shutdown budget instead.
  @drain_budget_ms 60_000

  # A force pass fires AT the drain deadline and its handover is async, so this
  # window is carved out of the drain timeout to let it land.
  @forced_handover_grace_ms 2_000

  def on_hub_started(hub_id, pid, namespace) do
    wait_until_hub_alive(hub_id)
    # Advertise placement cluster-wide; membership dies with the hub pid,
    # so stop/crash/nodedown need no leave call.
    Directory.join_hub(pid, namespace)
    _ = RecoveryHooks.register(hub_id)
    # Seed the snapshot cache with the coordinator's current state. The
    # recovery telemetry fired during recovery overwrites this entry as
    # the lifecycle progresses; the seed only matters when a hub boots
    # straight into `:normal` (auto-recovery off) and no telemetry event
    # will follow.
    _ = Snapshot.observe_recovery(hub_id, HubRecovery.recovery_state(hub_id))
  end

  @doc """
  Last known recovery state for `hub_id`. Returns
  `{:ok, :recovery_pending | :recovering | :normal | :skipped | :timeout}`,
  or `{:error, :unknown_hub}` if the hub is not registered with this
  supervisor.
  """
  @spec recovery_state(atom()) :: {:ok, atom()} | {:error, :unknown_hub}
  def recovery_state(hub_id) when is_atom(hub_id) do
    cond do
      not ProcessHub.is_alive?(hub_id) ->
        {:error, :unknown_hub}

      true ->
        case Snapshot.recovery_state(hub_id) do
          :unknown -> {:ok, HubRecovery.recovery_state(hub_id)}
          state -> {:ok, state}
        end
    end
  end

  # Supervisor.start_child returns once the hub's top supervisor is up,
  # but ProcessHub registers its named coordinator GenServer asynchronously
  # afterwards. Anything calling into the named hub_id (register_handler,
  # register_hook_handlers, alter_child_data callbacks) before that
  # registration completes raises :noproc and cascades the FleetManager
  # crash. Spin briefly until ProcessHub.is_alive? confirms the name is
  # claimed.
  def wait_until_hub_alive(hub_id, deadline_ms \\ 5_000) do
    deadline = System.monotonic_time(:millisecond) + deadline_ms
    do_wait_until_hub_alive(hub_id, deadline)
  end

  defp do_wait_until_hub_alive(hub_id, deadline) do
    cond do
      ProcessHub.is_alive?(hub_id) ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :timeout

      true ->
        Process.sleep(10)
        do_wait_until_hub_alive(hub_id, deadline)
    end
  end

  @doc """
  Migrate every locally-hosted operative of `namespace` to a peer controller
  through the migration-consent gate (live HotSwap handover), then wait for the
  local children to actually be gone.

  The wait is load-bearing: `Migration.drain/2` returns once the deferred list
  is empty, but the migration start requests are async and HotSwap stops a
  local child only AFTER the remote start succeeds. Locally-empty is the
  completion signal — acting on `drain/2`'s return alone kills mid-flight
  handovers. `:budget_ms` bounds the whole call; `:handover_grace_ms` is carved
  off it so the deadline force pass can land before the caller moves on.

  The one drain seam: graceful shutdown and the last-runner migration both
  reach ProcessHub through here.
  """
  @spec drain_hub(binary(), keyword()) :: {:ok, map()} | {:error, term()} | :deadline
  def drain_hub(namespace, opts \\ []) do
    budget = Keyword.get(opts, :budget_ms, @drain_budget_ms)
    grace = Keyword.get(opts, :handover_grace_ms, @forced_handover_grace_ms)
    deadline = System.monotonic_time(:millisecond) + budget

    Rows.with_live_hub(namespace, {:error, :no_target_nodes}, fn hub_id ->
      result = Migration.drain(hub_id, timeout: max(budget - grace, 1_000))
      with {:ok, _} <- result, do: await_local_children_gone(hub_id, deadline)
      result
    end)
  catch
    kind, reason -> {:error, {kind, reason}}
  end

  defp await_local_children_gone(hub_id, deadline) do
    cond do
      ProcessHub.process_list(hub_id, :local) == [] ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        :deadline

      true ->
        Process.sleep(100)
        await_local_children_gone(hub_id, deadline)
    end
  catch
    _kind, _reason -> :ok
  end

  def wipe_registry_dir(hub_id) do
    root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")
    File.rm_rf(Path.join(root, Atom.to_string(hub_id)))
  end

  @doc false
  def registry_backend_config(hub_id) do
    # When auto-restore is disabled (test mode + any operator who flips
    # the kill-switch), there is no point persisting the registry to
    # disk — the DETS file would only become test-isolation noise. ETS
    # gives us identical in-memory semantics with no on-disk residue.
    if Rows.durable_starts?() do
      # `:durable_ets` serves reads from ETS and mirrors writes to disk
      # synchronously, so the reconcile's per-round registry reads stay
      # in-memory while the file remains the record a cold cluster
      # recovers from. Same on-disk format as `:dets`.
      {:durable_ets, [path: registry_path(hub_id)]}
    else
      :ets
    end
  end

  @doc false
  def migration_strategy do
    %ProcessHub.Strategy.Migration.HotSwap{
      handover: true,
      consent_settings: consent_settings()
    }
  end

  # `max_defer_ms` nil = derive from the shutdown budget, so a deferring
  # operative can never outlive the graceful-shutdown window.
  @doc false
  def consent_settings do
    opts = Application.get_env(:flezha, :migration_consent, [])

    %ProcessHub.Strategy.Migration.MigrationConsent{
      consent_timeout: Keyword.get(opts, :consent_timeout_ms, 5_000),
      retry_interval: Keyword.get(opts, :retry_interval_ms, 2_000),
      max_defer_time: Keyword.get(opts, :max_defer_ms) || Coordinator.configured_timeout_ms()
    }
  end

  defp registry_path(hub_id) do
    root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")
    dir = Path.join(root, Atom.to_string(hub_id))
    File.mkdir_p!(dir)
    Path.join(dir, "registry.dets")
  end
end
