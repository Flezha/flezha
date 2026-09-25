defmodule Flezha.State.DurabilityTier do
  @moduledoc """
  Ships flezha's durable artifacts to a remote backend so a controller that dies
  **with its disk** is recoverable. See
  `docs/operations/remote_durability_tier.md`.

  Local files stay authoritative: every ack path gates on local fsync alone,
  shipping neither mutates nor deletes one, and turning the tier off is a
  complete rollback. What ships are immutable artifacts at boundaries that
  already exist: snapshots, WAL record bundles, task-journal suffixes, the
  durable input log, the registry cspec, parked DLQ records, the control-plane
  registries. The per-cell manifest is written last, so a torn upload is
  invisible to recovery.

  Every trigger, recovery and status function is a no-op while `backend: :none`,
  under which `children/0` starts nothing. An unset or unrecognised
  `FLEZHA_DURABILITY_BACKEND` takes that default and `children/0` says so;
  a backend the user did select and that lacks an option it cannot work
  without refuses the boot instead.
  """

  require Logger

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Process.Cspec
  alias Flezha.State.DurabilityTier.{Artifact, Hydrator, Shipper}
  alias Flezha.State.DurabilityTier.Backend.Config, as: BackendConfig
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver

  # Bounded by the slowest plausible ship, not tuned: both backends time out at
  # 30 s and the retry ladder caps at 30 s, and a failed manifest write
  # re-uploads its blobs — refreshing their age — so the real gap between an
  # upload and the entry naming it is minutes at worst. An hour is the headroom.

  # ---------- configuration ----------

  @doc "Whether a remote backend is configured. `false` makes every entry point inert."
  @spec enabled?() :: boolean()
  def enabled?(), do: BackendConfig.resolve() != :none

  @doc """
  How this controller's durable state is protected, as a user reads it.

  `:local_only` means the disk under this node is the only copy — a deliberate,
  supported posture, and one a fleet audit must be able to find, so it is
  surfaced on `/healthz` and `flz controller status` rather than only inferable
  from the absence of a backend.
  """
  @spec posture() :: :remote_tier | :local_only
  def posture(), do: if(enabled?(), do: :remote_tier, else: :local_only)

  @doc """
  Whether the deployment has made no durability decision at all.

  `config/runtime.exs` resolves an unset `FLEZHA_DURABILITY_BACKEND` to
  `:unset` in production only; every other environment keeps defaulting to
  `:none`, so the development loop is untouched. The posture is the same
  either way, because `BackendConfig.resolve/0` answers `:none` for both, and
  this is what separates a decision nobody made from one somebody made.
  """
  @spec unconfigured?() :: boolean()
  def unconfigured?(), do: configured_backend() == :unset

  @doc """
  The backend as a user named it, rather than as `Backend.resolve/0`
  resolves it — what `flz controller status` reports.
  """
  @spec configured_backend() :: atom() | binary() | {module(), keyword()}
  def configured_backend(),
    do: Config.get(:durability_backend)

  @doc """
  This controller's identity as a writer of remote lineages.

  Must be STABLE across restarts of the same controller — a restarted node is
  still itself and must extend its own lineages rather than bid against itself —
  and must NOT survive the loss of its disk, because a node that lost its state
  genuinely is a new claimant.

  `node()` satisfies both under a StatefulSet or a fixed host. Under a
  Deployment the pod name changes on every reschedule, which would make a
  rescheduled controller take over its own references one claim at a time;
  `FLEZHA_DURABILITY_WRITER_ID` is the override for that, and it is operational
  configuration rather than a test knob.
  """
  @spec writer_id() :: binary()
  def writer_id() do
    case Config.get(:durability_writer_id) do
      value when is_binary(value) and value != "" -> value
      value -> to_string(value)
    end
  end

  @doc """
  This node now runs `state_key`'s operator, so it may take over that
  reference's remote lineage.

  Shipping alone must never be enough to claim one: a migration leaves the
  source node holding a full copy of the state (`WalTransfer.pull_operator/3`
  copies, and nothing deletes the source), so its backstop sweep would
  otherwise bid the lineage back and fence the node that actually runs the
  operator now.
  """
  @spec adopt_operator(binary()) :: :ok
  def adopt_operator(state_key) when is_binary(state_key) do
    if enabled?() do
      case GenServer.whereis(Shipper) do
        nil -> :ok
        pid -> GenServer.cast(pid, {:adopt, state_key})
      end
    end

    :ok
  end

  @doc "Backstop sweep period — the bound on how stale a quiet cell's remote copy gets."
  @spec ship_interval_ms() :: pos_integer()
  def ship_interval_ms(),
    do: positive(:durability_ship_interval_ms)

  @doc "Time since the last successful ship at which the RPO alarm fires."
  @spec rpo_target_ms() :: pos_integer()
  def rpo_target_ms(), do: positive(:durability_rpo_target_ms)

  @doc "Orphan-blob collection period."
  @spec gc_interval_ms() :: pos_integer()
  def gc_interval_ms(), do: positive(:durability_gc_interval_ms)

  @doc """
  Age below which an unreferenced artifact is never collected, whoever owns it.

  This is the half of collection safety that does not depend on knowing whose
  artifact it is: an upload still in flight is by definition young, so a peer
  controller mid-ship is covered without any coordination between the two.
  """
  @spec gc_grace_ms() :: pos_integer()
  def gc_grace_ms(), do: positive(:durability_gc_grace_ms)

  @doc """
  Age at which an unreferenced artifact is collected even though no node holds
  local state for its reference — a stopped operator, or one whose state moved.

  Without it, scoping collection to what a node owns would leak those forever.
  Far longer than `gc_grace_ms/0` on purpose: it is the threshold past which no
  ship can plausibly still be in flight, so it must not be the same number, or
  the scoping would contribute nothing.
  """
  @spec gc_abandoned_after_ms() :: pos_integer()
  def gc_abandoned_after_ms(),
    do: positive(:durability_gc_abandoned_after_ms)

  # A user who sets one of these to zero or to something that is not a
  # count is asking for no sweep at all, which the tier has no meaning for; the
  # row's own value is what it falls back to.
  defp positive(name) do
    case Durations.get(name) do
      value when is_integer(value) and value > 0 -> value
      _ -> Durations.default(name)
    end
  end

  @doc """
  Supervision children for `Flezha.Application`: the backend connection, the
  boot-time hydration of the control-plane stores and the registry cspec, and
  the shipper — or `[]` when the tier is off, which is what makes an explicit
  `none` byte-identical to a build without the tier.

  Order is load-bearing. The hydration is a one-shot child that does its work in
  `start_link` and returns `:ignore`, so it completes before the supervisor
  starts anything after it — and it must be placed ahead of
  `Flezha.Service.HubManager`, because ProcessHub opens `registry.dets` at hub
  start and a hydration afterwards would be invisible to the orphan reconcile.
  The same placement puts the authz stores on disk before
  `Flezha.AuthZ.Supervisor` opens them and before the transport accepts a
  connection, so a resurrected controller enforces from its first accepted verb.
  """
  @spec children() :: [Supervisor.child_spec() | module() | {module(), term()}]
  def children() do
    resolved = BackendConfig.resolve()
    announce_selection(resolved)

    case resolved do
      :none ->
        []

      {module, opts} ->
        refuse_unusable_backend!(module, opts)

        backend_children(module, opts) ++
          [
            Cspec.build(
              :flezha_durability_boot_hydration,
              {Hydrator, :hydrate_on_boot, []},
              :worker_temporary
            ),
            Shipper
          ]
    end
  end

  # What the boot chose, said out loud, on every boot rather than only the one
  # that establishes it: the population these lines are for is whoever inherits
  # the node, not the user who ran the first boot.
  #
  # A warning and not a refusal. The refusal this replaces was written when the
  # boot was the only place the posture could be noticed; `/healthz` and
  # `flz controller status` both carry it now, and what the refusal actually
  # blocked was the first local run of a release, where there is no durability
  # decision to make yet.
  defp announce_selection(resolved) do
    case {configured_backend(), resolved} do
      {:unset, _} -> Logger.warning(unconfigured_message())
      {:none, _} -> :ok
      {name, :none} -> Logger.warning(unrecognised_message(name))
      _ -> :ok
    end
  end

  defp unconfigured_message() do
    """
    FLEZHA_DURABILITY_BACKEND is not set, and a production controller must make \
    an explicit durability decision.

    Without a remote durability tier this controller is both the coordination \
    brain and the durable store on one local disk: if it dies WITH that disk, \
    every operator's state goes with it.

    Two valid moves:

      FLEZHA_DURABILITY_BACKEND=postgres   # or s3, or local_path — durable state
                                           # leaves this node's disk
      FLEZHA_DURABILITY_BACKEND=none       # explicit opt-out: the local disk is
                                           # the only copy, and that is accepted

    Defaulting to none. See docs/operations/remote_durability_tier.md for backend
    provisioning.
    """
  end

  # A typo must not crash-loop the controller, so the name resolves to `none`
  # like an unset variable does. Saying which name was not understood is what
  # stops the user believing a tier is running.
  defp unrecognised_message(name) do
    "FLEZHA_DURABILITY_BACKEND=#{inspect(name)} is not a backend this release " <>
      "knows. Valid values: none, postgres, s3, local_path. Defaulting to none: " <>
      "this node's disk holds the only copy of its durable state."
  end

  # The one refusal this tier keeps, and it is not the one above. An unmade
  # decision has a safe answer and takes it; an impossible one does not. A
  # backend selected without the options it needs boots clean today, reports a
  # remote posture and raises at the first ship, having shipped nothing.
  defp refuse_unusable_backend!(module, opts) do
    missing = for key <- required_opts(module), Keyword.get(opts, key) in [nil, ""], do: key

    if missing != [] do
      raise "FLEZHA_DURABILITY_BACKEND selects #{inspect(module)}, which cannot " <>
              "work without #{Enum.map_join(missing, ", ", &naming/1)}. Set what is " <>
              "missing, or set FLEZHA_DURABILITY_BACKEND=none to accept local-only " <>
              "durability. See docs/operations/remote_durability_tier.md."
    end
  end

  defp required_opts(module) do
    if Code.ensure_loaded?(module) and function_exported?(module, :required_opts, 0),
      do: module.required_opts(),
      else: []
  end

  # The variable a user sets the option with, asked of the knob reader
  # rather than restated here: a backend option is a row like any other, and
  # the key it fills is the only name this module holds for it.
  defp naming(key) do
    case Config.env_name({:durability_tier, :backend_opts, key}) do
      nil -> to_string(key)
      variable -> "#{key} (#{variable})"
    end
  end

  defp backend_children(module, opts) do
    case module.child_spec(opts) do
      :ignore -> []
      spec -> [spec]
    end
  end

  # ---------- per-class sync mode ----------

  @doc """
  Whether `operator_class` requires the remote ship to complete before its
  commit barrier is acked (RPO 0).

  Resolved through the ordinary per-class cascade — user override, instance
  metadata, then the SDK declaration — so a user can force a class either
  way without an SDK change. Always `false` while the tier is off: a class
  cannot be made to wait on a backend that does not exist.
  """
  @spec sync_class?(binary()) :: boolean()
  def sync_class?(operator_id) when is_binary(operator_id) do
    # Callers on the commit path hold an operator ID, while the declaration is
    # recorded against the resolved CLASS. Where an `:operator_class` override
    # makes those differ, looking up the id finds nothing and the gate silently
    # never arms — the class asked for RPO 0 and gets ordinary async shipping.
    enabled?() and
      Enum.any?(ClassRegistry.state_keys(operator_id), &PersistenceResolver.sync_durability?/1)
  end

  # ---------- triggers ----------

  @doc """
  Enqueue `ref` for shipping. Returns immediately: the queue is drained off
  every ack path, and a failing backend retries with backoff while local
  operation continues.
  """
  @spec ship(Artifact.ref()) :: :ok
  def ship(ref), do: if(enabled?(), do: Shipper.enqueue(ref), else: :ok)

  @doc """
  Every shippable reference stored under one on-disk STATE KEY: the durable
  references `Paths.durable_refs/1` names (its Cells, a bounded operator's journal,
  its input log) that hold local state, and its dead-letter trees.

  An operator can occupy two state keys — see `refs_of_operator/1`, which is
  what anything holding an operator id should call.
  """
  @spec refs_of_state_key(binary()) :: [Artifact.ref()]
  def refs_of_state_key(state_key) when is_binary(state_key) do
    Enum.filter(Paths.durable_refs(state_key), &Artifact.has_local_state?/1) ++
      Artifact.dlq_refs(state_key)
  end

  @doc """
  Every shippable reference this node holds, as `writer` would ship them — the
  backstop sweep's input, and the one place "what is shippable" is answered for
  the whole node.

  The writer is a parameter because the cspec's identity includes it: two
  controllers hold separate replicas of one registry, so "what is shippable
  here" is only answerable for a named writer.
  """
  @spec all_refs(binary()) :: [Artifact.ref()]
  def all_refs(writer) when is_binary(writer) do
    # Classes with Cell state UNION classes with parked failures: a stopped
    # operator keeps its parks without holding state, and its parks are still
    # this node's to ship.
    (Paths.operator_classes() ++ Artifact.dlq_classes())
    |> Enum.uniq()
    |> Enum.flat_map(&refs_of_state_key/1)
    |> Enum.concat(Artifact.cspec_refs(writer))
    |> Enum.concat(Artifact.control_refs(writer))
  end

  @doc """
  Every shippable reference of one operator, across BOTH identities it is
  stored under — its instance id, which keys the task-journal, and its
  resolved class, which keys the Cells and input log (for a bounded operator the two
  are one key).

  Anything holding an operator id and wanting its whole durable identity goes
  through here; `refs_of/1` answers the narrower question about one state key.
  """
  @spec refs_of_operator(binary()) :: [Artifact.ref()]
  def refs_of_operator(operator_id) when is_binary(operator_id) do
    operator_id
    |> ClassRegistry.state_keys()
    |> Enum.flat_map(&refs_of_state_key/1)
    |> Enum.uniq()
  end

  @doc """
  A compaction landed: the snapshot is on disk and the WAL below it is gone, so
  the shipped set moves from record bundles to a baseline.
  """
  @spec on_compaction_completed(tuple()) :: :ok
  def on_compaction_completed({_, _, _} = cell_id), do: ship({:cell, cell_id})

  @doc """
  An effect record just became durable and dispatched. Ships only for a
  checkpoint-ack class, where an integer `consumed_offset` marks an interval
  close — the exact cut whose state and source position are consistent. Every
  other class reaches the tier through compaction, a commit barrier, or the
  backstop sweep.
  """
  @spec on_effect_flushed(tuple(), integer() | nil) :: :ok
  def on_effect_flushed({operator_class, _, _} = cell_id, consumed_offset)
      when is_integer(consumed_offset) do
    if enabled?() and ClassRegistry.checkpoint_ack?(operator_class) do
      ship({:cell, cell_id})

      # The input log rides the same cut. Shipping the state without it would
      # leave a hydrated Cell holding a consumed floor and nothing to replay
      # from — the inputs after the cut would be gone, and the next one would
      # land below the floor and be skipped as already consumed.
      ship({:input_log, operator_class})
    end

    :ok
  end

  def on_effect_flushed(_cell_id, _consumed_offset), do: :ok

  @doc """
  A bounded instance committed a task or an iteration: its journal suffix, and
  the input log holding what it has not consumed, are the artifacts that must
  reach the remote store for the instance to resume elsewhere.
  """
  @spec on_bounded_commit(binary()) :: :ok
  def on_bounded_commit(operator_id) when is_binary(operator_id) do
    if enabled?(), do: Enum.each(refs_of_operator(operator_id), &Shipper.enqueue/1)
    :ok
  end

  @doc "The registry cspec changed — what operators exist, and how they are configured."
  @spec ship_cspec(atom() | binary()) :: :ok
  def ship_cspec(hub_id), do: ship({:cspec, {to_string(hub_id), writer_id()}})

  @doc """
  Ship `operator_id`'s artifacts and wait for the manifest write, for a class
  that declared synchronous durability. Returns the backend's verdict so the
  caller can withhold the commit ack: a failure stalls the bounded operator at its
  barrier, loudly, which is what RPO 0 means.
  """
  @spec commit_sync(binary()) :: :ok | {:error, term()}
  def commit_sync(operator_id) when is_binary(operator_id) do
    # Both key spaces: shipping only one of them would let this barrier answer
    # `:ok` having never sent the Cell state the guarantee is about.
    Enum.reduce_while(refs_of_operator(operator_id), :ok, fn ref, :ok ->
      case Shipper.ship_now(ref) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  # ---------- recovery ----------

  @doc """
  Hydrate `operator_class`'s artifacts from the remote manifest when its local
  state is absent. A no-op when the tier is off, when nothing is shipped, or
  when local state already exists — local is always authoritative.
  """
  @spec hydrate_operator(binary()) :: :ok
  def hydrate_operator(operator_class) when is_binary(operator_class) do
    with {module, opts} <- BackendConfig.resolve(),
         do: Hydrator.hydrate_operator(module, opts, operator_class)

    :ok
  end

  @doc """
  Hydrate only the named cell keys of one Tasklet.

  The manifest is per-Cell, so a narrowed request costs exactly the artifacts
  it names — answering a question about one key never fetches a whole
  Tasklet.
  """
  @spec hydrate_cells(binary(), binary(), [binary()]) :: :ok
  def hydrate_cells(operator_class, tasklet_id, cell_keys) do
    with {module, opts} <- BackendConfig.resolve() do
      Hydrator.hydrate_cells(module, opts, operator_class, tasklet_id, cell_keys)
    end

    :ok
  end

  # ---------- observability ----------

  @doc """
  Per-reference shipping state: last success, in-flight age, consecutive
  failures, and which writer superseded this node on the reference, if any. A
  plain ETS read — no call into the shipper, so `/healthz` and the Watchdog
  sweep can take it on any process.
  """
  @spec status() :: [map()]
  def status(), do: Shipper.status()

  @doc """
  References whose time since the last successful ship exceeds the RPO target.
  Empty while the tier is off.
  """
  @spec breaches() :: [map()]
  def breaches() do
    if enabled?() do
      target = rpo_target_ms()
      now = System.system_time(:millisecond)

      status()
      |> Enum.filter(&(&1.pending_since_ms != nil and now - &1.pending_since_ms >= target))
      |> Enum.map(&Map.put(&1, :lag_ms, now - &1.pending_since_ms))
    else
      []
    end
  end
end
