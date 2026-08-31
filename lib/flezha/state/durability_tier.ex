defmodule Flezha.State.DurabilityTier do
  @moduledoc """
  Ships flezha's durable artifacts to a remote backend so a controller that dies
  **with its disk** is recoverable. See
  `docs/operations/remote_durability_tier.md`.

  Local files stay authoritative: every ack path gates on local fsync alone,
  shipping neither mutates nor deletes one, and turning the tier off is a
  complete rollback. What ships are immutable artifacts at boundaries that
  already exist: snapshots, WAL record bundles, proc-journal and inbox suffixes,
  the input log, the registry cspec, parked DLQ records, the control-plane
  registries. The per-entity manifest is written last, so a torn upload is
  invisible to recovery.

  Every trigger, recovery and status function is a no-op while `backend: :none`,
  under which `children/0` starts nothing; a production boot must choose, and
  `children/0` refuses an unset `FLEZHA_DURABILITY_BACKEND`.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.DurabilityTier.{Artifact, Hydrator, Shipper}
  alias Flezha.State.DurabilityTier.Backend.Config, as: BackendConfig
  alias Flezha.State.Paths
  alias Flezha.State.PersistenceResolver

  @default_ship_interval_ms 30_000
  @default_rpo_target_ms 120_000
  @default_gc_interval_ms 3_600_000
  # Bounded by the slowest plausible ship, not tuned: both backends time out at
  # 30 s and the retry ladder caps at 30 s, and a failed manifest write
  # re-uploads its blobs — refreshing their age — so the real gap between an
  # upload and the entry naming it is minutes at worst. An hour is the headroom.
  @default_gc_grace_ms 3_600_000
  @default_gc_abandoned_after_ms 604_800_000

  # ---------- configuration ----------

  @doc "Whether a remote backend is configured. `false` makes every entry point inert."
  @spec enabled?() :: boolean()
  def enabled?(), do: BackendConfig.resolve() != :none

  @doc """
  How this controller's durable state is protected, as an operator reads it.

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
  `:none`, so the development loop is untouched.
  """
  @spec unconfigured?() :: boolean()
  def unconfigured?(), do: configured_backend() == :unset

  @doc """
  The backend as an operator named it, rather than as `Backend.resolve/0`
  resolves it — what `flz controller status` reports.
  """
  @spec configured_backend() :: atom() | {module(), keyword()}
  def configured_backend(),
    do: Application.get_env(:flezha, :durability_tier, []) |> Keyword.get(:backend, :none)

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
    case System.get_env("FLEZHA_DURABILITY_WRITER_ID") do
      value when is_binary(value) and value != "" -> value
      _ -> to_string(node())
    end
  end

  @doc """
  This node now runs `state_key`'s operative, so it may take over that
  reference's remote lineage.

  Shipping alone must never be enough to claim one: a migration leaves the
  source node holding a full copy of the state (`WalTransfer.pull_operative/3`
  copies, and nothing deletes the source), so its backstop sweep would
  otherwise bid the lineage back and fence the node that actually runs the
  operative now.
  """
  @spec adopt_operative(binary()) :: :ok
  def adopt_operative(state_key) when is_binary(state_key) do
    if enabled?() do
      case GenServer.whereis(Shipper) do
        nil -> :ok
        pid -> GenServer.cast(pid, {:adopt, state_key})
      end
    end

    :ok
  end

  @doc "Backstop sweep period — the bound on how stale a quiet entity's remote copy gets."
  @spec ship_interval_ms() :: pos_integer()
  def ship_interval_ms(), do: config(:ship_interval_ms, @default_ship_interval_ms)

  @doc "Time since the last successful ship at which the RPO alarm fires."
  @spec rpo_target_ms() :: pos_integer()
  def rpo_target_ms(), do: config(:rpo_target_ms, @default_rpo_target_ms)

  @doc "Orphan-blob collection period."
  @spec gc_interval_ms() :: pos_integer()
  def gc_interval_ms(), do: config(:gc_interval_ms, @default_gc_interval_ms)

  @doc """
  Age below which an unreferenced artifact is never collected, whoever owns it.

  This is the half of collection safety that does not depend on knowing whose
  artifact it is: an upload still in flight is by definition young, so a peer
  controller mid-ship is covered without any coordination between the two.
  """
  @spec gc_grace_ms() :: pos_integer()
  def gc_grace_ms(), do: config(:gc_grace_ms, @default_gc_grace_ms)

  @doc """
  Age at which an unreferenced artifact is collected even though no node holds
  local state for its reference — a stopped operative, or one whose state moved.

  Without it, scoping collection to what a node owns would leak those forever.
  Far longer than `gc_grace_ms/0` on purpose: it is the threshold past which no
  ship can plausibly still be in flight, so it must not be the same number, or
  the scoping would contribute nothing.
  """
  @spec gc_abandoned_after_ms() :: pos_integer()
  def gc_abandoned_after_ms(),
    do: config(:gc_abandoned_after_ms, @default_gc_abandoned_after_ms)

  defp config(key, default) do
    case Application.get_env(:flezha, :durability_tier, []) |> Keyword.get(key, default) do
      value when is_integer(value) and value > 0 -> value
      _ -> default
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
    if unconfigured?(), do: refuse_unconfigured_backend!()

    case BackendConfig.resolve() do
      :none ->
        []

      {module, opts} ->
        backend_children(module, opts) ++
          [
            %{
              id: :flezha_durability_boot_hydration,
              start: {Hydrator, :hydrate_on_boot, []},
              restart: :temporary,
              type: :worker
            },
            Shipper
          ]
    end
  end

  # Fail closed, not warn. The audit finding this closes is that every real
  # deployment ships the default, so a warning would be ignored by exactly the
  # deployments that need it. Both moves are one env line, and the opt-out is
  # lossless.
  defp refuse_unconfigured_backend!() do
    message = """
    FLEZHA_DURABILITY_BACKEND is not set, and a production controller must make \
    an explicit durability decision.

    Without a remote durability tier this controller is both the coordination \
    brain and the durable store on one local disk: if it dies WITH that disk, \
    every operative's state goes with it.

    Two valid moves:

      FLEZHA_DURABILITY_BACKEND=postgres   # or s3, or local_path — durable state
                                           # leaves this node's disk
      FLEZHA_DURABILITY_BACKEND=none       # explicit opt-out: the local disk is
                                           # the only copy, and that is accepted

    See docs/operations/remote_durability_tier.md for backend provisioning.
    """

    # Printed as well as raised. The refusal reaches the operator as an
    # application-start failure, which the BEAM formats as an Erlang term and
    # can truncate — and a refusal whose guidance is unreadable is just a
    # container that will not come up.
    IO.puts(:stderr, message)
    raise message
  end

  defp backend_children(module, opts) do
    case module.child_spec(opts) do
      :ignore -> []
      spec -> [spec]
    end
  end

  # ---------- per-class sync mode ----------

  @doc """
  Whether `operative_class` requires the remote ship to complete before its
  commit barrier is acked (RPO 0).

  Resolved through the ordinary per-class cascade — operator override, instance
  metadata, then the SDK declaration — so an operator can force a class either
  way without an SDK change. Always `false` while the tier is off: a class
  cannot be made to wait on a backend that does not exist.
  """
  @spec sync_class?(binary()) :: boolean()
  def sync_class?(operative_id) when is_binary(operative_id) do
    # Callers on the commit path hold an operative ID, while the declaration is
    # recorded against the resolved CLASS. Where an `:operative_class` override
    # makes those differ, looking up the id finds nothing and the gate silently
    # never arms — the class asked for RPO 0 and gets ordinary async shipping.
    enabled?() and
      Enum.any?(ClassRegistry.state_keys(operative_id), &PersistenceResolver.sync_durability?/1)
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
  Every shippable reference stored under one on-disk STATE KEY: its Entities
  plus the reserved proc-journal/inbox identities (`Artifact.entity_refs/1`)
  and, when the effectively-once tier holds one, its durable input log.

  An operative can occupy two state keys — see `refs_of_operative/1`, which is
  what anything holding an operative id should call.
  """
  @spec refs_of_state_key(binary()) :: [Artifact.ref()]
  def refs_of_state_key(state_key) when is_binary(state_key) do
    input_log = {:input_log, state_key}
    extra = if Artifact.has_local_state?(input_log), do: [input_log], else: []
    Artifact.entity_refs(state_key) ++ extra ++ Artifact.dlq_refs(state_key)
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
    # Classes with Entity state UNION classes with parked failures: a stopped
    # operative keeps its parks without holding state, and its parks are still
    # this node's to ship.
    (Paths.operative_classes() ++ Artifact.dlq_classes())
    |> Enum.uniq()
    |> Enum.flat_map(&refs_of_state_key/1)
    |> Enum.concat(Artifact.cspec_refs(writer))
    |> Enum.concat(Artifact.control_refs(writer))
  end

  @doc """
  Every shippable reference of one operative, across BOTH identities it is
  stored under — its instance id, which keys the proc-journal and durable inbox,
  and its resolved class, which keys the Entities and input log.

  Anything holding an operative id and wanting its whole durable identity goes
  through here; `refs_of/1` answers the narrower question about one state key.
  """
  @spec refs_of_operative(binary()) :: [Artifact.ref()]
  def refs_of_operative(operative_id) when is_binary(operative_id) do
    operative_id
    |> ClassRegistry.state_keys()
    |> Enum.flat_map(&refs_of_state_key/1)
    |> Enum.uniq()
  end

  @doc """
  A compaction landed: the snapshot is on disk and the WAL below it is gone, so
  the shipped set moves from record bundles to a baseline.
  """
  @spec on_compaction_completed(tuple()) :: :ok
  def on_compaction_completed({_, _, _} = entity_id), do: ship({:entity, entity_id})

  @doc """
  An effect record just became durable and dispatched. Ships only for a
  checkpoint-ack class, where an integer `consumed_offset` marks an interval
  close — the exact cut whose state and source position are consistent. Every
  other class reaches the tier through compaction, a commit barrier, or the
  backstop sweep.
  """
  @spec on_effect_flushed(tuple(), integer() | nil) :: :ok
  def on_effect_flushed({operative_class, _, _} = entity_id, consumed_offset)
      when is_integer(consumed_offset) do
    if enabled?() and ClassRegistry.checkpoint_ack?(operative_class) do
      ship({:entity, entity_id})

      # The input log rides the same cut. Shipping the state without it would
      # leave a hydrated Entity holding a consumed floor and nothing to replay
      # from — the inputs after the cut would be gone, and the next one would
      # land below the floor and be skipped as already consumed.
      ship({:input_log, operative_class})
    end

    :ok
  end

  def on_effect_flushed(_entity_id, _consumed_offset), do: :ok

  @doc """
  A workflow instance committed a proc or an iteration: its journal suffix, and
  the inbox prefix the commit consumed, are the artifacts that must reach the
  remote store for the instance to resume elsewhere.
  """
  @spec on_workflow_commit(binary()) :: :ok
  def on_workflow_commit(operative_id) when is_binary(operative_id) do
    if enabled?(), do: Enum.each(refs_of_operative(operative_id), &Shipper.enqueue/1)
    :ok
  end

  @doc "The registry cspec changed — what operatives exist, and how they are configured."
  @spec ship_cspec(atom() | binary()) :: :ok
  def ship_cspec(hub_id), do: ship({:cspec, {to_string(hub_id), writer_id()}})

  @doc """
  Ship `operative_id`'s artifacts and wait for the manifest write, for a class
  that declared synchronous durability. Returns the backend's verdict so the
  caller can withhold the commit ack: a failure stalls the workflow at its
  barrier, loudly, which is what RPO 0 means.
  """
  @spec commit_sync(binary()) :: :ok | {:error, term()}
  def commit_sync(operative_id) when is_binary(operative_id) do
    # Both key spaces: shipping only one of them would let this barrier answer
    # `:ok` having never sent the Entity state the guarantee is about.
    Enum.reduce_while(refs_of_operative(operative_id), :ok, fn ref, :ok ->
      case Shipper.ship_now(ref) do
        :ok -> {:cont, :ok}
        {:error, _} = err -> {:halt, err}
      end
    end)
  end

  # ---------- recovery ----------

  @doc """
  Hydrate `operative_class`'s artifacts from the remote manifest when its local
  state is absent. A no-op when the tier is off, when nothing is shipped, or
  when local state already exists — local is always authoritative.
  """
  @spec hydrate_operative(binary()) :: :ok
  def hydrate_operative(operative_class) when is_binary(operative_class) do
    with {module, opts} <- BackendConfig.resolve(),
         do: Hydrator.hydrate_operative(module, opts, operative_class)

    :ok
  end

  @doc """
  Hydrate only the named entity keys of one Procedure.

  The manifest is per-Entity, so a narrowed request costs exactly the artifacts
  it names — answering a question about one key never fetches a whole
  Procedure.
  """
  @spec hydrate_entities(binary(), binary(), [binary()]) :: :ok
  def hydrate_entities(operative_class, procedure_id, entity_keys) do
    with {module, opts} <- BackendConfig.resolve() do
      Hydrator.hydrate_entities(module, opts, operative_class, procedure_id, entity_keys)
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
