defmodule Flezha.State.DurabilityTier do
  @moduledoc """
  Ships flezha's durable artifacts to a remote backend so a controller that dies
  **with its disk** is recoverable.

  Everything local is unchanged. Every ack path — append-before-forward,
  journal-then-ack, park-then-advance — still gates on local fsync and nothing
  else, and the local files stay authoritative: shipping never mutates or
  deletes them, and turning the tier off is a complete rollback.

  What ships are immutable artifacts at boundaries that already exist —
  compaction snapshots, seq-addressed WAL record bundles, proc-journal and inbox
  suffixes, the input log, the registry cspec — plus one backstop timer for
  entities that reach no boundary at all. The per-entity manifest is written
  last, so a torn upload is invisible to recovery.

  The public surface is triggers (`ship/1`, `on_compaction_completed/1`,
  `on_effect_flushed/2`, `commit_sync/1`), recovery (`hydrate_operative/1`,
  `hydrate_cspecs/0`) and status (`status/0`). All of them are cheap no-ops
  while `backend: :none` — the default, under which `children/0` returns `[]`
  and nothing at all is started.

  See `docs/remote_durability_tier.md`.
  """

  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.DurabilityTier.{Artifact, Backend, Hydrator, Shipper}
  alias Flezha.State.PersistenceResolver
  alias Flezha.State.Registry

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
  def enabled?(), do: Backend.resolve() != :none

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
  boot-time cspec hydration, and the shipper — or `[]` when the tier is off,
  which is what makes the default configuration byte-identical to a build
  without the tier.

  Order is load-bearing. The cspec hydration is a one-shot child that does its
  work in `start_link` and returns `:ignore`, so it completes before the
  supervisor starts anything after it — and it must be placed ahead of
  `Flezha.Service.HubManager`, because ProcessHub opens `registry.dets` at hub
  start and a hydration afterwards would be invisible to the orphan reconcile.
  """
  @spec children() :: [Supervisor.child_spec() | module() | {module(), term()}]
  def children() do
    case Backend.resolve() do
      :none ->
        []

      {module, opts} ->
        backend_children(module, opts) ++
          [
            %{
              id: :flezha_durability_cspec_hydration,
              start: {Hydrator, :hydrate_cspecs_on_boot, []},
              restart: :temporary,
              type: :worker
            },
            Shipper
          ]
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

  @doc "Enqueue every shippable artifact of one operative — its Entities, journal, inbox, input log."
  @spec ship_operative(binary()) :: :ok
  def ship_operative(operative_class) when is_binary(operative_class) do
    if enabled?() do
      Enum.each(refs_of(operative_class), &Shipper.enqueue/1)
    end

    :ok
  end

  @doc """
  Every shippable reference of `operative_class`: its Entities plus the
  reserved proc-journal/inbox identities (`Artifact.entity_refs/1`) and, when
  the effectively-once tier holds one, its durable input log — the source
  position a hydrated Entity replays from.
  """
  @spec refs_of(binary()) :: [Artifact.ref()]
  def refs_of(operative_class) do
    input_log = {:input_log, operative_class}
    extra = if Artifact.has_local_state?(input_log), do: [input_log], else: []
    Artifact.entity_refs(operative_class) ++ extra
  end

  @doc """
  Every shippable reference this node holds — the backstop sweep's input, and
  the one place "what is shippable" is answered for the whole node.
  """
  @spec all_refs() :: [Artifact.ref()]
  def all_refs() do
    Enum.flat_map(Registry.operative_classes(), &refs_of/1) ++ Artifact.cspec_refs()
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
    Enum.each(ClassRegistry.state_keys(operative_id), &ship_operative/1)
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
    # Both key spaces: the journal and inbox sit under the instance id, the
    # Entity WAL and input log under the resolved class. Shipping only one of
    # them would let this barrier answer `:ok` having never sent the Entity
    # state the guarantee is about.
    refs =
      operative_id
      |> ClassRegistry.state_keys()
      |> Enum.flat_map(&refs_of/1)
      |> Enum.uniq()

    Enum.reduce_while(refs, :ok, fn ref, :ok ->
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
    if enabled?(), do: Hydrator.hydrate_operative(operative_class), else: :ok
  end

  @doc """
  Hydrate only the named entity keys of one Procedure.

  The manifest is per-Entity, so a narrowed request costs exactly the artifacts
  it names — answering a question about one key never fetches a whole
  Procedure.
  """
  @spec hydrate_entities(binary(), binary(), [binary()]) :: :ok
  def hydrate_entities(operative_class, procedure_id, entity_keys) do
    if enabled?() do
      Hydrator.hydrate_entities(operative_class, procedure_id, entity_keys)
    else
      :ok
    end
  end

  @doc "Hydrate every shipped registry cspec whose local file is absent."
  @spec hydrate_cspecs() :: :ok
  def hydrate_cspecs(), do: if(enabled?(), do: Hydrator.hydrate_cspecs(), else: :ok)

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
