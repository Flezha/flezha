defmodule Flezha.State.DurabilityTier do
  @moduledoc """
  Ships flezha's durable artifacts to a remote backend so a controller that dies
  **with its disk** is recoverable. See
  `docs/resilience/data_durability.md`.

  Local files are the copy a controller runs from: a class at `durable: local` gates every
  acknowledgement on local fsync alone, one at `durable: remote` also on the
  ship of what that acknowledgement covers (`commit_sync/1`); shipping neither
  mutates nor deletes a local file, and turning the tier off is a complete
  rollback. What ships are immutable artifacts at boundaries that
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
  alias Flezha.Relay.ClassRegistry
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
  This node now runs `state_key`'s relay, so it may take over that
  reference's remote lineage.

  Shipping alone must never be enough to claim one: a migration leaves the
  source node holding a full copy of the state (`WalTransfer.pull_relay/3`
  copies, and nothing deletes the source), so its backstop sweep would
  otherwise bid the lineage back and fence the node that actually runs the
  relay now.
  """
  @spec adopt_relay(binary()) :: :ok
  def adopt_relay(state_key) when is_binary(state_key),
    do: if(enabled?(), do: Shipper.adopt(state_key), else: :ok)

  @doc """
  `state_key`'s relay no longer runs on this node, so the adoption is withdrawn:
  the node may extend a lineage it still holds, and takes none over until the
  relay binds here again (`adopt_relay/1`).

  A relay that `:moved` is released at once: its new node pulled the files, and
  a bid from here could only fence it. One `:stopped` here first ships every
  reference of the class and keeps the adoption until none is owed any more,
  since those ships are the only way its last turns reach the backend.
  """
  @spec release_relay(binary(), :moved | :stopped) :: :ok
  def release_relay(state_key, departure) when is_binary(state_key),
    do: if(enabled?(), do: Shipper.release(state_key, departure), else: :ok)

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
  local state for its reference — a stopped relay, or one whose state moved.

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
  Supervision children for the application module: the backend connection, the
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
    every relay's state goes with it.

    Two valid moves:

      FLEZHA_DURABILITY_BACKEND=postgres   # or s3, or local_path — durable state
                                           # leaves this node's disk
      FLEZHA_DURABILITY_BACKEND=none       # explicit opt-out: the local disk is
                                           # the only copy, and that is accepted

    Defaulting to none. See docs/configuration/environment_variables.md for
    the backend options.
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
              "durability. See docs/configuration/environment_variables.md."
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
  Whether `relay_class` requires the remote ship to complete before a
  commit point is acknowledged (RPO 0).

  The class's own `durable: remote` declaration and nothing else
  (`PersistenceResolver.remote?/1`), read under every state key the id/class
  pair holds. Always `false` while the tier is off: a class cannot be made to
  wait on a backend that does not exist.
  """
  @spec sync_class?(binary()) :: boolean()
  def sync_class?(relay_id) when is_binary(relay_id) do
    # Callers on the commit path hold a relay ID, while the declaration is
    # recorded against the resolved CLASS. Where an `:relay_class` override
    # makes those differ, looking up the id finds nothing and the gate silently
    # never arms — the class asked for RPO 0 and gets ordinary async shipping.
    enabled?() and
      Enum.any?(ClassRegistry.state_keys(relay_id), &PersistenceResolver.remote?/1)
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
  Which of `refs` have nothing left to ship: every change announced for them
  has landed, or another writer holds the lineage. All of them when no backend
  is configured, since there is then no remote copy to fall behind.
  """
  @spec caught_up([Artifact.ref()]) :: [Artifact.ref()]
  def caught_up([]), do: []
  def caught_up(refs), do: if(enabled?(), do: Shipper.caught_up(refs), else: refs)

  @doc """
  Every shippable reference stored under one on-disk STATE KEY: the durable
  references `Paths.durable_refs/1` names (its Cells, a steps relay's journal,
  its input log) that hold local state, and its dead-letter trees.

  A relay can occupy two state keys — see `refs_of_relay/1`, which is
  what anything holding a relay id should call.
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
    # relay keeps its parks without holding state, and its parks are still
    # this node's to ship.
    (Paths.relay_classes() ++ Artifact.dlq_classes())
    |> Enum.uniq()
    |> Enum.flat_map(&refs_of_state_key/1)
    |> Enum.concat(Artifact.cspec_refs(writer))
    |> Enum.concat(Artifact.control_refs(writer))
  end

  @doc """
  Every shippable reference of one relay, across BOTH identities it is
  stored under — its instance id, which keys the task-journal, and its
  resolved class, which keys the Cells and input log (for a steps relay the two
  are one key).

  Anything holding a relay id and wanting its whole durable identity goes
  through here; `refs_of/1` answers the narrower question about one state key.
  """
  @spec refs_of_relay(binary()) :: [Artifact.ref()]
  def refs_of_relay(relay_id) when is_binary(relay_id) do
    relay_id
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
  def on_effect_flushed({relay_class, _, _} = cell_id, consumed_offset)
      when is_integer(consumed_offset) do
    if enabled?() and ClassRegistry.ack_checkpoint?(relay_class) do
      ship({:cell, cell_id})

      # The input log rides the same cut. Shipping the state without it would
      # leave a hydrated Cell holding a consumed floor and nothing to replay
      # from — the inputs after the cut would be gone, and the next one would
      # land below the floor and be skipped as already consumed.
      ship({:input_log, relay_class})
    end

    :ok
  end

  def on_effect_flushed(_cell_id, _consumed_offset), do: :ok

  @doc """
  A steps instance committed a task or an iteration: its journal suffix, and
  the input log holding what it has not consumed, are the artifacts that must
  reach the remote store for the instance to resume elsewhere.
  """
  @spec on_steps_commit(binary()) :: :ok
  def on_steps_commit(relay_id) when is_binary(relay_id) do
    if enabled?(), do: Enum.each(refs_of_relay(relay_id), &Shipper.enqueue/1)
    :ok
  end

  @doc "The registry cspec changed — what relays exist, and how they are configured."
  @spec ship_cspec(atom() | binary()) :: :ok
  def ship_cspec(hub_id), do: ship({:cspec, {to_string(hub_id), writer_id()}})

  @doc """
  Ship a commit point's artifacts and wait for the manifest write, for a class
  that keeps its work `durable: remote`. Returns the backend's verdict so the
  caller can withhold the acknowledgement: a failure stalls the instance at its
  commit point, loudly, which is what RPO 0 means.

  The commit point is a steps instance's barrier, named by its relay id, a
  flow Cell's synced records, named by its Cell id: the Cell and the input log
  its consumed offsets point into, or a kept effect result, named by its
  `{:effects_log, relay_class}` reference. The wait is reported as
  `[:flezha, :state, :remote_commit]` for both.
  """
  @spec commit_sync(binary() | tuple()) :: :ok | {:error, term()}
  def commit_sync(subject) do
    started = System.monotonic_time(:millisecond)
    {relay, refs} = commit_target(subject)
    result = ship_through(refs, relay)

    :telemetry.execute(
      [:flezha, :state, :remote_commit],
      %{duration_ms: System.monotonic_time(:millisecond) - started},
      %{subject: inspect(subject), status: if(result == :ok, do: :ok, else: :error)}
    )

    result
  end

  @doc """
  `commit_sync/1` off the caller's process, so a writer or a commit worker
  keeps serving while the backend answers: `{:remote_committed, tag, result}`
  arrives in the caller's mailbox.
  """
  @spec commit_async(binary() | tuple(), term()) :: :ok
  def commit_async(subject, tag) do
    caller = self()

    {:ok, _pid} =
      Task.start(fn -> send(caller, {:remote_committed, tag, commit_sync(subject)}) end)

    :ok
  end

  @doc """
  A flow Cell stopped waiting on its ship: a refused ship's RPO clock, kept
  running while it waited (`Shipper.ship_now/1`), stops if another
  controller now ships the reference.
  """
  @spec end_wait(tuple()) :: :ok
  def end_wait({_, _, _} = cell_id) do
    if enabled?(), do: Enum.each(elem(commit_target(cell_id), 1), &Shipper.end_wait/1)
    :ok
  end

  # The relay a commit point belongs to, and what it ships. Both key spaces
  # of a steps instance: shipping only one of them would let its barrier answer
  # `:ok` having never sent the Cell state the guarantee is about. A flow Cell
  # ships its input log first, so the floor its records carry never lands
  # remotely ahead of the inputs above it.
  defp commit_target(relay_id) when is_binary(relay_id),
    do: {relay_id, refs_of_relay(relay_id)}

  defp commit_target({:effects_log, relay_class} = ref), do: {relay_class, [ref]}

  defp commit_target({relay_class, _, _} = cell_id),
    do: {relay_class, [{:input_log, relay_class}, {:cell, cell_id}]}

  defp ship_through(refs, relay) do
    Enum.reduce_while(refs, :ok, fn ref, :ok ->
      case Shipper.ship_now(ref) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:sync_ship_failed, relay, reason}}}
      end
    end)
  end

  # ---------- recovery ----------

  @doc """
  Hydrate `relay_class`'s artifacts from the remote manifest when its local
  state is absent (`Hydrator.hydrate_relay/4`). A no-op when the tier is off,
  when nothing is shipped, or when local state already exists; a newer shipped
  copy replaces a local one only through `Flezha.State.Adoption`.
  """
  @spec hydrate_relay(binary(), :all | :own) :: :ok
  def hydrate_relay(relay_class, scope \\ :all) when is_binary(relay_class) do
    with {module, opts} <- BackendConfig.resolve(),
         do: Hydrator.hydrate_relay(module, opts, relay_class, scope)

    :ok
  end

  @doc "`Hydrator.shipped_freshness/3` against the configured backend; `%{}` when the tier is off."
  @spec shipped_freshness(binary()) :: %{Paths.durable_ref() => term()}
  def shipped_freshness(relay_class) do
    case BackendConfig.resolve() do
      {module, opts} -> Hydrator.shipped_freshness(module, opts, relay_class)
      :none -> %{}
    end
  end

  @doc """
  Hydrate only the named cell keys of one Tasklet.

  The manifest is per-Cell, so a narrowed request costs exactly the artifacts
  it names — answering a question about one key never fetches a whole
  Tasklet.
  """
  @spec hydrate_cells(binary(), binary(), [binary()]) :: :ok
  def hydrate_cells(relay_class, tasklet_id, cell_keys) do
    with {module, opts} <- BackendConfig.resolve() do
      Hydrator.hydrate_cells(module, opts, relay_class, tasklet_id, cell_keys)
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
