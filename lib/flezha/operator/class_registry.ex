defmodule Flezha.Operator.ClassRegistry do
  @moduledoc """
  Resolves an `operator_class` to its `kind`, its declarations and its park
  record, for paths that carry only the class string.

  `Retention`, `Watchdog`, `StreamChannel` and the persistence resolver are
  keyed purely by `operator_class`; this registry answers them with what the
  class declared (its persistence switch, its sleep setting, its tier) and
  whether its instance is asleep.

  Every read resolves in three steps: the in-memory ETS table (written at bind,
  evicted at teardown), a durable per-class sidecar under `Paths.class_dir/1`
  so a process-less sweep still resolves it, then the default.

  Invariant: a sidecar is written only for a value other than its default, so
  a class that declares nothing keeps its on-disk layout unchanged and every
  `Paths` scanner ignores it.
  """

  alias Flezha.State.Paths

  require Logger

  @table :flezha_operator_class_registry
  @default_kind :unbounded
  @kinds [:unbounded, :bounded]
  @sidecar "kind"
  @tier_sidecar "effectively_once"
  @compensable_sidecar "compensable_tasks"
  @leaf_sidecar "leaf_tasks"
  @checkpoint_sidecar "checkpoint_ack"
  @correction_sidecar "correction_window_ms"
  @compact_bytes_sidecar "compact_after_bytes"
  @sync_durability_sidecar "sync_durability"
  @persistence_off_sidecar "persistence_off"
  @sleep_sidecar "sleep_when_idle"
  @parked_sidecar "parked"

  # Every per-class SDK declaration, keyed `{operator_class, key}` in the ETS
  # table. `delete/1` evicts them all, so a class that rebinds without a
  # declaration cannot read a stale one.
  @declaration_keys [
    :effectively_once,
    :checkpoint_ack,
    :compensable_tasks,
    :leaf_tasks,
    :correction_window_ms,
    :compact_after_bytes,
    :sync_durability,
    :persistence_off,
    :sleep_when_idle,
    :parked
  ]

  # ---------- table lifecycle ----------

  @doc "Create the registry table if absent. Idempotent and race-tolerant."
  @spec ensure_table() :: :ok
  def ensure_table() do
    case :ets.whereis(@table) do
      :undefined ->
        try do
          :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
        rescue
          ArgumentError -> :ok
        end

        :ok

      _ ->
        :ok
    end
  end

  # ---------- writes ----------

  @doc """
  Record `operator_class`'s `kind`: an in-memory ETS entry plus, for a
  non-`:unbounded` kind, a durable sidecar so a process-less `Retention`
  sweep resolves the kind after teardown. Called at `Overwatch` bind.
  """
  @spec put(binary(), atom()) :: :ok
  def put(operator_class, kind) when is_binary(operator_class) and is_atom(kind) do
    ensure_table()
    :ets.insert(@table, {operator_class, kind})
    if kind != @default_kind, do: write_sidecar(operator_class, kind)
    :ok
  end

  # ---------- declarations: decode ----------

  @typedoc """
  A class's SDK-side declarations, as resolved from the `AddOperator` response.

  `compensable_tasks` is the chain positions whose tasklet declares a `compensate`
  hook, or `nil` when the class declares nothing (an unbounded operator, or a Java runner) —
  which a rewind reads as "no coverage information", not as "nothing is
  compensable".
  """
  @type declarations :: %{
          effectively_once: boolean(),
          keyless: boolean(),
          loop_mode: loop_mode() | nil,
          checkpoint_ack: boolean(),
          compensable_tasks: [non_neg_integer()] | nil,
          leaf_tasks: [non_neg_integer()] | nil,
          correction_window_ms: pos_integer() | nil,
          compact_after_bytes: pos_integer() | nil,
          sync_durability: boolean(),
          persistent: boolean(),
          sleep_when_idle: boolean()
        }

  @typedoc "How a bounded class repeats; an unbounded class declares none."
  @type loop_mode :: :once | :per_input | :chained

  @typedoc """
  An instance asleep: the durable wake deadline it parked with. What it
  consumed lives in its own records, never here.
  """
  @type park :: %{wake_deadline_ms: non_neg_integer() | nil}

  @doc """
  Resolve a class's SDK-side declarations from the `AddOperator` response
  raw_data — an `AddOperatorResponse` metadata map when the class declares
  anything, empty otherwise (including every pre-tier or Java runner). The one
  place the wire form is decoded; `put_declarations/2` is the one place it is
  persisted. A new declared capability is added in those two and nowhere else.
  """
  @spec decode_declarations(binary() | nil) :: declarations()
  def decode_declarations(raw_data) do
    metadata = decode_metadata(raw_data)

    %{
      effectively_once: Map.get(metadata, "effectively_once") == "true",
      # The SDK declares `keyed` only when it is false — a class with no keyed
      # entry-fed tasklet, which cannot be partitioned.
      keyless: Map.get(metadata, "keyed") == "false",
      loop_mode: parse_loop_mode(Map.get(metadata, "loop_mode")),
      checkpoint_ack: Map.get(metadata, "ack_granularity") == "checkpoint",
      compensable_tasks: parse_ops(Map.get(metadata, "compensable_tasks")),
      leaf_tasks: parse_ops(Map.get(metadata, "leaf_tasks")),
      correction_window_ms: parse_window(Map.get(metadata, "correction_window_ms")),
      compact_after_bytes: parse_window(Map.get(metadata, "compact_after_bytes")),
      sync_durability: Map.get(metadata, "durability") == "sync_remote",
      # Announced only when off: a class that says nothing persists.
      persistent: Map.get(metadata, "persistent") != "false",
      sleep_when_idle: Map.get(metadata, "sleep_when_idle") == "true"
    }
  end

  defp decode_metadata(raw_data) when is_binary(raw_data) and raw_data != "" do
    case RunnerCommands.AddOperatorResponse.decode(raw_data) do
      {:ok, %RunnerCommands.AddOperatorResponse{metadata: metadata}} -> metadata
      {:error, _} -> %{}
    end
  end

  defp decode_metadata(_raw_data), do: %{}

  defp parse_loop_mode("once"), do: :once
  defp parse_loop_mode("per_input"), do: :per_input
  defp parse_loop_mode("chained"), do: :chained
  defp parse_loop_mode(_raw), do: nil

  defp parse_ops(nil), do: nil
  defp parse_ops(""), do: []
  defp parse_ops(csv), do: csv |> String.split(",") |> Enum.map(&String.to_integer/1)

  # A positive decimal declaration string as an integer, or nil when
  # absent/malformed/non-positive.
  defp parse_window(raw) when is_binary(raw) do
    case Integer.parse(raw) do
      {ms, ""} when ms > 0 -> ms
      _ -> nil
    end
  end

  defp parse_window(_raw), do: nil

  # ---------- declarations: persist ----------

  @doc """
  Persist every SDK-declared capability for `operator_class`, as decoded from
  the `AddOperator` response by `decode_declarations/1`.

  Each rides an ETS entry plus a durable sidecar, so a process-less `Retention`
  sweep — or a bind that happens while the instance is suspended — resolves it
  with no runner to ask. A declaration a class drops is cleared on its next
  bind, so the sidecar never outlives the claim.

  Together with `decode_declarations/1` this is the whole recipe for a new
  SDK-declared capability: decode it there, persist it here.
  """
  @spec put_declarations(binary(), map()) :: :ok
  def put_declarations(operator_class, declarations) when is_binary(operator_class) do
    put_effectively_once(operator_class, declarations.effectively_once)
    put_checkpoint_ack(operator_class, declarations.checkpoint_ack)
    put_compensable_ops(operator_class, declarations.compensable_tasks)
    put_leaf_ops(operator_class, declarations.leaf_tasks)
    put_correction_window(operator_class, declarations.correction_window_ms)
    put_compact_after_bytes(operator_class, declarations.compact_after_bytes)
    put_sync_durability(operator_class, declarations.sync_durability)
    put_persistent(operator_class, declarations.persistent)
    put_sleep_when_idle(operator_class, declarations.sleep_when_idle)
  end

  @doc """
  Record `operator_class`'s persistence switch. Durable because the bind-time
  restore and the live read resolve it through
  `Flezha.State.PersistenceResolver.persistence_enabled?/1`, possibly while the
  instance is unbound; the sidecar exists only while persistence is off.
  """
  @spec put_persistent(binary(), boolean()) :: :ok
  def put_persistent(operator_class, persistent?)
      when is_binary(operator_class) and is_boolean(persistent?) do
    put_declaration(operator_class, :persistence_off, @persistence_off_sidecar, not persistent?)
  end

  @doc """
  Resolve `operator_class`'s persistence switch: ETS → durable sidecar →
  `true` (a class persists unless it declares otherwise).
  """
  @spec persistent?(binary()) :: boolean()
  def persistent?(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :persistence_off, @persistence_off_sidecar, &(&1 == "true")) !=
      true
  end

  @doc """
  Record whether `operator_class`'s instances sleep when idle. Durable so an
  instance rediscovered asleep at boot journals the input that wakes it before
  any runner has told the controller again.
  """
  @spec put_sleep_when_idle(binary(), boolean()) :: :ok
  def put_sleep_when_idle(operator_class, sleeps?)
      when is_binary(operator_class) and is_boolean(sleeps?) do
    put_declaration(operator_class, :sleep_when_idle, @sleep_sidecar, sleeps?)
  end

  @doc "Resolve `operator_class`'s sleep setting: ETS → durable sidecar → `false`."
  @spec sleep_when_idle?(binary()) :: boolean()
  def sleep_when_idle?(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :sleep_when_idle, @sleep_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Record that `operator_class`'s instance is asleep, or clear it with `nil`.
  Written durably before the instance is evicted, so a controller restart
  rediscovers the sleep, and a write that did not land is answered as an error
  so the caller leaves the instance bound; cleared by the bind that wakes it,
  before the runner is told to start the instance.
  """
  @spec put_parked(binary(), park() | nil) :: :ok | {:error, term()}
  def put_parked(operator_class, nil) when is_binary(operator_class),
    do: put_declaration(operator_class, :parked, @parked_sidecar, nil)

  def put_parked(operator_class, park) when is_binary(operator_class) do
    with :ok <- install_sidecar(operator_class, @parked_sidecar, encode_declaration(park)) do
      ensure_table()
      :ets.insert(@table, {{operator_class, :parked}, park})
      :ok
    end
  end

  @doc """
  `put_parked/2` on behalf of `namespace`, refused when another namespace owns
  the instance's durable state.
  """
  @spec put_parked(binary(), park(), binary()) :: :ok | {:error, term()}
  def put_parked(operator_class, park, namespace) do
    case Paths.namespace_of_class(operator_class) do
      owner when owner in [namespace, "_system"] -> put_parked(operator_class, park)
      owner -> {:error, {:namespace_conflict, operator_class, owner}}
    end
  end

  @doc "The park record of `operator_class`'s sleeping instance, `nil` when awake."
  @spec parked(binary()) :: park() | nil
  def parked(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :parked, @parked_sidecar, &parse_park/1)
  end

  @typedoc """
  What decides whether a factory's class may run partitioned: no keyed
  entry-fed tasklet, and for a bounded operator its loop mode.
  """
  @type partitioning :: %{keyless: boolean(), loop_mode: loop_mode() | nil}

  @doc """
  Record `factory_id`'s partitioning facts, as its bind declared them.

  The one declaration keyed by FACTORY rather than operator class:
  partitionability is a property of the class's code, and every instance of a
  factory answers identically. In-memory only — a restarted controller simply
  learns again at the next bind, which is the same path a never-bound class
  takes, and the SDK's own refusal is the authority either way.
  """
  @spec put_partitioning(binary(), map()) :: :ok
  def put_partitioning(factory_id, %{keyless: keyless?, loop_mode: mode})
      when is_binary(factory_id) and is_boolean(keyless?) do
    ensure_table()
    :ets.insert(@table, {{factory_id, :partitioning}, %{keyless: keyless?, loop_mode: mode}})
    :ok
  end

  @doc """
  `factory_id`'s recorded partitioning facts; for a class that has never bound,
  nothing refuses, and the bind itself then does.
  """
  @spec partitioning(binary()) :: partitioning()
  def partitioning(factory_id) when is_binary(factory_id) do
    case ets_lookup({factory_id, :partitioning}) do
      {:ok, facts} -> facts
      :error -> %{keyless: false, loop_mode: nil}
    end
  end

  @doc """
  Record the **established** value of `attribute` for `factory_id` in
  `namespace` — the one value every runner of that factory there must
  advertise: its SDK language (`:sdk_lang`) and its kind (`:kind`).

  Scoped by namespace as well as factory, because a factory id names a class
  within a deployment, not across the cluster: the test fleet alone gives the
  same reference ids to every SDK, so a global record would let one namespace's
  Rust runner refuse another namespace's Java one.

  In-memory only, like `put_partitioning/2`: the durable header re-establishes the
  language at the next bind (`establish_sdk_lang_from_state/3`), the next
  advertisement re-establishes both, and the SDK's own refusal is the backstop
  either way — so a restarted controller simply learns again.
  """
  @spec put_established(binary(), binary(), :sdk_lang | :kind, byte() | atom()) :: :ok
  def put_established(namespace, factory_id, attribute, value)
      when is_binary(namespace) and is_binary(factory_id) and attribute in [:sdk_lang, :kind] do
    ensure_table()
    :ets.insert(@table, {{namespace, factory_id, attribute}, value})
    :ok
  end

  @doc """
  `factory_id`'s established `attribute` in `namespace`, or `nil` when none is
  established yet.
  """
  @spec established(binary(), binary(), :sdk_lang | :kind) :: byte() | atom() | nil
  def established(namespace, factory_id, attribute)
      when is_binary(namespace) and is_binary(factory_id) do
    case ets_lookup({namespace, factory_id, attribute}) do
      {:ok, value} -> value
      :error -> nil
    end
  end

  def established(_namespace, _factory_id, _attribute), do: nil

  @doc """
  Forget everything established for `factory_id` in `namespace`, so the next
  advertisement establishes it afresh.

  The escape hatch from a factory deliberately re-authored in another language
  or as the other kind.
  """
  @spec delete_established(binary(), binary()) :: :ok
  def delete_established(namespace, factory_id)
      when is_binary(namespace) and is_binary(factory_id) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, {namespace, factory_id, :sdk_lang})
      :ets.delete(@table, {namespace, factory_id, :kind})
    end

    :ok
  end

  @doc """
  Record that `instance_id` resolves to `operator_class` on disk.

  These are two different key spaces, not two names for one thing. A bounded operator
  instance's task-journal lives under its INSTANCE id (`Paths.journal_id/1`),
  while an operator's Cells and input log live under the resolved CLASS
  (`:operator_class`, defaulting to the instance id). They coincide unless a
  user sets that option, which a bounded operator is refused — and where they
  diverge, asking for one and getting the other returns half an operator's
  artifacts with no error, which is how a synchronous-durability barrier can
  pass having shipped almost nothing.

  Stored under both members so either answers, and in memory only: the callers
  are the commit path and hydration, both of which run against a live bind, and
  the shipping path enumerates directories rather than consulting this at all.
  """
  @spec put_state_keys(binary(), binary()) :: :ok
  def put_state_keys(instance_id, operator_class)
      when is_binary(instance_id) and is_binary(operator_class) do
    ensure_table()
    keys = Enum.uniq([instance_id, operator_class])
    :ets.insert(@table, {{instance_id, :state_keys}, keys})
    :ets.insert(@table, {{operator_class, :state_keys}, keys})
    :ok
  end

  @doc """
  Every on-disk state key `key` may hold artifacts under — itself, plus the
  other member of the id/class pair when they differ. `[key]` for anything not
  recorded, which is the correct answer for the overwhelmingly common case
  where the two are the same string.
  """
  @spec state_keys(binary()) :: [binary()]
  def state_keys(key) when is_binary(key) do
    case ets_lookup({key, :state_keys}) do
      {:ok, keys} -> keys
      _ -> [key]
    end
  end

  @doc """
  Record whether `operator_class` requires its commit barriers to be remotely
  durable before they are acked (`remote-durability-tier`). Same ETS +
  durable-sidecar shape as the tier: the gate is consulted on the commit path
  of an instance that may have been bound by a previous controller.
  """
  @spec put_sync_durability(binary(), boolean()) :: :ok
  def put_sync_durability(operator_class, sync?)
      when is_binary(operator_class) and is_boolean(sync?) do
    put_declaration(operator_class, :sync_durability, @sync_durability_sidecar, sync?)
  end

  @doc """
  Resolve `operator_class`'s synchronous-durability declaration: ETS →
  durable sidecar → `false` (the cost-free default).
  """
  @spec sync_durability?(binary()) :: boolean()
  def sync_durability?(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :sync_durability, @sync_durability_sidecar, &(&1 == "true")) ==
      true
  end

  @doc """
  Record `operator_class`'s declared per-Cell WAL compaction size threshold
  in bytes, or `nil` when it declares none (then the global default governs).

  Durable in the same sidecar shape as the correction window, because the
  Watchdog's compaction-size sweep resolves it with no runner to ask and may run
  while the instance is suspended.
  """
  @spec put_compact_after_bytes(binary(), pos_integer() | nil) :: :ok
  def put_compact_after_bytes(operator_class, bytes) when is_binary(operator_class) do
    put_declaration(operator_class, :compact_after_bytes, @compact_bytes_sidecar, bytes)
  end

  @doc """
  Record `operator_class`'s declared correction window in milliseconds, or `nil`
  when it declares none.

  Durable in the same sidecar shape as the tier, because the retention that a
  correction window REQUIRES is derived from it by two background sweeps — the
  input-log truncation and the snapshot age floor — neither of which has an
  Overwatch to ask, and both of which may run before the class is ever bound.
  """
  @spec put_correction_window(binary(), pos_integer() | nil) :: :ok
  def put_correction_window(operator_class, window_ms) when is_binary(operator_class) do
    put_declaration(operator_class, :correction_window_ms, @correction_sidecar, window_ms)
  end

  @doc """
  Record whether `operator_class` runs the effectively-once delivery tier.
  The sidecar exists only while the tier is on, so a tier-off class's on-disk
  layout is unchanged and a class that drops the declaration stops journaling
  on the next bind.
  """
  @spec put_effectively_once(binary(), boolean()) :: :ok
  def put_effectively_once(operator_class, tier?)
      when is_binary(operator_class) and is_boolean(tier?) do
    put_declaration(operator_class, :effectively_once, @tier_sidecar, tier?)
  end

  @doc """
  Record whether `operator_class` acknowledges its inputs only at an explicit
  checkpoint. Same ETS + durable-sidecar shape as the tier.
  """
  @spec put_checkpoint_ack(binary(), boolean()) :: :ok
  def put_checkpoint_ack(operator_class, checkpoint?)
      when is_binary(operator_class) and is_boolean(checkpoint?) do
    put_declaration(operator_class, :checkpoint_ack, @checkpoint_sidecar, checkpoint?)
  end

  @doc """
  Record which of `operator_class`'s chain tasklets declare a `compensate`
  hook, as declared by the SDK in the `AddOperator` response metadata. `nil`
  clears it (a class that declares nothing).

  Durable in the same sidecar shape as the tier, because a rewind is refused or
  allowed on this evidence while the instance may be suspended with no runner
  to ask.
  """
  @spec put_compensable_ops(binary(), [non_neg_integer()] | nil) :: :ok
  def put_compensable_ops(operator_class, tasks) when is_binary(operator_class) do
    put_declaration(operator_class, :compensable_tasks, @compensable_sidecar, tasks)
  end

  @doc """
  Record which of `operator_class`'s tree tasklets are **leaves** — the set
  whose commits mean the operator is done (`bounded-branching`). Same ETS +
  durable-sidecar shape as the compensable set, and read on the same evidence:
  the journal answers "complete?" with no runner to ask.
  """
  @spec put_leaf_ops(binary(), [non_neg_integer()] | nil) :: :ok
  def put_leaf_ops(operator_class, tasks) when is_binary(operator_class) do
    put_declaration(operator_class, :leaf_tasks, @leaf_sidecar, tasks)
  end

  @doc """
  Resolve which of `operator_class`'s chain tasklets declare a `compensate`
  hook: ETS → durable sidecar → `nil` (the class declared nothing).
  """
  @spec compensable_tasks(binary()) :: [non_neg_integer()] | nil
  def compensable_tasks(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :compensable_tasks, @compensable_sidecar, &parse_ops/1)
  end

  @doc """
  Resolve `operator_class`'s leaf tasklets: ETS → durable sidecar → `nil`
  (the class declared nothing — a stream, or an SDK without the declaration).
  """
  @spec leaf_tasks(binary()) :: [non_neg_integer()] | nil
  def leaf_tasks(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :leaf_tasks, @leaf_sidecar, &parse_ops/1)
  end

  @doc """
  Record the live Overwatch pid for `operator_class` and the stable identity
  of the runner it is bound to. Written at bind,
  before the runner is told to start the operator.

  The State subsystem's fsync-gated outbox flush resolves the `:on_output`
  dispatch target through the pid, with no registry scan, and every door a
  runner's frame comes through reads the owner (`owner/1`).
  """
  @spec put_overwatch(binary(), pid(), binary() | nil) :: :ok
  def put_overwatch(operator_class, pid, owner)
      when is_binary(operator_class) and is_pid(pid) do
    ensure_table()
    :ets.insert(@table, {{operator_class, :overwatch}, pid, owner})
    :ok
  end

  @doc """
  The Overwatch pid and the owning identity recorded for `operator_class`,
  or `:error` when nothing is recorded on this node. The pid is answered
  whether or not it is still alive.
  """
  @spec owner(binary()) :: {:ok, pid(), binary() | nil} | :error
  def owner(operator_class) when is_binary(operator_class) do
    with true <- :ets.whereis(@table) != :undefined,
         [{_key, pid, owner}] <- :ets.lookup(@table, {operator_class, :overwatch}) do
      {:ok, pid, owner}
    else
      _ -> :error
    end
  end

  @doc "Resolve the live Overwatch pid for `operator_class`."
  @spec overwatch_pid(binary()) :: {:ok, pid()} | :error
  def overwatch_pid(operator_class) when is_binary(operator_class) do
    case owner(operator_class) do
      {:ok, pid, _owner} -> if Process.alive?(pid), do: {:ok, pid}, else: :error
      :error -> :error
    end
  end

  @doc """
  Evict the in-memory entry. The durable sidecar is kept — finished-job state
  can outlive its process, and the process-less `Retention` sweep still needs
  the kind. The sidecar is reclaimed with the class dir.
  """
  @spec delete(binary()) :: :ok
  def delete(operator_class) when is_binary(operator_class) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, operator_class)
      :ets.delete(@table, {operator_class, :overwatch})

      for key <- @declaration_keys do
        :ets.delete(@table, {operator_class, key})
      end
    end

    :ok
  end

  # ---------- reads ----------

  @doc "Resolve `operator_class`'s kind: ETS → durable sidecar → `:unbounded`."
  @spec get(binary()) :: atom()
  def get(operator_class) when is_binary(operator_class) do
    case ets_lookup(operator_class) do
      {:ok, kind} -> kind
      :error -> read_sidecar(operator_class) || @default_kind
    end
  end

  @doc """
  Resolve `operator_class`'s effectively-once tier: ETS → durable
  sidecar → `false` (the cost-free default).
  """
  @spec effectively_once?(binary()) :: boolean()
  def effectively_once?(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :effectively_once, @tier_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Resolve `operator_class`'s checkpoint ack-granularity: ETS → durable
  sidecar → `false` (the per-input default).
  """
  @spec checkpoint_ack?(binary()) :: boolean()
  def checkpoint_ack?(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :checkpoint_ack, @checkpoint_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Resolve `operator_class`'s declared correction window in milliseconds: ETS →
  durable sidecar → `nil` (the class declared none). The retention floors it
  requires are derived from this — see `Flezha.State.InputLog.hold_window_ms/1`
  and `Flezha.State.PersistenceResolver.compact_after_age_ms/1`.
  """
  @spec correction_window_ms(binary()) :: pos_integer() | nil
  def correction_window_ms(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :correction_window_ms, @correction_sidecar, &parse_window/1)
  end

  @doc """
  Resolve `operator_class`'s declared compaction size threshold in bytes:
  ETS → durable sidecar → `nil` (the class declared none — the global default
  then governs). Reuses the positive-integer sidecar codec.
  """
  @spec compact_after_bytes(binary()) :: pos_integer() | nil
  def compact_after_bytes(operator_class) when is_binary(operator_class) do
    declaration(operator_class, :compact_after_bytes, @compact_bytes_sidecar, &parse_window/1)
  end

  # ---------- internals ----------

  # A declared boolean: ETS while the class is bound, a sidecar that exists
  # only while the flag is on (so an undeclaring class's on-disk layout is
  # unchanged, and one that drops the declaration reverts on the next bind).
  # ONE shape for every SDK declaration: an ETS entry plus a durable sidecar that
  # outlives the process. An undeclared capability — `false` for a flag, `nil` for
  # a tasklet set — CLEARS both, so a class that rebinds having dropped a
  # declaration can never read the previous bind's, and a flag-off class's on-disk
  # layout stays exactly as it was before the flag existed. The only
  # per-capability code is the codec pair.
  defp put_declaration(operator_class, key, sidecar, undeclared)
       when undeclared in [nil, false] do
    ensure_table()
    # Cached as undeclared rather than evicted: a bound class answers from ETS
    # either way, so the read path never stats a sidecar that is not there.
    :ets.insert(@table, {{operator_class, key}, undeclared})
    _ = File.rm(sidecar_path(operator_class, sidecar))
    :ok
  end

  defp put_declaration(operator_class, key, sidecar, value) do
    ensure_table()
    :ets.insert(@table, {{operator_class, key}, value})
    write_sidecar_file(operator_class, sidecar, encode_declaration(value))
  end

  # ETS → durable sidecar → `nil` (undeclared). `decode` turns the sidecar's text
  # back into the declared value; an empty tasklet set decodes to `[]`, which is
  # meaningful ("a tree, none of it selected") and is NOT undeclared.
  defp declaration(operator_class, key, sidecar, decode) do
    case ets_lookup({operator_class, key}) do
      {:ok, value} ->
        value

      :error ->
        case File.read(sidecar_path(operator_class, sidecar)) do
          {:ok, text} -> decode.(String.trim(text))
          {:error, _} -> nil
        end
    end
  end

  defp encode_declaration(true), do: "true"
  defp encode_declaration(tasks) when is_list(tasks), do: Enum.join(tasks, ",")
  defp encode_declaration(ms) when is_integer(ms), do: Integer.to_string(ms)

  defp encode_declaration(%{wake_deadline_ms: deadline}), do: "#{deadline || "-"}"

  # A record written before the park stopped carrying an offset has one more
  # field, which is not read.
  defp parse_park(text) do
    [deadline | _offset] = String.split(text, ",")
    %{wake_deadline_ms: parse_optional(deadline)}
  end

  defp parse_optional("-"), do: nil
  defp parse_optional(digits), do: String.to_integer(digits)

  defp ets_lookup(operator_class) do
    case :ets.whereis(@table) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@table, operator_class) do
          [{^operator_class, kind}] -> {:ok, kind}
          [] -> :error
        end
    end
  end

  defp sidecar_path(operator_class, filename \\ @sidecar),
    do: Path.join(Paths.class_dir(operator_class), filename)

  defp write_sidecar(operator_class, kind),
    do: write_sidecar_file(operator_class, @sidecar, Atom.to_string(kind))

  # A declaration whose sidecar did not land is logged and kept in memory: the
  # next bind writes it again.
  defp write_sidecar_file(operator_class, filename, content) do
    case install_sidecar(operator_class, filename, content) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning(
          "[operator] #{filename} sidecar write failed for #{operator_class}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp install_sidecar(operator_class, filename, content) do
    path = sidecar_path(operator_class, filename)
    Paths.atomic_install(path, &:file.write(&1, content), :class_kind)
  end

  defp read_sidecar(operator_class) do
    case File.read(sidecar_path(operator_class)) do
      {:ok, bin} -> parse_kind(String.trim(bin))
      {:error, _} -> nil
    end
  end

  defp parse_kind(str) do
    kind = String.to_existing_atom(str)
    if known_kind?(kind), do: kind, else: nil
  rescue
    ArgumentError -> nil
  end

  @doc "Whether `kind` is an operator kind."
  @spec known_kind?(atom()) :: boolean()
  def known_kind?(kind), do: kind in @kinds
end
