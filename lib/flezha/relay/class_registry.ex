defmodule Flezha.Relay.ClassRegistry do
  @moduledoc """
  Resolves a `relay_class` to how it runs (`:flow` or `:steps`), its
  declarations and the storage of its sleep record (`Flezha.Relay.Suspend`), for
  paths that carry only the class string.

  `Retention`, `Watchdog`, `StreamChannel` and the persistence resolver are
  keyed purely by `relay_class`; this registry answers them with what the
  class declared (its persistence switch, its sleep setting, its tier).
  `journals?/1` is the one question the controller asks of how a class runs:
  whether an instance keeps a task journal, which a class that runs in steps
  does.

  Every read resolves in three steps: the in-memory ETS table (written at bind,
  evicted at teardown), a durable per-class sidecar under `Paths.class_dir/1`
  so a process-less sweep still resolves it, then the default.

  Invariant: a sidecar is written only for a value other than its default, so
  a class that declares nothing keeps its on-disk layout unchanged and every
  `Paths` scanner ignores it.
  """

  alias Flezha.State.Paths

  require Logger

  @table :flezha_relay_class_registry
  @default_run :flow
  @runs [:flow, :steps]
  @sidecar "run"
  @tier_sidecar "effectively_once"
  @compensable_sidecar "compensable_tasks"
  @leaf_sidecar "leaf_tasks"
  @checkpoint_sidecar "ack_checkpoint"
  @correction_sidecar "correction_window_ms"
  @compact_bytes_sidecar "compact_after_bytes"
  @remote_sidecar "remote"
  @persistence_off_sidecar "persistence_off"
  @sleep_sidecar "sleep_after_ms"
  @established [:sdk_lang, :run, :each, :on_failure]

  # Every per-class SDK declaration, keyed `{relay_class, key}` in the ETS
  # table. `delete/1` evicts them all, so a class that rebinds without a
  # declaration cannot read a stale one.
  @declaration_keys [
    :effectively_once,
    :ack_checkpoint,
    :compensable_tasks,
    :leaf_tasks,
    :correction_window_ms,
    :compact_after_bytes,
    :remote,
    :persistence_off,
    :sleep_after_ms,
    :parked
  ]

  # ---------- table lifecycle ----------

  @doc "Create the registry table if absent. Idempotent and race-tolerant."
  @spec ensure_table() :: :ok
  def ensure_table() do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
    end

    :ok
  rescue
    ArgumentError -> :ok
  end

  # ---------- writes ----------

  @doc """
  Record how `relay_class` runs: an in-memory ETS entry plus, for
  `:steps`, a durable sidecar so a process-less `Retention` sweep resolves it
  after teardown. Called at `Overwatch` bind.
  """
  @spec put(binary(), run()) :: :ok
  def put(relay_class, run) when is_binary(relay_class) and run in @runs do
    ensure_table()
    :ets.insert(@table, {relay_class, run})
    if run != @default_run, do: write_sidecar(relay_class, run)
    :ok
  end

  # ---------- declarations: decode ----------

  @typedoc "How a class runs: in flow, or in steps."
  @type run :: :flow | :steps

  @typedoc "What a factory's announcement establishes."
  @type established :: :sdk_lang | :run | :each | :on_failure

  @typedoc """
  A class's settings and SDK-side declarations, as resolved from the
  `AddRelay` response and its factory's announcement.

  `compensable_tasks` is the chain positions whose tasklet declares a `compensate`
  hook, or `nil` when the class declares nothing (a flow relay, or a Java runner) —
  which a rewind reads as "no coverage information", not as "nothing is
  compensable".
  """
  @type declarations :: %{
          run: run(),
          each: each() | nil,
          on_failure: :undo_all | :keep_progress | nil,
          durable: :memory | :local | :remote,
          sleep_after_ms: pos_integer() | nil,
          max_duration_ms: pos_integer() | nil,
          effectively_once: boolean(),
          keyless: boolean(),
          ack_checkpoint: boolean(),
          compensable_tasks: [non_neg_integer()] | nil,
          leaf_tasks: [non_neg_integer()] | nil,
          correction_window_ms: pos_integer() | nil,
          compact_after_bytes: pos_integer() | nil,
          persistent: boolean()
        }

  @typedoc "How a steps class repeats; a flow class declares none."
  @type each :: :once | :per_input | :chained

  @doc """
  Resolve a class's settings and SDK-side declarations from the `AddRelay`
  response raw_data, an `AddRelayResponse` metadata map, and its run, loop
  shape and failure mode from `established`, what its factory is established
  at as placement reads it (`FleetManager.established/3`). The one place the
  wire form is decoded; `put_declarations/2` is the one place it is persisted.
  A new declared capability is added in those two and nowhere else. The sleep
  interval's default is the class's, announced by its SDK: a class that
  announces none never sleeps.
  """
  @spec decode_declarations(binary() | nil, map()) :: declarations()
  def decode_declarations(raw_data, established) do
    metadata = decode_metadata(raw_data)
    durable = parse_durable(Map.get(metadata, "durable"))

    %{
      run: established.run || :flow,
      each: established.each,
      on_failure: established.on_failure,
      durable: durable,
      sleep_after_ms: parse_window(Map.get(metadata, "sleep_after_ms")),
      max_duration_ms: parse_window(Map.get(metadata, "max_duration_ms")),
      effectively_once: Map.get(metadata, "exactly") == "effectively_once",
      # The SDK declares `keyed` only when it is false — a class with no keyed
      # entry-fed tasklet, which cannot be partitioned.
      keyless: Map.get(metadata, "keyed") == "false",
      ack_checkpoint: Map.get(metadata, "ack") == "checkpoint",
      compensable_tasks: parse_ops(Map.get(metadata, "compensable_tasks")),
      leaf_tasks: parse_ops(Map.get(metadata, "leaf_tasks")),
      correction_window_ms: parse_window(Map.get(metadata, "correction_ms")),
      compact_after_bytes: parse_window(Map.get(metadata, "compact_after_bytes")),
      persistent: durable != :memory
    }
  end

  defp decode_metadata(raw_data) when is_binary(raw_data) and raw_data != "" do
    case RunnerCommands.AddRelayResponse.decode(raw_data) do
      {:ok, %RunnerCommands.AddRelayResponse{metadata: metadata}} -> metadata
      {:error, _} -> %{}
    end
  end

  defp decode_metadata(_raw_data), do: %{}

  # A class that announces nothing keeps its work locally, as the tree's
  # default does.
  defp parse_durable("memory"), do: :memory
  defp parse_durable("remote"), do: :remote
  defp parse_durable(_raw), do: :local

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
  Persist every SDK-declared capability for `relay_class`, as decoded from
  the `AddRelay` response by `decode_declarations/2`.

  Each rides an ETS entry plus a durable sidecar, so a process-less `Retention`
  sweep — or a bind that happens while the instance is suspended — resolves it
  with no runner to ask. A declaration a class drops is cleared on its next
  bind, so the sidecar never outlives the claim.

  Together with `decode_declarations/2` this is the whole recipe for a new
  SDK-declared capability: decode it there, persist it here.
  """
  @spec put_declarations(binary(), map()) :: :ok
  def put_declarations(relay_class, declarations) when is_binary(relay_class) do
    put_effectively_once(relay_class, declarations.effectively_once)
    put_ack_checkpoint(relay_class, declarations.ack_checkpoint)
    put_compensable_ops(relay_class, declarations.compensable_tasks)
    put_leaf_ops(relay_class, declarations.leaf_tasks)
    put_correction_window(relay_class, declarations.correction_window_ms)
    put_compact_after_bytes(relay_class, declarations.compact_after_bytes)
    put_remote(relay_class, declarations.durable == :remote)
    put_persistent(relay_class, declarations.persistent)
    put_sleep_after(relay_class, declarations.sleep_after_ms)
  end

  @doc """
  Record `relay_class`'s persistence switch. Durable because the bind-time
  restore and the live read resolve it through `persistent?/1`, possibly while
  the instance is unbound; the sidecar exists only while persistence is off.
  """
  @spec put_persistent(binary(), boolean()) :: :ok
  def put_persistent(relay_class, persistent?)
      when is_binary(relay_class) and is_boolean(persistent?) do
    put_declaration(relay_class, :persistence_off, @persistence_off_sidecar, not persistent?)
  end

  @doc """
  Resolve `relay_class`'s persistence switch: ETS → durable sidecar →
  `true` (a class persists unless it declares otherwise).
  """
  @spec persistent?(binary()) :: boolean()
  def persistent?(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :persistence_off, @persistence_off_sidecar, &(&1 == "true")) !=
      true
  end

  @doc """
  Record how long `relay_class`'s idle instances wait before they sleep,
  or `nil` when they never do. Durable so an instance rediscovered asleep at
  boot journals the input that wakes it before any runner has told the
  controller again.
  """
  @spec put_sleep_after(binary(), pos_integer() | nil) :: :ok
  def put_sleep_after(relay_class, sleep_after_ms) when is_binary(relay_class) do
    put_declaration(relay_class, :sleep_after_ms, @sleep_sidecar, sleep_after_ms)
  end

  @doc "`relay_class`'s sleep interval: ETS → durable sidecar → `nil` (never)."
  @spec sleep_after_ms(binary()) :: pos_integer() | nil
  def sleep_after_ms(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :sleep_after_ms, @sleep_sidecar, &parse_window/1)
  end

  @doc "Whether `relay_class`'s instances sleep when idle."
  @spec sleeps?(binary()) :: boolean()
  def sleeps?(relay_class) when is_binary(relay_class),
    do: sleep_after_ms(relay_class) != nil

  # A record whose codec its owner keeps (`Flezha.Relay.Suspend`'s sleep
  # record), or `nil` to clear it. The sidecar lands before the ETS entry, so a
  # write that did not land is an error; a `namespace` not owning the class's
  # durable state is refused.
  @doc false
  @spec put_record(binary(), {atom(), binary()}, {term(), iodata()} | nil, binary() | nil) ::
          :ok | {:error, term()}
  def put_record(relay_class, {key, sidecar}, nil, nil) when is_binary(relay_class),
    do: put_declaration(relay_class, key, sidecar, nil)

  def put_record(relay_class, {key, sidecar}, {value, encoded}, nil)
      when is_binary(relay_class) do
    with :ok <- install_sidecar(relay_class, sidecar, encoded) do
      ensure_table()
      :ets.insert(@table, {{relay_class, key}, value})
      :ok
    end
  end

  def put_record(relay_class, where, record, namespace) do
    case Paths.namespace_of_class(relay_class) do
      owner when owner in [namespace, "_system"] -> put_record(relay_class, where, record, nil)
      owner -> {:error, {:namespace_conflict, relay_class, owner}}
    end
  end

  @typedoc """
  What decides whether a factory's class may run partitioned: no keyed
  entry-fed tasklet, and for a steps relay its loop mode.
  """
  @type partitioning :: %{keyless: boolean(), each: each() | nil}

  @doc """
  Record `factory_id`'s partitioning facts, as its bind declared them.

  The one declaration keyed by FACTORY rather than relay class:
  partitionability is a property of the class's code, and every instance of a
  factory answers identically. In-memory only — a restarted controller simply
  learns again at the next bind, which is the same path a never-bound class
  takes, and the SDK's own refusal is the authority either way.
  """
  @spec put_partitioning(binary(), map()) :: :ok
  def put_partitioning(factory_id, %{keyless: keyless?, each: mode})
      when is_binary(factory_id) and is_boolean(keyless?) do
    ensure_table()
    :ets.insert(@table, {{factory_id, :partitioning}, %{keyless: keyless?, each: mode}})
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
      :error -> %{keyless: false, each: nil}
    end
  end

  @doc """
  Whether a steps relay with loop shape `each` reads one input only, which
  also keeps it from running partitioned.
  """
  @spec single_input?(each() | nil) :: boolean()
  def single_input?(each), do: each in [:once, :chained]

  @doc """
  Record the **established** value of `attribute` for `factory_id` in
  `namespace` — the one value every runner of that factory there must
  advertise: its SDK language (`:sdk_lang`), how it runs (`:run`) and a
  steps run's loop shape (`:each`) and failure mode (`:on_failure`).

  Scoped by namespace as well as factory, because a factory id names a class
  within a deployment, not across the cluster: the test fleet alone gives the
  same reference ids to every SDK, so a global record would let one namespace's
  Rust runner refuse another namespace's Java one.

  In-memory only, like `put_partitioning/2`: the durable header re-establishes the
  language at the next bind (`establish_sdk_lang_from_state/3`), the next
  advertisement re-establishes them all, and the SDK's own refusal is the backstop
  either way — so a restarted controller simply learns again.
  """
  @spec put_established(binary(), binary(), established(), term()) :: :ok
  def put_established(namespace, factory_id, attribute, value)
      when is_binary(namespace) and is_binary(factory_id) and attribute in @established do
    ensure_table()
    :ets.insert(@table, {{namespace, factory_id, attribute}, value})
    :ok
  end

  @doc """
  `factory_id`'s established `attribute` in `namespace`, or `nil` when none is
  established yet.
  """
  @spec established(binary(), binary(), established()) :: term()
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
  or to run the other way.
  """
  @spec delete_established(binary(), binary()) :: :ok
  def delete_established(namespace, factory_id)
      when is_binary(namespace) and is_binary(factory_id) do
    if :ets.whereis(@table) != :undefined do
      Enum.each(@established, &:ets.delete(@table, {namespace, factory_id, &1}))
    end

    :ok
  end

  @doc """
  Record that `instance_id` resolves to `relay_class` on disk.

  These are two different key spaces, not two names for one thing. A steps relay
  instance's task-journal lives under its INSTANCE id (`Paths.journal_id/1`),
  while a relay's Cells and input log live under the resolved CLASS
  (`:relay_class`, defaulting to the instance id). They coincide unless a
  user sets that option, which a steps relay is refused — and where they
  diverge, asking for one and getting the other returns half a relay's
  artifacts with no error, which is how a synchronous-durability barrier can
  pass having shipped almost nothing.

  Stored under both members so either answers, and in memory only: the callers
  are the commit path and hydration, both of which run against a live bind, and
  the shipping path enumerates directories rather than consulting this at all.
  """
  @spec put_state_keys(binary(), binary()) :: :ok
  def put_state_keys(instance_id, relay_class)
      when is_binary(instance_id) and is_binary(relay_class) do
    ensure_table()
    keys = Enum.uniq([instance_id, relay_class])
    :ets.insert(@table, {{instance_id, :state_keys}, keys})
    :ets.insert(@table, {{relay_class, :state_keys}, keys})
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
  Record whether `relay_class` keeps its work `durable: remote`: its commit
  barriers are remotely durable before they are acked
  (`remote-durability-tier`). Same ETS + durable-sidecar shape as the tier: the
  gate is consulted on the commit path of an instance that may have been bound
  by a previous controller.
  """
  @spec put_remote(binary(), boolean()) :: :ok
  def put_remote(relay_class, remote?)
      when is_binary(relay_class) and is_boolean(remote?) do
    put_declaration(relay_class, :remote, @remote_sidecar, remote?)
  end

  @doc "Where `relay_class` keeps its work, as its `durable` level names it."
  @spec durable_level(binary()) :: :remote | :local | :memory
  def durable_level(relay_class) when is_binary(relay_class) do
    cond do
      remote?(relay_class) -> :remote
      persistent?(relay_class) -> :local
      true -> :memory
    end
  end

  @doc """
  Whether `relay_class` keeps its work remotely: ETS → durable sidecar →
  `false` (the cost-free default).
  """
  @spec remote?(binary()) :: boolean()
  def remote?(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :remote, @remote_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Record `relay_class`'s declared per-Cell WAL compaction size threshold
  in bytes, or `nil` when it declares none (then the global default governs).

  Durable in the same sidecar shape as the correction window, because the
  Watchdog's compaction-size sweep resolves it with no runner to ask and may run
  while the instance is suspended.
  """
  @spec put_compact_after_bytes(binary(), pos_integer() | nil) :: :ok
  def put_compact_after_bytes(relay_class, bytes) when is_binary(relay_class) do
    put_declaration(relay_class, :compact_after_bytes, @compact_bytes_sidecar, bytes)
  end

  @doc """
  Record `relay_class`'s declared correction window in milliseconds, or `nil`
  when it declares none.

  Durable in the same sidecar shape as the tier, because the retention that a
  correction window REQUIRES is derived from it by two background sweeps — the
  input-log truncation and the snapshot age floor — neither of which has an
  Overwatch to ask, and both of which may run before the class is ever bound.
  """
  @spec put_correction_window(binary(), pos_integer() | nil) :: :ok
  def put_correction_window(relay_class, window_ms) when is_binary(relay_class) do
    put_declaration(relay_class, :correction_window_ms, @correction_sidecar, window_ms)
  end

  @doc """
  Record whether `relay_class` runs the effectively-once delivery tier.
  The sidecar exists only while the tier is on, so a tier-off class's on-disk
  layout is unchanged and a class that drops the declaration stops journaling
  on the next bind.
  """
  @spec put_effectively_once(binary(), boolean()) :: :ok
  def put_effectively_once(relay_class, tier?)
      when is_binary(relay_class) and is_boolean(tier?) do
    put_declaration(relay_class, :effectively_once, @tier_sidecar, tier?)
  end

  @doc """
  Record whether `relay_class` acknowledges its inputs only at an explicit
  checkpoint. Same ETS + durable-sidecar shape as the tier.
  """
  @spec put_ack_checkpoint(binary(), boolean()) :: :ok
  def put_ack_checkpoint(relay_class, checkpoint?)
      when is_binary(relay_class) and is_boolean(checkpoint?) do
    put_declaration(relay_class, :ack_checkpoint, @checkpoint_sidecar, checkpoint?)
  end

  @doc """
  Record which of `relay_class`'s chain tasklets declare a `compensate`
  hook, as declared by the SDK in the `AddRelay` response metadata. `nil`
  clears it (a class that declares nothing).

  Durable in the same sidecar shape as the tier, because a rewind is refused or
  allowed on this evidence while the instance may be suspended with no runner
  to ask.
  """
  @spec put_compensable_ops(binary(), [non_neg_integer()] | nil) :: :ok
  def put_compensable_ops(relay_class, tasks) when is_binary(relay_class) do
    put_declaration(relay_class, :compensable_tasks, @compensable_sidecar, tasks)
  end

  # Which of `relay_class`'s tree tasklets are leaves, whose commits mean the
  # relay is done: kept like the compensable set, so the journal answers
  # "complete?" with no runner to ask.
  defp put_leaf_ops(relay_class, tasks) when is_binary(relay_class) do
    put_declaration(relay_class, :leaf_tasks, @leaf_sidecar, tasks)
  end

  @doc """
  Resolve which of `relay_class`'s chain tasklets declare a `compensate`
  hook: ETS → durable sidecar → `nil` (the class declared nothing).
  """
  @spec compensable_tasks(binary()) :: [non_neg_integer()] | nil
  def compensable_tasks(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :compensable_tasks, @compensable_sidecar, &parse_ops/1)
  end

  @doc """
  Resolve `relay_class`'s leaf tasklets: ETS → durable sidecar → `nil`
  (the class declared nothing — a stream, or an SDK without the declaration).
  """
  @spec leaf_tasks(binary()) :: [non_neg_integer()] | nil
  def leaf_tasks(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :leaf_tasks, @leaf_sidecar, &parse_ops/1)
  end

  @doc """
  Record the live Overwatch pid for `relay_class` and the stable identity
  of the runner it is bound to. Written at bind,
  before the runner is told to start the relay.

  The State subsystem's fsync-gated outbox flush resolves the `:on_output`
  dispatch target through the pid, with no registry scan, and every door a
  runner's frame comes through reads the owner (`owner/1`).
  """
  @spec put_overwatch(binary(), pid(), binary() | nil) :: :ok
  def put_overwatch(relay_class, pid, owner)
      when is_binary(relay_class) and is_pid(pid) do
    ensure_table()
    :ets.insert(@table, {{relay_class, :overwatch}, pid, owner})
    :ok
  end

  @doc """
  The Overwatch pid and the owning identity recorded for `relay_class`,
  or `:error` when nothing is recorded on this node. The pid is answered
  whether or not it is still alive.
  """
  @spec owner(binary()) :: {:ok, pid(), binary() | nil} | :error
  def owner(relay_class) when is_binary(relay_class) do
    with true <- :ets.whereis(@table) != :undefined,
         [{_key, pid, owner}] <- :ets.lookup(@table, {relay_class, :overwatch}) do
      {:ok, pid, owner}
    else
      _ -> :error
    end
  end

  @doc "Every class whose Overwatch is alive on this node, sorted."
  @spec running() :: [binary()]
  def running() do
    if :ets.whereis(@table) == :undefined do
      []
    else
      @table
      |> :ets.select([{{{:"$1", :overwatch}, :"$2", :_}, [], [{{:"$1", :"$2"}}]}])
      |> Enum.flat_map(fn {class, pid} -> if Process.alive?(pid), do: [class], else: [] end)
      |> Enum.sort()
    end
  end

  @doc "Resolve the live Overwatch pid for `relay_class`."
  @spec overwatch_pid(binary()) :: {:ok, pid()} | :error
  def overwatch_pid(relay_class) when is_binary(relay_class) do
    case owner(relay_class) do
      {:ok, pid, _owner} -> if Process.alive?(pid), do: {:ok, pid}, else: :error
      :error -> :error
    end
  end

  @doc """
  Evict the in-memory entry. The durable sidecar is kept — finished-job state
  can outlive its process, and the process-less `Retention` sweep still needs
  the run. The sidecar is reclaimed with the class dir.
  """
  @spec delete(binary()) :: :ok
  def delete(relay_class) when is_binary(relay_class) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, relay_class)
      :ets.delete(@table, {relay_class, :overwatch})

      Enum.each(@declaration_keys, &:ets.delete(@table, {relay_class, &1}))
    end

    :ok
  end

  # ---------- reads ----------

  @doc "Resolve how `relay_class` runs: ETS → durable sidecar → `:flow`."
  @spec get(binary()) :: run()
  def get(relay_class) when is_binary(relay_class) do
    case ets_lookup(relay_class) do
      {:ok, run} -> run
      :error -> read_sidecar(relay_class) || @default_run
    end
  end

  @doc """
  Whether an instance of `relay_class` keeps a task journal, answered by
  how the class runs.
  """
  @spec journals?(binary()) :: boolean()
  def journals?(relay_class) when is_binary(relay_class),
    do: get(relay_class) == :steps

  @doc """
  Resolve `relay_class`'s effectively-once tier: ETS → durable
  sidecar → `false` (the cost-free default).
  """
  @spec effectively_once?(binary()) :: boolean()
  def effectively_once?(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :effectively_once, @tier_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Resolve `relay_class`'s checkpoint ack-granularity: ETS → durable
  sidecar → `false` (the per-input default).
  """
  @spec ack_checkpoint?(binary()) :: boolean()
  def ack_checkpoint?(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :ack_checkpoint, @checkpoint_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Resolve `relay_class`'s declared correction window in milliseconds: ETS →
  durable sidecar → `nil` (the class declared none). The retention floors it
  requires are derived from this — see `Flezha.State.InputLog.hold_window_ms/1`
  and `Flezha.State.PersistenceResolver.compact_after_age_ms/1`.
  """
  @spec correction_window_ms(binary()) :: pos_integer() | nil
  def correction_window_ms(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :correction_window_ms, @correction_sidecar, &parse_window/1)
  end

  @doc """
  Resolve `relay_class`'s declared compaction size threshold in bytes:
  ETS → durable sidecar → `nil` (the class declared none — the global default
  then governs). Reuses the positive-integer sidecar codec.
  """
  @spec compact_after_bytes(binary()) :: pos_integer() | nil
  def compact_after_bytes(relay_class) when is_binary(relay_class) do
    declaration(relay_class, :compact_after_bytes, @compact_bytes_sidecar, &parse_window/1)
  end

  # ---------- internals ----------

  # ONE shape for every SDK declaration: an ETS entry plus a durable sidecar that
  # outlives the process. An undeclared capability — `false` for a flag, `nil` for
  # a tasklet set — CLEARS both, so a class that rebinds having dropped a
  # declaration can never read the previous bind's, and a flag-off class's on-disk
  # layout stays exactly as it was before the flag existed. The only
  # per-capability code is the codec pair.
  defp put_declaration(relay_class, key, sidecar, undeclared)
       when undeclared in [nil, false] do
    ensure_table()
    # Cached as undeclared rather than evicted: a bound class answers from ETS
    # either way, so the read path never stats a sidecar that is not there.
    :ets.insert(@table, {{relay_class, key}, undeclared})
    _ = File.rm(sidecar_path(relay_class, sidecar))
    :ok
  end

  defp put_declaration(relay_class, key, sidecar, value) do
    ensure_table()
    :ets.insert(@table, {{relay_class, key}, value})
    write_sidecar_file(relay_class, sidecar, encode_declaration(value))
  end

  # ETS → durable sidecar → `nil` (undeclared). `decode` turns the sidecar's text
  # back into the declared value; an empty tasklet set decodes to `[]`, which is
  # meaningful ("a tree, none of it selected") and is NOT undeclared.
  @doc false
  def declaration(relay_class, key, sidecar, decode) do
    case ets_lookup({relay_class, key}) do
      {:ok, value} ->
        value

      :error ->
        case File.read(sidecar_path(relay_class, sidecar)) do
          {:ok, text} -> decode.(String.trim(text))
          {:error, _} -> nil
        end
    end
  end

  defp encode_declaration(true), do: "true"
  defp encode_declaration(tasks) when is_list(tasks), do: Enum.join(tasks, ",")
  defp encode_declaration(ms) when is_integer(ms), do: Integer.to_string(ms)

  defp ets_lookup(relay_class) do
    case :ets.whereis(@table) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@table, relay_class) do
          [{^relay_class, run}] -> {:ok, run}
          [] -> :error
        end
    end
  end

  defp sidecar_path(relay_class, filename \\ @sidecar),
    do: Path.join(Paths.class_dir(relay_class), filename)

  defp write_sidecar(relay_class, run),
    do: write_sidecar_file(relay_class, @sidecar, Atom.to_string(run))

  # A declaration whose sidecar did not land is logged and kept in memory: the
  # next bind writes it again.
  defp write_sidecar_file(relay_class, filename, content) do
    with {:error, reason} <- install_sidecar(relay_class, filename, content) do
      Logger.warning(
        "[relay] #{filename} sidecar write failed for #{relay_class}: #{inspect(reason)}"
      )
    end

    :ok
  end

  defp install_sidecar(relay_class, filename, content) do
    path = sidecar_path(relay_class, filename)
    Paths.atomic_install(path, &:file.write(&1, content), :class_kind)
  end

  defp read_sidecar(relay_class) do
    case File.read(sidecar_path(relay_class)) do
      {:ok, bin} -> parse_run(String.trim(bin))
      {:error, _} -> nil
    end
  end

  defp parse_run(str) do
    run = String.to_existing_atom(str)
    if known_run?(run), do: run, else: nil
  rescue
    ArgumentError -> nil
  end

  @doc "Whether `run` is how a relay runs."
  @spec known_run?(atom()) :: boolean()
  def known_run?(run), do: run in @runs
end
