defmodule Flezha.Operative.ClassRegistry do
  @moduledoc """
  Resolves an `operative_class` to its `kind`, its declarations and its
  effectively-once tier, for paths that carry only the class string.

  `Retention`, `Watchdog`, `StreamChannel` and `TimeTravel` are keyed purely by
  `operative_class` and never carry a kind; this registry is the seam that lets
  them gate the stream-only capabilities through `Kind.capable?/2`.

  `get/1` resolves in three steps: the in-memory ETS table (written at bind,
  evicted at teardown), a durable per-class sidecar under `Paths.class_dir/1`
  so a process-less sweep still resolves the kind, then the default `:stream`.

  Invariant: the sidecar is written only for non-`:stream` kinds, so a stream
  operative's on-disk layout is unchanged and every `Paths` scanner ignores it.
  """

  alias Flezha.Operative.Kind
  alias Flezha.State.Paths

  require Logger

  @table :flezha_operative_class_registry
  @default_kind :stream
  @sidecar "kind"
  @tier_sidecar "effectively_once"
  @compensable_sidecar "compensable_procs"
  @leaf_sidecar "leaf_procs"
  @checkpoint_sidecar "checkpoint_ack"
  @correction_sidecar "correction_window_ms"
  @compact_bytes_sidecar "compact_after_bytes"
  @sync_durability_sidecar "sync_durability"

  # Every per-class SDK declaration, keyed `{operative_class, key}` in the ETS
  # table. `delete/1` evicts them all, so a class that rebinds without a
  # declaration cannot read a stale one.
  @declaration_keys [
    :effectively_once,
    :checkpoint_ack,
    :compensable_procs,
    :leaf_procs,
    :correction_window_ms,
    :compact_after_bytes,
    :sync_durability
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
  Record `operative_class`'s `kind`: an in-memory ETS entry plus, for a
  non-`:stream` kind, a durable sidecar so a process-less `Retention`
  sweep resolves the kind after teardown. Called at `Overwatch` bind.
  """
  @spec put(binary(), atom()) :: :ok
  def put(operative_class, kind) when is_binary(operative_class) and is_atom(kind) do
    ensure_table()
    :ets.insert(@table, {operative_class, kind})
    if kind != @default_kind, do: write_sidecar(operative_class, kind)
    :ok
  end

  # ---------- declarations: decode ----------

  @typedoc """
  A class's SDK-side declarations, as resolved from the `AddOperative` response.

  `compensable_procs` is the chain positions whose procedure declares a `compensate`
  hook, or `nil` when the class declares nothing (run-sugar, stream, or a Java
  runner) — which a rewind reads as "no coverage information", not as "nothing
  is compensable".
  """
  @type declarations :: %{
          effectively_once: boolean(),
          keyless: boolean(),
          checkpoint_ack: boolean(),
          compensable_procs: [non_neg_integer()] | nil,
          leaf_procs: [non_neg_integer()] | nil,
          correction_window_ms: pos_integer() | nil,
          compact_after_bytes: pos_integer() | nil,
          sync_durability: boolean()
        }

  @doc """
  Resolve a class's SDK-side declarations from the `AddOperative` response
  raw_data — an `AddOperativeResponse` metadata map when the class declares
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
      # entry-fed procedure, which cannot be partitioned.
      keyless: Map.get(metadata, "keyed") == "false",
      checkpoint_ack: Map.get(metadata, "ack_granularity") == "checkpoint",
      compensable_procs: parse_ops(Map.get(metadata, "compensable_procs")),
      leaf_procs: parse_ops(Map.get(metadata, "leaf_procs")),
      correction_window_ms: parse_window(Map.get(metadata, "correction_window_ms")),
      compact_after_bytes: parse_window(Map.get(metadata, "compact_after_bytes")),
      sync_durability: Map.get(metadata, "durability") == "sync_remote"
    }
  end

  defp decode_metadata(raw_data) when is_binary(raw_data) and raw_data != "" do
    case RunnerCommands.AddOperativeResponse.decode(raw_data) do
      {:ok, %RunnerCommands.AddOperativeResponse{metadata: metadata}} -> metadata
      {:error, _} -> %{}
    end
  end

  defp decode_metadata(_raw_data), do: %{}

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
  Persist every SDK-declared capability for `operative_class`, as decoded from
  the `AddOperative` response by `decode_declarations/1`.

  Each rides an ETS entry plus a durable sidecar, so a process-less `Retention`
  sweep — or a bind that happens while the instance is suspended — resolves it
  with no runner to ask. A declaration a class drops is cleared on its next
  bind, so the sidecar never outlives the claim.

  Together with `decode_declarations/1` this is the whole recipe for a new
  SDK-declared capability: decode it there, persist it here.
  """
  @spec put_declarations(binary(), map()) :: :ok
  def put_declarations(operative_class, declarations) when is_binary(operative_class) do
    put_effectively_once(operative_class, declarations.effectively_once)
    put_checkpoint_ack(operative_class, declarations.checkpoint_ack)
    put_compensable_ops(operative_class, declarations.compensable_procs)
    put_leaf_ops(operative_class, declarations.leaf_procs)
    put_correction_window(operative_class, declarations.correction_window_ms)
    put_compact_after_bytes(operative_class, declarations.compact_after_bytes)
    put_sync_durability(operative_class, declarations.sync_durability)
  end

  @doc """
  Record whether `factory_id`'s topology has no keyed entry-fed procedure.

  The one declaration keyed by FACTORY rather than operative class:
  partitionability is a property of the class's code, and every instance of a
  factory answers identically. In-memory only — a restarted controller simply
  learns again at the next bind, which is the same path a never-bound class
  takes, and the SDK's own refusal is the authority either way.
  """
  @spec put_keyless(binary(), boolean()) :: :ok
  def put_keyless(factory_id, keyless?) when is_binary(factory_id) and is_boolean(keyless?) do
    ensure_table()
    :ets.insert(@table, {{factory_id, :keyless}, keyless?})
    :ok
  end

  @doc """
  Whether `factory_id` is known to have no keyed entry-fed procedure. `false`
  for a class that has never bound — the bind itself then refuses.
  """
  @spec keyless?(binary()) :: boolean()
  def keyless?(factory_id) when is_binary(factory_id) do
    ets_lookup({factory_id, :keyless}) == {:ok, true}
  end

  def keyless?(_factory_id), do: false

  @doc """
  Record the **established SDK language** of `factory_id` in `namespace` — the
  one language every runner of that factory there must speak.

  Scoped by namespace as well as factory, because a factory id names a class
  within a deployment, not across the cluster: the test fleet alone gives the
  same reference ids to every SDK, so a global record would let one namespace's
  Rust runner refuse another namespace's Java one.

  In-memory only, like `put_keyless/2`: the durable header re-establishes it at
  the next bind (`establish_sdk_lang_from_state/3`), and the SDK's own refusal
  is the backstop either way — so a restarted controller simply learns again.
  """
  @spec put_sdk_lang(binary(), binary(), byte()) :: :ok
  def put_sdk_lang(namespace, factory_id, lang)
      when is_binary(namespace) and is_binary(factory_id) and is_integer(lang) and lang > 0 do
    ensure_table()
    :ets.insert(@table, {{namespace, factory_id, :sdk_lang}, lang})
    :ok
  end

  @doc """
  `factory_id`'s established SDK language in `namespace`, or `nil` when none is
  established yet.
  """
  @spec sdk_lang(binary(), binary()) :: byte() | nil
  def sdk_lang(namespace, factory_id) when is_binary(namespace) and is_binary(factory_id) do
    case ets_lookup({namespace, factory_id, :sdk_lang}) do
      {:ok, lang} -> lang
      :error -> nil
    end
  end

  def sdk_lang(_namespace, _factory_id), do: nil

  @doc """
  Forget the established language of `factory_id` in `namespace`, so the next
  advertisement establishes it afresh.

  The escape hatch from a factory deliberately re-authored in another language.
  """
  @spec delete_sdk_lang(binary(), binary()) :: :ok
  def delete_sdk_lang(namespace, factory_id)
      when is_binary(namespace) and is_binary(factory_id) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, {namespace, factory_id, :sdk_lang})
    end

    :ok
  end

  @doc """
  Record that `instance_id` resolves to `operative_class` on disk.

  These are two different key spaces, not two names for one thing. A workflow
  instance's proc-journal and durable inbox live under its INSTANCE id
  (`Paths.journal_id/1`), while its Entities and input log live under the
  resolved CLASS (`:operative_class`, defaulting to the instance id). They
  coincide unless an operator sets that option — and where they diverge, asking
  for one and getting the other returns half an operative's artifacts with no
  error, which is how a synchronous-durability barrier can pass having shipped
  almost nothing.

  Stored under both members so either answers, and in memory only: the callers
  are the commit path and hydration, both of which run against a live bind, and
  the shipping path enumerates directories rather than consulting this at all.
  """
  @spec put_state_keys(binary(), binary()) :: :ok
  def put_state_keys(instance_id, operative_class)
      when is_binary(instance_id) and is_binary(operative_class) do
    ensure_table()
    keys = Enum.uniq([instance_id, operative_class])
    :ets.insert(@table, {{instance_id, :state_keys}, keys})
    :ets.insert(@table, {{operative_class, :state_keys}, keys})
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
  Record whether `operative_class` requires its commit barriers to be remotely
  durable before they are acked (`remote-durability-tier`). Same ETS +
  durable-sidecar shape as the tier: the gate is consulted on the commit path
  of an instance that may have been bound by a previous controller.
  """
  @spec put_sync_durability(binary(), boolean()) :: :ok
  def put_sync_durability(operative_class, sync?)
      when is_binary(operative_class) and is_boolean(sync?) do
    put_declaration(operative_class, :sync_durability, @sync_durability_sidecar, sync?)
  end

  @doc """
  Resolve `operative_class`'s synchronous-durability declaration: ETS →
  durable sidecar → `false` (the cost-free default).
  """
  @spec sync_durability?(binary()) :: boolean()
  def sync_durability?(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :sync_durability, @sync_durability_sidecar, &(&1 == "true")) ==
      true
  end

  @doc """
  Record `operative_class`'s declared per-Entity WAL compaction size threshold
  in bytes, or `nil` when it declares none (then the global default governs).

  Durable in the same sidecar shape as the correction window, because the
  Watchdog's compaction-size sweep resolves it with no runner to ask and may run
  while the instance is suspended.
  """
  @spec put_compact_after_bytes(binary(), pos_integer() | nil) :: :ok
  def put_compact_after_bytes(operative_class, bytes) when is_binary(operative_class) do
    put_declaration(operative_class, :compact_after_bytes, @compact_bytes_sidecar, bytes)
  end

  @doc """
  Record `operative_class`'s declared correction window in milliseconds, or `nil`
  when it declares none.

  Durable in the same sidecar shape as the tier, because the retention that a
  correction window REQUIRES is derived from it by two background sweeps — the
  input-log truncation and the snapshot age floor — neither of which has an
  Overwatch to ask, and both of which may run before the class is ever bound.
  """
  @spec put_correction_window(binary(), pos_integer() | nil) :: :ok
  def put_correction_window(operative_class, window_ms) when is_binary(operative_class) do
    put_declaration(operative_class, :correction_window_ms, @correction_sidecar, window_ms)
  end

  @doc """
  Record whether `operative_class` runs the effectively-once delivery tier.
  The sidecar exists only while the tier is on, so a tier-off class's on-disk
  layout is unchanged and a class that drops the declaration stops journaling
  on the next bind.
  """
  @spec put_effectively_once(binary(), boolean()) :: :ok
  def put_effectively_once(operative_class, tier?)
      when is_binary(operative_class) and is_boolean(tier?) do
    put_declaration(operative_class, :effectively_once, @tier_sidecar, tier?)
  end

  @doc """
  Record whether `operative_class` acknowledges its inputs only at an explicit
  checkpoint. Same ETS + durable-sidecar shape as the tier.
  """
  @spec put_checkpoint_ack(binary(), boolean()) :: :ok
  def put_checkpoint_ack(operative_class, checkpoint?)
      when is_binary(operative_class) and is_boolean(checkpoint?) do
    put_declaration(operative_class, :checkpoint_ack, @checkpoint_sidecar, checkpoint?)
  end

  @doc """
  Record which of `operative_class`'s chain procedures declare a `compensate`
  hook, as declared by the SDK in the `AddOperative` response metadata. `nil`
  clears it (a class that declares nothing).

  Durable in the same sidecar shape as the tier, because a rewind is refused or
  allowed on this evidence while the instance may be suspended with no runner
  to ask.
  """
  @spec put_compensable_ops(binary(), [non_neg_integer()] | nil) :: :ok
  def put_compensable_ops(operative_class, procs) when is_binary(operative_class) do
    put_declaration(operative_class, :compensable_procs, @compensable_sidecar, procs)
  end

  @doc """
  Record which of `operative_class`'s tree procedures are **leaves** — the set
  whose commits mean the operative is done (`workflow-branching`). Same ETS +
  durable-sidecar shape as the compensable set, and read on the same evidence:
  the journal answers "complete?" with no runner to ask.
  """
  @spec put_leaf_ops(binary(), [non_neg_integer()] | nil) :: :ok
  def put_leaf_ops(operative_class, procs) when is_binary(operative_class) do
    put_declaration(operative_class, :leaf_procs, @leaf_sidecar, procs)
  end

  @doc """
  Resolve which of `operative_class`'s chain procedures declare a `compensate`
  hook: ETS → durable sidecar → `nil` (the class declared nothing).
  """
  @spec compensable_procs(binary()) :: [non_neg_integer()] | nil
  def compensable_procs(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :compensable_procs, @compensable_sidecar, &parse_ops/1)
  end

  @doc """
  Resolve `operative_class`'s leaf procedures: ETS → durable sidecar → `nil`
  (the class declared nothing — run-sugar, stream, or an SDK without the
  declaration).
  """
  @spec leaf_procs(binary()) :: [non_neg_integer()] | nil
  def leaf_procs(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :leaf_procs, @leaf_sidecar, &parse_ops/1)
  end

  @doc """
  Record the live Overwatch pid for `operative_class` (called at bind, from
  the Overwatch process). The State subsystem's fsync-gated outbox flush
  resolves the `:on_output` dispatch target through this — no registry scan.
  """
  @spec put_overwatch(binary(), pid()) :: :ok
  def put_overwatch(operative_class, pid) when is_binary(operative_class) and is_pid(pid) do
    ensure_table()
    :ets.insert(@table, {{operative_class, :overwatch}, pid})
    :ok
  end

  @doc "Resolve the live Overwatch pid for `operative_class`."
  @spec overwatch_pid(binary()) :: {:ok, pid()} | :error
  def overwatch_pid(operative_class) when is_binary(operative_class) do
    case ets_lookup({operative_class, :overwatch}) do
      {:ok, pid} when is_pid(pid) ->
        if Process.alive?(pid), do: {:ok, pid}, else: :error

      _ ->
        :error
    end
  end

  @doc """
  Evict the in-memory entry. The durable sidecar is kept — finished-job state
  can outlive its process, and the process-less `Retention` sweep still needs
  the kind. The sidecar is reclaimed with the class dir.
  """
  @spec delete(binary()) :: :ok
  def delete(operative_class) when is_binary(operative_class) do
    if :ets.whereis(@table) != :undefined do
      :ets.delete(@table, operative_class)
      :ets.delete(@table, {operative_class, :overwatch})

      for key <- @declaration_keys do
        :ets.delete(@table, {operative_class, key})
      end
    end

    :ok
  end

  # ---------- reads ----------

  @doc "Resolve `operative_class`'s kind: ETS → durable sidecar → `:stream`."
  @spec get(binary()) :: atom()
  def get(operative_class) when is_binary(operative_class) do
    case ets_lookup(operative_class) do
      {:ok, kind} -> kind
      :error -> read_sidecar(operative_class) || @default_kind
    end
  end

  @doc "Whether `operative_class`'s resolved kind declares `capability`."
  @spec capable?(binary(), Kind.capability()) :: boolean()
  def capable?(operative_class, capability) when is_binary(operative_class) do
    Kind.capable?(get(operative_class), capability)
  end

  @doc """
  Resolve `operative_class`'s effectively-once tier: ETS → durable
  sidecar → `false` (the cost-free default).
  """
  @spec effectively_once?(binary()) :: boolean()
  def effectively_once?(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :effectively_once, @tier_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Resolve `operative_class`'s checkpoint ack-granularity: ETS → durable
  sidecar → `false` (the per-input default).
  """
  @spec checkpoint_ack?(binary()) :: boolean()
  def checkpoint_ack?(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :checkpoint_ack, @checkpoint_sidecar, &(&1 == "true")) == true
  end

  @doc """
  Resolve `operative_class`'s declared correction window in milliseconds: ETS →
  durable sidecar → `nil` (the class declared none). The retention floors it
  requires are derived from this — see `Flezha.State.InputLog.hold_window_ms/1`
  and `Flezha.State.PersistenceResolver.compact_after_age_ms/1`.
  """
  @spec correction_window_ms(binary()) :: pos_integer() | nil
  def correction_window_ms(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :correction_window_ms, @correction_sidecar, &parse_window/1)
  end

  @doc """
  Resolve `operative_class`'s declared compaction size threshold in bytes:
  ETS → durable sidecar → `nil` (the class declared none — the global default
  then governs). Reuses the positive-integer sidecar codec.
  """
  @spec compact_after_bytes(binary()) :: pos_integer() | nil
  def compact_after_bytes(operative_class) when is_binary(operative_class) do
    declaration(operative_class, :compact_after_bytes, @compact_bytes_sidecar, &parse_window/1)
  end

  # ---------- internals ----------

  # A declared boolean: ETS while the class is bound, a sidecar that exists
  # only while the flag is on (so an undeclaring class's on-disk layout is
  # unchanged, and one that drops the declaration reverts on the next bind).
  # ONE shape for every SDK declaration: an ETS entry plus a durable sidecar that
  # outlives the process. An undeclared capability — `false` for a flag, `nil` for
  # a procedure set — CLEARS both, so a class that rebinds having dropped a
  # declaration can never read the previous bind's, and a flag-off class's on-disk
  # layout stays exactly as it was before the flag existed. The only
  # per-capability code is the codec pair.
  defp put_declaration(operative_class, key, sidecar, undeclared)
       when undeclared in [nil, false] do
    ensure_table()
    # Cached as undeclared rather than evicted: a bound class answers from ETS
    # either way, so the read path never stats a sidecar that is not there.
    :ets.insert(@table, {{operative_class, key}, undeclared})
    _ = File.rm(sidecar_path(operative_class, sidecar))
    :ok
  end

  defp put_declaration(operative_class, key, sidecar, value) do
    ensure_table()
    :ets.insert(@table, {{operative_class, key}, value})
    write_sidecar_file(operative_class, sidecar, encode_declaration(value))
  end

  # ETS → durable sidecar → `nil` (undeclared). `decode` turns the sidecar's text
  # back into the declared value; an empty procedure set decodes to `[]`, which is
  # meaningful ("a tree, none of it selected") and is NOT undeclared.
  defp declaration(operative_class, key, sidecar, decode) do
    case ets_lookup({operative_class, key}) do
      {:ok, value} ->
        value

      :error ->
        case File.read(sidecar_path(operative_class, sidecar)) do
          {:ok, text} -> decode.(String.trim(text))
          {:error, _} -> nil
        end
    end
  end

  defp encode_declaration(true), do: "true"
  defp encode_declaration(procs) when is_list(procs), do: Enum.join(procs, ",")
  defp encode_declaration(ms) when is_integer(ms), do: Integer.to_string(ms)

  defp ets_lookup(operative_class) do
    case :ets.whereis(@table) do
      :undefined ->
        :error

      _ ->
        case :ets.lookup(@table, operative_class) do
          [{^operative_class, kind}] -> {:ok, kind}
          [] -> :error
        end
    end
  end

  defp sidecar_path(operative_class, filename \\ @sidecar),
    do: Path.join(Paths.class_dir(operative_class), filename)

  defp write_sidecar(operative_class, kind),
    do: write_sidecar_file(operative_class, @sidecar, Atom.to_string(kind))

  defp write_sidecar_file(operative_class, filename, content) do
    path = sidecar_path(operative_class, filename)
    tmp = path <> ".tmp"

    with :ok <- File.mkdir_p(Path.dirname(path)),
         :ok <- File.write(tmp, content),
         :ok <- File.rename(tmp, path) do
      :ok
    else
      {:error, reason} ->
        _ = File.rm(tmp)

        Logger.warning(
          "[operative] #{filename} sidecar write failed for #{operative_class}: #{inspect(reason)}"
        )

        :ok
    end
  end

  defp read_sidecar(operative_class) do
    case File.read(sidecar_path(operative_class)) do
      {:ok, bin} -> parse_kind(String.trim(bin))
      {:error, _} -> nil
    end
  end

  defp parse_kind(str) do
    kind = String.to_existing_atom(str)
    if Kind.known?(kind), do: kind, else: nil
  rescue
    ArgumentError -> nil
  end
end
