defmodule Flezha.Operative.ClassRegistry do
  @moduledoc """
  Resolves an `operative_class` to its `kind` — and its effectively-once
  delivery tier — for the State subsystem.

  `Flezha.State.Retention`, `Flezha.State.Watchdog`,
  `Flezha.Runner.StreamChannel`, and `Flezha.State.TimeTravel` are keyed
  purely by `operative_class` strings and never carry a `kind`. This registry
  is the seam that lets those paths gate the stream-only capabilities
  (`time_travel?`, `dynamic_entities?`, `output_stall_watch?`) through
  `Flezha.Operative.Kind.capable?/2`.

  `get/1` resolves in three steps:

    1. the in-memory ETS table — populated at `Flezha.Operative.Overwatch`
       bind, evicted at teardown;
    2. a durable per-class `kind` sidecar under
       `<state_root>/<operative_class>/` — so a process-less `Retention` sweep
       (which walks class dirs on disk with no live Overwatch) still resolves
       the kind after teardown, while finished-job state is retained;
    3. the default `:stream` — every class was stream before workflow
       operatives gained durable state, so an unknown class keeps the full
       stream behaviour and gating is only ever *subtractive*.

  The sidecar is written only for non-`:stream` kinds, so a stream
  operative's on-disk layout is unchanged. Like `Flezha.State.Completion`, the
  `kind` file is inert to every `Flezha.State.Registry` scanner.
  """

  alias Flezha.Operative.Kind
  alias Flezha.State.Registry

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

  # Every per-class SDK declaration, keyed `{operative_class, key}` in the ETS
  # table. `delete/1` evicts them all, so a class that rebinds without a
  # declaration cannot read a stale one.
  @declaration_keys [
    :effectively_once,
    :checkpoint_ack,
    :compensable_procs,
    :leaf_procs,
    :correction_window_ms,
    :compact_after_bytes
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

  @doc """
  Persist every SDK-declared capability for `operative_class`, as decoded from
  the `AddOperative` response by `Flezha.Operative.Overwatch.decode_declarations/1`.

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

  defp parse_ops(""), do: []
  defp parse_ops(csv), do: csv |> String.split(",") |> Enum.map(&String.to_integer/1)

  defp parse_window(text) do
    case Integer.parse(text) do
      {ms, ""} when ms > 0 -> ms
      _ -> nil
    end
  end

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
    do: Path.join([Registry.state_root(), operative_class, filename])

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
