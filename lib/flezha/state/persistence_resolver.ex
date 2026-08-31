defmodule Flezha.State.PersistenceResolver do
  @moduledoc """
  Resolves per-Operative persistence configuration from three sources, in order
  of precedence: a runner runtime override (`Application.get_env(:flezha, <key>)
  :: %{<OperativeClass> => value}`), the per-instance metadata shipped with
  `AddOperative`, then a hard-coded global default.

  One resolver per key, each named for the key it resolves. The keys and their
  defaults are `docs/internals/state_persistence.md`'s configuration table,
  which is the one place they are enumerated; the module's job is the
  precedence, not the catalogue.

  A resolver returns the resolved value and never `nil`: an absent override
  falls through to the metadata and then to the default, so every caller gets a
  usable value without repeating the fallback.
  """

  alias Flezha.AuthZ.NamespaceStore
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.Paths

  @default_persistence_enabled true
  @default_retention_hours 6
  @default_wal_high_watermark_bytes 16 * 1024 * 1024
  @default_compact_after_bytes 8 * 1024 * 1024
  @default_output_stall_timeout_ms 60_000
  @default_compaction_timeout_ms 30_000
  @default_compact_after_age_ms 300_000
  @default_workflow_completed_retention_hours 72
  @default_record_log_cut_after_bytes 64 * 1024

  @spec persistence_enabled?(binary(), map()) :: boolean()
  def persistence_enabled?(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:operative_persistence, :operative_persistence_enabled, operative_class, metadata,
      valid: &is_boolean/1,
      default: fn -> @default_persistence_enabled end
    )
  end

  @doc """
  Whether an operative of `kind` + `operative_class` has durable per-Entity
  state (WAL + snapshot + restore) — the kind+class resolution of the
  `persistent_state?` capability that gates bind-time restore (crash-resume)
  and `:get_entity_state` introspection:

    * `:stream` — always durable.
    * `:workflow` — durable **by default**, opt-out per class (same precedence
      as `persistence_enabled?/2`: operator override → instance metadata →
      default `true`).
    * any other kind — not durable.

  Distinct from the pure per-kind `Flezha.Operative.Kind.capable?/2` flags
  (`time_travel?`, `dynamic_entities?`, `output_stall_watch?`) which a durable
  workflow operative does NOT inherit.
  """
  @spec persistent_state?(atom(), binary(), map()) :: boolean()
  def persistent_state?(kind, operative_class, metadata \\ %{})

  def persistent_state?(:stream, _operative_class, _metadata), do: true

  def persistent_state?(:workflow, operative_class, metadata)
      when is_binary(operative_class),
      do: persistence_enabled?(operative_class, metadata)

  def persistent_state?(_kind, _operative_class, _metadata), do: false

  @spec retention_hours(binary(), map()) :: non_neg_integer()
  def retention_hours(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:time_travel_retention_hours, :time_travel_retention_hours, operative_class, metadata,
      valid: &non_neg_int?/1,
      default: fn ->
        Application.get_env(
          :flezha,
          :default_time_travel_retention_hours,
          @default_retention_hours
        )
      end
    )
  end

  @doc """
  How many hours a finished workflow operative's data (retained Entity state +
  completion record) is kept before the retention sweeper reclaims it.
  Precedence: operator override map `:workflow_completed_retention_hours` →
  instance metadata → global `:default_workflow_completed_retention_hours`
  (`72`, env `FLEZHA_WORKFLOW_RETENTION_HOURS`).
  """
  @spec completed_retention_hours(binary(), map()) :: non_neg_integer()
  def completed_retention_hours(operative_class, metadata \\ %{})
      when is_binary(operative_class) do
    resolve(
      :workflow_completed_retention_hours,
      :workflow_completed_retention_hours,
      operative_class,
      metadata,
      valid: &non_neg_int?/1,
      default: fn ->
        Application.get_env(
          :flezha,
          :default_workflow_completed_retention_hours,
          @default_workflow_completed_retention_hours
        )
      end
    )
  end

  @doc """
  The per-Entity WAL size at which the Watchdog triggers a compaction, per
  operative class. Precedence (moduledoc): operator override map
  `:compact_after_bytes` → the SDK-declared value the class shipped in its
  `AddOperative` metadata (persisted in `ClassRegistry`, resolved here the way
  `compact_after_age_ms/2` resolves a declared correction window) →
  instance-metadata argument → global `:default_compact_after_bytes` (8 MiB).

  The SDK declaration is consulted through `ClassRegistry` rather than the
  `metadata` argument, because the Watchdog — the only caller — resolves this
  with only the class in hand.
  """
  @spec compact_after_bytes(binary(), map()) :: pos_integer()
  def compact_after_bytes(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:compact_after_bytes, :compact_after_bytes, operative_class, metadata,
      valid: &(is_integer(&1) and &1 > 0),
      default: fn -> declared_compact_after_bytes(operative_class) end
    )
  end

  # SDK-declared threshold (durable in ClassRegistry) → global default.
  defp declared_compact_after_bytes(operative_class) do
    case ClassRegistry.compact_after_bytes(operative_class) do
      v when is_integer(v) and v > 0 ->
        v

      _ ->
        Application.get_env(:flezha, :default_compact_after_bytes, @default_compact_after_bytes)
    end
  end

  @doc """
  Whether `operative_class` requires its commit barriers to reach the remote
  durability tier before they are acked (`remote-durability-tier`, RPO 0).

  Same three-source cascade as every other per-class knob, with the SDK
  declaration as the default — so an operator can force a class either way
  (`:sync_durability => %{class => bool}`) without an SDK change, and a class
  that declares nothing costs nothing.
  """
  @spec sync_durability?(binary(), map()) :: boolean()
  def sync_durability?(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:sync_durability, :sync_durability, operative_class, metadata,
      valid: &is_boolean/1,
      default: fn -> ClassRegistry.sync_durability?(operative_class) end
    )
  end

  @spec output_stall_timeout_ms(binary(), map()) :: non_neg_integer()
  def output_stall_timeout_ms(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:output_stall_timeout_ms, :output_stall_timeout_ms, operative_class, metadata,
      valid: &non_neg_int?/1,
      default: fn ->
        Application.get_env(
          :flezha,
          :default_output_stall_timeout_ms,
          @default_output_stall_timeout_ms
        )
      end
    )
  end

  @spec credit_window_bytes(binary(), map()) :: pos_integer()
  def credit_window_bytes(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:credit_window_bytes, :credit_window_bytes, operative_class, metadata,
      valid: &(is_integer(&1) and &1 > 0),
      default: &wal_high_watermark_bytes/0
    )
    |> cap_to_namespace_mailbox_quota(operative_class)
  end

  # Per-namespace mailbox-byte quota, folded into the same cascade every
  # buffered-byte window resolves through: while authz is enforced, no single
  # Operative's credit window may exceed its namespace's
  # `max_mailbox_bytes_total` (finish-platform-authn-authz §6.3).
  defp cap_to_namespace_mailbox_quota(window, operative_class) do
    if Flezha.AuthZ.enabled?() do
      namespace = Paths.namespace_of_class(operative_class)

      case NamespaceStore.get(namespace) do
        {:ok, %{quotas: %{max_mailbox_bytes_total: limit}}}
        when is_integer(limit) and limit > 0 ->
          min(window, limit)

        _ ->
          window
      end
    else
      window
    end
  rescue
    _ -> window
  catch
    _, _ -> window
  end

  @spec compaction_timeout_ms() :: non_neg_integer()
  def compaction_timeout_ms(),
    do: global_non_neg(:compaction_timeout_ms, @default_compaction_timeout_ms)

  @doc """
  The WAL soft watermark (`:wal_high_watermark_bytes`) — the one definition of
  "how many bytes is too many for one Entity-scale payload".

  It bounds the per-Entity WAL, the input log, a workflow journal entry, the
  default credit window, and a live state read, so it lives here rather than as
  a literal at each of those.
  """
  @spec wal_high_watermark_bytes() :: pos_integer()
  def wal_high_watermark_bytes(),
    do: Application.get_env(:flezha, :wal_high_watermark_bytes, @default_wal_high_watermark_bytes)

  @doc """
  WAL age at which the Watchdog triggers a compaction, per operative.

  Per-operative rather than global because it stopped being a tuning knob: a
  correction rewinds inside its declared window, and compaction truncates the
  WAL below each snapshot — so an age-floor shorter than the correction window
  destroys the very history the rewind needs a baseline inside. A declared
  correction window therefore RAISES this floor to at least itself.

  `0` disables age-triggered compaction entirely — WAL history is then retained
  without an age bound, which already satisfies any window, so `0` is preserved
  rather than raised. The polarity is the opposite of the input-log hold window's
  and conflating the two is the one way to get this wrong: raising `0` here would
  *enable* compaction and make retention worse.
  """
  @spec compact_after_age_ms(binary(), map()) :: non_neg_integer()
  def compact_after_age_ms(operative_class \\ nil, metadata \\ %{})

  def compact_after_age_ms(nil, _metadata),
    do: global_non_neg(:compact_after_age_ms, @default_compact_after_age_ms)

  def compact_after_age_ms(operative_class, metadata) when is_binary(operative_class) do
    case configured_compact_after_age_ms(operative_class, metadata) do
      0 -> 0
      n -> max(n, ClassRegistry.correction_window_ms(operative_class) || 0)
    end
  end

  @doc """
  The age floor BEFORE a declared correction window raises it — what the operator
  actually configured. For announcing a derivation, never for compacting against.
  """
  @spec configured_compact_after_age_ms(binary(), map()) :: non_neg_integer()
  def configured_compact_after_age_ms(operative_class, metadata \\ %{})
      when is_binary(operative_class) do
    resolve(:compact_after_age_ms, :compact_after_age_ms, operative_class, metadata,
      valid: &(is_integer(&1) and &1 >= 0),
      default: fn -> global_non_neg(:compact_after_age_ms, @default_compact_after_age_ms) end
    )
  end

  @doc """
  How many released bytes a per-instance record log accumulates before a
  prefix cut rewrites it (`:record_log_cut_after_bytes`). A cut is asked for at
  every iteration commit; the rewrite — a copy, an fsync and a rename — is paid
  once per this many bytes, so a loop's logs stay bounded without a rewrite
  per turn.
  """
  @spec record_log_cut_after_bytes() :: non_neg_integer()
  def record_log_cut_after_bytes(),
    do: global_non_neg(:record_log_cut_after_bytes, @default_record_log_cut_after_bytes)

  # Global (non-per-Operative) knob: app env if a valid non-negative
  # integer, else the compile-time default.
  defp global_non_neg(key, default) do
    case Application.get_env(:flezha, key, default) do
      v when is_integer(v) and v >= 0 -> v
      _ -> default
    end
  end

  # Shared three-source cascade (moduledoc precedence): operator override
  # map -> AddOperative instance metadata (string or atom key) -> default.
  # `valid:` gates each source; an invalid value falls through to the next.
  defp resolve(override_key, meta_key, operative_class, metadata, opts) do
    valid? = Keyword.fetch!(opts, :valid)
    default = Keyword.fetch!(opts, :default)

    with :error <- validated(operator_override(override_key, operative_class), valid?),
         :error <- validated(metadata_value(metadata, meta_key), valid?) do
      default.()
    else
      {:ok, v} -> v
    end
  end

  defp metadata_value(metadata, meta_key) do
    case Map.get(metadata, Atom.to_string(meta_key), Map.get(metadata, meta_key)) do
      nil -> :error
      v -> {:ok, v}
    end
  end

  defp validated({:ok, v}, valid?), do: if(valid?.(v), do: {:ok, v}, else: :error)
  defp validated(:error, _valid?), do: :error

  defp non_neg_int?(v), do: is_integer(v) and v >= 0

  defp operator_override(key, operative_class) do
    case Application.get_env(:flezha, key) do
      %{} = m ->
        case Map.fetch(m, operative_class) do
          {:ok, v} -> {:ok, v}
          :error -> :error
        end

      _ ->
        :error
    end
  end
end
