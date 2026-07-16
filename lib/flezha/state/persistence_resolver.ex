defmodule Flezha.State.PersistenceResolver do
  @moduledoc """
  Resolves per-Operative persistence configuration from three sources, in
  order of precedence:

    1. Runner runtime override:
       `Application.get_env(:flezha, <key>) :: %{<OperativeClass> => value}`
    2. Per-instance metadata shipped with `AddOperative`
       (passed in as the `metadata` argument).
    3. Hard-coded global default.

  Configuration keys:

    * `:operative_persistence` — `%{<OperativeClass> => boolean}`. Default `true`.
      Resolves both `persistence_enabled?/2` and, for `:bounded`,
      `persistent_state?/3` (durable-by-default, opt-out per class).
    * `:time_travel_retention_hours` — `%{<OperativeClass> => non_neg_integer}`.
       Global default `6`. `0` disables time-travel for that Operative class.
    * `:bounded_completed_retention_hours` — `%{<OperativeClass> => non_neg_integer}`
       per-class override for how long a finished bounded operative's data is
       retained; global default `:default_bounded_completed_retention_hours`
       (`72`, env `FLEZHA_BOUNDED_RETENTION_HOURS`).
    * `:compact_after_bytes` — per-Operation threshold, sourced from SDK
       metadata first; global default `8 * 1024 * 1024`.
    * `:output_stall_timeout_ms` — `%{<OperativeClass> => non_neg_integer}`.
       Global default `60_000`. `0` disables the output-staleness
       watchdog for that Operative class.
    * `:compaction_timeout_ms` — global; the bound on the
       `CompactSegment` → `segment_state_snapshot` handshake.
       Default `30_000`. `0` disables the compaction-timeout sweep.
    * `:compact_after_age_ms` — global; a Segment whose oldest
       un-snapshotted WAL record is older than this is compacted even
       if it never reaches `compact_after_bytes`. Default `300_000`.
       `0` disables the age trigger.
    * `:credit_window_bytes` — `%{<OperativeClass> => pos_integer}`.
       Byte window offered in `CreditWindowInit` for a state stream whose
       first Change belongs to that class. Global default
       `:wal_high_watermark_bytes` (16 MiB).
  """

  alias Flezha.Operative.ClassRegistry

  @default_persistence_enabled true
  @default_retention_hours 6
  @default_compact_after_bytes 8 * 1024 * 1024
  @default_output_stall_timeout_ms 60_000
  @default_compaction_timeout_ms 30_000
  @default_compact_after_age_ms 300_000
  @default_bounded_completed_retention_hours 72

  @spec persistence_enabled?(binary(), map()) :: boolean()
  def persistence_enabled?(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    resolve(:operative_persistence, :operative_persistence_enabled, operative_class, metadata,
      valid: &is_boolean/1,
      default: fn -> @default_persistence_enabled end
    )
  end

  @doc """
  Whether an operative of `kind` + `operative_class` has durable per-Segment
  state (WAL + snapshot + restore) — the kind+class resolution of the
  `persistent_state?` capability that gates bind-time restore (crash-resume)
  and `:get_segment_state` introspection:

    * `:unbounded` — always durable.
    * `:bounded` — durable **by default**, opt-out per class (same precedence
      as `persistence_enabled?/2`: operator override → instance metadata →
      default `true`).
    * any other kind — not durable.

  Distinct from the pure per-kind `Flezha.Operative.Kind.capable?/2` flags
  (`time_travel?`, `dynamic_segments?`, `output_stall_watch?`) which a durable
  bounded operative does NOT inherit.
  """
  @spec persistent_state?(atom(), binary(), map()) :: boolean()
  def persistent_state?(kind, operative_class, metadata \\ %{})

  def persistent_state?(:unbounded, _operative_class, _metadata), do: true

  def persistent_state?(:bounded, operative_class, metadata)
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
  How many hours a finished bounded operative's data (retained Segment state +
  completion record) is kept before the retention sweeper reclaims it.
  Precedence: operator override map `:bounded_completed_retention_hours` →
  instance metadata → global `:default_bounded_completed_retention_hours`
  (`72`, env `FLEZHA_BOUNDED_RETENTION_HOURS`).
  """
  @spec bounded_completed_retention_hours(binary(), map()) :: non_neg_integer()
  def bounded_completed_retention_hours(operative_class, metadata \\ %{})
      when is_binary(operative_class) do
    resolve(
      :bounded_completed_retention_hours,
      :bounded_completed_retention_hours,
      operative_class,
      metadata,
      valid: &non_neg_int?/1,
      default: fn ->
        Application.get_env(
          :flezha,
          :default_bounded_completed_retention_hours,
          @default_bounded_completed_retention_hours
        )
      end
    )
  end

  @spec compact_after_bytes(binary(), map()) :: pos_integer()
  def compact_after_bytes(_operation_id, metadata \\ %{}) do
    case Map.get(metadata, "compact_after_bytes", Map.get(metadata, :compact_after_bytes)) do
      v when is_integer(v) and v > 0 ->
        v

      _ ->
        Application.get_env(:flezha, :default_compact_after_bytes, @default_compact_after_bytes)
    end
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
      default: fn -> Application.get_env(:flezha, :wal_high_watermark_bytes, 16 * 1024 * 1024) end
    )
  end

  @spec compaction_timeout_ms() :: non_neg_integer()
  def compaction_timeout_ms(),
    do: global_non_neg(:compaction_timeout_ms, @default_compaction_timeout_ms)

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
  actually configured. Used to announce a derivation, never to compact against.
  """
  @spec configured_compact_after_age_ms(binary(), map()) :: non_neg_integer()
  def configured_compact_after_age_ms(operative_class, metadata \\ %{})
      when is_binary(operative_class) do
    resolve(:compact_after_age_ms, :compact_after_age_ms, operative_class, metadata,
      valid: &(is_integer(&1) and &1 >= 0),
      default: fn -> global_non_neg(:compact_after_age_ms, @default_compact_after_age_ms) end
    )
  end

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
