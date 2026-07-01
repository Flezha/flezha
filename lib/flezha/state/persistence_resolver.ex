defmodule Flezha.State.PersistenceResolver do
  @moduledoc """
  Resolves per-Operative persistence configuration from three sources, in
  order of precedence:

    1. Executor runtime override:
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
       metadata first; global default `64 * 1024 * 1024`.
    * `:output_stall_timeout_ms` — `%{<OperativeClass> => non_neg_integer}`.
       Global default `60_000`. `0` disables the output-staleness
       watchdog for that Operative class.
    * `:compaction_timeout_ms` — global; the bound on the
       `CompactSegment` → `segment_state_snapshot` handshake.
       Default `30_000`. `0` disables the compaction-timeout sweep.
  """

  @default_persistence_enabled true
  @default_retention_hours 6
  @default_compact_after_bytes 64 * 1024 * 1024
  @default_output_stall_timeout_ms 60_000
  @default_compaction_timeout_ms 30_000
  @default_bounded_completed_retention_hours 72

  @spec persistence_enabled?(binary(), map()) :: boolean()
  def persistence_enabled?(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    case operator_override(:operative_persistence, operative_class) do
      {:ok, value} when is_boolean(value) ->
        value

      _ ->
        case Map.get(
               metadata,
               "operative_persistence_enabled",
               Map.get(metadata, :operative_persistence_enabled)
             ) do
          v when is_boolean(v) -> v
          _ -> @default_persistence_enabled
        end
    end
  end

  @doc """
  Whether an operative of `kind` + `operative_class` has durable per-Segment
  state (WAL + snapshot + restore) — the kind+class resolution of the
  `persistent_state?` capability that gates bind-time restore (crash-resume)
  and `:get_segment_state` introspection:

    * `:continuous` — always durable.
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

  def persistent_state?(:continuous, _operative_class, _metadata), do: true

  def persistent_state?(:bounded, operative_class, metadata)
      when is_binary(operative_class),
      do: persistence_enabled?(operative_class, metadata)

  def persistent_state?(_kind, _operative_class, _metadata), do: false

  @spec retention_hours(binary(), map()) :: non_neg_integer()
  def retention_hours(operative_class, metadata \\ %{}) when is_binary(operative_class) do
    case operator_override(:time_travel_retention_hours, operative_class) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        value

      _ ->
        case Map.get(
               metadata,
               "time_travel_retention_hours",
               Map.get(metadata, :time_travel_retention_hours)
             ) do
          v when is_integer(v) and v >= 0 ->
            v

          _ ->
            Application.get_env(
              :flezha,
              :default_time_travel_retention_hours,
              @default_retention_hours
            )
        end
    end
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
    case operator_override(:bounded_completed_retention_hours, operative_class) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        value

      _ ->
        case Map.get(
               metadata,
               "bounded_completed_retention_hours",
               Map.get(metadata, :bounded_completed_retention_hours)
             ) do
          v when is_integer(v) and v >= 0 ->
            v

          _ ->
            Application.get_env(
              :flezha,
              :default_bounded_completed_retention_hours,
              @default_bounded_completed_retention_hours
            )
        end
    end
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
    case operator_override(:output_stall_timeout_ms, operative_class) do
      {:ok, value} when is_integer(value) and value >= 0 ->
        value

      _ ->
        case Map.get(
               metadata,
               "output_stall_timeout_ms",
               Map.get(metadata, :output_stall_timeout_ms)
             ) do
          v when is_integer(v) and v >= 0 ->
            v

          _ ->
            Application.get_env(
              :flezha,
              :default_output_stall_timeout_ms,
              @default_output_stall_timeout_ms
            )
        end
    end
  end

  @spec compaction_timeout_ms() :: non_neg_integer()
  def compaction_timeout_ms() do
    case Application.get_env(:flezha, :compaction_timeout_ms, @default_compaction_timeout_ms) do
      v when is_integer(v) and v >= 0 -> v
      _ -> @default_compaction_timeout_ms
    end
  end

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
