defmodule Flezha.State.PersistenceResolver do
  @moduledoc """
  Resolves per-Relay persistence configuration from three sources, in
  order of precedence:

    1. Executor runtime override:
       `Application.get_env(:flezha, <key>) :: %{<RelayClass> => value}`
    2. Per-instance metadata shipped with `AddRelay`
       (passed in as the `metadata` argument).
    3. Hard-coded global default.

  Configuration keys:

    * `:relay_persistence` — `%{<RelayClass> => boolean}`. Default `true`.
    * `:time_travel_retention_hours` — `%{<RelayClass> => non_neg_integer}`.
       Global default `6`. `0` disables time-travel for that Relay class.
    * `:compact_after_bytes` — per-Shard threshold, sourced from SDK
       metadata first; global default `64 * 1024 * 1024`.
  """

  @default_persistence_enabled true
  @default_retention_hours 6
  @default_compact_after_bytes 64 * 1024 * 1024

  @spec persistence_enabled?(binary(), map()) :: boolean()
  def persistence_enabled?(relay_class, metadata \\ %{}) when is_binary(relay_class) do
    case operator_override(:relay_persistence, relay_class) do
      {:ok, value} when is_boolean(value) ->
        value

      _ ->
        case Map.get(
               metadata,
               "relay_persistence_enabled",
               Map.get(metadata, :relay_persistence_enabled)
             ) do
          v when is_boolean(v) -> v
          _ -> @default_persistence_enabled
        end
    end
  end

  @spec retention_hours(binary(), map()) :: non_neg_integer()
  def retention_hours(relay_class, metadata \\ %{}) when is_binary(relay_class) do
    case operator_override(:time_travel_retention_hours, relay_class) do
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

  @spec compact_after_bytes(binary(), map()) :: pos_integer()
  def compact_after_bytes(_shard_id, metadata \\ %{}) do
    case Map.get(metadata, "compact_after_bytes", Map.get(metadata, :compact_after_bytes)) do
      v when is_integer(v) and v > 0 ->
        v

      _ ->
        Application.get_env(:flezha, :default_compact_after_bytes, @default_compact_after_bytes)
    end
  end

  defp operator_override(key, relay_class) do
    case Application.get_env(:flezha, key) do
      %{} = m ->
        case Map.fetch(m, relay_class) do
          {:ok, v} -> {:ok, v}
          :error -> :error
        end

      _ ->
        :error
    end
  end
end
