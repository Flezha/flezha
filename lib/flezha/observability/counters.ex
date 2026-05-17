defmodule Flezha.Observability.Counters do
  @moduledoc """
  Process-global atomic counters for foundation-internal accounting
  that does not belong to a specific source or sink: dispatcher
  failures, logger-bridge failures, telemetry-bridge failures.

  Backed by `:counters` with `:atomics` semantics — sub-100ns
  increment, lock-free.
  """

  @key {__MODULE__, :ref}
  @error_classes :flezha_observability_error_classes

  @slot :dispatcher_failure_count
  @slot_logger :logger_bridge_failures
  @slot_telemetry :telemetry_bridge_failures

  @slots [:dispatcher_failure_count, :logger_bridge_failures, :telemetry_bridge_failures]

  @doc false
  @spec init() :: :ok
  def init() do
    case :persistent_term.get(@key, nil) do
      nil ->
        ref = :counters.new(length(@slots), [:atomics])
        :persistent_term.put(@key, ref)

      _ref ->
        :ok
    end

    case :ets.whereis(@error_classes) do
      :undefined ->
        :ets.new(@error_classes, [
          :set,
          :public,
          :named_table,
          read_concurrency: true,
          write_concurrency: true
        ])

      _ ->
        :ok
    end

    :ok
  end

  @doc """
  Returns `true` the first time `class` is seen, `false` thereafter.
  Used to log dispatch failures once per unique error class.
  """
  @spec first_seen?(term()) :: boolean()
  def first_seen?(class) do
    case :ets.whereis(@error_classes) do
      :undefined -> true
      _ -> :ets.insert_new(@error_classes, {class, true})
    end
  end

  @spec bump(atom()) :: :ok
  def bump(slot) when slot in @slots do
    case :persistent_term.get(@key, nil) do
      nil -> :ok
      ref -> :counters.add(ref, slot_index(slot), 1)
    end
  end

  @spec get(atom()) :: non_neg_integer()
  def get(slot) when slot in @slots do
    case :persistent_term.get(@key, nil) do
      nil -> 0
      ref -> :counters.get(ref, slot_index(slot))
    end
  end

  @doc false
  def reset() do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      ref ->
        Enum.each(@slots, fn slot ->
          current = :counters.get(ref, slot_index(slot))
          if current > 0, do: :counters.sub(ref, slot_index(slot), current)
        end)
    end

    case :ets.whereis(@error_classes) do
      :undefined -> :ok
      _ -> :ets.delete_all_objects(@error_classes)
    end

    :ok
  end

  defp slot_index(@slot), do: 1
  defp slot_index(@slot_logger), do: 2
  defp slot_index(@slot_telemetry), do: 3
end
