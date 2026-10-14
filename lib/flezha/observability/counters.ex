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

  use Flezha.Observability.CounterTable,
    table: :counter,
    slots: [
      [name: :dispatcher_failure_count, kind: :count],
      [name: :logger_bridge_failures, kind: :count],
      [name: :telemetry_bridge_failures, kind: :count]
    ]

  @slots Enum.map(@counter_rows, &Keyword.fetch!(&1, :name))

  @doc false
  @spec init() :: :ok
  def init() do
    case :persistent_term.get(@key, nil) do
      nil ->
        ref = counter_new()
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
  The class of a caught error: the exception's module, the `:exit` / `:throw`
  kind, or the term itself. Half the key `first_seen?/1` is asked about.
  """
  @spec error_class(term()) :: term()
  def error_class(%{__struct__: mod}), do: mod
  def error_class({kind, _reason}) when kind in [:exit, :throw], do: kind
  def error_class(other), do: other

  @doc """
  Returns `true` the first time `class` is seen, `false` thereafter.
  A caller logs a dispatch failure once per unique error class.
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
      ref -> counter_bump(ref, slot)
    end
  end

  @spec get(atom()) :: non_neg_integer()
  def get(slot) when slot in @slots do
    case :persistent_term.get(@key, nil) do
      nil -> 0
      ref -> counter_get(ref, slot)
    end
  end

  @doc false
  def reset() do
    case :persistent_term.get(@key, nil) do
      nil ->
        :ok

      ref ->
        Enum.each(@slots, fn slot ->
          current = counter_get(ref, slot)
          if current > 0, do: counter_sub(ref, slot, current)
        end)
    end

    case :ets.whereis(@error_classes) do
      :undefined -> :ok
      _ -> :ets.delete_all_objects(@error_classes)
    end

    :ok
  end
end
