defmodule Flezha.State.DurabilityTier.ShipStatus do
  @moduledoc """
  One status row per shipped reference: when it last landed, since when it is
  owed, its failures and retry deadline, and who holds its lineage when this
  node does not. Only `Flezha.State.DurabilityTier.Shipper` writes it; any
  process may read it, also while the shipper is busy.
  """

  alias Flezha.Config.Backoff

  @table :flezha_durability_status

  @doc """
  Per-reference shipping state, straight out of ETS. Readable from any process
  and while the shipper is busy — which is what makes it safe for `/healthz`.
  Each row adds `retry_in_ms`, how long until a pending retry (`nil` when none
  is pending), so a reader on another node never compares this node's
  monotonic `retry_after_ms` with its own clock.
  """
  @spec all() :: [map()]
  def all() do
    case :ets.whereis(@table) do
      :undefined ->
        []

      _ ->
        now = System.monotonic_time(:millisecond)

        @table
        |> :ets.select([{{:_, :"$1"}, [], [:"$1"]}])
        |> Enum.map(&Map.put(&1, :retry_in_ms, retry_in(&1, now)))
    end
  end

  defp retry_in(%{retry_after_ms: at}, now) when is_integer(at), do: max(at - now, 0)
  defp retry_in(_entry, _now), do: nil

  # The writes below are made by the shipper alone.

  def ensure_table() do
    case :ets.whereis(@table) do
      :undefined -> :ets.new(@table, [:set, :public, :named_table, read_concurrency: true])
      _ -> @table
    end
  end

  def mark_pending(ref) do
    now = System.system_time(:millisecond)

    update_status(ref, fn entry ->
      %{entry | pending_since_ms: entry.pending_since_ms || now}
    end)
  end

  def record_success(ref, caught_up?) do
    now = System.system_time(:millisecond)

    update_status(ref, fn entry ->
      %{
        entry
        | last_shipped_ms: now,
          pending_since_ms: if(caught_up?, do: nil, else: entry.pending_since_ms || now),
          failures: Backoff.reset(:durability_ship, ref),
          retry_after_ms: nil,
          last_error: nil
      }
    end)
  end

  def record_failure(ref, reason, attempt, delay) do
    update_status(ref, fn entry ->
      %{
        entry
        | failures: attempt,
          retry_after_ms: System.monotonic_time(:millisecond) + delay,
          last_error: inspect(reason),
          pending_since_ms: entry.pending_since_ms || System.system_time(:millisecond)
      }
    end)
  end

  def record_superseded(ref, holder) do
    update_status(ref, fn entry ->
      %{
        entry
        | superseded_by: holder,
          pending_since_ms: nil,
          retry_after_ms: nil,
          last_error: nil
      }
    end)
  end

  def not_owed(ref), do: update_status(ref, &%{&1 | pending_since_ms: nil})

  def clear_supersession(ref) do
    update_status(ref, &%{&1 | superseded_by: nil})
  end

  defp update_status(ref, fun) do
    ensure_table()
    :ets.insert(@table, {ref, fun.(get(ref))})
    :ok
  end

  def get(ref) do
    case :ets.whereis(@table) do
      :undefined ->
        blank_status(ref)

      _ ->
        case :ets.lookup(@table, ref) do
          [{^ref, entry}] -> entry
          [] -> blank_status(ref)
        end
    end
  end

  defp blank_status(ref) do
    %{
      ref: ref,
      last_shipped_ms: nil,
      pending_since_ms: nil,
      failures: 0,
      retry_after_ms: nil,
      superseded_by: nil,
      last_error: nil
    }
  end
end
