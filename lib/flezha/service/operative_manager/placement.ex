defmodule Flezha.Service.OperativeManager.Placement do
  @moduledoc """
  Which runner an operative lands on: rendezvous choice, the candidate lookup,
  and the bounded poll a caller waits on.
  """

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Runner.FleetManager

  # Per-attempt budget for the polling path. Must stay well under any caller's
  # deadline, or one blocked lookup consumes it and the loop never iterates.

  # Floor for the clamped attempt: below this a healthy FleetManager could not
  # answer, and we would blame it for our own exhausted budget.

  @doc """
  Pick an active compute engine for `operative_id` in `namespace`.

  Placement is deterministic — rendezvous (HRW) hashing of `operative_id` over
  the candidate runners — so every controller and every tick computes the
  same target with no coordination, load spreads evenly, and only ~1/N
  operatives move when the runner set changes. A newly connected runner
  never reshuffles existing operatives: placement is computed only at start /
  forced rebind.

  When `factory_id` is given the pool is restricted to runners advertising
  that factory. Without this filter a mixed-factory namespace (e.g.
  market-operative owning `crypto_trades` + market-app owning
  `market-init-state`) would pick the wrong node, routing every input frame
  to a process that doesn't exist on the foreign node (the Java SDK surfaces
  this as `DefaultOperativeDispatcher.enqueue: operative 'X' not found`). Returns
  `nil` when no candidate exists.
  """
  @spec assign_runner(String.t(), binary() | nil, binary()) :: binary() | nil
  def assign_runner(namespace, factory_id, operative_id) do
    case resolve_runner(namespace, factory_id, operative_id, Durations.get(:runner_lookup_ms)) do
      {:ok, runner_id} -> runner_id
      {:error, :unavailable} -> nil
    end
  end

  @doc """
  True when `runner_id`'s control unit is registered — the cheap half of runner
  readiness, shared by everything that must not hand out a phantom runner id.
  """
  @spec runner_registered?(binary()) :: boolean()
  def runner_registered?(runner_id) when is_binary(runner_id),
    do: match?([{_pid, _}], Registry.lookup(Identificator, "#{runner_id}_cu"))

  def runner_registered?(_), do: false

  @doc """
  Poll for a runner `ready_fun` accepts until `timeout_ms` elapses.

  Returns `{:ok, runner_id}`, `{:error, :timeout}` when the fleet simply has no
  candidate, or `{:error, :unavailable}` when `FleetManager` never answered.
  Callers reach for this whenever the answer may arrive late: a runner becomes
  placeable only after `FleetManager` finishes its `runner_connect/3` handler,
  which is the same handler that starts the hub and its recovery replay.

  Every attempt is capped at the smaller of #{Durations.get(:runner_poll_call_ms)} ms
  and what is left of `timeout_ms`, floored at #{Durations.get(:runner_min_call_ms)} ms,
  so a blocked `FleetManager` can overrun the deadline by at most that floor no
  matter how many callers share one budget.
  """
  @spec await_runner(
          String.t(),
          binary() | nil,
          binary(),
          non_neg_integer(),
          (binary() -> boolean())
        ) :: {:ok, binary()} | {:error, :timeout | :unavailable}
  def await_runner(namespace, factory_id, operative_id, timeout_ms, ready_fun \\ fn _ -> true end)

  # An exhausted budget must cost nothing: blocking here is what walks a large
  # batch past `cross_node_request_timeout`. `:timeout`, not `:unavailable` —
  # the fleet was never asked.
  def await_runner(_namespace, _factory_id, _operative_id, timeout_ms, _ready_fun)
      when timeout_ms <= 0,
      do: {:error, :timeout}

  def await_runner(namespace, factory_id, operative_id, timeout_ms, ready_fun) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_await_runner(namespace, factory_id, operative_id, deadline, ready_fun)
  end

  # Every branch hands `poll_again` the reason its own attempt produced, so the
  # returned error always describes the last attempt.
  defp do_await_runner(namespace, factory_id, operative_id, deadline, ready_fun) do
    case resolve_runner(namespace, factory_id, operative_id, call_timeout(deadline)) do
      {:ok, runner_id} when is_binary(runner_id) ->
        if ready_fun.(runner_id),
          do: {:ok, runner_id},
          else: poll_again(namespace, factory_id, operative_id, deadline, ready_fun, :timeout)

      {:ok, nil} ->
        poll_again(namespace, factory_id, operative_id, deadline, ready_fun, :timeout)

      {:error, :unavailable} ->
        poll_again(namespace, factory_id, operative_id, deadline, ready_fun, :unavailable)
    end
  end

  defp poll_again(namespace, factory_id, operative_id, deadline, ready_fun, last_reason) do
    if System.monotonic_time(:millisecond) + Durations.get(:runner_poll_interval_ms) >= deadline do
      {:error, last_reason}
    else
      Process.sleep(Durations.get(:runner_poll_interval_ms))
      do_await_runner(namespace, factory_id, operative_id, deadline, ready_fun)
    end
  end

  defp call_timeout(deadline) do
    (deadline - System.monotonic_time(:millisecond))
    |> min(Durations.get(:runner_poll_call_ms))
    |> max(Durations.get(:runner_min_call_ms))
  end

  defp resolve_runner(namespace, factory_id, operative_id, call_timeout_ms) do
    case candidate_runners(namespace, factory_id, call_timeout_ms) do
      :unavailable -> {:error, :unavailable}
      runner_ids -> {:ok, pick_runner(runner_ids, operative_id)}
    end
  end

  # `:unavailable`, never `[]`: collapsing an unanswered read into "no runners"
  # blames the fleet for a control-plane stall (see `Convergence.Probe`).
  defp candidate_runners(namespace, factory_id, call_timeout_ms) do
    lookup_runners(namespace, factory_id, call_timeout_ms)
  catch
    :exit, reason ->
      Logger.warning(
        "[OperativeManager] FleetManager runner lookup did not answer for namespace " <>
          "#{namespace} factory #{inspect(factory_id)}: #{inspect(reason)}"
      )

      :unavailable
  end

  defp lookup_runners(namespace, nil, timeout),
    do: FleetManager.active_runners(namespace, timeout)

  defp lookup_runners(namespace, factory_id, timeout) when is_binary(factory_id),
    do: FleetManager.runners_for_factory(namespace, factory_id, timeout)

  # The candidate set is the authority: a runner is in it only while its
  # transport connection is open. HRW then picks the runner with the
  # highest hash of `{operative_id, runner}` — deterministic and stable under
  # membership change.
  @doc false
  def pick_runner([], _operative_id), do: nil

  def pick_runner(runner_ids, operative_id),
    do: Enum.max_by(runner_ids, fn ce -> :erlang.phash2({operative_id, ce}) end)
end
