defmodule Flezha.Service.RelayManager.Placement do
  @moduledoc """
  Which runner a relay lands on: the call that asks the fleet, the node
  choice that shares its ranking, and the bounded poll a caller waits on.

  The choice itself is `Flezha.Runner.FleetManager.Load` — the fleet filters,
  ranks and counts in one step, so nothing here holds a candidate set.
  """

  require Logger

  alias Flezha.Config.Durations
  alias Flezha.Identificator
  alias Flezha.Process.Poll
  alias Flezha.Runner.FleetManager
  alias Flezha.Runner.FleetManager.Load

  @doc """
  Pick an active compute engine for `relay_id` in `namespace`.

  Candidates are ranked by rendezvous (HRW) hashing of `relay_id` over the
  runner set, and the one chosen is the highest-ranked whose count of bound
  relays is under a fairness cap — so a namespace holding fewer relays
  than it has runners still uses all of them, which a plain hash does not. The
  cap and its knob are `Flezha.Runner.FleetManager.Load`; `FleetManager` reads
  the count and selects in one step, so a recovery batch spreads across its
  members rather than placing them all as if none of the others existed.

  A newly connected runner never reshuffles existing relays: placement is
  computed only at start / forced rebind.

  When `factory_id` is given the pool is restricted to runners advertising
  that factory. Without this filter a mixed-factory namespace (e.g.
  market-relay owning `crypto_trades` + market-app owning
  `market-init-state`) would pick the wrong node, routing every input frame
  to a process that doesn't exist on the foreign node (the Java SDK surfaces
  this as `DefaultRelayDispatcher.enqueue: relay 'X' not found`). Returns
  `nil` when no candidate exists.
  """
  @spec assign_runner(String.t(), binary() | nil, binary()) :: binary() | nil
  def assign_runner(namespace, factory_id, relay_id) do
    case resolve_runner(namespace, factory_id, relay_id, Durations.get(:runner_lookup_ms)) do
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
  so a blocked `FleetManager` can overrun the deadline by at most that floor.
  A trapping caller whose supervisor stops it between polls exits with that
  stop's reason.
  """
  @spec await_runner(
          String.t(),
          binary() | nil,
          binary(),
          non_neg_integer(),
          (binary() -> boolean())
        ) :: {:ok, binary()} | {:error, :timeout | :unavailable}
  def await_runner(namespace, factory_id, relay_id, timeout_ms, ready_fun \\ fn _ -> true end)

  # An exhausted budget costs nothing. `:timeout`, not `:unavailable`: the
  # fleet was never asked.
  def await_runner(_namespace, _factory_id, _relay_id, timeout_ms, _ready_fun)
      when timeout_ms <= 0,
      do: {:error, :timeout}

  def await_runner(namespace, factory_id, relay_id, timeout_ms, ready_fun) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    attempt = fn -> attempt(namespace, factory_id, relay_id, deadline, ready_fun) end
    Poll.until(deadline, Durations.get(:runner_poll_interval_ms), attempt, &pause/1)
  end

  # Every refusal is the reason its own attempt produced, so the returned error
  # always describes the last attempt.
  defp attempt(namespace, factory_id, relay_id, deadline, ready_fun) do
    case resolve_runner(namespace, factory_id, relay_id, call_timeout(deadline)) do
      {:ok, runner_id} when is_binary(runner_id) ->
        if ready_fun.(runner_id), do: {:ok, runner_id}, else: {:error, :timeout}

      {:ok, nil} ->
        {:error, :timeout}

      {:error, :unavailable} ->
        {:error, :unavailable}
    end
  end

  # The wait runs inside the caller's callback, where a supervisor's stop sits
  # unread until its shutdown bound kills the caller and skips its `terminate/2`;
  # a stop that arrives between polls ends the wait as that stop.
  defp pause(interval_ms) do
    [parent | _] = Process.get(:"$ancestors", [nil])
    parent = if is_atom(parent), do: Process.whereis(parent), else: parent

    receive do
      {:EXIT, ^parent, reason} -> exit(reason)
    after
      interval_ms -> :ok
    end
  end

  # `runner_poll_call_ms` must stay well under any caller's deadline, or one
  # blocked lookup consumes it and the loop never iterates; `runner_min_call_ms`
  # is the floor, below which a healthy FleetManager could not answer and we
  # would blame it for our own exhausted budget.
  defp call_timeout(deadline) do
    (deadline - System.monotonic_time(:millisecond))
    |> min(Durations.get(:runner_poll_call_ms))
    |> max(Durations.get(:runner_min_call_ms))
  end

  # `{:error, :unavailable}`, never `{:ok, nil}`: collapsing an unanswered read
  # into "no runners" blames the fleet for a control-plane stall (see
  # `Convergence.Probe`). The filter, the choice and the count all happen inside
  # the manager's own call — see `FleetManager.resolve_runner/4` for why they
  # cannot be a read followed by a separate count.
  defp resolve_runner(namespace, factory_id, relay_id, call_timeout_ms) do
    {:ok, FleetManager.resolve_runner(namespace, factory_id, relay_id, call_timeout_ms)}
  catch
    :exit, reason ->
      Logger.warning(
        "[RelayManager] FleetManager runner lookup did not answer for namespace " <>
          "#{namespace} factory #{inspect(factory_id)}: #{inspect(reason)}"
      )

      {:error, :unavailable}
  end

  # Rendezvous hashing with no load input, for the callers choosing a cluster
  # NODE rather than a runner: a node has no relay count in the fleet's map,
  # and repeated convergence ticks have to agree on one destination. Same
  # implementation as the runner case, with the cap left off — an empty load map
  # is no cap.
  @doc false
  def pick_runner(candidates, relay_id), do: Load.select(candidates, relay_id, %{})
end
