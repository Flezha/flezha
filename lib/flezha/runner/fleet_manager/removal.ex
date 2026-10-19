defmodule Flezha.Runner.FleetManager.Removal do
  @moduledoc """
  The removal grace: a disconnected runner held `:inactive` is dropped from the
  roster, with its factory refs, once it has outlived
  `FLEZHA_RUNNER_REMOVAL_GRACE_MS` (`0` drops it at the next sweep). Runs inside
  the fleet manager, on the state it owns.
  """

  alias Flezha.Config.Durations
  alias Flezha.Runner.FleetManager.Factories
  alias Flezha.Runner.FleetManager.Marks
  alias Flezha.Runner.FleetManager.Roster
  alias Flezha.Service.HubManager

  use Flezha.Process.Timer, keys: [:runner_cleanup]

  @spec arm_runner_cleanup(map()) :: map()
  def arm_runner_cleanup(state) do
    arm_timer(
      state,
      :runner_cleanup,
      Durations.get(:runner_removal_grace_ms) + Durations.get(:runner_cleanup_slack_ms),
      :schedule_runner_cleanup
    )
  end

  # `expired_runners/4` answers `nil` for a runner still inside its removal grace.
  defp drop_runner_ids(runners, removal_runners) do
    Enum.reduce(removal_runners, runners, fn
      nil, acc -> acc
      runner_id, acc -> Map.delete(acc, runner_id)
    end)
  end

  # Which of `namespace`'s inactive runners have outlived the removal grace. The hub
  # is reclaimed on the way past only when it is vacant.
  defp expired_runners(state, namespace, now, grace_ms) do
    state
    |> Roster.get_runner_ids(namespace, :inactive)
    |> Enum.map(fn runner_id ->
      {_status, timestamp} = state.runner_namespaces[namespace][runner_id]

      if timestamp + grace_ms <= now do
        reclaim_vacant_hub(state, namespace)
        runner_id
      end
    end)
  end

  @spec reclaim_vacant_hub(map(), String.t()) :: :ok | term()
  def reclaim_vacant_hub(state, namespace) do
    if Roster.occupants(state, namespace) == [], do: HubManager.stop_hub_if_vacant(namespace)
  end

  @spec runner_cleanup(map()) :: map()
  def runner_cleanup(state) do
    Marks.prune()

    now = System.monotonic_time(:millisecond)
    grace_ms = Durations.get(:runner_removal_grace_ms)

    namespaces = Map.keys(state.runner_namespaces)

    removal = Enum.map(namespaces, &{&1, expired_runners(state, &1, now, grace_ms)})

    runner_namespaces =
      Enum.reduce(removal, state.runner_namespaces, fn {namespace, removal_runners}, acc ->
        Map.put(acc, namespace, drop_runner_ids(acc[namespace], removal_runners))
      end)

    state = %{state | runner_namespaces: runner_namespaces}

    # Clean factory refs for removed CEs
    Enum.reduce(removal, state, fn {namespace, removal_runners}, acc ->
      removal_runners
      |> Enum.reduce(acc, fn
        nil, acc -> acc
        ce_id, acc -> Factories.remove_ce_factories(acc, namespace, ce_id)
      end)
      |> Factories.prune_empty_factories(namespace)
    end)
  end
end
