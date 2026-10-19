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

  # Which of `namespace`'s inactive runners have outlived the removal grace. The hub
  # is reclaimed on the way past only when it is vacant.
  defp expired_runners(state, namespace, now, grace_ms) do
    state
    |> Roster.get_runner_ids(namespace, :inactive)
    |> Enum.filter(fn runner_id ->
      {_status, timestamp} = state.runner_namespaces[namespace][runner_id]
      expired? = timestamp + grace_ms <= now
      if expired?, do: reclaim_vacant_hub(state, namespace)
      expired?
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

    Enum.reduce(Map.keys(state.runner_namespaces), state, fn namespace, acc ->
      expired = expired_runners(state, namespace, now, grace_ms)
      acc = update_in(acc.runner_namespaces[namespace], &Map.drop(&1, expired))

      expired
      |> Enum.reduce(acc, &Factories.remove_ce_factories(&2, namespace, &1))
      |> Factories.prune_empty_factories(namespace)
    end)
  end
end
