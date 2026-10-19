defmodule Flezha.Runner.Admission.Quota do
  @moduledoc """
  The runner principal is minted from the namespace the connection PROVED
  (`Flezha.AuthZ.RunnerProof`), so both sides of the row's namespace rule read
  the proven value and neither reads the frame. Cross-namespace action is
  refused before this, at the proof; what this row judges is that the identity
  holds `runner.connect` at all. Also the per-namespace `max_runners` quota
  point — both refusals are observable through the authz denial event / the
  quota telemetry below.
  """
  @behaviour Flezha.Runner.Admission.Check

  alias Flezha.AuthZ.Quotas
  alias Flezha.AuthZ.Verb
  alias Flezha.Runner.FleetManager.Session

  require Logger

  @impl true
  def phase, do: :caller

  @impl true
  def check(%{namespace: namespace, transport: transport}, _context) do
    runner_ctx = Session.runner_caller_context(namespace)

    case Verb.run(:runner_connect, runner_ctx, [namespace], fn _call ->
           Quotas.check_allocation(namespace, :max_runners)
         end) do
      :ok ->
        :ok

      {:error, {:quota_exceeded, _kind, %{current: current, limit: limit}} = reason} ->
        Logger.warning(
          "[FleetManager] runner connect refused for namespace=#{namespace}: " <>
            "max_runners quota reached (#{current}/#{limit})"
        )

        :telemetry.execute(
          [:flezha, :runner, :quota_refused],
          %{count: 1},
          %{namespace: namespace, transport: transport, current: current, limit: limit}
        )

        {:error, reason}

      {:error, reason} ->
        Logger.warning(
          "[FleetManager] runner connect refused for namespace=#{namespace}: #{inspect(reason)}"
        )

        {:error, reason}
    end
  end
end
