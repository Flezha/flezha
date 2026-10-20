defmodule Flezha.Runner.Admission.Quota do
  @moduledoc """
  Refuse a connect whose runner principal, minted from the namespace the
  connection proved, lacks `runner.connect`, or whose namespace is at its
  `max_runners` quota: the authz denial event and the quota telemetry say which.
  """
  @behaviour Flezha.Runner.Admission.Check

  alias Flezha.AuthZ.Quotas
  alias Flezha.AuthZ.Verb
  alias Flezha.Runner.FleetManager.Session

  require Logger

  @impl true
  def check(%{namespace: namespace, transport: transport}, _context) do
    runner_ctx = Session.runner_caller_context(namespace)

    case Verb.run(:runner_connect, runner_ctx, [namespace], fn _call ->
           Quotas.check_allocation(namespace, :max_runners)
         end) do
      :ok ->
        :ok

      {:error, {:quota_exceeded, _kind, %{current: current, limit: limit}} = reason} ->
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
