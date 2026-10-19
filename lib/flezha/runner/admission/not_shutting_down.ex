defmodule Flezha.Runner.Admission.NotShuttingDown do
  @moduledoc "Refuse a connect to a namespace that is shutting down."
  @behaviour Flezha.Runner.Admission.Check

  alias Flezha.Shutdown

  require Logger

  @impl true
  def check(%{namespace: namespace}, _context) do
    with {:error, :shutting_down} = refusal <- Shutdown.admit(:namespace, namespace) do
      Logger.info("[shutdown] rejecting runner connect attempt for namespace=#{namespace}")
      refusal
    end
  end
end
