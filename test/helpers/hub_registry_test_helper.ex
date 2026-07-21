defmodule FlezhaTest.Helper.HubRegistryTestHelper do
  @moduledoc """
  Seed a real ProcessHub and inject synthetic registry entries so hub-facing
  tests can exercise registry reads and hub lifecycle without a live SDK or
  runner process.
  """
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry

  @doc "Start (or reuse) the namespace's hub, normalising the start-result shapes."
  def ensure_hub(ns) do
    case HubManager.start_hub(ns) do
      {:ok, pid} -> {:ok, pid}
      {:ok, pid, _info} -> {:ok, pid}
      {:error, {:already_started, pid}} -> {:ok, pid}
      other -> other
    end
  end

  @doc """
  Inject a synthetic registry entry with an explicit `node_pids` list, so a
  multi-node operative is reproduced without a real peer node + runner.
  """
  def inject_operative(ns, operative_id, node_pids) do
    hub_id = Identificator.hub(ns)

    child_spec = %{
      id: operative_id,
      start: {Flezha.Operative.Overwatch, :start_link, [{ns, operative_id}]}
    }

    :ok = ProcessRegistry.bulk_insert(hub_id, %{operative_id => {child_spec, node_pids, %{}}})
  end

  @doc """
  Inject a registered operative bound to a live local pid — the common case,
  and the shape a durably-suspended operative keeps (only its runner binding is
  cleared, its Overwatch pid stays in the registry).
  """
  def inject_operative(ns, operative_id),
    do: inject_operative(ns, operative_id, [{node(), spawn(fn -> Process.sleep(:infinity) end)}])
end
