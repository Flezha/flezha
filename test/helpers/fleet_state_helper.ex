defmodule FlezhaTest.Helper.FleetStateHelper do
  @moduledoc """
  Synthetic fleet state for tests that drive the live `Flezha.Runner.FleetManager`
  with `:sys.replace_state` — one definition of "this namespace holds these
  runners advertising these factories".
  """

  alias Flezha.Runner.FleetManager

  @doc "Seed `namespace` with `%{runner_id => [factory_id]}`, all `:active`."
  def seed(namespace, runner_factories) do
    ts = System.system_time(:millisecond)

    :sys.replace_state(FleetManager, fn state ->
      runners = runner_factories |> Map.keys() |> Map.new(&{&1, {:active, ts}})

      ns_factories =
        Enum.reduce(runner_factories, %{}, fn {runner, factories}, acc ->
          Enum.reduce(factories, acc, fn id, acc ->
            existing = Map.get(acc, id, %{description: nil, ce_ids: MapSet.new()})
            Map.put(acc, id, %{existing | ce_ids: MapSet.put(existing.ce_ids, runner)})
          end)
        end)

      state
      |> put_in([:runner_namespaces, namespace], runners)
      |> put_in([:namespace_factories, namespace], ns_factories)
    end)
  end

  @doc """
  Deliver a `sync_factories` cast for `runner`, then a same-sender call so an
  assertion following this runs after the cast is handled.
  """
  def sync(namespace, runner, factory_ids) do
    factories = Enum.map(factory_ids, fn id -> %{"id" => id, "description" => nil} end)
    GenServer.cast(FleetManager, {:sync_factories, namespace, runner, factories})
    {:ok, _} = FleetManager.list_factories(namespace)
    :ok
  end
end
