defmodule Flezha.Service.HubManagerTest do
  @moduledoc """
  Asserts the cspec-mutation contract for
  `Flezha.Service.HubManager.update_relay_plugins/3` and
  `Flezha.Service.HubManager.relay_plugins/2`.

  These functions back the
  `plugin-subscription-persistence` capability — every
  `flz plugin enable` / `flz plugin disable` writes through to the
  ProcessHub child-spec via this path, and the persisted list is what
  `Flezha.Relay.Overwatch.init/1` reads on every restart.

  Tests run against the running `default`-namespace hub started by the
  application supervisor; we seed cspecs directly through
  `ProcessHub.Service.ProcessRegistry` so the hub does not actually
  spawn an Overwatch.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Relay.Builder
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry

  @namespace "default"

  defp hub_id, do: Identificator.hub(@namespace)

  defp relay_builder(opts) do
    %Builder{
      id: "ignored",
      factory_id: "do_nothing",
      init_values: %{},
      opts: opts
    }
  end

  defp insert_cspec(relay_id, start_arg) do
    cspec = %{
      id: relay_id,
      start: {Flezha.Relay.Overwatch, :start_link, [start_arg]},
      restart: :transient
    }

    :ok = ProcessRegistry.insert(hub_id(), cspec, [{node(), self()}])

    on_exit(fn ->
      _ = ProcessRegistry.delete(hub_id(), relay_id)
    end)

    cspec
  end

  defp plugin(id, opts \\ %{}) do
    Plugin.new(
      id: id,
      provider: __MODULE__.NoopProvider,
      opts: opts,
      subscriptions: [{:on_output, []}],
      namespace: @namespace,
      relay_id: "ignored"
    )
  end

  describe "update_relay_plugins/3" do
    test "appends a plugin to a cspec with empty opts[:plugins]" do
      relay_id = "hm_test_append_empty"
      builder = relay_builder(plugins: [])
      insert_cspec(relay_id, {@namespace, "ce_1", builder})

      new_plugin = plugin("plug_x", %{a: 1})

      assert :ok =
               HubManager.update_relay_plugins(@namespace, relay_id, fn current ->
                 current ++ [{"plug_x", new_plugin}]
               end)

      assert {:ok, [{"plug_x", got}]} = HubManager.relay_plugins(@namespace, relay_id)
      assert got.id == "plug_x"
      assert got.opts == %{a: 1}
    end

    test "removes a plugin from a cspec with multiple opts[:plugins]" do
      relay_id = "hm_test_remove_one"
      a = plugin("a")
      b = plugin("b")
      c = plugin("c")

      builder = relay_builder(plugins: [{"a", a}, {"b", b}, {"c", c}])
      insert_cspec(relay_id, {@namespace, "ce_1", builder})

      assert :ok =
               HubManager.update_relay_plugins(@namespace, relay_id, fn current ->
                 Enum.reject(current, fn {id, _} -> id == "b" end)
               end)

      assert {:ok, plugins} = HubManager.relay_plugins(@namespace, relay_id)
      assert Enum.map(plugins, fn {id, _} -> id end) == ["a", "c"]
    end

    test "handles the {namespace, relay} start-args shape" do
      relay_id = "hm_test_shape_2tuple"
      builder = relay_builder(plugins: [])
      insert_cspec(relay_id, {@namespace, builder})

      assert :ok =
               HubManager.update_relay_plugins(@namespace, relay_id, fn _ ->
                 [{"only", plugin("only")}]
               end)

      assert {:ok, [{"only", _}]} = HubManager.relay_plugins(@namespace, relay_id)
    end

    test "handles the {namespace, executor_id, relay} start-args shape" do
      relay_id = "hm_test_shape_3tuple"
      builder = relay_builder(plugins: [])
      insert_cspec(relay_id, {@namespace, "ce_42", builder})

      assert :ok =
               HubManager.update_relay_plugins(@namespace, relay_id, fn _ ->
                 [{"only", plugin("only")}]
               end)

      assert {:ok, [{"only", _}]} = HubManager.relay_plugins(@namespace, relay_id)

      # Confirm the executor_id position survives the rewrite — if
      # we lost it, alter_child_data's invariants break and the next
      # restart routes to a random compute engine.
      registry = ProcessHub.registry_dump(hub_id())
      assert {cspec, _, _} = Map.get(registry, relay_id)
      assert {Flezha.Relay.Overwatch, :start_link, [arg]} = cspec.start
      assert {@namespace, "ce_42", %Builder{}} = arg
    end

    test "returns {:error, _} when the relay is not registered" do
      assert {:error, _} =
               HubManager.update_relay_plugins(@namespace, "no_such_relay", fn p -> p end)
    end

    @tag :bench
    test "1000 sequential cspec-updates stay under the documented latency budget" do
      relay_id = "hm_bench_seq"
      builder = relay_builder(plugins: [])
      insert_cspec(relay_id, {@namespace, "ce_1", builder})

      times =
        for _ <- 1..1000 do
          {us, :ok} =
            :timer.tc(fn ->
              HubManager.update_relay_plugins(@namespace, relay_id, fn p -> p end)
            end)

          us
        end

      sorted = Enum.sort(times)
      median_us = Enum.at(sorted, div(length(sorted), 2))
      # design.md Risks states the cost is sub-millisecond on dev compose;
      # 5 ms is a generous CI ceiling that still catches regressions like
      # an accidental cross-node round-trip.
      assert median_us < 5_000
    end
  end

  defmodule NoopProvider do
    @moduledoc false
  end
end
