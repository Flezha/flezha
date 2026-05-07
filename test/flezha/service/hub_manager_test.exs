defmodule Flezha.Service.HubManagerTest do
  @moduledoc """
  Asserts the cspec-mutation contract for
  `Flezha.Service.HubManager.update_processor_plugins/3` and
  `Flezha.Service.HubManager.processor_plugins/2`.

  These functions back the
  `plugin-subscription-persistence` capability — every
  `flz plugin enable` / `flz plugin disable` writes through to the
  ProcessHub child-spec via this path, and the persisted list is what
  `Flezha.Processor.Overwatch.init/1` reads on every restart.

  Tests run against the running `default`-namespace hub started by the
  application supervisor; we seed cspecs directly through
  `ProcessHub.Service.ProcessRegistry` so the hub does not actually
  spawn an Overwatch.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin
  alias Flezha.Processor.Builder
  alias Flezha.Service.HubManager
  alias Flezha.Service.Identificator
  alias ProcessHub.Service.ProcessRegistry

  @namespace "default"

  defp hub_id, do: Identificator.hub(@namespace)

  defp processor_builder(opts) do
    %Builder{
      id: "ignored",
      factory_id: "do_nothing",
      init_values: %{},
      opts: opts
    }
  end

  defp insert_cspec(processor_id, start_arg) do
    cspec = %{
      id: processor_id,
      start: {Flezha.Processor.Overwatch, :start_link, [start_arg]},
      restart: :transient
    }

    :ok = ProcessRegistry.insert(hub_id(), cspec, [{node(), self()}])

    on_exit(fn ->
      _ = ProcessRegistry.delete(hub_id(), processor_id)
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
      processor_id: "ignored"
    )
  end

  describe "update_processor_plugins/3" do
    test "appends a plugin to a cspec with empty opts[:plugins]" do
      processor_id = "hm_test_append_empty"
      builder = processor_builder(plugins: [])
      insert_cspec(processor_id, {@namespace, "ce_1", builder})

      new_plugin = plugin("plug_x", %{a: 1})

      assert :ok =
               HubManager.update_processor_plugins(@namespace, processor_id, fn current ->
                 current ++ [{"plug_x", new_plugin}]
               end)

      assert {:ok, [{"plug_x", got}]} = HubManager.processor_plugins(@namespace, processor_id)
      assert got.id == "plug_x"
      assert got.opts == %{a: 1}
    end

    test "removes a plugin from a cspec with multiple opts[:plugins]" do
      processor_id = "hm_test_remove_one"
      a = plugin("a")
      b = plugin("b")
      c = plugin("c")

      builder = processor_builder(plugins: [{"a", a}, {"b", b}, {"c", c}])
      insert_cspec(processor_id, {@namespace, "ce_1", builder})

      assert :ok =
               HubManager.update_processor_plugins(@namespace, processor_id, fn current ->
                 Enum.reject(current, fn {id, _} -> id == "b" end)
               end)

      assert {:ok, plugins} = HubManager.processor_plugins(@namespace, processor_id)
      assert Enum.map(plugins, fn {id, _} -> id end) == ["a", "c"]
    end

    test "handles the {namespace, processor} start-args shape" do
      processor_id = "hm_test_shape_2tuple"
      builder = processor_builder(plugins: [])
      insert_cspec(processor_id, {@namespace, builder})

      assert :ok =
               HubManager.update_processor_plugins(@namespace, processor_id, fn _ ->
                 [{"only", plugin("only")}]
               end)

      assert {:ok, [{"only", _}]} = HubManager.processor_plugins(@namespace, processor_id)
    end

    test "handles the {namespace, compute_engine_id, processor} start-args shape" do
      processor_id = "hm_test_shape_3tuple"
      builder = processor_builder(plugins: [])
      insert_cspec(processor_id, {@namespace, "ce_42", builder})

      assert :ok =
               HubManager.update_processor_plugins(@namespace, processor_id, fn _ ->
                 [{"only", plugin("only")}]
               end)

      assert {:ok, [{"only", _}]} = HubManager.processor_plugins(@namespace, processor_id)

      # Confirm the compute_engine_id position survives the rewrite — if
      # we lost it, alter_child_data's invariants break and the next
      # restart routes to a random compute engine.
      registry = ProcessHub.registry_dump(hub_id())
      assert {cspec, _, _} = Map.get(registry, processor_id)
      assert {Flezha.Processor.Overwatch, :start_link, [arg]} = cspec.start
      assert {@namespace, "ce_42", %Builder{}} = arg
    end

    test "returns {:error, _} when the processor is not registered" do
      assert {:error, _} =
               HubManager.update_processor_plugins(@namespace, "no_such_processor", fn p -> p end)
    end

    @tag :bench
    test "1000 sequential cspec-updates stay under the documented latency budget" do
      processor_id = "hm_bench_seq"
      builder = processor_builder(plugins: [])
      insert_cspec(processor_id, {@namespace, "ce_1", builder})

      times =
        for _ <- 1..1000 do
          {us, :ok} =
            :timer.tc(fn ->
              HubManager.update_processor_plugins(@namespace, processor_id, fn p -> p end)
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
