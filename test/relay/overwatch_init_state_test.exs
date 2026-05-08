defmodule FlezhaTest.Relay.OverwatchInitStateTest do
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Relay.Builder
  alias Flezha.Service.RelayManager

  use ExUnit.Case

  @namespace "default"
  @factory "store_input"

  defmodule ProbeProvider do
    def on_relay_init(%Ctx.Init{} = ctx, opts) do
      pid = Map.fetch!(opts, :report_to)
      send(pid, {:probe, :called})
      ret = Map.get(opts, :return, %{})
      {:ok, %Ctx.Init{ctx | accumulated: Map.merge(ctx.accumulated, ret)}}
    end
  end

  defmodule FailProvider do
    def on_relay_init(_ctx, _opts), do: {:error, :intentional_failure}
  end

  setup do
    on_exit(fn -> RelayManager.stop_all_relays(@namespace) end)
    :ok
  end

  test "4.2.1 cold start with empty inline init_values runs chain and SDK observes resolved map" do
    relay_id = "init_state_cold_start_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{"binary_inputs" => [], "text_inputs" => ["seeded_by_plugin"]}
      },
      subscriptions: [{:on_relay_init, []}]
    }

    relay = %Builder{
      id: relay_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = RelayManager.start_relay(@namespace, relay)
    assert_receive {:probe, :called}, 500

    {:ok, json_state} =
      RelayManager.get_partition_state(@namespace, relay_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => ["seeded_by_plugin"]
           }
  end

  test "4.2.2 cold start with non-empty inline init_values bypasses chain" do
    relay_id = "init_state_inline_bypass_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{"binary_inputs" => [], "text_inputs" => ["FROM_PLUGIN"]}
      },
      subscriptions: [{:on_relay_init, []}]
    }

    relay = %Builder{
      id: relay_id,
      factory_id: @factory,
      init_values: %{"binary_inputs" => [], "text_inputs" => ["FROM_INLINE"]},
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = RelayManager.start_relay(@namespace, relay)
    refute_receive {:probe, :called}, 200

    {:ok, json_state} =
      RelayManager.get_partition_state(@namespace, relay_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "binary_inputs" => [],
             "text_inputs" => ["FROM_INLINE"]
           }
  end

  test "4.2.3 migration path does not invoke resolver even when plugins are configured" do
    relay_id = "init_state_migration_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{report_to: self(), return: %{"binary_inputs" => [], "text_inputs" => ["x"]}},
      subscriptions: [{:on_relay_init, []}]
    }

    relay = %Builder{
      id: relay_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, %{pid: pid, executor_id: current_id}} =
             RelayManager.start_relay(@namespace, relay)

    assert_receive {:probe, :called}, 500

    try do
      GenServer.call(pid, {:executor_migrate, current_id})
    catch
      :exit, _ -> :ok
    end

    refute_receive {:probe, :called}, 300
  end

  test "4.2.4 {:error, reason} from resolver surfaces and relay is not registered" do
    relay_id = "init_state_error_surfaces_test"

    plugin = %Plugin{
      id: "fail",
      provider: FailProvider,
      opts: %{},
      subscriptions: [{:on_relay_init, []}]
    }

    relay = %Builder{
      id: relay_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:error, %{reason: :intentional_failure}} =
             RelayManager.start_relay(@namespace, relay)

    assert RelayManager.exist?(@namespace, relay_id) == false
  end
end
