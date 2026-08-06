defmodule FlezhaTest.Operative.OverwatchInitStateTest do
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Operative.Builder
  alias Flezha.Service.OperativeManager

  use ExUnit.Case

  @namespace "default"
  @factory "store_input"

  defmodule ProbeProvider do
    def on_operative_init(%Ctx.Init{} = ctx, opts) do
      pid = Map.fetch!(opts, :report_to)
      send(pid, {:probe, :called})
      ret = Map.get(opts, :return, %{})
      {:ok, %Ctx.Init{ctx | accumulated: Map.merge(ctx.accumulated, ret)}}
    end
  end

  defmodule FailProvider do
    def on_operative_init(_ctx, _opts), do: {:error, :intentional_failure}
  end

  setup do
    # Clean per-Entity state so a cold-start test sees no leftover WAL to
    # restore (restore now rehydrates on bind).
    Flezha.State.TestHelpers.reset_all_entities()
    on_exit(fn -> OperativeManager.stop_all_operatives(@namespace) end)
    :ok
  end

  test "4.2.1 cold start with empty inline init_values runs chain and SDK observes resolved map" do
    operative_id = "init_state_cold_start_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{
          "store_input_procedure" => %{
            "binary_inputs" => [],
            "text_inputs" => ["seeded_by_plugin"]
          }
        }
      },
      subscriptions: [{:on_operative_init, []}]
    }

    operative = %Builder{
      id: operative_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    assert_receive {:probe, :called}, 500

    {:ok, json_state} =
      OperativeManager.get_entity_state(@namespace, operative_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "store_input_procedure" => %{
               "binary_inputs" => [],
               "text_inputs" => ["seeded_by_plugin"]
             }
           }
  end

  test "4.2.2 cold start with non-empty inline init_values bypasses chain" do
    operative_id = "init_state_inline_bypass_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{"binary_inputs" => [], "text_inputs" => ["FROM_PLUGIN"]}
      },
      subscriptions: [{:on_operative_init, []}]
    }

    operative = %Builder{
      id: operative_id,
      factory_id: @factory,
      init_values: %{
        "store_input_procedure" => %{"binary_inputs" => [], "text_inputs" => ["FROM_INLINE"]}
      },
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperativeManager.start_operative(@namespace, operative)
    refute_receive {:probe, :called}, 200

    {:ok, json_state} =
      OperativeManager.get_entity_state(@namespace, operative_id)

    assert Flezha.Service.Payloader.json_to_map(json_state) == %{
             "store_input_procedure" => %{
               "binary_inputs" => [],
               "text_inputs" => ["FROM_INLINE"]
             }
           }
  end

  test "4.2.3 migration path does not invoke resolver even when plugins are configured" do
    operative_id = "init_state_migration_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{
          "store_input_procedure" => %{"binary_inputs" => [], "text_inputs" => ["x"]}
        }
      },
      subscriptions: [{:on_operative_init, []}]
    }

    operative = %Builder{
      id: operative_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, %{pid: pid, runner_id: current_id}} =
             OperativeManager.start_operative(@namespace, operative)

    assert_receive {:probe, :called}, 500

    try do
      GenServer.call(pid, {:runner_migrate, current_id})
    catch
      :exit, _ -> :ok
    end

    refute_receive {:probe, :called}, 300
  end

  test "4.2.4 {:error, reason} from resolver surfaces and operative is not registered" do
    operative_id = "init_state_error_surfaces_test"

    plugin = %Plugin{
      id: "fail",
      provider: FailProvider,
      opts: %{},
      subscriptions: [{:on_operative_init, []}]
    }

    operative = %Builder{
      id: operative_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:error, %{reason: :intentional_failure}} =
             OperativeManager.start_operative(@namespace, operative)

    assert OperativeManager.exist?(@namespace, operative_id) == false
  end
end
