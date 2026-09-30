defmodule FlezhaTest.Operator.OverwatchInitStateTest do
  alias Flezha.Operator.Builder
  alias Flezha.Payloader
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Service.OperatorManager
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.NodeTestHelper

  use ExUnit.Case

  @namespace "default"
  @factory "store_input"

  defmodule ProbeProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        report_to: [type: :any, internal: true],
        return: [type: :any, internal: true],
        tag: [type: :any, internal: true]
      ]

    @impl true
    def on_operator_init(%Ctx.Init{} = ctx, opts) do
      pid = Map.fetch!(opts, :report_to)
      send(pid, {:probe, :called})
      ret = Map.get(opts, :return, %{})
      {:ok, %Ctx.Init{ctx | accumulated: Map.merge(ctx.accumulated, ret)}}
    end
  end

  defmodule FailProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        report_to: [type: :any, internal: true],
        return: [type: :any, internal: true],
        tag: [type: :any, internal: true]
      ]

    @impl true
    def on_operator_init(_ctx, _opts), do: {:error, :intentional_failure}
  end

  setup do
    # Clean per-Cell state so a cold-start test sees no leftover WAL to
    # restore (restore now rehydrates on bind).
    TestHelpers.reset_all_cells()
    on_exit(fn -> OperatorManager.stop_all_operators(@namespace) end)
    :ok
  end

  test "4.2.1 cold start with empty inline init_values runs chain and SDK observes resolved map" do
    operator_id = "init_state_cold_start_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{
          "store_input_tasklet" => %{
            "binary_inputs" => [],
            "text_inputs" => ["seeded_by_plugin"]
          }
        }
      },
      hooks: [{:on_operator_init, []}]
    }

    operator = %Builder{
      id: operator_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperatorManager.start_operator(@namespace, operator)
    assert_receive {:probe, :called}, 500

    {:ok, json_state} =
      OperatorManager.get_cell_state(@namespace, operator_id)

    assert Payloader.json_to_map(json_state) == %{
             "store_input_tasklet" => %{
               "binary_inputs" => [],
               "text_inputs" => ["seeded_by_plugin"]
             }
           }
  end

  test "a seeded first message is ingested first, on the entry channel its metadata names" do
    operator_id = "seed_input_first_test"

    operator = %Builder{
      id: operator_id,
      factory_id: @factory,
      opts: [seed_input: {"seeded-first", %{"channel" => "input"}}]
    }

    assert {:ok, _} = OperatorManager.start_operator(@namespace, operator)
    # A send that lands after the bind is the instance's second input: the
    # seed rides the bind itself, so nothing can be ingested ahead of it.
    assert FlezhaTest.Helper.NodeTestHelper.wait_until(
             fn ->
               not match?(
                 {:error, _},
                 OperatorManager.send_io_confirming(operator_id, @namespace, "second", %{})
               )
             end,
             5_000
           )

    assert FlezhaTest.Helper.NodeTestHelper.wait_until(
             fn -> text_inputs(operator_id) == ["seeded-first", "second"] end,
             5_000
           ),
           "the seed did not land first: #{inspect(text_inputs(operator_id))}"
  end

  defp text_inputs(operator_id) do
    case OperatorManager.get_cell_state(@namespace, operator_id) do
      {:ok, json} -> get_in(Payloader.json_to_map(json), ["store_input_tasklet", "text_inputs"])
      _ -> nil
    end
  end

  test "4.2.2 cold start with non-empty inline init_values bypasses chain" do
    operator_id = "init_state_inline_bypass_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{"binary_inputs" => [], "text_inputs" => ["FROM_PLUGIN"]}
      },
      hooks: [{:on_operator_init, []}]
    }

    operator = %Builder{
      id: operator_id,
      factory_id: @factory,
      init_values: %{
        "store_input_tasklet" => %{"binary_inputs" => [], "text_inputs" => ["FROM_INLINE"]}
      },
      opts: [plugins: [plugin]]
    }

    assert {:ok, _} = OperatorManager.start_operator(@namespace, operator)
    # The plugins are activated in the Overwatch's own turn; a call to it
    # answers after that.
    {:ok, overwatch} = OperatorManager.get_operator_pid(@namespace, operator_id)
    _ = :sys.get_state(overwatch)
    refute_received {:probe, :called}

    {:ok, json_state} =
      OperatorManager.get_cell_state(@namespace, operator_id)

    assert Payloader.json_to_map(json_state) == %{
             "store_input_tasklet" => %{
               "binary_inputs" => [],
               "text_inputs" => ["FROM_INLINE"]
             }
           }
  end

  test "4.2.3 migration path does not invoke resolver even when plugins are configured" do
    operator_id = "init_state_migration_test"

    plugin = %Plugin{
      id: "probe",
      provider: ProbeProvider,
      opts: %{
        report_to: self(),
        return: %{
          "store_input_tasklet" => %{"binary_inputs" => [], "text_inputs" => ["x"]}
        }
      },
      hooks: [{:on_operator_init, []}]
    }

    operator = %Builder{
      id: operator_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:ok, %{pid: pid, runner_id: current_id}} =
             OperatorManager.start_operator(@namespace, operator)

    assert_receive {:probe, :called}, 500

    try do
      GenServer.call(pid, {:runner_migrate, current_id})
    catch
      :exit, _ -> :ok
    end

    # The move binds in a task; the resolver would run inside it.
    assert NodeTestHelper.wait_until(fn -> :sys.get_state(pid).migrate_task == nil end, 10_000)
    refute_received {:probe, :called}
  end

  test "4.2.4 {:error, reason} from resolver surfaces and operator is not registered" do
    operator_id = "init_state_error_surfaces_test"

    plugin = %Plugin{
      id: "fail",
      provider: FailProvider,
      opts: %{},
      hooks: [{:on_operator_init, []}]
    }

    operator = %Builder{
      id: operator_id,
      factory_id: @factory,
      opts: [plugins: [plugin]]
    }

    assert {:error, {:start_failed, ^operator_id, :intentional_failure}} =
             OperatorManager.start_operator(@namespace, operator)

    assert OperatorManager.exist?(@namespace, operator_id) == false
  end
end

defmodule FlezhaTest.Operator.OverwatchCellStateGateTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  `:get_cell_state` is gated on the class's persistence switch, for either
  kind: a class that persists proceeds to query the runner; one whose switch is
  off, declared by the class or forced by the user, is refused as
  `{:error, :persistence_off}` and never contacts the runner (the handler
  replies synchronously instead of spawning the send).
  """
  # async: false: writes Application env and the class registry.
  use FlezhaTest.Case, fixtures: [:env, :tmp_app_data], async: false

  alias Flezha.Operator.Builder
  alias Flezha.Operator.ClassRegistry
  alias Flezha.Operator.Overwatch

  defp state_for(kind, id \\ "r_gate") do
    %Overwatch{
      id: id,
      namespace: "ns",
      # A runner id whose control unit is not registered, so the proceeding
      # path's spawned send resolves to a clean error rather than a crash.
      runner_id: "no_such_runner_#{System.unique_integer([:positive])}",
      builder: %Builder{id: id, factory_id: "f", kind: kind}
    }
  end

  test "an :unbounded kind proceeds to query the runner" do
    state = state_for(:unbounded)

    # {:noreply, state} == the gate passed and the handler went on to contact
    # the runner off-loop (replies later via GenServer.reply), as it does today.
    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_cell_state, %{}}, {self(), make_ref()}, state)
  end

  test "a durable :bounded kind proceeds to query the runner" do
    state = state_for(:bounded)

    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_cell_state, %{}}, {self(), make_ref()}, state)
  end

  test "a class that declares persistence off is refused, for either kind" do
    for kind <- [:unbounded, :bounded] do
      id = "declared_off_#{kind}"
      :ok = ClassRegistry.put_persistent(id, false)
      state = state_for(kind, id)

      # A synchronous {:reply, error, state} (no spawn) proves the runner was
      # not contacted for state.
      assert {:reply, {:error, {:persistence_off, ^id}}, ^state} =
               Overwatch.handle_call({:get_cell_state, %{}}, {self(), make_ref()}, state)

      :ok = ClassRegistry.put_persistent(id, true)
    end
  end

  test "a user opt-out is refused the same way" do
    Application.put_env(:flezha, :operator_persistence, %{"opted_out" => false})
    state = state_for(:unbounded, "opted_out")

    assert {:reply, {:error, {:persistence_off, "opted_out"}}, ^state} =
             Overwatch.handle_call({:get_cell_state, %{}}, {self(), make_ref()}, state)
  end
end

defmodule FlezhaTest.Operator.OverwatchOutputStampPassthroughTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  Output-identity pass-through: the SDK stamps `flz.cell` / `flz.seq` /
  `flz.key` on keyed-channel frames, and the controller must deliver them
  to the `:on_output` plugin chain unchanged — StreamChannel routes the
  frame's metadata as-is and Overwatch hands it to the plugin ctx as-is.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.StreamChannel

  defmodule ProbeProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        report_to: [type: :any, internal: true],
        return: [type: :any, internal: true],
        tag: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}
    @impl true
    def activate(_opts), do: :ok

    @impl true
    def on_output(ctx, opts) do
      send(Map.fetch!(opts, :report_to), {:on_output_observed, ctx.message, ctx.metadata})
      {:ok, ctx}
    end
  end

  @stamped_metadata %{
    "channel" => "out",
    "flz.cell" => "operator-1/ohlc_tasklet/BTC",
    "flz.seq" => "42.1",
    "flz.key" => "BTC/1m/1720000000",
    "user_key" => "user_value"
  }

  test "flz.* stamps flow unchanged through StreamChannel and the :on_output chain" do
    message = "candle-bytes"

    # Leg 1 — StreamChannel: a secondary-stream data frame for a mapped
    # operator is cast to the Overwatch pid with metadata untouched.
    channel_state = %{
      operator_mapping: %{"operator-1" => [self()]},
      last_seen: 0,
      liveness_ref: nil
    }

    assert {:noreply, _state} =
             StreamChannel.handle_info(
               {:flz_data, 7, "operator-1", message, @stamped_metadata},
               channel_state
             )

    assert_receive {:"$gen_cast", {:io_out, ^message, forwarded_metadata}}
    assert forwarded_metadata == @stamped_metadata

    # Leg 2 — Overwatch: the forwarded frame reaches the :on_output plugin
    # chain with the same metadata map.
    state = %Overwatch{
      id: "operator-1",
      namespace: "default",
      runner_id: "exec-1",
      factory_id: "test_factory",
      channel: nil,
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      builder: nil
    }

    plugin = %Plugin{
      id: "stamp_probe_plugin",
      provider: ProbeProvider,
      opts: %{report_to: self()},
      hooks: [on_output: []],
      namespace: "default",
      operator_id: "operator-1"
    }

    {:reply, :ok, state_with_plugin} =
      Overwatch.handle_call(
        {:plugin_subscribe, "stamp_probe_plugin", plugin},
        {self(), make_ref()},
        state
      )

    assert {:noreply, _} =
             Overwatch.handle_cast({:io_out, message, forwarded_metadata}, state_with_plugin)

    assert_receive {:on_output_observed, ^message, observed_metadata}

    assert observed_metadata == @stamped_metadata,
           "the :on_output plugin chain must observe the flz.* stamps unchanged; " <>
             "got #{inspect(observed_metadata)}"
  end
end

defmodule FlezhaTest.Operator.OverwatchResumeCountTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  Crash-resume gate (controller half): `Flezha.Operator.Replay.resume_cell_count/1`
  is the number of persisted Cells the bind-time restore path will ship a
  `RestoreInitState` for, threaded onto `AddOperator.resume_cell_count` so a
  durable bounded operator's run holds its first Tasklet until that many restores land.

  `0` for a fresh spawn and for a class whose persistence is off.
  """
  # async: false: writes Application env; a shared on-disk root.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  alias Flezha.Operator.Replay
  alias Flezha.State.{Paths, TestHelpers}

  # Drop a minimal WAL file on disk for a Cell so `all_cells/0` enumerates
  # it and `has_persisted_state?/1` returns true (as after a real durable run).
  defp seed_cell(operator_class, tasklet_id, cell_key) do
    cell_id = {operator_class, tasklet_id, cell_key}
    path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, TestHelpers.build_wal_record(1, <<>>))
    cell_id
  end

  test "a durable bounded operator counts its persisted Cells" do
    seed_cell("resume_class", "task", "even")
    seed_cell("resume_class", "task", "odd")

    assert Replay.resume_cell_count("resume_class") == 2
  end

  test "a fresh durable bounded operator (no WAL) waits for nothing" do
    assert Replay.resume_cell_count("unseeded_class") == 0
  end

  test "an opted-out class counts zero even with WAL on disk" do
    Application.put_env(:flezha, :operator_persistence, %{"optout_class" => false})
    seed_cell("optout_class", "task", "s")

    assert Replay.resume_cell_count("optout_class") == 0
  end
end

defmodule FlezhaTest.Operator.OverwatchPluginSubscribeTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  Regression: runtime `flz plugin enable` must transition the new plugin to
  `:running`, not leave it stuck in `:prepared`. The provider's `activate/1`
  callback is where upstream I/O (TLS connects, WS subscribes) happens; if
  it's never invoked, the operator produces no data.

  Bug observed in prod: WS plugins logged `init_provider prepared` and then
  silence — Kafka topics stayed empty because no `activate/1` ever fired.
  """

  alias Flezha.Operator.Overwatch
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table

  # async: false: the primary Logger; node-global tables.

  use ExUnit.Case, async: false

  # One case drives a handover with no control unit, which logs a deserialize
  # error by design; capture so a green run stays quiet.
  @moduletag :capture_log

  # Its `activate/1` fails while `:down` is set, so one provider drives both the
  # "upstream still down" and the "upstream recovered" halves of the ladder.
  defmodule FlappyProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        report_to: [type: :any, internal: true],
        return: [type: :any, internal: true],
        tag: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}

    @impl true
    def activate(_opts) do
      case :persistent_term.get({__MODULE__, :down}, true) do
        true -> {:error, :upstream_down}
        false -> :ok
        reason -> {:error, reason}
      end
    end

    @impl true
    def health(_opts), do: {:unhealthy, :upstream_down}
    @impl true
    def on_input(ctx, _opts), do: {:ok, ctx}
  end

  defmodule ProbeProvider do
    use Flezha.Plugin.ProviderBehaviour,
      options: [
        report_to: [type: :any, internal: true],
        return: [type: :any, internal: true],
        tag: [type: :any, internal: true]
      ]

    @impl true
    def init_provider(opts), do: {:ok, opts}

    @impl true
    def activate(opts) do
      send(Map.fetch!(opts, :report_to), {:activate_called, Map.get(opts, :tag)})
      :ok
    end

    @impl true
    def on_input(ctx, _opts), do: {:ok, ctx}
  end

  test "plugin_subscribe runtime path transitions plugin to :running and fires activate/1" do
    state = %Overwatch{
      id: "test_operator",
      namespace: "default",
      runner_id: "test_exec",
      factory_id: "test_factory",
      channel: nil,
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      builder: nil
    }

    plugin_id = "probe_runtime_plugin"

    plugin = %Plugin{
      id: plugin_id,
      provider: ProbeProvider,
      opts: %{report_to: self(), tag: :runtime_subscribe},
      hooks: [on_input: []],
      namespace: "default",
      operator_id: "test_operator"
    }

    {:reply, :ok, new_state} =
      Overwatch.handle_call({:plugin_subscribe, plugin_id, plugin}, {self(), make_ref()}, state)

    assert_receive {:activate_called, :runtime_subscribe},
                   500,
                   "ProbeProvider.activate/1 was never invoked — runtime plugin enable is " <>
                     "leaving the plugin stuck in :prepared (see Table.bulk_activate doc)"

    assert Table.phase(new_state.extensions, plugin_id) == :running,
           "expected plugin #{plugin_id} to be :running after :plugin_subscribe; " <>
             "got #{inspect(Table.phase(new_state.extensions, plugin_id))}"
  end

  test "alter_handover_state activates rehydrated plugins (HotSwap migration / rolling-restart)" do
    state = %Overwatch{
      id: "test_operator_handover",
      namespace: "default",
      runner_id: "test_exec_handover_no_control_unit",
      factory_id: "test_factory",
      channel: nil,
      extensions: Table.new(Overwatch.__extensions_catalog__()),
      builder: nil
    }

    plugin_id = "probe_handover_plugin"

    plugin = %Plugin{
      id: plugin_id,
      provider: ProbeProvider,
      opts: %{report_to: self(), tag: :handover_rehydrate},
      hooks: [on_input: []],
      namespace: "default",
      operator_id: "test_operator_handover"
    }

    handover_state = %{
      factory_id: "test_factory",
      plugins: [{plugin_id, plugin}],
      init_values: %{}
    }

    new_state = Overwatch.alter_handover_state(state, {handover_state, <<>>})

    assert_receive {:activate_called, :handover_rehydrate},
                   500,
                   "ProbeProvider.activate/1 was never invoked after HotSwap handover — " <>
                     "alter_handover_state is leaving rehydrated plugins stuck in :prepared"

    assert Table.phase(new_state.extensions, plugin_id) == :running,
           "expected plugin #{plugin_id} to be :running after handover; " <>
             "got #{inspect(Table.phase(new_state.extensions, plugin_id))}"
  end

  describe "a heal reports what it achieved" do
    setup do
      :persistent_term.put({FlappyProvider, :down}, true)
      on_exit(fn -> :persistent_term.erase({FlappyProvider, :down}) end)

      state = %Overwatch{
        id: "heal_operator",
        namespace: "default",
        runner_id: "heal_exec",
        factory_id: "test_factory",
        channel: nil,
        extensions: Table.new(Overwatch.__extensions_catalog__()),
        builder: nil
      }

      plugin = %Plugin{
        id: "src",
        provider: FlappyProvider,
        opts: %{},
        hooks: [on_input: []],
        namespace: "default",
        operator_id: "heal_operator"
      }

      %{state: state, plugin: plugin}
    end

    test "plugin_subscribe whose activation fails answers an error, not :ok", ctx do
      {:reply, reply, new_state} =
        Overwatch.handle_call(
          {:plugin_subscribe, "src", ctx.plugin},
          {self(), make_ref()},
          ctx.state
        )

      assert {:error, {:activation_failed, :upstream_down}} = reply

      assert Table.phase(new_state.extensions, "src") == :prepared,
             "the plugin acquired no upstream resources, so it must not be :running"

      # And it is now visible as not-running, which is what the convergence
      # loop diffs as missing and heals under backoff.
      assert Table.running_ids(new_state.extensions) == []
      assert Table.subscribed_ids(new_state.extensions) == ["src"]
    end

    test "an activation failure the catalogue maps keeps its own code", ctx do
      reason = {:http_listen_failed, %{operator: "heal_operator", port: 1, reason: :eaddrinuse}}
      :persistent_term.put({FlappyProvider, :down}, reason)

      {:reply, reply, new_state} =
        Overwatch.handle_call(
          {:plugin_subscribe, "src", ctx.plugin},
          {self(), make_ref()},
          ctx.state
        )

      assert {:error, ^reason} = reply
      assert Table.phase(new_state.extensions, "src") == :prepared
    end

    test "a bare reason that shares a catalogued tag is still an activation failure", ctx do
      :persistent_term.put({FlappyProvider, :down}, :closed)

      {:reply, reply, _state} =
        Overwatch.handle_call(
          {:plugin_subscribe, "src", ctx.plugin},
          {self(), make_ref()},
          ctx.state
        )

      assert {:error, {:activation_failed, :closed}} = reply
    end

    test "plugin_restart whose activation fails answers an error, not :ok", ctx do
      {:reply, reply, new_state} =
        Overwatch.handle_call(
          {:plugin_restart, "src", ctx.plugin},
          {self(), make_ref()},
          ctx.state
        )

      assert {:error, {:activation_failed, :upstream_down}} = reply
      assert Table.phase(new_state.extensions, "src") == :prepared

      # `Heal.plugin_call/3` matches `{:ok, :ok} -> :ok` and everything else to
      # `:error`, so this reply is recorded as a FAILED heal with no change to
      # `Heal` — a remedy cannot hide the fault it failed to fix.
      refute match?(:ok, reply)
    end

    test "bringing up an already-running plugin changes nothing", %{state: state} do
      plugin = %Plugin{
        id: "steady",
        provider: ProbeProvider,
        opts: %{report_to: self(), tag: :first_activation},
        hooks: [on_input: []],
        namespace: "default",
        operator_id: "heal_operator"
      }

      {:reply, :ok, state} =
        Overwatch.handle_call({:plugin_subscribe, "steady", plugin}, {self(), make_ref()}, state)

      assert_receive {:activate_called, :first_activation}
      assert Table.phase(state.extensions, "steady") == :running

      # `bulk_activate/2` skips a `:running` plugin, so the idempotent path is a
      # no-op rather than a second upstream acquisition.
      {:reply, reply, again} =
        Overwatch.handle_call({:plugin_subscribe, "steady", plugin}, {self(), make_ref()}, state)

      assert reply == :ok
      assert Table.phase(again.extensions, "steady") == :running
      refute_received {:activate_called, :first_activation}
    end

    test "the same restart reports :ok once upstream recovers", ctx do
      {:reply, {:error, _}, state} =
        Overwatch.handle_call(
          {:plugin_restart, "src", ctx.plugin},
          {self(), make_ref()},
          ctx.state
        )

      :persistent_term.put({FlappyProvider, :down}, false)

      {:reply, reply, healed} =
        Overwatch.handle_call({:plugin_restart, "src", ctx.plugin}, {self(), make_ref()}, state)

      assert reply == :ok
      assert Table.phase(healed.extensions, "src") == :running
      assert Table.running_ids(healed.extensions) == ["src"]
    end
  end
end
