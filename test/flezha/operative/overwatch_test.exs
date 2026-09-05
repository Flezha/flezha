defmodule FlezhaTest.Operative.OverwatchInitStateTest do
  alias Flezha.Operative.Builder
  alias Flezha.Payloader
  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Ctx
  alias Flezha.Service.OperativeManager
  alias Flezha.State.TestHelpers

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
    def on_operative_init(%Ctx.Init{} = ctx, opts) do
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
    def on_operative_init(_ctx, _opts), do: {:error, :intentional_failure}
  end

  setup do
    # Clean per-Entity state so a cold-start test sees no leftover WAL to
    # restore (restore now rehydrates on bind).
    TestHelpers.reset_all_entities()
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

    assert Payloader.json_to_map(json_state) == %{
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
    # The plugins are activated in the Overwatch's own turn; a call to it
    # answers after that.
    {:ok, overwatch} = OperativeManager.get_operative_pid(@namespace, operative_id)
    _ = :sys.get_state(overwatch)
    refute_received {:probe, :called}

    {:ok, json_state} =
      OperativeManager.get_entity_state(@namespace, operative_id)

    assert Payloader.json_to_map(json_state) == %{
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

    refute_received {:probe, :called}
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

defmodule FlezhaTest.Operative.OverwatchEntityStateGateTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  §5b.2: `:get_entity_state` is gated on `persistent_state?` (kind+class).
  `:stream` and a durable-by-default `:workflow` class proceed to query the
  runner exactly as before; a `:workflow` class that opted out of persistence —
  or any stateless kind — is rejected with
  `{:error, {:unsupported_for_kind, :persistent_state}}` and never contacts the
  runner (the handler replies synchronously instead of spawning the send).
  """
  # async: false: writes Application env.
  use ExUnit.Case, async: false

  alias Flezha.Operative.Builder
  alias Flezha.Operative.Overwatch

  setup do
    on_exit(fn -> Application.delete_env(:flezha, :operative_persistence) end)
    :ok
  end

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

  test "an :stream kind proceeds to query the runner" do
    state = state_for(:stream)

    # {:noreply, state} == the gate passed and the handler went on to contact
    # the runner off-loop (replies later via GenServer.reply), as it does today.
    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end

  test "a durable :workflow kind proceeds to query the runner" do
    state = state_for(:workflow)

    assert {:noreply, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end

  test "an opted-out :workflow class is rejected and does not contact the runner" do
    Application.put_env(:flezha, :operative_persistence, %{"opted_out" => false})
    state = state_for(:workflow, "opted_out")

    # A synchronous {:reply, error, state} (no spawn) proves the runner was not
    # contacted for state.
    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end

  test "a stateless kind is rejected and does not contact the runner" do
    state = state_for(:made_up_kind)

    assert {:reply, {:error, {:unsupported_for_kind, :persistent_state}}, ^state} =
             Overwatch.handle_call({:get_entity_state, %{}}, {self(), make_ref()}, state)
  end
end

defmodule FlezhaTest.Operative.OverwatchOutputStampPassthroughTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  Output-identity pass-through: the SDK stamps `flz.entity` / `flz.seq` /
  `flz.key` on keyed-channel frames, and the controller must deliver them
  to the `:on_output` plugin chain unchanged — StreamChannel routes the
  frame's metadata as-is and Overwatch hands it to the plugin ctx as-is.
  """
  use ExUnit.Case, async: true

  alias Flezha.Operative.Overwatch
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
    "flz.entity" => "operative-1/ohlc_procedure/BTC",
    "flz.seq" => "42.1",
    "flz.key" => "BTC/1m/1720000000",
    "user_key" => "user_value"
  }

  test "flz.* stamps flow unchanged through StreamChannel and the :on_output chain" do
    message = "candle-bytes"

    # Leg 1 — StreamChannel: a secondary-stream data frame for a mapped
    # operative is cast to the Overwatch pid with metadata untouched.
    channel_state = %{
      operative_mapping: %{"operative-1" => [self()]},
      last_seen: 0,
      liveness_ref: nil
    }

    assert {:noreply, _state} =
             StreamChannel.handle_info(
               {:flz_data, 7, "operative-1", message, @stamped_metadata},
               channel_state
             )

    assert_receive {:"$gen_cast", {:io_out, ^message, forwarded_metadata}}
    assert forwarded_metadata == @stamped_metadata

    # Leg 2 — Overwatch: the forwarded frame reaches the :on_output plugin
    # chain with the same metadata map.
    state = %Overwatch{
      id: "operative-1",
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
      subscriptions: [on_output: []],
      namespace: "default",
      operative_id: "operative-1"
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

defmodule FlezhaTest.Operative.OverwatchResumeCountTest do
  use Flezha.Plugin.ProviderBehaviour,
    options: [
      report_to: [type: :any, internal: true],
      return: [type: :any, internal: true],
      tag: [type: :any, internal: true]
    ]

  @moduledoc """
  §5b.2 crash-resume gate (controller half): `Flezha.Operative.Replay.resume_entity_count/2`
  is the number of persisted Entities the bind-time restore path will ship a
  `RestoreInitState` for, threaded onto `AddOperative.resume_entity_count` so a
  durable workflow driver holds its first Procedure until that many restores land.

  Non-zero only for a durable `:workflow` class with WAL on disk; `0` for a fresh
  spawn, a stream operative, or an opted-out workflow one.
  """
  # async: false: writes Application env; a shared on-disk root.
  use ExUnit.Case, async: false

  alias Flezha.Operative.Replay
  alias Flezha.State.{Paths, TestHelpers}

  setup do
    # A fresh tmp state_root isolates each case: `all_entities/0` +
    # `has_persisted_state?/1` then see only the Entities this test seeds.
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :operative_persistence)
      File.rm_rf(root)
    end)

    :ok
  end

  # Drop a minimal WAL file on disk for an Entity so `all_entities/0` enumerates
  # it and `has_persisted_state?/1` returns true (as after a real durable run).
  defp seed_entity(operative_class, procedure_id, entity_key) do
    entity_id = {operative_class, procedure_id, entity_key}
    path = Paths.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, TestHelpers.build_wal_record(1, <<>>))
    entity_id
  end

  test "a durable workflow operative counts its persisted Entities" do
    seed_entity("resume_class", "proc", "even")
    seed_entity("resume_class", "proc", "odd")

    assert Replay.resume_entity_count(:workflow, "resume_class") == 2
  end

  test "a fresh durable workflow operative (no WAL) waits for nothing" do
    assert Replay.resume_entity_count(:workflow, "unseeded_class") == 0
  end

  test "a stream operative never waits for a workflow-style restore gate" do
    seed_entity("cont_class", "proc", "s")

    assert Replay.resume_entity_count(:stream, "cont_class") == 0
  end

  test "an opted-out workflow class counts zero even with WAL on disk" do
    Application.put_env(:flezha, :operative_persistence, %{"optout_class" => false})
    seed_entity("optout_class", "proc", "s")

    assert Replay.resume_entity_count(:workflow, "optout_class") == 0
  end
end

defmodule FlezhaTest.Operative.OverwatchPluginSubscribeTest do
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
  it's never invoked, the operative produces no data.

  Bug observed in prod: WS plugins logged `init_provider prepared` and then
  silence — Kafka topics stayed empty because no `activate/1` ever fired.
  """

  alias Flezha.Operative.Overwatch
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
      if :persistent_term.get({__MODULE__, :down}, true),
        do: {:error, :upstream_down},
        else: :ok
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
      id: "test_operative",
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
      subscriptions: [on_input: []],
      namespace: "default",
      operative_id: "test_operative"
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
      id: "test_operative_handover",
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
      subscriptions: [on_input: []],
      namespace: "default",
      operative_id: "test_operative_handover"
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
        id: "heal_operative",
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
        subscriptions: [on_input: []],
        namespace: "default",
        operative_id: "heal_operative"
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
        subscriptions: [on_input: []],
        namespace: "default",
        operative_id: "heal_operative"
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
