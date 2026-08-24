defmodule FlezhaTest.Operative.OverwatchPluginSubscribeTest do
  @moduledoc """
  Regression: runtime `flz plugin enable` must transition the new plugin to
  `:running`, not leave it stuck in `:prepared`. The provider's `activate/1`
  callback is where upstream I/O (TLS connects, WS subscribes) happens; if
  it's never invoked, the operative produces no data.

  Bug observed in prod: WS plugins logged `init_provider prepared` and then
  silence — Kafka topics stayed empty because no `activate/1` ever fired.
  """

  alias Flezha.Plugin
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Operative.Overwatch

  use ExUnit.Case, async: false

  # One case drives a handover with no control unit, which logs a deserialize
  # error by design; capture so a green run stays quiet.
  @moduletag :capture_log

  # Its `activate/1` fails while `:down` is set, so one provider drives both the
  # "upstream still down" and the "upstream recovered" halves of the ladder.
  defmodule FlappyProvider do
    def init_provider(opts), do: {:ok, opts}

    def activate(_opts) do
      if :persistent_term.get({__MODULE__, :down}, true),
        do: {:error, :upstream_down},
        else: :ok
    end

    def health(_opts), do: {:unhealthy, :upstream_down}
    def on_input(ctx, _opts), do: {:ok, ctx}
  end

  defmodule ProbeProvider do
    def init_provider(opts), do: {:ok, opts}

    def activate(opts) do
      send(Map.fetch!(opts, :report_to), {:activate_called, Map.get(opts, :tag)})
      :ok
    end

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
      # `Heal` — the remedy can no longer hide the fault it failed to fix.
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
      refute_receive {:activate_called, :first_activation}, 100
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
