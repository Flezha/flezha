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
      executor_id: "test_exec",
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
      executor_id: "test_exec_handover_no_control_unit",
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
end
