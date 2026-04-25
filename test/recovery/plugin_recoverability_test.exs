defmodule Flezha.Recovery.PluginRecoverabilityTest do
  use ExUnit.Case, async: true

  alias Flezha.Plugin
  alias Flezha.Recovery.PluginRecoverability

  test "Kafka.Producer is recoverable" do
    plugin = %Plugin{id: "k", provider: Flezha.Plugin.Provider.Kafka.Producer}
    assert PluginRecoverability.recoverable?(plugin)
  end

  test "Websocket is recoverable" do
    plugin = %Plugin{id: "w", provider: Flezha.Plugin.Provider.Websocket}
    assert PluginRecoverability.recoverable?(plugin)
  end

  test "BeamMsgForward is non-recoverable (caller pid baked in)" do
    plugin = %Plugin{id: "b", provider: Flezha.Plugin.Provider.BeamMsgForward}
    refute PluginRecoverability.recoverable?(plugin)
  end

  test "MonitoredIOForward is non-recoverable (monitor ref to caller pid)" do
    plugin = %Plugin{id: "m", provider: Flezha.Plugin.Provider.MonitoredIOForward}
    refute PluginRecoverability.recoverable?(plugin)
  end

  test "non-Plugin terms are not recoverable" do
    refute PluginRecoverability.recoverable?(:not_a_plugin)
    refute PluginRecoverability.recoverable?(nil)
    refute PluginRecoverability.recoverable?(%{not: :plugin})
  end

  test "non_recoverable_providers/0 returns the canonical list" do
    list = PluginRecoverability.non_recoverable_providers()
    assert Flezha.Plugin.Provider.BeamMsgForward in list
    assert Flezha.Plugin.Provider.MonitoredIOForward in list
  end
end
