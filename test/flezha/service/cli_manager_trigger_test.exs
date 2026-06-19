defmodule FlezhaTest.Service.CliManagerTriggerTest do
  @moduledoc """
  Guards the legacy `--trigger=<mode>` → subscription mapping.

  Fails when: a `--trigger=proactive` plugin is wired to `:on_terminate`
  (or any non-`:on_interval` extension), so a proactive plugin would never
  receive its periodic tick — the regression this test was written to catch.
  """
  use ExUnit.Case, async: true

  alias Flezha.Service.CliManager

  test "proactive and interval both map onto :on_interval" do
    assert CliManager.trigger_to_subscription("proactive") == [{:on_interval, [interval_ms: 5_000]}]
    assert CliManager.trigger_to_subscription("interval") == [{:on_interval, [interval_ms: 5_000]}]
  end

  test "on_input / on_output map to their own extensions; unknown falls back to on_input" do
    assert CliManager.trigger_to_subscription("on_input") == [{:on_input, []}]
    assert CliManager.trigger_to_subscription("on_output") == [{:on_output, []}]
    assert CliManager.trigger_to_subscription("bogus") == [{:on_input, []}]
  end
end
