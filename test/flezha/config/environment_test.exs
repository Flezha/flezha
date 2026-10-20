defmodule Flezha.Config.EnvironmentTest do
  @moduledoc """
  The boot scan: a `:flezha` key no row reads, or a value its row's type
  refuses, stops the boot naming it, rather than configuring nothing.
  """
  use ExUnit.Case, async: true

  alias Flezha.Config.Environment

  defp refusal(env) do
    assert_raise ArgumentError, fn -> Environment.check!(env) end
  end

  test "a retired key is refused by name" do
    # A per-class persistence map no reader takes any more.
    assert refusal(relay_persistence: %{"eo_class" => false}).message =~
             ":relay_persistence is no setting"
  end

  test "a per-class map over a class's own value is refused" do
    assert refusal(credit_window_bytes: %{"MyClass" => 1_024}).message =~
             ":credit_window_bytes is no setting"
  end

  test "a misspelt key is refused naming the row it was meant for" do
    assert refusal(compact_after_byte: 1_024).message =~
             ":compact_after_byte is no setting; the nearest is :default_compact_after_bytes"
  end

  test "a value its row's type refuses is refused naming the key, the value and the type" do
    assert refusal(call_pending_max: "10").message =~
             ~s(:call_pending_max is "10", not a :positive_int)
  end

  test "a duration below zero is refused" do
    assert refusal(compact_after_age_ms: -5).message =~ ":compact_after_age_ms is -5, not a :ms"
  end

  test "a sweep interval of zero is refused" do
    assert refusal(durability_tier: [gc_interval_ms: 0]).message =~
             "{:durability_tier, :gc_interval_ms} is 0, not a :ms of at least 1"
  end

  test "a cell stall threshold of zero is refused" do
    assert refusal(convergence_cell_stall_ms: 0).message =~
             ":convergence_cell_stall_ms is 0, not a :ms of at least 1"
  end
end
