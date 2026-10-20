defmodule Flezha.Observability.TelemetryBridgeGoldenTest do
  @moduledoc """
  The bridge's tables pinned term for term, so reshaping where its lines are
  kept leaves every bridged event, line and registration as it was. A change
  that means to alter one edits `telemetry_bridge_golden.exs` by exactly the
  lines it names.
  """

  use ExUnit.Case, async: true

  alias Flezha.Observability.TelemetryBridge

  @golden Path.expand("telemetry_bridge_golden.exs", __DIR__)

  test "the bridge's tables and registrations equal the golden" do
    {golden, _binding} = Code.eval_file(@golden)
    current = snapshot()

    assert Map.keys(current) == Map.keys(golden)

    for {key, value} <- golden,
        do: assert(current[key] == value, "#{key} differs from the golden")
  end

  # A registration's kinds and signals are compared as sets: their order follows
  # map iteration, which says nothing to a reader.
  @doc false
  def snapshot() do
    %{
      mapping: TelemetryBridge.mapping(),
      derived: TelemetryBridge.derived(),
      unexported: TelemetryBridge.unexported(),
      unwritten: TelemetryBridge.unwritten(),
      lines: TelemetryBridge.lines(),
      registrations:
        %{}
        |> TelemetryBridge.registrations()
        |> Enum.map(fn entry ->
          %{
            entry
            | kinds: Enum.sort(entry.kinds),
              signals: Enum.sort_by(entry.signals, & &1.name)
          }
        end)
        |> Enum.sort_by(& &1.name)
    }
  end
end
