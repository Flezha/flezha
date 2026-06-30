defmodule FlezhaTest.Service.OperativeInfoTest do
  @moduledoc """
  §6.1: `flz operative info` surfaces the operative's `kind`, lifecycle `phase`
  (`running` for a live operative in Phase 1), and `uptime_ms`, in both the text
  and JSON renderings. The richer bounded phases (compensating / completing /
  finished) + elapsed-vs-max-duration + per-operation progress arrive with the
  saga + retention + SDK-reporting work in later phases.
  """

  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager

  @namespace "operative-info-test"

  setup :set_mimic_global

  defp stub_one(state) do
    OperativeManager
    |> stub(:get_operative_ids, fn _ns -> {:ok, ["op-1"]} end)
    |> stub(:get_overwatch_state, fn _ns, "op-1" -> {:ok, state} end)
  end

  defp bounded_state(uptime_ms) do
    %{
      builder: %{kind: :bounded},
      executor_id: "exec1",
      namespace: @namespace,
      started_at: System.system_time(:millisecond) - uptime_ms,
      default_input_type: "2",
      extensions: %{plugins: %{}}
    }
  end

  test "json output carries kind, phase and uptime_ms" do
    stub_one(bounded_state(120))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [op]} = Jason.decode!(out)

    assert op["id"] == "op-1"
    assert op["kind"] == "bounded"
    assert op["phase"] == "running"
    assert is_integer(op["uptime_ms"]) and op["uptime_ms"] >= 120
  end

  test "text output shows Kind / Phase / Uptime lines" do
    stub_one(bounded_state(0))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "text") end)

    assert out =~ "Kind: bounded"
    assert out =~ "Phase: running"
    assert out =~ "Uptime (ms):"
  end

  test "a continuous operative reports its kind too" do
    stub_one(%{bounded_state(0) | builder: %{kind: :continuous}})

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [op]} = Jason.decode!(out)
    assert op["kind"] == "continuous"
  end
end
