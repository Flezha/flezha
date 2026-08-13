defmodule FlezhaTest.Service.OperativeInfoTest do
  @moduledoc """
  §6.1: `flz operative info` surfaces the operative's `kind`, lifecycle `phase`
  (`running` for a live operative in Phase 1), and `uptime_ms`, in both the text
  and JSON renderings. The richer workflow phases (compensating / completing /
  finished) + elapsed-vs-max-duration + per-procedure progress arrive with the
  saga + retention + SDK-reporting work in later phases.
  """

  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.{Completion, TestHelpers}

  @namespace "operative-info-test"

  setup :set_mimic_global

  setup do
    # Isolate the state root `operative_info` scans for finished-job records.
    base = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    :ok
  end

  defp stub_one(state) do
    OperativeManager
    |> stub(:get_operative_ids, fn _ns -> {:ok, ["op-1"]} end)
    |> stub(:get_overwatch_state, fn _ns, "op-1" -> {:ok, state} end)
  end

  defp workflow_state(uptime_ms) do
    %{
      builder: %{kind: :workflow},
      runner_id: "exec1",
      namespace: @namespace,
      started_at: System.system_time(:millisecond) - uptime_ms,
      default_input_type: "2",
      extensions: %{plugins: %{}}
    }
  end

  test "json output carries kind, phase and uptime_ms" do
    stub_one(workflow_state(120))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)

    assert proc["id"] == "op-1"
    assert proc["kind"] == "workflow"
    assert proc["phase"] == "running"
    assert is_integer(proc["uptime_ms"]) and proc["uptime_ms"] >= 120
  end

  test "text output shows Kind / Phase / Uptime lines" do
    stub_one(workflow_state(0))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "text") end)

    assert out =~ "Kind: workflow"
    assert out =~ "Phase: running"
    assert out =~ "Uptime (ms):"
  end

  test "a stream operative reports its kind too" do
    stub_one(%{workflow_state(0) | builder: %{kind: :stream}})

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)
    assert proc["kind"] == "stream"
  end

  test "a finished workflow operative is surfaced from its completion record" do
    # No live operatives; the finished one comes only from its on-disk record.
    stub(OperativeManager, :get_operative_ids, fn _ns -> {:ok, []} end)

    :ok =
      Completion.write("done-1", %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "RES",
        finished_at: System.system_time(:second)
      })

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => procs} = Jason.decode!(out)
    proc = Enum.find(procs, &(&1["id"] == "done-1"))

    assert proc["kind"] == "workflow"
    assert proc["phase"] == "finished"
    assert proc["status"] == "ok"
    assert proc["result"] == Base.encode64("RES")
  end

  test "a finished operative outside the retention window is not surfaced" do
    stub(OperativeManager, :get_operative_ids, fn _ns -> {:ok, []} end)
    Application.put_env(:flezha, :workflow_completed_retention_hours, %{"done-old" => 0})
    on_exit(fn -> Application.delete_env(:flezha, :workflow_completed_retention_hours) end)

    :ok =
      Completion.write("done-old", %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "",
        finished_at: System.system_time(:second) - 10
      })

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => procs} = Jason.decode!(out)
    assert Enum.find(procs, &(&1["id"] == "done-old")) == nil
  end
end
