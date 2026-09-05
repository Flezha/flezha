defmodule FlezhaTest.Service.OperativeInfoTest do
  @moduledoc """
  `flz operative info` surfaces the operative's `kind`, its lifecycle `phase`
  (`running` / `suspended` / `parked`, and `finished` for a retained completion
  record), the convergence loop's per-operative `health` verdict, and
  `uptime_ms`, in both the text and JSON renderings. Phase and health are two
  axes of one operative and must never contradict each other: the phase is read
  from the operative's own state, the health from the map the loop publishes and
  `flz operative list` reads.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureIO

  alias Flezha.Convergence.Intent
  alias Flezha.Convergence.Snapshot
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager
  alias Flezha.State.{Completion, TestHelpers}

  @namespace "operative-info-test"

  setup :set_mimic_global_until_exit

  setup do
    # Isolate the state root `operative_info` scans for finished-job records.
    TestHelpers.tmp_app_data_dir()

    # The published health map is process-global; restore what was there.
    published = Snapshot.current()
    on_exit(fn -> Snapshot.publish(published) end)
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

  # `Lifecycle.park/2` stores the failure reason in `init_error` and leaves the
  # Overwatch alive, so a parked operative is still listed and still readable.
  defp parked_state(reason \\ {:no_runner, "exec1"}),
    do: Map.put(workflow_state(0), :init_error, reason)

  # The observation `Convergence.Probe` builds from an Overwatch state, so both
  # operator surfaces are driven from one operative state rather than two
  # fixtures that could drift apart.
  defp observed(state) do
    %{
      bound_runner: state.runner_id,
      parked?: Map.get(state, :init_error) != nil,
      suspended?: Map.get(state, :suspended, false)
    }
  end

  # The records one convergence tick would mint for this operative — judged
  # through the same two calls the Loop makes, so the surfaces under test read
  # what the loop would really publish.
  defp judged(state) do
    intent = %Intent{
      operatives: [%{namespace: @namespace, operative_id: "op-1", factory_id: nil, plugins: []}]
    }

    FlezhaTest.Helper.ConvergenceFixtures.judge(intent, reality(state))
  end

  defp reality(state) do
    %{
      operatives: %{{@namespace, "op-1"} => observed(state)},
      active_runners: %{@namespace => MapSet.new([state.runner_id])}
    }
  end

  test "json output reports a parked operative as parked, not running" do
    stub_one(parked_state())

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)

    assert proc["phase"] == "parked"
  end

  test "text output reports a parked operative as parked, not running" do
    stub_one(parked_state())

    out = capture_io(fn -> CliManager.operative_info(@namespace, "text") end)

    assert out =~ "Phase: parked"
    refute out =~ "Phase: running"
  end

  test "a suspended instance still reports suspended when it also carries a park reason" do
    stub_one(Map.put(parked_state(), :suspended, true))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)

    assert proc["phase"] == "suspended"
  end

  test "info and the loop's verdict record agree about one parked operative" do
    state = parked_state()
    stub_one(state)

    health = judged(state)
    assert health[{:operative, {@namespace, "op-1"}}].state == :parked

    Snapshot.publish(Map.put(Snapshot.current(), :operative_health, health))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "parked"
    assert proc["phase"] == "parked"

    text = capture_io(fn -> CliManager.operative_info(@namespace, "text") end)
    assert text =~ "Health: parked"
    assert text =~ "Phase: parked"
  end

  test "a healthy operative carries the loop's own verdict on both surfaces" do
    state = workflow_state(0)
    stub_one(state)

    health = judged(state)
    assert health[{:operative, {@namespace, "op-1"}}].state == :healthy

    Snapshot.publish(Map.put(Snapshot.current(), :operative_health, health))

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "healthy"
    assert proc["phase"] == "running"
  end

  test "health is unknown, never invented, before the loop has observed the operative" do
    stub_one(parked_state())

    out = capture_io(fn -> CliManager.operative_info(@namespace, "json") end)
    %{"operatives" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "unknown"
    assert proc["phase"] == "parked"
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
