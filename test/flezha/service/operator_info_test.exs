defmodule FlezhaTest.Service.OperatorInfoTest do
  @moduledoc """
  `flz operator info` surfaces the operator's `kind`, its lifecycle `phase`
  (`running` / `suspended` / `parked`, and `finished` for a retained completion
  record), the convergence loop's per-operator `health` verdict, and
  `uptime_ms`, in both the text and JSON renderings. Phase and health are two
  axes of one operator and must never contradict each other: the phase is read
  from the operator's own state, the health from the map the loop publishes and
  `flz operator list` reads.
  """

  # async: false: writes Application env; Mimic global mode; Mimic stubs.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureIO

  alias Flezha.Config.Durations
  alias Flezha.Convergence.Intent
  alias Flezha.Convergence.Snapshot
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperatorManager
  alias Flezha.State.Completion

  @namespace "operator-info-test"

  setup :set_mimic_global_until_exit

  setup do
    # The published health map is process-global; restore what was there.
    published = Snapshot.current()
    on_exit(fn -> Snapshot.publish(published) end)
    :ok
  end

  defp stub_one(state) do
    OperatorManager
    |> stub(:get_operator_ids, fn _ns -> {:ok, ["op-1"]} end)
    |> stub(:get_overwatch_state, fn _ns, "op-1" -> {:ok, state} end)
  end

  defp bounded_state(uptime_ms) do
    %{
      builder: %{kind: :bounded},
      runner_id: "exec1",
      namespace: @namespace,
      started_at: System.system_time(:millisecond) - uptime_ms,
      default_input_type: "2",
      extensions: %{plugins: %{}}
    }
  end

  test "json output carries kind, phase and uptime_ms" do
    stub_one(bounded_state(120))

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["id"] == "op-1"
    assert proc["kind"] == "bounded"
    assert proc["phase"] == "running"
    assert is_integer(proc["uptime_ms"]) and proc["uptime_ms"] >= 120
  end

  test "text output shows Kind / Phase / Uptime lines" do
    stub_one(bounded_state(0))

    out = capture_io(fn -> CliManager.operator_info(@namespace, "text") end)

    assert out =~ "Kind: bounded"
    assert out =~ "Phase: running"
    assert out =~ "Uptime (ms):"
  end

  test "an unbounded operator reports its kind too" do
    stub_one(%{bounded_state(0) | builder: %{kind: :unbounded}})

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)
    assert proc["kind"] == "unbounded"
  end

  # `Lifecycle.park/2` stores the failure reason in `init_error` and leaves the
  # Overwatch alive, so a parked operator is still listed and still readable.
  defp parked_state(reason \\ {:no_runner, "exec1"}),
    do: Map.put(bounded_state(0), :init_error, reason)

  # The observation `Convergence.Probe` builds from an Overwatch state, so both
  # user surfaces are driven from one operator state rather than two
  # fixtures that could drift apart.
  defp observed(state) do
    %{
      bound_runner: state.runner_id,
      parked?: Map.get(state, :init_error) != nil,
      suspended?: Map.get(state, :suspended, false)
    }
  end

  # The records one convergence tick would mint for this operator — judged
  # through the same two calls the Loop makes, so the surfaces under test read
  # what the loop would really publish.
  defp judged(state) do
    intent = %Intent{
      operators: [%{namespace: @namespace, operator_id: "op-1", factory_id: nil, plugins: []}]
    }

    FlezhaTest.Helper.ConvergenceFixtures.judge(intent, reality(state))
  end

  defp reality(state) do
    %{
      operators: %{{@namespace, "op-1"} => observed(state)},
      active_runners: %{@namespace => MapSet.new([state.runner_id])}
    }
  end

  test "json output reports a parked operator as parked, not running" do
    stub_one(parked_state())

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["phase"] == "parked"
  end

  test "text output reports a parked operator as parked, not running" do
    stub_one(parked_state())

    out = capture_io(fn -> CliManager.operator_info(@namespace, "text") end)

    assert out =~ "Phase: parked"
    refute out =~ "Phase: running"
  end

  test "a suspended instance still reports suspended when it also carries a park reason" do
    stub_one(Map.put(parked_state(), :suspended, true))

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["phase"] == "suspended"
  end

  test "info and the loop's verdict record agree about one parked operator" do
    state = parked_state()
    stub_one(state)

    health = judged(state)
    assert health[{:operator, {@namespace, "op-1"}}].state == :parked

    Snapshot.publish(Map.put(Snapshot.current(), :operator_health, health))

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "parked"
    assert proc["phase"] == "parked"

    text = capture_io(fn -> CliManager.operator_info(@namespace, "text") end)
    assert text =~ "Health: parked"
    assert text =~ "Phase: parked"
  end

  # A bind that found no runner records its outcome the way a park does, but the
  # operator holds no binding at all — which is what separates the two words.
  defp held_state,
    do:
      %{bounded_state(0) | runner_id: nil}
      |> Map.put(:init_error, {:runner_not_ready, :timeout})

  # A held operator's health as the loop publishes it, the intent unmet for
  # `unmet_ms`.
  defp publish_held(unmet_ms) do
    state = held_state()
    stub_one(state)

    health = judged(state)
    assert health[{:operator, {@namespace, "op-1"}}].state == :unbound

    Snapshot.current()
    |> Map.put(:operator_health, health)
    |> Map.put(:unmet_intent_ms, unmet_ms)
    |> Snapshot.publish()
  end

  # fails when: the phase derivation tests `init_error` before the binding, so a
  # held operator reads `parked` on the surface the loop calls `unbound`; or the
  # grace outlives its window and hides the verdict.
  test "info and the loop's verdict record agree that a held operator is unbound" do
    publish_held(Durations.get(:convergence_unmet_intent_grace_ms) + 1)

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "unbound"
    assert proc["phase"] == "unbound"

    text = capture_io(fn -> CliManager.operator_info(@namespace, "text") end)
    assert text =~ "Health: unbound"
    assert text =~ "Phase: unbound"
    refute text =~ "parked"
  end

  # fails when: `info` shows the raw verdict while the loop is still inside its
  # grace, so it disagrees with `flz operator list` at the same moment.
  test "a held operator reads recovering while the loop's grace runs" do
    publish_held(0)

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "recovering"
  end

  test "a held operator reports unbound before the loop has judged it too" do
    stub_one(held_state())

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["phase"] == "unbound"
  end

  test "a healthy operator carries the loop's own verdict on both surfaces" do
    state = bounded_state(0)
    stub_one(state)

    health = judged(state)
    assert health[{:operator, {@namespace, "op-1"}}].state == :healthy

    Snapshot.publish(Map.put(Snapshot.current(), :operator_health, health))

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "healthy"
    assert proc["phase"] == "running"
  end

  test "health is unknown, never invented, before the loop has observed the operator" do
    stub_one(parked_state())

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => [proc]} = Jason.decode!(out)

    assert proc["health"] == "unknown"
    assert proc["phase"] == "parked"
  end

  test "a finished bounded operator is surfaced from its completion record" do
    # No live operators; the finished one comes only from its on-disk record.
    stub(OperatorManager, :get_operator_ids, fn _ns -> {:ok, []} end)

    :ok =
      Completion.write("done-1", %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "RES",
        finished_at: System.system_time(:second)
      })

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => procs} = Jason.decode!(out)
    proc = Enum.find(procs, &(&1["id"] == "done-1"))

    assert proc["kind"] == "bounded"
    assert proc["phase"] == "finished"
    assert proc["status"] == "ok"
    assert proc["result"] == Base.encode64("RES")
  end

  test "a finished operator outside the retention window is not surfaced" do
    stub(OperatorManager, :get_operator_ids, fn _ns -> {:ok, []} end)
    Application.put_env(:flezha, :bounded_completed_retention_hours, %{"done-old" => 0})

    :ok =
      Completion.write("done-old", %{
        namespace: @namespace,
        status: "ok",
        error: "",
        result: "",
        finished_at: System.system_time(:second) - 10
      })

    out = capture_io(fn -> CliManager.operator_info(@namespace, "json") end)
    %{"operators" => procs} = Jason.decode!(out)
    assert Enum.find(procs, &(&1["id"] == "done-old")) == nil
  end
end
