defmodule Flezha.Service.CliManagerScheduleTest do
  # Error verdicts `System.halt/1`, so only success paths are asserted
  # in-process (the same posture as `cli_manager_send_test`); validation
  # failures are covered store-side in `Flezha.Schedule.StoreTest`.
  # async: false: Mimic stubs; a shared on-disk root.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Runner.FleetManager
  alias Flezha.Schedule.Store, as: ScheduleStore
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperatorManager
  alias FlezhaTest.Helper.ScheduleTestHelper

  setup do
    ScheduleTestHelper.isolated_store_setup()
  end

  # `flz schedule add digest …` with `extra` overriding the defaults; answers
  # what the verb printed.
  defp add_digest(extra \\ []) do
    flags = Keyword.merge([cron: "0 8 * * *", tz: "Etc/UTC", factory: "ai_digest"], extra)
    capture_io(fn -> CliManager.schedule_add("digest", flags) end)
  end

  test "schedule_add stores the entry and previews the next fire" do
    stub(FleetManager, :runners_for_factory, fn "default", "ai_digest" -> ["runner-1"] end)

    output = add_digest(tz: "Europe/Tallinn")

    assert output =~ "Schedule digest added"
    assert output =~ "next fire 20"
    refute output =~ "warning"

    assert {:ok, entry} = ScheduleStore.get("digest")
    assert entry.namespace == "default"
    assert entry.init_values == %{}
  end

  test "schedule_add warns when the factory has no live runner" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> [] end)

    output =
      add_digest(
        tz: "Europe/Tallinn",
        namespace: "prod",
        init_values: ~s({"mode": "daily"})
      )

    assert output =~ "warning: factory ai_digest has no live runner in namespace prod"
    assert {:ok, %{init_values: %{"mode" => "daily"}}} = ScheduleStore.get("digest")
  end

  test "schedule_add refuses a factory its runner announced as unbounded" do
    stub(FleetManager, :factory_kind, fn "default", "prices" -> {:ok, :unbounded} end)

    err =
      capture_io(:stderr, fn ->
        assert catch_exit(add_digest(factory: "prices")) == {:shutdown, 2}
      end)

    assert err =~ "factory 'prices' is an unbounded operator"
    assert err =~ "(operator.factory_not_bounded)"
    assert ScheduleStore.get("digest") == :error
  end

  test "schedule_list renders entries with the next fire time" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    add_digest()

    output = capture_io(fn -> CliManager.schedule_list() end)

    assert output =~ "NAME"
    assert output =~ "NEXT FIRE (UTC)"
    assert output =~ "digest"
    assert output =~ "0 8 * * *"
    assert output =~ "T08:00:00Z"
  end

  test "schedule_list shows the node running each schedule" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    stub(Flezha.Schedule.Hub, :statuses, fn ->
      %{"digest" => %{node: :flz@testhost, last_fire: nil}}
    end)

    add_digest()

    output = capture_io(fn -> CliManager.schedule_list() end)

    assert output =~ "NODE"
    assert output =~ "flz@testhost"
  end

  test "schedule_list renders a dash when the schedule is unplaced" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)
    stub(Flezha.Schedule.Hub, :statuses, fn -> %{} end)

    add_digest()

    output = capture_io(fn -> CliManager.schedule_list() end)

    # The NODE column is padded to 22, so match the column — a bare "-" would also
    # match the ISO date in NEXT FIRE.
    assert output =~ "-" <> String.duplicate(" ", 21)
  end

  test "schedule_list renders the last fire and its result" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    stub(Flezha.Schedule.Hub, :statuses, fn ->
      %{
        "digest" => %{
          node: :flz@testhost,
          last_fire: %{
            instance_id: "digest@2026-08-09T08:00:00Z",
            outcome: :ok,
            at: ~U[2026-08-09 08:00:03.123456Z]
          }
        }
      }
    end)

    add_digest()

    output = capture_io(fn -> CliManager.schedule_list() end)

    assert output =~ "LAST FIRE (UTC)"
    assert output =~ "LAST RESULT"
    assert output =~ "2026-08-09T08:00:03Z"
    assert output =~ "ok"
  end

  test "schedule_list truncates a retrying result to its column" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    reason = "no runners for factory ai_digest in namespace default on any cluster node"

    stub(Flezha.Schedule.Hub, :statuses, fn ->
      %{
        "digest" => %{
          node: :flz@testhost,
          last_fire: %{
            instance_id: "digest@2026-08-09T08:00:00Z",
            outcome: {:retrying, reason},
            at: ~U[2026-08-09 08:00:03Z]
          }
        }
      }
    end)

    add_digest()

    output = capture_io(fn -> CliManager.schedule_list() end)

    assert output =~ "retrying: no runners for factory"
    # Truncated to the column; the full reason stays in log/telemetry.
    refute output =~ "any cluster node"
  end

  test "schedule_list with no entries" do
    assert capture_io(fn -> CliManager.schedule_list() end) =~ "no schedules"
  end

  # `--plugins` reaches the controller as the text the caller wrote, and the
  # stored row holds it verbatim: providers are resolved again at every firing.
  test "schedule_add stores the plugin spec as the text it was given" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    spec =
      ~s([{"id":"out","provider":"operator_stdout","opts":{},"hooks":["on_output"]}])

    output = add_digest(plugins: spec)

    assert output =~ "Schedule digest added"
    assert {:ok, %{plugins: ^spec}} = ScheduleStore.get("digest")
  end

  test "schedule_run spawns immediately with a manual id" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    add_digest()

    expect(OperatorManager, :start_bounded, fn "default", id, factory_id, _init, _opts ->
      assert id =~ ~r/^digest@manual-\d+$/
      assert factory_id == "ai_digest"
      {:ok, %{operator_id: id}}
    end)

    output = capture_io(fn -> CliManager.schedule_run("digest") end)
    assert output =~ "Spawned digest@manual-"
  end

  # Every case above runs with no `:flz_schedules` hub, so these also pin that
  # the CLI stays usable when the hub is down.
  test "schedule_remove deletes the entry" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)

    add_digest()

    assert capture_io(fn -> CliManager.schedule_remove("digest") end) =~
             "Schedule digest removed"

    assert ScheduleStore.list() == []
  end
end
