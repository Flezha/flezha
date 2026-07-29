defmodule Flezha.Service.CliManagerScheduleTest do
  # Error verdicts `System.halt/1`, so only success paths are asserted
  # in-process (the same posture as `cli_manager_send_test`); validation
  # failures are covered store-side in `Flezha.Schedule.StoreTest`.
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureIO

  alias Flezha.Runner.FleetManager
  alias Flezha.Service.CliManager
  alias Flezha.Service.OperativeManager

  setup do
    FlezhaTest.Helper.ScheduleTestHelper.isolated_store_setup()
  end

  test "schedule_add stores the entry and previews the next fire" do
    stub(FleetManager, :runners_for_factory, fn "default", "ai_digest" -> ["runner-1"] end)

    output =
      capture_io(fn ->
        CliManager.schedule_add("digest", "0 8 * * *", "Europe/Tallinn", "ai_digest")
      end)

    assert output =~ "Schedule digest added"
    assert output =~ "next fire 20"
    refute output =~ "warning"

    assert {:ok, entry} = Flezha.Schedule.Store.get("digest")
    assert entry.namespace == "default"
    assert entry.init_values == %{}
  end

  test "schedule_add warns when the factory has no live runner" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> [] end)

    output =
      capture_io(fn ->
        CliManager.schedule_add(
          "digest",
          "0 8 * * *",
          "Europe/Tallinn",
          "ai_digest",
          "prod",
          ~s({"mode": "daily"}),
          "bounded"
        )
      end)

    assert output =~ "warning: factory ai_digest has no live runner in namespace prod"
    assert {:ok, %{init_values: %{"mode" => "daily"}}} = Flezha.Schedule.Store.get("digest")
  end

  test "schedule_list renders entries with the next fire time" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)
    capture_io(fn -> CliManager.schedule_add("digest", "0 8 * * *", "Etc/UTC", "ai_digest") end)

    output = capture_io(fn -> CliManager.schedule_list() end)

    assert output =~ "NAME"
    assert output =~ "NEXT FIRE (UTC)"
    assert output =~ "digest"
    assert output =~ "0 8 * * *"
    assert output =~ "T08:00:00Z"
  end

  test "schedule_list shows the node running each schedule" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)
    stub(Flezha.Schedule.Hub, :placement, fn -> %{"digest" => :flz@testhost} end)
    capture_io(fn -> CliManager.schedule_add("digest", "0 8 * * *", "Etc/UTC", "ai_digest") end)

    output = capture_io(fn -> CliManager.schedule_list() end)

    assert output =~ "NODE"
    assert output =~ "flz@testhost"
  end

  test "schedule_list renders a dash when the schedule is unplaced" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)
    stub(Flezha.Schedule.Hub, :placement, fn -> %{} end)
    capture_io(fn -> CliManager.schedule_add("digest", "0 8 * * *", "Etc/UTC", "ai_digest") end)

    output = capture_io(fn -> CliManager.schedule_list() end)

    # The NODE cell is padded to 22, so match the cell — a bare "-" would also
    # match the ISO date in NEXT FIRE.
    assert output =~ "-" <> String.duplicate(" ", 21)
  end

  test "schedule_list with no entries" do
    assert capture_io(fn -> CliManager.schedule_list() end) =~ "no schedules"
  end

  test "schedule_run spawns immediately with a manual id" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)
    capture_io(fn -> CliManager.schedule_add("digest", "0 8 * * *", "Etc/UTC", "ai_digest") end)

    expect(OperativeManager, :start_bounded, fn "default", id, factory_id, _init, _opts ->
      assert id =~ ~r/^digest@manual-\d+$/
      assert factory_id == "ai_digest"
      {:ok, %{operative_id: id}}
    end)

    output = capture_io(fn -> CliManager.schedule_run("digest") end)
    assert output =~ "Spawned digest@manual-"
  end

  # Every case above runs with no `:flz_schedules` hub, so these also pin that
  # the CLI stays usable when the hub is down.
  test "schedule_remove deletes the entry" do
    stub(FleetManager, :runners_for_factory, fn _, _ -> ["runner-1"] end)
    capture_io(fn -> CliManager.schedule_add("digest", "0 8 * * *", "Etc/UTC", "ai_digest") end)

    assert capture_io(fn -> CliManager.schedule_remove("digest") end) =~
             "Schedule digest removed"

    assert Flezha.Schedule.Store.list() == []
  end
end
