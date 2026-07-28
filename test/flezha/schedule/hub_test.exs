defmodule Flezha.Schedule.HubTest do
  use ExUnit.Case, async: false
  use Mimic

  @moduletag :capture_log

  alias Flezha.Schedule.{Hub, Job, Store}
  alias Flezha.Service.HubManager
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.ScheduleTestHelper, as: Helper

  setup :set_mimic_global

  setup do
    Helper.isolated_store_setup()

    # Belt and braces: no entry here is due, but a stray fire must not raise
    # inside a job process.
    stub(OperativeManager, :start_operative, fn _ns, builder ->
      {:ok, %{operative_id: builder.id}}
    end)

    :ok
  end

  # Far-future so no job fires mid-test.
  defp add(name) do
    {:ok, entry} = Store.add(Helper.entry_fields(%{name: name, cron: "0 0 1 1 *"}))
    entry
  end

  describe "with the hub running" do
    setup do
      Helper.schedule_hub_setup()
    end

    test "assert/1 starts exactly one job for the schedule" do
      entry = add("digest")
      :ok = Hub.assert(entry)

      assert [pid] = Helper.await_registered("digest")
      assert Process.alive?(pid)
    end

    test "a duplicate start is refused by the hub — this is the arbitration" do
      entry = add("digest")
      :ok = Hub.assert(entry)
      Helper.await_registered("digest")

      # Pins the dep contract the no-op path in `Hub.assert/1` reads.
      assert {:error, {:already_started, ["digest"]}} =
               ProcessHub.start_child(HubManager.schedule_hub_id(), Job.child_spec(entry))
    end

    test "asserting twice leaves the same single job" do
      entry = add("digest")
      :ok = Hub.assert(entry)
      [pid] = Helper.await_registered("digest")

      :ok = Hub.assert(entry)
      assert [^pid] = Helper.await_registered("digest")
    end

    test "assert_all/0 registers every stored definition" do
      add("digest")
      add("rollup")

      :ok = Hub.assert_all()

      assert [_] = Helper.await_registered("digest")
      assert [_] = Helper.await_registered("rollup")
    end

    test "retract/1 stops the job and clears the registry row" do
      entry = add("digest")
      :ok = Hub.assert(entry)
      Helper.await_registered("digest")

      :ok = Hub.retract("digest")

      assert [] = Helper.await_unregistered("digest")
    end

    test "placement/0 reports the node running each schedule" do
      entry = add("digest")
      :ok = Hub.assert(entry)
      Helper.await_registered("digest")

      assert %{"digest" => node} = Hub.placement()
      assert node == node()
    end

    test "the schedule hub stays invisible to the operative-oriented walkers" do
      refute HubManager.schedule_hub_id() in HubManager.list_running_hub_ids()
      refute "flz_schedules" in HubManager.get_running_namespaces()
    end

    test "the schedule hub writes nothing under the process-hub registry root" do
      add("digest") |> Hub.assert()
      Helper.await_registered("digest")

      root = Application.get_env(:flezha, :process_hub_registry_root, "app_data/process_hub")

      refute File.exists?(Path.join(root, Atom.to_string(HubManager.schedule_hub_id())))
    end
  end

  describe "with the hub down" do
    test "assert/1 and retract/1 no-op" do
      entry = add("digest")

      assert :ok = Hub.assert(entry)
      assert :ok = Hub.retract("digest")
    end

    test "placement/0 is empty" do
      assert Hub.placement() == %{}
    end
  end
end
