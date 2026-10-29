defmodule Flezha.Schedule.HubTest do
  # async: false: Mimic global mode; Mimic stubs; a shared on-disk root.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  @moduletag :capture_log

  alias Flezha.Schedule.{Hub, Job}
  alias Flezha.Service.HubManager
  alias Flezha.Service.RelayManager
  alias FlezhaTest.Helper.ScheduleTestHelper, as: Helper

  setup :set_mimic_global_until_exit

  setup do
    Helper.isolated_store_setup()

    # Belt and braces: no entry here is due, but a stray fire must not raise
    # inside a job process.
    stub(RelayManager, :start_steps, fn _ns, id, _factory, _init, _opts, _start_opts ->
      {:ok, %{relay_id: id}}
    end)

    :ok
  end

  describe "with the hub running" do
    setup do
      Helper.schedule_hub_setup()
    end

    test "assert/1 starts exactly one job for the schedule" do
      entry = Helper.add_quiet("digest")
      :ok = Hub.assert(entry)

      assert [pid] = Helper.await_registered("digest")
      assert Process.alive?(pid)
    end

    test "a duplicate start is refused by the hub — this is the arbitration" do
      entry = Helper.add_quiet("digest")
      :ok = Hub.assert(entry)
      Helper.await_registered("digest")

      # Pins the dep contract the no-op path in `Hub.assert/1` reads.
      assert {:error, {:already_started, ["digest"]}} =
               ProcessHub.start_child(HubManager.schedule_hub_id(), Job.child_spec(entry))
    end

    test "asserting twice leaves the same single job" do
      entry = Helper.add_quiet("digest")
      :ok = Hub.assert(entry)
      [pid] = Helper.await_registered("digest")

      :ok = Hub.assert(entry)
      assert [^pid] = Helper.await_registered("digest")
    end

    test "assert_all/0 registers every stored definition" do
      Helper.add_quiet("digest")
      Helper.add_quiet("rollup")

      :ok = Hub.assert_all()

      assert [_] = Helper.await_registered("digest")
      assert [_] = Helper.await_registered("rollup")
    end

    test "retract/1 stops the job and clears the registry row" do
      entry = Helper.add_quiet("digest")
      :ok = Hub.assert(entry)
      Helper.await_registered("digest")

      :ok = Hub.retract("digest")

      assert [] = Helper.await_unregistered("digest")
    end

    test "statuses/0 reports the node and last_fire of each schedule" do
      entry = Helper.add_quiet("digest")
      :ok = Hub.assert(entry)
      [pid | _] = Helper.await_registered("digest")

      # A quiet entry has not fired yet; its next fire is the slot it is armed for.
      assert %{"digest" => %{node: node, last_fire: nil, dormant?: false, next_fire: next}} =
               Hub.statuses()

      assert node == node()
      assert next == :sys.get_state(pid).target

      # A wedged job reports its node only — the walk must render regardless.
      :ok = :sys.suspend(pid)
      assert %{"digest" => %{node: ^node, last_fire: nil}} = Hub.statuses()
      :ok = :sys.resume(pid)
    end

    test "the schedule hub stays invisible to the relay-oriented walkers" do
      refute HubManager.schedule_hub_id() in HubManager.list_running_hub_ids()
      refute "flz_schedules" in HubManager.get_running_namespaces()
    end

    test "the schedule hub writes nothing under the process-hub registry root" do
      Helper.add_quiet("digest") |> Hub.assert()
      Helper.await_registered("digest")

      root = Application.get_env(:flezha, :process_hub_registry_root, "storage/app/process_hub")

      refute File.exists?(Path.join(root, Atom.to_string(HubManager.schedule_hub_id())))
    end
  end

  describe "with the hub down" do
    test "assert/1 and retract/1 no-op" do
      entry = Helper.add_quiet("digest")

      assert :ok = Hub.assert(entry)
      assert :ok = Hub.retract("digest")
    end

    test "statuses/0 is empty" do
      assert Hub.statuses() == %{}
    end
  end
end
