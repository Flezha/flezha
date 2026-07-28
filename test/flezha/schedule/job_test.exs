defmodule Flezha.Schedule.JobTest do
  use ExUnit.Case, async: false
  use Mimic

  import ExUnit.CaptureLog

  alias Flezha.Schedule.{Cron, Job}
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.ScheduleTestHelper, as: Helper

  # The job runs in its own process, so stubs must apply beyond the test pid.
  setup :set_mimic_global

  setup do
    Helper.isolated_store_setup()
  end

  defp cron!(expr) do
    {:ok, cron} = Cron.parse(expr)
    cron
  end

  defp warning_level do
    prev = Logger.level()
    Logger.configure(level: :warning)
    on_exit(fn -> Logger.configure(level: prev) end)
  end

  describe "catch_up_slot/4" do
    test "returns the most recent slot inside the grace window, not every one" do
      assert {:ok, ~U[2026-07-27 08:01:00Z]} =
               Job.catch_up_slot(
                 cron!("* * * * *"),
                 Helper.entry(),
                 ~U[2026-07-27 08:01:20Z],
                 120_000
               )
    end

    test "is floored at created_at so a fresh schedule never fires history" do
      entry = Helper.entry(%{created_at: "2026-07-27T08:01:10Z"})

      assert :none =
               Job.catch_up_slot(cron!("* * * * *"), entry, ~U[2026-07-27 08:01:20Z], 120_000)
    end

    test "a zero grace window opts out entirely" do
      assert :none =
               Job.catch_up_slot(cron!("* * * * *"), Helper.entry(), ~U[2026-07-27 08:01:20Z], 0)
    end
  end

  describe "arm_action/2" do
    test "a slot within the chunk fires directly" do
      now = ~U[2026-07-27 08:00:00Z]
      assert {:fire, 600_000} = Job.arm_action(~U[2026-07-27 08:10:00Z], now)
    end

    test "a slot beyond the send_after ceiling re-arms in chunks" do
      now = ~U[2026-07-27 08:00:00Z]
      assert {:rearm, 86_400_000} = Job.arm_action(~U[2027-02-12 08:00:00Z], now)
    end

    test "an already-past slot fires immediately" do
      now = ~U[2026-07-27 08:00:00Z]
      assert {:fire, 0} = Job.arm_action(~U[2026-07-27 07:59:00Z], now)
    end
  end

  describe "the job process" do
    test "arms a timer for the next slot" do
      pid = start_supervised!({Job, Helper.quiet_entry()})
      state = :sys.get_state(pid)

      assert state.target.month == 1
      assert state.target.day == 1
      assert is_reference(state.timer)
    end

    test "fires a slot inside the grace window on start, through the bounded path" do
      test_pid = self()

      expect(OperativeManager, :start_operative, fn "default", builder ->
        send(test_pid, {:spawned, builder})
        {:ok, %{operative_id: builder.id}}
      end)

      start_supervised!({Job, Helper.entry()})

      assert_receive {:spawned, builder}, 2_000
      assert builder.kind == :bounded
      assert builder.factory_id == "fac"
      assert builder.init_values == %{"a" => 1}
      assert builder.id =~ ~r/^job@\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z$/
    end

    test "a due slot fires and re-arms" do
      test_pid = self()

      stub(OperativeManager, :start_operative, fn "default", builder ->
        send(test_pid, {:spawned, builder.id})
        {:ok, %{operative_id: builder.id}}
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})

      assert_receive {:spawned, "job@2026-07-27T08:01:00Z"}, 2_000
      assert %{timer: timer} = :sys.get_state(pid)
      assert is_reference(timer)
    end

    test "an early wake re-arms instead of firing ahead of the slot" do
      test_pid = self()

      stub(OperativeManager, :start_operative, fn "default", builder ->
        send(test_pid, {:spawned, builder.id})
        {:ok, %{operative_id: builder.id}}
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      future = DateTime.add(DateTime.utc_now(), 3_600, :second)
      send(pid, {:fire, future})

      # Sync on the mailbox before asserting the absence of a fire.
      _ = :sys.get_state(pid)
      refute_received {:spawned, _}
    end

    test "a duplicate fire no-ops on the deterministic id and stays armed" do
      stub(OperativeManager, :start_operative, fn "default", builder ->
        {:error, %{operative_id: builder.id, node: node(), reason: "operative id already in use"}}
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})

      assert %{timer: timer} = :sys.get_state(pid)
      assert is_reference(timer)
      assert Process.alive?(pid)
    end

    test "a failed spawn is logged and not retried" do
      stub(OperativeManager, :start_operative, fn "default", _builder ->
        {:error, %{operative_id: "x", node: node(), reason: "Factory with id fac does not exist"}}
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})

      log =
        capture_log(fn ->
          send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})
          _ = :sys.get_state(pid)
        end)

      assert log =~ "spawn of job@2026-07-27T08:01:00Z failed"
      assert Process.alive?(pid)
    end

    test "an unparsable cron goes dormant instead of crash-looping" do
      warning_level()

      log =
        capture_log(fn ->
          pid = start_supervised!({Job, Helper.entry(%{cron: "not a cron"})})
          assert %{timer: nil, target: nil} = :sys.get_state(pid)
          assert Process.alive?(pid)
        end)

      assert log =~ "job is dormant"
    end

    test "a cron with no future slot goes dormant" do
      warning_level()

      log =
        capture_log(fn ->
          pid = start_supervised!({Job, Helper.entry(%{cron: "0 0 30 2 *"})})
          assert %{timer: nil} = :sys.get_state(pid)
          assert Process.alive?(pid)
        end)

      assert log =~ "job is dormant"
    end
  end
end
