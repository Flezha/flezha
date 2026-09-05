defmodule Flezha.Schedule.JobTest do
  # async: false: Mimic global mode; Mimic stubs; a shared on-disk root.
  use ExUnit.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  import ExUnit.CaptureLog

  alias Flezha.Schedule.{Cron, Job}
  alias Flezha.Service.OperativeManager
  alias FlezhaTest.Helper.ScheduleTestHelper, as: Helper

  # The job runs in its own process, so stubs must apply beyond the test pid.
  setup :set_mimic_global_until_exit

  setup do
    Helper.isolated_store_setup()
  end

  defp cron!(expr) do
    {:ok, cron} = Cron.parse(expr)
    cron
  end

  # Forward every spawn's arguments to the test process, so no call site needs
  # its own stub shape.
  defp stub_spawn(test_pid) do
    stub(OperativeManager, :start_workflow, fn "default", id, factory_id, init_values, opts ->
      send(test_pid, {:spawned, spawn_args(id, factory_id, init_values, opts)})
      {:ok, %{operative_id: id}}
    end)
  end

  defp spawn_args(id, factory_id, init_values, opts) do
    %{id: id, factory_id: factory_id, init_values: init_values, opts: opts}
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

    test "fires a slot inside the grace window on start, through the workflow path" do
      test_pid = self()

      expect(OperativeManager, :start_workflow, fn "default", id, factory_id, init_values, opts ->
        send(test_pid, {:spawned, spawn_args(id, factory_id, init_values, opts)})
        {:ok, %{operative_id: id}}
      end)

      start_supervised!({Job, Helper.entry()})

      assert_receive {:spawned, builder}, 2_000
      assert builder.factory_id == "fac"
      assert builder.init_values == %{"a" => 1}
      assert builder.id =~ ~r/^job@\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:00Z$/
    end

    test "a due slot fires and re-arms" do
      test_pid = self()

      stub_spawn(test_pid)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})

      assert_receive {:spawned, %{id: "job@2026-07-27T08:01:00Z"}}, 2_000
      assert %{timer: timer} = :sys.get_state(pid)
      assert is_reference(timer)
    end

    test "an early wake re-arms instead of firing ahead of the slot" do
      test_pid = self()

      stub_spawn(test_pid)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      future = DateTime.add(DateTime.utc_now(), 3_600, :second)
      send(pid, {:fire, future})

      # Sync on the mailbox before asserting the absence of a fire.
      _ = :sys.get_state(pid)
      refute_received {:spawned, _}
    end

    test "a duplicate fire no-ops on the deterministic id and stays armed" do
      stub(OperativeManager, :start_workflow, fn "default", _id, _factory, _init, _opts ->
        :duplicate
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})

      assert %{timer: timer} = :sys.get_state(pid)
      assert is_reference(timer)
      assert Process.alive?(pid)
    end

    test "a failed spawn is logged with its attempt number" do
      stub(OperativeManager, :start_workflow, fn "default", _id, _factory, _init, _opts ->
        {:error, %{operative_id: "x", node: node(), reason: "Factory with id fac does not exist"}}
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})

      log =
        capture_log(fn ->
          send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})
          _ = :sys.get_state(pid)
        end)

      assert log =~ "job@2026-07-27T08:01:00Z not started (attempt 1): spawn failed"
      assert Process.alive?(pid)
    end

    test "the entry's plugin spec is attached at spawn with the slot's identity" do
      test_pid = self()

      stub_spawn(test_pid)

      spec =
        ~s([{"id":"reports","provider":"operative_kafka_producer",) <>
          ~s("subscriptions":["on_output"],"opts":{"topic":"ai.reports.v1"}}])

      pid = start_supervised!({Job, Helper.quiet_entry(%{plugins: spec})})
      send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})

      assert_receive {:spawned, builder}, 2_000
      assert [plugin] = builder.opts[:plugins]
      # Prefixed by the instance id so two overlapping slots never collide.
      assert plugin.id == "job@2026-07-27T08:01:00Z_reports"
      assert plugin.operative_id == "job@2026-07-27T08:01:00Z"
      assert plugin.namespace == "default"
      assert plugin.provider == Flezha.Plugin.Provider.Kafka.Producer
      # The decoded form is the boundary shape; `Extension.Table.canonicalise/2`
      # resolves the name against the host catalog at subscribe time.
      assert plugin.subscriptions == [{"on_output", []}]
      assert plugin.opts == %{topic: "ai.reports.v1"}
    end

    test "an entry with no plugin spec spawns with no plugins opt" do
      test_pid = self()

      stub_spawn(test_pid)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})

      assert_receive {:spawned, builder}, 2_000
      assert builder.opts == []
    end

    test "a spec that no longer decodes starts nothing rather than a mute run" do
      stub(OperativeManager, :start_workflow, fn _ns, _id, _factory, _init, _opts ->
        flunk("an undecodable plugin spec must not reach the start path")
      end)

      spec = ~s([{"id":"reports","provider":"retired_provider","subscriptions":["on_output"]}])
      pid = start_supervised!({Job, Helper.quiet_entry(%{plugins: spec})})

      log =
        capture_log(fn ->
          send(pid, {:fire, ~U[2026-07-27 08:01:00Z]})
          _ = :sys.get_state(pid)
        end)

      assert log =~ "plugin spec did not decode"
      assert log =~ "retired_provider"
      assert Process.alive?(pid)
    end

    test "a failed spawn arms a retry with the slot id and reports retrying" do
      stub(OperativeManager, :start_workflow, fn "default", _id, _factory, _init, _opts ->
        {:error, %{reason: "no runners"}}
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      slot = ~U[2026-07-27 08:01:00Z]

      log =
        capture_log(fn ->
          send(pid, {:fire, slot})
          _ = :sys.get_state(pid)
        end)

      assert log =~ "not started (attempt 1)"

      state = :sys.get_state(pid)
      assert %{slot: ^slot, attempt: 2, timer: timer} = state.pending_retry
      # First rung of the ladder: the 1s base.
      remaining = Process.read_timer(timer)
      assert is_integer(remaining) and remaining <= 1_000

      assert %{last_fire: %{instance_id: "job@2026-07-27T08:01:00Z", outcome: {:retrying, _}}} =
               GenServer.call(pid, :status)
    end

    test "a retry re-uses the slot id; success clears the ladder" do
      test_pid = self()
      {:ok, verdicts} = Agent.start_link(fn -> :fail end)

      stub(OperativeManager, :start_workflow, fn "default", id, _factory, _init, _opts ->
        case Agent.get(verdicts, & &1) do
          :fail ->
            {:error, %{reason: "no runners"}}

          verdict ->
            send(test_pid, {:spawned, id})
            if verdict == :duplicate, do: :duplicate, else: {:ok, %{operative_id: id}}
        end
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      slot = ~U[2026-07-27 08:01:00Z]

      capture_log(fn ->
        send(pid, {:fire, slot})
        _ = :sys.get_state(pid)
      end)

      Agent.update(verdicts, fn _ -> :ok end)
      send(pid, {:retry, slot, 2})

      assert_receive {:spawned, "job@2026-07-27T08:01:00Z"}, 2_000
      state = :sys.get_state(pid)
      assert state.pending_retry == nil
      assert state.last_fire.outcome == :ok
    end

    test "a duplicate answer on retry counts as success — someone fired the slot" do
      {:ok, verdicts} = Agent.start_link(fn -> :fail end)

      stub(OperativeManager, :start_workflow, fn "default", _id, _factory, _init, _opts ->
        case Agent.get(verdicts, & &1) do
          :fail -> {:error, %{reason: "no runners"}}
          :duplicate -> :duplicate
        end
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})
      slot = ~U[2026-07-27 08:01:00Z]

      capture_log(fn ->
        send(pid, {:fire, slot})
        _ = :sys.get_state(pid)
      end)

      Agent.update(verdicts, fn _ -> :duplicate end)
      send(pid, {:retry, slot, 2})

      state = :sys.get_state(pid)
      assert state.pending_retry == nil
      assert state.last_fire.outcome == :duplicate
    end

    test "the next slot supersedes a pending retry — no back-fill, stale retries ignored" do
      test_pid = self()
      slot1 = ~U[2026-07-27 08:01:00Z]
      slot2 = ~U[2026-07-27 08:02:00Z]
      failing_id = "job@#{DateTime.to_iso8601(slot1)}"

      stub(OperativeManager, :start_workflow, fn "default", id, _factory, _init, _opts ->
        if id == failing_id do
          {:error, %{reason: "no runners"}}
        else
          send(test_pid, {:spawned, id})
          {:ok, %{operative_id: id}}
        end
      end)

      pid = start_supervised!({Job, Helper.quiet_entry()})

      capture_log(fn ->
        send(pid, {:fire, slot1})
        _ = :sys.get_state(pid)
      end)

      assert %{pending_retry: %{slot: ^slot1}} = :sys.get_state(pid)

      send(pid, {:fire, slot2})
      assert_receive {:spawned, "job@2026-07-27T08:02:00Z"}, 2_000

      state = :sys.get_state(pid)
      assert state.pending_retry == nil

      # The abandoned slot's retry message (already in flight) must no-op.
      send(pid, {:retry, slot1, 2})
      _ = :sys.get_state(pid)
      refute_received {:spawned, ^failing_id}
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
