defmodule FlezhaTest.Runner.StreamChannelTest do
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.StreamChannel
  alias Flezha.State.Paths
  alias Flezha.State.Wire
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: writes Application env; a shared on-disk root; registered runner channels.

  use FlezhaTest.Case, async: false

  setup context do
    name =
      "stream_channel_test_#{context.test}"
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    {:ok, pid} = StreamChannel.start_link({"ce_test", name, :primary})

    on_exit(fn ->
      NodeTestHelper.ensure_stopped(pid)
    end)

    %{pid: pid, name: name}
  end

  describe "stream closed → output_stall telemetry" do
    test "{:flz_closed} emits output_stall telemetry for every operative in operative_mapping",
         %{pid: pid} do
      :ok = GenServer.call(pid, {:operative_pid_update, "operative_a", [self()]})
      :ok = GenServer.call(pid, {:operative_pid_update, "operative_b", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      test_pid = self()
      handler_id = "stall_test_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:flezha, :state, :output_stall],
        stall_forwarder(test_pid),
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      send(pid, {:flz_closed, :ref_x, :eof})
      # flush through mailbox
      _ = GenServer.call(pid, :stream_ref_status)

      assert_receive {:output_stall, "operative_a"}
      assert_receive {:output_stall, "operative_b"}
    end

    test "{:flz_closed} with empty operative_mapping emits no output_stall telemetry",
         %{pid: pid} do
      test_pid = self()
      handler_id = "stall_test_empty_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:flezha, :state, :output_stall],
        stall_forwarder(test_pid),
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      :ok = GenServer.call(pid, {:bind, :ref_y})
      send(pid, {:flz_closed, :ref_y, :eof})
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:output_stall, _}
    end
  end

  describe "rebind contract" do
    test "fresh channel has nil stream_ref", %{pid: pid} do
      assert GenServer.call(pid, :stream_ref_status) == nil
    end

    test "{:flz_closed, ...} clears stream_ref synchronously", %{pid: pid} do
      :ok = GenServer.call(pid, {:bind, :ref_a})
      assert GenServer.call(pid, :stream_ref_status) == :ref_a

      send(pid, {:flz_closed, :ref_a, :eof})
      assert GenServer.call(pid, :stream_ref_status) == nil
    end

    test "{:bind, ref} on nil-ref returns :ok and updates state", %{pid: pid} do
      assert :ok = GenServer.call(pid, {:bind, :ref_new})
      assert GenServer.call(pid, :stream_ref_status) == :ref_new
    end

    test "{:bind, ref} on live-ref returns :already_bound and preserves state", %{pid: pid} do
      :ok = GenServer.call(pid, {:bind, :ref_live})
      assert :already_bound = GenServer.call(pid, {:bind, :ref_other})
      assert GenServer.call(pid, :stream_ref_status) == :ref_live
    end

    test "io_in/3 on nil-ref returns {:error, :no_stream}", %{pid: pid, name: name} do
      # The :no_stream error proves the guard fired before any NIF call —
      # there's no other code path that can reach this clause.
      assert {:error, :no_stream} = StreamChannel.io_in(name, "p1", {"msg", %{}})
      assert GenServer.call(pid, :stream_ref_status) == nil
    end

    test "io_in/3 on an unknown channel is no_stream, never a crash" do
      assert {:error, :no_stream} = StreamChannel.io_in(:no_such_channel, "p1", {"msg", %{}})
      assert {:error, :no_stream} = StreamChannel.io_in(nil, "p1", {"msg", %{}})
    end

    test "io_in/3 on nil-ref counts the drop (6.3) instead of silently dropping",
         %{pid: pid, name: name} do
      test_pid = self()
      id = "stream-channel-drop-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        id,
        [:flezha, :stream, :drop],
        fn _e, m, md, _ -> send(test_pid, {:drop, m, md}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(id) end)

      {:error, :no_stream} = StreamChannel.io_in(name, "p1", {"msg", %{}})
      assert_receive {:drop, %{count: 1}, %{operative_id: "p1"}}
      assert GenServer.call(pid, :stream_ref_status) == nil
    end
  end

  describe "heartbeat liveness → output_stall telemetry" do
    @threshold_ms 25

    setup %{name: name} do
      FlezhaTest.Case.install([:env])

      # Small threshold for a fast test; interval 0 disables the auto-timer so the
      # only liveness ticks are the ones the test drives explicitly.
      Application.put_env(:flezha, :io_stream_liveness_threshold_ms, @threshold_ms)
      Application.put_env(:flezha, :io_stream_liveness_interval_ms, 0)

      test_pid = self()
      id = "io_stale_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        id,
        [:flezha, :state, :output_stall],
        stall_forwarder(test_pid),
        nil
      )

      on_exit(fn -> :telemetry.detach(id) end)

      # Heartbeats only arrive on secondary streams; the check is gated to them.
      sec_name = :"#{name}_secondary"
      {:ok, pid} = StreamChannel.start_link({"ce_test", sec_name, :secondary})
      on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)

      %{pid: pid, name: sec_name}
    end

    test "idle operative that keeps heart-beating is never flagged", %{pid: pid} do
      :ok = GenServer.call(pid, {:operative_pid_update, "operative_a", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      # No real output, but a fresh heartbeat lands right before the check.
      age_stream(pid, @threshold_ms * 3)
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{}})
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:output_stall, _}
    end

    test "operative whose heartbeats stop is flagged for every operative", %{pid: pid} do
      :ok = GenServer.call(pid, {:operative_pid_update, "operative_a", [self()]})
      :ok = GenServer.call(pid, {:operative_pid_update, "operative_b", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      # Last heartbeat, then silence past the threshold.
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{}})
      age_stream(pid, @threshold_ms * 3)
      send(pid, :io_liveness_check)

      assert_receive {:output_stall, "operative_a"}
      assert_receive {:output_stall, "operative_b"}
    end

    test "unbound stream is never flagged", %{pid: pid} do
      :ok = GenServer.call(pid, {:operative_pid_update, "operative_a", [self()]})

      age_stream(pid, @threshold_ms * 3)
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:output_stall, _}
    end

    test "primary (send-only) stream is never flagged even when bound and silent" do
      name = :"sc_primary_#{System.unique_integer([:positive])}"
      {:ok, pid} = StreamChannel.start_link({"ce_test", name, :primary})
      on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)

      :ok = GenServer.call(pid, {:operative_pid_update, "operative_a", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      age_stream(pid, @threshold_ms * 3)
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:output_stall, _}
    end

    test "heartbeat metadata records the runner's dispatch self-report", %{
      pid: pid,
      name: name
    } do
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{"dispatches" => "42"}})
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{"dispatches" => "57"}})
      _ = GenServer.call(pid, :stream_ref_status)

      row =
        LivenessCounters.snapshot_all().channels
        |> Enum.find(fn c -> c.runner_id == "ce_test" and c.name == name end)

      assert row.hb_count == 2
      assert row.hb_dispatches == 57
      assert row.last_heartbeat_ms != 0, "no heartbeat stamped when it landed"
    end

    test "heartbeat metadata records the runner's handout self-report", %{pid: pid, name: name} do
      send(
        pid,
        {:flz_data, :ref_x, "data_heartbeat", "",
         %{"dispatches" => "42", "inflight" => "3", "inflight_oldest_ms" => "612345"}}
      )

      _ = GenServer.call(pid, :stream_ref_status)
      row = channel_row(name)

      assert row.hb_inflight == 3
      assert row.hb_inflight_oldest_ms == 612_345
    end

    # An SDK that has not shipped the self-report sends neither key. Absent
    # must read as zero — never as a stalled entity.
    test "a heartbeat without the handout keys reads as zeros", %{pid: pid, name: name} do
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{"dispatches" => "42"}})
      _ = GenServer.call(pid, :stream_ref_status)
      row = channel_row(name)

      assert row.hb_inflight == 0
      assert row.hb_inflight_oldest_ms == 0
    end

    # Every key the runner's heartbeat carries lands in a slot. The cross-SDK
    # gate proves the two key sets name the same things; this proves the value
    # actually arrives in the slot the mirror derives.
    test "every reported key mirrors into its own slot", %{
      pid: pid,
      name: name
    } do
      send(
        pid,
        {:flz_data, :ref_x, "data_heartbeat", "",
         %{
           "dispatches" => "1",
           "credit_parked" => "2",
           "blocked_parks" => "3",
           "inflight" => "4",
           "inflight_oldest_ms" => "5",
           "drops_newest" => "6",
           "drops_oldest" => "7",
           "pending_pushes" => "8",
           "pending_push_oldest_ms" => "9",
           "restore_discarded_inputs" => "10",
           "timers_armed" => "11",
           "timers_fired" => "12"
         }}
      )

      _ = GenServer.call(pid, :stream_ref_status)
      row = channel_row(name)

      for {slot, value} <- [
            hb_dispatches: 1,
            hb_credit_parked: 2,
            hb_blocked_parks: 3,
            hb_inflight: 4,
            hb_inflight_oldest_ms: 5,
            hb_drops_newest: 6,
            hb_drops_oldest: 7,
            hb_pending_pushes: 8,
            hb_pending_push_oldest_ms: 9,
            hb_restore_discarded_inputs: 10,
            hb_timers_armed: 11,
            hb_timers_fired: 12
          ] do
        assert Map.fetch(row, slot) == {:ok, value}, "#{slot} did not mirror"
      end
    end

    defp channel_row(name) do
      LivenessCounters.snapshot_all().channels
      |> Enum.find(fn c -> c.runner_id == "ce_test" and c.name == name end)
    end
  end

  describe "output_stall / delete gating (kind-aware)" do
    setup do
      FlezhaTest.Case.install([:tmp_app_data])
      ClassRegistry.ensure_table()

      :ok
    end

    test "a workflow operative is not flagged for output_stall on stream close", %{pid: pid} do
      ClassRegistry.put("bnd_op", :workflow)
      on_exit(fn -> ClassRegistry.delete("bnd_op") end)

      :ok = GenServer.call(pid, {:operative_pid_update, "bnd_op", [self()]})
      :ok = GenServer.call(pid, {:operative_pid_update, "cont_op", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_g})

      test_pid = self()
      hid = "stall_gate_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        hid,
        [:flezha, :state, :output_stall],
        stall_forwarder(test_pid),
        nil
      )

      on_exit(fn -> :telemetry.detach(hid) end)

      send(pid, {:flz_closed, :ref_g, :eof})
      _ = GenServer.call(pid, :stream_ref_status)

      assert_receive {:output_stall, "cont_op"}
      refute_received {:output_stall, "bnd_op"}
    end

    test "delete_entity_state is ignored for a workflow (non-dynamic) class", %{pid: pid} do
      class = "bnd_del_#{System.unique_integer([:positive])}"
      ClassRegistry.put(class, :workflow)
      on_exit(fn -> ClassRegistry.delete(class) end)

      seg = {class, "proc", "seg"}
      wal = Paths.wal_path(seg)
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "dummy")

      frame = Wire.encode_entity_id(class, "proc", "seg")
      send(pid, {:flz_data, :ref_z, "delete_entity_state", frame, %{}})
      # Flush the frame through the channel: the gate is decided inside the
      # handler, so a delete it did not spawn there is never spawned.
      _ = GenServer.call(pid, :stream_ref_status)

      assert File.exists?(wal)
    end
  end

  describe "route cache invalidation" do
    setup do
      FlezhaTest.Case.install([:tmp_app_data])
      ClassRegistry.ensure_table()

      :ok
    end

    test "a frame for an operative whose cached Overwatch died routes through the registry",
         %{pid: pid} do
      class = "route_dead_#{System.unique_integer([:positive])}"
      on_exit(fn -> ClassRegistry.delete(class) end)

      # The cache names the Overwatch that is about to die; the class registry
      # names the live replacement — the crash-resume shape, in one process.
      test_pid = self()
      dead = spawn(fn -> Process.sleep(:infinity) end)

      # A stand-in Overwatch: forwards the cast the channel routes to it, so a
      # delivered frame is observable from the test process.
      live =
        spawn_link(fn ->
          receive do
            {:"$gen_cast", {:io_out, message, _md}} -> send(test_pid, {:io_out, message})
          end
        end)

      :ok = GenServer.call(pid, {:operative_pid_update, class, [dead]})
      ClassRegistry.put(class, :workflow)
      ClassRegistry.put_overwatch(class, live)

      Process.exit(dead, :kill)

      assert NodeTestHelper.wait_until(
               fn -> not Map.has_key?(:sys.get_state(pid).operative_mapping, class) end,
               1_000
             ),
             "the cached pid outlived the process it cached"

      send(pid, {:flz_data, :ref_dead, class, "out-after-death", %{}})

      # Reaching the live pid at all is the proof: had the cache won, the frame
      # would have been cast into a dead process and lost with no drop counted.
      assert_receive {:io_out, "out-after-death"}, 1_000
    end

    test "a cached pid is monitored while cached and demonitored when it leaves", %{pid: pid} do
      class = "route_gone_#{System.unique_integer([:positive])}"
      holder = spawn(fn -> Process.sleep(:infinity) end)

      :ok = GenServer.call(pid, {:operative_pid_update, class, [holder]})
      assert {:process, holder} in monitors_of(pid)

      :ok = GenServer.call(pid, {:operative_pid_remove, class})
      refute {:process, holder} in monitors_of(pid)

      Process.exit(holder, :kill)
    end
  end

  defp monitors_of(pid) do
    {:monitors, monitors} = Process.info(pid, :monitors)
    monitors
  end

  # The output_stall event is global: real channels elsewhere in the suite can
  # flag leftovers of other tests' operatives mid-refute-window. Forward only
  # this file's instances so the refutes cannot be contaminated.
  @tracked_instances ["operative_a", "operative_b", "cont_op", "bnd_op"]
  defp stall_forwarder(test_pid) do
    fn _e, _m, md, _ ->
      if md.operative_instance in @tracked_instances do
        send(test_pid, {:output_stall, md.operative_instance})
      end
    end
  end

  # The stream's last frame was `ms` ago: the liveness window lapses without
  # the test waiting it out.
  defp age_stream(pid, ms) do
    :sys.replace_state(pid, &%{&1 | last_seen: &1.last_seen - ms})
  end
end

defmodule Flezha.Runner.StreamChannelLivenessTest do
  @moduledoc """
  The StreamChannel bumps liveness counters on the data path. An unbound
  channel (stream_ref nil) must count both the io_in arrival and the drop, so a
  stall classifier can tell "fed but unbound" from "fed but not sending".
  """

  # async: false: registered runner channels; node-global tables.

  use ExUnit.Case, async: false

  alias Flezha.Runner.StreamChannel
  alias Flezha.Transport.LivenessCounters

  test "unbound channel counts io_in_received and nil_drops" do
    exec = "exec_sc_#{System.unique_integer([:positive])}"
    name = :"sc_#{System.unique_integer([:positive])}"

    {:ok, _pid} = StreamChannel.start_link({exec, name, :primary})

    {:error, :no_stream} = StreamChannel.io_in(name, "operative1", {"msg", %{}})
    {:error, :no_stream} = StreamChannel.io_in(name, "operative1", {"msg", %{}})

    row =
      LivenessCounters.snapshot_all().channels
      |> Enum.find(fn c -> c.runner_id == exec and c.name == name end)

    assert row.io_in_received == 2
    assert row.nil_drops == 2
    assert row.send_frame_calls == 0
  end
end

defmodule FlezhaTest.Runner.IoStreamStaleRefReproTest do
  @moduledoc """
  Reproduction of a production incident: the IO data stream silently severs and
  NEVER self-heals — WS ingest keeps flowing, the runner goes idle, all
  procedure WALs/topics freeze, and only a operative recreate restores it.

  Root cause under test: the IO `StreamChannel` can hold a STALE non-nil
  `stream_ref` after its NIF stream dies WITHOUT emitting `{:flz_closed, ...}`.
  Because `{:bind}` only binds a `nil` ref, a reconnecting live stream is
  rejected with `:already_bound`, and there is NO liveness watchdog to reset
  the ref. This is the IO-side twin of the stale handle the CONTROL channel
  resets in `ControlChannel.reset_stream_ref/1`, which the IO channel does
  not do.

  Fails when: a dead-but-unsignalled IO stream can be recovered without a
  process restart (i.e. if a watchdog/reset is ever added, step 3 changes).
  """
  alias Flezha.Runner.{StreamChannel, StreamPool}
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: registered runner channels.

  use ExUnit.Case, async: false

  setup context do
    name =
      ("io_repro_" <> to_string(context.test))
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    {:ok, pid} = StreamChannel.start_link({"ce_repro", name, :primary})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)
    %{pid: pid, name: name}
  end

  test "a live IO stream that dies WITHOUT flz_closed is permanently severed", %{
    pid: pid,
    name: name
  } do
    # 1. HEALTHY: a live NIF stream is bound — this is the 10:15→13:50 window
    #    where data flowed and all procedure WALs advanced every minute.
    :ok = GenServer.call(pid, {:bind, :stream_ref_live})
    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_live

    # 2. PROD CONDITION (13:50:42): the NIF stream dies but emits NO
    #    {:flz_closed, ...}. In prod there were ZERO "Connection closed for IO
    #    socket" log lines all day, and no StreamChannel restart — so the only
    #    two paths that nil stream_ref (flz_closed / re-init) never ran.
    #    We therefore deliberately send NOTHING here. The ref stays stale.
    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_live

    # 3. RECONNECT: the runner opens a fresh IO stream; the acceptor tries to
    #    claim a channel via the real bind path. The stale non-nil ref makes the
    #    bind return :already_bound, so bind_stream_to_first_nil/2 finds NO slot.
    #    => the live, healthy reconnecting stream can NEVER take over.
    assert nil == StreamPool.bind_stream_to_first_nil([name], :stream_ref_reconnect)

    # 4. The channel STILL points at the dead ref. Nothing — no watchdog, no
    #    timer, no heal — ever clears it. This is the permanent severance.
    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_live

    # 5. PROOF THE MISSING SIGNAL IS THE GAP: had flz_closed been delivered
    #    (what the NIF failed to emit), recovery works immediately.
    send(pid, {:flz_closed, :stream_ref_live, :eof})
    assert GenServer.call(pid, :stream_ref_status) == nil

    assert {:bound, ^pid, ^name} =
             StreamPool.bind_stream_to_first_nil([name], :stream_ref_reconnect)

    assert GenServer.call(pid, :stream_ref_status) == :stream_ref_reconnect
  end

  test "the IO StreamChannel has no reset API while the control channel does (the 2026-06-01 asymmetry)" do
    refute function_exported?(StreamChannel, :reset_stream_ref, 1)
    assert function_exported?(Flezha.Runner.ControlChannel, :reset_stream_ref, 1)
  end
end
