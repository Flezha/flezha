defmodule FlezhaTest.Runner.StreamChannelTest do
  alias Flezha.Convergence.Loop
  alias Flezha.Relay.ClassRegistry
  alias Flezha.Runner.ControlChannel
  alias Flezha.Runner.ControlUnit
  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.StreamChannel.Prefix
  alias Flezha.State.Paths
  alias Flezha.State.Wire
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.MessageTestHelper
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.Telemetry
  alias FlezhaTest.Helper.WireEncoders

  # async: false: writes Application env; a shared on-disk root; registered runner channels.

  use FlezhaTest.Case, async: false
  use Mimic
  import FlezhaTest.Helper.MimicGlobal

  setup context do
    name =
      "stream_channel_test_#{context.test}"
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    {:ok, pid} = StreamChannel.start_link({"_system", "ce_test", name, :primary})

    on_exit(fn ->
      NodeTestHelper.ensure_stopped(pid)
    end)

    %{pid: pid, name: name}
  end

  describe "stream closed" do
    # fails when: a stream lost under a live session declares an output stall,
    # and the loop's remedy re-seats the relays' live Cells from records that
    # lag them, losing and doubling acknowledged inputs.
    test "{:flz_closed} declares no output_stall: the session rebinds its data plane",
         %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      capture_stalls()
      # The close is a warning, below the test config's primary gate.
      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        ExUnit.CaptureLog.capture_log([level: :warning], fn ->
          send(pid, {:flz_closed, :ref_x, :eof})
          # flush through mailbox
          _ = GenServer.call(pid, :stream_ref_status)
        end)

      assert log =~ "Connection closed for IO socket (eof)."
      refute_received {:output_stall, _}
    end

    # fails when: a close for a stream the channel already let go, or one a bind
    # replaced, unbinds the channel or asks for a rebind, which releases the
    # fresh streams the running rebind bound.
    test "a close for a stream the channel no longer holds changes nothing", %{pid: pid} do
      set_mimic_global_until_exit()
      test_pid = self()
      stub(ControlChannel, :rebind_data_streams, fn id -> send(test_pid, {:rebind, id}) end)
      :ok = GenServer.call(pid, {:bind, :ref_fresh})

      send(pid, {:flz_closed, :ref_replaced, :eof})
      assert GenServer.call(pid, :stream_ref_status) == :ref_fresh

      [] = GenServer.call(pid, :release_stream)
      send(pid, {:flz_closed, :ref_fresh, :eof})
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:rebind, _}
    end
  end

  describe "session teardown" do
    # Fails as: every runner reconnect leaves its streams' handles published,
    # pinning the listener's descriptors until the controller hits EMFILE.
    test "a supervisor shutdown unpublishes the channel's stream handle" do
      name = :stream_channel_teardown_test

      start_supervised!(
        Supervisor.child_spec({StreamChannel, {"_system", "ce_test", name, :secondary}}, id: name)
      )

      :ok = GenServer.call(name, {:bind, :ref_teardown})
      assert {:ref_teardown, _} = :persistent_term.get({StreamChannel, name})

      :ok = stop_supervised(name)

      assert :persistent_term.get({StreamChannel, name}, :erased) == :erased
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
      Telemetry.capture([:flezha, :stream, :drop], as: :drop)

      {:error, :no_stream} = StreamChannel.io_in(name, "p1", {"msg", %{}})
      assert_receive {:drop, %{count: 1}, %{relay_id: "p1"}}
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

      capture_stalls()

      # Heartbeats only arrive on secondary streams; the check is gated to them.
      sec_name = :"#{name}_secondary"
      {:ok, pid} = StreamChannel.start_link({"_system", "ce_test", sec_name, :secondary})
      on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)

      %{pid: pid, name: sec_name}
    end

    test "idle relay that keeps heart-beating is never flagged", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      # No real output, but a fresh heartbeat lands right before the check.
      age_stream(pid, @threshold_ms * 3)
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{}})
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:output_stall, _}
    end

    test "relay whose heartbeats stop is flagged for every relay", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_b", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      # Last heartbeat, then silence past the threshold.
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{}})
      age_stream(pid, @threshold_ms * 3)
      send(pid, :io_liveness_check)

      assert_receive {:output_stall, "relay_a"}
      assert_receive {:output_stall, "relay_b"}
    end

    test "a silent stream declares each stall against its runner", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_z})

      MessageTestHelper.become_loop()
      handler_id = "stall_declare_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:flezha, :state, :output_stall],
        &Loop.handle_telemetry/4,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      age_stream(pid, @threshold_ms * 3)
      send(pid, :io_liveness_check)

      assert_receive {:declare, :output_stall, "relay_a", %{runner_id: "ce_test"}}
    end

    test "unbound stream is never flagged", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})

      age_stream(pid, @threshold_ms * 3)
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:output_stall, _}
    end

    test "primary (send-only) stream is never flagged even when bound and silent" do
      name = :"sc_primary_#{System.unique_integer([:positive])}"
      {:ok, pid} = StreamChannel.start_link({"_system", "ce_test", name, :primary})
      on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)

      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
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
    # must read as zero — never as a stalled cell.
    test "a heartbeat without the handout keys reads as zeros", %{pid: pid, name: name} do
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{"dispatches" => "42"}})
      _ = GenServer.call(pid, :stream_ref_status)
      row = channel_row(name)

      assert row.hb_inflight == 0
      assert row.hb_inflight_oldest_ms == 0
      assert row.hb_handouts == 0
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
           "timers_fired" => "12",
           "handouts" => "13"
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
            hb_timers_fired: 12,
            hb_handouts: 13
          ] do
        assert Map.fetch(row, slot) == {:ok, value}, "#{slot} did not mirror"
      end
    end

    defp channel_row(name) do
      LivenessCounters.snapshot_all().channels
      |> Enum.find(fn c -> c.runner_id == "ce_test" and c.name == name end)
    end
  end

  describe "output_stall and delete for either kind" do
    setup do
      FlezhaTest.Case.install([:tmp_app_data])
      ClassRegistry.ensure_table()

      :ok
    end

    test "a steps relay is flagged for output_stall on a silent stream, as a stream is",
         %{name: name} do
      FlezhaTest.Case.install([:env])
      Application.put_env(:flezha, :io_stream_liveness_threshold_ms, 25)
      Application.put_env(:flezha, :io_stream_liveness_interval_ms, 0)
      ClassRegistry.put("bnd_op", :steps)
      on_exit(fn -> ClassRegistry.delete("bnd_op") end)

      {:ok, pid} =
        StreamChannel.start_link({"_system", "ce_test", :"#{name}_secondary", :secondary})

      on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)

      :ok = GenServer.call(pid, {:relay_pid_update, "bnd_op", [self()]})
      :ok = GenServer.call(pid, {:relay_pid_update, "cont_op", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_g})

      capture_stalls()

      age_stream(pid, 75)
      send(pid, :io_liveness_check)

      assert_receive {:output_stall, "cont_op"}
      assert_receive {:output_stall, "bnd_op"}
    end

    test "delete_cell_state removes a steps Cell's state, as a stream's", %{pid: pid} do
      class = "bnd_del_#{System.unique_integer([:positive])}"
      ClassRegistry.put(class, :steps)

      on_exit(fn ->
        ClassRegistry.delete(class)
        File.rm_rf!(Paths.class_dir(class))
      end)

      seg = {class, "task", "seg"}
      wal = Paths.wal_path(seg)
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "dummy")

      frame = Wire.encode_cell_id(class, "task", "seg")
      send(pid, {:flz_data, :ref_z, "delete_cell_state", frame, %{}})

      assert NodeTestHelper.wait_until(fn -> not File.exists?(wal) end, 5_000),
             "the evicted steps Cell's state was kept"
    end
  end

  # A session proven to `shop` names a Cell of `billing`'s relay, which has no
  # Overwatch on this node: directly, or through a path under its own relay.
  describe "a runner frame never reaches another namespace's state" do
    setup [:set_mimic_global_until_exit, :stub_rem_relay]

    setup %{name: name} do
      FlezhaTest.Case.install([:tmp_app_data])
      ClassRegistry.ensure_table()
      on_exit(NodeTestHelper.override_log_level(:warning))

      n = System.unique_integer([:positive])
      runner = "ce_shop_#{n}"
      own = "orders_#{n}"
      victim = "payments_#{n}"
      :ok = Paths.claim_namespace(own, "shop")
      :ok = Paths.claim_namespace(victim, "billing")
      :ok = ClassRegistry.put_overwatch(own, self(), runner)

      {:ok, shop} = StreamChannel.start_link({"shop", runner, :"#{name}_shop", :primary})

      cell = {victim, "main", "k1"}
      files = [Paths.wal_path(cell), Paths.snapshot_path(cell, 3)]
      File.mkdir_p!(Paths.state_dir(cell))
      Enum.each(files, &File.write!(&1, "victim"))

      on_exit(fn ->
        NodeTestHelper.ensure_stopped(shop)
        ClassRegistry.delete(own)
      end)

      %{shop: shop, runner: runner, own: own, victim: victim, files: files}
    end

    # Fails when: a relay of another namespace with no record on this node is
    # admitted as unrecorded and its Cell deleted.
    test "naming an unrecorded relay of another namespace is refused and deletes nothing",
         %{shop: shop, runner: runner, victim: victim, files: files} do
      log =
        ExUnit.CaptureLog.capture_log([level: :warning], fn ->
          send_delete(shop, Wire.encode_cell_id(victim, "main", "k1"))
          assert_received {:rem_relay, ^runner, ^victim}
        end)

      assert log =~ "#{inspect(victim)}, which belongs to \"billing\"; refused"
      assert Enum.all?(files, &File.exists?/1), "billing's Cell state was deleted"
    end

    # Fails when: a relay class or tasklet id is joined into a path unchecked,
    # so the sender's own relay, or a class named past its namespace, walks
    # into another namespace's tree.
    test "a relay class or tasklet id naming another directory is refused at decode",
         %{shop: shop, own: own, victim: victim, files: files} do
      log =
        ExUnit.CaptureLog.capture_log([level: :warning], fn ->
          send_delete(shop, Wire.encode_cell_id(own, "../../billing/#{victim}/main", "k1"))
          send_delete(shop, Wire.encode_cell_id("../billing/#{victim}", "main", "k1"))
        end)

      assert log =~ "delete_cell_state decode failed: :bad_cell_id"
      refute_received {:rem_relay, _, _}
      assert Enum.all?(files, &File.exists?/1), "billing's Cell state was deleted"
    end

    # Fails when: the namespace rule refuses the unrecorded relay of the
    # session's own namespace, such as a steps Cell after its relay ended.
    test "an unrecorded relay of the session's own namespace is still admitted",
         %{shop: shop} do
      mine = "shop_unrecorded_#{System.unique_integer([:positive])}"
      :ok = Paths.claim_namespace(mine, "shop")
      wal = Paths.wal_path({mine, "main", "k1"})
      File.mkdir_p!(Path.dirname(wal))
      File.write!(wal, "mine")

      send_delete(shop, Wire.encode_cell_id(mine, "main", "k1"))

      refute File.exists?(wal), "the session's own unrecorded Cell was kept"
      refute_received {:rem_relay, _, _}
    end
  end

  describe "route cache invalidation" do
    setup do
      FlezhaTest.Case.install([:tmp_app_data])
      ClassRegistry.ensure_table()

      :ok
    end

    test "a frame for a relay whose cached Overwatch died routes through the registry",
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

      :ok = GenServer.call(pid, {:relay_pid_update, class, [dead]})
      ClassRegistry.put(class, :steps)
      ClassRegistry.put_overwatch(class, live, "ce_test")

      Process.exit(dead, :kill)

      assert NodeTestHelper.wait_until(
               fn -> not Map.has_key?(:sys.get_state(pid).relay_mapping, class) end,
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

      :ok = GenServer.call(pid, {:relay_pid_update, class, [holder]})
      assert {:process, holder} in monitors_of(pid)

      :ok = GenServer.call(pid, {:relay_pid_remove, class})
      refute {:process, holder} in monitors_of(pid)

      Process.exit(holder, :kill)
    end
  end

  describe "an output is routed only for the runner that owns its relay" do
    setup [:set_mimic_global_until_exit, :stub_rem_relay]

    setup do
      class = "owned_#{System.unique_integer([:positive])}"
      on_exit(fn -> ClassRegistry.delete(class) end)

      %{class: class}
    end

    test "the owner's first output is routed through the ownership record and cached",
         %{pid: pid, class: class} do
      :ok = ClassRegistry.put_overwatch(class, self(), "ce_test")

      send(pid, {:flz_data, :ref, class, "first", %{}})

      assert_receive {:"$gen_cast", {:io_out, "first", _}}, 1_000
      assert :sys.get_state(pid).relay_mapping[class] == [self()]
    end

    test "a part takes the route its instance's output takes, as a preview",
         %{pid: pid, class: class} do
      :ok = ClassRegistry.put_overwatch(class, self(), "ce_test")
      metadata = %{"flz.relay" => class, "flz.part" => "0"}

      send(pid, {:flz_data, :ref, "output_part", "he", metadata})

      assert_receive {:"$gen_cast", {:io_part, "he", ^metadata}}, 1_000
      refute_received {:"$gen_cast", {:io_out, _, _}}
    end

    test "a part from a session that no longer owns its instance is dropped",
         %{pid: pid, class: class} do
      :ok = ClassRegistry.put_overwatch(class, self(), "runner_a")

      send(pid, {:flz_data, :ref, "output_part", "he", %{"flz.relay" => class}})
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:"$gen_cast", {:io_part, _, _}}
      assert_received {:rem_relay, "ce_test", ^class}
    end

    test "another runner's output is refused, counted every time and reported once",
         %{pid: pid, class: class} do
      :ok = ClassRegistry.put_overwatch(class, self(), "runner_a")
      Telemetry.capture([:flezha, :runner, :foreign_frame_refused], as: :refused)

      send(pid, {:flz_data, :ref, class, "forged-1", %{}})
      send(pid, {:flz_data, :ref, class, "forged-2", %{}})
      _ = GenServer.call(pid, :stream_ref_status)

      refute_received {:"$gen_cast", {:io_out, _, _}}

      assert_received {:refused, %{count: 1},
                       %{
                         runner_id: "ce_test",
                         identity: "ce_test",
                         owner: "runner_a",
                         relay_id: ^class,
                         kind: :output
                       }}

      refute_received {:refused, _, _}
      assert_received {:rem_relay, "ce_test", ^class}
      refute_received {:rem_relay, _, _}
      rows = LivenessCounters.by_runner(LivenessCounters.snapshot_all().channels)
      assert get_in(rows, ["ce_test", :foreign_frame_refused]) == 2
    end

    test "an output for a relay nobody records is the counted unrouted drop",
         %{pid: pid, class: class} do
      Telemetry.capture([:flezha, :stream, :drop], as: :drop)

      send(pid, {:flz_data, :ref, class, "unrouted", %{}})
      _ = GenServer.call(pid, :stream_ref_status)

      assert_received {:drop, %{count: 1}, %{relay_id: ^class, reason: :unrouted}}
      refute_received {:rem_relay, _, _}
    end
  end

  # A refusal's `RemRelay` reaches the test process as `{:rem_relay, runner, relay}`.
  defp stub_rem_relay(_context) do
    test_pid = self()

    stub(ControlUnit, :stop_relay_async, fn runner_id, id ->
      send(test_pid, {:rem_relay, runner_id, id})
      :ok
    end)

    :ok
  end

  # Send one `delete_cell_state` and wait out whatever it started: the channel
  # handles the frame before it answers the call, and runs a deletion in a
  # durable task.
  defp send_delete(channel, frame) do
    send(channel, {:flz_data, :ref_kb877, "delete_cell_state", frame, %{}})
    _ = GenServer.call(channel, :stream_ref_status)
    NodeTestHelper.await_durable_tasks()
  end

  defp monitors_of(pid) do
    {:monitors, monitors} = Process.info(pid, :monitors)
    monitors
  end

  # The output_stall event is global: real channels elsewhere in the suite can
  # flag leftovers of other tests' relays mid-refute-window. Forward only
  # this file's instances so the refutes cannot be contaminated.
  @tracked_instances ["relay_a", "relay_b", "cont_op", "bnd_op"]
  defp capture_stalls() do
    Telemetry.capture([:flezha, :state, :output_stall],
      filter: &(&1.relay_instance in @tracked_instances),
      as: fn _e, _m, md -> {:output_stall, md.relay_instance} end
    )
  end

  # fails when: an off-tier park stops telling the Overwatch, once its entry is
  # durable, that the input it parked is dead-lettered, with the call waiting on
  # it if there is one.
  test "a park frame is announced once its entry is durable" do
    FlezhaTest.Helper.DlqFixtures.isolated_dir()
    set_mimic_global_until_exit()
    stub(Flezha.Runner.ControlChannel, :send_control_async, fn _runner, _command -> :ok end)
    rc = "parked_call_#{System.unique_integer([:positive])}"
    :ok = ClassRegistry.put_overwatch(rc, self(), "runner-a")
    on_exit(fn -> ClassRegistry.delete(rc) end)

    park = fn offset, reply_to ->
      body =
        WireEncoders.encode_park_body(%{
          park_id: "p#{offset}",
          origin_offset: offset,
          channel: "in",
          error: "kaboom",
          attempts: 3,
          payload: "poison",
          reply_to: reply_to
        })

      header = WireEncoders.encode_header(1, 1, 1, 0, <<>>)
      frame = WireEncoders.encode_frame(Wire.encode_cell_id(rc, "task", "k"), 0, header, body)
      StreamChannel.Records.handle_park_payload(%{runner_id: "r1", identity: "runner-a"}, frame)
    end

    park.(3, "w:caller")

    assert_receive {:"$gen_cast",
                    {:dead_lettered,
                     %{token: "w:caller", entry: ref, error: "kaboom", offset: 3, new: true}}},
                   2_000

    assert [%{"id" => id}] = Flezha.Dlq.Store.list(rc)
    assert ref == "#{rc}/task/#{id}"

    # A re-sent park lands on the entry: its caller is answered, no listener told.
    park.(3, "w:caller")
    assert_receive {:"$gen_cast", {:dead_lettered, %{token: "w:caller", offset: 3, new: false}}}

    park.(4, nil)

    assert_receive {:"$gen_cast",
                    {:dead_lettered, %{token: nil, offset: 4, tasklet: "task", attempts: 3}}},
                   2_000
  end

  # fails when: a draining stop's final marker is read as an offset, or the
  # relay's Overwatch hears it drained before its writers hold the records the
  # runner sent ahead of the marker.
  test "a final marker tells the relay's Overwatch it drained once its records are written" do
    rc = "drained_#{System.unique_integer([:positive])}"
    :ok = ClassRegistry.put_overwatch(rc, self(), "runner-a")
    on_exit(fn -> ClassRegistry.delete(rc) end)
    {[], fenced} = Prefix.fenced(Prefix.new())
    cell = {rc, "task", ""}

    state = %{
      runner_id: "r1",
      identity: "runner-a",
      relay_mapping: %{rc => [self()]},
      prefix: Prefix.received(fenced, cell, 10)
    }

    header = WireEncoders.encode_header(1, 1, 1, 0, <<>>)
    frame = WireEncoders.encode_frame(Wire.encode_cell_id(rc, "", ""), 0, header, <<1>>)
    assert {:ok, ^rc, :final, 1} = StreamChannel.Records.processed_prefix(state, frame)

    state = StreamChannel.Records.confirm_prefix(state, &Prefix.marker(&1, rc, :final, 1))
    refute_received {:relay_drained, _}
    _ = StreamChannel.Records.confirm_prefix(state, &Prefix.drained(&1, cell, 10))
    assert_received {:relay_drained, ^rc}
  end

  # fails when: a stopping relay with no durable state, whose ownership record
  # its stop already removed, has its runner's final marker refused as another
  # namespace's, so the stop waits out its whole budget and the runner is told
  # to remove a relay it already removed.
  test "a stopping relay's final marker is admitted on the channel that still routes it" do
    rc = "stopping_#{System.unique_integer([:positive])}"

    state = %{
      runner_id: "r1",
      identity: "runner-a",
      namespace: "bench",
      liveness_ref: nil,
      relay_mapping: %{rc => [self()]}
    }

    header = WireEncoders.encode_header(1, 1, 1, 0, <<>>)
    frame = WireEncoders.encode_frame(Wire.encode_cell_id(rc, "", ""), 0, header, <<1>>)
    assert {:ok, ^rc, :final, 1} = StreamChannel.Records.processed_prefix(state, frame)
  end

  test "a frame that does not decode is reported once per session and kind" do
    session = "undecodable_#{System.unique_integer([:positive])}"
    state = %{runner_id: session, identity: session}
    on_exit(NodeTestHelper.override_log_level(:warning))
    on_exit(fn -> Flezha.Runner.FleetManager.Marks.forget_identity(session) end)

    log =
      ExUnit.CaptureLog.capture_log([level: :warning], fn ->
        for _ <- 1..3, do: StreamChannel.Records.handle_cell_state_change(state, "garbage")
        StreamChannel.Records.handle_delete_cell_state(state, "garbage")
      end)

    assert [_, _] = String.split(log, "cell_state_change decode failed")
    assert log =~ "delete_cell_state decode failed"
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

    {:ok, _pid} = StreamChannel.start_link({"_system", exec, name, :primary})

    {:error, :no_stream} = StreamChannel.io_in(name, "relay1", {"msg", %{}})
    {:error, :no_stream} = StreamChannel.io_in(name, "relay1", {"msg", %{}})

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
  tasklet WALs/topics freeze, and only a relay recreate restores it.

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

    {:ok, pid} = StreamChannel.start_link({"_system", "ce_repro", name, :primary})
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)
    %{pid: pid, name: name}
  end

  test "a live IO stream that dies WITHOUT flz_closed is permanently severed", %{
    pid: pid,
    name: name
  } do
    # 1. HEALTHY: a live NIF stream is bound — this is the 10:15→13:50 window
    #    where data flowed and all tasklet WALs advanced every minute.
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
