defmodule FlezhaTest.Runner.StreamChannelTest do
  alias Flezha.Operative.ClassRegistry
  alias Flezha.Runner.StreamChannel
  alias Flezha.State.Paths
  alias Flezha.State.TestHelpers
  alias Flezha.State.Wire
  alias Flezha.Transport.LivenessCounters
  alias FlezhaTest.Helper.NodeTestHelper

  # async: false: writes Application env; a shared on-disk root; registered runner channels.

  use ExUnit.Case, async: false

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
      prev_threshold = Application.get_env(:flezha, :io_stream_liveness_threshold_ms)
      prev_interval = Application.get_env(:flezha, :io_stream_liveness_interval_ms)
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

      on_exit(fn ->
        :telemetry.detach(id)
        restore_env(:io_stream_liveness_threshold_ms, prev_threshold)
        restore_env(:io_stream_liveness_interval_ms, prev_interval)
      end)

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

    defp channel_row(name) do
      LivenessCounters.snapshot_all().channels
      |> Enum.find(fn c -> c.runner_id == "ce_test" and c.name == name end)
    end
  end

  describe "output_stall / delete gating (kind-aware)" do
    setup do
      base = TestHelpers.tmp_app_data_dir()
      ClassRegistry.ensure_table()

      on_exit(fn ->
        Application.delete_env(:flezha, :state_root)
        File.rm_rf!(base)
      end)

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

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, value), do: Application.put_env(:flezha, key, value)

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
