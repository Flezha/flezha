defmodule FlezhaTest.Executor.StreamChannelTest do
  alias Flezha.Executor.StreamChannel

  use ExUnit.Case, async: false

  setup context do
    name =
      "stream_channel_test_#{context.test}"
      |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
      |> String.to_atom()

    {:ok, pid} = StreamChannel.start_link({"ce_test", name, :primary})

    on_exit(fn ->
      if Process.alive?(pid), do: GenServer.stop(pid)
    end)

    %{pid: pid, name: name}
  end

  describe "stream closed → output_stall telemetry" do
    test "{:flz_closed} emits output_stall telemetry for every relay in relay_mapping",
         %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_b", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      test_pid = self()
      handler_id = "stall_test_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:flezha, :state, :output_stall],
        fn _e, _m, md, _ -> send(test_pid, {:output_stall, md.relay_instance}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      send(pid, {:flz_closed, :ref_x, :eof})
      # flush through mailbox
      _ = GenServer.call(pid, :stream_ref_status)

      assert_receive {:output_stall, "relay_a"}
      assert_receive {:output_stall, "relay_b"}
    end

    test "{:flz_closed} with empty relay_mapping emits no output_stall telemetry",
         %{pid: pid} do
      test_pid = self()
      handler_id = "stall_test_empty_#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler_id,
        [:flezha, :state, :output_stall],
        fn _e, _m, md, _ -> send(test_pid, {:output_stall, md.relay_instance}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler_id) end)

      :ok = GenServer.call(pid, {:bind, :ref_y})
      send(pid, {:flz_closed, :ref_y, :eof})
      _ = GenServer.call(pid, :stream_ref_status)

      refute_receive {:output_stall, _}, 100
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

    test "{:io_in, ...} call on nil-ref returns {:error, :no_stream}", %{pid: pid} do
      # The :no_stream error proves the guard fired before any NIF call —
      # there's no other code path that can reach this clause.
      assert {:error, :no_stream} =
               GenServer.call(pid, {:io_in, "p1", {"msg", %{}}})
    end

    test "{:io_in, ...} cast on nil-ref drops without raising", %{pid: pid} do
      # Cast must not call into the NIF when stream_ref is nil — the
      # GenServer survives the cast and remains responsive.
      :ok = GenServer.cast(pid, {:io_in, "p1", {"msg", %{}}})
      # Round-trip a synchronous call to flush the cast through the mailbox
      # and confirm the channel is still alive and unchanged.
      assert GenServer.call(pid, :stream_ref_status) == nil
    end

    test "{:io_in, ...} cast on nil-ref counts the drop (6.3) instead of silently dropping",
         %{pid: pid} do
      test_pid = self()
      id = "stream-channel-drop-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        id,
        [:flezha, :stream, :drop],
        fn _e, m, md, _ -> send(test_pid, {:drop, m, md}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(id) end)

      :ok = GenServer.cast(pid, {:io_in, "p1", {"msg", %{}}})
      assert_receive {:drop, %{count: 1}, %{relay_id: "p1"}}
      assert GenServer.call(pid, :stream_ref_status) == nil
    end
  end

  describe "heartbeat liveness → output_stall telemetry" do
    @threshold_ms 25

    setup do
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
        fn _e, _m, md, _ -> send(test_pid, {:output_stall, md.relay_instance}) end,
        nil
      )

      on_exit(fn ->
        :telemetry.detach(id)
        restore_env(:io_stream_liveness_threshold_ms, prev_threshold)
        restore_env(:io_stream_liveness_interval_ms, prev_interval)
      end)

      :ok
    end

    test "idle relay that keeps heart-beating is never flagged", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      # No real output, but a fresh heartbeat lands right before the check.
      Process.sleep(@threshold_ms * 3)
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{}})
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_receive {:output_stall, _}, 100
    end

    test "relay whose heartbeats stop is flagged for every relay", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_b", [self()]})
      :ok = GenServer.call(pid, {:bind, :ref_x})

      # Last heartbeat, then silence past the threshold.
      send(pid, {:flz_data, :ref_x, "data_heartbeat", "", %{}})
      Process.sleep(@threshold_ms * 3)
      send(pid, :io_liveness_check)

      assert_receive {:output_stall, "relay_a"}
      assert_receive {:output_stall, "relay_b"}
    end

    test "unbound stream is never flagged", %{pid: pid} do
      :ok = GenServer.call(pid, {:relay_pid_update, "relay_a", [self()]})

      Process.sleep(@threshold_ms * 3)
      send(pid, :io_liveness_check)
      _ = GenServer.call(pid, :stream_ref_status)

      refute_receive {:output_stall, _}, 100
    end
  end

  defp restore_env(key, nil), do: Application.delete_env(:flezha, key)
  defp restore_env(key, value), do: Application.put_env(:flezha, key, value)
end
