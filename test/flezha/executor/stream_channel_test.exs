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
end
