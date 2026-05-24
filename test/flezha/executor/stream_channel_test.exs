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
