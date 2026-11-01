defmodule Flezha.Transport.DriverParityTest do
  @moduledoc """
  The one driver loop, driven through the real NIF over a real UDS socket, with
  the peer a `:gen_tcp` client speaking the frame.

  What it proves: the `SendFrame` arm reaches the peer and the peer's frames
  reach their owner; the driver self-report carries what the driver did; a
  stream activated once hands its owner one frame; the `CloseStream` arm
  releases the stream; a peer that goes away is reported with a word from
  the one declared set; and a released stream refuses a frame.

  The tree has no QUIC client of its own, so the same cases over QUIC, against a
  real `quinn_proto` peer, are the Rust conformance table's in
  `crates/flz_nif/src/driver/conformance_tests.rs`, which runs every scenario over both
  wires.
  """

  # async: false: each scenario binds a real listening socket and a UDP port.
  use ExUnit.Case, async: false

  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.StreamPair

  # Every word `flz_nif`'s `CloseReason` renders. A reason outside this set is a
  # transport spelling an event with a word of its own, which is the shape the
  # one declared vocabulary replaced.
  @declared_reasons [
    "connection_closed",
    "stream_reset",
    "read_error",
    "write_error",
    "frame_too_large",
    "malformed_frame"
  ]

  defp declared?("connection_lost: " <> _), do: true
  defp declared?(reason), do: reason in @declared_reasons

  defp start_server do
    path = Path.join(System.tmp_dir!(), "flz_parity_#{System.unique_integer([:positive])}.sock")
    {:ok, server} = Native.start_uds_server(path, self())
    on_exit(fn -> Native.stop_server(server) end)
    {server, path}
  end

  # A NIF resource is freed the moment nothing on a live heap names it, and the
  # handle IS the stream: a collected `StreamHandle` closes it and a collected
  # QUIC connection closes the whole connection. A handle the test still means
  # to use is stashed where the collector can see it.
  defp hold(term) do
    Process.put({__MODULE__, :held}, [term | Process.get({__MODULE__, :held}, [])])
    term
  end

  # A peer opened from the test process, plus the server-side handle the driver
  # announced for it. Only the test process is the acceptor, so only it can
  # wait for that announcement.
  defp connect(path) do
    client = StreamPair.peer(path)
    assert_receive {:flz_connection, server_handle}, 5_000
    {client, hold(server_handle)}
  end

  defp send_up(client, payload_id, message, metadata \\ %{}) do
    table =
      for {k, v} <- metadata, into: <<>>, do: <<byte_size(k), byte_size(v), k::binary, v::binary>>

    id = String.pad_trailing(payload_id, 32)
    body = <<byte_size(message)::32, message::binary, byte_size(table)::16, table::binary>>
    :gen_tcp.send(client, [id, <<byte_size(body)::32>>, body])
  end

  # The driver-elapsed clock starts at zero, so a read taken in the driver's
  # first millisecond is stamped `0` and says nothing. Wait for the clock to
  # leave zero before asking a case to prove a stamp.
  defp await_driver_clock(server) do
    assert NodeTestHelper.wait_until(fn -> Native.driver_stats(server).now_ms > 0 end, 1_000),
           "the driver clock never advanced"
  end

  defp forward(pid) do
    receive do
      message -> send(pid, {:forwarded, message})
    end

    forward(pid)
  end

  defp await_stream_row(server, stream_id, want) do
    NodeTestHelper.wait_until(
      fn ->
        row =
          server
          |> Native.driver_stats()
          |> Map.fetch!(:streams)
          |> Enum.find(&(&1.stream_id == stream_id))

        if want.(row), do: row || :gone
      end,
      2_000
    )
  end

  describe "notices" do
    # The suite's primary level is `:error`, which drops a warning before any
    # capture sees it.
    setup do
      on_exit(NodeTestHelper.override_log_level(:warning))
    end

    test "a frame the driver refuses is said in the controller's log" do
      {_server, path} = start_server()
      {_client, server_handle} = connect(path)

      sinks =
        for {{Flezha.Transport.Server, _}, pid, _, _} <-
              Supervisor.which_children(Flezha.Supervisor),
            is_pid(pid),
            do: pid

      for pid <- sinks, do: :erlang.trace(pid, true, [:receive])
      on_exit(fn -> for pid <- sinks, do: :erlang.trace(pid, false, [:receive]) end)

      {_, log} =
        ExUnit.CaptureLog.with_log(fn ->
          metadata = %{String.duplicate("k", 300) => "v"}
          :ok = Native.send_frame(server_handle, "p-refused", "x", metadata)

          assert_receive {:trace, sink, :receive,
                          {:flz_notice, :warn, "flz_nif::driver",
                           "frame refused for payload p-refused: " <> _}},
                         5_000

          :sys.get_state(sink)
        end)

      assert log =~ "[warning] [flz_nif::driver] frame refused for payload p-refused: "
    end
  end

  describe "uds" do
    test "a frame crosses in both directions", _ctx do
      {_server, path} = start_server()
      {client, server_handle} = connect(path)
      :ok = Native.set_active(server_handle)
      server_id = Native.stream_id(server_handle)

      :ok = send_up(client, "p-up", "up", %{"k" => "v"})
      assert_receive {:flz_data, ^server_id, "p-up", "up", %{"k" => "v"}}, 5_000

      :ok = Native.send_frame(server_handle, "p-down", "down", %{})
      assert_receive {:flz_data, 0, "p-down", "down", %{}}, 5_000
    end

    test "the driver self-report carries what this driver did", _ctx do
      {server, path} = start_server()
      await_driver_clock(server)
      {client, server_handle} = connect(path)
      :ok = Native.set_active(server_handle)
      server_id = Native.stream_id(server_handle)

      :ok = send_up(client, "p-up", "up")
      assert_receive {:flz_data, ^server_id, "p-up", _, _}, 5_000
      :ok = Native.send_frame(server_handle, "p-down", "down", %{})

      row =
        await_stream_row(server, server_id, fn
          nil -> false
          row -> row.frames_written > 0 and row.frames_read > 0
        end)

      assert is_map(row), "the driver reported no row for stream #{server_id}"
      assert row.last_read_ms > 0, "a read the loop took stamps the loop's clock"
      assert row.last_write_ms > 0, "a write the peer took stamps the loop's clock"
      assert row.write_buffer_len == 0, "a reading peer leaves nothing buffered"
      assert row.write_buffer_dropped == 0, "a healthy peer has nothing refused"
    end

    test "a stream activated once hands its owner one frame and holds the rest",
         _ctx do
      {server, path} = start_server()
      {client, server_handle} = connect(path)
      server_id = Native.stream_id(server_handle)
      test_pid = self()

      sent = ["p-1", "p-2", "p-3"]
      for id <- sent, do: :ok = send_up(client, id, "")

      # Every frame is read before the stream is activated, so which ones reach
      # an owner is decided by the mode and not by timing.
      read_all = &(&1 != nil and &1.frames_read == length(sent))

      assert await_stream_row(server, server_id, read_all),
             "the driver never read the #{length(sent)} frames sent"

      [first | held] = sent
      :ok = Native.set_active_once(server_handle)
      assert_receive {:flz_data, ^server_id, ^first, _, _}, 5_000

      next_owner = spawn(fn -> forward(test_pid) end)
      on_exit(fn -> Process.exit(next_owner, :kill) end)
      :ok = Native.controlling_process(server_handle, next_owner)
      :ok = Native.set_active(server_handle)

      for id <- held,
          do: assert_receive({:forwarded, {:flz_data, ^server_id, ^id, _, _}}, 5_000)

      refute_received {:flz_data, ^server_id, _, _, _}
    end

    test "closing a stream releases it", _ctx do
      {server, path} = start_server()
      {_client, server_handle} = connect(path)
      :ok = Native.set_active(server_handle)
      server_id = Native.stream_id(server_handle)

      assert await_stream_row(server, server_id, &is_map/1),
             "the driver never reported the stream it announced"

      :ok = Native.close(server_handle)

      assert await_stream_row(server, server_id, &is_nil/1) == :gone,
             "a closed stream must leave the self-report"

      assert Native.send_frame(server_handle, "late", "frame", %{}) == {:error, :closed}
    end

    test "a peer that dies is reported with a declared word", _ctx do
      {_server, path} = start_server()
      test_pid = self()

      # The peer holds its handles on its own heap, so killing it frees them
      # at a moment the test chooses rather than at the collector's.
      peer =
        spawn(fn ->
          _client = StreamPair.peer(path)
          send(test_pid, :peer_up)
          receive do: (:never -> :ok)
        end)

      assert_receive :peer_up, 10_000
      assert_receive {:flz_connection, server_handle}, 5_000
      server_id = Native.stream_id(server_handle)

      Process.exit(peer, :kill)

      assert_receive {:flz_closed, ^server_id, reason}, 10_000

      assert declared?(reason),
             "closed with #{inspect(reason)}, which the driver declares nowhere"

      assert reason == "connection_closed"

      # fails when: a frame written on a stream the wire closed is answered
      # `:ok` and dropped by the driver, so its sender believes it went.
      assert Native.send_frame(server_handle, "late", "frame", %{}) == {:error, :closed}
    end
  end

  describe "a peer speaking raw bytes" do
    # `:gen_tcp` reaches a Unix domain socket with a `{:local, path}` address,
    # which is the only raw peer the tree has. Both arms below are loop code
    # the two wires share.
    defp raw_peer(path) do
      {:ok, socket} = :gen_tcp.connect({:local, path}, 0, [:binary, active: false, packet: :raw])
      assert_receive {:flz_connection, server_handle}, 5_000
      :ok = Native.set_active(server_handle)
      {socket, Native.stream_id(server_handle)}
    end

    @tag :capture_log
    test "a header declaring an over-limit payload closes the stream" do
      {_server, path} = start_server()
      {socket, server_id} = raw_peer(path)

      # 36 bytes: a 32-byte payload id, then a big-endian payload size the
      # driver refuses BEFORE it allocates for it.
      header = String.pad_trailing("too-big", 32) <> <<1_000_000_000::big-unsigned-32>>
      :ok = :gen_tcp.send(socket, header)

      assert_receive {:flz_closed, ^server_id, "frame_too_large"}, 5_000
      :gen_tcp.close(socket)
    end

    test "a body that does not decode closes the stream and is counted" do
      {server, path} = start_server()
      {socket, server_id} = raw_peer(path)

      # The header is honest about the 11-byte body; the body's own message
      # length overruns it.
      body = <<100::32, "short", 0::16>>
      id = String.pad_trailing("bad-body", 32)
      :ok = :gen_tcp.send(socket, [id, <<byte_size(body)::32>>, body])

      assert_receive {:flz_closed, ^server_id, "malformed_frame"}, 5_000
      assert Native.driver_stats(server).malformed_frames == 1
      :gen_tcp.close(socket)
    end

    test "a peer that ends the stream is connection_closed" do
      {_server, path} = start_server()
      {socket, server_id} = raw_peer(path)

      :ok = :gen_tcp.close(socket)

      assert_receive {:flz_closed, ^server_id, "connection_closed"}, 5_000
    end
  end
end
