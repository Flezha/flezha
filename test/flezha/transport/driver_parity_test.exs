defmodule Flezha.Transport.DriverParityTest do
  @moduledoc """
  The one driver loop, driven through the real NIF over a real socket, once per
  server transport.

  `flz_nif` used to hold two whole drivers with no contract between them, and
  nothing in the tree ran the same case over both: a capability one had and the
  other lacked read as a backlog item rather than as a failing build. Every
  scenario in the table below is written once and runs against UDS and against
  QUIC, so a difference between the wires is a red suite.

  What it proves per transport: the `SendFrame` arm reaches the peer and the
  peer's frames reach their owner; the driver self-report carries the same
  fields, moved by the same jobs; the `CloseStream` arm releases the stream;
  and a peer that goes away is reported with a word from the one declared set.

  The last two rows need a peer speaking raw bytes and are UDS-only, because
  the tree has no raw QUIC client. The arms they enter — `frame_too_large` and
  the peer's clean end — are loop code shared by both wires, which is what the
  fold bought: proving them on one transport proves them for the other. The
  QUIC half of the peer-EOF case is driven against a real `quinn_proto` peer by
  the Rust conformance table in `flz_nif/src/driver/conformance_tests.rs`.
  """

  # async: false: each scenario binds a real listening socket and a UDP port.
  use ExUnit.Case, async: false

  alias Flezha.Transport.CertGenerator
  alias Flezha.SDK.Native, as: Client
  alias Flezha.Transport.Native

  # Every word `flz_nif`'s `CloseReason` renders. A reason outside this set is a
  # transport spelling an event with a word of its own, which is the shape the
  # one declared vocabulary replaced.
  @declared_reasons [
    "connection_closed",
    "stream_reset",
    "read_error",
    "write_error",
    "frame_too_large"
  ]

  defp declared?("connection_lost: " <> _), do: true
  defp declared?(reason), do: reason in @declared_reasons

  # A UDP port nothing is listening on right now; the QUIC bind is the next
  # thing that happens, so the window is the test's own scheduling.
  defp free_udp_port do
    {:ok, socket} = :gen_udp.open(0, [:binary, ip: {127, 0, 0, 1}])
    {:ok, port} = :inet.port(socket)
    :ok = :gen_udp.close(socket)
    port
  end

  defp ca_path, do: Application.get_env(:flezha, :ca_cert_path, "app_data/certs/ca.pem")

  defp start_server(:uds) do
    path = Path.join(System.tmp_dir!(), "flz_parity_#{System.unique_integer([:positive])}.sock")
    {:ok, server} = Native.start_uds_server(path, self())
    on_exit(fn -> Native.stop_server(server) end)
    {server, path}
  end

  defp start_server(:quic) do
    CertGenerator.ensure_certs()
    cert = Application.get_env(:flezha, :server_cert_path, "app_data/certs/server.pem")
    key = Application.get_env(:flezha, :server_key_path, "app_data/certs/server.key")

    address = "127.0.0.1:#{free_udp_port()}"
    {:ok, server} = Native.start_quic_server(address, cert, key, "", self())
    on_exit(fn -> Native.stop_server(server) end)
    {server, address}
  end

  # A NIF resource is freed the moment nothing on a live heap names it, and the
  # handle IS the stream: a collected `StreamHandle` closes it and a collected
  # QUIC connection closes the whole connection. A handle the test still means
  # to use is stashed where the collector can see it.
  defp hold(term) do
    Process.put({__MODULE__, :held}, [term | Process.get({__MODULE__, :held}, [])])
    term
  end

  # Open a peer stream. The QUIC control stream carries no preamble, so quinn
  # surfaces it only once the peer writes: the opening frame is what makes the
  # stream visible to the driver.
  defp open_peer(:uds, path) do
    {:ok, client} = Client.client_connect_uds(path, self())
    [client]
  end

  defp open_peer(:quic, address) do
    {:ok, conn} = Client.client_connect_quic(address, ca_path())
    {:ok, client} = Client.client_open_stream(conn, 0, 0, self())
    :ok = Client.send_frame(client, "p-open", "open", %{})
    [client, conn]
  end

  # A peer opened from the test process, plus the server-side handle the driver
  # announced for it. Only the test process is the acceptor, so only it can
  # wait for that announcement.
  defp connect(transport, dial) do
    [client | _] = handles = open_peer(transport, dial)
    Enum.each(handles, &hold/1)
    assert_receive {:flz_connection, server_handle}, 5_000
    {client, hold(server_handle)}
  end

  # The driver-elapsed clock starts at zero, so a read taken in the driver's
  # first millisecond is stamped `0` and says nothing. Wait for the clock to
  # leave zero before asking a case to prove a stamp.
  defp await_driver_clock(server) do
    assert Enum.any?(1..200, fn _ ->
             if Native.driver_stats(server).now_ms > 0,
               do: true,
               else:
                 (
                   Process.sleep(5)
                   false
                 )
           end),
           "the driver clock never advanced"
  end

  defp relay(pid) do
    receive do
      message -> send(pid, {:relayed, message})
    end

    relay(pid)
  end

  defp await_stream_row(server, stream_id, want) do
    Enum.find_value(1..200, fn _ ->
      row =
        server
        |> Native.driver_stats()
        |> Map.fetch!(:streams)
        |> Enum.find(&(&1.stream_id == stream_id))

      if want.(row),
        do: row || :gone,
        else:
          (
            Process.sleep(10)
            nil
          )
    end)
  end

  for transport <- [:uds, :quic] do
    describe "#{transport}" do
      @describetag transport: transport

      test "a frame crosses in both directions", %{transport: transport} do
        {_server, dial} = start_server(transport)
        {client, server_handle} = connect(transport, dial)
        :ok = Native.set_active(server_handle)
        client_id = Client.stream_id(client)
        server_id = Native.stream_id(server_handle)

        :ok = Client.send_frame(client, "p-up", "up", %{"k" => "v"})
        assert_receive {:flz_data, ^server_id, "p-up", "up", %{"k" => "v"}}, 5_000

        :ok = Native.send_frame(server_handle, "p-down", "down", %{})
        assert_receive {:flz_data, ^client_id, "p-down", "down", %{}}, 5_000
      end

      test "the driver self-report carries what this driver did", %{transport: transport} do
        {server, dial} = start_server(transport)
        await_driver_clock(server)
        {client, server_handle} = connect(transport, dial)
        :ok = Native.set_active(server_handle)
        server_id = Native.stream_id(server_handle)

        :ok = Client.send_frame(client, "p-up", "up", %{})
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
           %{transport: transport} do
        {server, dial} = start_server(transport)
        {client, server_handle} = connect(transport, dial)
        server_id = Native.stream_id(server_handle)
        test_pid = self()

        opening = if transport == :quic, do: ["p-open"], else: []
        sent = opening ++ ["p-1", "p-2", "p-3"]
        for id <- sent -- opening, do: :ok = Client.send_frame(client, id, "", %{})

        # Every frame is read before the stream is activated, so which ones reach
        # an owner is decided by the mode and not by timing.
        read_all = &(&1 != nil and &1.frames_read == length(sent))

        assert await_stream_row(server, server_id, read_all),
               "the driver never read the #{length(sent)} frames sent"

        [first | held] = sent
        :ok = Native.set_active_once(server_handle)
        assert_receive {:flz_data, ^server_id, ^first, _, _}, 5_000

        next_owner = spawn(fn -> relay(test_pid) end)
        on_exit(fn -> Process.exit(next_owner, :kill) end)
        :ok = Native.controlling_process(server_handle, next_owner)
        :ok = Native.set_active(server_handle)

        for id <- held, do: assert_receive({:relayed, {:flz_data, ^server_id, ^id, _, _}}, 5_000)
        refute_received {:flz_data, ^server_id, _, _, _}
      end

      test "closing a stream releases it", %{transport: transport} do
        {server, dial} = start_server(transport)
        {_client, server_handle} = connect(transport, dial)
        :ok = Native.set_active(server_handle)
        server_id = Native.stream_id(server_handle)

        assert await_stream_row(server, server_id, &is_map/1),
               "the driver never reported the stream it announced"

        :ok = Native.close(server_handle)

        assert await_stream_row(server, server_id, &is_nil/1) == :gone,
               "a closed stream must leave the self-report"
      end

      test "a peer that dies is reported with a declared word", %{transport: transport} do
        {_server, dial} = start_server(transport)
        test_pid = self()

        # The peer holds its handles on its own heap, so killing it frees them
        # at a moment the test chooses rather than at the collector's.
        peer =
          spawn(fn ->
            handles = open_peer(transport, dial)
            send(test_pid, :peer_up)
            receive do: (:never -> :ok)
            Enum.each(handles, &Client.stream_id/1)
          end)

        assert_receive :peer_up, 10_000
        assert_receive {:flz_connection, server_handle}, 5_000
        server_id = Native.stream_id(server_handle)

        Process.exit(peer, :kill)

        assert_receive {:flz_closed, ^server_id, reason}, 10_000

        assert declared?(reason),
               "#{transport} closed with #{inspect(reason)}, which the driver declares nowhere"

        expected =
          unquote(if transport == :uds, do: "connection_closed", else: "connection_lost: ")

        assert String.starts_with?(reason, expected),
               "#{transport} reported #{inspect(reason)}, not #{expected}"
      end
    end
  end

  describe "one self-report, two transports" do
    test "a UDS row and a QUIC row carry the same fields" do
      uds = one_row(:uds)
      quic = one_row(:quic)

      assert Map.keys(uds) == Map.keys(quic),
             "the transports report different fields, so one driver counts what the other does not"
    end

    defp one_row(transport) do
      {server, dial} = start_server(transport)
      {client, server_handle} = connect(transport, dial)
      :ok = Native.set_active(server_handle)
      server_id = Native.stream_id(server_handle)

      :ok = Client.send_frame(client, "p-up", "up", %{})
      assert_receive {:flz_data, ^server_id, "p-up", _, _}, 5_000

      row =
        await_stream_row(server, server_id, fn
          nil -> false
          row -> row.frames_read > 0
        end)

      assert is_map(row), "#{transport} reported no row"
      row
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
      {_server, path} = start_server(:uds)
      {socket, server_id} = raw_peer(path)

      # 36 bytes: a 32-byte payload id, then a big-endian payload size the
      # driver refuses BEFORE it allocates for it.
      header = String.pad_trailing("too-big", 32) <> <<1_000_000_000::big-unsigned-32>>
      :ok = :gen_tcp.send(socket, header)

      assert_receive {:flz_closed, ^server_id, "frame_too_large"}, 5_000
      :gen_tcp.close(socket)
    end

    test "a peer that ends the stream is connection_closed" do
      {_server, path} = start_server(:uds)
      {socket, server_id} = raw_peer(path)

      :ok = :gen_tcp.close(socket)

      assert_receive {:flz_closed, ^server_id, "connection_closed"}, 5_000
    end
  end
end
