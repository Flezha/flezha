defmodule Flezha.Http.ClientTest do
  @moduledoc """
  One exchange over `Flezha.Http.Client` against a listener in the test. Its
  callers are processes whose calls, casts and timers keep arriving while a
  request is in flight, so the exchange reads only its own connection's
  messages and leaves every other one where it was.
  """

  use ExUnit.Case, async: true

  alias Flezha.Http.Client
  alias FlezhaTest.Helper.WebhookServer

  test "a message for the caller that arrives during a request stays in its mailbox" do
    # The server reports the request to this process before it writes the
    # response, so a loop reading every message meets that report first.
    %{port: port} = WebhookServer.start([{200, [], "ok"}])

    {:ok, conn} = Client.connect(:http, "127.0.0.1", port)
    assert {:ok, conn, 200, _headers, "ok"} = Client.request(conn, "GET", "/", [], "", 5_000)
    Client.close(conn)

    assert_received {:request, "GET / HTTP/1.1", _headers, ""}
  end

  test "a connection closed before a response is a failure, not a wait" do
    %{port: port} = WebhookServer.start([:close])

    {:ok, conn} = Client.connect(:http, "127.0.0.1", port)
    assert {:error, _reason, _conn} = Client.request(conn, "GET", "/", [], "", 5_000)
  end

  describe "max_body_bytes" do
    test "an integer abandons the exchange once the body passes it" do
      %{port: port} = WebhookServer.start([&large_body/1])

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)

      assert {:error, {:body_too_large, bytes}, conn} =
               Client.request(conn, "GET", "/", [], "", 5_000, max_body_bytes: 100)

      assert bytes > 100
      Client.close(conn)
    end

    test "{n, :close} answers the status once the body passes n and closes the connection" do
      %{port: port} = WebhookServer.start([&large_body/1])

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)

      assert {:ok, conn, 200, headers, body} =
               Client.request(conn, "GET", "/", [], "", 5_000, max_body_bytes: {100, :close})

      assert Client.header(headers, "content-length") == "1000000"
      assert byte_size(body) > 100 and byte_size(body) < 1_000_000
      refute Client.open?(conn)
      refute_received {:tcp, _socket, _data}
    end

    test "{n, :close} keeps a connection whose body stays within n" do
      %{port: port} = WebhookServer.start([{200, [], "ok"}])

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)

      assert {:ok, conn, 200, _headers, "ok"} =
               Client.request(conn, "GET", "/", [], "", 5_000, max_body_bytes: {100, :close})

      assert Client.open?(conn)
      Client.close(conn)
    end

    test "{n, :close} keeps a connection whose complete body passes n in one read" do
      body = :binary.copy("x", 10 * 1024)
      # Each response is written in a single send.
      %{port: port} = server = WebhookServer.start([{200, [], body}])

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)
      opts = [max_body_bytes: {8 * 1024, :close}]

      assert {:ok, conn, 200, _headers, ^body} =
               Client.request(conn, "GET", "/", [], "", 5_000, opts)

      assert Client.open?(conn)

      assert {:ok, conn, 200, _headers, ^body} =
               Client.request(conn, "GET", "/", [], "", 5_000, opts)

      assert WebhookServer.connections(server) == 1
      Client.close(conn)
    end
  end

  # A 1 000 000-byte body, written in pieces so the client meets the cap
  # before the whole body has arrived.
  defp large_body(socket) do
    :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 1000000\r\n\r\n")
    for _ <- 1..100, do: :gen_tcp.send(socket, :binary.copy("x", 10_000))
  end
end
