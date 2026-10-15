defmodule Flezha.Http.ClientTest do
  @moduledoc """
  One exchange over `Flezha.Http.Client` against a listener in the test. Its
  callers are processes whose calls, casts and timers keep arriving while a
  request is in flight, so the exchange reads only its own connection's
  messages and leaves every other one where it was.
  """

  use ExUnit.Case, async: true

  alias Flezha.Http.Client

  test "a message for the caller that arrives during a request stays in its mailbox" do
    port =
      serve(fn socket, test ->
        # Delivered before the response is written, so a loop reading every
        # message meets it first.
        send(test, :unrelated)
        :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
      end)

    {:ok, conn} = Client.connect(:http, "127.0.0.1", port)
    assert {:ok, conn, 200, _headers, "ok"} = Client.request(conn, "GET", "/", [], "", 5_000)
    Client.close(conn)

    assert_received :unrelated
  end

  test "a connection closed before a response is a failure, not a wait" do
    port = serve(fn _socket, _test -> :ok end)

    {:ok, conn} = Client.connect(:http, "127.0.0.1", port)
    assert {:error, _reason, _conn} = Client.request(conn, "GET", "/", [], "", 5_000)
  end

  describe "max_body_bytes" do
    test "an integer abandons the exchange once the body passes it" do
      port = serve(&large_body/2)

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)

      assert {:error, {:body_too_large, bytes}, conn} =
               Client.request(conn, "GET", "/", [], "", 5_000, max_body_bytes: 100)

      assert bytes > 100
      Client.close(conn)
    end

    test "{n, :close} answers the status once the body passes n and closes the connection" do
      port = serve(&large_body/2)

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)

      assert {:ok, conn, 200, headers, body} =
               Client.request(conn, "GET", "/", [], "", 5_000, max_body_bytes: {100, :close})

      assert Client.header(headers, "content-length") == "1000000"
      assert byte_size(body) > 100 and byte_size(body) < 1_000_000
      refute Client.open?(conn)
      refute_received {:tcp, _socket, _data}
    end

    test "{n, :close} keeps a connection whose body stays within n" do
      port =
        serve(fn socket, _test ->
          :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
        end)

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)

      assert {:ok, conn, 200, _headers, "ok"} =
               Client.request(conn, "GET", "/", [], "", 5_000, max_body_bytes: {100, :close})

      assert Client.open?(conn)
      Client.close(conn)
    end

    test "{n, :close} keeps a connection whose complete body passes n in one read" do
      body = :binary.copy("x", 10 * 1024)
      response = "HTTP/1.1 200 OK\r\ncontent-length: #{byte_size(body)}\r\n\r\n" <> body
      port = serve_keep_alive(2, response)

      {:ok, conn} = Client.connect(:http, "127.0.0.1", port)
      opts = [max_body_bytes: {8 * 1024, :close}]

      assert {:ok, conn, 200, _headers, ^body} =
               Client.request(conn, "GET", "/", [], "", 5_000, opts)

      assert Client.open?(conn)

      assert {:ok, conn, 200, _headers, ^body} =
               Client.request(conn, "GET", "/", [], "", 5_000, opts)

      Client.close(conn)
    end
  end

  # One connection that answers `count` requests with `response`, each written
  # in a single send, so a second request succeeds only on the same connection.
  defp serve_keep_alive(count, response) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listener)

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listener)

      for _ <- 1..count do
        {:ok, _request} = :gen_tcp.recv(socket, 0)
        :ok = :gen_tcp.send(socket, response)
      end

      :gen_tcp.close(socket)
    end)

    port
  end

  # A 1 000 000-byte body, written in pieces so the client meets the cap
  # before the whole body has arrived.
  defp large_body(socket, _test) do
    :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\ncontent-length: 1000000\r\n\r\n")
    for _ <- 1..100, do: :gen_tcp.send(socket, :binary.copy("x", 10_000))
  end

  # A one-request listener: it reads the request, runs `respond`, then closes.
  defp serve(respond) do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false])
    {:ok, port} = :inet.port(listener)
    test = self()

    spawn_link(fn ->
      {:ok, socket} = :gen_tcp.accept(listener)
      {:ok, _request} = :gen_tcp.recv(socket, 0)
      respond.(socket, test)
      :gen_tcp.close(socket)
    end)

    port
  end
end
