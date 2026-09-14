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
