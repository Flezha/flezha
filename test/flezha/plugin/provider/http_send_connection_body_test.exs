defmodule FlezhaTest.Plugin.Provider.HttpSendConnectionBodyTest do
  @moduledoc """
  An HTTP sender uses only the status of a response, so a 2xx with a large
  body must neither be held in memory while it is read nor read as a failure,
  and the connection it abandoned must not be reused.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.HttpSend.Connection

  @body_bytes 50 * 1024 * 1024
  @chunk :binary.copy("x", 64 * 1024)

  test "a 200 with a 50 MiB body answers its status without holding the body" do
    {:ok, listen} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true])
    {:ok, port} = :inet.port(listen)
    server = Task.async(fn -> serve(listen) end)

    endpoint = %{scheme: :http, host: "127.0.0.1", port: port, target: "/hook"}
    conn = Connection.start_link(endpoint, 30_000)
    sampler = Task.async(fn -> sample(conn, 0) end)

    :ok = Connection.request(conn, {"POST", [{"content-type", "application/json"}], "{}"})
    assert_receive {:response, ^conn, result}, 30_000
    send(sampler.pid, :stop)
    peak = Task.await(sampler)

    assert {:ok, 200, _headers} = result
    assert peak < 1024 * 1024, "the connection held #{peak} bytes of a body it never uses"
    assert {:message_queue_len, 0} = Process.info(conn, :message_queue_len)

    :ok = Connection.request(conn, {"POST", [], "{}"})
    assert_receive {:response, ^conn, {:ok, 204, _headers}}, 10_000
    assert {sent, :second_connection} = Task.await(server, 30_000)
    assert sent < @body_bytes

    Connection.stop(conn)
    :gen_tcp.close(listen)
  end

  # Peak bytes of binaries the connection process references, sampled until told to stop.
  defp sample(pid, peak) do
    receive do
      :stop -> peak
    after
      1 ->
        held =
          case Process.info(pid, :binary) do
            {:binary, bins} -> bins |> Enum.map(&elem(&1, 1)) |> Enum.sum()
            nil -> 0
          end

        sample(pid, max(peak, held))
    end
  end

  # The first connection streams the large body until the client stops reading
  # it; the second request must arrive on a new connection.
  defp serve(listen) do
    first = accept_request(listen)

    :ok =
      :gen_tcp.send(
        first,
        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: #{@body_bytes}\r\n\r\n"
      )

    sent = stream_body(first, 0)
    second = accept_request(listen)
    :ok = :gen_tcp.send(second, "HTTP/1.1 204 No Content\r\n\r\n")
    :gen_tcp.close(first)
    :gen_tcp.close(second)
    {sent, :second_connection}
  end

  defp accept_request(listen) do
    {:ok, socket} = :gen_tcp.accept(listen, 10_000)
    {:ok, _request} = :gen_tcp.recv(socket, 0, 10_000)
    socket
  end

  defp stream_body(_socket, sent) when sent >= @body_bytes, do: sent

  defp stream_body(socket, sent) do
    case :gen_tcp.send(socket, @chunk) do
      :ok -> stream_body(socket, sent + byte_size(@chunk))
      {:error, _closed} -> sent
    end
  end
end
