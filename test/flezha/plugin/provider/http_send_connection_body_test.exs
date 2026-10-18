defmodule FlezhaTest.Plugin.Provider.HttpSendConnectionBodyTest do
  @moduledoc """
  An HTTP sender uses only the status of a response, so a 2xx with a large
  body must neither be held in memory while it is read nor read as a failure,
  and the connection it abandoned must not be reused.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Provider.HttpSend.Connection
  alias FlezhaTest.Helper.WebhookServer

  @body_bytes 50 * 1024 * 1024
  @chunk :binary.copy("x", 64 * 1024)

  test "a 200 with a 50 MiB body answers its status without holding the body" do
    test = self()
    server = WebhookServer.start([&send(test, {:streamed, stream_body(&1)}), 204])

    endpoint = %{scheme: :http, host: "127.0.0.1", port: server.port, target: "/hook"}
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
    assert_receive {:streamed, sent}, 30_000
    assert sent < @body_bytes
    assert WebhookServer.connections(server) == 2

    Connection.stop(conn)
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

  # The first request is answered with the large body, streamed until the
  # client stops reading it; the second must arrive on a new connection.
  defp stream_body(socket) do
    :ok =
      :gen_tcp.send(
        socket,
        "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: #{@body_bytes}\r\n\r\n"
      )

    stream_body(socket, 0)
  end

  defp stream_body(_socket, sent) when sent >= @body_bytes, do: sent

  defp stream_body(socket, sent) do
    case :gen_tcp.send(socket, @chunk) do
      :ok -> stream_body(socket, sent + byte_size(@chunk))
      {:error, _closed} -> sent
    end
  end
end
