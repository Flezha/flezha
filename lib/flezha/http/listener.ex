defmodule Flezha.Http.Listener do
  @moduledoc """
  Shared hand-rolled `:gen_tcp` HTTP/1.1 plumbing for the small internal
  endpoints (`/metrics`, `/healthz`). Owns the listen socket, the accept
  loop, request-line parsing with idle-timeout hygiene, and response
  writing. Each endpoint GenServer supplies a `route/2` that maps a parsed
  request to a response. No Cowboy/Plug dependency.
  """

  @idle_timeout_ms 5_000
  @max_request_line_bytes 8_192

  @type request :: {:get, binary()} | {:method_not_allowed, binary()} | :bad

  @doc "Open the listen socket, returning the bound port (config may pass 0)."
  @spec listen(:inet.port_number(), binary()) ::
          {:ok, :gen_tcp.socket(), :inet.port_number()} | {:error, term()}
  def listen(port, bind) do
    {:ok, ip} = :inet.parse_address(String.to_charlist(bind))

    listen_opts = [:binary, packet: :raw, active: false, reuseaddr: true, ip: ip, backlog: 128]

    with {:ok, socket} <- :gen_tcp.listen(port, listen_opts),
         {:ok, actual_port} <- :inet.port(socket) do
      {:ok, socket, actual_port}
    end
  end

  @doc """
  Accept connections until the listen socket closes or `parent` dies. Each
  connection is served in its own process; `route` maps the parsed request
  to a response.
  """
  @spec accept_loop(pid(), :gen_tcp.socket(), (:gen_tcp.socket(), request() -> any())) :: :ok
  def accept_loop(parent, listen_socket, route) when is_function(route, 2) do
    case :gen_tcp.accept(listen_socket, 1_000) do
      {:ok, socket} ->
        spawn(fn ->
          try do
            serve(socket, route)
          catch
            _, _ -> :gen_tcp.close(socket)
          end
        end)

        accept_loop(parent, listen_socket, route)

      {:error, :timeout} ->
        if Process.alive?(parent), do: accept_loop(parent, listen_socket, route), else: :ok

      {:error, _} ->
        :ok
    end
  end

  @spec send_response(:gen_tcp.socket(), 200..599, binary(), iodata()) :: :ok
  def send_response(socket, status, content_type, body) do
    body_bin = IO.iodata_to_binary(body)

    response = [
      "HTTP/1.1 ",
      Integer.to_string(status),
      " ",
      status_text(status),
      "\r\nContent-Type: ",
      content_type,
      "\r\nContent-Length: ",
      Integer.to_string(byte_size(body_bin)),
      "\r\nConnection: close\r\n\r\n",
      body_bin
    ]

    _ = :gen_tcp.send(socket, response)
    :ok
  end

  # ---------- internals ----------

  defp serve(socket, route) do
    case read_request_line(socket) do
      {:ok, line} ->
        _ = drain_headers(socket, 5)
        route.(socket, parse_request_line(line))

      {:error, :timeout} ->
        :ok

      {:error, _} ->
        _ = send_response(socket, 400, "text/plain", "bad request\n")
    end

    :gen_tcp.close(socket)
  end

  defp read_request_line(socket) do
    :inet.setopts(socket, packet: :line)

    case :gen_tcp.recv(socket, 0, @idle_timeout_ms) do
      {:ok, line} when byte_size(line) > @max_request_line_bytes -> {:error, :too_long}
      {:ok, line} -> {:ok, line}
      err -> err
    end
  end

  defp drain_headers(socket, attempts) when attempts > 0 do
    case :gen_tcp.recv(socket, 0, 500) do
      {:ok, "\r\n"} -> :ok
      {:ok, "\n"} -> :ok
      {:ok, _other} -> drain_headers(socket, attempts - 1)
      _ -> :ok
    end
  end

  defp drain_headers(_socket, _), do: :ok

  defp parse_request_line(line) do
    case String.split(String.trim(line)) do
      [method, path, "HTTP/" <> _] ->
        case String.upcase(method) do
          "GET" -> {:get, path}
          _ -> {:method_not_allowed, path}
        end

      _ ->
        :bad
    end
  end

  defp status_text(200), do: "OK"
  defp status_text(400), do: "Bad Request"
  defp status_text(404), do: "Not Found"
  defp status_text(405), do: "Method Not Allowed"
  defp status_text(503), do: "Service Unavailable"
  defp status_text(_), do: "OK"
end
