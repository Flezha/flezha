defmodule Flezha.Http.Listener do
  @moduledoc """
  Shared hand-rolled `:gen_tcp` HTTP/1.1 plumbing for the small internal
  endpoints (`/metrics`, `/healthz`). Owns the listen socket, the accept
  loop, request-line parsing with idle-timeout hygiene, and response
  writing. No Cowboy/Plug dependency.

  An endpoint is `use Flezha.Http.Listener, port: :prometheus_port, bind:
  :prometheus_bind`, the two `Flezha.Config` keys it listens on. That makes it
  a named GenServer with `start_link/1` and `listen_port/0`; it writes only
  `route(socket, path)` for the `GET` paths it serves, answering `:not_found`
  for any other. A `GET` it does not route answers 404, another method 405 and
  an unreadable request 400.
  """

  alias Flezha.Config
  alias Flezha.Config.Durations
  alias Flezha.SDK.SafeCall

  @max_request_line_bytes 8_192

  @type route :: (:gen_tcp.socket(), binary() -> :not_found | term())

  defmacro __using__(opts) do
    port = Keyword.fetch!(opts, :port)
    bind = Keyword.fetch!(opts, :bind)

    quote do
      use GenServer

      def start_link(opts \\ []), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

      @doc "Return the listening port — useful for tests that bind to port 0."
      @spec listen_port() :: {:ok, :inet.port_number()} | :error
      def listen_port(), do: Flezha.Http.Listener.listen_port(__MODULE__)

      @impl true
      def init(_opts), do: Flezha.Http.Listener.open(unquote(port), unquote(bind))

      @impl true
      def handle_call(:listen_port, _from, %{port: port} = state),
        do: {:reply, {:ok, port}, state}

      @impl true
      def handle_info(:accept_loop, %{listen_socket: socket} = state) do
        Flezha.Http.Listener.accept(socket, &route/2)
        {:noreply, state}
      end

      def handle_info(_msg, state), do: {:noreply, state}

      @impl true
      def terminate(_reason, %{listen_socket: socket}) do
        _ = :gen_tcp.close(socket)
        :ok
      end
    end
  end

  @doc false
  @spec listen_port(atom()) :: {:ok, :inet.port_number()} | :error
  def listen_port(server) do
    case Process.whereis(server) do
      pid when is_pid(pid) ->
        SafeCall.bounded(pid, :listen_port, Durations.get(:listen_port_call_ms), on_error: :error)

      _ ->
        :error
    end
  end

  @doc false
  @spec open(atom(), atom()) :: {:ok, map()} | {:stop, {:listen_failed, term()}}
  def open(port_key, bind_key) do
    case listen(Config.get(port_key), Config.get(bind_key)) do
      {:ok, socket, port} ->
        send(self(), :accept_loop)
        {:ok, %{listen_socket: socket, port: port}}

      {:error, reason} ->
        {:stop, {:listen_failed, reason}}
    end
  end

  @doc false
  @spec accept(:gen_tcp.socket(), route()) :: pid()
  def accept(socket, route) do
    parent = self()
    spawn(fn -> accept_loop(parent, socket, route) end)
  end

  # Opens the listen socket, returning the bound port (config may pass 0).
  defp listen(port, bind) do
    {:ok, ip} = :inet.parse_address(String.to_charlist(bind))

    listen_opts = [:binary, packet: :raw, active: false, reuseaddr: true, ip: ip, backlog: 128]

    with {:ok, socket} <- :gen_tcp.listen(port, listen_opts),
         {:ok, actual_port} <- :inet.port(socket) do
      {:ok, socket, actual_port}
    end
  end

  # Accepts connections until the listen socket closes or `parent` dies; each
  # connection is served in its own process.
  defp accept_loop(parent, listen_socket, route) do
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
        respond(socket, parse_request_line(line), route)

      {:error, :timeout} ->
        :ok

      {:error, _} ->
        _ = send_response(socket, 400, "text/plain", "bad request\n")
    end

    :gen_tcp.close(socket)
  end

  defp respond(socket, {:get, path}, route) do
    if route.(socket, path) == :not_found,
      do: send_response(socket, 404, "text/plain", "not found\n")
  end

  defp respond(socket, {:method_not_allowed, _path}, _route),
    do: send_response(socket, 405, "text/plain", "method not allowed\n")

  defp respond(socket, :bad, _route),
    do: send_response(socket, 400, "text/plain", "bad request\n")

  defp read_request_line(socket) do
    :inet.setopts(socket, packet: :line)

    case :gen_tcp.recv(socket, 0, Durations.get(:http_listener_idle_ms)) do
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
