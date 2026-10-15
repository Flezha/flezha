defmodule Flezha.Http.Client do
  @moduledoc """
  One HTTP/1.1 request/response over `:mint`, for the places flezha talks to an
  external HTTP service directly rather than through a provider plugin — the
  OTLP exporter and the S3 client, `Flezha.Http.S3`.

  `request/7` sends on a caller-owned connection and drains the response to
  completion, returning status, headers and body. The connection stays the
  caller's: the OTLP exporter keeps one alive across posts and reconnects on
  disconnect, while the S3 client opens and closes one around each call. That
  difference is the whole reason the connection is a parameter — everything
  else about driving a Mint exchange is identical, and lived in two copies
  before this.

  Responses are drained fully, since a half-drained connection cannot be
  reused, unless `max_body_bytes` caps the body. Only the
  connection's own socket messages are read while a request is in flight: every
  caller is a process whose calls, casts and timers keep arriving, and a
  message taken here would be lost to it.
  """

  @type conn :: Mint.HTTP.t()
  @type header :: {binary(), binary()}

  # The tags of the messages a connection's socket sends its owner.
  @socket_message [:tcp, :ssl, :tcp_error, :ssl_error]
  @socket_closed [:tcp_closed, :ssl_closed]

  @doc """
  Open a connection. A thin pass-through, here so a caller needs only this
  module rather than `:mint` directly. `transport_opts` reach the socket, as a
  caller's own CA store and peer verification do.
  """
  @spec connect(:http | :https, binary(), :inet.port_number(), keyword()) ::
          {:ok, conn()} | {:error, term()}
  def connect(scheme, host, port, transport_opts \\ []),
    do: Mint.HTTP.connect(scheme, host, port, transport_opts: transport_opts)

  @doc "Close `conn`; a connection never opened (`nil`) closes as well."
  @spec close(conn() | nil) :: :ok
  def close(nil), do: :ok

  def close(conn) do
    _ = Mint.HTTP.close(conn)
    :ok
  end

  @doc """
  `conn` while it is open, or a new connection: the one way a caller that keeps
  a connection across requests reopens it after a disconnect.
  """
  @spec ensure_open(conn() | nil, :http | :https, binary(), :inet.port_number(), keyword()) ::
          {:ok, conn()} | {:error, term()}
  def ensure_open(conn, scheme, host, port, transport_opts \\ []) do
    if open?(conn), do: {:ok, conn}, else: connect(scheme, host, port, transport_opts)
  end

  @doc "The request target of `uri`: its path, `/` when it has none, and its query."
  @spec target(URI.t()) :: binary()
  def target(%URI{path: path, query: query}) do
    path = if path in [nil, ""], do: "/", else: path
    if query, do: path <> "?" <> query, else: path
  end

  @spec open?(conn() | nil) :: boolean()
  def open?(nil), do: false
  def open?(conn), do: Mint.HTTP.open?(conn)

  @doc """
  Send one request on `conn` and return its complete response.

  The connection is returned in both the success and the failure shape so the
  caller can decide whether to keep or discard it — a failed exchange usually
  means the connection is unusable, which only the caller knows how to handle.

  With `max_body_bytes: n` the exchange is abandoned, `{:error, {:body_too_large,
  bytes}, conn}`, as soon as the response body passes `n` bytes. With
  `max_body_bytes: {n, :close}`, for a caller that uses only the status, an
  unfinished body past `n` answers `{:ok, conn, status, headers,
  body_read_so_far}` instead and closes `conn`, which `ensure_open/5` then
  replaces; a response complete in the read that passed `n` keeps it.
  """
  @spec request(conn(), binary(), binary(), [header()], iodata(), timeout(), keyword()) ::
          {:ok, conn(), non_neg_integer(), [header()], binary()}
          | {:error, term(), conn()}
  def request(conn, method, path, headers, body, timeout, opts \\ []) do
    case Mint.HTTP.request(conn, method, path, headers, body) do
      {:ok, conn, ref} ->
        deadline = System.monotonic_time(:millisecond) + timeout
        {max, past_max} = max_body(opts[:max_body_bytes])
        recv(conn, {ref, deadline, max, past_max}, nil, [], "")

      {:error, conn, reason} ->
        {:error, reason, conn}
    end
  end

  defp recv(conn, {_ref, deadline, _max, _past_max} = exchange, status, headers, body) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)
    socket = Mint.HTTP.get_socket(conn)

    receive do
      {tag, ^socket, _data_or_reason} = message when tag in @socket_message ->
        stream(conn, message, exchange, status, headers, body)

      {tag, ^socket} = message when tag in @socket_closed ->
        stream(conn, message, exchange, status, headers, body)
    after
      timeout -> {:error, :timeout, conn}
    end
  end

  defp stream(conn, message, {ref, _deadline, max, past_max} = exchange, status, headers, body) do
    case Mint.HTTP.stream(conn, message) do
      {:ok, conn, responses} ->
        case fold(responses, ref, status, headers, body) do
          {read, status, headers, body}
          when max != nil and byte_size(body) > max and (read == :cont or past_max == :error) ->
            past_max(past_max, conn, status, headers, body)

          {:done, status, headers, body} ->
            {:ok, conn, status, headers, body}

          {:cont, status, headers, body} ->
            recv(conn, exchange, status, headers, body)
        end

      {:error, conn, reason, _responses} ->
        {:error, reason, conn}
    end
  end

  defp max_body({max, :close}), do: {max, :close}
  defp max_body(max), do: {max, :error}

  defp past_max(:error, conn, _status, _headers, body),
    do: {:error, {:body_too_large, byte_size(body)}, conn}

  # The rest of the body is never read, so the connection cannot carry another
  # request; whatever its socket delivered before the close is dropped with it.
  # Only that socket's messages are taken, and a closed socket sends no more.
  defp past_max(:close, conn, status, headers, body) do
    socket = Mint.HTTP.get_socket(conn)
    {:ok, conn} = Mint.HTTP.close(conn)
    flush(socket)
    {:ok, conn, status, headers, body}
  end

  defp flush(socket) do
    receive do
      {tag, ^socket, _data_or_reason} when tag in @socket_message -> flush(socket)
      {tag, ^socket} when tag in @socket_closed -> flush(socket)
    after
      0 -> :ok
    end
  end

  defp fold([], _ref, status, headers, body), do: {:cont, status, headers, body}

  defp fold([{:status, ref, code} | rest], ref, _status, headers, body),
    do: fold(rest, ref, code, headers, body)

  defp fold([{:headers, ref, h} | rest], ref, status, headers, body),
    do: fold(rest, ref, status, headers ++ h, body)

  defp fold([{:data, ref, chunk} | rest], ref, status, headers, body),
    do: fold(rest, ref, status, headers, body <> chunk)

  defp fold([{:done, ref} | _rest], ref, status, headers, body),
    do: {:done, status || 0, headers, body}

  defp fold([_other | rest], ref, status, headers, body),
    do: fold(rest, ref, status, headers, body)

  @doc "The value of `name` (lower-cased) in a response header list, or `nil`."
  @spec header([header()], binary()) :: binary() | nil
  def header(headers, name) do
    Enum.find_value(headers, fn {k, v} -> if String.downcase(k) == name, do: v end)
  end
end
