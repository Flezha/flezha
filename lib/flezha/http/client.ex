defmodule Flezha.Http.Client do
  @moduledoc """
  One HTTP/1.1 request/response over `:mint`, for the places flezha talks to an
  external HTTP service directly rather than through a provider plugin — the
  OTLP exporter and the remote durability tier's S3 backend.

  `request/7` sends on a caller-owned connection and drains the response to
  completion, returning status, headers and body. The connection stays the
  caller's: the OTLP exporter keeps one alive across posts and reconnects on
  disconnect, while the S3 backend opens and closes around each call. That
  difference is the whole reason the connection is a parameter — everything
  else about driving a Mint exchange is identical, and lived in two copies
  before this.

  Responses are drained fully even when the caller only wants the status: a
  half-drained connection cannot be reused, and the exporter's is.
  """

  @type conn :: Mint.HTTP.t()
  @type header :: {binary(), binary()}

  @doc """
  Open a connection. A thin pass-through, here so a caller needs only this
  module rather than `:mint` directly.
  """
  @spec connect(:http | :https, binary(), :inet.port_number()) ::
          {:ok, conn()} | {:error, term()}
  def connect(scheme, host, port), do: Mint.HTTP.connect(scheme, host, port)

  @spec close(conn()) :: :ok
  def close(conn) do
    _ = Mint.HTTP.close(conn)
    :ok
  end

  @spec open?(conn() | nil) :: boolean()
  def open?(nil), do: false
  def open?(conn), do: Mint.HTTP.open?(conn)

  @doc """
  Send one request on `conn` and return its complete response.

  The connection is returned in both the success and the failure shape so the
  caller can decide whether to keep or discard it — a failed exchange usually
  means the connection is unusable, which only the caller knows how to handle.
  """
  @spec request(conn(), binary(), binary(), [header()], iodata(), timeout()) ::
          {:ok, conn(), non_neg_integer(), [header()], binary()}
          | {:error, term(), conn()}
  def request(conn, method, path, headers, body, timeout) do
    case Mint.HTTP.request(conn, method, path, headers, body) do
      {:ok, conn, ref} ->
        recv(conn, ref, System.monotonic_time(:millisecond) + timeout, nil, [], [])

      {:error, conn, reason} ->
        {:error, reason, conn}
    end
  end

  defp recv(conn, ref, deadline, status, headers, body) do
    timeout = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      message ->
        case Mint.HTTP.stream(conn, message) do
          {:ok, conn, responses} ->
            case fold(responses, ref, status, headers, body) do
              {:done, status, headers, body} ->
                {:ok, conn, status, headers, IO.iodata_to_binary(body)}

              {:cont, status, headers, body} ->
                recv(conn, ref, deadline, status, headers, body)
            end

          {:error, conn, reason, _responses} ->
            {:error, reason, conn}

          # A message that is not this connection's — keep waiting.
          :unknown ->
            recv(conn, ref, deadline, status, headers, body)
        end
    after
      timeout -> {:error, :timeout, conn}
    end
  end

  defp fold([], _ref, status, headers, body), do: {:cont, status, headers, body}

  defp fold([{:status, ref, code} | rest], ref, _status, headers, body),
    do: fold(rest, ref, code, headers, body)

  defp fold([{:headers, ref, h} | rest], ref, status, headers, body),
    do: fold(rest, ref, status, headers ++ h, body)

  defp fold([{:data, ref, chunk} | rest], ref, status, headers, body),
    do: fold(rest, ref, status, headers, body ++ [chunk])

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
