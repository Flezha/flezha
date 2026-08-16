defmodule Flezha.Observability.Export.OtlpClient do
  @moduledoc """
  HTTP/1.1 client wrapping `:mint` for OTLP/HTTP POSTs.

  Maintains a single connection to the configured `:otlp_endpoint`;
  reconnects on disconnect. Supports plain HTTP and TLS (verified
  against the system CA store).

  Applies bounded exponential backoff on transient failures
  (`5xx`, `:econnrefused`, `:timeout`) per proposal D8: 250 ms, 500,
  1_000, 2_000, 4_000 (cap), max 5 attempts. Sets
  `Content-Type: application/x-protobuf` on every request and
  injects any configured `:otlp_headers`.
  """

  use GenServer

  @backoffs_ms [250, 500, 1_000, 2_000, 4_000]
  @max_attempts 5
  @request_timeout_ms 10_000

  alias Flezha.Http.Client, as: HttpClient

  # ---------- Public API ----------

  def child_spec(opts) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [opts]},
      type: :worker,
      restart: :permanent
    }
  end

  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  POST an OTLP/HTTP request to the configured endpoint's signal path.
  `kind` is `:metrics | :logs | :traces`. Returns `{:ok, status}` or
  `{:error, reason}` after exhausting the backoff schedule.
  """
  @spec post(:metrics | :logs | :traces, iodata()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def post(kind, body) do
    case Process.whereis(__MODULE__) do
      pid when is_pid(pid) ->
        GenServer.call(pid, {:post, kind, IO.iodata_to_binary(body)}, 60_000)

      _ ->
        {:error, :client_not_running}
    end
  end

  @doc false
  @spec backoff_schedule() :: [pos_integer()]
  def backoff_schedule(), do: @backoffs_ms

  # ---------- GenServer ----------

  @impl true
  def init(_opts) do
    {:ok,
     %{
       conn: nil,
       endpoint: load_endpoint(),
       headers: load_headers()
     }}
  end

  @impl true
  def handle_call({:post, kind, body}, _from, state) do
    {result, state} = do_post_with_retry(state, kind, body, 0)
    {:reply, result, state}
  end

  defp do_post_with_retry(state, kind, body, attempts) when attempts < @max_attempts do
    case attempt_post(state, kind, body) do
      {{:ok, status}, state} when status < 500 ->
        {{:ok, status}, state}

      {{:ok, _status}, state} ->
        backoff(attempts)
        do_post_with_retry(close_conn(state), kind, body, attempts + 1)

      {{:error, reason}, state} when reason in [:closed, :timeout, :econnrefused] ->
        backoff(attempts)
        do_post_with_retry(close_conn(state), kind, body, attempts + 1)

      {{:error, _} = err, state} ->
        # Non-transient error — give up early.
        {err, state}
    end
  end

  defp do_post_with_retry(state, _kind, _body, _attempts),
    do: {{:error, :retries_exhausted}, state}

  defp attempt_post(state, kind, body) do
    case state.endpoint do
      nil ->
        {{:error, :no_endpoint_configured}, state}

      endpoint ->
        {scheme, host, port, base_path} = endpoint

        path = signal_path(base_path, kind)

        with {:ok, state} <- ensure_conn(state, scheme, host, port),
             {:ok, state, status} <- send_request(state, path, body) do
          {{:ok, status}, state}
        else
          {:error, reason} -> {{:error, reason}, state}
          {:error, reason, state2} -> {{:error, reason}, state2}
        end
    end
  end

  defp signal_path(base, :metrics), do: ensure_suffix(base, "/v1/metrics")
  defp signal_path(base, :logs), do: ensure_suffix(base, "/v1/logs")
  defp signal_path(base, :traces), do: ensure_suffix(base, "/v1/traces")

  defp ensure_suffix("", suffix), do: suffix
  defp ensure_suffix("/", suffix), do: suffix

  defp ensure_suffix(base, suffix) do
    if String.ends_with?(base, suffix), do: base, else: base <> suffix
  end

  # The connection is kept across posts and reopened on disconnect; only the
  # exchange itself is shared with the durability tier's S3 backend.
  defp ensure_conn(state, scheme, host, port) do
    if HttpClient.open?(state.conn) do
      {:ok, state}
    else
      case HttpClient.connect(scheme, host, port) do
        {:ok, conn} -> {:ok, %{state | conn: conn}}
        {:error, reason} -> {:error, reason}
      end
    end
  end

  defp send_request(%{conn: conn, headers: extra} = state, path, body) do
    headers =
      [{"content-type", "application/x-protobuf"}] ++
        Enum.map(extra, fn {k, v} -> {to_string(k), to_string(v)} end)

    case HttpClient.request(conn, "POST", path, headers, body, @request_timeout_ms) do
      {:ok, conn, status, _headers, _body} -> {:ok, %{state | conn: conn}, status}
      {:error, reason, conn} -> {:error, reason, %{state | conn: conn}}
    end
  end

  defp close_conn(%{conn: nil} = state), do: state

  defp close_conn(%{conn: conn} = state) do
    HttpClient.close(conn)
    %{state | conn: nil}
  end

  defp backoff(attempt) do
    Enum.at(@backoffs_ms, attempt, 4_000)
    |> Process.sleep()
  end

  defp load_endpoint() do
    case Application.get_env(:flezha, :observability_export, []) |> Keyword.get(:otlp_endpoint) do
      nil ->
        nil

      "" ->
        nil

      url ->
        parse_url(url)
    end
  end

  defp load_headers() do
    Application.get_env(:flezha, :observability_export, [])
    |> Keyword.get(:otlp_headers, %{})
    |> Map.to_list()
  end

  defp parse_url(url) do
    uri = URI.parse(url)
    scheme = if uri.scheme in ["https", "https://"], do: :https, else: :http

    port =
      cond do
        is_integer(uri.port) -> uri.port
        scheme == :https -> 443
        true -> 80
      end

    host = uri.host || "127.0.0.1"
    path = uri.path || ""
    {scheme, host, port, path}
  end
end
