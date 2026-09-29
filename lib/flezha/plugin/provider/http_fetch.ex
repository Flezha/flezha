defmodule Flezha.Plugin.Provider.HttpFetch do
  @moduledoc """
  The one HTTP GET a provider polls an upstream resource with, and the
  attach-time refusals that guard the endpoint it names.

  Two providers pull slowly-changing content over HTTP on an interval —
  `HttpPoll` injects the body onto an entry channel, `Websocket` reads it as a
  desired subscription set — and they need the same request semantics: a
  streamed body cancelled at a byte cap, a conditional GET on the server's
  ETag, verified TLS, and no redirect-following (static headers may carry a
  bearer token, and `:httpc` would replay it to whatever a redirect names).
  Those semantics live here once rather than once per caller.

  `fetch/1` is written to run in a short-lived child process: it blocks in a
  `receive` for the streamed response, so a caller that must stay answerable
  spawns it rather than calling it inline.
  """

  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions

  @typedoc """
  One request. `:etag` is the validator from the previous successful fetch, or
  `nil` to fetch unconditionally.
  """
  @type request() :: %{
          url: binary(),
          headers: %{binary() => binary()},
          etag: binary() | nil,
          timeout_ms: pos_integer(),
          max_body_bytes: pos_integer()
        }

  @type result() ::
          {:ok, body :: binary(), etag :: binary() | nil}
          | {:ok, :not_modified}
          | {:error, term()}

  @doc """
  Perform one GET. Blocks until the response settles, the body exceeds
  `:max_body_bytes`, or `:timeout_ms` elapses.
  """
  @spec fetch(request()) :: result()
  def fetch(request) do
    with {:ok, ssl_opts} <- ssl_opts(request.url) do
      http_opts = [
        timeout: request.timeout_ms,
        connect_timeout: request.timeout_ms,
        ssl: ssl_opts,
        # Static headers may carry a bearer token, and `:httpc` replays them to
        # whatever a redirect names — a host this provider never validated. A
        # 3xx is reported as the ordinary status failure it is.
        autoredirect: false
      ]

      case :httpc.request(
             :get,
             {String.to_charlist(request.url), request_headers(request)},
             http_opts,
             sync: false,
             stream: :self,
             body_format: :binary
           ) do
        {:ok, id} -> collect(id, request)
        {:error, reason} -> {:error, reason}
      end
    end
  end

  # `stream: :self` streams only 2xx-with-a-body; every other response arrives
  # whole, which is what makes 304 and the error statuses cheap.
  defp collect(id, request) do
    receive do
      {:http, {^id, :stream_start, headers}} ->
        collect_body(id, request, etag(headers), [], 0)

      {:http, {^id, {{_v, 304, _}, _headers, _body}}} ->
        {:ok, :not_modified}

      {:http, {^id, {{_v, status, _}, headers, body}}} when status >= 200 and status < 300 ->
        {:ok, body, etag(headers)}

      {:http, {^id, {{_v, status, _}, _headers, _body}}} ->
        {:error, {:http_status, status}}

      {:http, {^id, {:error, reason}}} ->
        {:error, reason}
    after
      request.timeout_ms -> cancel(id, :timeout)
    end
  end

  # The cap is enforced as the body arrives and the request is cancelled at the
  # boundary, so an endpoint returning gigabytes costs the controller the cap,
  # not the response.
  defp collect_body(id, request, etag, acc, size) do
    receive do
      {:http, {^id, :stream, chunk}} ->
        size = size + byte_size(chunk)

        if size > request.max_body_bytes do
          cancel(id, {:oversized_body, size, request.max_body_bytes})
        else
          collect_body(id, request, etag, [acc, chunk], size)
        end

      {:http, {^id, :stream_end, _headers}} ->
        {:ok, IO.iodata_to_binary(acc), etag}

      {:http, {^id, {:error, reason}}} ->
        {:error, reason}
    after
      request.timeout_ms -> cancel(id, :timeout)
    end
  end

  defp cancel(id, reason) do
    :httpc.cancel_request(id)
    {:error, reason}
  end

  defp request_headers(request) do
    configured =
      Enum.map(request.headers, fn {k, v} ->
        {String.to_charlist(to_string(k)), String.to_charlist(to_string(v))}
      end)

    # Conditional GET saves the transfer when the server offers an ETag; the
    # caller's own change gate stays authoritative, so a server that omits or
    # ignores it changes nothing but bandwidth.
    case request.etag do
      nil -> configured
      etag -> [{~c"if-none-match", String.to_charlist(etag)} | configured]
    end
  end

  defp etag(headers) do
    Enum.find_value(headers, fn {name, value} ->
      if String.downcase(to_string(name)) == "etag", do: to_string(value)
    end)
  end

  # Plain HTTP needs no TLS options; HTTPS is verified against the system CA
  # store, and a host with no usable store fails the fetch rather than silently
  # dropping to an unverified connection.
  defp ssl_opts("http://" <> _), do: {:ok, []}

  defp ssl_opts(_https) do
    {:ok,
     [
       verify: :verify_peer,
       cacerts: :public_key.cacerts_get(),
       depth: 3,
       customize_hostname_check: [
         match_fun: :public_key.pkix_verify_hostname_match_fun(:https)
       ]
     ]}
  rescue
    err -> {:error, {:no_ca_store, Exception.message(err)}}
  end

  ## ---------- attach-time refusals ----------

  @doc """
  The URL with credentials and query stripped — the only form that reaches a
  log line, a telemetry label, or an error string. Config endpoints routinely
  carry an API key in the query or userinfo, and every telemetry label is
  exported to the observability sinks.
  """
  @spec redact(binary()) :: binary()
  def redact(url) do
    url
    |> URI.parse()
    |> Map.merge(%{userinfo: nil, query: nil, fragment: nil})
    |> URI.to_string()
  end

  @doc """
  Accept only an absolute http/https URL carrying no credentials. `provider`,
  `field` and `headers_field` name the caller's own option so the refusal reads
  as its own configuration rather than this module's.
  """
  @spec validate_url(term(), binary(), atom(), atom()) :: {:ok, binary()} | {:error, term()}
  def validate_url(url, provider, field \\ :url, headers_field \\ :headers)

  def validate_url(url, provider, field, headers_field) when is_binary(url) and url != "" do
    case URI.parse(url) do
      # Credentials in the URL would be replayed on every request and echoed
      # into any error naming it; headers are the place for a secret.
      %URI{userinfo: userinfo} when is_binary(userinfo) ->
        {:error, {:url_has_credentials, provider, field, headers_field}}

      %URI{scheme: scheme, host: host}
      when scheme in ["http", "https"] and is_binary(host) and host != "" ->
        {:ok, url}

      %URI{scheme: scheme} when scheme in ["http", "https"] ->
        {:error, {:url_without_host, provider, field, redact(url)}}

      _ ->
        {:error, {:url_scheme_unsupported, provider, field, redact(url)}}
    end
  end

  def validate_url(_url, provider, field, _headers_field),
    do: ProviderOptions.refuse_required(provider, [field])

  @doc """
  Accept a header map, or the JSON object a CLI flag delivers it as, and
  normalise both names and values to strings.
  """
  @spec validate_headers(term(), binary(), atom()) ::
          {:ok, %{binary() => binary()}} | {:error, term()}
  def validate_headers(headers, provider, field \\ :headers)

  def validate_headers(nil, _provider, _field), do: {:ok, %{}}

  def validate_headers(headers, provider, field) when is_binary(headers) do
    case Jason.decode(headers) do
      {:ok, decoded} when is_map(decoded) -> validate_headers(decoded, provider, field)
      _ -> {:error, {:headers_invalid, provider, field}}
    end
  end

  def validate_headers(headers, provider, field) when is_map(headers) do
    if Enum.all?(headers, fn {_k, v} -> is_binary(v) or is_number(v) end) do
      {:ok, Map.new(headers, fn {k, v} -> {to_string(k), to_string(v)} end)}
    else
      {:error, {:headers_invalid, provider, field}}
    end
  end

  def validate_headers(_headers, provider, field),
    do: {:error, {:headers_invalid, provider, field}}
end
