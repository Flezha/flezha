defmodule Flezha.Http.S3 do
  @moduledoc """
  A client for an S3-compatible object store, called by the remote durability
  tier's S3 backend and by the file plugins under an `s3://` root.

  Hand-rolled AWS Signature V4 over `Flezha.Http.Client`: an object store is a
  handful of HTTP verbs and one signing algorithm, not worth a heavyweight SDK
  and its transitive tree. Addressing is path-style, which every S3-compatible
  implementation accepts and MinIO needs.

  Options: `:endpoint`, `:bucket`, `:region` (`us-east-1`), `:access_key_id`,
  `:secret_access_key_env`, which names the variable holding the secret key,
  and `:prefix`, joined in front of every key. The secret is read when a
  request is signed and is in no answer.
  """

  alias Flezha.Config.Durations
  alias Flezha.Http.Client, as: HttpClient

  @algorithm "AWS4-HMAC-SHA256"
  @service "s3"

  # The counter a guarded write carries. Objects the durability tier has already
  # written carry it under this name.
  @counter "x-amz-meta-epoch"

  @type opts :: keyword()

  @doc "The object at `key`, or `{:error, :not_found}`."
  @spec get(binary(), opts()) :: {:ok, binary()} | {:error, term()}
  def get(key, opts), do: fetch(key, [], opts)

  @doc """
  At most `bytes` bytes of the object at `key`, from `offset`. Given an `etag`,
  the read answers only while the object still carries it, and
  `{:error, :precondition_failed}` once it has been replaced. A range starting
  at or past the end holds no bytes and answers `""`.
  """
  @spec get_range(binary(), non_neg_integer(), pos_integer(), binary() | nil, opts()) ::
          {:ok, binary()} | {:error, term()}
  def get_range(key, offset, bytes, etag, opts) do
    range = {"range", "bytes=#{offset}-#{offset + bytes - 1}"}
    condition = if etag, do: [{"if-match", etag}], else: []

    case fetch(key, [range | condition], opts) do
      {:error, {:http, 416, _body}} -> {:ok, ""}
      result -> result
    end
  end

  @doc """
  The size, the ETag and the guarded-write counter of the object at `key`; the
  counter is `0` on an object written without one.
  """
  @spec head(binary(), opts()) ::
          {:ok, %{size: non_neg_integer(), etag: binary() | nil, counter: non_neg_integer()}}
          | {:error, term()}
  def head(key, opts) do
    case request("HEAD", object_path(key, opts), [], "", opts) do
      {:ok, 200, headers, _body} ->
        {:ok,
         %{
           size: integer_header(headers, "content-length"),
           etag: header(headers, "etag"),
           counter: integer_header(headers, @counter)
         }}

      {:ok, 404, _headers, _body} ->
        {:error, :not_found}

      {:ok, status, _headers, body} ->
        {:error, {:http, status, body}}

      {:error, _} = err ->
        err
    end
  end

  @doc "Write `blob` to `key`, with `headers` signed into the request."
  @spec put(binary(), iodata(), [HttpClient.header()], opts()) :: :ok | {:error, term()}
  def put(key, blob, headers, opts) do
    case request("PUT", object_path(key, opts), headers, blob, opts) do
      {:ok, status, _headers, _body} when status in 200..299 -> :ok
      {:ok, 412, _headers, _body} -> {:error, :precondition_failed}
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  @doc """
  Write `blob` to `key` carrying `counter`, unless `admits?` refuses the counter
  the stored object carries, which answers `{:error, {:refused, stored}}`. A
  missing object admits any counter.

  The read and the write are two requests. The write carries `If-Match` on the
  ETag it read, or `If-None-Match: *` when there was no object, and that
  precondition, not the comparison, is what separates two writers reading one
  counter: the second answers `{:error, :precondition_failed}`.
  """
  @spec put_if(binary(), iodata(), non_neg_integer(), (non_neg_integer() -> boolean()), opts()) ::
          :ok | {:error, term()}
  def put_if(key, blob, counter, admits?, opts) do
    carried = {@counter, to_string(counter)}

    case head(key, opts) do
      {:ok, %{counter: stored, etag: etag}} ->
        if admits?.(stored),
          do: put(key, blob, [{"if-match", etag || "*"}, carried], opts),
          else: {:error, {:refused, stored}}

      {:error, :not_found} ->
        put(key, blob, [{"if-none-match", "*"}, carried], opts)

      {:error, _} = err ->
        err
    end
  end

  @doc "Delete the object at `key`; one already gone is deleted too."
  @spec delete(binary(), opts()) :: :ok | {:error, term()}
  def delete(key, opts) do
    case request("DELETE", object_path(key, opts), [], "", opts) do
      {:ok, status, _headers, _body} when status in [200, 204, 404] -> :ok
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  @doc """
  Every key under `prefix` with its age in milliseconds, the configured prefix
  stripped back off.
  """
  @spec list(binary(), opts()) :: {:ok, [{binary(), non_neg_integer()}]} | {:error, term()}
  def list(prefix, opts), do: list_page(prefix, opts, nil, [])

  @doc """
  Create the configured bucket if it does not exist. Provisioning, not a
  request any ship or plugin path makes: call it once from an operator script
  or a test fixture.
  """
  @spec create_bucket(opts()) :: :ok | {:error, term()}
  def create_bucket(opts) do
    case request("PUT", "/" <> bucket(opts), [], "", opts) do
      {:ok, status, _headers, _body} when status in 200..299 -> :ok
      # Both spellings mean the bucket is already there and ours.
      {:ok, 409, _headers, _body} -> :ok
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  @doc """
  Whether two endpoints name one server: the scheme, host and port a request
  to either would connect to.
  """
  @spec same_endpoint?(term(), term()) :: boolean()
  def same_endpoint?(one, other) when is_binary(one) and is_binary(other),
    do: named_server(one) == named_server(other)

  def same_endpoint?(_one, _other), do: false

  # ---------- operations ----------

  defp fetch(key, headers, opts) do
    case request("GET", object_path(key, opts), headers, "", opts) do
      {:ok, status, _headers, body} when status in [200, 206] -> {:ok, body}
      {:ok, 404, _headers, _body} -> {:error, :not_found}
      {:ok, 412, _headers, _body} -> {:error, :precondition_failed}
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  # ListObjectsV2, following continuation tokens so a prefix larger than one
  # page is still a complete answer: the garbage collector's live/orphan diff
  # is only safe against a complete listing.
  defp list_page(prefix, opts, token, acc) do
    query =
      [{"list-type", "2"}, {"prefix", full_key(prefix, opts)}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    path = "/" <> bucket(opts) <> "?" <> canonical_query(query)

    case request("GET", path, [], "", opts) do
      {:ok, 200, _headers, body} ->
        acc = acc ++ contents_entries(body, opts)

        case {scan_tags(body, "IsTruncated"), scan_tags(body, "NextContinuationToken")} do
          {["true"], [next]} -> list_page(prefix, opts, next, acc)
          _ -> {:ok, acc}
        end

      {:ok, status, _headers, body} ->
        {:error, {:http, status, body}}

      {:error, _} = err ->
        err
    end
  end

  # Parsed per `<Contents>` block rather than by zipping two flat tag scans: a
  # zip silently pairs the wrong key with the wrong age the moment one entry
  # omits an element, and mispairing an age is exactly how collection would
  # delete something young.
  #
  # Age is `now - LastModified`. Unlike Postgres, which computes it server-side,
  # this one spans two clocks, tolerable only because the threshold it feeds is
  # an hour, so ordinary NTP skew cannot move a decision.
  defp contents_entries(body, opts) do
    now = DateTime.utc_now()

    ~r|<Contents>(.*?)</Contents>|s
    |> Regex.scan(body)
    |> Enum.flat_map(fn [_, block] ->
      case {scan_tags(block, "Key"), scan_tags(block, "LastModified")} do
        {[key | _], [written | _]} -> [{strip_prefix(key, opts), age_of(now, written)}]
        {[key | _], []} -> [{strip_prefix(key, opts), 0}]
        _ -> []
      end
    end)
  end

  # An unparseable timestamp reports age 0, so the artifact reads as young and is
  # left alone. Collection may leak, never lose: the safe direction for every
  # unknown here.
  defp age_of(now, last_modified) do
    case DateTime.from_iso8601(last_modified) do
      {:ok, written_at, _offset} -> max(0, DateTime.diff(now, written_at, :millisecond))
      _unparseable -> 0
    end
  end

  # The listing response is the one XML this client reads, and it reads exactly
  # one element name out of it at a time: a regex scan is the honest tool for
  # that, and keeps an XML parser out of the dependency tree.
  defp scan_tags(body, tag) do
    ~r/<#{tag}>([^<]*)<\/#{tag}>/
    |> Regex.scan(body)
    |> Enum.map(fn [_, value] -> value end)
  end

  # ---------- HTTP ----------

  # One connection per request: shipping happens at boundary cadence, not per
  # record, so the exporter's persistent-connection bookkeeping would buy
  # nothing here.
  defp request(method, path, extra_headers, body, opts) do
    uri = URI.parse(endpoint(opts))
    {scheme, host, port} = server(uri)
    host_header = if uri.port in [nil, 80, 443], do: host, else: "#{host}:#{port}"

    headers = sign(method, path, extra_headers, body, host_header, opts)

    case HttpClient.connect(scheme, host, port) do
      {:ok, conn} ->
        conn
        |> HttpClient.request(method, path, headers, body, Durations.get(:s3_recv_ms))
        |> close_after()

      {:error, reason} ->
        {:error, reason}
    end
  end

  # The scheme, host and port a request to an endpoint connects to.
  defp server(%URI{scheme: scheme, host: host, port: port}) do
    scheme = if scheme == "https", do: :https, else: :http
    {scheme, host, port || if(scheme == :https, do: 443, else: 80)}
  end

  defp named_server(endpoint) do
    {scheme, host, port} = endpoint |> URI.parse() |> server()
    {scheme, host && String.downcase(host), port}
  end

  defp close_after({:ok, conn, status, headers, body}) do
    HttpClient.close(conn)
    {:ok, status, headers, body}
  end

  defp close_after({:error, reason, conn}) do
    HttpClient.close(conn)
    {:error, reason}
  end

  defp header(headers, name), do: HttpClient.header(headers, name)

  defp integer_header(headers, name) do
    case header(headers, name) do
      nil -> 0
      value -> String.to_integer(String.trim(value))
    end
  end

  # ---------- signature v4 ----------

  defp sign(method, path, extra_headers, body, host_header, opts) do
    now = DateTime.utc_now()
    amz_date = Calendar.strftime(now, "%Y%m%dT%H%M%SZ")
    date_stamp = Calendar.strftime(now, "%Y%m%d")
    payload_hash = hex(:crypto.hash(:sha256, body))

    headers =
      ([
         {"host", host_header},
         {"x-amz-content-sha256", payload_hash},
         {"x-amz-date", amz_date}
       ] ++ extra_headers)
      |> Enum.map(fn {k, v} -> {String.downcase(k), String.trim(to_string(v))} end)
      |> Enum.sort_by(&elem(&1, 0))

    signed_headers = Enum.map_join(headers, ";", &elem(&1, 0))
    {canonical_uri, canonical_qs} = split_path(path)

    canonical_request =
      Enum.join(
        [
          method,
          canonical_uri,
          canonical_qs,
          Enum.map_join(headers, "", fn {k, v} -> "#{k}:#{v}\n" end),
          signed_headers,
          payload_hash
        ],
        "\n"
      )

    scope = "#{date_stamp}/#{region(opts)}/#{@service}/aws4_request"

    string_to_sign =
      Enum.join(
        [@algorithm, amz_date, scope, hex(:crypto.hash(:sha256, canonical_request))],
        "\n"
      )

    signature = hex(:crypto.mac(:hmac, :sha256, signing_key(date_stamp, opts), string_to_sign))

    authorization =
      "#{@algorithm} Credential=#{access_key_id(opts)}/#{scope}, " <>
        "SignedHeaders=#{signed_headers}, Signature=#{signature}"

    [{"authorization", authorization} | headers]
  end

  defp signing_key(date_stamp, opts) do
    mac = &:crypto.mac(:hmac, :sha256, &1, &2)

    ("AWS4" <> secret_access_key(opts))
    |> mac.(date_stamp)
    |> mac.(region(opts))
    |> mac.(@service)
    |> mac.("aws4_request")
  end

  defp split_path(path) do
    case String.split(path, "?", parts: 2) do
      [uri] -> {uri, ""}
      [uri, query] -> {uri, query}
    end
  end

  # Canonical query: URI-encoded pairs sorted by key, which is what the
  # signature covers; a differently ordered request string signs differently.
  defp canonical_query(pairs) do
    pairs
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map_join("&", fn {k, v} -> "#{escape(k)}=#{escape(v)}" end)
  end

  defp escape(value), do: URI.encode(to_string(value), &URI.char_unreserved?/1)

  defp hex(binary), do: Base.encode16(binary, case: :lower)

  # ---------- options ----------

  # Each path segment is escaped separately: S3 signs the path with `/` intact
  # and everything else percent-encoded.
  defp object_path(key, opts) do
    escaped =
      full_key(key, opts)
      |> String.split("/")
      |> Enum.map_join("/", &escape/1)

    "/" <> bucket(opts) <> "/" <> escaped
  end

  defp full_key(key, opts) do
    case prefix(opts) do
      "" -> key
      p -> p <> "/" <> key
    end
  end

  defp strip_prefix(key, opts) do
    case prefix(opts) do
      "" -> key
      p -> String.replace_prefix(key, p <> "/", "")
    end
  end

  defp prefix(opts), do: (Keyword.get(opts, :prefix) || "") |> String.trim("/")
  defp bucket(opts), do: Keyword.fetch!(opts, :bucket)
  defp region(opts), do: Keyword.get(opts, :region) || "us-east-1"
  defp endpoint(opts), do: Keyword.fetch!(opts, :endpoint)
  defp access_key_id(opts), do: Keyword.fetch!(opts, :access_key_id)

  defp secret_access_key(opts) do
    case Keyword.get(opts, :secret_access_key_env) do
      env when is_binary(env) and env != "" -> System.get_env(env) || ""
      _ -> Keyword.get(opts, :secret_access_key) || ""
    end
  end
end
