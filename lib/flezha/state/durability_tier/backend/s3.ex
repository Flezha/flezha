defmodule Flezha.State.DurabilityTier.Backend.S3 do
  @moduledoc """
  Remote durability backend over an S3-compatible object store.

  The client is hand-rolled AWS Signature V4 over `:mint`: an object store is
  four HTTP verbs and one signing algorithm, not worth a heavyweight SDK and its
  transitive tree. Addressing is path-style, which every S3-compatible
  implementation accepts and MinIO needs.

  `put_if/4` is read-then-conditional-write: the counter rides
  `x-amz-meta-epoch`, the write carries `If-Match` on the observed ETag (or
  `If-None-Match: *` when absent), and a guard the counter does not admit
  reports `{:claim_held, stored}`. It is that precondition, not the comparison,
  that separates two claimants reading one value.

  Options: `:endpoint`, `:bucket`, `:region`, `:access_key_id`,
  `:secret_access_key_env`, `:prefix` (`flezha`).
  """

  @behaviour Flezha.State.DurabilityTier.Backend

  alias Flezha.Http.Client, as: HttpClient
  alias Flezha.State.DurabilityTier.Backend

  @algorithm "AWS4-HMAC-SHA256"
  @service "s3"
  @recv_timeout_ms 30_000

  @impl true
  def child_spec(_opts), do: :ignore

  @impl true
  def put(key, blob, opts), do: put_object(key, blob, [], opts)

  @impl true
  def get(key, opts) do
    case request("GET", object_path(key, opts), [], "", opts) do
      {:ok, 200, _headers, body} -> {:ok, body}
      {:ok, 404, _headers, _body} -> {:error, :not_found}
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  @impl true
  def delete(key, opts) do
    case request("DELETE", object_path(key, opts), [], "", opts) do
      {:ok, status, _headers, _body} when status in [200, 204, 404] -> :ok
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  @impl true
  def list(prefix, opts), do: list_page(prefix, opts, nil, [])

  @impl true
  def put_if(key, blob, guard, opts) do
    claim = elem(guard, 1)

    case head(key, opts) do
      {:ok, stored, etag} ->
        if Backend.guard_passes?(guard, stored) do
          # `if-match` on the ETag is the real compare-and-set: two claimants
          # that both read the same stored counter still resolve to one winner,
          # because the loser's precondition fails against the object the winner
          # just wrote.
          put_object(
            key,
            blob,
            [{"if-match", etag}, {"x-amz-meta-epoch", to_string(claim)}],
            opts
          )
        else
          {:error, {:claim_held, stored}}
        end

      :not_found ->
        put_object(
          key,
          blob,
          [{"if-none-match", "*"}, {"x-amz-meta-epoch", to_string(claim)}],
          opts
        )

      {:error, _} = err ->
        err
    end
  end

  @doc """
  Create the configured bucket if it does not exist — the S3 counterpart of the
  Postgres backend's `auto_create`. Provisioning, not part of the blob
  contract: call it once from an operator script or a test fixture, never on a
  ship path.
  """
  @spec create_bucket(keyword()) :: :ok | {:error, term()}
  def create_bucket(opts) do
    case request("PUT", "/" <> bucket(opts), [], "", opts) do
      {:ok, status, _headers, _body} when status in 200..299 -> :ok
      # Both spellings mean the bucket is already there and ours.
      {:ok, 409, _headers, _body} -> :ok
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  # ---------- operations ----------

  defp put_object(key, blob, extra_headers, opts) do
    case request("PUT", object_path(key, opts), extra_headers, blob, opts) do
      {:ok, status, _headers, _body} when status in 200..299 -> :ok
      {:ok, 412, _headers, _body} -> {:error, :precondition_failed}
      {:ok, status, _headers, body} -> {:error, {:http, status, body}}
      {:error, _} = err -> err
    end
  end

  defp head(key, opts) do
    case request("HEAD", object_path(key, opts), [], "", opts) do
      {:ok, 200, headers, _body} ->
        epoch =
          case header(headers, "x-amz-meta-epoch") do
            nil -> 0
            value -> String.to_integer(String.trim(value))
          end

        {:ok, epoch, header(headers, "etag") || "*"}

      {:ok, 404, _headers, _body} ->
        :not_found

      {:ok, status, _headers, body} ->
        {:error, {:http, status, body}}

      {:error, _} = err ->
        err
    end
  end

  # ListObjectsV2, following continuation tokens so a prefix larger than one
  # page is still a complete answer — the garbage collector's live/orphan diff
  # is only safe against a complete listing.
  defp list_page(prefix, opts, token, acc) do
    query =
      [{"list-type", "2"}, {"prefix", full_key(prefix, opts)}] ++
        if(token, do: [{"continuation-token", token}], else: [])

    path = "/" <> bucket(opts) <> "?" <> canonical_query(query)

    case request("GET", path, [], "", opts) do
      {:ok, 200, headers, body} ->
        acc = acc ++ contents_entries(body, headers, opts)

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
  # this one spans two clocks — tolerable only because the threshold it feeds is
  # an hour, so ordinary NTP skew cannot move a decision.
  defp contents_entries(body, _headers, opts) do
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
  # left alone. Collection may leak, never lose — the safe direction for every
  # unknown here.
  defp age_of(now, last_modified) do
    with {:ok, written_at, _offset} <- DateTime.from_iso8601(last_modified) do
      max(0, DateTime.diff(now, written_at, :millisecond))
    else
      _ -> 0
    end
  end

  # The listing response is the one XML this backend reads, and it reads exactly
  # one element name out of it — a regex scan is the honest tool for that, and
  # keeps an XML parser out of the dependency tree.
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
    scheme = if uri.scheme == "https", do: :https, else: :http
    port = uri.port || if(scheme == :https, do: 443, else: 80)
    host_header = if uri.port in [nil, 80, 443], do: uri.host, else: "#{uri.host}:#{uri.port}"

    headers = sign(method, path, extra_headers, body, host_header, opts)

    case HttpClient.connect(scheme, uri.host, port) do
      {:ok, conn} ->
        conn
        |> HttpClient.request(method, path, headers, body, @recv_timeout_ms)
        |> close_after()

      {:error, reason} ->
        {:error, reason}
    end
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
  # signature covers — a differently ordered request string signs differently.
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
