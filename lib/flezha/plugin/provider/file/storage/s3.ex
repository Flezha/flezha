defmodule Flezha.Plugin.Provider.File.Storage.S3 do
  @moduledoc """
  A file plugin's objects under an `s3://<bucket>/<prefix>` root, through
  `Flezha.Http.S3` with the controller's own file plugin S3 settings.

  A path is a key resolved without a request, and a root is refused while a
  setting a request needs is unset or while it overlaps the durability tier's
  objects. An object has no descriptor and no inode. A reader's identity is the ETag,
  and every ranged read is conditioned on it, so a replaced object answers
  `:replaced` rather than bytes of the new one. An object cannot be appended
  to, so a sink's open roll is held in memory as its handle and committed by
  one PUT, and the sidecar is written only while it does not go backwards.

  A missing object is `:enoent`, as a missing file is. Any other failed request
  is `{:http, status}`, without the response body.
  """

  @behaviour Flezha.Plugin.Provider.File.Storage

  alias Flezha.Config
  alias Flezha.Http.S3, as: Client
  alias Flezha.Plugin.ProviderBehaviour.Options, as: ProviderOptions
  alias Flezha.State.DurabilityTier.Backend

  @scheme "s3://"

  # What a request cannot be signed or addressed without.
  @required_settings [:endpoint, :access_key_id, :secret_access_key_env]

  # One PUT carries at most 5 GiB, and a part is one PUT.
  @max_part_bytes 5_368_709_120

  @doc "Whether `path` is an `s3://` URL."
  @spec url?(term()) :: boolean()
  def url?(path), do: is_binary(path) and String.starts_with?(path, @scheme)

  # ---------- the root ----------

  @doc "`<prefix>/<namespace>` under the root, once the root itself is one a request can use."
  @impl true
  def namespace_root(root, namespace) do
    with {:ok, _bucket, _prefix} <- checked_root(root),
         do: {:ok, String.trim_trailing(root, "/") <> "/" <> namespace}
  end

  @doc """
  Resolve `path` to an object under the `s3://<bucket>/<prefix>` root without
  reading anything: a relative path is joined to the prefix and an `s3://` path
  is taken as written, each with its `.` and `..` segments removed. The object
  must lie under the prefix in the root's bucket, and a path starting with `/`
  names this controller's disk, so either is refused naming the root. Refused
  before that: a root missing a setting a request needs, one naming no bucket,
  and one overlapping the durability tier's objects.
  """
  @impl true
  def resolve(path, root) do
    with {:ok, bucket, prefix} <- checked_root(root),
         {:ok, ^bucket, key} <- parse(object_url(path, root)),
         true <- length(key) > length(prefix) and List.starts_with?(key, prefix) do
      {:ok, url(bucket, key)}
    else
      {:error, _reason} = refusal -> refusal
      _elsewhere -> {:error, {:file_path_outside_root, path, root}}
    end
  end

  @doc """
  An object is never appended to and one PUT carries at most 5 GiB, so a sink
  takes `commit: rename` and a `roll_bytes` one PUT carries. Nothing is made: a
  key prefix needs no directory.
  """
  @impl true
  def prepare_sink(%{commit: "rename", roll_bytes: bytes} = opts)
      when bytes <= @max_part_bytes,
      do: {:ok, opts}

  def prepare_sink(%{commit: "rename"} = opts),
    do: ProviderOptions.refuse_above(:roll_bytes, opts.roll_bytes, @max_part_bytes)

  def prepare_sink(opts), do: {:error, {:file_commit_unsupported, opts.path, opts.commit}}

  @impl true
  def shown(root), do: root

  defp checked_root(root) do
    with :ok <- require_settings(root),
         {:ok, bucket, prefix} <- parse_root(root),
         :ok <- refuse_durable_overlap(root, bucket, prefix),
         do: {:ok, bucket, prefix}
  end

  defp parse_root(root) do
    with :error <- parse(root), do: {:error, {:file_root_invalid, root}}
  end

  defp object_url("/" <> _absolute, _root), do: nil
  defp object_url(path, root), do: if(url?(path), do: path, else: root <> "/" <> path)

  # Refused at enable, where the user reads it, rather than at the first
  # request a worker makes.
  defp require_settings(root) do
    settings = settings()

    case Enum.find(@required_settings, &(settings[&1] in [nil, ""])) do
      nil ->
        :ok

      key ->
        {:error, {:file_root_setting_missing, root, Config.env_name({:file_plugin_s3, key})}}
    end
  end

  # A plugin able to write under the durability tier's prefix could overwrite a
  # manifest, whatever credentials it was given.
  defp refuse_durable_overlap(root, bucket, prefix) do
    with {Backend.S3, tier} <- Backend.Config.resolve(),
         true <- tier[:bucket] == bucket,
         true <- Client.same_endpoint?(tier[:endpoint], settings()[:endpoint]),
         tier_prefix = String.split(tier[:prefix] || "", "/", trim: true),
         true <- List.starts_with?(prefix, tier_prefix) or List.starts_with?(tier_prefix, prefix) do
      {:error,
       {:file_root_overlaps_durability, root, bucket, shown_prefix(tier_prefix),
        shown_prefix(prefix)}}
    else
      _apart -> :ok
    end
  end

  defp shown_prefix([]), do: "(the whole bucket)"
  defp shown_prefix(segments), do: Enum.join(segments, "/")

  # ---------- reading ----------

  @impl true
  def stat(url) do
    {key, opts} = locate(url)

    case Client.head(key, opts) do
      {:ok, %{size: size, etag: etag}} -> {:ok, %{type: :regular, size: size, identity: etag}}
      error -> failure(error)
    end
  end

  @impl true
  def open_read(url, etag), do: {:ok, %{url: url, etag: etag}}

  @impl true
  def pread(%{url: url, etag: etag}, offset, bytes) do
    {key, opts} = locate(url)

    case Client.get_range(key, offset, bytes, etag, opts) do
      {:ok, ""} -> :eof
      {:ok, data} -> {:ok, data}
      error -> failure(error)
    end
  end

  @impl true
  def close(handle), do: handle

  # ---------- writing ----------

  @impl true
  def open_write(roll, _url, _keep?), do: {:ok, roll || []}

  @impl true
  def append(roll, iodata), do: {:ok, [roll | iodata]}

  @impl true
  def commit_part(roll, _url, part) do
    {key, opts} = locate(part)

    case Client.put(key, IO.iodata_to_binary(roll), [], opts) do
      :ok -> {:ok, []}
      error -> {:error, reason(error), roll}
    end
  end

  @impl true
  def list(dir) do
    {key, opts} = locate(dir)

    case Client.list(key <> "/", opts) do
      {:ok, entries} ->
        {:ok,
         entries
         |> Enum.map(fn {object, _age} -> String.replace_prefix(object, key <> "/", "") end)
         |> Enum.reject(&String.contains?(&1, "/"))}

      error ->
        failure(error)
    end
  end

  @impl true
  def delete(url) do
    {key, opts} = locate(url)
    with {:error, _} = error <- Client.delete(key, opts), do: failure(error)
  end

  @impl true
  def read(url) do
    {key, opts} = locate(url)
    with {:error, _} = error <- Client.get(key, opts), do: failure(error)
  end

  # A worker a move left lingering must not rewind the sidecar a live one has
  # advanced, so the write carries the part and is refused below a higher one.
  @impl true
  def write_progress(url, part, body) do
    {key, opts} = locate(url)

    case Client.put_if(key, body, part, &(&1 <= part), opts) do
      :ok -> :ok
      {:error, {:refused, stored}} -> {:error, {:progress_ahead, stored}}
      error -> failure(error)
    end
  end

  # ---------- addressing ----------

  defp locate(url) do
    {:ok, bucket, segments} = parse(url)
    {Enum.join(segments, "/"), [{:bucket, bucket} | settings()]}
  end

  # The bucket an `s3://` URL names and its key as segments, `.` and `..` removed;
  # `:error` when it names no bucket or its key climbs above one.
  defp parse(@scheme <> rest) do
    [bucket | segments] = String.split(rest, "/")

    case fold(segments, []) do
      {:ok, key} when bucket != "" -> {:ok, bucket, key}
      _unusable -> :error
    end
  end

  defp parse(_other), do: :error

  defp url(bucket, segments), do: Enum.join([@scheme <> bucket | segments], "/")

  # The connection settings, as `Flezha.Http.S3` takes them less the bucket.
  defp settings(), do: Config.group(:file_plugin_s3)

  defp fold([], kept), do: {:ok, Enum.reverse(kept)}
  defp fold([segment | rest], kept) when segment in ["", "."], do: fold(rest, kept)
  defp fold([".." | _rest], []), do: :error
  defp fold([".." | rest], [_parent | kept]), do: fold(rest, kept)
  defp fold([segment | rest], kept), do: fold(rest, [segment | kept])

  defp failure(error), do: {:error, reason(error)}

  defp reason({:error, :not_found}), do: :enoent
  defp reason({:error, :precondition_failed}), do: :replaced
  defp reason({:error, {:http, status, _body}}), do: {:http, status}
  defp reason({:error, reason}), do: reason
end
