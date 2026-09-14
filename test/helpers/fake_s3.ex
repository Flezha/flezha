defmodule FlezhaTest.Helper.FakeS3 do
  @moduledoc """
  An in-memory bucket in place of `Flezha.Http.S3`, for a default-suite test of
  code that reaches S3 from processes of its own: objects with an ETag and a
  guarded-write counter, `If-Match` on a ranged read, `:not_found` for a missing
  object, and a `403` for a request whose secret is not the one the bucket was
  started with. The client is stubbed for every process until the test ends.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Http.S3
  alias FlezhaTest.Helper.MimicGlobal

  @stubbed [head: 2, get: 2, get_range: 5, put: 4, put_if: 5, delete: 2, list: 2]

  @doc """
  Start the bucket, accepting the secret the variable `opts` names holds now,
  and answer `opts`. The bucket is stopped by the last `on_exit`, so a worker
  stopped when the test ends still reaches it.
  """
  @spec start!(keyword()) :: keyword()
  def start!(opts) do
    secret = System.get_env(opts[:secret_access_key_env])
    {:ok, bucket} = Agent.start(fn -> %{secret: secret, objects: %{}} end, name: __MODULE__)
    on_exit(fn -> Agent.stop(bucket) end)

    MimicGlobal.set_mimic_global_until_exit()

    for {name, arity} <- @stubbed,
        do: Mimic.stub(S3, name, Function.capture(__MODULE__, name, arity))

    opts
  end

  def head(key, opts) do
    with {:ok, object} <- fetch(key, opts),
         do: {:ok, %{size: byte_size(object.body), etag: object.etag, counter: object.counter}}
  end

  def get(key, opts), do: with({:ok, object} <- fetch(key, opts), do: {:ok, object.body})

  def get_range(key, offset, bytes, etag, opts) do
    with {:ok, %{body: body} = object} <- fetch(key, opts) do
      cond do
        etag != nil and etag != object.etag -> {:error, :precondition_failed}
        offset >= byte_size(body) -> {:ok, ""}
        true -> {:ok, binary_part(body, offset, min(bytes, byte_size(body) - offset))}
      end
    end
  end

  def put(key, blob, _headers, opts), do: with(:ok <- signed(opts), do: store(key, blob, 0, opts))

  def put_if(key, blob, counter, admits?, opts) do
    case fetch(key, opts) do
      {:ok, %{counter: stored}} ->
        if admits?.(stored),
          do: store(key, blob, counter, opts),
          else: {:error, {:refused, stored}}

      {:error, :not_found} ->
        store(key, blob, counter, opts)

      error ->
        error
    end
  end

  def delete(key, opts) do
    with :ok <- signed(opts),
         do:
           Agent.update(__MODULE__, &%{&1 | objects: Map.delete(&1.objects, address(key, opts))})
  end

  def list(prefix, opts) do
    with :ok <- signed(opts) do
      {bucket, under} = address(prefix, opts)
      objects = Agent.get(__MODULE__, & &1.objects)

      {:ok,
       for({{^bucket, key}, _object} <- objects, String.starts_with?(key, under), do: key)
       |> Enum.sort()
       |> Enum.map(&{strip(&1, opts), 0})}
    end
  end

  defp fetch(key, opts) do
    with :ok <- signed(opts) do
      case Agent.get(__MODULE__, &Map.fetch(&1.objects, address(key, opts))) do
        {:ok, object} -> {:ok, object}
        :error -> {:error, :not_found}
      end
    end
  end

  defp store(key, blob, counter, opts) do
    body = IO.iodata_to_binary(blob)
    etag = ~s("#{Base.encode16(:crypto.hash(:md5, body), case: :lower)}")
    object = %{body: body, counter: counter, etag: etag}
    Agent.update(__MODULE__, &%{&1 | objects: Map.put(&1.objects, address(key, opts), object)})
  end

  defp signed(opts) do
    if System.get_env(opts[:secret_access_key_env]) == Agent.get(__MODULE__, & &1.secret),
      do: :ok,
      else: {:error, {:http, 403, "<Error><Code>SignatureDoesNotMatch</Code></Error>"}}
  end

  defp address(key, opts) do
    case prefix(opts) do
      "" -> {opts[:bucket], key}
      prefix -> {opts[:bucket], prefix <> "/" <> key}
    end
  end

  defp strip(key, opts) do
    case prefix(opts) do
      "" -> key
      prefix -> String.replace_prefix(key, prefix <> "/", "")
    end
  end

  defp prefix(opts), do: String.trim(opts[:prefix] || "", "/")
end
