defmodule FlezhaTest.Helper.Minio do
  @moduledoc """
  The MinIO in `deploy/docker/dev/docker-compose.yml`, for the tests tagged
  `:s3_integration`. The default `mix test` skips them; run one with

      docker compose -f deploy/docker/dev/docker-compose.yml up -d minio
      S3_INTEGRATION=1 mix test --include s3_integration <file>

  MinIO listens on localhost:9100 with user and password
  `flezha`/`flezha_test_secret`; `MINIO_ENDPOINT`, `MINIO_USER`, `MINIO_SECRET`
  and `MINIO_BUCKET` point the tests elsewhere.

  `file_plugin_root/1` needs no MinIO: it only sets the controller's settings,
  so a default-suite test of an `s3://` root reads the same ones, and
  `plugin_bucket!/1` answers `FlezhaTest.Helper.FakeS3` in MinIO's place unless
  `S3_INTEGRATION=1`.
  """

  import ExUnit.Callbacks, only: [on_exit: 1]

  alias Flezha.Http.S3
  alias FlezhaTest.Helper.{FakeS3, MimicGlobal}

  @secret_env "FLEZHA_TEST_S3_SECRET"

  @doc """
  Client options for `bucket` under `prefix`, with the bucket created and the
  secret in the variable the options name. Raises without `S3_INTEGRATION=1`,
  so a stray `--include` fails at once rather than timing out against nothing.
  """
  @spec opts!(binary(), binary()) :: keyword()
  def opts!(bucket, prefix) do
    if not integration?() do
      raise "Refusing to run s3_integration tests without S3_INTEGRATION=1"
    end

    System.put_env(@secret_env, secret())

    opts =
      [bucket: System.get_env("MINIO_BUCKET", bucket), region: "us-east-1", prefix: prefix] ++
        settings()

    :ok = S3.create_bucket(opts)
    opts
  end

  @doc """
  Point the file plugins at `root`, an `s3://` URL, with MinIO's connection
  settings. Reads and writes nothing; the caller declares the `:env` fixture.
  """
  @spec file_plugin_root(binary()) :: :ok
  def file_plugin_root(root) do
    Application.put_env(:flezha, :file_plugin_root, root)
    Application.put_env(:flezha, :file_plugin_s3, settings())
  end

  @doc "Whether the tests reach MinIO: `S3_INTEGRATION=1`."
  @spec integration?() :: boolean()
  def integration?(), do: System.get_env("S3_INTEGRATION") == "1"

  @doc """
  A file plugin root under a prefix of its own in `bucket`, with its settings
  set, and client options for that prefix: MinIO's bucket under
  `S3_INTEGRATION=1`, emptied when the test ends, else a `FakeS3` one. The
  caller declares the `:env` and `:system_env` fixtures.
  """
  @spec plugin_bucket!(binary()) :: %{root: binary(), s3: keyword()}
  def plugin_bucket!(bucket) do
    prefix = "files-#{System.unique_integer([:positive])}"
    root = "s3://#{bucket}/#{prefix}"
    file_plugin_root(root)
    System.put_env(@secret_env, secret())

    s3 =
      if integration?() do
        opts = opts!(bucket, prefix)

        on_exit(fn ->
          System.put_env(@secret_env, secret())
          empty!(opts)
        end)

        opts
      else
        FakeS3.start!([bucket: bucket, prefix: prefix] ++ settings())
      end

    %{root: root, s3: s3}
  end

  @doc """
  Hold every guarded write `pid` makes: the test is sent `{:held, pid}` and the
  call never returns, while any other caller is served by the bucket in use.
  """
  @spec hold_guarded_writes!(pid()) :: :ok
  def hold_guarded_writes!(pid) do
    test = self()

    serve =
      if integration?(),
        do: &Mimic.call_original(S3, :put_if, &1),
        else: &apply(FakeS3, :put_if, &1)

    MimicGlobal.set_mimic_global_until_exit()

    Mimic.stub(S3, :put_if, fn key, blob, counter, admits?, opts ->
      if self() == pid do
        send(test, {:held, pid})
        Process.sleep(:infinity)
      else
        serve.([key, blob, counter, admits?, opts])
      end
    end)

    :ok
  end

  @doc "Delete every object under the options' prefix."
  @spec empty!(keyword()) :: :ok
  def empty!(opts) do
    {:ok, entries} = S3.list("", opts)
    Enum.each(entries, fn {key, _age} -> :ok = S3.delete(key, opts) end)
  end

  defp secret(), do: System.get_env("MINIO_SECRET", "flezha_test_secret")

  defp settings() do
    [
      endpoint: System.get_env("MINIO_ENDPOINT", "http://localhost:9100"),
      access_key_id: System.get_env("MINIO_USER", "flezha"),
      secret_access_key_env: @secret_env
    ]
  end
end
