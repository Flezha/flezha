defmodule Flezha.Http.S3Test do
  @moduledoc """
  The two reads a file source makes of an object, against a real S3 server: the HEAD it
  polls and the ranged GET it reads with. Tagged `:s3_integration`;
  `FlezhaTest.Helper.Minio` says how to run it.
  """

  # async: false: writes the system environment; every test empties one prefix.

  use ExUnit.Case, async: false

  alias Flezha.Http.S3
  alias FlezhaTest.Helper.Minio

  @moduletag :s3_integration

  @key "objects/events.ndjson"

  setup_all do
    %{opts: Minio.opts!("flezha-http-test", "http")}
  end

  setup %{opts: opts} do
    Minio.empty!(opts)
    :ok = S3.put(@key, "hello\nworld\n", [], opts)
    {:ok, %{etag: etag}} = S3.head(@key, opts)
    %{etag: etag}
  end

  test "a HEAD answers the size and the ETag, and a missing object is not found",
       %{opts: opts, etag: etag} do
    assert is_binary(etag) and etag != ""
    assert {:ok, %{size: 12, etag: ^etag, counter: 0}} = S3.head(@key, opts)
    assert {:error, :not_found} = S3.head("objects/absent", opts)
  end

  test "a ranged GET answers the bytes inside the object and stops at its end",
       %{opts: opts, etag: etag} do
    assert {:ok, "world"} = S3.get_range(@key, 6, 5, etag, opts)
    assert {:ok, "ld\n"} = S3.get_range(@key, 9, 100, etag, opts)
  end

  test "a range starting at or past the end answers no bytes", %{opts: opts, etag: etag} do
    assert {:ok, ""} = S3.get_range(@key, 12, 10, etag, opts)
    assert {:ok, ""} = S3.get_range(@key, 50, 10, etag, opts)
  end

  test "a ranged GET against a replaced object fails its precondition",
       %{opts: opts, etag: etag} do
    :ok = S3.put(@key, "replaced\n", [], opts)

    assert {:error, :precondition_failed} = S3.get_range(@key, 0, 5, etag, opts)
  end

  test "a ranged GET of a missing object is not found", %{opts: opts} do
    assert {:error, :not_found} = S3.get_range("objects/absent", 0, 5, nil, opts)
  end
end
