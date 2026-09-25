defmodule Flezha.State.DurabilityTier.Backend.S3Test do
  @moduledoc """
  The S3 durability backend against MinIO, tagged `:s3_integration`;
  `FlezhaTest.Helper.Minio` says how to run it.

  The signature is the part worth exercising against a real server: a
  canonical-request bug produces a `403 SignatureDoesNotMatch` that no unit
  test over a stub would ever see. The listing case is the second — orphan
  collection is only safe against a complete, correctly-paged listing.
  """

  # async: false: writes Application env; a shared on-disk root; registered singletons.

  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.Backend.S3
  alias Flezha.State.DurabilityTier.Shipper
  alias Flezha.State.Paths
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.Minio

  @moduletag :s3_integration

  setup_all do
    %{opts: Minio.opts!("flezha-durability-test", "flezha")}
  end

  setup %{opts: opts} do
    Minio.empty!(opts)
  end

  test "put, get and delete round-trip opaque bytes under the configured prefix", %{opts: opts} do
    blob = :crypto.strong_rand_bytes(64 * 1024)

    assert :ok = S3.put("blob/e/cls/task/k/r-0-0-9", blob, opts)
    assert {:ok, ^blob} = S3.get("blob/e/cls/task/k/r-0-0-9", opts)

    assert :ok = S3.delete("blob/e/cls/task/k/r-0-0-9", opts)
    assert {:error, :not_found} = S3.get("blob/e/cls/task/k/r-0-0-9", opts)
    assert :ok = S3.delete("blob/e/cls/task/k/r-0-0-9", opts), "delete is idempotent"
  end

  test "list is prefix-scoped and strips the configured prefix back off", %{opts: opts} do
    assert :ok = S3.put("manifest/e/cls/task/_key", "a", opts)
    assert :ok = S3.put("manifest/e/other/task/k", "b", opts)

    # `<LastModified>` rides the same listing response as `<Key>`, so the age
    # costs no extra request.
    assert {:ok, [{"manifest/e/cls/task/_key", age}]} = S3.list("manifest/e/cls/", opts)
    assert is_integer(age) and age >= 0
  end

  test "put_if extends an owned lineage and refuses a bid that is not ahead", %{opts: opts} do
    key = "manifest/e/cls/task/k"

    assert :ok = S3.put_if(key, "claim-1", {:claim, 1}, opts)
    assert :ok = S3.put_if(key, "claim-1-again", {:extend, 1}, opts)
    assert {:ok, "claim-1-again"} = S3.get(key, opts)

    assert {:error, {:claim_held, 1}} = S3.put_if(key, "collision", {:claim, 1}, opts)

    assert :ok = S3.put_if(key, "claim-2", {:claim, 2}, opts)
    assert {:error, {:claim_held, 2}} = S3.put_if(key, "stale", {:extend, 1}, opts)
    assert {:ok, "claim-2"} = S3.get(key, opts)
  end

  test "the whole tier ships and hydrates through this backend", %{opts: opts} do
    Application.put_env(:flezha, :durability_tier,
      backend: {S3, opts},
      ship_interval_ms: 60_000,
      rpo_target_ms: 60_000,
      gc_interval_ms: 3_600_000
    )

    cell_id = {"s3cls_#{System.unique_integer([:positive])}", "task", "k1"}
    path = Paths.wal_path(cell_id)
    File.mkdir_p!(Path.dirname(path))

    records =
      Enum.map_join(0..3, &TestHelpers.build_wal_record(&1, "change-#{&1}"))

    File.write!(path, records)

    {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)
    assert :ok = Shipper.ship_now({:cell, cell_id})

    File.rm!(path)
    assert :ok = DurabilityTier.hydrate_operator(elem(cell_id, 0))
    assert File.read!(path) == records
  end
end
