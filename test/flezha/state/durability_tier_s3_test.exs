defmodule Flezha.State.DurabilityTier.Backend.S3Test do
  @moduledoc """
  The S3 durability backend against MinIO in `docker/dev/docker-compose.yml`.
  Tagged `:s3_integration` so the default `mix test` skips it — run with:

      S3_INTEGRATION=1 mix test --include s3_integration \\
        test/flezha/state/durability_tier_s3_test.exs

  Pre-req: `docker compose -f docker/dev/docker-compose.yml up -d minio`
  (reachable at localhost:9100, user/password `flezha`/`flezha_test_secret`).

  The signature is the part worth exercising against a real server: a
  canonical-request bug produces a `403 SignatureDoesNotMatch` that no unit
  test over a stub would ever see. The listing case is the second — orphan
  collection is only safe against a complete, correctly-paged listing.
  """

  use ExUnit.Case, async: false

  alias Flezha.State.DurabilityTier.Backend.S3

  @moduletag :s3_integration

  setup_all do
    if System.get_env("S3_INTEGRATION") != "1" do
      raise "Refusing to run s3_integration tests without S3_INTEGRATION=1"
    end

    System.put_env("FLEZHA_TEST_S3_SECRET", System.get_env("MINIO_SECRET", "flezha_test_secret"))

    opts = [
      endpoint: System.get_env("MINIO_ENDPOINT", "http://localhost:9100"),
      bucket: System.get_env("MINIO_BUCKET", "flezha-durability-test"),
      region: "us-east-1",
      access_key_id: System.get_env("MINIO_USER", "flezha"),
      secret_access_key_env: "FLEZHA_TEST_S3_SECRET",
      prefix: "flezha"
    ]

    :ok = S3.create_bucket(opts)
    %{opts: opts}
  end

  setup %{opts: opts} do
    {:ok, entries} = S3.list("", opts)
    Enum.each(entries, fn {key, _age} -> S3.delete(key, opts) end)
    %{opts: opts}
  end

  test "put, get and delete round-trip opaque bytes under the configured prefix", %{opts: opts} do
    blob = :crypto.strong_rand_bytes(64 * 1024)

    assert :ok = S3.put("blob/e/cls/proc/k/r-0-0-9", blob, opts)
    assert {:ok, ^blob} = S3.get("blob/e/cls/proc/k/r-0-0-9", opts)

    assert :ok = S3.delete("blob/e/cls/proc/k/r-0-0-9", opts)
    assert {:error, :not_found} = S3.get("blob/e/cls/proc/k/r-0-0-9", opts)
    assert :ok = S3.delete("blob/e/cls/proc/k/r-0-0-9", opts), "delete is idempotent"
  end

  test "list is prefix-scoped and strips the configured prefix back off", %{opts: opts} do
    assert :ok = S3.put("manifest/e/cls/proc/_key", "a", opts)
    assert :ok = S3.put("manifest/e/other/proc/k", "b", opts)

    # `<LastModified>` rides the same listing response as `<Key>`, so the age
    # costs no extra request.
    assert {:ok, [{"manifest/e/cls/proc/_key", age}]} = S3.list("manifest/e/cls/", opts)
    assert is_integer(age) and age >= 0
  end

  test "put_if extends an owned lineage and refuses a bid that is not ahead", %{opts: opts} do
    key = "manifest/e/cls/proc/k"

    assert :ok = S3.put_if(key, "claim-1", {:claim, 1}, opts)
    assert :ok = S3.put_if(key, "claim-1-again", {:extend, 1}, opts)
    assert {:ok, "claim-1-again"} = S3.get(key, opts)

    assert {:error, {:claim_held, 1}} = S3.put_if(key, "collision", {:claim, 1}, opts)

    assert :ok = S3.put_if(key, "claim-2", {:claim, 2}, opts)
    assert {:error, {:claim_held, 2}} = S3.put_if(key, "stale", {:extend, 1}, opts)
    assert {:ok, "claim-2"} = S3.get(key, opts)
  end

  test "the whole tier ships and hydrates through this backend", %{opts: opts} do
    base = Flezha.State.TestHelpers.tmp_app_data_dir()

    Application.put_env(:flezha, :durability_tier,
      backend: {S3, opts},
      ship_interval_ms: 60_000,
      rpo_target_ms: 60_000,
      gc_interval_ms: 3_600_000
    )

    entity_id = {"s3cls_#{System.unique_integer([:positive])}", "proc", "k1"}
    path = Flezha.State.Registry.wal_path(entity_id)
    File.mkdir_p!(Path.dirname(path))

    records =
      Enum.map_join(0..3, &Flezha.State.TestHelpers.build_wal_record(&1, "change-#{&1}"))

    File.write!(path, records)

    on_exit(fn ->
      Application.delete_env(:flezha, :durability_tier)
      Application.delete_env(:flezha, :state_root)
      _ = File.rm_rf(base)
    end)

    {:ok, _pid} = start_supervised(Flezha.State.DurabilityTier.Shipper)
    assert :ok = Flezha.State.DurabilityTier.Shipper.ship_now({:entity, entity_id})

    File.rm!(path)
    assert :ok = Flezha.State.DurabilityTier.hydrate_operative(elem(entity_id, 0))
    assert File.read!(path) == records
  end
end
