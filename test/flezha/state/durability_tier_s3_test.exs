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

  use FlezhaTest.DurabilityBackendContract, backend: S3

  setup_all do
    %{opts: Minio.opts!("flezha-durability-test", "flezha")}
  end

  setup %{opts: opts} do
    Minio.empty!(opts)
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
    assert :ok = DurabilityTier.hydrate_relay(elem(cell_id, 0))
    assert File.read!(path) == records
  end
end
