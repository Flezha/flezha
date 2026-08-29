defmodule Flezha.State.DurabilityTier.Backend.LocalPathTest do
  @moduledoc """
  The filesystem durability backend, plus the declared-manifest adapter that
  rides whichever backend the tier resolves. Dependency-free — unlike the S3
  and Postgres suites this one always runs, so it doubles as the blob-contract
  regression net for the shared `put_if/4` guard semantics.
  """

  # async: false: writes Application env; a shared on-disk root.

  use ExUnit.Case, async: false

  alias Flezha.State.DurabilityTier.Backend.LocalPath
  alias Flezha.State.DurabilityTier.DeclaredManifest

  setup do
    root =
      Path.join([System.tmp_dir!(), "flezha_local_path_#{System.unique_integer([:positive])}"])

    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)
    %{opts: [path: root], root: root}
  end

  test "put, get and delete round-trip opaque bytes", %{opts: opts} do
    blob = :crypto.strong_rand_bytes(64 * 1024)

    assert :ok = LocalPath.put("blob/e/cls/proc/k/r-0-0-9", blob, opts)
    assert {:ok, ^blob} = LocalPath.get("blob/e/cls/proc/k/r-0-0-9", opts)

    assert :ok = LocalPath.delete("blob/e/cls/proc/k/r-0-0-9", opts)
    assert {:error, :not_found} = LocalPath.get("blob/e/cls/proc/k/r-0-0-9", opts)
    assert :ok = LocalPath.delete("blob/e/cls/proc/k/r-0-0-9", opts), "delete is idempotent"
  end

  test "list is prefix-scoped, ages entries, and hides claim sidecars", %{opts: opts} do
    assert :ok = LocalPath.put("manifest/e/cls/proc/_key", "a", opts)
    assert :ok = LocalPath.put("manifest/e/other/proc/k", "b", opts)
    assert :ok = LocalPath.put_if("manifest/e/cls/proc/_key", "a2", {:extend, 1}, opts)

    assert {:ok, [{"manifest/e/cls/proc/_key", age}]} = LocalPath.list("manifest/e/cls/", opts)
    assert is_integer(age) and age >= 0

    {:ok, all} = LocalPath.list("", opts)

    assert Enum.sort(Enum.map(all, &elem(&1, 0))) ==
             ["manifest/e/cls/proc/_key", "manifest/e/other/proc/k"]
  end

  test "put_if extends an owned lineage and refuses a bid that is not ahead", %{opts: opts} do
    key = "manifest/e/cls/proc/k"

    assert :ok = LocalPath.put_if(key, "claim-1", {:claim, 1}, opts)
    assert :ok = LocalPath.put_if(key, "claim-1-again", {:extend, 1}, opts)
    assert {:ok, "claim-1-again"} = LocalPath.get(key, opts)

    assert {:error, {:claim_held, 1}} = LocalPath.put_if(key, "collision", {:claim, 1}, opts)

    assert :ok = LocalPath.put_if(key, "claim-2", {:claim, 2}, opts)
    assert {:error, {:claim_held, 2}} = LocalPath.put_if(key, "stale", {:extend, 1}, opts)
    assert {:ok, "claim-2"} = LocalPath.get(key, opts)
  end

  test "an unconditional put preserves the stored claim", %{opts: opts} do
    key = "manifest/e/cls/proc/k"

    assert :ok = LocalPath.put_if(key, "claimed", {:claim, 3}, opts)
    assert :ok = LocalPath.put(key, "overwritten", opts)
    assert {:error, {:claim_held, 3}} = LocalPath.put_if(key, "bid", {:claim, 2}, opts)
  end

  describe "DeclaredManifest over the resolved backend" do
    setup %{root: root} do
      previous = Application.get_env(:flezha, :durability_tier)

      Application.put_env(:flezha, :durability_tier,
        backend: :local_path,
        backend_opts: [path: root]
      )

      on_exit(fn ->
        if previous,
          do: Application.put_env(:flezha, :durability_tier, previous),
          else: Application.delete_env(:flezha, :durability_tier)
      end)

      :ok
    end

    test "store/fetch round-trips the versioned blob" do
      assert :not_found = DeclaredManifest.fetch(:dm_hub, [])
      assert :ok = DeclaredManifest.store(:dm_hub, 3, "blob-v3", [])
      assert {:ok, {3, "blob-v3"}} = DeclaredManifest.fetch(:dm_hub, [])
    end

    test "a stale write never overwrites a higher stored version" do
      assert :ok = DeclaredManifest.store(:dm_hub, 5, "blob-v5", [])
      # Superseded is :ok by contract — the shipper must not retry it forever.
      assert :ok = DeclaredManifest.store(:dm_hub, 4, "blob-v4", [])
      assert {:ok, {5, "blob-v5"}} = DeclaredManifest.fetch(:dm_hub, [])

      # Re-writing the stored version is an idempotent :ok.
      assert :ok = DeclaredManifest.store(:dm_hub, 5, "blob-v5", [])
      assert {:ok, {5, "blob-v5"}} = DeclaredManifest.fetch(:dm_hub, [])
    end

    test "hubs get separate keys, outside the tier GC's blob/ scope" do
      assert :ok = DeclaredManifest.store(:dm_hub_a, 1, "a", [])
      assert :ok = DeclaredManifest.store(:dm_hub_b, 2, "b", [])
      assert {:ok, {1, "a"}} = DeclaredManifest.fetch(:dm_hub_a, [])
      assert {:ok, {2, "b"}} = DeclaredManifest.fetch(:dm_hub_b, [])

      {:ok, blob_scope} = LocalPath.list("blob/", path: tier_path())
      assert blob_scope == []
    end

    test "validate_config reflects whether the tier is on" do
      assert :ok = DeclaredManifest.validate_config([])

      Application.delete_env(:flezha, :durability_tier)
      assert {:error, :durability_tier_disabled} = DeclaredManifest.validate_config([])
      assert {:error, :durability_tier_disabled} = DeclaredManifest.store(:dm_hub, 1, "x", [])
      assert {:error, :durability_tier_disabled} = DeclaredManifest.fetch(:dm_hub, [])
    end

    defp tier_path do
      Application.get_env(:flezha, :durability_tier)
      |> Keyword.fetch!(:backend_opts)
      |> Keyword.fetch!(:path)
    end
  end
end
