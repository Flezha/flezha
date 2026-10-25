defmodule Flezha.State.DurabilityTier.Backend.LocalPathTest do
  @moduledoc """
  The filesystem durability backend, plus the declared-manifest adapter that
  rides whichever backend the tier resolves. Dependency-free — unlike the S3
  and Postgres suites this one always runs, so its run of
  `FlezhaTest.DurabilityBackendContract` is the blob contract's regression net
  in the default suite.
  """

  # async: false: writes Application env; a shared on-disk root.

  use FlezhaTest.Case, fixtures: [:tmp_dir, :env], async: false

  alias Flezha.State.DurabilityTier.Backend.LocalPath
  alias Flezha.State.DurabilityTier.DeclaredManifest
  alias Flezha.State.TestHelpers

  use FlezhaTest.DurabilityBackendContract, backend: LocalPath

  setup %{tmp: root} do
    %{opts: [path: root], root: root}
  end

  test "list hides claim sidecars", %{opts: opts} do
    assert :ok = LocalPath.put("manifest/e/cls/task/_key", "a", opts)
    assert :ok = LocalPath.put("manifest/e/other/task/k", "b", opts)
    assert :ok = LocalPath.put_if("manifest/e/cls/task/_key", "a2", {:extend, 1}, opts)

    assert {:ok, [{"manifest/e/cls/task/_key", _age}]} = LocalPath.list("manifest/e/cls/", opts)

    {:ok, all} = LocalPath.list("", opts)

    assert Enum.sort(Enum.map(all, &elem(&1, 0))) ==
             ["manifest/e/cls/task/_key", "manifest/e/other/task/k"]
  end

  test "list enters only the directory its prefix names", %{opts: opts, root: root} do
    for class <- ~w(cls clsx other), task <- 1..20, key <- 1..5 do
      :ok = LocalPath.put("manifest/c/#{class}/t#{task}/k#{key}", "m", opts)
    end

    for i <- 1..50, do: :ok = LocalPath.put("blob/c/other/t/k/s-#{i}", "b", opts)

    {{:ok, whole}, whole_calls} = listing("manifest/c/cls/", opts, root)
    assert length(whole) == 100
    assert Enum.all?(whole, &String.starts_with?(&1, "manifest/c/cls/"))
    assert "manifest/c/cls" in whole_calls
    assert Enum.all?(whole_calls, &(&1 == "manifest/c/cls" or &1 =~ ~r"^manifest/c/cls/"))

    # A trailing partial name is matched among the entries of its directory.
    {{:ok, partial}, partial_calls} = listing("manifest/c/cl", opts, root)
    assert length(partial) == 200
    refute Enum.any?(partial, &String.starts_with?(&1, "manifest/c/other/"))
    assert "manifest/c" in partial_calls
    assert Enum.all?(partial_calls, &(&1 == "manifest/c" or &1 =~ ~r"^manifest/c/cls"))
  end

  test "an unconditional put preserves the stored claim", %{opts: opts} do
    key = "manifest/e/cls/task/k"

    assert :ok = LocalPath.put_if(key, "claimed", {:claim, 3}, opts)
    assert :ok = LocalPath.put(key, "overwritten", opts)
    assert {:error, {:claim_held, 3}} = LocalPath.put_if(key, "bid", {:claim, 2}, opts)
  end

  # The keys a listing returns, and every directory it listed on the way, both
  # relative to the root.
  defp listing(prefix, opts, root) do
    {result, calls} = TestHelpers.file_calls(fn -> LocalPath.list(prefix, opts) end)
    listed = for {:list_dir, [dir]} <- calls, do: Path.relative_to(to_string(dir), root)

    {with({:ok, entries} <- result, do: {:ok, Enum.map(entries, &elem(&1, 0))}), listed}
  end

  describe "DeclaredManifest over the resolved backend" do
    setup %{root: root} do
      Application.put_env(:flezha, :durability_tier,
        backend: :local_path,
        backend_opts: [path: root]
      )

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
