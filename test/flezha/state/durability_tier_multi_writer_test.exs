defmodule Flezha.State.DurabilityTierMultiWriterTest do
  @moduledoc """
  Collection with more than one controller on one backend — the configuration
  the tier exists to serve, since a fresh node adopting another's operatives is
  the whole point.

  Two `Shipper`s over disjoint state roots and one shared store stand in for two
  nodes. That is the whole fixture: collection reads nothing local except
  `all_refs/1`, so a second shipper makes exactly the decisions a second node
  would, and a real peer BEAM would add nothing but cost.

  The cases pin both halves of the safety argument independently — remove the
  grace and the in-flight case fails; remove the scoping and the
  older-than-grace case fails — because either one alone is a weaker guarantee
  than it reads.
  """
  # async: false: writes Application env; a shared on-disk root; the primary Logger.
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog

  alias Flezha.AuthZ.Config
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.{Artifact, Hydrator, Manifest, Shipper}
  alias Flezha.State.TestHelpers
  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub
  alias FlezhaTest.Helper.NodeTestHelper

  @peer_writer "peer-controller"

  # Fires a one-shot hook after a blob lands, which is the only way to place the
  # peer's collection where it is fatal: between the upload and the manifest
  # write of the same ship.
  defmodule HookedBackend do
    @behaviour Flezha.State.DurabilityTier.Backend

    alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub

    @hook {__MODULE__, :hook}

    def arm(fun), do: :persistent_term.put(@hook, fun)
    def disarm(), do: :persistent_term.erase(@hook)

    @impl true
    def child_spec(_opts), do: :ignore

    @impl true
    def put(key, blob, opts) do
      result = Stub.put(key, blob, opts)

      case :persistent_term.get(@hook, nil) do
        nil -> :ok
        fun -> disarm() && fun.(key)
      end

      result
    end

    @impl true
    def get(key, opts), do: Stub.get(key, opts)

    @impl true
    def list(prefix, opts), do: Stub.list(prefix, opts)

    @impl true
    def put_if(key, blob, epoch, opts), do: Stub.put_if(key, blob, epoch, opts)

    @impl true
    def delete(key, opts), do: Stub.delete(key, opts)
  end

  setup do
    base = TestHelpers.tmp_app_data_dir()
    peer_base = base <> "_peer"
    File.mkdir_p!(peer_base)
    Application.put_env(:flezha, :process_hub_registry_root, Path.join(base, "process_hub"))

    Stub.install()

    Application.put_env(
      :flezha,
      :durability_tier,
      Application.get_env(:flezha, :durability_tier) |> Keyword.put(:backend, {HookedBackend, []})
    )

    class = "dur_" <> Integer.to_string(System.unique_integer([:positive]))
    entity_id = {class, "proc", "k1"}

    on_exit(fn ->
      HookedBackend.disarm()
      Stub.uninstall()
      Application.delete_env(:flezha, :state_root)
      Application.delete_env(:flezha, :process_hub_registry_root)
      File.rm_rf(base)
      File.rm_rf(peer_base)
    end)

    %{base: base, peer_base: peer_base, class: class, entity_id: entity_id}
  end

  test "a peer's collection cannot delete a blob whose manifest entry is still in flight",
       ctx do
    ref = {:entity, ctx.entity_id}

    TestHelpers.write_wal(ctx.entity_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    peer = start_peer_node(ctx.peer_base)
    TestHelpers.append_wal(ctx.entity_id, 4..5)

    # The peer's hourly tick lands between this node's blob upload and its
    # manifest write — the one interleaving where "unreferenced" and "not
    # committed yet" are the same observation.
    HookedBackend.arm(fn key ->
      if String.starts_with?(key, "blob/"), do: {:ok, _} = GenServer.call(peer, :gc)
    end)

    assert :ok = Shipper.ship_now(ref)

    manifest = Stub.stored_manifest(ref)

    assert [%{from: 0, to: 3}, %{from: 4, to: 5}] =
             Enum.map(manifest.ranges, &Map.take(&1, [:from, :to]))

    assert Enum.all?(Manifest.blob_keys(manifest), &(&1 in Stub.keys())),
           "every artifact the manifest names must still exist"

    TestHelpers.wipe_entity(ctx.entity_id)
    assert :ok = DurabilityTier.hydrate_operative(ctx.class)
    assert Artifact.seq_bounds(ref) == {0, 5}
  end

  test "a FIRST ship is protected too, where no manifest entry exists to consult", ctx do
    ref = {:entity, ctx.entity_id}

    {:ok, _} = start_supervised(Shipper)
    peer = start_peer_node(ctx.peer_base)
    TestHelpers.write_wal(ctx.entity_id, 0..3)

    HookedBackend.arm(fn key ->
      if String.starts_with?(key, "blob/") do
        # The wide window: this reference has no manifest key yet, so it cannot
        # appear in the peer's listing and no fetch anywhere in its loop could
        # put this blob in the live set. Age is the only thing protecting it.
        assert {:error, :not_found} = Stub.get(Manifest.key(ref), [])
        {:ok, _} = GenServer.call(peer, :gc)
      end
    end)

    assert :ok = Shipper.ship_now(ref)

    assert [blob] = Manifest.blob_keys(Stub.stored_manifest(ref))
    assert blob in Stub.keys()

    TestHelpers.wipe_entity(ctx.entity_id)
    assert :ok = DurabilityTier.hydrate_operative(ctx.class)
    assert Artifact.seq_bounds(ref) == {0, 3}
  end

  test "past the grace, a peer still leaves another node's reference alone", ctx do
    ref = {:entity, ctx.entity_id}

    TestHelpers.write_wal(ctx.entity_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    TestHelpers.append_wal(ctx.entity_id, 4..5)
    Stub.fail_next_manifest()
    assert {:error, :manifest_write_lost} = Shipper.ship_now(ref)

    peer = start_peer_node(ctx.peer_base)
    orphan = only_orphan(ref)

    # Old enough that the grace has passed, but this reference belongs to the
    # other node, so the peer must not judge it.
    Stub.advance_clock(120_000)
    assert {:ok, 0} = peer_gc(peer, ctx.peer_base)
    assert orphan in Stub.keys()

    # The owner collects it, because it is the only node that can tell an
    # orphan from an upload in flight.
    assert {:ok, 1} = Shipper.gc()
    refute orphan in Stub.keys()
  end

  test "an abandoned reference is still reclaimed, so scoping does not leak", ctx do
    ref = {:entity, ctx.entity_id}

    TestHelpers.write_wal(ctx.entity_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    TestHelpers.append_wal(ctx.entity_id, 4..5)
    Stub.fail_next_manifest()
    assert {:error, :manifest_write_lost} = Shipper.ship_now(ref)
    orphan = only_orphan(ref)

    peer = start_peer_node(ctx.peer_base)

    # The operative is stopped and its state deleted, so now NO node holds local
    # state for this reference and scoping alone would strand the orphan.
    TestHelpers.wipe_entity(ctx.entity_id)

    Stub.advance_clock(120_000)
    assert {:ok, 0} = peer_gc(peer, ctx.peer_base), "past the grace, inside the abandoned window"
    assert orphan in Stub.keys()

    Stub.advance_clock(7_200_000)
    assert {:ok, 1} = peer_gc(peer, ctx.peer_base)
    refute orphan in Stub.keys()
  end

  describe "the platform stores under two writers" do
    test "one parked set has one shipper, and the loser stops quietly", ctx do
      ref = {:dlq, {ctx.class, "proc"}}
      write_park(ctx.class, "parked-by-owner")

      {:ok, _} = start_supervised(Shipper)
      assert :ok = Shipper.ship_now(ref)
      assert Stub.stored_manifest(ref).owner == this_writer()

      # The operative migrates, and its parked failures move with it: the parks
      # are claim-fenced exactly like the Entity state, so the new node bids the
      # lineage up rather than writing beside it.
      peer = start_peer_node(ctx.peer_base)

      with_root(ctx.peer_base, fn ->
        GenServer.cast(peer, {:adopt, ctx.class})
        assert :ok = GenServer.call(peer, {:ship_now, ref})
      end)

      assert %{owner: @peer_writer, claim: 2} = Stub.stored_manifest(ref)

      write_park(ctx.class, "parked-after-handover")
      assert {:error, {:superseded, @peer_writer}} = Shipper.ship_now(ref)
    end

    test "two controllers' control-plane replicas never collide", ctx do
      auth_dir = Config.auth_dir()
      File.mkdir_p!(auth_dir)
      roles = Path.join(auth_dir, "roles.dets")

      peer = start_peer_node(ctx.peer_base)
      File.write!(roles, "peer-roles")
      with_root(ctx.peer_base, fn -> :ok = GenServer.call(peer, :sweep_now) end)

      # The replica tie-break is the writing node's wall clock, so the two ships
      # have to land in different milliseconds for "newest" to mean anything.
      shipped_at = System.system_time(:millisecond)

      assert NodeTestHelper.wait_until(
               fn -> System.system_time(:millisecond) > shipped_at end,
               1_000,
               1
             )

      {:ok, _} = start_supervised(Shipper)
      File.write!(roles, "owner-roles")
      :ok = Shipper.sweep_now()

      own_ref = {:control, {"authz", this_writer()}}
      peer_ref = {:control, {"authz", @peer_writer}}

      # Separate keys, separate lineages: neither node is fenced out of shipping
      # its own copy of a store the cluster never replicates.
      assert Manifest.key(own_ref) != Manifest.key(peer_ref)
      assert %{owner: peer_owner} = Stub.stored_manifest(peer_ref)
      assert peer_owner == @peer_writer
      assert Stub.stored_manifest(own_ref).owner == this_writer()

      File.rm_rf!(auth_dir)
      assert :ok = Hydrator.hydrate_control_stores()
      assert File.read!(roles) == "owner-roles"
    end
  end

  test "two nodes holding one reference cannot overwrite each other's bytes", ctx do
    ref = {:entity, ctx.entity_id}

    TestHelpers.write_wal(ctx.entity_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    # The peer holds the same reference with different records above the shipped
    # cut — what an ordinary migration leaves behind, since `pull_operative/3`
    # copies and nothing deletes the source.
    peer = start_peer_node(ctx.peer_base)

    with_root(ctx.peer_base, fn ->
      TestHelpers.write_wal(ctx.entity_id, 0..3)
      TestHelpers.append_wal(ctx.entity_id, 4..5, "peer")
      GenServer.cast(peer, {:adopt, ctx.class})
      assert :ok = GenServer.call(peer, {:ship_now, ref})
    end)

    peer_blob = newest_range_blob(ref)
    assert {:ok, peer_bytes} = Stub.get(peer_blob, [])

    # This node's cached entry still says it owns the reference, so it uploads
    # its OWN 4..5 before it can discover otherwise — blobs go up first, and the
    # refusal only comes at the manifest write. Under position-addressed names
    # both cuts would be `r-0-4-5` and this upload would silently replace the
    # peer's records with different ones inside the peer's live manifest.
    TestHelpers.append_wal(ctx.entity_id, 4..5, "owner")
    assert {:error, {:superseded, @peer_writer}} = Shipper.ship_now(ref)

    assert {:ok, ^peer_bytes} = Stub.get(peer_blob, []),
           "the peer's artifact must survive a losing writer's upload of the same seq range"
  end

  describe "who owns a lineage" do
    test "an adopting node takes the lineage over and the loser stops quietly", ctx do
      ref = {:entity, ctx.entity_id}

      TestHelpers.write_wal(ctx.entity_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      assert :ok = Shipper.ship_now(ref)
      assert Stub.stored_manifest(ref).owner == this_writer()

      peer = peer_adopts_and_ships(ctx, ref)

      # Claim 1 was consumed by the first owner: an unclaimed lineage starts at
      # zero and the first ship bids against it, which is what makes two nodes
      # first-shipping one reference resolve to a winner.
      assert %{owner: @peer_writer, claim: 2} = Stub.stored_manifest(ref)
      assert Process.alive?(peer)

      # The loser stops, and it is NOT an alarm: a handover produces one of
      # these every time, and `pending_since_ms` must clear with it or the
      # reference reports a permanent RPO breach.
      TestHelpers.append_wal(ctx.entity_id, 6..7)
      assert {:error, {:superseded, @peer_writer}} = Shipper.ship_now(ref)
      assert %{superseded_by: @peer_writer, pending_since_ms: nil} = status_of(ref)
      assert DurabilityTier.breaches() == []
    end

    test "a node that lost the operative cannot bid its lineage back", ctx do
      ref = {:entity, ctx.entity_id}

      TestHelpers.write_wal(ctx.entity_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      assert :ok = Shipper.ship_now(ref)

      peer_adopts_and_ships(ctx, ref)

      # This node still holds every file — `pull_operative/3` copies and nothing
      # deletes the source — so "has the state" would let it claim straight
      # back. Only never having adopted stops it.
      :ok = Shipper.sweep_now()
      assert %{owner: @peer_writer} = Stub.stored_manifest(ref)
    end

    test "a restart extends its own lineage instead of bidding against itself", ctx do
      ref = {:entity, ctx.entity_id}

      TestHelpers.write_wal(ctx.entity_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      assert :ok = Shipper.ship_now(ref)
      assert %{owner: owner, claim: 1} = Stub.stored_manifest(ref)

      # A restart loses the manifest cache but keeps the writer id, so the next
      # ship re-reads the entry and finds itself already the owner.
      :ok = stop_supervised(Shipper)
      {:ok, _} = start_supervised(Shipper)
      TestHelpers.append_wal(ctx.entity_id, 4..5)
      assert :ok = Shipper.ship_now(ref)

      assert %{owner: ^owner, claim: 1} = Stub.stored_manifest(ref),
             "a restarted owner must not consume a claim"
    end

    test "each adoption hands the lineage on and consumes exactly one claim", ctx do
      ref = {:entity, ctx.entity_id}

      TestHelpers.write_wal(ctx.entity_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      peer = start_peer_node(ctx.peer_base)

      with_root(ctx.peer_base, fn ->
        TestHelpers.write_wal(ctx.entity_id, 0..3)
        GenServer.cast(peer, {:adopt, ctx.class})
        assert :ok = GenServer.call(peer, {:ship_now, ref})
      end)

      assert %{owner: @peer_writer, claim: 1} = Stub.stored_manifest(ref)

      # The operative comes back here. Adoption is what entitles this node to
      # take the lineage back — and it costs one claim, so the counter records
      # how many times ownership actually moved.
      GenServer.cast(Shipper, {:adopt, ctx.class})
      assert :ok = Shipper.ship_now(ref)
      assert %{owner: owner, claim: 2} = Stub.stored_manifest(ref)
      assert owner == this_writer()
    end

    test "a bid is a compare-and-set, so simultaneous claimants cannot both win", _ctx do
      # Below the shipper, because the race it closes cannot be staged through
      # one mailbox: two nodes that read the same counter both bid the same
      # number, and only the store can order them.
      key = "manifest/e/cls/proc/k"

      assert :ok = Stub.put_if(key, "first", {:claim, 1}, [])
      assert {:error, {:claim_held, 1}} = Stub.put_if(key, "second", {:claim, 1}, [])
      assert {:ok, "first"} = Stub.get(key, [])

      # The winner still extends its own lineage at the claim it holds.
      assert :ok = Stub.put_if(key, "again", {:extend, 1}, [])
      assert {:ok, "again"} = Stub.get(key, [])
    end
  end

  test "CONTROL: a node's own collection can never race its own ship", ctx do
    ref = {:entity, ctx.entity_id}

    TestHelpers.write_wal(ctx.entity_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    TestHelpers.append_wal(ctx.entity_id, 4..5)

    # A collection queues on the shipper's own mailbox, so it is served after
    # the ship it would have raced. Single-node safety is structural, and stays
    # true independently of the grace.
    send(Shipper, :gc_tick)
    assert :ok = Shipper.ship_now(ref)

    manifest = Stub.stored_manifest(ref)
    assert Enum.all?(Manifest.blob_keys(manifest), &(&1 in Stub.keys()))

    TestHelpers.wipe_entity(ctx.entity_id)
    capture_log(fn -> assert :ok = DurabilityTier.hydrate_operative(ctx.class) end)
    assert Artifact.seq_bounds(ref) == {0, 5}
  end

  # A second controller: its own (empty) state root, so its backstop sweep finds
  # none of this node's references — exactly what a peer holds in a real
  # deployment. Only the backend is shared.
  defp start_peer_node(peer_root, writer \\ @peer_writer) do
    own_root = Application.get_env(:flezha, :state_root)
    Application.put_env(:flezha, :state_root, peer_root)
    # The writer id is read once at init, so it only has to be in place across
    # the start — which is also what makes two shippers in one BEAM distinct
    # writers at all, since they share a `node()`.
    Application.put_env(:flezha, :durability_writer_id, writer)
    {:ok, pid} = GenServer.start(Shipper, [])
    # Force the initial-sweep continue to finish before the id goes back.
    _ = :sys.get_state(pid)
    Application.delete_env(:flezha, :durability_writer_id)
    Application.put_env(:flezha, :state_root, own_root)
    on_exit(fn -> NodeTestHelper.ensure_stopped(pid) end)
    pid
  end

  defp this_writer(), do: to_string(node())

  # The parked set as the store lays it out — an index line plus a payload blob.
  # Written directly: the case is about who may ship the directory, not about
  # how a park gets into it.
  defp write_park(class, payload) do
    dir = DlqStore.proc_dir(class, "proc")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "entries.jsonl"), ~s({"id":"#{payload}"}\n))
    File.write!(Path.join(dir, "#{payload}.bin"), payload)
  end

  defp status_of(ref), do: Enum.find(Shipper.status(), &(&1.ref == ref))

  # The operative migrates: the peer adopts it and ships. The adopt is a cast and
  # the ship a call on the same mailbox, so both are served inside the window
  # where the peer's own state root is in place.
  defp peer_adopts_and_ships(ctx, ref) do
    peer = start_peer_node(ctx.peer_base)

    with_root(ctx.peer_base, fn ->
      TestHelpers.write_wal(ctx.entity_id, 0..5)
      GenServer.cast(peer, {:adopt, ctx.class})
      assert :ok = GenServer.call(peer, {:ship_now, ref})
    end)

    peer
  end

  # `all_refs/1` reads `:state_root` when collection runs, and app env is global
  # to the BEAM — so the peer's scope has to be in place for the duration of its
  # own pass. That is a cost of simulating two nodes in one VM, not a property
  # of the tier.
  defp peer_gc(peer, peer_root), do: with_root(peer_root, fn -> GenServer.call(peer, :gc) end)

  defp with_root(root, fun) do
    own_root = Application.get_env(:flezha, :state_root)
    Application.put_env(:flezha, :state_root, root)
    result = fun.()
    Application.put_env(:flezha, :state_root, own_root)
    result
  end

  # The blob of the highest range the entry names — the cut the peer just shipped.
  defp newest_range_blob(ref) do
    Stub.stored_manifest(ref).ranges |> Enum.max_by(& &1.to) |> Map.fetch!(:blob)
  end

  # Scoped to this ref's OWN blobs through the declared inverse of
  # `blob_key/2`: the stub store is one namespace shared by every case, so
  # whether some other ref has an unreferenced blob says nothing about this one
  # — and asserting store-wide made the case fail on test order alone.
  defp only_orphan(ref) do
    referenced = Manifest.blob_keys(Stub.stored_manifest(ref))
    mine = Enum.filter(Stub.keys(), &(Manifest.ref_of_blob_key(&1) == {:ok, ref}))
    [orphan] = mine -- referenced
    orphan
  end
end
