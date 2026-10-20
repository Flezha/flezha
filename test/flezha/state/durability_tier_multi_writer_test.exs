defmodule Flezha.State.DurabilityTierMultiWriterTest do
  @moduledoc """
  Collection with more than one controller on one backend — the configuration
  the tier exists to serve, since a fresh node adopting another's relays is
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
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  import ExUnit.CaptureLog

  alias Flezha.Layout.Table, as: Layout
  alias Flezha.Dlq.Store, as: DlqStore
  alias Flezha.Relay.Lifecycle
  alias Flezha.Relay.Overwatch
  alias Flezha.Relay.Replay
  alias Flezha.Service.HubManager
  alias Flezha.Shutdown
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.{Artifact, Hydrator, Manifest, Shipper}
  alias Flezha.State.{Adoption, Lineage, Paths, Ref, TestHelpers, WalWriter}
  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub
  alias FlezhaTest.Helper.{NodeTestHelper, OverwatchHost}

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

  setup %{base: base} do
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
    cell_id = {class, "task", "k1"}

    on_exit(fn ->
      HookedBackend.disarm()
      Stub.uninstall()
      File.rm_rf(peer_base)
    end)

    %{peer_base: peer_base, class: class, cell_id: cell_id}
  end

  test "a peer's collection cannot delete a blob whose manifest entry is still in flight",
       ctx do
    ref = {:cell, ctx.cell_id}

    TestHelpers.write_wal(ctx.cell_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    Shipper.adopt(ctx.class)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    peer = start_peer_node(ctx.peer_base)
    TestHelpers.append_wal(ctx.cell_id, 4..5)

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

    TestHelpers.wipe_cell(ctx.cell_id)
    assert :ok = DurabilityTier.hydrate_relay(ctx.class)
    assert Artifact.seq_bounds(ref) == {0, 5}
  end

  test "a FIRST ship is protected too, where no manifest entry exists to consult", ctx do
    ref = {:cell, ctx.cell_id}

    {:ok, _} = start_supervised(Shipper)
    peer = start_peer_node(ctx.peer_base)
    TestHelpers.write_wal(ctx.cell_id, 0..3)

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

    TestHelpers.wipe_cell(ctx.cell_id)
    assert :ok = DurabilityTier.hydrate_relay(ctx.class)
    assert Artifact.seq_bounds(ref) == {0, 3}
  end

  test "past the grace, a peer still leaves another node's reference alone", ctx do
    ref = {:cell, ctx.cell_id}

    TestHelpers.write_wal(ctx.cell_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    Shipper.adopt(ctx.class)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    TestHelpers.append_wal(ctx.cell_id, 4..5)
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
    ref = {:cell, ctx.cell_id}

    TestHelpers.write_wal(ctx.cell_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    Shipper.adopt(ctx.class)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    TestHelpers.append_wal(ctx.cell_id, 4..5)
    Stub.fail_next_manifest()
    assert {:error, :manifest_write_lost} = Shipper.ship_now(ref)
    orphan = only_orphan(ref)

    peer = start_peer_node(ctx.peer_base)

    # The relay is stopped and its state deleted, so now NO node holds local
    # state for this reference and scoping alone would strand the orphan.
    TestHelpers.wipe_cell(ctx.cell_id)

    Stub.advance_clock(120_000)
    assert {:ok, 0} = peer_gc(peer, ctx.peer_base), "past the grace, inside the abandoned window"
    assert orphan in Stub.keys()

    Stub.advance_clock(7_200_000)
    assert {:ok, 1} = peer_gc(peer, ctx.peer_base)
    refute orphan in Stub.keys()
  end

  describe "the platform stores under two writers" do
    test "one parked set has one shipper, and the loser stops quietly", ctx do
      ref = {:dlq, {ctx.class, "task"}}
      write_park(ctx.class, "parked-by-owner")

      {:ok, _} = start_supervised(Shipper)
      assert :ok = Shipper.ship_now(ref)
      assert Stub.stored_manifest(ref).owner == this_writer()

      # The relay migrates, and its parked failures move with it: the parks
      # are claim-fenced exactly like the Cell state, so the new node bids the
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
      auth_dir = Layout.dir(:auth)
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
    ref = {:cell, ctx.cell_id}

    TestHelpers.write_wal(ctx.cell_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    Shipper.adopt(ctx.class)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    # The peer holds the same reference with different records above the shipped
    # cut — what an ordinary migration leaves behind, since `pull_relay/3`
    # copies and nothing deletes the source.
    peer = start_peer_node(ctx.peer_base)

    with_root(ctx.peer_base, fn ->
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      TestHelpers.append_wal(ctx.cell_id, 4..5, "peer")
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
    TestHelpers.append_wal(ctx.cell_id, 4..5, "owner")
    assert {:error, {:superseded, @peer_writer}} = Shipper.ship_now(ref)

    assert {:ok, ^peer_bytes} = Stub.get(peer_blob, []),
           "the peer's artifact must survive a losing writer's upload of the same seq range"
  end

  describe "who owns a lineage" do
    test "an adopting node takes the lineage over and the loser stops quietly", ctx do
      ref = {:cell, ctx.cell_id}

      # The loser still holds the relay's adoption, as a late `:adopt` leaves
      # it: one that does not waits rather than extend, and learns nothing.
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert Stub.stored_manifest(ref).owner == this_writer()

      peer = peer_adopts_and_ships(ctx, ref)

      # Claim 1 was consumed by the first owner: an unclaimed lineage starts at
      # zero and the first ship bids against it, which is what makes two nodes
      # first-shipping one reference resolve to a winner.
      assert %{owner: @peer_writer, claim: 2} = Stub.stored_manifest(ref)
      assert Process.alive?(peer)

      # The loser's backstop stops, and it is NOT an alarm: a handover produces
      # one of these every time, and `pending_since_ms` must clear with it or the
      # reference reports a permanent RPO breach. (A `ship_now` caller waiting
      # on the ship keeps the clock running until it stops waiting.)
      TestHelpers.append_wal(ctx.cell_id, 6..7)
      :ok = Shipper.sweep_now()
      assert %{superseded_by: @peer_writer, pending_since_ms: nil} = status_of(ref)
      assert DurabilityTier.breaches() == []
    end

    test "a node that lost the relay cannot bid its lineage back", ctx do
      ref = {:cell, ctx.cell_id}

      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()

      peer_adopts_and_ships(ctx, ref)

      # This node still holds every file — `pull_relay/3` copies and nothing
      # deletes the source — so "has the state" would let it claim straight
      # back. Only never having adopted stops it.
      :ok = Shipper.sweep_now()
      assert %{owner: @peer_writer} = Stub.stored_manifest(ref)
    end

    test "a restart extends its own lineage instead of bidding against itself", ctx do
      ref = {:cell, ctx.cell_id}

      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{owner: owner, claim: 1} = Stub.stored_manifest(ref)

      # A restart loses the manifest cache but keeps the writer id, so the next
      # ship, once the relay binds here again, re-reads the entry and finds
      # itself already the owner.
      :ok = stop_supervised(Shipper)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      TestHelpers.append_wal(ctx.cell_id, 4..5)
      assert :ok = Shipper.ship_now(ref)

      assert %{owner: ^owner, claim: 1} = Stub.stored_manifest(ref),
             "a restarted owner must not consume a claim"
    end

    test "each adoption hands the lineage on and consumes exactly one claim", ctx do
      ref = {:cell, ctx.cell_id}

      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      peer = start_peer_node(ctx.peer_base)

      with_root(ctx.peer_base, fn ->
        TestHelpers.write_wal(ctx.cell_id, 0..3)
        GenServer.cast(peer, {:adopt, ctx.class})
        assert :ok = GenServer.call(peer, {:ship_now, ref})
      end)

      assert %{owner: @peer_writer, claim: 1} = Stub.stored_manifest(ref)

      # The relay comes back here. Adoption is what entitles this node to
      # take the lineage back — and it costs one claim, so the counter records
      # how many times ownership actually moved.
      GenServer.cast(Shipper, {:adopt, ctx.class})
      assert :ok = Shipper.ship_now(ref)
      assert %{owner: owner, claim: 2} = Stub.stored_manifest(ref)
      assert owner == this_writer()
    end

    # fails when: a node that stayed up while its relay ran elsewhere keeps the
    # entry it cached naming itself owner, so after the relay moves back its
    # ship bids `{:extend, old}`, the backend answers `claim_held`, and the node
    # records itself superseded while it is the one running the relay.
    test "a node the relay moves back to ships again past the entry it cached", ctx do
      ref = {:cell, ctx.cell_id}
      ship_stale_here_newer_on_peer(ctx, ref)
      assert %{owner: @peer_writer, claim: 2} = Stub.stored_manifest(ref)

      # The relay moves back: the bind adopts the newer copy, then the lineage.
      assert :ok = Adoption.adopt_newest(ctx.class, :backend)
      GenServer.cast(Shipper, {:adopt, ctx.class})
      TestHelpers.append_wal(ctx.cell_id, 6..7)

      assert :ok = Shipper.ship_now(ref)
      assert %{owner: owner, claim: 3} = Stub.stored_manifest(ref)
      assert owner == this_writer()
      assert %{superseded_by: nil} = status_of(ref)
    end

    # fails when: a claim refused to a node that once adopted the relay is
    # re-bid instead of recorded as lost. Its adoption stands until its teardown
    # releases it, so that node would fence the writer that runs it now.
    test "a node the relay moved away from stays superseded though it once adopted", ctx do
      ref = {:cell, ctx.cell_id}

      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      GenServer.cast(Shipper, {:adopt, ctx.class})
      assert :ok = Shipper.ship_now(ref)

      peer_adopts_and_ships(ctx, ref)

      TestHelpers.append_wal(ctx.cell_id, 4..5)
      assert {:error, {:superseded, @peer_writer}} = Shipper.ship_now(ref)
      assert %{owner: @peer_writer, claim: 2} = Stub.stored_manifest(ref)
    end

    # fails when: an adoption outlives the relay on this node, so a reference
    # it never shipped, which it has no entry cached for, is bid above the
    # writer that runs the relay now: the remote cut goes back to this node's
    # older files and the relay's return keeps them.
    test "a node the relay left never bids on a reference it never shipped", ctx do
      k1 = {:cell, ctx.cell_id}
      k2_id = {ctx.class, "task", "k2"}
      k2 = {:cell, k2_id}

      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{claim: 1} = Stub.stored_manifest(k1)
      TestHelpers.write_wal(k2_id, 0..3)

      relay_leaves_here(ctx.class, :moved)
      peer = start_peer_node(ctx.peer_base)

      newer =
        with_root(ctx.peer_base, fn ->
          TestHelpers.write_wal(ctx.cell_id, 0..3)
          TestHelpers.write_wal(k2_id, 0..3)
          TestHelpers.append_wal(k2_id, 4..5, "peer")
          GenServer.cast(peer, {:adopt, ctx.class})
          assert :ok = GenServer.call(peer, :sweep_now)
          File.read!(Paths.wal_path(k2_id))
        end)

      :ok = Shipper.sweep_now()
      assert %{owner: @peer_writer, claim: 1} = shipped = Stub.stored_manifest(k2)
      assert Manifest.shipped_through(shipped) == 5
      assert %{superseded_by: @peer_writer} = status_of(k2)

      # The relay comes back through the bind: the newer copy, then the lineage.
      assert :ok = Adoption.adopt_newest(ctx.class, :backend)
      assert File.read!(Paths.wal_path(k2_id)) == newer
      Shipper.adopt(ctx.class)
      assert :ok = Shipper.ship_now(k2)
      assert %{owner: owner, claim: 2} = shipped = Stub.stored_manifest(k2)
      assert owner == this_writer()
      assert Manifest.shipped_through(shipped) == 5
    end

    # fails when: the adoption is withdrawn only by the old Overwatch's teardown,
    # so in a HotSwap move, where the old process lives until the new one has
    # registered, a backstop tick on the old node bids a reference it never
    # shipped above the claim the new node already landed, and the remote cut
    # goes back to the old node's older files.
    test "a node the relay is moving from never bids above the new node's claim", ctx do
      k1 = {:cell, ctx.cell_id}
      k2_id = {ctx.class, "task", "k2"}
      k2 = {:cell, k2_id}

      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{claim: 1} = Stub.stored_manifest(k1)
      TestHelpers.write_wal(k2_id, 0..3)

      # The move starts: the old Overwatch hands its state over and lives on.
      _ = Overwatch.prepare_handover_state(OverwatchHost.state(ctx.class))
      assert_received :calls_handed_over

      # The new node binds and ships while the old Overwatch is still alive.
      peer = start_peer_node(ctx.peer_base)

      with_root(ctx.peer_base, fn ->
        TestHelpers.write_wal(ctx.cell_id, 0..3)
        TestHelpers.write_wal(k2_id, 0..3)
        TestHelpers.append_wal(k2_id, 4..5, "peer")
        GenServer.cast(peer, {:adopt, ctx.class})
        assert :ok = GenServer.call(peer, :sweep_now)
      end)

      assert %{owner: @peer_writer, claim: 1} = Stub.stored_manifest(k2)

      # The old node's backstop tick in the gap, before its lineage step runs.
      :ok = Shipper.sweep_now()
      shipped = Stub.stored_manifest(k2)
      assert %{owner: @peer_writer, claim: 1} = shipped
      assert Manifest.shipped_through(shipped) == 5

      relay_leaves_here(ctx.class, :moved)
    end

    # fails when: two histories of one lineage are compared by epoch and length
    # alone, so the node that kept unshipped turns past the cut another node
    # continued from keeps its longer, abandoned history when the relay returns.
    test "a returning node takes the history its relay continued, not the longer one it left",
         ctx do
      ref = {:cell, ctx.cell_id}
      FlezhaTest.Case.install([:state_tree])
      write_through_writer(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      write_through_writer(ctx.cell_id, 4..7)

      peer = start_peer_node(ctx.peer_base)

      continued =
        with_root(ctx.peer_base, fn ->
          assert :ok = DurabilityTier.hydrate_relay(ctx.class)
          assert Artifact.seq_bounds(ref) == {0, 3}
          write_through_writer(ctx.cell_id, 4..5)
          GenServer.cast(peer, {:adopt, ctx.class})
          assert :ok = GenServer.call(peer, {:ship_now, ref})
          _ = :sys.get_state(peer)
          File.read!(Paths.wal_path(ctx.cell_id))
        end)

      assert %{owner: @peer_writer} = Stub.stored_manifest(ref)

      assert :ok = Adoption.adopt_newest(ctx.class, :backend)
      assert File.read!(Paths.wal_path(ctx.cell_id)) == continued
    end

    # fails when: an adoption that lands after the relay's release still lets
    # this node bid its own lineage over the node that continued it.
    test "a late adoption on the node the relay left never bids over its continuation", ctx do
      ref = {:cell, ctx.cell_id}
      FlezhaTest.Case.install([:state_tree])
      write_through_writer(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{claim: 1} = Stub.stored_manifest(ref)

      relay_leaves_here(ctx.class, :moved)
      peer_takes_turns(ctx, [4])
      assert %{owner: @peer_writer, claim: claim} = Stub.stored_manifest(ref)

      write_through_writer(ctx.cell_id, 4..6)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{owner: @peer_writer, claim: ^claim} = Stub.stored_manifest(ref)
      assert %{superseded_by: @peer_writer, pending_since_ms: nil} = status_of(ref)
    end

    # fails when: the node a relay left extends the entry it owns with the
    # turns it kept, before the node the relay moved to claims it.
    test "a node the relay left does not extend its entry with the turns it kept", ctx do
      ref = {:cell, ctx.cell_id}
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()

      relay_leaves_here(ctx.class, :moved)
      TestHelpers.append_wal(ctx.cell_id, 4..6)
      :ok = Shipper.sweep_now()

      assert Manifest.shipped_through(Stub.stored_manifest(ref)) == 3
      assert %{pending_since_ms: nil, superseded_by: nil} = status_of(ref)
    end

    # fails when: a restarted node ships a relay it has not bound again, or
    # counts the reference as owed and latches an RPO alarm for it.
    test "after a restart a reference waits, not owed, until its bind adopts", ctx do
      ref = {:cell, ctx.cell_id}
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()

      :ok = stop_supervised(Shipper)
      TestHelpers.append_wal(ctx.cell_id, 4..5)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      assert Manifest.shipped_through(Stub.stored_manifest(ref)) == 3
      assert %{pending_since_ms: nil} = status_of(ref)
      assert DurabilityTier.breaches() == []

      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert Manifest.shipped_through(Stub.stored_manifest(ref)) == 5
    end

    # fails when: a ship continuing the entry's lineage keeps the entry's
    # ranges above the fork, so the continuation's records past it never ship.
    test "a continuation drops the entry's turns above its fork", ctx do
      ref = {:cell, ctx.cell_id}
      FlezhaTest.Case.install([:state_tree])
      write_through_writer(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()

      peer = start_peer_node(ctx.peer_base)
      with_root(ctx.peer_base, fn -> assert :ok = DurabilityTier.hydrate_relay(ctx.class) end)
      write_through_writer(ctx.cell_id, 4..5)
      :ok = Shipper.sweep_now()
      assert Manifest.shipped_through(Stub.stored_manifest(ref)) == 5

      continued = peer_takes_turns(ctx, 4..7, peer)
      TestHelpers.wipe_cell(ctx.cell_id)
      assert :ok = DurabilityTier.hydrate_relay(ctx.class)
      assert File.read!(Paths.wal_path(ctx.cell_id)) == continued
    end

    # fails when: an owner whose lineage the entry does not know ships onto
    # the entry's ranges, so a hydration splices two unrelated histories.
    test "an unrelated lineage re-ships its copy whole", ctx do
      ref = {:cell, ctx.cell_id}
      FlezhaTest.Case.install([:state_tree])
      peer_takes_turns(ctx, 0..2)
      write_through_writer(ctx.cell_id, 0..5)
      local = File.read!(Paths.wal_path(ctx.cell_id))
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{owner: owner} = Stub.stored_manifest(ref)
      assert owner == this_writer()

      TestHelpers.wipe_cell(ctx.cell_id)
      assert :ok = DurabilityTier.hydrate_relay(ctx.class)
      assert File.read!(Paths.wal_path(ctx.cell_id)) == local
    end

    # fails when: a relay stopped here is released before its last ships land,
    # so a reference whose takeover claim had not landed is recorded as lost to
    # the writer it moved from, and the turns it took here never reach the
    # backend.
    test "a relay stopped here ships its last turns before it is released", ctx do
      ref = {:cell, ctx.cell_id}
      k2_id = {ctx.class, "task", "k2"}
      peer = start_peer_node(ctx.peer_base)

      with_root(ctx.peer_base, fn ->
        TestHelpers.write_wal(ctx.cell_id, 0..3)
        GenServer.cast(peer, {:adopt, ctx.class})
        assert :ok = GenServer.call(peer, :sweep_now)
      end)

      # The relay moved here and took turns; its takeover claim has not landed.
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      _ = :sys.get_state(Shipper)
      TestHelpers.write_wal(ctx.cell_id, 0..5)

      relay_leaves_here(ctx.class, :stopped)
      :ok = Shipper.sweep_now()

      assert %{owner: owner, claim: 2} = shipped = Stub.stored_manifest(ref)
      assert owner == this_writer()
      assert Manifest.shipped_through(shipped) == 5
      assert %{superseded_by: nil, pending_since_ms: nil} = status_of(ref)

      # Nothing is owed any more, so the release is complete: a reference
      # written after it is not bid above the writer that ships it next.
      TestHelpers.write_wal(k2_id, 0..3)

      with_root(ctx.peer_base, fn ->
        TestHelpers.write_wal(k2_id, 0..5)
        GenServer.cast(peer, {:adopt, ctx.class})
        assert :ok = GenServer.call(peer, :sweep_now)
      end)

      :ok = Shipper.sweep_now()
      assert %{owner: @peer_writer, claim: 1} = Stub.stored_manifest({:cell, k2_id})
    end

    # fails when: hydration skips every reference with local files, so the node
    # a relay returns to binds from the copy it was left holding and loses
    # the turns taken elsewhere.
    test "a returning node adopts the newer lineage over the stale copy it kept", ctx do
      ref = {:cell, ctx.cell_id}
      newer = ship_stale_here_newer_on_peer(ctx, ref)
      sidecar = Path.join(Paths.class_dir(ctx.class), "kind")
      File.write!(sidecar, "flow")

      assert :ok = Adoption.adopt_newest(ctx.class, :backend)

      assert File.read!(Paths.wal_path(ctx.cell_id)) == newer
      assert File.read!(sidecar) == "flow", "a class-level sidecar is not part of the copy"
      refute File.exists?(Paths.aside_path(ctx.class, :set_aside))
    end

    test "CONTROL: a local copy newer than the remote one is kept", ctx do
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      TestHelpers.append_wal(ctx.cell_id, 4..7)
      local = File.read!(Paths.wal_path(ctx.cell_id))

      assert :ok = Adoption.adopt_newest(ctx.class, :backend)
      assert File.read!(Paths.wal_path(ctx.cell_id)) == local
    end

    # fails when: a controller's own shipped cut reads as nothing to adopt, so
    # a controller that lost its disk binds the relay empty.
    test "a controller that lost its disk adopts its own shipped cut", ctx do
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      shipped = File.read!(Paths.wal_path(ctx.cell_id))
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      File.rm_rf!(Paths.class_dir(ctx.class))

      assert :ok = Adoption.adopt_newest(ctx.class, :backend)
      assert File.read!(Paths.wal_path(ctx.cell_id)) == shipped
    end

    # fails when: re-landing this controller's own cut marks it landed, so the
    # first record after each bind mints a lineage no other node continues.
    test "a relay re-landing its own cut at each bind mints nothing", ctx do
      ref = {:cell, ctx.cell_id}
      FlezhaTest.Case.install([:state_tree])
      write_through_writer(ctx.cell_id, 0..1)
      {:ok, _} = start_supervised(Shipper)
      Shipper.adopt(ctx.class)
      :ok = Shipper.sweep_now()
      assert %{id: id} = Ref.lineage(ref)

      for seqs <- [2..3, 4..5] do
        File.rm_rf!(Paths.class_dir(ctx.class))
        assert :ok = Adoption.adopt_newest(ctx.class, :backend)
        write_through_writer(ctx.cell_id, seqs)
        :ok = Shipper.sweep_now()
      end

      assert Ref.lineage(ref) == %{id: id, trail: []}
      assert %{lineage: %{id: ^id}} = shipped = Stub.stored_manifest(ref)
      assert Manifest.shipped_through(shipped) == 5
    end

    # fails when: a landing that stopped after its data and before its lineage
    # record reads as a lineage of its own, or the next adoption keeps it.
    test "a landing cut short before its lineage record is replaced by the next", ctx do
      ref = {:cell, ctx.cell_id}
      FlezhaTest.Case.install([:state_tree])
      shipped = peer_takes_turns(ctx, 0..3)
      File.mkdir_p!(Path.dirname(Paths.wal_path(ctx.cell_id)))
      File.write!(Paths.wal_path(ctx.cell_id), shipped)
      assert Ref.lineage(ref) == nil

      assert :ok = Adoption.adopt_newest(ctx.class, :backend)
      assert %{lineage: %{id: id}} = Stub.stored_manifest(ref)
      assert %{id: ^id, landed: true, epoch: 0} = Lineage.read(ctx.cell_id).record
      assert File.read!(Paths.wal_path(ctx.cell_id)) == shipped
    end

    # fails when: a Cell this controller shipped and lost since stays absent
    # while the class holds other references, so the bind starts it empty and
    # its first ship prunes the shipped cut away (`Manifest.prune_above/2`).
    test "a Cell lost here comes back from this controller's own cut", ctx do
      ref = {:cell, ctx.cell_id}
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      shipped = File.read!(Paths.wal_path(ctx.cell_id))
      TestHelpers.write_wal({ctx.class, "task", "k2"}, 0..1)
      {:ok, _} = start_supervised(Shipper)
      :ok = Shipper.sweep_now()
      Enum.each(Ref.files(ref), &File.rm/1)
      relay = %Flezha.Relay.Builder{id: ctx.class, factory_id: "f"}

      Replay.adopt_unlanded_state(
        %Flezha.Relay.Overwatch{id: ctx.class, builder: relay},
        relay,
        self()
      )

      assert File.read!(Paths.wal_path(ctx.cell_id)) == shipped
    end

    # fails when: a Cell created here inside the last ship interval, which no
    # manifest names yet, is dropped with the copy the newer cut replaces.
    test "a Cell no manifest names yet survives the adoption", ctx do
      ref = {:cell, ctx.cell_id}
      newer = ship_stale_here_newer_on_peer(ctx, ref)
      unshipped = {ctx.class, "task", "unshipped"}
      TestHelpers.write_wal(unshipped, 0..1, "local")
      local = File.read!(Paths.wal_path(unshipped))

      assert :ok = Adoption.adopt_newest(ctx.class, :backend)

      assert File.read!(Paths.wal_path(ctx.cell_id)) == newer
      assert File.read!(Paths.wal_path(unshipped)) == local
    end

    # fails when: the stale copy is removed before the newer one has landed, so
    # an unreadable remote lineage costs the node the only copy it had.
    test "a newer lineage that cannot be read leaves the stale copy in place", ctx do
      ref = {:cell, ctx.cell_id}
      ship_stale_here_newer_on_peer(ctx, ref)
      stale = File.read!(Paths.wal_path(ctx.cell_id))
      Stub.delete(newest_range_blob(ref), [])
      on_exit(NodeTestHelper.override_log_level(:warning))

      log = capture_log(fn -> assert :not_landed = Adoption.adopt_newest(ctx.class, :backend) end)

      assert log =~ "which the backend does not hold"
      assert log =~ "did not land whole; the local copy stays"
      assert File.read!(Paths.wal_path(ctx.cell_id)) == stale
      refute File.exists?(Paths.aside_path(ctx.class, :set_aside))
    end

    # fails when: a node that held nothing keeps the part of a newer lineage
    # that hydrated, so it binds Cells without the rest of their instance.
    test "a node that held nothing stays cold when the newer lineage does not hydrate whole",
         ctx do
      other = {ctx.class, "task", "k2"}
      peer_adopts_and_ships(ctx, {:cell, ctx.cell_id}, [other])
      Stub.delete(newest_range_blob({:cell, other}), [])
      on_exit(NodeTestHelper.override_log_level(:warning))

      log = capture_log(fn -> assert :not_landed = Adoption.adopt_newest(ctx.class, :backend) end)

      refute File.exists?(Paths.wal_path(ctx.cell_id))
      refute File.exists?(Paths.class_dir(ctx.class))
      refute File.exists?(Paths.aside_path(ctx.class, :set_aside))
      assert log =~ "which the backend does not hold"
      assert log =~ "did not land whole"
    end

    # fails when: the bind-time restore enumeration hydrates every shipped
    # reference whose local files are absent, so the part of a newer lineage
    # the adoption refused to keep lands anyway and the bind restores it.
    test "the bind-time restore keeps a node cold that the adoption left cold", ctx do
      other = {ctx.class, "task", "k2"}
      peer_adopts_and_ships(ctx, {:cell, ctx.cell_id}, [other])
      Stub.delete(newest_range_blob({:cell, other}), [])
      on_exit(NodeTestHelper.override_log_level(:warning))

      capture_log(fn ->
        Adoption.adopt_newest(ctx.class, :backend)
        refute File.exists?(Paths.wal_path(ctx.cell_id))

        assert Replay.resume_cell_count(ctx.class) == 0
      end)

      refute File.exists?(Paths.wal_path(ctx.cell_id)),
             "the readable Cell of a lineage that did not hydrate whole was hydrated"
    end

    test "a bid is a compare-and-set, so simultaneous claimants cannot both win", _ctx do
      # Below the shipper, because the race it closes cannot be staged through
      # one mailbox: two nodes that read the same counter both bid the same
      # number, and only the store can order them.
      key = "manifest/e/cls/task/k"

      assert :ok = Stub.put_if(key, "first", {:claim, 1}, [])
      assert {:error, {:claim_held, 1}} = Stub.put_if(key, "second", {:claim, 1}, [])
      assert {:ok, "first"} = Stub.get(key, [])

      # The winner still extends its own lineage at the claim it holds.
      assert :ok = Stub.put_if(key, "again", {:extend, 1}, [])
      assert {:ok, "again"} = Stub.get(key, [])
    end
  end

  test "CONTROL: a node's own collection can never race its own ship", ctx do
    ref = {:cell, ctx.cell_id}

    TestHelpers.write_wal(ctx.cell_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    Shipper.adopt(ctx.class)
    :ok = Shipper.sweep_now()
    assert :ok = Shipper.ship_now(ref)

    TestHelpers.append_wal(ctx.cell_id, 4..5)

    # A collection queues on the shipper's own mailbox, so it is served after
    # the ship it would have raced. Single-node safety is structural, and stays
    # true independently of the grace.
    send(Shipper, :gc_tick)
    assert :ok = Shipper.ship_now(ref)

    manifest = Stub.stored_manifest(ref)
    assert Enum.all?(Manifest.blob_keys(manifest), &(&1 in Stub.keys()))

    TestHelpers.wipe_cell(ctx.cell_id)
    capture_log(fn -> assert :ok = DurabilityTier.hydrate_relay(ctx.class) end)
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

  # The relay's Overwatch ends on this node: the teardown step that releases
  # its lineage. A move keeps the relay's declared entry, a stop removes it.
  defp relay_leaves_here(class, departure) do
    declared = if departure == :moved, do: [%{id: class, factory_id: "chat"}], else: []
    Mimic.stub(HubManager, :declared_relays, fn _namespace -> declared end)
    overwatch = OverwatchHost.state(class)
    steps = Enum.filter(Lifecycle.shutdown_steps(), &(&1.name == :lineage))
    assert {:ok, _report} = Shutdown.run(:process, {overwatch, :shutdown}, 5_000, steps)
  end

  # The parked set as the store lays it out — an index line plus a payload blob.
  # Written directly: the case is about who may ship the directory, not about
  # how a park gets into it.
  defp write_park(class, payload) do
    dir = DlqStore.task_dir(class, "task")
    File.mkdir_p!(dir)
    File.write!(Path.join(dir, "entries.jsonl"), ~s({"id":"#{payload}"}\n))
    File.write!(Path.join(dir, "#{payload}.bin"), payload)
  end

  defp status_of(ref), do: Enum.find(Shipper.status(), &(&1.ref == ref))

  # The relay migrates: the peer adopts it and ships. The adopt is a cast and
  # the ship a call on the same mailbox, so both are served inside the window
  # where the peer's own state root is in place. `more_cells` are written and
  # shipped beside `ctx.cell_id`.
  defp peer_adopts_and_ships(ctx, ref, more_cells \\ []) do
    peer = start_peer_node(ctx.peer_base)

    with_root(ctx.peer_base, fn ->
      Enum.each([ctx.cell_id | more_cells], &TestHelpers.write_wal(&1, 0..5))
      GenServer.cast(peer, {:adopt, ctx.class})
      assert :ok = GenServer.call(peer, {:ship_now, ref})
      Enum.each(more_cells, &assert(:ok = GenServer.call(peer, {:ship_now, {:cell, &1}})))
    end)

    peer
  end

  # The peer hydrates whatever is shipped when it holds nothing, takes `seqs`
  # through a writer (a new lineage id) and ships them. Returns the peer's WAL.
  defp peer_takes_turns(ctx, seqs, peer \\ nil) do
    peer = peer || start_peer_node(ctx.peer_base)

    with_root(ctx.peer_base, fn ->
      unless File.exists?(Paths.wal_path(ctx.cell_id)),
        do: assert(:ok = DurabilityTier.hydrate_relay(ctx.class))

      write_through_writer(ctx.cell_id, seqs, "peer")
      GenServer.cast(peer, {:adopt, ctx.class})
      assert :ok = GenServer.call(peer, :sweep_now)
      File.read!(Paths.wal_path(ctx.cell_id))
    end)
  end

  # The relay moves away and back: this node shipped 0..3 and kept the files,
  # the peer adopted it, took turns 4..5 and shipped them. Returns the peer's WAL.
  defp ship_stale_here_newer_on_peer(ctx, ref) do
    TestHelpers.write_wal(ctx.cell_id, 0..3)
    {:ok, _} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()

    peer = start_peer_node(ctx.peer_base)

    with_root(ctx.peer_base, fn ->
      TestHelpers.write_wal(ctx.cell_id, 0..3)
      TestHelpers.append_wal(ctx.cell_id, 4..5, "peer")
      GenServer.cast(peer, {:adopt, ctx.class})
      assert :ok = GenServer.call(peer, {:ship_now, ref})
      # The adopt also queued a ship; it must run while the peer's root is in
      # place, or it ships this node's stale files as the peer's lineage.
      _ = :sys.get_state(peer)
      File.read!(Paths.wal_path(ctx.cell_id))
    end)
  end

  # `all_refs/1` reads `:state_root` when collection runs, and app env is global
  # to the BEAM — so the peer's scope has to be in place for the duration of its
  # own pass. That is a cost of simulating two nodes in one VM, not a property
  # of the tier.
  defp peer_gc(peer, peer_root), do: with_root(peer_root, fn -> GenServer.call(peer, :gc) end)

  # Records taken through a writer, as a relay's turns are: the writer is where
  # a Cell's dedup and lineage files are written.
  defp write_through_writer(cell_id, seqs, tag \\ "turn") do
    {:ok, srv} = Flezha.State.Supervisor.find_or_start(cell_id)
    header = TestHelpers.stub_header()
    for n <- seqs, do: :ok = WalWriter.append(srv, n, header, "#{tag}-#{n}")
    TestHelpers.close_and_await(srv, cell_id)
  end

  # The peer's state root and writer id, so a hydration there reads an entry
  # this node wrote as another controller's.
  defp with_root(root, fun) do
    own_root = Application.get_env(:flezha, :state_root)
    Application.put_env(:flezha, :state_root, root)
    Application.put_env(:flezha, :durability_writer_id, @peer_writer)
    result = fun.()
    Application.delete_env(:flezha, :durability_writer_id)
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
