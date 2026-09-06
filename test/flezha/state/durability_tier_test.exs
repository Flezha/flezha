defmodule Flezha.State.DurabilityTierTest do
  @moduledoc """
  The remote durability tier: what ships, what supersedes what, and what a
  recovery can see.

  Every case here is about one of the three properties the tier rests on — the
  manifest is written last (so a torn upload is invisible), local truth is
  mirrored rather than accumulated (so compaction and correction supersede
  correctly), and nothing on an ack path ever waits for the backend.

  Records and snapshots are written as bytes rather than driven through a live
  `WalWriter`: the tier reads the on-disk framing, so writing that framing
  directly is both the real input and the only way to pin a seq range exactly.
  """
  # async: false: writes Application env; a shared on-disk root; registered singletons.
  use FlezhaTest.Case, fixtures: [:tmp_app_data, :env], async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.{AuthState, NamespaceStore, QuotaCounters, RoleStore, TokenStore}
  alias Flezha.Dlq
  alias Flezha.Operative.ClassRegistry
  alias Flezha.State.DurabilityTier
  alias Flezha.State.DurabilityTier.{Artifact, Hydrator, Manifest, Shipper}
  alias Flezha.State.{Outbox, Paths, TestHelpers}
  alias FlezhaTest.Helper.AuthTestHelper
  alias FlezhaTest.Helper.DurabilityBackendStub, as: Stub
  alias FlezhaTest.Helper.NodeTestHelper

  setup %{base: base} do
    hub_root = Path.join(base, "process_hub")
    Application.put_env(:flezha, :process_hub_registry_root, hub_root)

    class = "dur_" <> Integer.to_string(System.unique_integer([:positive]))
    entity_id = {class, "proc", "k1"}

    on_exit(&Stub.uninstall/0)

    %{hub_root: hub_root, class: class, entity_id: entity_id}
  end

  # ---------- default off ----------

  describe "with no backend configured" do
    test "nothing is wired into the supervision tree and every entry point is inert", ctx do
      Application.delete_env(:flezha, :durability_tier)

      assert DurabilityTier.children() == []
      refute DurabilityTier.enabled?()
      assert DurabilityTier.ship({:entity, ctx.entity_id}) == :ok
      assert DurabilityTier.hydrate_operative(ctx.class) == :ok
      assert Hydrator.hydrate_cspecs() == :ok
      assert DurabilityTier.breaches() == []
      refute DurabilityTier.sync_class?(ctx.class)
      assert Shipper.status() == []
    end

    test "an unrecognised backend name is off, not a boot failure", ctx do
      Application.put_env(:flezha, :durability_tier, backend: :postgress)
      refute DurabilityTier.enabled?()
      assert DurabilityTier.children() == []
      assert DurabilityTier.ship({:entity, ctx.entity_id}) == :ok
    end
  end

  # ---------- the durability decision ----------

  describe "the durability posture" do
    test "a production boot that made no decision refuses to start" do
      Application.put_env(:flezha, :durability_tier, backend: :unset)
      assert DurabilityTier.unconfigured?()

      {error, printed} =
        with_io(:stderr, fn -> assert_raise RuntimeError, fn -> DurabilityTier.children() end end)

      # Whoever reads this is looking at a container that will not come up, so
      # the guidance has to be actionable without any other document — and it
      # has to survive the BEAM's crash-report formatting, which is why it is
      # printed as well as raised.
      for text <- [error.message, printed] do
        assert text =~ "FLEZHA_DURABILITY_BACKEND"
        assert text =~ "FLEZHA_DURABILITY_BACKEND=postgres"
        assert text =~ "FLEZHA_DURABILITY_BACKEND=none"
      end
    end

    test "config/runtime.exs resolves an unset variable to :unset in prod and :none in dev" do
      System.delete_env("FLEZHA_DURABILITY_BACKEND")

      assert backend_from_runtime_config(:prod) == :unset
      assert backend_from_runtime_config(:dev) == :none
    end

    test "an explicit opt-out is inert and reports the local-only posture", ctx do
      Application.put_env(:flezha, :durability_tier, backend: :none)

      refute DurabilityTier.unconfigured?()
      assert DurabilityTier.children() == []
      assert DurabilityTier.posture() == :local_only
      assert DurabilityTier.ship({:entity, ctx.entity_id}) == :ok
    end

    test "a configured backend reports the remote posture" do
      Stub.install()
      assert DurabilityTier.posture() == :remote_tier
    end
  end

  # ---------- ship and hydrate ----------

  describe "shipping a seq-addressed artifact" do
    setup ctx do
      Stub.install()
      TestHelpers.write_wal(ctx.entity_id, 0..4)
      start_shipper()
      ctx
    end

    test "the shipped manifest reproduces the local WAL byte for byte", ctx do
      before = File.read!(Paths.wal_path(ctx.entity_id))
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert File.read!(Paths.wal_path(ctx.entity_id)) == before
    end

    test "a second ship uploads only the records above the shipped cut", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      manifest = Stub.stored_manifest({:entity, ctx.entity_id})
      assert [%{from: 0, to: 4}] = Enum.map(manifest.ranges, &Map.take(&1, [:from, :to]))

      TestHelpers.append_wal(ctx.entity_id, 5..6)
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      assert [%{from: 0, to: 4}, %{from: 5, to: 6}] =
               Stub.stored_manifest({:entity, ctx.entity_id}).ranges
               |> Enum.map(&Map.take(&1, [:from, :to]))
    end

    test "an unchanged artifact re-ships nothing", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      writes = Stub.writes()

      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      assert Stub.writes() == writes
    end

    test "hydration never overwrites local state", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      File.write!(Paths.wal_path(ctx.entity_id), local_only_record())
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert File.read!(Paths.wal_path(ctx.entity_id)) == local_only_record()
    end

    test "the class-level input log ships and hydrates with its Entities", ctx do
      # It sits outside the per-Entity manifest prefix, so hydration has to ask
      # for it by name. Missing it is silent and fatal: the Entity comes back
      # with its consumed floor intact and no log to replay from, so the next
      # input lands at offset 0, below the floor, and is skipped as consumed.
      log_path = Paths.input_log_path(ctx.class)
      File.mkdir_p!(Path.dirname(log_path))
      File.write!(log_path, TestHelpers.wal_records(0..2))

      assert :ok = Shipper.ship_now({:input_log, ctx.class})

      File.rm!(log_path)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert File.read!(log_path) == TestHelpers.wal_records(0..2)
      assert File.dir?(Path.dirname(log_path)), "hydration must not replace the class dir"
    end

    test "a subset request fetches only the entity keys it names", ctx do
      other = {ctx.class, "proc", "k2"}
      TestHelpers.write_wal(other, 0..2)
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      assert :ok = Shipper.ship_now({:entity, other})

      TestHelpers.wipe_entity(ctx.entity_id)
      TestHelpers.wipe_entity(other)

      assert :ok = DurabilityTier.hydrate_entities(ctx.class, "proc", ["k1"])

      assert File.exists?(Paths.wal_path(ctx.entity_id))
      refute File.exists?(Paths.wal_path(other))
    end
  end

  # ---------- compaction and correction ----------

  describe "supersession" do
    setup ctx do
      Stub.install()
      TestHelpers.write_wal(ctx.entity_id, 0..5)
      start_shipper()
      ctx
    end

    test "a compaction snapshot supersedes the ranges below the WAL's new floor", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      # What compaction leaves behind: a snapshot at 3 and a WAL holding 4..5.
      File.write!(Paths.snapshot_path(ctx.entity_id, 3), "snapshot-at-3")
      TestHelpers.write_wal(ctx.entity_id, 4..5)
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      manifest = Stub.stored_manifest({:entity, ctx.entity_id})
      assert [%{seq: 3}] = Enum.map(manifest.snapshots, &Map.take(&1, [:seq]))
      refute Enum.any?(manifest.ranges, &(&1.to <= 3))

      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)
      assert File.read!(Paths.snapshot_path(ctx.entity_id, 3)) == "snapshot-at-3"
      assert seqs_in_wal(ctx.entity_id) == [4, 5]
    end

    test "a correction rewind supersedes the shipped ranges above it at a higher epoch", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      assert Stub.stored_manifest({:entity, ctx.entity_id}).epoch == 0

      # What `WalWriter.rewind_to(2, 1)` leaves behind: records 0..2 and a
      # bumped epoch. The records above the rewind are gone locally, and must
      # stop being referenced remotely.
      TestHelpers.write_wal(ctx.entity_id, 0..2)
      set_epoch(ctx.entity_id, 1)
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      manifest = Stub.stored_manifest({:entity, ctx.entity_id})
      assert manifest.epoch == 1
      assert Enum.all?(manifest.ranges, &(&1.to <= 2))

      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)
      assert seqs_in_wal(ctx.entity_id) == [0, 1, 2]
    end
  end

  # ---------- what a recovery has to put back besides the data ----------

  describe "recovery restores an Entity's local facts" do
    setup ctx do
      Stub.install()
      TestHelpers.write_wal(ctx.entity_id, 0..3)
      # A runner restart realigned this Entity, which is the ordinary case: the
      # epoch is bumped by `WalWriter.realign_epoch/4`, not only by a rewind.
      set_epoch(ctx.entity_id, 2)
      start_shipper()
      ctx
    end

    test "a recovered Entity keeps shipping instead of stopping on its own lineage", ctx do
      ref = {:entity, ctx.entity_id}
      assert :ok = Shipper.ship_now(ref)

      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      # The epoch's only home on disk is the dedup cache, which is not an
      # artifact. It does not gate the manifest write, but it is still
      # the supersession fact the manifest algebra reads, so a recovered Entity
      # that came back at epoch 0 would mis-order its own cuts.
      assert Outbox.read(ctx.entity_id).epoch == 2
      assert :ok = Shipper.ship_now(ref)
      refute Enum.any?(Shipper.status(), &(&1.ref == ref and &1.superseded_by != nil))
    end

    test "the restored flush cursor lags rather than leads", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      # A cursor restored ahead of the restored WAL would drop those effect
      # records from the boot re-drive (`seq > cursor`) and never dispatch their
      # outputs. `-1` re-drives everything, which sink dedup absorbs.
      assert Outbox.read(ctx.entity_id).outbox_flushed_seq == -1
    end

    test "the saga compensation markers survive a recovery", ctx do
      # Without these the resumed driver never takes its compensation branch and
      # resumes FORWARD, silently skipping a rollback it owes.
      :ok = Paths.write_compensation_marker(ctx.entity_id, :intent)
      :ok = Paths.write_compensation_marker(ctx.entity_id, :done)

      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert %{intent: true, done: true} = Paths.compensation_markers(ctx.entity_id)
    end

    test "an unset marker is not resurrected by a recovery", ctx do
      :ok = Paths.write_compensation_marker(ctx.entity_id, :intent)
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert %{intent: true, done: false} = Paths.compensation_markers(ctx.entity_id)
    end

    test "a marker appearing rewrites an otherwise unchanged manifest", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      assert Stub.stored_manifest({:entity, ctx.entity_id}).markers.done == false

      # No new records — only the marker. A cut comparison that ignored it would
      # skip the write and the marker would never ship.
      :ok = Paths.write_compensation_marker(ctx.entity_id, :done)
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      assert Stub.stored_manifest({:entity, ctx.entity_id}).markers.done
    end
  end

  # ---------- atomicity, fencing, isolation ----------

  describe "failure handling" do
    setup ctx do
      Stub.install()
      TestHelpers.write_wal(ctx.entity_id, 0..3)
      start_shipper()
      ctx
    end

    test "a crash between the blobs and the manifest leaves the previous entry authoritative",
         ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      first = Stub.stored_manifest({:entity, ctx.entity_id})

      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.fail_next_manifest()
      assert {:error, :manifest_write_lost} = Shipper.ship_now({:entity, ctx.entity_id})

      # The manifest still describes the first cut; the blobs uploaded for the
      # second are present but unreferenced, so a hydration cannot see them.
      assert Stub.stored_manifest({:entity, ctx.entity_id}).ranges == first.ranges
      TestHelpers.wipe_entity(ctx.entity_id)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)
      assert seqs_in_wal(ctx.entity_id) == [0, 1, 2, 3]
    end

    test "garbage collection removes the orphans and never a referenced blob", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})
      referenced = Manifest.blob_keys(Stub.stored_manifest({:entity, ctx.entity_id}))

      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.fail_next_manifest()
      assert {:error, :manifest_write_lost} = Shipper.ship_now({:entity, ctx.entity_id})

      # Past the grace, so the torn upload is genuinely orphaned rather than
      # possibly still in flight.
      Stub.advance_clock(120_000)

      assert {:ok, collected} = Shipper.gc()
      assert collected > 0
      assert Enum.all?(referenced, &(&1 in Stub.keys()))
    end

    test "an orphan younger than the grace is left for a later pass", ctx do
      assert :ok = Shipper.ship_now({:entity, ctx.entity_id})

      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.fail_next_manifest()
      assert {:error, :manifest_write_lost} = Shipper.ship_now({:entity, ctx.entity_id})

      # Derived from the store rather than constructed, so the case says nothing
      # about how a blob is named.
      referenced = Manifest.blob_keys(Stub.stored_manifest({:entity, ctx.entity_id}))
      prefix = Manifest.blob_key({:entity, ctx.entity_id}, "")
      blobs = Enum.filter(Stub.keys(), &String.starts_with?(&1, prefix))
      assert [orphan] = blobs -- referenced

      # No clock advance: this is exactly what a peer controller's in-flight
      # upload looks like from here, and the two are indistinguishable.
      assert {:ok, 0} = Shipper.gc()
      assert orphan in Stub.keys()
    end

    test "a claim held elsewhere stops shipping without raising an alarm", ctx do
      ref = {:entity, ctx.entity_id}
      :telemetry_test.attach_event_handlers(self(), [[:flezha, :state, :claim_superseded]])
      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.fence_at(9)

      assert {:error, {:superseded, _holder}} = Shipper.ship_now(ref)
      assert_receive {[:flezha, :state, :claim_superseded], _, _, _}
      assert Enum.any?(Shipper.status(), &(&1.ref == ref and &1.superseded_by != nil))

      # The reference is not retried — overwriting a writer that legitimately
      # took the lineage over is the one thing worse than not shipping.
      assert {:error, :superseded} = Shipper.ship_now(ref)
    end

    test "losing a claim does not leave a permanent RPO breach behind", ctx do
      ref = {:entity, ctx.entity_id}

      Application.put_env(
        :flezha,
        :durability_tier,
        Application.get_env(:flezha, :durability_tier) |> Keyword.put(:rpo_target_ms, 1)
      )

      # Unshipped local progress, so the reference is genuinely pending...
      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.set_down(true)
      assert {:error, :backend_down} = Shipper.ship_now(ref)
      # The breach is a millisecond past the failed ship.
      assert NodeTestHelper.wait_until(
               fn -> DurabilityTier.breaches() != [] end,
               1_000,
               1
             )

      assert [%{ref: ^ref}] = DurabilityTier.breaches()

      # ...and then another controller takes it over. A handover is not a
      # backend failure: leaving `pending_since_ms` set would report a breach
      # forever, and `/healthz` would read `degraded` after every migration the
      # cluster performs.
      Stub.set_down(false)
      Stub.fence_at(9)
      assert {:error, {:superseded, _holder}} = Shipper.ship_now(ref)
      assert DurabilityTier.breaches() == []
    end

    test "a backend outage retries with backoff and reports the reference as pending", ctx do
      ref = {:entity, ctx.entity_id}
      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.set_down(true)

      assert {:error, :backend_down} = Shipper.ship_now(ref)
      assert %{failures: 1, pending_since_ms: pending} = status_of(ref)
      assert is_integer(pending)

      Stub.set_down(false)
      assert :ok = Shipper.ship_now(ref)
      assert %{failures: 0, pending_since_ms: nil} = status_of(ref)
    end

    test "an RPO breach is reported once the target is exceeded", ctx do
      ref = {:entity, ctx.entity_id}

      Application.put_env(
        :flezha,
        :durability_tier,
        Application.get_env(:flezha, :durability_tier) |> Keyword.put(:rpo_target_ms, 1)
      )

      TestHelpers.append_wal(ctx.entity_id, 4..5)
      Stub.set_down(true)
      assert {:error, :backend_down} = Shipper.ship_now(ref)
      # The breach is a millisecond past the failed ship.
      assert NodeTestHelper.wait_until(
               fn -> DurabilityTier.breaches() != [] end,
               1_000,
               1
             )

      assert [%{ref: ^ref}] = DurabilityTier.breaches()

      # The breach clears the moment the backend catches up again — an alarm
      # that only ever latches is not an alarm.
      Stub.set_down(false)
      assert :ok = Shipper.ship_now(ref)
      assert DurabilityTier.breaches() == []
    end
  end

  # ---------- whole-file artifacts ----------

  describe "the registry cspec" do
    setup ctx do
      Stub.install()
      start_shipper()
      ctx
    end

    test "ships as a content-addressed whole file and hydrates back", ctx do
      hub_id = "ns_hub"
      path = Path.join([ctx.hub_root, hub_id, "registry.dets"])
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "cspec-bytes")

      # The writer is part of the identity: every controller in a namespace has
      # its own replica of one gossiped registry, so they are separate lineages
      # rather than writers competing for one.
      ref = {:cspec, {hub_id, DurabilityTier.writer_id()}}
      assert :ok = Shipper.ship_now(ref)

      writes = Stub.writes()
      assert :ok = Shipper.ship_now(ref)
      assert Stub.writes() == writes, "unchanged bytes must not re-upload"

      File.rm!(path)
      assert :ok = Hydrator.hydrate_cspecs()
      assert File.read!(path) == "cspec-bytes"
    end

    test "two controllers' replicas never collide, and hydration takes the newest", ctx do
      hub_id = "ns_hub"
      path = Path.join([ctx.hub_root, hub_id, "registry.dets"])
      File.mkdir_p!(Path.dirname(path))

      File.write!(path, "peer-registry")
      peer_ref = {:cspec, {hub_id, "peer-controller"}}
      assert :ok = Shipper.ship_now(peer_ref)

      # `shipped_at_ms` is wall-clock milliseconds and equal stamps break on the
      # writer id, so the two ships have to land in different milliseconds for
      # "newest" to name the second one at all.
      await_next_millisecond()

      File.write!(path, "local-registry")
      own_ref = {:cspec, {hub_id, DurabilityTier.writer_id()}}
      assert :ok = Shipper.ship_now(own_ref)

      # Two entries, two blobs. Under one shared key the second ship would have
      # clobbered the first, at an epoch that could never refuse it.
      assert Manifest.key(peer_ref) != Manifest.key(own_ref)
      assert Manifest.key(peer_ref) in Stub.keys()
      assert Manifest.key(own_ref) in Stub.keys()

      File.rm!(path)
      assert :ok = Hydrator.hydrate_cspecs()
      assert File.read!(path) == "local-registry"
    end
  end

  # ---------- platform stores ----------

  describe "a controller that died with its disk" do
    setup ctx do
      Stub.install()

      dlq_dir = Path.join(ctx.base, "dlq")
      on_exit(TestHelpers.override_env(:dlq, dir: dlq_dir))

      auth = AuthTestHelper.setup_auth_stores(prefix: "flz_dur_authz")
      store = start_supervised!({Dlq.Store, name: :"dlq_#{System.unique_integer([:positive])}"})

      start_shipper()
      Map.merge(ctx, %{dlq_dir: dlq_dir, auth_dir: auth.auth_dir, dlq: store})
    end

    test "its operatives, its parked failures and its security state all come back", ctx do
      TestHelpers.write_wal(ctx.entity_id, 0..3)
      {:ok, entry_ref} = Dlq.Store.append(ctx.dlq, park(ctx, "poison-bytes"))
      :ok = RoleStore.bind("alice", "_system", :admin)
      {:ok, raw_token, _entry} = TokenStore.issue("alice", "_system", :admin, 3600)
      :ok = AuthState.set_enabled()

      # One sweep is the whole cut: the backstop reaches every reference this
      # node holds, which is what an RPO window buys an operator.
      assert :ok = Shipper.sweep_now()

      # The disk dies with the controller. The stores have to be down before it
      # goes, or DETS writes a closing image over the empty directory.
      AuthTestHelper.stop_if_running([
        AuthState,
        RoleStore,
        NamespaceStore,
        TokenStore,
        QuotaCounters
      ])

      TestHelpers.wipe_entity(ctx.entity_id)
      File.rm_rf!(ctx.dlq_dir)
      File.rm_rf!(ctx.auth_dir)

      assert :ignore = Hydrator.hydrate_on_boot()
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert seqs_in_wal(ctx.entity_id) == [0, 1, 2, 3]
      assert [%{"id" => _}] = Dlq.Store.list(ctx.dlq, ctx.class)
      assert {:ok, _entry, "poison-bytes"} = Dlq.Store.get(ctx.dlq, entry_ref)

      {:ok, _} = RoleStore.start_link([])
      {:ok, _} = TokenStore.start_link([])
      {:ok, _} = AuthState.start_link([])

      assert RoleStore.admin_system_exists?(), "bootstrap must read already-claimed"
      assert AuthState.enabled?()
      assert {:ok, %{principal: "alice", role: :admin}} = TokenStore.verify(raw_token)
    end

    test "the bootstrap secret never reaches the backend", ctx do
      token_path = Path.join(ctx.auth_dir, "bootstrap.token")
      File.write!(token_path, "SECRET")
      :ok = RoleStore.bind("alice", "_system", :admin)

      # What leaves the node is exactly the named store files: the shipped set is
      # a list, not a scan of the directory the secret happens to sit in.
      {:ok, packed} = Artifact.pack({:control, {"authz", DurabilityTier.writer_id()}})
      files = :erlang.binary_to_term(packed)

      assert Map.has_key?(files, "roles.dets")
      refute Map.has_key?(files, "bootstrap.token")

      assert :ok = Shipper.sweep_now()
      File.rm_rf!(ctx.auth_dir)
      assert :ok = Hydrator.hydrate_control_stores()

      assert File.exists?(Path.join(ctx.auth_dir, "roles.dets"))
      refute File.exists?(token_path)
    end

    test "a resolved park is removed remotely, not resurrected", ctx do
      {:ok, entry_ref} = Dlq.Store.append(ctx.dlq, park(ctx, "poison-bytes"))
      assert :ok = Shipper.sweep_now()

      :ok = Dlq.Store.drop(ctx.dlq, entry_ref)
      assert :ok = Shipper.sweep_now()

      File.rm_rf!(ctx.dlq_dir)
      assert :ok = DurabilityTier.hydrate_operative(ctx.class)

      assert Dlq.Store.list(ctx.dlq, ctx.class) == [],
             "the artifact is the whole directory, so a drop must reach the backend too"
    end

    test "schedule definitions ride the same replica shape", ctx do
      dir = Path.join(ctx.base, "schedules")
      on_exit(TestHelpers.override_env(:schedules, dir: dir, application_managed: false))
      File.mkdir_p!(dir)
      File.write!(Path.join(dir, "schedules.dets"), "cron-defs")

      assert :ok = Shipper.sweep_now()

      File.rm_rf!(dir)
      assert :ok = Hydrator.hydrate_control_stores()
      assert File.read!(Path.join(dir, "schedules.dets")) == "cron-defs"
    end
  end

  # ---------- id and class are two key spaces ----------

  describe "an operative whose instance id and class differ" do
    setup ctx do
      Stub.install()
      start_shipper()

      instance = "inst_" <> Integer.to_string(System.unique_integer([:positive]))
      ClassRegistry.put_state_keys(instance, ctx.class)

      Map.put(ctx, :instance, instance)
    end

    test "the synchronous barrier ships both key spaces, not half of them", ctx do
      # The Entity WAL and the input log live under the resolved CLASS...
      entity = {ctx.class, "proc", "k1"}
      TestHelpers.write_wal(entity, 0..3)
      # ...while the proc-journal lives under the INSTANCE id.
      journal = Paths.journal_id(ctx.instance)
      TestHelpers.write_wal(journal, 0..1)

      assert :ok = DurabilityTier.commit_sync(ctx.instance)

      assert Manifest.key({:entity, journal}) in Stub.keys()

      assert Manifest.key({:entity, entity}) in Stub.keys(),
             "RPO 0 must not answer :ok having shipped no Entity state"
    end

    test "the RPO-0 gate arms on the class an instance resolves to", ctx do
      Application.put_env(:flezha, :sync_durability, %{ctx.class => true})

      assert DurabilityTier.sync_class?(ctx.instance),
             "a gate keyed by class must still arm for a caller holding the id"
    end
  end

  # ---------- helpers ----------

  # `sweep_now/0` is a barrier: it sweeps and drains, so the boot sweep the
  # shipper runs on start cannot land in the middle of a case.
  defp start_shipper() do
    {:ok, _pid} = start_supervised(Shipper)
    :ok = Shipper.sweep_now()
  end

  defp park(ctx, payload) do
    %{
      operative_class: ctx.class,
      procedure_id: "proc",
      entity_key: "k1",
      channel: "input",
      origin_offset: nil,
      error: "boom",
      attempts: 3,
      payload: payload,
      park_id: "park-1"
    }
  end

  # Evaluates the real `config/runtime.exs` for one environment, which is the
  # only way to pin the prod-vs-dev half of the decision: the refusal itself is
  # `children/0`'s, but what reaches it comes from here.
  defp backend_from_runtime_config(env) do
    Config.Reader.read!("config/runtime.exs", env: env, target: :host)
    |> get_in([:flezha, :durability_tier, :backend])
  end

  defp local_only_record(), do: TestHelpers.build_wal_record(99, "local-only")

  defp set_epoch(entity_id, epoch) do
    :ok =
      Outbox.write(entity_id, %{last_persisted_seq: 0, outbox_flushed_seq: -1, epoch: epoch})
  end

  defp status_of(ref), do: Enum.find(Shipper.status(), &(&1.ref == ref))

  defp seqs_in_wal(entity_id) do
    {lo, hi} = Artifact.seq_bounds({:entity, entity_id})
    Enum.to_list(lo..hi)
  end

  # Spin to the next wall-clock millisecond. A sleep would do the same job less
  # precisely and this waits under a millisecond.
  defp await_next_millisecond() do
    now = System.system_time(:millisecond)
    if System.system_time(:millisecond) > now, do: :ok, else: await_next_millisecond()
  end
end
