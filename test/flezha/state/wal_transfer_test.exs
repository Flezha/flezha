defmodule FlezhaTest.State.WalTransferTest do
  @moduledoc """
  Cross-controller pull of an Entity's durable files.

  This is what makes the last-runner migration safe: the handover sources its
  state from the runner, and on that path the runner is dead, so the source
  controller's disk is the only surviving copy — and `state_root` is node-local.
  """
  # async: false: writes Application env; a shared on-disk root; a peer BEAM.
  use ExUnit.Case, async: false

  alias Flezha.State.{FsyncBatcher, Paths, Supervisor, TestHelpers, WalWriter, WalTransfer}
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    {:ok, root: root}
  end

  defp seed_entity(suffix) do
    entity_id = TestHelpers.sample_entity_id(suffix)
    {:ok, srv} = Supervisor.find_or_start(entity_id)
    # An empty WAL sheds any frame before seq 0 and pulls a baseline instead.
    :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "alpha")
    :ok = WalWriter.append(srv, 1, TestHelpers.stub_header(), "beta")
    FsyncBatcher.flush_now()
    entity_id
  end

  test "manifest lists only files that exist" do
    entity_id = seed_entity("man")

    assert Path.basename(Paths.wal_path(entity_id)) in WalTransfer.manifest(entity_id)

    # An Entity with nothing on disk offers nothing — the destination cold-starts.
    assert WalTransfer.manifest(TestHelpers.sample_entity_id("empty")) == []
  end

  test "entities enumerates a class the destination cannot see locally" do
    entity_id = {class, _, _} = seed_entity("enum")

    assert entity_id in WalTransfer.entities(class)
    assert WalTransfer.entities("no_such_class") == []
  end

  # fails when: read_chunk joins the requested name onto the state directory.
  # The name arrives over :erpc from another controller, so a traversing name
  # would let a peer read arbitrary files off this node.
  test "read_chunk refuses a name that is not in the entity's file set" do
    entity_id = seed_entity("guard")

    assert {:error, :not_in_manifest} =
             WalTransfer.read_chunk(entity_id, "../../../etc/passwd", 0)

    assert {:error, :not_in_manifest} = WalTransfer.read_chunk(entity_id, "unrelated.wal", 0)
  end

  test "read_chunk walks a real file to :eof" do
    entity_id = seed_entity("chunk")
    name = Path.basename(Paths.wal_path(entity_id))

    assert {:ok, first} = WalTransfer.read_chunk(entity_id, name, 0)
    assert byte_size(first) > 0
    assert :eof = WalTransfer.read_chunk(entity_id, name, byte_size(first))
  end

  test "pulling from this node is a no-op" do
    entity_id = seed_entity("self")

    assert {:ok, 0} = WalTransfer.pull_entity(node(), entity_id)
    assert 0 = WalTransfer.pull_operative(node(), elem(entity_id, 0))
  end

  @tag :multi_node
  test "an entity moves from a peer into another Operative's tree, source last" do
    # This is the shape a partition rescale needs: the Entity keeps its
    # procedure and key but changes OWNER, so it lands under a different
    # Operative class — and the peer's copy may only go once ours is safe.
    {class, procedure, key} = source = seed_entity("moveacross")
    source_bytes = File.read!(Paths.wal_path(source))
    destination = {class <> "_moved", procedure, key}

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer2, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    # The peer took the root this node was on, so the files live there and
    # nowhere here once this node moves off it.
    refute File.exists?(Paths.wal_path(source))

    assert {:ok, files} = WalTransfer.move_entity(peer_node, source, destination)
    assert files > 0

    assert File.read!(Paths.wal_path(destination)) == source_bytes
    refute File.exists?(Paths.wal_path(destination) <> ".transferring")

    # The peer no longer holds it — the delete runs only after the rename.
    assert :erpc.call(peer_node, WalTransfer, :manifest, [source]) == []

    # Re-running is a no-op: the source is gone, so nothing is owed and the
    # destination is untouched. That is what makes a crashed rescale re-runnable.
    assert {:ok, 0} = WalTransfer.move_entity(peer_node, source, destination)
    assert File.read!(Paths.wal_path(destination)) == source_bytes

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :multi_node
  test "a move whose pull fails leaves the peer's copy alone" do
    source = seed_entity("movefail")

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer3, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    # A destination whose entity key differs has no matching basename in this
    # layout, so every file fails to resolve and the pull aborts.
    {class, procedure, _key} = source
    unresolvable = {class, procedure, "a-different-key"}

    assert {:error, _reason} = WalTransfer.move_entity(peer_node, source, unresolvable)

    # Nothing was deleted: a half-moved Entity is the one outcome that loses data.
    assert Path.basename(Paths.wal_path(source)) in :erpc.call(
             peer_node,
             WalTransfer,
             :manifest,
             [source]
           )

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :multi_node
  test "an entity's files are pulled from a peer controller into this node's tree" do
    entity_id = {class, _, _} = seed_entity("xnode")
    source_bytes = File.read!(Paths.wal_path(entity_id))

    # The peer is handed the CURRENT state_root, so it is the node holding the
    # files. Moving this node's root afterwards makes the pull a genuine
    # cross-tree copy rather than a file onto itself.
    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer1, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    refute File.exists?(Paths.wal_path(entity_id))

    assert 1 = WalTransfer.pull_operative(peer_node, class)

    assert File.read!(Paths.wal_path(entity_id)) == source_bytes
    # The staging artefact never survives a successful pull.
    refute File.exists?(Paths.wal_path(entity_id) <> ".transferring")

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
