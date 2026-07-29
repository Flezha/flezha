defmodule FlezhaTest.State.WalTransferTest do
  @moduledoc """
  Cross-controller pull of a Segment's durable files.

  This is what makes the last-runner migration safe: the handover sources its
  state from the runner, and on that path the runner is dead, so the source
  controller's disk is the only surviving copy — and `state_root` is node-local.
  """
  use ExUnit.Case, async: false

  alias Flezha.State.{FsyncBatcher, Registry, Supervisor, TestHelpers, WalWriter, WalTransfer}
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    root = TestHelpers.tmp_app_data_dir()

    on_exit(fn ->
      Application.delete_env(:flezha, :state_root)
      File.rm_rf(root)
    end)

    {:ok, root: root}
  end

  defp seed_segment(suffix) do
    segment_id = TestHelpers.sample_segment_id(suffix)
    {:ok, srv} = Supervisor.find_or_start(segment_id)
    # An empty WAL sheds any frame before seq 0 and pulls a baseline instead.
    :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "alpha")
    :ok = WalWriter.append(srv, 1, TestHelpers.stub_header(), "beta")
    FsyncBatcher.flush_now()
    segment_id
  end

  test "manifest lists only files that exist" do
    segment_id = seed_segment("man")

    assert Path.basename(Registry.wal_path(segment_id)) in WalTransfer.manifest(segment_id)

    # A Segment with nothing on disk offers nothing — the destination cold-starts.
    assert WalTransfer.manifest(TestHelpers.sample_segment_id("empty")) == []
  end

  test "segments enumerates a class the destination cannot see locally" do
    segment_id = {class, _, _} = seed_segment("enum")

    assert segment_id in WalTransfer.segments(class)
    assert WalTransfer.segments("no_such_class") == []
  end

  # fails when: read_chunk joins the requested name onto the state directory.
  # The name arrives over :erpc from another controller, so a traversing name
  # would let a peer read arbitrary files off this node.
  test "read_chunk refuses a name that is not in the segment's file set" do
    segment_id = seed_segment("guard")

    assert {:error, :not_in_manifest} =
             WalTransfer.read_chunk(segment_id, "../../../etc/passwd", 0)

    assert {:error, :not_in_manifest} = WalTransfer.read_chunk(segment_id, "unrelated.wal", 0)
  end

  test "read_chunk walks a real file to :eof" do
    segment_id = seed_segment("chunk")
    name = Path.basename(Registry.wal_path(segment_id))

    assert {:ok, first} = WalTransfer.read_chunk(segment_id, name, 0)
    assert byte_size(first) > 0
    assert :eof = WalTransfer.read_chunk(segment_id, name, byte_size(first))
  end

  test "pulling from this node is a no-op" do
    segment_id = seed_segment("self")

    assert {:ok, 0} = WalTransfer.pull_segment(node(), segment_id)
    assert 0 = WalTransfer.pull_operative(node(), elem(segment_id, 0))
  end

  @tag :multi_node
  test "a segment's files are pulled from a peer controller into this node's tree" do
    segment_id = {class, _, _} = seed_segment("xnode")
    source_bytes = File.read!(Registry.wal_path(segment_id))

    # The peer inherits the CURRENT state_root, so it is the node holding the
    # files. Moving this node's root afterwards makes the pull a genuine
    # cross-tree copy rather than a file onto itself.
    {peer_pid, peer_node} = NodeTestHelper.start_peer_node(:waltransfer1)
    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    refute File.exists?(Registry.wal_path(segment_id))

    assert 1 = WalTransfer.pull_operative(peer_node, class)

    assert File.read!(Registry.wal_path(segment_id)) == source_bytes
    # The staging artefact never survives a successful pull.
    refute File.exists?(Registry.wal_path(segment_id) <> ".transferring")

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
