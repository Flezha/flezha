defmodule FlezhaTest.State.WalTransferTest do
  @moduledoc """
  Cross-controller pull of a Cell's durable files.

  This is what makes the last-runner migration safe: the handover sources its
  state from the runner, and on that path the runner is dead, so the source
  controller's disk is the only surviving copy — and `state_root` is node-local.
  """
  # async: false: writes Application env; a shared on-disk root; a peer BEAM.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  alias Flezha.State.{FsyncBatcher, InputLog, Paths, Supervisor, TaskJournal, TestHelpers}
  alias Flezha.State.{WalTransfer, WalWriter}
  alias FlezhaTest.Helper.NodeTestHelper

  defp seed_cell(suffix) do
    cell_id = TestHelpers.sample_cell_id(suffix)
    {:ok, srv} = Supervisor.find_or_start(cell_id)
    on_exit(fn -> Supervisor.delete_cell(cell_id) end)
    # An empty WAL sheds any frame before seq 0 and pulls a baseline instead.
    :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "alpha")
    :ok = WalWriter.append(srv, 1, TestHelpers.stub_header(), "beta")
    FsyncBatcher.flush_now()
    cell_id
  end

  test "manifest lists only files that exist" do
    cell_id = seed_cell("man")

    assert Path.basename(Paths.wal_path(cell_id)) in WalTransfer.manifest({:cell, cell_id})

    # A Cell with nothing on disk offers nothing — the destination cold-starts.
    assert WalTransfer.manifest({:cell, TestHelpers.sample_cell_id("empty")}) == []
  end

  test "durable_refs enumerates a class the destination cannot see locally" do
    cell_id = {class, _, _} = seed_cell("enum")

    assert {:cell, cell_id} in Paths.durable_refs(class)
    assert Paths.durable_refs("no_such_class") == []
  end

  # fails when: read_chunk joins the requested name onto the state directory.
  # The name arrives over :erpc from another controller, so a traversing name
  # would let a peer read arbitrary files off this node.
  test "read_chunk refuses a name that is not in the cell's file set" do
    cell_id = seed_cell("guard")

    assert {:error, :not_in_manifest} =
             WalTransfer.read_chunk({:cell, cell_id}, "../../../etc/passwd", 0)

    assert {:error, :not_in_manifest} =
             WalTransfer.read_chunk({:cell, cell_id}, "unrelated.wal", 0)
  end

  # fails when: the log's name is resolved by joining it onto the class
  # directory, where the class's sidecars live beside it.
  test "read_chunk refuses a name that is not the input log's" do
    class = "bounded_guard_#{System.unique_integer([:positive])}"
    {:ok, 0} = InputLog.append(class, "m", %{})
    FsyncBatcher.flush_now()

    assert {:error, :not_in_manifest} = WalTransfer.read_chunk({:input_log, class}, "kind", 0)

    assert {:error, :not_in_manifest} =
             WalTransfer.read_chunk({:input_log, class}, "../../../etc/passwd", 0)

    assert {:ok, _chunk} = WalTransfer.read_chunk({:input_log, class}, "input.log", 0)
  end

  test "read_chunk walks a real file to :eof" do
    cell_id = seed_cell("chunk")
    name = Path.basename(Paths.wal_path(cell_id))

    assert {:ok, first} = WalTransfer.read_chunk({:cell, cell_id}, name, 0)
    assert byte_size(first) > 0
    assert :eof = WalTransfer.read_chunk({:cell, cell_id}, name, byte_size(first))
  end

  test "pulling from this node is a no-op" do
    cell_id = seed_cell("self")

    assert {:ok, 0} = WalTransfer.pull_cell(node(), {:cell, cell_id})
    assert 0 = WalTransfer.pull_operator(node(), elem(cell_id, 0))
  end

  @tag :multi_node
  test "a cell moves from a peer into another Operator's tree, source last" do
    # This is the shape a partition rescale needs: the Cell keeps its
    # tasklet and key but changes OWNER, so it lands under a different
    # Operator class — and the peer's copy may only go once ours is safe.
    {class, tasklet, key} = source = seed_cell("moveacross")
    source_bytes = File.read!(Paths.wal_path(source))
    destination = {class <> "_moved", tasklet, key}

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer2, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    # The peer took the root this node was on, so the files live there and
    # nowhere here once this node moves off it.
    refute File.exists?(Paths.wal_path(source))

    assert {:ok, files} = WalTransfer.move_cell(peer_node, {:cell, source}, {:cell, destination})
    assert files > 0

    assert File.read!(Paths.wal_path(destination)) == source_bytes
    refute File.exists?(Paths.wal_path(destination) <> ".transferring")

    # The peer does not hold it — the delete runs only after the rename.
    assert :erpc.call(peer_node, WalTransfer, :manifest, [{:cell, source}]) == []

    # Re-running is a no-op: the source is gone, so nothing is owed and the
    # destination is untouched. That is what makes a crashed rescale re-runnable.
    assert {:ok, 0} = WalTransfer.move_cell(peer_node, {:cell, source}, {:cell, destination})
    assert File.read!(Paths.wal_path(destination)) == source_bytes

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :multi_node
  test "a move whose pull fails leaves the peer's copy alone" do
    source = seed_cell("movefail")

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer3, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    # A destination whose cell key differs has no matching basename in this
    # layout, so every file fails to resolve and the pull aborts.
    {class, tasklet, _key} = source
    unresolvable = {class, tasklet, "a-different-key"}

    assert {:error, _reason} =
             WalTransfer.move_cell(peer_node, {:cell, source}, {:cell, unresolvable})

    # Nothing was deleted: a half-moved Cell is the one outcome that loses data.
    assert Path.basename(Paths.wal_path(source)) in :erpc.call(
             peer_node,
             WalTransfer,
             :manifest,
             [{:cell, source}]
           )

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: the same-id clause answers before asking where the Cell is,
  # so a rescale's gather of a member that keeps its id leaves its Cells on
  # the peer.
  @tag :multi_node
  test "a same-id move from a peer pulls the cell and removes the peer's copy" do
    cell_id = seed_cell("sameid")
    source_bytes = File.read!(Paths.wal_path(cell_id))

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer4, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    assert {:ok, files} = WalTransfer.move_cell(peer_node, {:cell, cell_id}, {:cell, cell_id})
    assert files > 0
    assert File.read!(Paths.wal_path(cell_id)) == source_bytes
    assert :erpc.call(peer_node, WalTransfer, :manifest, [{:cell, cell_id}]) == []

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  # fails when: the pull enumerates Cells only, so a bounded instance placed
  # on another controller resumes without its journal and its input log.
  @tag :multi_node
  test "a bounded instance's journal and input log are pulled with its cells" do
    class = "bounded_pull_#{System.unique_integer([:positive])}"
    cell_id = {class, "counter", "k"}
    {:ok, srv} = Supervisor.find_or_start(cell_id)
    :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "alpha")
    :ok = TaskJournal.append_iter_committed(class, 0, 0, [])
    {:ok, 0} = InputLog.append(class, "m", %{})
    FsyncBatcher.flush_now()

    journal_bytes = File.read!(Paths.journal_path(class))
    log_bytes = File.read!(Paths.input_log_path(class))

    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer5, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    WalTransfer.pull_operator(peer_node, class)

    assert File.exists?(Paths.wal_path(cell_id))
    assert File.read!(Paths.journal_path(class)) == journal_bytes
    assert File.read!(Paths.input_log_path(class)) == log_bytes

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end

  @tag :multi_node
  test "a cell's files are pulled from a peer controller into this node's tree" do
    cell_id = {class, _, _} = seed_cell("xnode")
    source_bytes = File.read!(Paths.wal_path(cell_id))

    # The peer is handed the CURRENT state_root, so it is the node holding the
    # files. Moving this node's root afterwards makes the pull a genuine
    # cross-tree copy rather than a file onto itself.
    {peer_pid, peer_node} =
      NodeTestHelper.start_peer_node(:waltransfer1, state_root: Paths.state_root())

    dest_root = TestHelpers.tmp_app_data_dir()
    on_exit(fn -> File.rm_rf(dest_root) end)

    refute File.exists?(Paths.wal_path(cell_id))

    assert 1 = WalTransfer.pull_operator(peer_node, class)

    assert File.read!(Paths.wal_path(cell_id)) == source_bytes
    # The staging artefact never survives a successful pull.
    refute File.exists?(Paths.wal_path(cell_id) <> ".transferring")

    NodeTestHelper.stop_peer_node(peer_pid, peer_node)
  end
end
