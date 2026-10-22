defmodule FlezhaTest.State.WalTransferTest do
  @moduledoc """
  Cross-controller pull of a Cell's durable files.

  This is what makes the last-runner migration safe: the handover sources its
  state from the runner, and on that path the runner is dead, so the source
  controller's disk is the only surviving copy — and `state_root` is node-local.
  """
  # async: false: writes Application env; a shared on-disk root; a peer BEAM.
  use FlezhaTest.Case, fixtures: [:tmp_app_data], async: false

  use Mimic

  import ExUnit.CaptureLog

  alias Flezha.Relay.{Builder, Overwatch, Replay}
  alias Flezha.State.{Adoption, FsyncBatcher, InputLog, Lineage, Paths, Supervisor, TaskJournal}
  alias Flezha.State.TestHelpers
  alias Flezha.State.{WalTransfer, WalWriter}
  alias FlezhaTest.Helper.NodeTestHelper

  # One peer serves every test across controllers: each hands it the root it
  # wrote under (`move_root_to_peer/1`), so it holds nothing of another test.
  setup_all do
    if :multi_node in (ExUnit.configuration()[:include] || []),
      do: %{peer: start_peer()},
      else: :ok
  end

  # A test picked by its line runs without the tier's include.
  setup ctx do
    if ctx[:multi_node] && !ctx[:peer], do: %{peer: start_peer()}, else: :ok
  end

  defp start_peer do
    {_pid, peer} = NodeTestHelper.start_peer_node(:waltransfer)
    peer
  end

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
    class = "steps_guard_#{System.unique_integer([:positive])}"
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
    assert 0 = WalTransfer.pull_relay(node(), elem(cell_id, 0))
  end

  @tag :multi_node
  test "a cell moves from a peer into another Relay's tree, source last", %{peer: peer} do
    # This is the shape a partition rescale needs: the Cell keeps its
    # tasklet and key but changes OWNER, so it lands under a different
    # Relay class — and the peer's copy may only go once ours is safe.
    {class, tasklet, key} = source = seed_cell("moveacross")
    source_bytes = File.read!(Paths.wal_path(source))
    destination = {class <> "_moved", tasklet, key}
    move_root_to_peer(peer)

    # The peer took the root this node was on, so the files live there and
    # nowhere here once this node moves off it.
    refute File.exists?(Paths.wal_path(source))

    assert {:ok, files} = WalTransfer.move_cell(peer, {:cell, source}, {:cell, destination})
    assert files > 0

    assert File.read!(Paths.wal_path(destination)) == source_bytes
    refute File.exists?(Paths.wal_path(destination) <> ".transferring")

    # The peer does not hold it — the delete runs only after the rename.
    assert :erpc.call(peer, WalTransfer, :manifest, [{:cell, source}]) == []

    # Re-running is a no-op: the source is gone, so nothing is owed and the
    # destination is untouched. That is what makes a crashed rescale re-runnable.
    assert {:ok, 0} = WalTransfer.move_cell(peer, {:cell, source}, {:cell, destination})
    assert File.read!(Paths.wal_path(destination)) == source_bytes
  end

  @tag :multi_node
  test "a move whose pull fails leaves the peer's copy alone", %{peer: peer} do
    source = seed_cell("movefail")
    move_root_to_peer(peer)

    # A destination whose cell key differs has no matching basename in this
    # layout, so every file fails to resolve and the pull aborts.
    {class, tasklet, _key} = source
    unresolvable = {class, tasklet, "a-different-key"}

    assert {:error, _reason} =
             WalTransfer.move_cell(peer, {:cell, source}, {:cell, unresolvable})

    # Nothing was deleted: a half-moved Cell is the one outcome that loses data.
    assert Path.basename(Paths.wal_path(source)) in :erpc.call(
             peer,
             WalTransfer,
             :manifest,
             [{:cell, source}]
           )
  end

  # fails when: the same-id clause answers before asking where the Cell is,
  # so a rescale's gather of a member that keeps its id leaves its Cells on
  # the peer.
  @tag :multi_node
  test "a same-id move from a peer pulls the cell and removes the peer's copy", %{peer: peer} do
    cell_id = seed_cell("sameid")
    source_bytes = File.read!(Paths.wal_path(cell_id))
    %{id: id, landed: false} = Lineage.read(cell_id).record
    move_root_to_peer(peer)

    assert {:ok, files} = WalTransfer.move_cell(peer, {:cell, cell_id}, {:cell, cell_id})
    assert files > 0
    assert File.read!(Paths.wal_path(cell_id)) == source_bytes
    assert :erpc.call(peer, WalTransfer, :manifest, [{:cell, cell_id}]) == []
    # Another controller's copy: the Cell's next writer mints over it.
    assert %{id: ^id, landed: true} = Lineage.read(cell_id).record
  end

  # fails when: the pull enumerates Cells only, so a steps instance placed
  # on another controller resumes without its journal and its input log.
  @tag :multi_node
  test "a steps instance's journal and input log are pulled with its cells", %{peer: peer} do
    class = "steps_pull_#{System.unique_integer([:positive])}"
    cell_id = {class, "counter", "k"}
    {:ok, srv} = Supervisor.find_or_start(cell_id)
    :ok = WalWriter.append(srv, 0, TestHelpers.stub_header(), "alpha")
    :ok = TaskJournal.append_iter_committed(class, 0, 0, [])
    {:ok, 0} = InputLog.append(class, "m", %{})
    FsyncBatcher.flush_now()

    journal_bytes = File.read!(Paths.journal_path(class))
    log_bytes = File.read!(Paths.input_log_path(class))

    move_root_to_peer(peer)
    WalTransfer.pull_relay(peer, class)

    assert File.exists?(Paths.wal_path(cell_id))
    assert File.read!(Paths.journal_path(class)) == journal_bytes
    assert File.read!(Paths.input_log_path(class)) == log_bytes
  end

  # fails when: a source that cannot be asked leaves the local copy bound
  # without a word.
  test "says when the node a relay moves from cannot be asked" do
    {class, cell_id} = class_and_cell("unreachable")
    TestHelpers.write_wal(cell_id, 0..3, "local")
    local = File.read!(Paths.wal_path(cell_id))
    on_exit(NodeTestHelper.override_log_level(:warning))

    log =
      capture_log(fn ->
        assert :ok = Adoption.adopt_newest(class, {:node, :"nobody@127.0.0.1"})
      end)

    assert log =~ "could not be asked for its copy"
    assert log =~ "the local copy is bound unchecked"
    assert File.read!(Paths.wal_path(cell_id)) == local
    # A node that cannot answer for its lineage (unreachable, or a build
    # without lineage records) is compared as before.
    assert WalTransfer.lineage(:"nobody@127.0.0.1", class) == %{}
  end

  describe "a relay moving back to a node that kept a copy" do
    @describetag :multi_node

    # fails when: a node holding any of the instance's files skips the pull, so
    # a relay moving back binds from the copy this node was left holding.
    test "takes the source's newer copy, and keeps a newer one of its own", %{peer: peer} do
      {class, cell_id} = class_and_cell("newer")
      newer = with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..3, "stale")

      assert :ok = Adoption.adopt_newest(class, {:node, peer})
      assert File.read!(Paths.wal_path(cell_id)) == newer
      refute File.exists?(Paths.aside_path(class, :set_aside))

      TestHelpers.append_wal(cell_id, 6..7, "local")
      local = File.read!(Paths.wal_path(cell_id))
      assert :ok = Adoption.adopt_newest(class, {:node, peer})
      assert File.read!(Paths.wal_path(cell_id)) == local
    end

    # fails when: a writer a relay stopped before its writers closed stays
    # open, so the bind's writer start returns it and appends to the copy set
    # aside and removed.
    # fails when: it closes after the newer copy landed, and writes the stale
    # copy's cursors over the newer one's.
    test "closes the writers a stopped relay left open, then adopts", %{peer: peer} do
      {class, cell_id} = class_and_cell("open")
      newer = with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..3, "stale")
      {:ok, writer} = Supervisor.find_or_start(cell_id)
      ref = Process.monitor(writer)

      assert :ok = Adoption.adopt_newest(class, {:node, peer})

      assert_received {:DOWN, ^ref, :process, ^writer, :normal}
      assert File.read!(Paths.wal_path(cell_id)) == newer
      refute File.exists?(Paths.outbox_dedup_path(cell_id))
    end

    # fails when: a writer a late runner frame opens while the newer copy is
    # pulled stays open on the file the pull replaced.
    test "closes a writer opened while the newer copy is pulled", %{peer: peer} do
      {class, cell_id} = class_and_cell("late")
      newer = with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..3, "stale")

      test_pid = self()

      stub(WalTransfer, :pull_relay, fn source, class ->
        {:ok, late} = Supervisor.find_or_start(cell_id)
        send(test_pid, {:late, late, Process.monitor(late)})
        call_original(WalTransfer, :pull_relay, [source, class])
      end)

      assert :ok = Adoption.adopt_newest(class, {:node, peer})

      assert_received {:late, late, ref}
      assert_received {:DOWN, ^ref, :process, ^late, :normal}
      assert File.read!(Paths.wal_path(cell_id)) == newer
    end

    # fails when: the adoption swaps the files of a relay another
    # Overwatch here is bound with, so records it acknowledges are lost.
    test "leaves a copy alone while a relay bound here owns it", %{peer: peer} do
      {class, cell_id} = class_and_cell("bound")
      with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..3, "stale")
      stale = File.read!(Paths.wal_path(cell_id))
      bound = spawn_link(fn -> Process.sleep(:infinity) end)
      :ok = Flezha.Relay.ClassRegistry.put_overwatch(class, bound, nil)
      on_exit(fn -> Flezha.Relay.ClassRegistry.delete(class) end)
      on_exit(NodeTestHelper.override_log_level(:warning))

      log = capture_log(fn -> assert :ok = Adoption.adopt_newest(class, {:node, peer}) end)

      assert log =~ "the relay is bound on this node; the local copy stays"
      assert File.read!(Paths.wal_path(cell_id)) == stale
    end

    # fails when: a bind task adopting for the Overwatch it binds reads that
    # Overwatch as another relay bound here, so a restarted runner's bind
    # never pulls a copy that did not land on arrival.
    test "adopts for the Overwatch bound here when it binds", %{peer: peer} do
      {class, cell_id} = class_and_cell("owner")
      newer = with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..3, "stale")
      bound = spawn_link(fn -> Process.sleep(:infinity) end)
      :ok = Flezha.Relay.ClassRegistry.put_overwatch(class, bound, nil)
      on_exit(fn -> Flezha.Relay.ClassRegistry.delete(class) end)

      assert :ok = Adoption.adopt_newest(class, {:node, peer}, bound)
      assert File.read!(Paths.wal_path(cell_id)) == newer
    end

    # fails when: the incoming copy must hold every reference the local one
    # does, so a Cell the source deleted blocks every adoption, silently.
    # fails when: a reference only this node holds is dropped with the local
    # copy. A partition member re-adopting from the node that rescaled it ties
    # on every old key, and a key it wrote since exists only here.
    test "keeps a Cell only this node holds when every shared one ties", %{peer: peer} do
      {class, cell_id} = class_and_cell("only_here")
      source = with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..5, "source")
      carl = {class, "task", "carl"}
      TestHelpers.write_wal(carl, 0..2, "only_here")
      carl_bytes = File.read!(Paths.wal_path(carl))

      assert :ok = Adoption.adopt_newest(class, {:node, peer})

      assert File.read!(Paths.wal_path(cell_id)) == source
      assert File.read!(Paths.wal_path(carl)) == carl_bytes
    end

    # fails when: each reference is decided alone, so the Cells come from the
    # source while the input log stays behind and the consumed offsets point
    # past the end of the log.
    test "keeps every reference as it was when one of them cannot be pulled", %{peer: peer} do
      {class, cell_id} = class_and_cell("partial")
      with_source_copy(cell_id, 0..5)
      File.write!(Paths.input_log_path(class), TestHelpers.wal_records(0..5, "in"))
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..3, "stale")
      File.write!(Paths.input_log_path(class), TestHelpers.wal_records(0..3, "stale"))
      stale = {File.read!(Paths.wal_path(cell_id)), File.read!(Paths.input_log_path(class))}
      # The source's log is judged newer and then does not come over.
      pull_only(cell_id)

      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        capture_log(fn -> assert :not_landed = Adoption.adopt_newest(class, {:node, peer}) end)

      assert log =~ "did not land whole; the local copy stays"

      assert {File.read!(Paths.wal_path(cell_id)), File.read!(Paths.input_log_path(class))} ==
               stale

      refute File.exists?(Paths.aside_path(class, :set_aside))
    end

    # fails when: a node that held nothing keeps the part of a moving relay's
    # copy that landed, so a steps instance whose source stopped answering
    # after its Cells binds them without its journal and reads their
    # compensation intents as an undo in flight.
    test "a node that held nothing stays cold when the newer copy does not land whole", %{
      peer: peer
    } do
      {class, cell_id} = class_and_cell("cold")
      with_source_copy(cell_id, 0..5)
      :ok = TaskJournal.append_iter_committed(class, 0, 0, [])
      FsyncBatcher.flush_now()
      move_root_to_peer(peer)
      refute File.exists?(Paths.wal_path(cell_id))
      # The Cells come before the journal (`Paths.durable_refs/1`); the source
      # stops answering in between.
      pull_only(cell_id)

      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        capture_log(fn -> assert :not_landed = Adoption.adopt_newest(class, {:node, peer}) end)

      refute File.exists?(Paths.journal_path(class))
      refute File.exists?(Paths.wal_path(cell_id))
      refute File.exists?(Paths.class_dir(class))
      refute File.exists?(Paths.aside_path(class, :set_aside))
      assert log =~ "did not land whole"
    end

    # fails when: the arrival drops the node it moved from after one pull that
    # did not land whole, so the relay binds empty while the source still holds
    # its copy, and no later bind pulls it again.
    test "a pull that did not land whole is retried by the next bind", %{peer: peer} do
      {class, cell_id} = class_and_cell("retry")
      source = with_source_copy(cell_id, 0..5)
      :ok = TaskJournal.append_iter_committed(class, 0, 0, [])
      FsyncBatcher.flush_now()
      move_root_to_peer(peer)
      pull_only(cell_id)

      relay = %Builder{id: class, factory_id: "f", run: :steps, opts: [state_source_node: peer]}
      state = %Overwatch{id: class, builder: relay}
      on_exit(NodeTestHelper.override_log_level(:warning))

      log =
        capture_log(fn ->
          rebound = Replay.adopt_moved_state(state, relay, :undefined)
          refute File.exists?(Paths.wal_path(cell_id))
          # The source answers again; the relay's next bind on this node.
          stub(WalTransfer, :pull_relay, &call_original(WalTransfer, :pull_relay, [&1, &2]))
          Replay.adopt_unlanded_state(%Overwatch{state | builder: rebound}, rebound, self())
        end)

      assert log =~ "did not land whole"
      assert File.read!(Paths.wal_path(cell_id)) == source
      assert File.exists?(Paths.journal_path(class))
    end

    # fails when: the source kept after a pull that did not land is pulled
    # again once the relay took turns here, so those acknowledged turns are set
    # aside and dropped.
    test "a relay that bound empty keeps the turns it took since", %{peer: peer} do
      {class, cell_id} = class_and_cell("turns")
      with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      pull_only({class, "task", "other"})
      relay = %Builder{id: class, factory_id: "f", run: :steps, opts: [state_source_node: peer]}
      state = %Overwatch{id: class, builder: relay}
      on_exit(NodeTestHelper.override_log_level(:warning))

      {rebound, _log} = with_log(fn -> Replay.adopt_moved_state(state, relay, :undefined) end)
      TestHelpers.write_wal(cell_id, 0..1, "turn")
      turns = File.read!(Paths.wal_path(cell_id))
      stub(WalTransfer, :pull_relay, &call_original(WalTransfer, :pull_relay, [&1, &2]))

      next = Replay.adopt_unlanded_state(%Overwatch{state | builder: rebound}, rebound, self())
      assert File.read!(Paths.wal_path(cell_id)) == turns
      refute Keyword.has_key?(next.opts, :state_source_node)
    end

    # fails when: a tie keeps the local copy, so two histories of equal length
    # resolve toward the one the relay's current owner did not write.
    test "settles a tie toward the copy the relay moves from", %{peer: peer} do
      {class, cell_id} = class_and_cell("tie")
      source = with_source_copy(cell_id, 0..5)
      move_root_to_peer(peer)
      TestHelpers.write_wal(cell_id, 0..5, "local")

      assert :ok = Adoption.adopt_newest(class, {:node, peer})
      assert File.read!(Paths.wal_path(cell_id)) == source
    end
  end

  defp class_and_cell(suffix) do
    class = "adopt_#{suffix}_#{System.unique_integer([:positive])}"
    {class, {class, "task", "k"}}
  end

  defp with_source_copy(cell_id, seqs) do
    TestHelpers.write_wal(cell_id, seqs, "source")
    File.read!(Paths.wal_path(cell_id))
  end

  # Of the source's copy only `cell_id` comes over.
  defp pull_only(cell_id) do
    stub(WalTransfer, :pull_relay, fn source, _class ->
      {:ok, _} = WalTransfer.pull_cell(source, {:cell, cell_id})
      1
    end)
  end

  # The peer takes the root this node was on, so it is the source; this node
  # moves to a fresh root and writes there what it kept. The peer gets its own
  # root back before this test's is removed.
  defp move_root_to_peer(peer) do
    restore = :erpc.call(peer, TestHelpers, :override_env, [:state_root, Paths.state_root()])
    on_exit(fn -> :erpc.call(peer, restore) end)
    TestHelpers.tmp_app_data_dir()
  end

  @tag :multi_node
  test "a cell's files are pulled from a peer controller into this node's tree", %{peer: peer} do
    cell_id = {class, _, _} = seed_cell("xnode")
    source_bytes = File.read!(Paths.wal_path(cell_id))

    # The peer is handed the CURRENT state_root, so it is the node holding the
    # files. Moving this node's root afterwards makes the pull a genuine
    # cross-tree copy rather than a file onto itself.
    move_root_to_peer(peer)

    refute File.exists?(Paths.wal_path(cell_id))

    assert 1 = WalTransfer.pull_relay(peer, class)

    assert File.read!(Paths.wal_path(cell_id)) == source_bytes
    # The staging artefact never survives a successful pull.
    refute File.exists?(Paths.wal_path(cell_id) <> ".transferring")
  end
end
