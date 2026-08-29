defmodule FlezhaTest.Boot.ControllerColocationTest do
  @moduledoc """
  Several controllers on one machine, each owning its own resources
  (`controller-instance-isolation`).

  The two ways an operator reaches the arrangement by accident are the two
  guards here: a socket directory another controller is listening in, which
  clearing would strip of its control socket and every attached runner's
  stream socket, and a state root another controller is writing, which is two
  unsynchronised writers over one entity WAL tree.
  """
  # async: false: writes Application env; a shared on-disk root; the live controller and its fleet.
  use ExUnit.Case, async: false

  alias Flezha.Boot.StateRootLock
  alias Flezha.Runner.FleetManager
  alias Flezha.Socket.Support
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.Native
  alias FlezhaTest.Helper.NodeTestHelper

  # ---------- the socket directory ----------

  test "a live peer's socket directory is refused, and nothing in it is removed" do
    # This node is the live peer: its controller is listening in its own
    # socket directory with the boot runners attached.
    dir = Support.socket_dir()
    control = Support.control_socket_path()
    streams = Path.wildcard(Path.join(dir, "*_stream_*.sock"))

    assert File.exists?(control)
    assert streams != []
    assert Native.socket_dir_live(dir)

    error = assert_raise(RuntimeError, fn -> Support.prepare_socket_dir() end)
    message = error.message

    # The path, the peer it found, and the variable that separates instances.
    assert message =~ dir
    assert message =~ control
    assert message =~ "FLEZHA_SOCKET_FILES_PATH"

    # Nothing removed: the running controller keeps its control socket and
    # every attached runner keeps its stream sockets.
    assert File.exists?(control)
    assert Enum.all?(streams, &File.exists?/1)
    assert Native.socket_dir_live(dir)
  end

  test "runners attached to the refused controller keep their bindings" do
    before = FleetManager.active_runners("default")
    assert before != []

    assert_raise RuntimeError, fn -> Support.prepare_socket_dir() end

    assert Enum.sort(FleetManager.active_runners("default")) == Enum.sort(before)
  end

  test "a socket directory nobody is listening in is stale, and is cleared" do
    dir = tmp_dir("flezha_stale")
    stale = Path.join(dir, Path.basename(Support.control_socket_path()))

    # A controller that died hard: the socket file outlives the listener.
    {:ok, listener} = :gen_tcp.listen(0, [{:ifaddr, {:local, String.to_charlist(stale)}}])
    :ok = :gen_tcp.close(listener)
    assert File.exists?(stale)
    refute Native.socket_dir_live(dir)

    on_exit(TestHelpers.override_env(:socket_files_path, dir))

    assert :ok = Support.prepare_socket_dir()
    refute File.exists?(stale)
    assert File.dir?(dir)
  end

  # ---------- the state root ----------

  test "this controller claimed its state root at boot, and still holds it" do
    lock_file = StateRootLock.held()

    assert is_binary(lock_file)
    assert Path.basename(lock_file) == ".instance.lock"

    # Re-claiming the root this process already holds is the no-op a restart in
    # one OS process takes; it answering at all is the claim still standing.
    assert StateRootLock.claim!(Path.dirname(lock_file)) == lock_file
  end

  @tag :multi_node
  test "a second controller is refused the state root, and the lock dies with its holder" do
    Process.flag(:trap_exit, true)
    root = tmp_dir("flezha_lock")

    {holder_pid, holder} = NodeTestHelper.start_bare_peer(:colocation_holder)
    assert {:ok, lock_file} = :erpc.call(holder, Native, :lock_state_root, [root])
    assert lock_file == Path.join(root, ".instance.lock")

    # Re-claiming is what one controller restarting in one OS process does.
    assert {:ok, ^lock_file} = :erpc.call(holder, Native, :lock_state_root, [root])

    # This node is the second controller: refused, and told who holds it.
    holder_os_pid = :erpc.call(holder, :os, :getpid, [])
    assert {:error, message} = Native.lock_state_root(root)
    assert message =~ "held by pid #{holder_os_pid}"

    # And the refusal an operator meets names the root and the variable.
    refusal = assert_raise(RuntimeError, fn -> StateRootLock.claim!(root) end).message

    assert refusal =~ Path.expand(root)
    assert refusal =~ "FLEZHA_STATE_DIR"

    # An untrapped kill leaves nothing to recover: the kernel drops the lock,
    # and the next controller over this root takes it.
    :erpc.cast(holder, :erlang, :halt, [1])
    assert_receive {:EXIT, ^holder_pid, _}, 10_000

    {_pid, successor} = NodeTestHelper.start_bare_peer(:colocation_successor)
    assert {:ok, ^lock_file} = :erpc.call(successor, Native, :lock_state_root, [root])
  end

  @tag :multi_node
  test "a controller booted over this one's socket directory refuses and clobbers nothing" do
    dir = Support.socket_dir()
    control = Support.control_socket_path()
    streams = Path.wildcard(Path.join(dir, "*_stream_*.sock"))
    runners = Enum.sort(FleetManager.active_runners("default"))

    {_pid, peer} = NodeTestHelper.start_peer_node(:colocation_clobber, socket_files_path: dir)

    # It never reached ready, and its refusal names the path and the variable.
    assert :erpc.call(peer, Process, :whereis, [Flezha.Supervisor]) == nil

    assert {:error, reason} = :erpc.call(peer, Application, :ensure_all_started, [:flezha])
    assert inspect(reason) =~ "FLEZHA_SOCKET_FILES_PATH"

    # This controller kept its control socket, its runners' stream sockets and
    # the runners themselves.
    assert File.exists?(control)
    assert Enum.all?(streams, &File.exists?/1)
    assert Enum.sort(FleetManager.active_runners("default")) == runners
  end

  # ---------- two controllers, driven ----------

  @tag :multi_node
  test "two controllers on one host serve their own runners and share no tree" do
    namespace = "colocation_ns"
    {peer_pid, peer} = NodeTestHelper.start_peer_node(:colocation_peer)

    # Both reached ready: the peer's boot took a socket directory, a state root
    # and a hub registry root of its own, so neither guard fired.
    for key <- NodeTestHelper.owned_resources() do
      mine = Application.get_env(:flezha, key)
      theirs = :erpc.call(peer, Application, :get_env, [:flezha, key])
      assert theirs != mine, "#{key} is shared between the two controllers"
    end

    # Each holds its own state root, and neither could take the other's.
    peer_lock = :erpc.call(peer, StateRootLock, :held, [])
    assert peer_lock != StateRootLock.held()
    assert_raise RuntimeError, fn -> StateRootLock.claim!(Path.dirname(peer_lock)) end

    # A runner attached to the peer is the peer's, and only the peer's.
    runner_id = NodeTestHelper.start_runner_remote(peer, namespace)
    assert :erpc.call(peer, FleetManager, :active_runners, [namespace]) == [runner_id]
    assert FleetManager.active_runners(namespace) == []

    # Its sockets are in the peer's directory and in no other.
    peer_dir = :erpc.call(peer, Support, :socket_dir, [])
    assert Path.wildcard(Path.join(peer_dir, "*_stream_#{runner_id}.sock")) != []
    assert Path.wildcard(Path.join(Support.socket_dir(), "*_stream_#{runner_id}.sock")) == []
    assert File.exists?(Path.join(peer_dir, Path.basename(Support.control_socket_path())))

    NodeTestHelper.stop_runner_remote(peer, namespace, runner_id)
    NodeTestHelper.stop_peer_node(peer_pid, peer)
  end

  # ---------- helpers ----------

  defp tmp_dir(prefix) do
    dir = Path.join(System.tmp_dir!(), "#{prefix}_#{System.unique_integer([:positive])}")
    File.mkdir_p!(dir)
    on_exit(fn -> File.rm_rf(dir) end)
    dir
  end
end
