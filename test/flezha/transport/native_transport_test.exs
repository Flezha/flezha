defmodule Flezha.Transport.NativeTransportTest do
  # async: false: a peer BEAM.
  use ExUnit.Case, async: false

  alias Flezha.Runner.RunnerSupervisor
  alias Flezha.Transport.Server
  alias Flezha.Transport.Local.Stream
  alias FlezhaTest.Helper.NodeTestHelper

  defp stop_native do
    _ = Supervisor.terminate_child(Flezha.Supervisor, {Server, :native})
    _ = Supervisor.delete_child(Flezha.Supervisor, {Server, :native})
    :ok
  end

  describe "get_stats(:native)" do
    setup do
      {:ok, _pid} = Server.ensure_started(:native)
      on_exit(&stop_native/0)
      :ok
    end

    test "returns the QUIC-shaped map with the :driver key ABSENT" do
      assert {:ok, stats} = Server.get_stats(:native)
      assert stats.transport == :native
      assert stats.status == :up
      assert stats.pending_streams == 0
      refute Map.has_key?(stats, :driver)
    end

    test "ensure_started/1 is idempotent" do
      {:ok, p1} = Server.ensure_started(:native)
      {:ok, p2} = Server.ensure_started(:native)
      assert p1 == p2
    end
  end

  test "the transport whitelist refuses an unknown atom string (runtime.exs closure)" do
    # Mirrors config/runtime.exs's local closure (kept out of a project module
    # because runtime.exs is evaluated before lib/ compiles on a cold checkout).
    parse = fn value ->
      case value do
        v when v in ["uds", "quic", "native"] -> String.to_atom(v)
        other -> raise "unknown transport #{inspect(other)}"
      end
    end

    assert parse.("native") == :native
    assert_raise RuntimeError, ~r/unknown transport "bogus"/, fn -> parse.("bogus") end
  end

  test "RunnerSupervisor.init raises on an unknown transport atom" do
    # Build the atom dynamically so the compiler's type checker treats it as
    # `atom()` rather than the literal the clauses reject.
    bogus = String.to_atom("bogus")

    assert_raise ArgumentError, ~r/unknown transport :bogus/, fn ->
      RunnerSupervisor.init({"ns", "r1", make_ref(), bogus})
    end
  end

  test ":native transport is not started by default" do
    refute :native in Server.list_active_transports()
  end

  @tag :multi_node
  test "connect/2 rejects a control handle owned by another BEAM node" do
    {peer_pid, node} = NodeTestHelper.start_peer_node(:native_reject_peer)
    on_exit(fn -> NodeTestHelper.stop_peer_node(peer_pid, node) end)

    # A live pid on the peer node — the native transport is same-node only.
    remote_pid = :erpc.call(node, :erlang, :whereis, [:code_server])
    assert node(remote_pid) == node
    handle = %Stream{pid: remote_pid, id: 1, side: :controller}

    assert {:error, :remote_node_rejected} =
             Server.connect(%{"namespace" => "default"}, handle)
  end
end
