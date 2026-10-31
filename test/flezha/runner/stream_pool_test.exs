defmodule FlezhaTest.Runner.StreamPoolTest do
  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.StreamPool

  use ExUnit.Case, async: true

  describe "bind_stream_to_first_nil/2" do
    setup context do
      base =
        "stream_sup_test_#{context.test}"
        |> String.replace(~r/[^a-zA-Z0-9_]/, "_")

      live_name = String.to_atom(base <> "_live")
      nil_name = String.to_atom(base <> "_nil")

      live_pid = start_channel({"ce_test", live_name, :primary})
      nil_pid = start_channel({"ce_test", nil_name, :primary})

      :ok = GenServer.call(live_pid, {:bind, :existing_ref})

      %{live_name: live_name, live_pid: live_pid, nil_name: nil_name, nil_pid: nil_pid}
    end

    test "skips live-ref candidates and binds to the first nil-ref one", ctx do
      result = StreamPool.bind_stream_to_first_nil([ctx.live_name, ctx.nil_name], :new_ref)
      assert {:bound, bound_pid, bound_name} = result
      assert bound_pid == ctx.nil_pid
      assert bound_name == ctx.nil_name

      assert GenServer.call(ctx.live_pid, :stream_ref_status) == :existing_ref
      assert GenServer.call(ctx.nil_pid, :stream_ref_status) == :new_ref
    end

    test "returns nil when every candidate is already bound", ctx do
      :ok = GenServer.call(ctx.nil_pid, {:bind, :other_ref})

      assert nil ==
               StreamPool.bind_stream_to_first_nil(
                 [ctx.live_name, ctx.nil_name],
                 :new_ref
               )

      assert GenServer.call(ctx.live_pid, :stream_ref_status) == :existing_ref
      assert GenServer.call(ctx.nil_pid, :stream_ref_status) == :other_ref
    end

    test "skips dead/missing candidates and proceeds to live ones", ctx do
      result =
        StreamPool.bind_stream_to_first_nil(
          [:no_such_process_xyz, ctx.nil_name],
          :new_ref
        )

      assert {:bound, _pid, bound_name} = result
      assert bound_name == ctx.nil_name
      assert GenServer.call(ctx.nil_pid, :stream_ref_status) == :new_ref
    end
  end

  describe "routes" do
    test "reach a session's channels and leave them" do
      runner_id = "route_runner_#{System.unique_integer([:positive])}"
      [name | _] = StreamPool.stream_ids(runner_id, :primary)
      channel = start_channel({runner_id, name, :primary})

      assert StreamPool.put_route(runner_id, "routed_op", self()) == :ok
      assert :sys.get_state(channel).relay_mapping["routed_op"] == [self()]

      assert StreamPool.drop_route(runner_id, "routed_op") == :ok
      refute Map.has_key?(:sys.get_state(channel).relay_mapping, "routed_op")
    end

    test "for a session with no channels, change nothing and raise nothing" do
      assert StreamPool.put_route("no_such_runner", "r1", self()) == :ok
      assert StreamPool.drop_route("no_such_runner", "r1") == :ok
    end
  end

  describe "release_streams/1" do
    # fails when: the rebind leaves a relay of the session unparked, and it
    # forwards fresh input ahead of the tail its lost stream dropped.
    test "lets every channel's stream go, then parks each relay they route, once" do
      runner_id = "release_runner_#{System.unique_integer([:positive])}"
      elsewhere = "#{runner_id}_elsewhere"
      [first, second | _] = StreamPool.stream_ids(runner_id, :primary)
      start_channel({runner_id, first, :primary})
      start_channel({runner_id, second, :primary})
      start_channel({elsewhere, hd(StreamPool.stream_ids(elsewhere, :primary)), :primary})
      :ok = GenServer.call(first, {:bind, :old_ref})
      mine = answering_relay()
      other = answering_relay()
      :ok = StreamPool.put_route(runner_id, "routed_op", mine)
      :ok = StreamPool.put_route(elsewhere, "routed_op", other)

      assert StreamPool.release_streams(runner_id) == :ok

      assert GenServer.call(first, :stream_ref_status) == nil
      assert_received {:asked, ^mine, {:park_stream, ^runner_id}}
      refute_received {:asked, ^mine, _}
      refute_received {:asked, ^other, _}
    end
  end

  # A stand-in Overwatch that answers every call `:ok` and tells the test what
  # it was asked.
  defp answering_relay do
    test_pid = self()
    pid = spawn(fn -> answer_calls(test_pid) end)
    on_exit(fn -> Process.exit(pid, :kill) end)
    pid
  end

  defp answer_calls(test_pid) do
    receive do
      {:"$gen_call", from, request} ->
        send(test_pid, {:asked, self(), request})
        GenServer.reply(from, :ok)
        answer_calls(test_pid)
    end
  end

  defp start_channel({runner_id, name, type}) do
    args = {"_system", runner_id, name, type}
    start_supervised!(Supervisor.child_spec({StreamChannel, args}, id: name))
  end
end
