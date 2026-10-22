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

  defp start_channel({runner_id, name, type}) do
    args = {"_system", runner_id, name, type}
    start_supervised!(Supervisor.child_spec({StreamChannel, args}, id: name))
  end
end
