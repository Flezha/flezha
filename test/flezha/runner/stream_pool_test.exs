defmodule FlezhaTest.Runner.StreamPoolTest do
  alias Flezha.Runner.StreamChannel
  alias Flezha.Runner.StreamPool

  # async: false: registered runner channels.

  use ExUnit.Case, async: false

  describe "bind_stream_to_first_nil/2" do
    setup context do
      base =
        "stream_sup_test_#{context.test}"
        |> String.replace(~r/[^a-zA-Z0-9_]/, "_")

      live_name = String.to_atom(base <> "_live")
      nil_name = String.to_atom(base <> "_nil")

      {:ok, live_pid} = StreamChannel.start_link({"ce_test", live_name, :primary})
      {:ok, nil_pid} = StreamChannel.start_link({"ce_test", nil_name, :primary})

      :ok = GenServer.call(live_pid, {:bind, :existing_ref})

      on_exit(fn ->
        for pid <- [live_pid, nil_pid], Process.alive?(pid), do: GenServer.stop(pid)
      end)

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
end
