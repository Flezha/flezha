defmodule FlezhaTest.Operative.BindReforwardTest do
  @moduledoc """
  `input-delivery-guarantee`: an input the Controller accepts is delivered or
  refused, never accepted-and-dropped — plus §6.3, that the `io_in` path counts
  an unbound-stream drop instead of discarding the channel's answer silently.

  The precondition every case here needs is "the operative's data stream is not
  bound yet". It is forced, never raced: a real `StreamChannel` starts with a
  nil `stream_ref` and stays there until this test binds a `Local.Stream` to it,
  so the unbound window is as wide as the test wants and the bind happens on the
  product's own `{:bind, ref}` path.
  """
  # async: false: registered channel names, a shared state tree, :state_root,
  # a global telemetry handler.
  use ExUnit.Case, async: false

  alias Flezha.Operative.Ingest
  alias Flezha.Operative.Overwatch
  alias Flezha.Operative.Replay
  alias Flezha.Plugin.Extension.Table
  alias Flezha.Runner.StreamChannel
  alias Flezha.State.InputLog
  alias Flezha.State.TestHelpers
  alias Flezha.Transport.Local
  alias FlezhaTest.Helper.NodeTestHelper

  setup do
    base = TestHelpers.tmp_app_data_dir()
    class = "bindfwd_" <> Integer.to_string(System.unique_integer([:positive]))
    TestHelpers.start_state_tree()

    {channel, channel_pid} = NodeTestHelper.unbound_stream_channel("bindfwd_runner")
    assert GenServer.call(channel, :stream_ref_status) == nil

    on_exit(fn ->
      TestHelpers.close_input_log(class)
      Application.delete_env(:flezha, :state_root)
      File.rm_rf!(base)
    end)

    %{class: class, channel: channel, channel_pid: channel_pid}
  end

  describe "an unbound stream" do
    test "counts the drop instead of discarding the channel's answer", ctx do
      test_pid = self()
      handler = "ow-io-drop-#{System.unique_integer([:positive])}"

      :telemetry.attach(
        handler,
        [:flezha, :stream, :drop],
        fn _e, m, md, _ -> send(test_pid, {:drop, m, md}) end,
        nil
      )

      on_exit(fn -> :telemetry.detach(handler) end)

      # An empty extension catalog makes the `:on_input` chain fall through to
      # the message passthrough, so a non-nil payload reaches the channel.
      state = overwatch(ctx, [])

      assert {:reply, :ok, ^state} =
               Overwatch.handle_call({:io_in, "msg", %{}}, {self(), make_ref()}, state)

      class = ctx.class
      assert_receive {:drop, %{count: 1}, %{operative_id: ^class}}
    end
  end

  describe "face one — an input accepted while the stream is unbound" do
    test "is forwarded once the stream binds", ctx do
      state = overwatch(ctx, effectively_once: true)

      # The cast path: nobody is told anything, so the journal's `:ok` is the
      # only record of what happened to this input.
      assert {:pending, state} = Ingest.confirming_ingest(state, "m0", %{}, :cast)
      assert_receive log_reply
      assert {:ok, state} = Ingest.journal_reply(state, log_reply)

      # Accepted: the input log holds it.
      assert [{0, "m0", _}] = InputLog.read_range(ctx.class, 0, 0)

      # ... and nothing reached a stream, because there was no stream.
      bind_stream(ctx)
      assert drain_frames() == []

      # The bind is what owes the delivery.
      settle_bind(state, ctx)

      assert_receive {:flz_data, _, class, "m0", %{"flz.offset" => "0"}}
      assert class == ctx.class
    end

    test "is refused for a class the Controller does not journal", ctx do
      state = overwatch(ctx, effectively_once: false)

      # Nothing durable holds it, so `:ok` would be a lie. The confirming entry
      # point is told so; the cast has nobody to tell, which is why the bind
      # must not leave this window open at all.
      assert {{:error, :not_ready}, _state} =
               Ingest.confirming_ingest(state, "m0", %{}, {:ingest, {self(), make_ref()}})
    end
  end

  describe "face two — a restore re-forward onto a stream that has not bound" do
    test "is delivered once the stream binds", ctx do
      for i <- 0..2, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})
      state = overwatch(ctx, effectively_once: true)

      # This is the restore's own re-forward, issued while the fresh runner has
      # completed its control handshake but not bound its data stream.
      assert :ok = Replay.replay_journaled_inputs(state)

      bind_stream(ctx)
      assert drain_frames() == []

      settle_bind(state, ctx)

      assert_receive {:flz_data, _, _, "m0", %{"flz.offset" => "0"}}
      assert_receive {:flz_data, _, _, "m1", %{"flz.offset" => "1"}}
      assert_receive {:flz_data, _, _, "m2", %{"flz.offset" => "2"}}
    end

    test "forwards only the tail above the Entity's consumed offset", ctx do
      for i <- 0..3, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})
      InputLog.record_consumed(ctx.class, {ctx.class, "proc", "a"}, 2)

      state = overwatch(ctx, effectively_once: true)
      assert :ok = Replay.replay_journaled_inputs(state)

      bind_stream(ctx)
      settle_bind(state, ctx)

      assert_receive {:flz_data, _, _, "m3", %{"flz.offset" => "3"}}
      assert drain_frames() == []
    end
  end

  describe "a flapping stream" do
    test "re-forwards the same tail rather than an accumulating one", ctx do
      for i <- 0..1, do: {:ok, _} = InputLog.append(ctx.class, "m#{i}", %{"input_type" => "2"})
      state = overwatch(ctx, effectively_once: true)

      delivered =
        Enum.flat_map(1..3, fn _ ->
          assert :ok = Replay.replay_journaled_inputs(state)
          bind_stream(ctx)
          settle_bind(state, ctx)
          frames = drain_frames()
          unbind_stream(ctx)
          frames
        end)

      # Three binds with no Entity progress deliver the same two inputs each
      # time; the DISTINCT count is the size of the tail, which is what the
      # effectively-once identity absorbs.
      assert delivered |> Enum.uniq() |> Enum.sort() == ["m0", "m1"]
      assert length(delivered) == 6
    end
  end

  describe "the bind gate" do
    test "passes straight through once a stream is bound", ctx do
      bind_stream(ctx)
      assert :ok = StreamChannel.await_bind(ctx.channel, 0)
    end

    test "returns when the bind lands during the wait", ctx do
      test_pid = self()

      gate =
        spawn_link(fn ->
          send(test_pid, {:gate, StreamChannel.await_bind(ctx.channel, 5_000)})
        end)

      # The interest is recorded in the channel's own turn BEFORE the bind —
      # which is the window the gate exists for, and why deciding and recording
      # cannot be two calls.
      assert NodeTestHelper.wait_until(
               fn -> MapSet.member?(:sys.get_state(ctx.channel_pid).bind_waiters, gate) end,
               1_000
             )

      {_sdk, controller} = Local.Stream.open()
      :ok = GenServer.call(ctx.channel, {:bind, controller})

      assert_receive {:gate, :ok}
    end

    test "gives up on a stream that never binds", ctx do
      assert {:error, :stream_unbound} = StreamChannel.await_bind(ctx.channel, 20)
    end

    test "has nothing to wait for when the runner has no channel", _ctx do
      assert :ok = StreamChannel.await_bind(nil, 5_000)
      assert :ok = StreamChannel.await_bind(:no_such_channel_process, 5_000)
    end
  end

  # --- helpers ---

  # Bind a live native stream to the channel through the product's own atomic
  # bind call, with this process owning the SDK side so every forwarded frame
  # lands in the test's mailbox.
  defp bind_stream(%{channel: channel}) do
    {sdk, controller} = Local.Stream.open()
    :ok = Local.Stream.controlling_process(sdk, self())
    :ok = Local.Stream.set_active(sdk)
    :ok = GenServer.call(channel, {:bind, controller})
    on_exit(fn -> Local.Stream.close(controller) end)
    :ok
  end

  # Drive the bind notification the way the Overwatch process would. Its
  # absence is tolerated so a failure reads as "the input was never delivered"
  # — the defect — rather than as one missing message.
  defp settle_bind(state, %{channel: channel}) do
    receive do
      {:stream_bound, ^channel} = msg ->
        assert {:noreply, _} = Overwatch.handle_info(msg, state)
    after
      200 -> :no_notification
    end
  end

  # Put the channel back in the unbound state the product reaches on a dropped
  # stream, so the next round starts from the same forced precondition.
  defp unbind_stream(%{channel: channel, channel_pid: pid}) do
    send(pid, {:flz_closed, 0, "test"})
    assert GenServer.call(channel, :stream_ref_status) == nil
    :ok
  end

  # Every frame the stream carried, in order — `[]` when it carried none.
  defp drain_frames(acc \\ []) do
    receive do
      {:flz_data, _, _, message, _} -> drain_frames([message | acc])
    after
      100 -> Enum.reverse(acc)
    end
  end

  defp overwatch(%{class: class, channel: channel}, overrides) do
    struct!(
      %Overwatch{
        id: class,
        namespace: "ns",
        default_input_type: "2",
        channel: channel,
        builder: %Flezha.Operative.Builder{id: class, factory_id: "f"},
        extensions: Table.new(%{})
      },
      overrides
    )
  end
end
