defmodule Flezha.Plugin.Provider.WebsocketAppliedSetTest do
  @moduledoc """
  Everything that reads the applied set a subscription source converged on: the
  connect handshake and the data-liveness watchdog re-issuing it, the reconcile
  report that renders it, the partition range that narrows it, and the static
  `subscription_items` source that fills it without a poll.
  """

  # async: false: the live controller and its fleet; global telemetry handlers.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ProviderPrepare, only: [prepare: 2]

  import FlezhaTest.Helper.WebsocketSourceFixtures

  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.Websocket.Subscription
  alias FlezhaTest.Helper.NodeTestHelper
  alias FlezhaTest.Helper.WebsocketSourceFixtures
  alias Flezha.Process.Timer

  @sub_template WebsocketSourceFixtures.sub_template()
  @unsub_template WebsocketSourceFixtures.unsub_template()
  @post_connect %{"payload" => %{"type" => "text", "message" => "static-subscribe"}}

  ## ---------- 8.4 connect and watchdog convergence ----------

  describe "connect and watchdog" do
    test "a reconnect issues the current applied set, not the boot-time payload" do
      s =
        state(
          applied: MapSet.new(["a", "b"]),
          post_connect: @post_connect,
          websocket_established?: false
        )

      assert {:reply, [frame], _ns} = Websocket.Client.handle_connect(101, [], s)
      assert items_of(frame) == ["a", "b"]
      refute match?({:text, "static-subscribe"}, frame)
    end

    test "the data-liveness re-subscribe resends the current applied set" do
      attach_telemetry([:flezha, :plugin, :websocket, :resubscribe])
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          applied: MapSet.new(["a", "b"]),
          post_connect: @post_connect,
          resubscribe_after_ms: 100,
          last_frame_at: now,
          last_data_frame_at: now - 500,
          last_resubscribe_at: now - 500
        )

      assert {:reply, [frame], _ns} = Websocket.Client.handle_info(:idle_check, s)
      assert items_of(frame) == ["a", "b"]
      assert_receive {:telemetry, [:flezha, :plugin, :websocket, :resubscribe], _, _}, 500
    end

    test "a re-assertion carries the reserved req_id so its ack cannot commit a delta" do
      s = state(applied: MapSet.new(["a"]), websocket_established?: false)

      assert {:reply, [frame], ns} = Websocket.Client.handle_connect(101, [], s)
      assert req_id(frame) == 0
      # The in-flight space starts at 1, so nothing can collide with it.
      assert ns.next_req_id == 1
    end

    test "an empty applied set asserts nothing on connect" do
      s = state(applied: MapSet.new(), websocket_established?: false)
      assert {:ok, ns} = Websocket.Client.handle_connect(101, [], s)
      assert ns.websocket_established? == true
    end

    test "a plugin with no subscription source still sends post_connect unchanged" do
      s =
        state(post_connect: @post_connect, websocket_established?: false)
        |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:reply, [{:text, "static-subscribe"}], ns} =
               Websocket.Client.handle_connect(101, [], s)

      assert ns.websocket_established? == true
    end

    test "a static plugin's data-liveness re-subscribe still resends post_connect" do
      now = :erlang.monotonic_time(:millisecond)

      s =
        state(
          post_connect: @post_connect,
          resubscribe_after_ms: 100,
          last_frame_at: now,
          last_data_frame_at: now - 500,
          last_resubscribe_at: now - 500
        )
        |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:reply, [{:text, "static-subscribe"}], _ns} =
               Websocket.Client.handle_info(:idle_check, s)
    end

    test "reconnecting does not leave a second poll chain armed" do
      # fails when: `handle_connect/3` schedules a poll without cancelling the
      # one the previous connection armed, so every reconnect doubles the rate
      # the subscription source is polled at.
      s = state(subscription: subscription(%{interval_ms: 60_000}), websocket_established?: false)

      assert {:ok, first} = Websocket.Client.handle_connect(101, [], s)
      assert Timer.armed?(first.timers, :poll)
      {first_ref, _msg} = Map.fetch!(first.timers, :poll)

      assert {:ok, second} = Websocket.Client.handle_connect(101, [], first)
      {second_ref, _msg} = Map.fetch!(second.timers, :poll)
      assert second_ref != first_ref
      # The first connection's timer is cancelled, not merely forgotten.
      assert Process.read_timer(first_ref) == false
      assert is_integer(Process.read_timer(second_ref))
    end

    test "a static plugin schedules no subscription poll" do
      s =
        state(websocket_established?: false)
        |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:ok, connected} = Websocket.Client.handle_connect(101, [], s)
      refute Timer.armed?(connected.timers, :poll), "a poll timer was armed"
      refute_received :subscription_poll
    end
  end

  ## ---------- 7.1 observability ----------

  describe "reconcile state is reportable" do
    test "a static plugin reports nothing" do
      assert Websocket.inspect_state(%{uri: "wss://test.local"}) == nil
    end

    test "a configured plugin with no live connection reports its last-known set" do
      report =
        Websocket.inspect_state(%{
          subscription: subscription(),
          applied_subscriptions: ["a", "b"],
          owned_pid: nil
        })

      assert report.applied == ["a", "b"]
      assert report.last_error == :no_connection
    end

    test "a live connection reports its applied set, last reconcile and last error" do
      parent = self()

      pid =
        spawn(fn ->
          s = state(applied: MapSet.new(["a"]), last_error: :nxdomain)
          s = %{s | last_reconcile: %{added: 1, removed: 0, sent: true}}
          send(parent, :armed)

          receive do
            msg ->
              {:ok, _} = Websocket.Client.handle_info(msg, s)
              NodeTestHelper.park()
          end
        end)

      on_exit(fn -> Process.exit(pid, :kill) end)
      assert_receive :armed, 500

      report = Websocket.inspect_state(%{subscription: subscription(), owned_pid: pid})

      assert report.applied == ["a"]
      assert report.last_reconcile == %{added: 1, removed: 0, sent: true}
      assert report.last_error == :nxdomain
    end
  end

  ## ---------- partitioned sources ----------

  describe "partition filtering" do
    # ranges(3) = [{0, 84}, {85, 169}, {170, 255}]; SOL-USD hashes to group 60,
    # BTC-USD to 129, ETH-USD to 213 — one item per partition, pinned by
    # `Flezha.PartitionTest`.
    @items ["SOL-USD", "BTC-USD", "ETH-USD"]

    defp partitioned(index) do
      range = Enum.at(Flezha.Partition.ranges(3), index)
      state(%{subscription: subscription(%{range: range})})
    end

    test "each member's desired set contains only the items it owns" do
      for {index, owned} <- [{0, ["SOL-USD"]}, {1, ["BTC-USD"]}, {2, ["ETH-USD"]}] do
        assert {:reply, [frame], updated} = poll(partitioned(index), source(@items))
        assert items_of(frame) == owned
        assert method_of(frame) == "subscribe"
        assert Enum.sort(updated.desired) == owned
      end
    end

    test "a member never commits another member's item to its durable applied set" do
      {:reply, [frame], updated} = poll(partitioned(1), source(@items))
      {:ok, committed} = Websocket.Client.handle_in(ack(req_id(frame)), updated)

      assert Enum.sort(committed.applied) == ["BTC-USD"]
      assert_receive {:"$gen_cast", {:plugin_opt_persist, _, :applied_subscriptions, ["BTC-USD"]}}
    end

    test "a new item is picked up by exactly the member owning its key group" do
      # ETH-USD appears in the published list; only p2 subscribes to it.
      for {index, expected} <- [{0, []}, {1, []}, {2, ["ETH-USD"]}] do
        state = partitioned(index)
        {_, updated} = settle(poll(state, source(["SOL-USD", "BTC-USD"])))
        {:ok, committed} = commit_all(updated)

        case poll(committed, source(@items, 2)) do
          {:reply, [frame], _} -> assert items_of(frame) == expected
          {:ok, _} -> assert expected == []
        end
      end
    end

    test "an unpartitioned plugin still sees the whole list" do
      assert {:reply, [frame], _} = poll(state(), source(@items))
      assert Enum.sort(items_of(frame)) == Enum.sort(@items)
    end
  end

  describe "a static subscription_items source" do
    test "drives the same reconcile and ack gating as a polled list" do
      assert {:ok, opts} =
               prepare(Websocket, %{
                 uri: "wss://test.local",
                 subscription_items: ["BTC-USD", "ETH-USD"],
                 subscribe_template: @sub_template,
                 unsubscribe_template: @unsub_template
               })

      assert opts.subscription.items == ["BTC-USD", "ETH-USD"]
      assert opts.subscription.url == nil
      refute Subscription.polled?(opts.subscription)

      desired = Subscription.static_desired(opts.subscription)
      assert Enum.sort(desired) == ["BTC-USD", "ETH-USD"]

      # The reconcile is the loop: with nothing applied, connect subscribes.
      state = state(%{subscription: opts.subscription, desired: desired})
      assert {:reply, frames, updated} = Websocket.Client.handle_connect(101, %{}, state)
      subscribe = Enum.find(frames, &(method_of(&1) == "subscribe"))
      assert Enum.sort(items_of(subscribe)) == ["BTC-USD", "ETH-USD"]

      # Uncommitted until acknowledged, exactly like a polled delta.
      assert updated.applied == MapSet.new()
      {:ok, committed} = Websocket.Client.handle_in(ack(req_id(subscribe)), updated)
      assert Enum.sort(committed.applied) == ["BTC-USD", "ETH-USD"]
    end

    test "a static list is partition-filtered like a polled one" do
      assert {:ok, opts} =
               prepare(Websocket, %{
                 uri: "wss://test.local",
                 subscription_items: ["SOL-USD", "BTC-USD", "ETH-USD"],
                 subscribe_template: @sub_template,
                 unsubscribe_template: @unsub_template,
                 partition_index: 1,
                 partition_count: 3,
                 key_group_from: 85,
                 key_group_to: 169
               })

      assert Enum.sort(Subscription.static_desired(opts.subscription)) == ["BTC-USD"]
    end

    test "declaring both sources is refused at init" do
      assert {:error, {:options_exclusive, :subscription_url, :subscription_items}} =
               prepare(Websocket, %{
                 uri: "wss://test.local",
                 subscription_url: "https://cfg.internal/subs",
                 subscription_items: ["BTC-USD"],
                 subscribe_template: @sub_template,
                 unsubscribe_template: @unsub_template
               })
    end

    test "a non-string item list is refused" do
      assert {:error, {:invalid_option_value, :subscription_items, [1, 2]}} =
               prepare(Websocket, %{
                 uri: "wss://test.local",
                 subscription_items: [1, 2],
                 subscribe_template: @sub_template,
                 unsubscribe_template: @unsub_template
               })
    end
  end

  # Fold a `{:reply, frames, state}` / `{:ok, state}` pair into `{frames, state}`.
  defp settle({:reply, frames, state}), do: {frames, state}
  defp settle({:ok, state}), do: {[], state}

  # Acknowledge every in-flight request so the deltas commit.
  defp commit_all(state) do
    Enum.reduce(Map.keys(state.inflight), {:ok, state}, fn id, {:ok, acc} ->
      Websocket.Client.handle_in(ack(id), acc)
    end)
  end
end
