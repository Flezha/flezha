defmodule Flezha.Plugin.Provider.WebsocketSubscriptionSourceTest do
  @moduledoc """
  The polled subscription source on `Plugin.Provider.Websocket`: attach-time
  refusals, the reconcile loop's in-place delta frames, acknowledgement-gated
  commit, durability of the applied set, and the convergence of the connect
  handshake and the data-liveness tier onto that set.

  The reconcile loop is driven at its own seam — `handle_info/2` with a settled
  poll result — rather than through a live socket, which is what lets a test
  assert on the exact frames a given source transition produces.
  """

  use ExUnit.Case, async: false

  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.Provider.Websocket.Subscription
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.HubManager
  alias Flezha.Service.RegistryUpdate

  @sub_template ~s({"method":"subscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})
  @unsub_template ~s({"method":"unsubscribe","params":{"symbol":{{items}}},"req_id":{{req_id}}})
  @post_connect %{"payload" => %{"type" => "text", "message" => "static-subscribe"}}

  @valid_opts %{
    uri: "wss://test.local",
    subscription_url: "https://cfg.internal/subs",
    subscribe_template: @sub_template,
    unsubscribe_template: @unsub_template
  }

  ## ---------- helpers ----------

  defp subscription(overrides \\ %{}) do
    struct!(
      Subscription,
      Map.merge(
        %{
          url: "https://cfg.internal/subs",
          headers: %{},
          # Long enough that the rescheduled poll never fires mid-test.
          interval_ms: 600_000,
          subscribe_template: @sub_template,
          unsubscribe_template: @unsub_template,
          ack_mode: "required"
        },
        overrides
      )
    )
  end

  defp state(overrides \\ %{}) do
    now = :erlang.monotonic_time(:millisecond)

    Map.merge(
      %{
        uri: "wss://test.local",
        plugin_id: "test_ws",
        post_connect: %{},
        # `self()` so the `{:io_in, …}` ingest and the applied-set persist cast
        # are both assertable.
        overwatcher_pid: self(),
        payload_metadata: %{},
        idle_timeout_ms: 10_000,
        resubscribe_after_ms: 0,
        ignore_pattern: nil,
        websocket_established?: true,
        frames_seen: 0,
        last_frame_at: now,
        last_data_frame_at: now,
        last_resubscribe_at: now,
        subscription: subscription(),
        applied: MapSet.new(),
        desired: nil,
        inflight: %{},
        next_req_id: 1,
        cycle: 0,
        poll: nil,
        poll_timer: nil,
        source_etag: nil,
        last_reconcile: nil,
        last_error: nil,
        source_plugin_id: "ws_plugin",
        source_namespace: "default",
        source_operative_id: "op-1"
      },
      Map.new(overrides)
    )
  end

  # Drive one settled poll through the reconcile loop.
  defp poll(state, result) do
    pid = spawn(fn -> :ok end)

    Websocket.handle_info({:subscription_fetched, pid, result}, %{state | poll: {pid, make_ref()}})
  end

  defp source(items, version \\ 1) do
    {:ok, Jason.encode!(%{"version" => version, "items" => items}), nil}
  end

  defp req_id(frame) do
    {:text, json} = frame
    Jason.decode!(json)["req_id"]
  end

  defp items_of(frame) do
    {:text, json} = frame
    Jason.decode!(json)["params"]["symbol"]
  end

  defp method_of(frame) do
    {:text, json} = frame
    Jason.decode!(json)["method"]
  end

  defp ack(req_id, extra \\ %{}) do
    {:text, Jason.encode!(Map.merge(%{"req_id" => req_id, "success" => true}, extra))}
  end

  defp attach_telemetry(event) do
    parent = self()
    handler_id = {__MODULE__, event, :erlang.unique_integer([:positive])}

    :telemetry.attach(
      handler_id,
      event,
      fn ^event, meas, meta, _ -> send(parent, {:telemetry, event, meas, meta}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)
  end

  ## ---------- 8.5 attach-time refusals ----------

  describe "8.5 validation" do
    test "a valid subscription source is accepted and normalised" do
      assert {:ok, opts} = Websocket.init_provider(@valid_opts)

      assert %{
               url: "https://cfg.internal/subs",
               interval_ms: 30_000,
               ack_mode: "required",
               headers: %{}
             } = opts.subscription

      assert opts.subscription.subscribe_template == @sub_template
      assert opts.applied_subscriptions == []
    end

    test "no subscription_url leaves the static provider exactly as it was" do
      assert {:ok, opts} = Websocket.init_provider(%{uri: "wss://test.local"})
      assert opts.subscription == nil
      assert opts.applied_subscriptions == []
    end

    test "an empty subscription_url is read as unconfigured, not as a malformed one" do
      assert {:ok, opts} =
               Websocket.init_provider(%{uri: "wss://test.local", subscription_url: ""})

      assert opts.subscription == nil
    end

    test "a subscribe_template with no {{items}} placeholder is refused" do
      assert {:error, message} =
               Websocket.init_provider(%{@valid_opts | subscribe_template: ~s({"req_id":1})})

      assert message =~ "subscribe_template"
      assert message =~ "{{items}}"
    end

    test "an unsubscribe_template with no {{items}} placeholder is refused" do
      assert {:error, message} =
               Websocket.init_provider(%{@valid_opts | unsubscribe_template: ~s({"req_id":1})})

      assert message =~ "unsubscribe_template"
      assert message =~ "{{items}}"
    end

    test "a subscription_url missing either template is refused" do
      assert {:error, message} =
               Websocket.init_provider(Map.delete(@valid_opts, :subscribe_template))

      assert message =~ "subscribe_template"

      assert {:error, message} =
               Websocket.init_provider(Map.delete(@valid_opts, :unsubscribe_template))

      assert message =~ "unsubscribe_template"
    end

    test "an interval below the 1000 ms floor is refused" do
      assert {:error, message} =
               Websocket.init_provider(Map.put(@valid_opts, :subscription_interval_ms, 100))

      assert message =~ "1000 ms minimum"
    end

    test "a non-http scheme, a hostless url and embedded credentials are each refused" do
      assert {:error, m1} =
               Websocket.init_provider(%{@valid_opts | subscription_url: "ftp://cfg/subs"})

      assert m1 =~ "http or https"

      assert {:error, m2} = Websocket.init_provider(%{@valid_opts | subscription_url: "https://"})
      assert m2 =~ "no host"

      assert {:error, m3} =
               Websocket.init_provider(%{@valid_opts | subscription_url: "https://u:p@cfg/subs"})

      assert m3 =~ "must not embed credentials"
      refute m3 =~ "p@"
    end

    test "malformed subscription headers are refused" do
      assert {:error, message} =
               Websocket.init_provider(Map.put(@valid_opts, :subscription_headers, "not-json"))

      assert message =~ "JSON object"

      assert {:error, message} =
               Websocket.init_provider(
                 Map.put(@valid_opts, :subscription_headers, %{"a" => %{"nested" => 1}})
               )

      assert message =~ "scalar"
    end

    test "an unrecognised ack mode is refused" do
      assert {:error, message} =
               Websocket.init_provider(Map.put(@valid_opts, :subscription_ack_mode, "maybe"))

      assert message =~ "subscription_ack_mode"
    end

    test "an unknown option is refused rather than silently ignored" do
      assert {:error, message} =
               Websocket.init_provider(Map.put(@valid_opts, :reconnect_policy, "aggressive"))

      assert message =~ "unknown option(s) for websocket: reconnect_policy"
    end

    test "every host-injected option is accepted" do
      opts =
        Enum.reduce(ProviderBehaviour.host_opts(), @valid_opts, &Map.put(&2, &1, nil))

      assert {:ok, _} = Websocket.init_provider(opts)
    end
  end

  ## ---------- 8.1 reconcile sends deltas in place ----------

  describe "8.1 reconcile" do
    test "an added item is subscribed in place, naming only the addition" do
      s = state(applied: MapSet.new(["a"]), desired: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      assert method_of(frame) == "subscribe"
      assert items_of(frame) == ["b"]
      # In place: the socket is untouched and the process did not exit.
      assert ns.websocket_established? == true
      assert ns.last_reconcile == %{added: 1, removed: 0, sent: true}
    end

    test "a removed item is unsubscribed in place, naming only the removal" do
      s = state(applied: MapSet.new(["a", "b"]), desired: MapSet.new(["a", "b"]))

      assert {:reply, [frame], ns} = poll(s, source(["a"]))
      assert method_of(frame) == "unsubscribe"
      assert items_of(frame) == ["b"]
      assert ns.websocket_established? == true
    end

    test "a simultaneous addition and removal sends both frames with distinct req_ids" do
      s = state(applied: MapSet.new(["a", "b"]), desired: MapSet.new(["a", "b"]))

      assert {:reply, [sub, unsub], _ns} = poll(s, source(["a", "c"]))
      assert method_of(sub) == "subscribe"
      assert items_of(sub) == ["c"]
      assert method_of(unsub) == "unsubscribe"
      assert items_of(unsub) == ["b"]
      assert req_id(sub) != req_id(unsub)
    end

    test "an unchanged source sends nothing across three cycles" do
      s = state(applied: MapSet.new(["a", "b"]), desired: MapSet.new(["a", "b"]))

      Enum.reduce(1..3, s, fn _, acc ->
        assert {:ok, ns} = poll(acc, source(["a", "b"]))
        assert ns.last_reconcile == %{added: 0, removed: 0, sent: false}
        ns
      end)
    end

    test "a reconcile that sent frames emits telemetry carrying the counts" do
      attach_telemetry([:flezha, :ws, :reconcile])
      s = state(applied: MapSet.new(["a", "b"]))

      assert {:reply, _, _} = poll(s, source(["a", "c", "d"]))

      assert_receive {:telemetry, [:flezha, :ws, :reconcile], %{added: 2, removed: 1}, meta}, 500
      assert meta.plugin_id == "test_ws"
    end

    test "an unchanged source emits no reconcile telemetry" do
      attach_telemetry([:flezha, :ws, :reconcile])
      s = state(applied: MapSet.new(["a"]))

      assert {:ok, _} = poll(s, source(["a"]))
      refute_receive {:telemetry, [:flezha, :ws, :reconcile], _, _}, 200
    end

    test "a send against a transiently unestablished socket is skipped, not fatal" do
      s = state(applied: MapSet.new(["a"]), websocket_established?: false)

      assert {:ok, ns} = poll(s, source(["a", "b"]))
      # Nothing committed and nothing in flight — the next cycle re-derives it.
      assert ns.applied == MapSet.new(["a"])
      assert ns.inflight == %{}
      assert ns.last_reconcile == %{added: 0, removed: 0, sent: false}

      # And the retry does happen once the socket is back.
      assert {:reply, [frame], _} = poll(%{ns | websocket_established?: true}, source(["a", "b"]))
      assert items_of(frame) == ["b"]
    end
  end

  ## ---------- 8.2 acknowledgement-gated commit ----------

  describe "8.2 commit gating" do
    test "an acknowledged subscribe commits and the next cycle sends nothing" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      refute MapSet.member?(ns.applied, "b")

      assert {:ok, ns} = Websocket.handle_in(ack(req_id(frame)), ns)
      assert ns.applied == MapSet.new(["a", "b"])
      assert ns.inflight == %{}

      assert {:ok, ns} = poll(ns, source(["a", "b"]))
      assert ns.last_reconcile == %{added: 0, removed: 0, sent: false}
    end

    test "an acknowledged unsubscribe removes the item from the applied set" do
      s = state(applied: MapSet.new(["a", "b"]))

      assert {:reply, [frame], ns} = poll(s, source(["a"]))
      assert {:ok, ns} = Websocket.handle_in(ack(req_id(frame)), ns)
      assert ns.applied == MapSet.new(["a"])
    end

    test "a failed acknowledgement is not committed and the delta is re-derived" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      first_req = req_id(frame)

      assert {:ok, ns} =
               Websocket.handle_in(
                 {:text, Jason.encode!(%{"req_id" => first_req, "success" => false})},
                 ns
               )

      refute MapSet.member?(ns.applied, "b")
      assert ns.last_error == {:rejected, first_req}

      # Re-derived identically, under a fresh req_id.
      assert {:reply, [retry], _} = poll(ns, source(["a", "b"]))
      assert items_of(retry) == ["b"]
      assert req_id(retry) != first_req
    end

    test "an error-shaped acknowledgement is a failure even with no success field" do
      s = state(applied: MapSet.new())

      assert {:reply, [frame], ns} = poll(s, source(["a"]))

      assert {:ok, ns} =
               Websocket.handle_in(
                 {:text, Jason.encode!(%{"req_id" => req_id(frame), "error" => "bad symbol"})},
                 ns
               )

      assert ns.applied == MapSet.new()
    end

    test "an unanswered request is re-derived on the next cycle and the stale id expires" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      first_req = req_id(frame)
      assert Map.has_key?(ns.inflight, first_req)

      assert {:reply, [retry], ns} = poll(ns, source(["a", "b"]))
      assert items_of(retry) == ["b"]

      # The abandoned id is gone, so a very late acknowledgement for it cannot
      # commit a set the source has since moved on from.
      refute Map.has_key?(ns.inflight, first_req)
      assert Map.has_key?(ns.inflight, req_id(retry))

      assert {:ok, unchanged} = Websocket.handle_in(ack(first_req), ns)
      assert unchanged.applied == MapSet.new(["a"])
    end

    test "an acknowledgement naming an unknown req_id changes nothing" do
      s = state(applied: MapSet.new(["a"]))
      assert {:reply, [_frame], ns} = poll(s, source(["a", "b"]))

      assert {:ok, after_ack} = Websocket.handle_in(ack(9_999), ns)
      assert after_ack.applied == ns.applied
      assert after_ack.inflight == ns.inflight
    end

    test "ack-free mode commits on send" do
      s = state(applied: MapSet.new(["a"]), subscription: subscription(%{ack_mode: "none"}))

      assert {:reply, [_frame], ns} = poll(s, source(["a", "b"]))
      assert ns.applied == MapSet.new(["a", "b"])
      assert ns.inflight == %{}

      assert {:ok, ns} = poll(ns, source(["a", "b"]))
      assert ns.last_reconcile == %{added: 0, removed: 0, sent: false}
    end

    test "an acknowledgement affects data liveness only through ignore_pattern" do
      s =
        state(
          applied: MapSet.new(),
          ignore_pattern: :binary.compile_pattern("req_id"),
          last_data_frame_at: 0,
          last_frame_at: 0
        )

      assert {:reply, [frame], ns} = poll(s, source(["a"]))

      # The acknowledgement matches the ignore pattern, so it refreshes
      # connection liveness only — exactly as any other heartbeat-shaped frame.
      assert {:ok, ns} = Websocket.handle_in(ack(req_id(frame)), ns)
      assert ns.last_frame_at != 0
      assert ns.last_data_frame_at == 0
      # …and it still committed.
      assert ns.applied == MapSet.new(["a"])
    end

    test "a plugin with no subscription source never reaches the ack clause" do
      # No `:inflight` key at all: the ack clause head cannot match, which is
      # what makes the per-frame path free for a static plugin.
      s = state() |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:ok, ns} = Websocket.handle_in({:text, ~s({"req_id":1,"success":true})}, s)
      refute Map.has_key?(ns, :inflight)
      assert ns.frames_seen == 1
    end
  end

  ## ---------- 8.3 durability ----------

  describe "8.3 durability" do
    test "a committed delta is persisted to the host for the cspec" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      assert {:ok, _ns} = Websocket.handle_in(ack(req_id(frame)), ns)

      assert_receive {:"$gen_cast",
                      {:plugin_opt_persist, "ws_plugin", :applied_subscriptions, ["a", "b"]}}
    end

    test "init_provider restores a persisted applied set" do
      assert {:ok, opts} =
               Websocket.init_provider(
                 Map.put(@valid_opts, :applied_subscriptions, ["b", "a", "a"])
               )

      # Normalised: de-duplicated and ordered, so a restored set and a
      # freshly-committed one compare equal.
      assert opts.applied_subscriptions == ["a", "b"]
    end

    test "a restored applied set is what the socket asserts on connect" do
      s = state(applied: MapSet.new(["a", "b"]), websocket_established?: false)

      assert {:reply, [frame], ns} = Websocket.handle_connect(101, [], s)
      assert method_of(frame) == "subscribe"
      assert items_of(frame) == ["a", "b"]
      assert ns.websocket_established? == true
    end

    test "the extension table carries a committed opt across a plugin restart" do
      plugin = %Flezha.Plugin{
        id: "ws_plugin",
        provider: Websocket,
        opts: %{uri: "wss://test.local"},
        subscriptions: []
      }

      table = %Table{plugins: %{"ws_plugin" => plugin}, subs: %{}}

      updated = Table.put_plugin_opt(table, "ws_plugin", :applied_subscriptions, ["a", "b"])
      assert updated.plugins["ws_plugin"].opts[:applied_subscriptions] == ["a", "b"]

      # `attempt_restart/3` re-runs `init_provider/1` from exactly this map.
      assert {:ok, opts} = Websocket.init_provider(updated.plugins["ws_plugin"].opts)
      assert opts.applied_subscriptions == ["a", "b"]
    end

    test "put_plugin_opt updates the subscription entries too, so there is no split brain" do
      plugin = %Flezha.Plugin{
        id: "ws_plugin",
        provider: Websocket,
        opts: %{uri: "wss://test.local"},
        subscriptions: []
      }

      table = %Table{
        plugins: %{"ws_plugin" => plugin},
        subs: %{
          on_input: [
            %{
              plugin_id: "ws_plugin",
              provider: Websocket,
              provider_opts: %{uri: "wss://test.local"}
            },
            %{plugin_id: "other", provider: Websocket, provider_opts: %{uri: "wss://other"}}
          ]
        }
      }

      updated = Table.put_plugin_opt(table, "ws_plugin", :applied_subscriptions, ["a"])
      [mine, other] = updated.subs.on_input

      assert mine.provider_opts[:applied_subscriptions] == ["a"]
      refute Map.has_key?(other.provider_opts, :applied_subscriptions)
    end

    test "an unknown plugin_id is a no-op rather than a crash" do
      table = %Table{plugins: %{}, subs: %{}}
      assert Table.put_plugin_opt(table, "gone", :applied_subscriptions, ["a"]) == table
    end

    test "the cspec transform round-trips the applied set for a controller restart" do
      plugin = %Flezha.Plugin{
        id: "ws_plugin",
        provider: Websocket,
        opts: %{uri: "wss://test.local"},
        subscriptions: []
      }

      cspec = %{
        id: "op-1",
        start:
          {Flezha.Operative.Overwatch, :start_link,
           [{"default", %{opts: [plugins: [{"ws_plugin", plugin}]]}}]}
      }

      rewritten =
        HubManager.rewrite_cspec_plugins(
          cspec,
          RegistryUpdate.put_plugin_opt("ws_plugin", :applied_subscriptions, ["a", "b"])
        )

      {_m, _f, [{_ns, operative}]} = rewritten.start
      [{"ws_plugin", restored}] = operative.opts[:plugins]
      assert restored.opts[:applied_subscriptions] == ["a", "b"]
    end

    test "a plugin no longer in the manifest is not resurrected by a late write" do
      transform = RegistryUpdate.put_plugin_opt("gone", :applied_subscriptions, ["a"])
      assert transform.([]) == []
    end

    test "three consecutive failed polls never unsubscribe anything" do
      s = state(applied: MapSet.new(["a", "b"]), desired: MapSet.new(["a", "b"]))

      final =
        Enum.reduce(
          [{:error, :nxdomain}, {:error, :timeout}, {:error, {:http_status, 503}}],
          s,
          fn
            result, acc ->
              assert {:ok, ns} = poll(acc, result)
              assert ns.applied == MapSet.new(["a", "b"])
              ns
          end
        )

      assert final.last_error == {:http_status, 503}
      refute_received {:"$gen_cast", {:plugin_opt_persist, _, _, _}}
    end

    test "a malformed or itemless source is a failure, never an empty desired set" do
      s = state(applied: MapSet.new(["a"]), desired: MapSet.new(["a"]))

      assert {:ok, ns} = poll(s, {:ok, "not json", nil})
      assert ns.applied == MapSet.new(["a"])
      assert ns.last_error == :invalid_json

      assert {:ok, ns} = poll(s, {:ok, Jason.encode!(%{"version" => 1}), nil})
      assert ns.applied == MapSet.new(["a"])
      assert ns.last_error == :missing_items

      assert {:ok, ns} = poll(s, {:ok, Jason.encode!(%{"items" => [1, 2]}), nil})
      assert ns.applied == MapSet.new(["a"])
      assert ns.last_error == :items_must_be_strings
    end

    test "an explicitly empty source does unsubscribe everything" do
      # The counterpart to the refusals above: an empty set that the source
      # actually published is a real instruction, not a failure.
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], _ns} = poll(s, source([]))
      assert method_of(frame) == "unsubscribe"
      assert items_of(frame) == ["a"]
    end
  end

  ## ---------- 8.4 connect and watchdog convergence ----------

  describe "8.4 connect and watchdog" do
    test "a reconnect issues the current applied set, not the boot-time payload" do
      s =
        state(
          applied: MapSet.new(["a", "b"]),
          post_connect: @post_connect,
          websocket_established?: false
        )

      assert {:reply, [frame], _ns} = Websocket.handle_connect(101, [], s)
      assert items_of(frame) == ["a", "b"]
      refute match?({:text, "static-subscribe"}, frame)
    end

    test "the data-liveness re-subscribe resends the current applied set" do
      attach_telemetry([:flezha, :ws, :resubscribe])
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

      assert {:reply, [frame], _ns} = Websocket.handle_info(:idle_check, s)
      assert items_of(frame) == ["a", "b"]
      assert_receive {:telemetry, [:flezha, :ws, :resubscribe], _, _}, 500
    end

    test "a re-assertion carries the reserved req_id so its ack cannot commit a delta" do
      s = state(applied: MapSet.new(["a"]), websocket_established?: false)

      assert {:reply, [frame], ns} = Websocket.handle_connect(101, [], s)
      assert req_id(frame) == 0
      # The in-flight space starts at 1, so nothing can collide with it.
      assert ns.next_req_id == 1
    end

    test "an empty applied set asserts nothing on connect" do
      s = state(applied: MapSet.new(), websocket_established?: false)
      assert {:ok, ns} = Websocket.handle_connect(101, [], s)
      assert ns.websocket_established? == true
    end

    test "a plugin with no subscription source still sends post_connect unchanged" do
      s =
        state(post_connect: @post_connect, websocket_established?: false)
        |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:reply, [{:text, "static-subscribe"}], ns} = Websocket.handle_connect(101, [], s)
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

      assert {:reply, [{:text, "static-subscribe"}], _ns} = Websocket.handle_info(:idle_check, s)
    end

    test "reconnecting does not leave a second poll chain armed" do
      # fails when: `handle_connect/3` schedules a poll without cancelling the
      # one the previous connection armed, so every reconnect doubles the rate
      # the subscription source is polled at.
      s = state(subscription: subscription(%{interval_ms: 60_000}), websocket_established?: false)

      assert {:ok, first} = Websocket.handle_connect(101, [], s)
      assert is_reference(first.poll_timer)

      assert {:ok, second} = Websocket.handle_connect(101, [], first)
      assert second.poll_timer != first.poll_timer
      # The first connection's timer is cancelled, not merely forgotten.
      assert Process.read_timer(first.poll_timer) == false
      assert is_integer(Process.read_timer(second.poll_timer))
    end

    test "a static plugin schedules no subscription poll" do
      s =
        state(websocket_established?: false)
        |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:ok, _} = Websocket.handle_connect(101, [], s)
      refute_receive :subscription_poll, 200
    end
  end

  ## ---------- 7.1 observability ----------

  describe "7.1 reconcile state is reportable" do
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
              {:ok, _} = Websocket.handle_info(msg, s)
              Process.sleep(200)
          end
        end)

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
      {:ok, committed} = Websocket.handle_in(ack(req_id(frame)), updated)

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
               Websocket.init_provider(%{
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
      assert {:reply, frames, updated} = Websocket.handle_connect(101, %{}, state)
      subscribe = Enum.find(frames, &(method_of(&1) == "subscribe"))
      assert Enum.sort(items_of(subscribe)) == ["BTC-USD", "ETH-USD"]

      # Uncommitted until acknowledged, exactly like a polled delta.
      assert updated.applied == MapSet.new()
      {:ok, committed} = Websocket.handle_in(ack(req_id(subscribe)), updated)
      assert Enum.sort(committed.applied) == ["BTC-USD", "ETH-USD"]
    end

    test "a static list is partition-filtered like a polled one" do
      assert {:ok, opts} =
               Websocket.init_provider(%{
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
      assert {:error, message} =
               Websocket.init_provider(%{
                 uri: "wss://test.local",
                 subscription_url: "https://cfg.internal/subs",
                 subscription_items: ["BTC-USD"],
                 subscribe_template: @sub_template,
                 unsubscribe_template: @unsub_template
               })

      assert message =~ "mutually exclusive"
    end

    test "a non-string item list is refused" do
      assert {:error, message} =
               Websocket.init_provider(%{
                 uri: "wss://test.local",
                 subscription_items: [1, 2],
                 subscribe_template: @sub_template,
                 unsubscribe_template: @unsub_template
               })

      assert message =~ "non-empty strings"
    end
  end

  # Fold a `{:reply, frames, state}` / `{:ok, state}` pair into `{frames, state}`.
  defp settle({:reply, frames, state}), do: {frames, state}
  defp settle({:ok, state}), do: {[], state}

  # Acknowledge every in-flight request so the deltas commit.
  defp commit_all(state) do
    Enum.reduce(Map.keys(state.inflight), {:ok, state}, fn id, {:ok, acc} ->
      Websocket.handle_in(ack(id), acc)
    end)
  end
end
