defmodule Flezha.Plugin.Provider.WebsocketSubscriptionSourceTest do
  @moduledoc """
  The polled subscription source on `Plugin.Provider.Websocket`: attach-time
  refusals, the reconcile loop's in-place delta frames, acknowledgement-gated
  commit, and durability of the applied set.

  The reconcile loop is driven at its own seam — `handle_info/2` with a settled
  poll result — rather than through a live socket, which is what lets a test
  assert on the exact frames a given source transition produces.
  """

  # async: false: the live controller and its fleet; global telemetry handlers.

  use ExUnit.Case, async: false

  import FlezhaTest.Helper.ProviderPrepare, only: [prepare: 2]

  import FlezhaTest.Helper.WebsocketSourceFixtures

  alias Flezha.Plugin.Extension.Table
  alias Flezha.Plugin.Provider.Websocket
  alias Flezha.Plugin.ProviderBehaviour
  alias Flezha.Service.HubManager
  alias Flezha.Service.RegistryUpdate
  alias FlezhaTest.Helper.WebsocketSourceFixtures

  @sub_template WebsocketSourceFixtures.sub_template()

  @valid_opts %{
    uri: "wss://test.local",
    subscription_url: "https://cfg.internal/subs",
    subscribe_template: @sub_template,
    unsubscribe_template: WebsocketSourceFixtures.unsub_template()
  }

  describe "validation" do
    test "a valid subscription source is accepted and normalised" do
      assert {:ok, opts} = prepare(Websocket, @valid_opts)

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
      assert {:ok, opts} = prepare(Websocket, %{uri: "wss://test.local"})
      assert opts.subscription == nil
      assert opts.applied_subscriptions == []
    end

    test "an empty subscription_url is read as unconfigured, not as a malformed one" do
      assert {:ok, opts} =
               prepare(Websocket, %{uri: "wss://test.local", subscription_url: ""})

      assert opts.subscription == nil
    end

    test "a subscribe_template with no {{items}} placeholder is refused" do
      assert {:error, {:template_missing_placeholder, :subscribe_template, "{{items}}"}} =
               prepare(Websocket, %{@valid_opts | subscribe_template: ~s({"req_id":1})})
    end

    test "an unsubscribe_template with no {{items}} placeholder is refused" do
      assert {:error, {:template_missing_placeholder, :unsubscribe_template, "{{items}}"}} =
               prepare(Websocket, %{@valid_opts | unsubscribe_template: ~s({"req_id":1})})
    end

    test "a subscription_url missing either template is refused" do
      assert {:error, {:option_requires, :subscription_url, :subscribe_template}} =
               prepare(Websocket, Map.delete(@valid_opts, :subscribe_template))

      assert {:error, {:option_requires, :subscription_url, :unsubscribe_template}} =
               prepare(Websocket, Map.delete(@valid_opts, :unsubscribe_template))
    end

    test "an interval below the 1000 ms floor is refused" do
      assert {:error, {:option_below_minimum, :subscription_interval_ms, 100, 1000}} =
               prepare(Websocket, Map.put(@valid_opts, :subscription_interval_ms, 100))
    end

    test "a non-http scheme, a hostless url and embedded credentials are each refused" do
      assert {:error, {:url_scheme_unsupported, "websocket", :subscription_url, _}} =
               prepare(Websocket, %{@valid_opts | subscription_url: "ftp://cfg/subs"})

      assert {:error, {:url_without_host, "websocket", :subscription_url, _}} =
               prepare(Websocket, %{@valid_opts | subscription_url: "https://"})

      assert {:error, {:url_has_credentials, "websocket", :subscription_url, _} = reason} =
               prepare(Websocket, %{@valid_opts | subscription_url: "https://u:p@cfg/subs"})

      refute inspect(reason) =~ "p@"
    end

    test "malformed subscription headers are refused" do
      assert {:error, {:invalid_option_value, :subscription_headers, "not-json"}} =
               prepare(Websocket, Map.put(@valid_opts, :subscription_headers, "not-json"))

      assert {:error, {:headers_invalid, "websocket", :subscription_headers}} =
               prepare(
                 Websocket,
                 Map.put(@valid_opts, :subscription_headers, %{"a" => %{"nested" => 1}})
               )
    end

    test "an unrecognised ack mode is refused" do
      assert {:error, {:invalid_option_value, :subscription_ack_mode, "maybe"}} =
               prepare(Websocket, Map.put(@valid_opts, :subscription_ack_mode, "maybe"))
    end

    test "an unknown option is refused rather than silently ignored" do
      assert {:error, {:unknown_options, "websocket", ["reconnect_policy"]}} =
               prepare(Websocket, Map.put(@valid_opts, :reconnect_policy, "aggressive"))
    end

    test "every host-injected option is accepted" do
      opts =
        Enum.reduce(ProviderBehaviour.host_opts(), @valid_opts, &Map.put(&2, &1, nil))

      assert {:ok, _} = prepare(Websocket, opts)
    end
  end

  ## ---------- 8.1 reconcile sends deltas in place ----------

  describe "reconcile" do
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
      attach_telemetry([:flezha, :plugin, :websocket, :reconcile])
      s = state(applied: MapSet.new(["a", "b"]))

      assert {:reply, _, _} = poll(s, source(["a", "c", "d"]))

      assert_receive {:telemetry, [:flezha, :plugin, :websocket, :reconcile],
                      %{added: 2, removed: 1}, meta},
                     500

      assert meta.plugin_id == "test_ws"
    end

    test "an unchanged source emits no reconcile telemetry" do
      attach_telemetry([:flezha, :plugin, :websocket, :reconcile])
      s = state(applied: MapSet.new(["a"]))

      assert {:ok, _} = poll(s, source(["a"]))
      refute_received {:telemetry, [:flezha, :plugin, :websocket, :reconcile], _, _}
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

  describe "commit gating" do
    test "an acknowledged subscribe commits and the next cycle sends nothing" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      refute MapSet.member?(ns.applied, "b")

      assert {:ok, ns} = Websocket.Client.handle_in(ack(req_id(frame)), ns)
      assert ns.applied == MapSet.new(["a", "b"])
      assert ns.inflight == %{}

      assert {:ok, ns} = poll(ns, source(["a", "b"]))
      assert ns.last_reconcile == %{added: 0, removed: 0, sent: false}
    end

    test "an acknowledged unsubscribe removes the item from the applied set" do
      s = state(applied: MapSet.new(["a", "b"]))

      assert {:reply, [frame], ns} = poll(s, source(["a"]))
      assert {:ok, ns} = Websocket.Client.handle_in(ack(req_id(frame)), ns)
      assert ns.applied == MapSet.new(["a"])
    end

    test "a failed acknowledgement is not committed and the delta is re-derived" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      first_req = req_id(frame)

      assert {:ok, ns} =
               Websocket.Client.handle_in(
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
               Websocket.Client.handle_in(
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

      assert {:ok, unchanged} = Websocket.Client.handle_in(ack(first_req), ns)
      assert unchanged.applied == MapSet.new(["a"])
    end

    test "an acknowledgement naming an unknown req_id changes nothing" do
      s = state(applied: MapSet.new(["a"]))
      assert {:reply, [_frame], ns} = poll(s, source(["a", "b"]))

      assert {:ok, after_ack} = Websocket.Client.handle_in(ack(9_999), ns)
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
      assert {:ok, ns} = Websocket.Client.handle_in(ack(req_id(frame)), ns)
      assert ns.last_frame_at != 0
      assert ns.last_data_frame_at == 0
      # …and it still committed.
      assert ns.applied == MapSet.new(["a"])
    end

    test "a plugin with no subscription source never reaches the ack clause" do
      # No `:inflight` key at all: the ack clause head cannot match, which is
      # what makes the per-frame path free for a static plugin.
      s = state() |> Map.drop([:subscription, :applied, :inflight, :desired, :next_req_id])

      assert {:ok, ns} = Websocket.Client.handle_in({:text, ~s({"req_id":1,"success":true})}, s)
      refute Map.has_key?(ns, :inflight)
      assert ns.frames_seen == 1
    end
  end

  ## ---------- 8.3 durability ----------

  describe "durability" do
    test "a committed delta is persisted to the host for the cspec" do
      s = state(applied: MapSet.new(["a"]))

      assert {:reply, [frame], ns} = poll(s, source(["a", "b"]))
      assert {:ok, _ns} = Websocket.Client.handle_in(ack(req_id(frame)), ns)

      assert_receive {:"$gen_cast",
                      {:plugin_opt_persist, "ws_plugin", :applied_subscriptions, ["a", "b"]}}
    end

    test "init_provider restores a persisted applied set" do
      assert {:ok, opts} =
               prepare(
                 Websocket,
                 Map.put(@valid_opts, :applied_subscriptions, ["b", "a", "a"])
               )

      # Normalised: de-duplicated and ordered, so a restored set and a
      # freshly-committed one compare equal.
      assert opts.applied_subscriptions == ["a", "b"]
    end

    test "a restored applied set is what the socket asserts on connect" do
      s = state(applied: MapSet.new(["a", "b"]), websocket_established?: false)

      assert {:reply, [frame], ns} = Websocket.Client.handle_connect(101, [], s)
      assert method_of(frame) == "subscribe"
      assert items_of(frame) == ["a", "b"]
      assert ns.websocket_established? == true
    end

    test "the extension table carries a committed opt across a plugin restart" do
      plugin = %Flezha.Plugin{
        id: "ws_plugin",
        provider: Websocket,
        opts: %{uri: "wss://test.local"},
        hooks: []
      }

      table = %Table{plugins: %{"ws_plugin" => plugin}, subs: %{}}

      updated = Table.put_plugin_opt(table, "ws_plugin", :applied_subscriptions, ["a", "b"])
      assert updated.plugins["ws_plugin"].opts[:applied_subscriptions] == ["a", "b"]

      # `attempt_restart/3` re-runs `init_provider/1` from exactly this map.
      assert {:ok, opts} = prepare(Websocket, updated.plugins["ws_plugin"].opts)
      assert opts.applied_subscriptions == ["a", "b"]
    end

    test "put_plugin_opt updates the subscription entries too, so there is no split brain" do
      plugin = %Flezha.Plugin{
        id: "ws_plugin",
        provider: Websocket,
        opts: %{uri: "wss://test.local"},
        hooks: []
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
        hooks: []
      }

      cspec = %{
        id: "op-1",
        start:
          {Flezha.Operator.Overwatch, :start_link,
           [{"default", %{opts: [plugins: [{"ws_plugin", plugin}]]}}]}
      }

      rewritten =
        HubManager.rewrite_cspec_plugins(
          cspec,
          RegistryUpdate.put_plugin_opt("ws_plugin", :applied_subscriptions, ["a", "b"])
        )

      {_m, _f, [{_ns, operator}]} = rewritten.start
      [{"ws_plugin", restored}] = operator.opts[:plugins]
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
end
