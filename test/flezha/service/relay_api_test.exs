defmodule Flezha.Service.RelayApiTest do
  @moduledoc """
  `relay-api` on the controller: the factory table keeps each runner's
  announced API, `Relay.api` lists the latest, `Relay.call_channel` encodes a
  value by it and calls or sends, and `flz relay info <id>` and
  `flz relay call --arg` read it. The callee's Overwatch is a stand-in, as in
  `Flezha.Service.RelayCallTest`.
  """
  # async: false: Mimic global mode, and the live FleetManager.
  use FlezhaTest.Case, async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Error
  alias Flezha.Identificator
  alias Flezha.Partition
  alias Flezha.Relay.{Builder, Overwatch, PartitionGroup}
  alias Flezha.Runner.FleetManager
  alias Flezha.Service.{Cli, HubManager, Relay, RelayApi, RelayManager}
  alias FlezhaTest.Helper.{CliCapture, FleetStateHelper}
  alias ProcessHub.Service.ProcessRegistry

  setup :set_mimic_global_until_exit

  @context %{source: :ui, principal: "console@_system"}

  @place_schema %{
    "type" => "object",
    "properties" => %{"pair" => %{"type" => "string"}, "amount" => %{"type" => "number"}},
    "required" => ["pair", "amount"]
  }

  defp member(group, index) do
    %{
      id: Partition.member_id(group, index),
      partition: %{range: Enum.at(Partition.ranges(2), index)}
    }
  end

  # An entry as the factory table keeps it.
  defp entry(channel, codec, opts \\ []) do
    %{
      channel: channel,
      codec: codec,
      schema: opts[:schema],
      description: opts[:description],
      answer_codec: opts[:answer],
      answer_schema: nil
    }
  end

  defp orders do
    [
      entry("place", "json",
        schema: @place_schema,
        description: "Places an order",
        answer: "json"
      ),
      entry("audit", "text"),
      entry("blob", "bytes", answer: "bytes")
    ]
  end

  # The same entry in the JSON shape a connect frame carries it in.
  defp announced(%{} = entry) do
    %{
      "channel" => entry.channel,
      "codec" => entry.codec,
      "schema_json" => if(entry.schema, do: Jason.encode!(entry.schema), else: ""),
      "description" => entry.description || "",
      "answer_codec" => entry.answer_codec || "",
      "answer_schema_json" => ""
    }
  end

  describe "the factory table" do
    setup do
      original = :sys.get_state(FleetManager)
      namespace = "relay_api_#{System.unique_integer([:positive])}"
      FleetStateHelper.seed(namespace, %{"r1" => [], "r2" => []})

      on_exit(fn ->
        FleetStateHelper.forget_established(namespace, ["orders"])
        :sys.replace_state(FleetManager, fn _ -> original end)
      end)

      %{namespace: namespace}
    end

    defp orders_factory(api) do
      %{"id" => "orders", "description" => "Demo orders", "sdk_lang" => 1, "run" => "flow"}
      |> then(&if api, do: Map.put(&1, "api", Enum.map(api, fn e -> announced(e) end)), else: &1)
    end

    test "the API is known as soon as a runner announces it, before any instance runs",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [orders_factory(orders())])

      assert [%{factory: "orders", description: "Demo orders", disagree: false} = factory] =
               RelayApi.list(ns)

      assert [place, audit, blob] = factory.channels
      assert %{channel: "place", schema: @place_schema, description: "Places an order"} = place
      assert %{channel: "audit", answer_codec: nil, description: nil} = audit
      assert %{channel: "blob", codec: "bytes", answer_codec: "bytes"} = blob
    end

    test "the latest announcement wins and a disagreement is recorded; both runners still place",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [orders_factory(orders())])
      FleetStateHelper.announce(ns, "r2", [orders_factory([hd(orders())])])

      assert [%{channels: [%{channel: "place"}], disagree: true}] = RelayApi.list(ns)
      assert Enum.sort(FleetManager.runners_for_factory(ns, "orders")) == ["r1", "r2"]

      FleetStateHelper.announce(ns, "r2", [orders_factory(orders())])
      assert [%{channels: [_, _, _], disagree: false}] = RelayApi.list(ns)
    end

    test "a runner that announces no API is placed, and its factory lists no channels",
         %{namespace: ns} do
      FleetStateHelper.announce(ns, "r1", [orders_factory(nil)])

      assert [%{factory: "orders", channels: []}] = RelayApi.list(ns)
      assert FleetManager.runners_for_factory(ns, "orders") == ["r1"]
    end
  end

  # A rolling deploy: runner `old` announces channel `a`, runner `new` channel
  # `b`. Each relay's registry row names the runner hosting it; `idle` has none,
  # and `stranded` names `old` but runs only on a node that is gone.
  describe "a relay's API is its hosting runner's" do
    @hosts %{
      "on-old" => "old",
      "on-new" => "new",
      "idle" => nil,
      "stranded" => "old",
      "prices#p0" => "old",
      "prices#p1" => "new"
    }

    setup do
      original = :sys.get_state(FleetManager)
      namespace = "relay_api_host_#{System.unique_integer([:positive])}"
      hub_id = Identificator.hub(namespace)
      FleetStateHelper.seed(namespace, %{"old" => [], "new" => []})

      on_exit(fn ->
        FleetStateHelper.forget_established(namespace, ["orders"])
        :sys.replace_state(FleetManager, fn _ -> original end)
      end)

      stub(ProcessHub, :is_alive?, fn
        ^hub_id -> true
        other -> Mimic.call_original(ProcessHub, :is_alive?, [other])
      end)

      stub(ProcessRegistry, :lookup, fn
        ^hub_id, id, [with_metadata: true] when is_map_key(@hosts, id) ->
          relay = %Builder{id: id, factory_id: "orders"}
          cspec = %{id: id, start: {Overwatch, :start_link, [{namespace, relay}]}}
          node = if id == "stranded", do: :"gone@127.0.0.1", else: node()
          {cspec, [{node, self()}], %{tag: @hosts[id]}}

        hub, id, opts ->
          Mimic.call_original(ProcessRegistry, :lookup, [hub, id, opts])
      end)

      stub(PartitionGroup, :members, fn
        _namespace, "prices" -> for i <- 0..1, do: member("prices", i)
        _namespace, _id -> []
      end)

      %{namespace: namespace}
    end

    defp announce(namespace, runner, channel) do
      graph = %{"channels" => [], "tasklets" => [%{"name" => "on-#{runner}"}]}
      factory = Map.put(orders_factory([entry(channel, "text")]), "graph", graph)
      FleetStateHelper.announce(namespace, runner, [factory])
    end

    defp channels(namespace, relay_id) do
      {:ok, %{channels: channels}} = RelayApi.factory_of(namespace, relay_id)
      Enum.map(channels, & &1.channel)
    end

    defp tasklets(namespace, relay_id) do
      {:ok, %{graph: %{tasklets: tasklets}}} = RelayApi.graph_of(namespace, relay_id)
      Enum.map(tasklets, & &1.name)
    end

    test "a relay on the old runner keeps the channel its runner announced", %{namespace: ns} do
      announce(ns, "old", "a")
      announce(ns, "new", "b")

      assert RelayApi.refuse_unknown_entry(ns, "on-old", "a") == :ok
      assert channels(ns, "on-old") == ["a"]
      assert tasklets(ns, "on-old") == ["on-old"]
      assert channels(ns, "on-new") == ["b"]

      assert {:error, %Error{code: "api.channel_unknown"}} =
               RelayApi.refuse_unknown_entry(ns, "on-old", "b")

      # The namespace listing and a relay with no runner or no live node read
      # the latest; a partition group its first member's runner.
      assert [%{channels: [%{channel: "b"}], disagree: true}] = RelayApi.list(ns)
      assert channels(ns, "idle") == ["b"]
      assert channels(ns, "stranded") == ["b"]

      # A host node that cannot be asked (not connected, as an older node
      # without the read answers `:undef`) falls back the same way.
      stub(HubManager, :relay_spec, fn
        _namespace, "ghost" ->
          {:ok, %Builder{id: "ghost", factory_id: "orders"},
           %{runner: "old", node: :"ghost@127.0.0.1"}}

        namespace, id ->
          Mimic.call_original(HubManager, :relay_spec, [namespace, id])
      end)

      assert channels(ns, "ghost") == ["b"]
      assert channels(ns, "prices") == ["a"]
    end

    test "an old runner reconnecting does not take the API from a relay on the new runner",
         %{namespace: ns} do
      announce(ns, "new", "b")
      announce(ns, "old", "a")

      assert channels(ns, "on-new") == ["b"]
      assert tasklets(ns, "on-new") == ["on-new"]
      assert RelayApi.refuse_unknown_entry(ns, "on-new", "b") == :ok
      assert channels(ns, "idle") == ["a"]
    end
  end

  describe "Relay.call_channel and Relay.api" do
    setup do
      test_pid = self()
      api = orders()

      stub(HubManager, :hub_alive?, fn _namespace -> true end)

      # `orders-eu` is an `orders` relay; `prices` a group of two of them.
      stub(HubManager, :relay_spec, fn
        _namespace, id when id in ["orders-eu", "prices#p0", "prices#p1"] ->
          {:ok, %{factory_id: Process.get(:factory, "orders"), opts: []},
           %{runner: nil, node: nil}}

        namespace, id ->
          {:error, {:relay_not_found, namespace, id}}
      end)

      stub(PartitionGroup, :members, fn
        _namespace, "prices" -> for i <- 0..1, do: member("prices", i)
        _namespace, _id -> []
      end)

      stub(RelayManager, :factory_row, fn _namespace, factory, _host ->
        %{
          id: factory,
          description: "Demo orders",
          run: :flow,
          api: Process.get(:api, api),
          api_at: 1,
          api_disagree: false
        }
      end)

      # Stands in for the relay's Overwatch: answers a call with the answer
      # the test put in its process dictionary.
      stub(HubManager, :get_pid, fn
        _namespace, id when id in ["orders-eu", "prices#p0", "prices#p1"] ->
          answer = Process.get(:answer, ~s({"status":"placed"}))

          {:ok,
           spawn_link(fn ->
             receive do
               {:"$gen_call", from, {:call, message, metadata, deadline_ms}} ->
                 send(test_pid, {:called, message, metadata, deadline_ms})
                 GenServer.reply(from, {:ok, answer, %{"channel" => "placed"}})
             end
           end)}

        _namespace, _id ->
          {:error, "no pid found"}
      end)

      stub(RelayManager, :send_io_confirming, fn id, _namespace, message, metadata ->
        send(test_pid, {:sent, id, message, metadata})
        :ok
      end)

      :ok
    end

    defp call(channel, value, opts \\ []),
      do: Relay.call_channel("default", "orders-eu", channel, value, opts, @context)

    test "a JSON value is the input itself, and the answer is decoded" do
      order = %{"pair" => "EUR/USD", "amount" => 1000}

      assert {:ok, %{answer: %{"status" => "placed"}, answer_codec: "json"}} =
               call("place", order)

      assert_received {:called, message, %{"channel" => "place"}, 30_000}
      assert Jason.decode!(message) == order
    end

    test "a channel without an answer is sent to and answers accepted" do
      assert {:ok, %{accepted: true}} = call("audit", "checked")
      assert_received {:sent, "orders-eu", "checked", %{"channel" => "audit"}}
      refute_received {:called, _, _, _}
    end

    test "bytes travel as base64, both ways" do
      Process.put(:answer, <<255, 0>>)

      assert {:ok, %{answer: "/wA=", answer_codec: "bytes"}} =
               call("blob", Base.encode64(<<1, 2, 3>>))

      assert_received {:called, <<1, 2, 3>>, _metadata, _deadline}
    end

    test "an unknown channel is named with the channels the relay has" do
      assert {:error, %Error{code: "api.channel_unknown"} = error} = call("plce", %{})
      assert error.details["channels"] == "place, audit, blob"
      assert error.message =~ "plce"
    end

    test "a value that does not fit the format is refused naming the expected form" do
      assert {:error, %Error{code: "api.input_invalid", details: %{"expected" => "a string"}}} =
               call("audit", %{"no" => "text"})

      assert {:error, %Error{details: %{"expected" => "a base64 string"}}} = call("blob", "%%%")
      refute_received {:called, _, _, _}
      refute_received {:sent, _, _, _}
    end

    test "a factory with no announced API is refused, unless the payload is given as it is" do
      Process.put(:api, [])

      assert {:error, %Error{code: "api.not_advertised", class: :unavailable}} =
               call("place", %{})

      Process.put(:answer, "plain")

      assert {:ok, %{answer: "plain", answer_codec: "text"}} =
               call("", "hello", payload: "text", deadline_ms: 100)

      assert_received {:called, "hello", _metadata, 100}
    end

    test "Relay.api names one relay's factory" do
      assert {:ok, [%{relay: "orders-eu", factory: "orders", channels: [_, _, _]}]} =
               Relay.api("default", "orders-eu", @context)

      assert {:error, %Error{code: "relay.not_found"}} = Relay.api("default", "ghost", @context)
    end

    test "flz relay info <id> shows each channel's format, answer, schema and a call" do
      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.api", ~s(["default","orders-eu"]), []) end)

      assert out =~ "API of relay 'orders-eu' (factory 'orders'):"
      assert out =~ "place  json → answer json"
      assert out =~ "Places an order"
      assert out =~ ~s(schema: #{Jason.encode!(@place_schema)})
      assert out =~ "MCP tool: orders_place"
      assert out =~ "flz relay call orders-eu --channel place --arg pair=... --arg amount=..."
      assert out =~ "audit  text, no answer"
      assert out =~ "flz relay call orders-eu --channel audit --text <payload>"
    end

    test "a tool name a catalogue tool holds is not given, and info says so" do
      Process.put(:factory, "relay")
      Process.put(:api, [entry("list", "json", description: "Lists")])

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.api", ~s(["default","orders-eu"]), []) end)

      assert out =~ "MCP tool: tool name taken (relay_list)"
    end

    test "a partition group's API is its members' factory's, and --arg reaches the member its key owns" do
      assert {:ok, [%{relay: "prices", factory: "orders"}]} =
               Relay.api("default", "prices", @context)

      key =
        Enum.find(
          ~w(EURUSD GBPUSD USDJPY AUDUSD),
          &Partition.owns?(member("prices", 1).partition.range, &1)
        )

      args = ["default", "prices", "place", %{"pair" => "EUR/USD", "amount" => "1"}, %{key: key}]

      {_outcome, out, ""} =
        CliCapture.run(fn ->
          Cli.call("Relay.call_channel", Jason.encode!(args), coerce: true)
        end)

      assert out == ~s({"status":"placed"}\n)
      assert_received {:called, _message, _metadata, 30_000}
    end

    test "a schema's required fields are held at the call, on every surface" do
      assert {:error, %Error{code: "api.input_invalid", details: details}} =
               call("place", %{"pair" => "EUR/USD"})

      assert details["reason"] == "missing amount"

      assert {:error, %Error{code: "api.input_invalid"}} =
               call("place", Base.encode64(~s({"pair":"EUR/USD"})), payload: "base64")

      refute_received {:called, _, _, _}
    end

    test "a field the schema does not declare is refused only when it forbids others" do
      closed = Map.put(@place_schema, "additionalProperties", false)
      Process.put(:api, [entry("place", "json", schema: closed, answer: "json")])
      order = %{"pair" => "EUR/USD", "amount" => 1, "a.b" => "1"}

      assert {:error, %Error{code: "api.input_invalid", details: %{"reason" => "undeclared a.b"}}} =
               call("place", order)

      Process.put(:api, orders())
      assert {:ok, _answer} = call("place", order)
      assert_received {:called, message, _metadata, _deadline}
      assert Jason.decode!(message)["a.b"] == "1", "a dotted name is a field, never nested"
    end

    test "a payload given as base64 to an unannounced factory is refused, never sent or called" do
      Process.put(:api, [])

      assert {:error, %Error{code: "api.not_advertised"}} =
               call("audit", Base.encode64("checked"), payload: "base64")

      refute_received {:sent, _, _, _}
      refute_received {:called, _, _, _}
    end

    test "flz relay call --arg types each field by the channel's schema" do
      args = ["default", "orders-eu", "place", %{"pair" => "EUR/USD", "amount" => "1000"}, %{}]

      {_outcome, out, ""} =
        CliCapture.run(fn ->
          Cli.call("Relay.call_channel", Jason.encode!(args), coerce: true)
        end)

      assert out == ~s({"status":"placed"}\n)
      assert_received {:called, message, _metadata, 30_000}
      assert %{"pair" => "EUR/USD", "amount" => 1000} = Jason.decode!(message)
    end

    test "flz relay call --arg refuses a value that does not coerce, naming the field" do
      args = ["default", "orders-eu", "place", %{"amount" => "lots"}, %{}]

      {{:exit, {:shutdown, 2}}, "", err} =
        CliCapture.run(fn ->
          Cli.call("Relay.call_channel", Jason.encode!(args), coerce: true)
        end)

      assert err =~ "(api.input_invalid)"
      assert err =~ "amount"
      refute_received {:called, _, _, _}
    end

    test "flz relay call --arg refuses a channel it cannot type, naming --text" do
      args = ["default", "orders-eu", "audit", %{"note" => "x"}, %{}]

      {{:exit, {:shutdown, 2}}, "", err} =
        CliCapture.run(fn ->
          Cli.call("Relay.call_channel", Jason.encode!(args), coerce: true)
        end)

      assert err =~ "(api.input_invalid)"
      assert err =~ "--text"
    end

    test "flz relay call --text sends the payload as written and prints accepted for a send" do
      args = ["default", "orders-eu", "audit", "checked", %{"payload" => "text"}]

      {_outcome, out, ""} =
        CliCapture.run(fn -> Cli.call("Relay.call_channel", Jason.encode!(args), []) end)

      assert out == "accepted\n"
      assert_received {:sent, "orders-eu", "checked", _metadata}
    end
  end

  describe "decode/2" do
    test "reads an answer by its format, and bytes that are not text as base64" do
      assert RelayApi.decode("json", ~s({"a":1})) == %{answer: %{"a" => 1}, answer_codec: "json"}
      assert RelayApi.decode("json", "plain") == %{answer: "plain", answer_codec: "text"}
      assert RelayApi.decode("text", "héllo") == %{answer: "héllo", answer_codec: "text"}
      assert RelayApi.decode("text", <<255>>) == %{answer: "/w==", answer_codec: "bytes"}
      assert RelayApi.decode("msgpack", <<1>>) == %{answer: "AQ==", answer_codec: "msgpack"}
      assert RelayApi.decode("custom", <<1>>) == %{answer: "AQ==", answer_codec: "custom"}
    end
  end
end
