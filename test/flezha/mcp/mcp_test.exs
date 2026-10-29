defmodule Flezha.McpTest do
  use ExUnit.Case, async: true

  alias Flezha.Mcp

  @types ~w(string integer boolean object array)
  @keywords ~w(type description enum minimum maximum)

  test "every catalogue entry has its own tool name, one a model API accepts" do
    names = Enum.map(Flezha.Service.catalogue(), &Mcp.tool_name(&1.name))

    assert length(Enum.uniq(names)) == length(names)
    assert Enum.all?(names, &Regex.match?(~r/\A[a-zA-Z0-9_-]{1,64}\z/, &1))
    assert Mcp.tool_name("PluginManager.enable") == "plugin_manager_enable"
    assert Mcp.tool_name("Dlq.list") == "dlq_list"
  end

  # The keywords a schema uses are checked against JSON Schema 2020-12's own
  # rules for them: an object schema, typed properties, required names that are
  # properties, bounds that are integers.
  test "every tool's input schema is a valid JSON Schema object" do
    for entry <- Flezha.Service.catalogue() do
      schema =
        entry |> Mcp.tool() |> Map.fetch!(:inputSchema) |> Jason.encode!() |> Jason.decode!()

      assert schema["type"] == "object"
      assert schema["additionalProperties"] == false
      assert is_map(schema["properties"])
      assert Map.get(schema, "required", ["x"]) != []
      assert Enum.all?(Map.get(schema, "required", []), &Map.has_key?(schema["properties"], &1))

      for {name, property} <- schema["properties"] do
        assert Map.keys(property) -- @keywords == [], "#{entry.name}.#{name}"
        assert Map.get(property, "type", "string") in @types, "#{entry.name}.#{name}"
        assert is_binary(property["description"])

        for bound <- ~w(minimum maximum),
            Map.has_key?(property, bound),
            do: assert(is_integer(property[bound]))

        if enum = property["enum"], do: assert(enum != [] and Enum.all?(enum, &is_binary/1))
      end
    end
  end

  test "readOnlyHint holds only for an operation whose row writes no audit" do
    tools = Map.new(Flezha.Service.catalogue(), &{&1.name, Mcp.tool(&1)})

    assert tools["Dlq.list"].annotations.readOnlyHint
    refute tools["Relay.start"].annotations.readOnlyHint
    assert tools["Logs.read"].annotations.readOnlyHint
    assert tools["Dlq.drop"].annotations.destructiveHint
    refute tools["Dlq.replay"].annotations.destructiveHint
    assert tools["Relay.start"].description =~ "`relay.start`"
  end

  test "a relay's diagnosis is a read-only tool naming the relay" do
    entry = Enum.find(Flezha.Service.catalogue(), &(&1.name == "Relay.diagnose"))
    tool = Mcp.tool(entry)

    assert tool.name == "relay_diagnose"
    assert tool.annotations.readOnlyHint
    assert tool.inputSchema.required == ["namespace", "relay_id"]
  end
end

defmodule Flezha.McpRelayToolsTest do
  @moduledoc """
  Relay tools: every described entry channel of the caller's namespace is a
  tool beside the catalogue and knowledge tools, for a role that may call
  relays, and a call of one is `Relay.call_channel` through the caller's door.
  """
  use ExUnit.Case, async: true

  alias Flezha.Mcp

  @place %{
    "channel" => "place",
    "codec" => "json",
    "schema" => %{"type" => "object", "properties" => %{"pair" => %{"type" => "string"}}},
    "description" => "Places an order",
    "answer_codec" => "json",
    "answer_schema" => %{"type" => "object", "properties" => %{"status" => %{"type" => "string"}}}
  }

  defp channel(name, codec, extra \\ %{}) do
    Map.merge(
      %{
        "channel" => name,
        "codec" => codec,
        "schema" => nil,
        "description" => "#{name} it",
        "answer_codec" => nil,
        "answer_schema" => nil
      },
      extra
    )
  end

  defp channels do
    [
      @place,
      channel("audit", "text", %{"description" => nil}),
      channel("note", "text"),
      channel("blob", "bytes", %{"answer_codec" => "bytes"}),
      channel("raw", "json"),
      channel("keyed", "json", %{"schema" => owning("key")}),
      channel("routed", "json", %{"schema" => owning("relay")}),
      channel("Place", "json", %{"description" => "Shadows place"})
    ]
  end

  defp owning(name),
    do: %{
      "type" => "object",
      "properties" => %{name => %{"type" => "integer"}},
      "required" => [name]
    }

  # A door that answers `Relay.api` with the `orders` factory, reports every
  # call to the test and answers `Relay.call_channel` with `answer`.
  defp door(answer \\ ~s({"accepted":true})) do
    test = self()

    api =
      Jason.encode!([
        %{"factory" => "orders", "description" => "Demo orders", "channels" => channels()}
      ])

    fn
      "Relay.api", args ->
        send(test, {:door, "Relay.api", args})
        {:ok, api}

      name, args ->
        send(test, {:door, name, args})
        {:ok, answer}
    end
  end

  defp catalogue(names),
    do: fn -> Enum.filter(Flezha.Service.catalogue(), &(&1.name in names)) end

  defp rpc(method, params \\ %{}),
    do: Jason.encode!(%{jsonrpc: "2.0", id: 1, method: method, params: params})

  defp tools(catalogue) do
    {200, %{result: %{tools: tools}}, nil} =
      Mcp.answer(rpc("tools/list"), nil, catalogue, door(), "desk")

    Map.new(tools, &{&1.name, &1})
  end

  defp call(name, arguments, answer) do
    {200, %{result: result}, "Relay.call_channel"} =
      Mcp.answer(
        rpc("tools/call", %{"name" => name, "arguments" => arguments}),
        nil,
        catalogue([]),
        door(answer),
        "desk"
      )

    result
  end

  test "a role that may call relays sees one tool per described channel of its namespace" do
    tools = tools(catalogue(["Relay.list", "Relay.call_channel"]))

    assert Map.has_key?(tools, "relay_list")
    assert Map.has_key?(tools, "flezha_guide")
    assert_received {:door, "Relay.api", ["desk"]}

    assert %{description: "Demo orders: Places an order", inputSchema: schema, outputSchema: out} =
             tools["orders_place"]

    assert schema["properties"]["relay"]["type"] == "string"
    assert schema["properties"]["pair"] == %{"type" => "string"}
    assert schema["required"] == ["relay"]
    assert out == @place["answer_schema"]

    refute Map.has_key?(tools, "orders_audit"), "an undescribed channel is no tool"
    assert tools["orders_note"].inputSchema["required"] == ["relay", "text"]
    assert tools["orders_blob"].inputSchema["properties"]["base64"]["contentEncoding"] == "base64"
    assert tools["orders_raw"].inputSchema["required"] == ["relay", "input"]

    refute Enum.any?(Map.values(tools), &(&1[:description] == "Demo orders: Shadows place")),
           "a second channel that names the same tool is not listed"
  end

  test "a role that may not call relays sees no relay tool and asks for none" do
    tools = tools(catalogue(["Relay.list"]))

    refute Map.has_key?(tools, "orders_place")
    refute_received {:door, "Relay.api", _}
  end

  test "a call is Relay.call_channel, its JSON answer structured content" do
    result =
      call(
        "orders_place",
        %{"relay" => "orders-eu", "pair" => "EUR/USD"},
        ~s({"answer":{"status":"placed"},"answer_codec":"json"})
      )

    assert_received {:door, "Relay.call_channel",
                     ["desk", "orders-eu", "place", %{"pair" => "EUR/USD"}, %{}]}

    assert %{structuredContent: %{"status" => "placed"}, isError: false} = result
    assert [%{type: "text", text: ~s({"status":"placed"})}] = result.content
  end

  test "text, bytes and a send each answer in their own form" do
    text =
      call(
        "orders_note",
        %{"relay" => "r", "text" => "hi"},
        ~s({"answer":"ok","answer_codec":"text"})
      )

    assert_received {:door, "Relay.call_channel", ["desk", "r", "note", "hi", %{}]}
    assert [%{type: "text", text: "ok"}] = text.content

    blob =
      call(
        "orders_blob",
        %{"relay" => "r", "base64" => "AQID"},
        ~s({"answer":"AQ==","answer_codec":"bytes"})
      )

    assert [%{type: "resource", resource: %{blob: "AQ==", mimeType: "application/octet-stream"}}] =
             blob.content

    sent = call("orders_note", %{"relay" => "r", "text" => "hi"}, ~s({"accepted":true}))
    assert [%{type: "text", text: "accepted"}] = sent.content
  end

  test "every relay tool takes an optional key, passed as the call's key" do
    tools = tools(catalogue(["Relay.call_channel"]))

    for name <- ~w(orders_place orders_note orders_blob orders_raw) do
      assert tools[name].inputSchema["properties"]["key"]["type"] == "string"
      refute "key" in tools[name].inputSchema["required"]
    end

    call(
      "orders_place",
      %{"relay" => "g", "key" => "EURUSD", "pair" => "EUR/USD"},
      ~s({"accepted":true})
    )

    assert_received {:door, "Relay.call_channel",
                     ["desk", "g", "place", %{"pair" => "EUR/USD"}, %{"key" => "EURUSD"}]}

    call(
      "orders_note",
      %{"relay" => "g", "key" => "EURUSD", "text" => "hi"},
      ~s({"accepted":true})
    )

    assert_received {:door, "Relay.call_channel",
                     ["desk", "g", "note", "hi", %{"key" => "EURUSD"}]}
  end

  test "an object input with its own relay or key property rides under input" do
    tools = tools(catalogue(["Relay.call_channel"]))

    for {name, own} <- [{"orders_keyed", "key"}, {"orders_routed", "relay"}] do
      schema = tools[name].inputSchema
      assert schema["required"] == ["relay", "input"]
      assert schema["properties"]["input"] == owning(own)
      assert schema["properties"]["key"]["type"] == "string"
    end

    call(
      "orders_keyed",
      %{"relay" => "g", "key" => "EURUSD", "input" => %{"key" => 7}},
      ~s({"accepted":true})
    )

    assert_received {:door, "Relay.call_channel",
                     ["desk", "g", "keyed", %{"key" => 7}, %{"key" => "EURUSD"}]}
  end

  test "a refused call is a tool error carrying the error object" do
    {200, %{result: result}, "Relay.call_channel"} =
      Mcp.answer(
        rpc("tools/call", %{"name" => "orders_place", "arguments" => %{"relay" => "ghost"}}),
        nil,
        catalogue([]),
        fn
          "Relay.api", _args ->
            door().("Relay.api", [])

          _name, _args ->
            {:error, Flezha.Error.new("relay.not_found", namespace: "desk", relay: "ghost")}
        end,
        "desk"
      )

    assert result.isError
    assert result.structuredContent["error"]["code"] == "relay.not_found"
  end

  test "a tool name is both parts in snake case, never a lost letter, and at most 64 bytes" do
    names =
      Mcp.relay_tool_names([
        {"orders", "placeOrder"},
        {"orders", "Place"},
        {"orders", "lace"},
        {"orders", "caf\u00e9"},
        {"relay", "list"},
        {"OrderDesk", String.duplicate("x", 80)},
        {"orders", "place_order"}
      ])

    assert names[{"orders", "placeOrder"}] == {:ok, "orders_place_order"}
    assert names[{"orders", "Place"}] == {:ok, "orders_place"}
    assert names[{"orders", "lace"}] == {:ok, "orders_lace"}
    assert names[{"orders", "caf\u00e9"}] == {:ok, "orders_caf_"}
    assert names[{"relay", "list"}] == {:taken, "relay_list"}
    assert names[{"orders", "place_order"}] == {:taken, "orders_place_order"}

    {:ok, long} = names[{"OrderDesk", String.duplicate("x", 80)}]
    assert byte_size(long) == 64
    assert String.starts_with?(long, "order_desk_xxx")

    assert Mcp.relay_tool_names([{"OrderDesk", String.duplicate("x", 80)}]) ==
             %{{"OrderDesk", String.duplicate("x", 80)} => {:ok, long}},
           "the same pair always gets the same name"
  end
end
