defmodule Flezha.Service.RelayGroupSurfacesTest do
  @moduledoc """
  A partition group called through every surface that reaches
  `Relay.call_channel`: the published verb, the door's generic call and the
  MCP relay tool each carry a key and reach the member owning it; a group
  called with no key is refused on each.
  """
  # async: false: Mimic global mode.
  use FlezhaTest.Case, async: false
  use Mimic

  import FlezhaTest.Helper.MimicGlobal

  alias Flezha.Mcp
  alias Flezha.Partition
  alias Flezha.Relay.PartitionGroup
  alias Flezha.Service.{Admin, HubManager, Relay, RelayManager}

  setup :set_mimic_global_until_exit

  @context %{source: :ui, principal: "console@_system"}
  @entry %{
    channel: "in",
    codec: "text",
    schema: nil,
    description: "Quotes a pair",
    answer_codec: "text",
    answer_schema: nil
  }

  setup do
    test_pid = self()
    members = for index <- 0..1, do: member("prices", index)

    callee = fn id ->
      spawn_link(fn ->
        receive do
          {:"$gen_call", from, {:call, message, _metadata, _deadline_ms}} ->
            send(test_pid, {:called, id, message})
            GenServer.reply(from, {:ok, "#{id}:#{message}", %{"channel" => "out"}})
        end
      end)
    end

    running = Enum.map(members, & &1.id)
    stub(HubManager, :hub_alive?, fn _namespace -> true end)

    stub(HubManager, :get_pid, fn _namespace, id ->
      if id in running, do: {:ok, callee.(id)}, else: {:error, "no pid found"}
    end)

    stub(HubManager, :relay_spec, fn namespace, id ->
      if id in running,
        do: {:ok, %{id: id, factory_id: "prices"}, %{runner: nil, node: nil}},
        else: {:error, {:relay_not_found, namespace, id}}
    end)

    stub(RelayManager, :factory_row, fn _namespace, "prices", _host ->
      %{id: "prices", description: "Prices", api: [@entry], api_at: 1, api_disagree: false}
    end)

    stub(PartitionGroup, :members, fn
      _namespace, "prices" -> members
      _namespace, _id -> []
    end)

    key = Enum.find(~w(EURUSD GBPUSD USDJPY AUDUSD USDCAD NZDUSD), &owns?(members, 1, &1))
    %{key: key}
  end

  defp member(group, index),
    do: %{
      id: Partition.member_id(group, index),
      partition: %{range: Enum.at(Partition.ranges(2), index)}
    }

  defp owns?(members, index, key),
    do: Partition.owns?(Enum.at(members, index).partition.range, key)

  defp door do
    api = Admin.json([%{factory: "prices", description: "Prices", channels: [@entry]}])

    fn
      "Relay.api", _args -> {:ok, api}
      name, args -> Admin.call(name, args, @context, :given)
    end
  end

  defp mcp(method, params) do
    catalogue = fn ->
      Enum.filter(Flezha.Service.catalogue(), &(&1.name == "Relay.call_channel"))
    end

    body = Jason.encode!(%{jsonrpc: "2.0", id: 1, method: method, params: params})
    Mcp.answer(body, nil, catalogue, door())
  end

  defp tool_call(arguments) do
    {200, %{result: result}, "Relay.call_channel"} =
      mcp("tools/call", %{"name" => "prices_in", "arguments" => arguments})

    result
  end

  test "the verb reaches the member owning the key", %{key: key} do
    assert {:ok, %{answer: "prices#p1:x"}} =
             Relay.call_channel("default", "prices", "in", "x", [key: key], @context)
  end

  test "the door's generic call carries the key (POST /v1/call/Relay.call_channel)",
       %{key: key} do
    assert {:ok, _json} =
             Admin.call(
               "Relay.call_channel",
               ["default", "prices", "in", "x", %{"key" => key}],
               @context,
               :given
             )

    assert_received {:called, "prices#p1", "x"}
  end

  # The arguments `FlezhaHttp.Api.call_relay/6` builds for
  # `POST /v1/relays/prices/in` with body "x" and no `?key=`.
  test "the HTTP relay route's arguments without a key are refused for a group" do
    args = ["default", "prices", "in", Base.encode64("x"), %{"payload" => "base64"}]

    assert {:error, %Flezha.Error{code: "call.target_unkeyed"}} =
             Admin.call("Relay.call_channel", args, @context, :given)
  end

  test "the MCP relay tool takes a key and reaches the member owning it", %{key: key} do
    {200, %{result: %{tools: tools}}, nil} = mcp("tools/list", %{})
    schema = Enum.find(tools, &(&1.name == "prices_in")).inputSchema

    assert %{"type" => "string"} = schema["properties"]["key"]
    refute "key" in schema["required"]

    result = tool_call(%{"relay" => "prices", "text" => "x", "key" => key})
    assert %{isError: false, content: [%{text: "prices#p1:x"}]} = result
    assert_received {:called, "prices#p1", "x"}
  end

  test "the MCP relay tool without a key is refused for a group" do
    result = tool_call(%{"relay" => "prices", "text" => "x"})

    assert result.isError
    assert result.structuredContent["error"]["code"] == "call.target_unkeyed"
    refute_received {:called, _member, _message}
  end
end
