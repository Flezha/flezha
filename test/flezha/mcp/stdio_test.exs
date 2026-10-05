defmodule Flezha.Mcp.StdioTest do
  @moduledoc """
  The two answers behind `flz mcp`: on the controller as the caller's token,
  and with no controller. Each prints one JSON-RPC line, or nothing for a
  notification.
  """
  # async: false: the auth stores and the posture are global.
  use FlezhaTest.Case,
    fixtures: [{:auth_stores, prefix: "flz_mcp_stdio", claim: true, register_source: true}],
    async: false

  import ExUnit.CaptureIO

  alias Flezha.AuthZ.TokenStore
  alias Flezha.Mcp.Stdio
  alias FlezhaTest.Helper.AuthTestHelper

  defp rpc(method, params \\ %{}),
    do: Jason.encode!(%{jsonrpc: "2.0", id: 1, method: method, params: params})

  defp offline(message),
    do: capture_io(fn -> Stdio.offline(message, "/run/flezha/control.sock") end)

  defp online(message, opts), do: capture_io(fn -> Stdio.answer(message, opts) end)

  defp result(line), do: Jason.decode!(line)["result"]

  defp tools(line), do: for(tool <- result(line)["tools"], do: tool["name"])

  describe "with no controller" do
    test "it initializes with the rules as instructions" do
      answer = result(offline(rpc("initialize")))

      assert answer["serverInfo"]["name"] == "flezha"
      assert answer["instructions"] == Flezha.Mcp.Knowledge.rules()
    end

    test "it lists the knowledge tools and every operation an assistant may reach" do
      listed = tools(offline(rpc("tools/list")))

      assert "flezha_sdk_example" in listed
      assert "relay_list" in listed
      assert "relay_start" in listed
      refute "controller_stop" in listed
      refute "relay_listen" in listed
      refute "namespace_issue_token" in listed
    end

    test "a knowledge tool answers and an operation is refused as unreachable" do
      call = %{"name" => "flezha_sdk_example", "arguments" => %{"language" => "rust"}}

      assert %{"isError" => false, "content" => [%{"text" => text}]} =
               result(offline(rpc("tools/call", call)))

      assert text =~ "examples/echo/src/main.rs"

      answer = result(offline(rpc("tools/call", %{"name" => "relay_list"})))
      error = answer["structuredContent"]["error"]

      assert answer["isError"]
      assert error["code"] == "controller.unreachable"
      assert error["message"] =~ "/run/flezha/control.sock"
      assert error["hint"] == "flz controller start"
    end

    test "a request of the pinned revision is answered with no headers to repeat it" do
      meta = %{
        "io.modelcontextprotocol/protocolVersion" => "2026-07-28",
        "io.modelcontextprotocol/clientCapabilities" => %{}
      }

      answer = result(offline(rpc("server/discover", %{"_meta" => meta})))
      assert "2026-07-28" in answer["supportedVersions"]
      assert answer["instructions"] == Flezha.Mcp.Knowledge.rules()
    end

    test "a knowledge tool refuses an argument it does not take, as an operation does" do
      call = %{"name" => "flezha_guide", "arguments" => %{"page" => "x"}}
      answer = result(offline(rpc("tools/call", call)))

      assert answer["isError"]
      assert answer["structuredContent"]["error"]["code"] == "control.unknown_parameter"
    end

    test "a line that is not JSON is answered under a null id" do
      assert %{"id" => nil, "error" => %{"code" => -32_700}} = Jason.decode!(offline("{not json"))
    end

    test "a notification prints nothing and every reply is one line" do
      assert offline(~s({"jsonrpc":"2.0","method":"notifications/initialized"})) == ""
      assert [_one] = String.split(offline(rpc("ping")), "\n", trim: true)
    end
  end

  describe "on the controller" do
    test "a viewer's list follows its role, and a start it may not make is refused and audited" do
      {:ok, token, _} = TokenStore.issue("assistant", "default", :viewer, 3600)
      listed = tools(online(rpc("tools/list"), token: token))

      assert "relay_list" in listed
      assert "flezha_guide" in listed
      refute "relay_start" in listed

      boundary = System.monotonic_time(:microsecond)
      relay = %{"id" => "o1", "factory_id" => "echo"}

      start = %{
        "name" => "relay_start",
        "arguments" => %{"namespace" => "default", "relay" => relay}
      }

      answer = result(online(rpc("tools/call", start), token: token))

      assert answer["isError"]
      assert answer["structuredContent"]["error"]["code"] =~ ~r/^authz\./

      assert [signal | _] =
               AuthTestHelper.await_platform_authz_events(:authorization_denied, boundary)

      assert signal.metadata.principal == "assistant"
    end

    test "a token that does not verify refuses every operation with its own error" do
      listed = tools(online(rpc("tools/list"), token: "flzt_not_a_token"))
      call = %{"name" => "relay_list", "arguments" => %{"namespace" => "default"}}
      answer = result(online(rpc("tools/call", call), token: "flzt_not_a_token"))

      assert listed == ~w(flezha_guide flezha_sdk_example flezha_error flezha_docs)
      assert answer["isError"]
      assert answer["structuredContent"]["error"]["code"] == "authz.token_unknown"
    end

    test "an assistant reaches no operation kept off the network, whatever its token" do
      {:ok, token, _} = TokenStore.issue("root", "_system", :admin, 3600)
      listed = tools(online(rpc("tools/list"), token: token))
      stop = %{"name" => "controller_stop", "arguments" => %{}}

      refute "controller_stop" in listed
      refute "namespace_issue_token" in listed

      assert Jason.decode!(online(rpc("tools/call", stop), token: token))["error"]["code"] ==
               -32_602
    end
  end
end
