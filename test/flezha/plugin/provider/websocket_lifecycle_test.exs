defmodule FlezhaTest.Plugin.Provider.WebsocketLifecycleTest do
  @moduledoc """
  Asserts `Plugin.Provider.Websocket.init_provider/1` does NOT open a TLS
  connection — that work moves to `activate/1` per
  `fix-overwatch-restart-storm` capability `operative-extensions` "Plugin
  providers SHALL NOT perform upstream I/O in `init_provider/1`".

  Approach: hand `init_provider/1` an unroutable URI and require it
  returns `{:ok, opts}` quickly without raising or blocking. If the TLS
  handshake fires, the call would block on DNS / connect attempts for
  many seconds and the test would either time out or raise; the
  assertion is "return quickly with the prepared opts and no failure".
  """

  alias Flezha.Plugin.Provider.Websocket

  use ExUnit.Case, async: true

  test "init_provider returns prepared opts without opening TLS connection" do
    opts = %{
      uri: "wss://flezha-test.invalid:9/never-connect",
      plugin_id: "ws_lifecycle_test"
    }

    {time_us, result} = :timer.tc(fn -> Websocket.init_provider(opts) end)

    assert {:ok, prepared_opts} = result

    # < 250 ms is comfortably below any DNS-then-TCP-connect attempt.
    assert time_us < 250_000,
           "init_provider took #{time_us}us — it must NOT block on upstream I/O"

    assert prepared_opts.uri == opts.uri
    assert prepared_opts.plugin_id == opts.plugin_id
    # Sentinel state the new activate/1 reads on the way out.
    assert is_pid(prepared_opts.overwatcher_pid)
  end

  describe "post_connect is validated at preparation" do
    defp prepared(post_connect) do
      Websocket.init_provider(%{
        uri: "wss://flezha-test.invalid:9/never-connect",
        plugin_id: "ws_post_connect_test",
        post_connect: post_connect
      })
    end

    test "a frame type that is neither text nor binary is refused" do
      assert {:error, message} =
               prepared(%{"payload" => %{"type" => "Binary", "message" => "hi"}})

      assert message =~ "type"
      assert message =~ "Binary"
    end

    test "a payload that cannot be decoded is refused, not silently emptied" do
      assert {:error, message} = prepared("{")
      assert is_binary(message)
    end

    test "both frame types are accepted, as a map and as its JSON string" do
      for type <- ["text", "binary"] do
        assert {:ok, opts} = prepared(%{"payload" => %{"type" => type, "message" => "hi"}})
        assert opts.post_connect["payload"]["type"] == type

        assert {:ok, opts} = prepared(~s({"payload":{"type":"#{type}","message":"hi"}}))
        assert opts.post_connect["payload"]["type"] == type
      end
    end

    test "no post_connect at all is still the empty map" do
      assert {:ok, opts} = prepared(nil)
      assert opts.post_connect == %{}
    end
  end
end
